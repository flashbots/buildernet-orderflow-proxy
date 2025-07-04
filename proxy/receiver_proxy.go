package proxy

import (
	"context"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/flashbots/go-utils/rpcclient"
	"github.com/flashbots/go-utils/signature"
	"github.com/google/uuid"
	"github.com/hashicorp/golang-lru/v2/expirable"
	"golang.org/x/time/rate"
)

var (
	requestsRLUSize = 4096
	requestsRLUTTL  = time.Second * 12

	peerUpdateTime = time.Second * 30

	replacementNonceSize = 4096
	replacementNonceTTL  = time.Second * 5 * 12

	ReceiverProxyWorkerQueueSize = 10000
)

type replacementNonceKey struct {
	uuid   uuid.UUID
	signer common.Address
}

type ReceiverProxy struct {
	ReceiverProxyConstantConfig

	ConfigHub *BuilderConfigHub

	OrderflowSigner *signature.Signer

	UserHandler   http.Handler
	SystemHandler http.Handler

	updatePeers chan []ConfighubBuilder
	shareQueue  chan *ParsedRequest

	archiveQueue      chan *ParsedRequest
	archiveFlushQueue chan struct{}

	peersMu          sync.RWMutex
	lastFetchedPeers []ConfighubBuilder

	requestUniqueKeysRLU *expirable.LRU[uuid.UUID, struct{}]

	replacementNonceRLU *expirable.LRU[replacementNonceKey, int]

	peerUpdaterClose chan struct{}

	userAPIRateLimiter *rate.Limiter

	localBuilderSender LocalBuilderSender

	builderReadyEndpoint string
}

type ReceiverProxyConstantConfig struct {
	Log *slog.Logger
	// Name is optional field and it used to distringuish multiple proxies when running in the same process in tests
	Name                   string
	FlashbotsSignerAddress common.Address
	LocalBuilderEndpoint   string
}

type ReceiverProxyConfig struct {
	ReceiverProxyConstantConfig

	BuilderConfigHubEndpoint string
	ArchiveEndpoint          string
	ArchiveConnections       int
	BuilderReadyEndpoint     string

	// EthRPC should support eth_blockNumber API
	EthRPC string

	MaxRequestBodySizeBytes int64

	ConnectionsPerPeer int
	MaxUserRPS         int
	ArchiveWorkerCount int
}

func NewReceiverProxy(config ReceiverProxyConfig) (*ReceiverProxy, error) {
	orderflowSigner, err := signature.NewRandomSigner()
	if err != nil {
		return nil, err
	}

	limit := rate.Limit(config.MaxUserRPS)
	if config.MaxUserRPS == 0 {
		limit = rate.Inf
	}
	userAPIRateLimiter := rate.NewLimiter(limit, config.MaxUserRPS)
	localBuilderSender, err := NewLocalBuilderSender(config.Log, config.LocalBuilderEndpoint, config.ConnectionsPerPeer)
	if err != nil {
		return nil, err
	}
	prx := &ReceiverProxy{
		ReceiverProxyConstantConfig: config.ReceiverProxyConstantConfig,
		ConfigHub:                   NewBuilderConfigHub(config.Log, config.BuilderConfigHubEndpoint),
		OrderflowSigner:             orderflowSigner,
		requestUniqueKeysRLU:        expirable.NewLRU[uuid.UUID, struct{}](requestsRLUSize, nil, requestsRLUTTL),
		replacementNonceRLU:         expirable.NewLRU[replacementNonceKey, int](replacementNonceSize, nil, replacementNonceTTL),
		userAPIRateLimiter:          userAPIRateLimiter,
		localBuilderSender:          localBuilderSender,
		builderReadyEndpoint:        config.BuilderReadyEndpoint,
	}
	maxRequestBodySizeBytes := DefaultMaxRequestBodySizeBytes
	if config.MaxRequestBodySizeBytes != 0 {
		maxRequestBodySizeBytes = config.MaxRequestBodySizeBytes
	}

	systemHandler, err := prx.SystemJSONRPCHandler(maxRequestBodySizeBytes)
	if err != nil {
		return nil, err
	}
	prx.SystemHandler = systemHandler

	userHandler, err := prx.UserJSONRPCHandler(maxRequestBodySizeBytes)
	if err != nil {
		return nil, err
	}
	prx.UserHandler = userHandler

	shareQeueuCh := make(chan *ParsedRequest, ReceiverProxyWorkerQueueSize)
	updatePeersCh := make(chan []ConfighubBuilder)
	prx.shareQueue = shareQeueuCh
	prx.updatePeers = updatePeersCh

	queue := ShareQueue{
		name:           prx.Name,
		log:            prx.Log,
		queue:          shareQeueuCh,
		updatePeers:    updatePeersCh,
		signer:         prx.OrderflowSigner,
		workersPerPeer: config.ConnectionsPerPeer,
	}
	go queue.Run()

	archiveQueueCh := make(chan *ParsedRequest, ReceiverProxyWorkerQueueSize)
	archiveFlushCh := make(chan struct{})
	prx.archiveQueue = archiveQueueCh
	prx.archiveFlushQueue = archiveFlushCh
	archiveHTTPClient := HTTPClientWithMaxConnections(config.ArchiveConnections)
	archiveClient := rpcclient.NewClientWithOpts(config.ArchiveEndpoint, &rpcclient.RPCClientOpts{
		Signer:     orderflowSigner,
		HTTPClient: archiveHTTPClient,
	})
	archiveQueue := ArchiveQueue{
		log:               prx.Log,
		queue:             archiveQueueCh,
		flushQueue:        archiveFlushCh,
		archiveClient:     archiveClient,
		blockNumberSource: NewBlockNumberSource(config.EthRPC),
		workerCount:       config.ArchiveWorkerCount,
	}
	go archiveQueue.Run()

	prx.peerUpdaterClose = make(chan struct{})
	go func() {
		for {
			select {
			case _, more := <-prx.peerUpdaterClose:
				if !more {
					return
				}
			case <-time.After(peerUpdateTime):
				err := prx.RequestNewPeers()
				if err != nil {
					prx.Log.Error("Failed to update peers", slog.Any("error", err))
				}
			}
		}
	}()

	// request peers on the first start
	_ = prx.RequestNewPeers()

	return prx, nil
}

func (prx *ReceiverProxy) Stop() {
	close(prx.shareQueue)
	close(prx.updatePeers)
	close(prx.archiveQueue)
	close(prx.archiveFlushQueue)
	close(prx.peerUpdaterClose)
}

// RequestNewPeers updates currently available peers from the builder config hub
func (prx *ReceiverProxy) RequestNewPeers() error {
	builders, err := prx.ConfigHub.Builders(false)
	if err != nil {
		return err
	}

	prx.peersMu.Lock()
	prx.lastFetchedPeers = builders
	prx.peersMu.Unlock()

	select {
	case prx.updatePeers <- builders:
	default:
	}
	return nil
}

// FlushArchiveQueue forces the archive queue to flush
func (prx *ReceiverProxy) FlushArchiveQueue() {
	prx.archiveFlushQueue <- struct{}{}
}

func (prx *ReceiverProxy) RegisterSecrets(ctx context.Context) error {
	const maxRetries = 10
	const timeBetweenRetries = time.Second * 10

	retry := 0
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		err := prx.ConfigHub.RegisterCredentials(ctx, ConfighubOrderflowProxyCredentials{
			TLSCert:            "",
			EcdsaPubkeyAddress: prx.OrderflowSigner.Address(),
		})
		if err == nil {
			prx.Log.Info("Credentials registered on config hub")
			return nil
		}

		retry += 1
		if retry >= maxRetries {
			return err
		}
		prx.Log.Error("Fail to register credentials", slog.Any("error", err))
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(timeBetweenRetries):
		}
	}
}
