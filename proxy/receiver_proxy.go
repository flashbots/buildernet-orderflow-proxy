package proxy

import (
	"context"
	"crypto/tls"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/flashbots/go-utils/rpcclient"
	"github.com/flashbots/go-utils/signature"
	utils_tls "github.com/flashbots/go-utils/tls"
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
	PublicCertPEM   []byte
	Certificate     tls.Certificate

	localBuilder rpcclient.RPCClient

	UserHandler   http.Handler
	SystemHandler http.Handler
	CertHandler   http.Handler // this endpoint just returns generated certificate

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
}

type ReceiverProxyConstantConfig struct {
	Log *slog.Logger
	// Name is optional field and it used to distringuish multiple proxies when running in the same process in tests
	Name                   string
	FlashbotsSignerAddress common.Address
}

type ReceiverProxyConfig struct {
	ReceiverProxyConstantConfig

	CertValidDuration time.Duration
	CertHosts         []string
	CertPath          string
	CertKeyPath       string

	BuilderConfigHubEndpoint string
	ArchiveEndpoint          string
	ArchiveConnections       int
	LocalBuilderEndpoint     string

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

	cert, key, err := utils_tls.GetOrGenerateTLS(config.CertPath, config.CertKeyPath, config.CertValidDuration, config.CertHosts)
	if err != nil {
		return nil, err
	}

	certificate, err := tls.X509KeyPair(cert, key)
	if err != nil {
		return nil, err
	}

	localBuilder := rpcclient.NewClient(config.LocalBuilderEndpoint)

	limit := rate.Limit(config.MaxUserRPS)
	if config.MaxUserRPS == 0 {
		limit = rate.Inf
	}
	userAPIRateLimiter := rate.NewLimiter(limit, config.MaxUserRPS)
	prx := &ReceiverProxy{
		ReceiverProxyConstantConfig: config.ReceiverProxyConstantConfig,
		ConfigHub:                   NewBuilderConfigHub(config.Log, config.BuilderConfigHubEndpoint),
		OrderflowSigner:             orderflowSigner,
		PublicCertPEM:               cert,
		Certificate:                 certificate,
		localBuilder:                localBuilder,
		requestUniqueKeysRLU:        expirable.NewLRU[uuid.UUID, struct{}](requestsRLUSize, nil, requestsRLUTTL),
		replacementNonceRLU:         expirable.NewLRU[replacementNonceKey, int](replacementNonceSize, nil, replacementNonceTTL),
		userAPIRateLimiter:          userAPIRateLimiter,
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

	prx.CertHandler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Add("Content-Type", "application/octet-stream")
		_, err := w.Write(prx.PublicCertPEM)
		if err != nil {
			prx.Log.Warn("Failed to serve certificate", slog.Any("error", err))
		}
	})

	shareQeueuCh := make(chan *ParsedRequest, ReceiverProxyWorkerQueueSize)
	updatePeersCh := make(chan []ConfighubBuilder)
	prx.shareQueue = shareQeueuCh
	prx.updatePeers = updatePeersCh
	queue := ShareQueue{
		name:           prx.Name,
		log:            prx.Log,
		queue:          shareQeueuCh,
		updatePeers:    updatePeersCh,
		localBuilder:   prx.localBuilder,
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

func (prx *ReceiverProxy) TLSConfig() *tls.Config {
	return &tls.Config{
		Certificates: []tls.Certificate{prx.Certificate},
		MinVersion:   tls.VersionTLS13,
	}
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
			TLSCert:            string(prx.PublicCertPEM),
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
