package proxy

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/flashbots/go-utils/rpcserver"
	"github.com/flashbots/go-utils/rpctypes"
	"github.com/flashbots/go-utils/signature"
)

type SenderProxyConstantConfig struct {
	Log             *slog.Logger
	OrderflowSigner *signature.Signer
}

type SenderProxyConfig struct {
	SenderProxyConstantConfig
	BuilderConfigHubEndpoint string
	MaxRequestBodySizeBytes  int64
	ConnectionsPerPeer       int
}

type SenderProxy struct {
	SenderProxyConstantConfig
	ConfigHub *BuilderConfigHub
	Handler   http.Handler

	updatePeers chan []ConfighubBuilder
	shareQueue  chan *ParsedRequest

	PeerUpdateForce chan struct{}
}

func NewSenderProxy(config SenderProxyConfig) (*SenderProxy, error) {
	maxRequestBodySizeBytes := DefaultMaxRequestBodySizeBytes
	if config.MaxRequestBodySizeBytes != 0 {
		maxRequestBodySizeBytes = config.MaxRequestBodySizeBytes
	}

	prx := &SenderProxy{
		SenderProxyConstantConfig: config.SenderProxyConstantConfig,
		ConfigHub:                 NewBuilderConfigHub(config.Log, config.BuilderConfigHubEndpoint),
		Handler:                   nil,
		updatePeers:               make(chan []ConfighubBuilder),
		shareQueue:                make(chan *ParsedRequest),
		PeerUpdateForce:           make(chan struct{}),
	}

	handler, err := rpcserver.NewJSONRPCHandler(rpcserver.Methods{
		EthSendBundleMethod:         prx.EthSendBundle,
		MevSendBundleMethod:         prx.MevSendBundle,
		EthCancelBundleMethod:       prx.EthCancelBundle,
		EthSendRawTransactionMethod: prx.EthSendRawTransaction,
		BidSubsidiseBlockMethod:     prx.BidSubsidiseBlock,
	},
		rpcserver.JSONRPCHandlerOpts{
			Log:                     prx.Log,
			MaxRequestBodySizeBytes: maxRequestBodySizeBytes,
		},
	)
	if err != nil {
		return nil, err
	}
	prx.Handler = handler

	queue := ShareQueue{
		log:            prx.Log,
		queue:          prx.shareQueue,
		updatePeers:    prx.updatePeers,
		localBuilder:   nil,
		signer:         prx.OrderflowSigner,
		workersPerPeer: config.ConnectionsPerPeer,
	}
	go queue.Run()

	go func() {
		for {
			select {
			case _, more := <-prx.PeerUpdateForce:
				if !more {
					return
				}
			case <-time.After(peerUpdateTime):
			}
			builders, err := prx.ConfigHub.Builders(true)
			if err != nil {
				prx.Log.Error("Failed to update peers", slog.Any("error", err))
				continue
			}

			prx.Log.Info("Updated peers", slog.Int("peerCount", len(builders)))

			select {
			case prx.updatePeers <- builders:
			default:
			}
		}
	}()

	prx.PeerUpdateForce <- struct{}{}

	return prx, nil
}

func (prx *SenderProxy) Stop() {
	close(prx.shareQueue)
	close(prx.updatePeers)
	close(prx.PeerUpdateForce)
}

func (prx *SenderProxy) EthSendBundle(ctx context.Context, ethSendBundle rpctypes.EthSendBundleArgs) error {
	parsedRequest := ParsedRequest{
		ethSendBundle: &ethSendBundle,
		method:        EthSendBundleMethod,
	}

	err := ValidateEthSendBundle(&ethSendBundle, true)
	if err != nil {
		return err
	}

	// quick workaround for people setting timestamp to 0
	if ethSendBundle.MaxTimestamp != nil && *ethSendBundle.MaxTimestamp == 0 {
		ethSendBundle.MaxTimestamp = nil
	}
	if ethSendBundle.MinTimestamp != nil && *ethSendBundle.MinTimestamp == 0 {
		ethSendBundle.MinTimestamp = nil
	}
	// we set explicitly bundles to be v1 for all protect flow
	if ethSendBundle.Version == nil || *ethSendBundle.Version == "" {
		version := rpctypes.BundleVersionV1
		ethSendBundle.Version = &version
	}

	return prx.HandleParsedRequest(ctx, parsedRequest)
}

func (prx *SenderProxy) MevSendBundle(ctx context.Context, mevSendBundle rpctypes.MevSendBundleArgs) error {
	parsedRequest := ParsedRequest{
		mevSendBundle: &mevSendBundle,
		method:        MevSendBundleMethod,
	}

	err := ValidateMevSendBundle(&mevSendBundle, true)
	if err != nil {
		return err
	}

	return prx.HandleParsedRequest(ctx, parsedRequest)
}

func (prx *SenderProxy) EthCancelBundle(ctx context.Context, ethCancelBundle rpctypes.EthCancelBundleArgs) error {
	parsedRequest := ParsedRequest{
		ethCancelBundle: &ethCancelBundle,
		method:          EthCancelBundleMethod,
	}

	err := ValidateEthCancelBundle(&ethCancelBundle, true)
	if err != nil {
		return err
	}

	return prx.HandleParsedRequest(ctx, parsedRequest)
}

func (prx *SenderProxy) EthSendRawTransaction(ctx context.Context, ethSendRawTransaction rpctypes.EthSendRawTransactionArgs) error {
	parsedRequest := ParsedRequest{
		ethSendRawTransaction: &ethSendRawTransaction,
		method:                EthSendRawTransactionMethod,
	}
	return prx.HandleParsedRequest(ctx, parsedRequest)
}

func (prx *SenderProxy) BidSubsidiseBlock(ctx context.Context, bidSubsidiseBlock rpctypes.BidSubsisideBlockArgs) error {
	parsedRequest := ParsedRequest{
		bidSubsidiseBlock: &bidSubsidiseBlock,
		method:            BidSubsidiseBlockMethod,
	}

	return prx.HandleParsedRequest(ctx, parsedRequest)
}

func (prx *SenderProxy) HandleParsedRequest(ctx context.Context, parsedRequest ParsedRequest) error {
	parsedRequest.receivedAt = apiNow()
	// we set it explicitly to note that we need to proxy all calls to all peers
	parsedRequest.systemEndpoint = false
	prx.Log.Debug("Received request", slog.String("method", parsedRequest.method))

	select {
	case <-ctx.Done():
	case prx.shareQueue <- &parsedRequest:
	}
	return nil
}
