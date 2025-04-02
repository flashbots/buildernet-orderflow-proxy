package proxy

import (
	"bytes"
	"context"
	_ "embed"
	"errors"
	"html/template"
	"log/slog"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/flashbots/go-utils/rpcserver"
	"github.com/flashbots/go-utils/rpctypes"
	"github.com/google/uuid"
)

const DefaultMaxRequestBodySizeBytes = int64(30 * 1024 * 1024) // 30 MB

const (
	FlashbotsPeerName = "flashbots"

	EthSendBundleMethod         = "eth_sendBundle"
	MevSendBundleMethod         = "mev_sendBundle"
	EthCancelBundleMethod       = "eth_cancelBundle"
	EthSendRawTransactionMethod = "eth_sendRawTransaction"
	BidSubsidiseBlockMethod     = "bid_subsidiseBlock"
)

//go:embed html/index.html
var landingPageHTML string

var (
	errUnknownPeer          = errors.New("unknown peers can't send to the public address")
	errSubsidyWrongEndpoint = errors.New("subsidy can only be called on public method")
	errSubsidyWrongCaller   = errors.New("subsidy can only be called by Flashbots")
	errRateLimiting         = errors.New("requests to local API are rate limited")

	errUUIDParse = errors.New("failed to parse UUID")

	apiNow = time.Now

	handleParsedRequestTimeout = time.Second * 1
)

func (prx *ReceiverProxy) PublicJSONRPCHandler(maxRequestBodySizeBytes int64) (*rpcserver.JSONRPCHandler, error) {
	handler, err := rpcserver.NewJSONRPCHandler(rpcserver.Methods{
		EthSendBundleMethod:         prx.EthSendBundlePublic,
		MevSendBundleMethod:         prx.MevSendBundlePublic,
		EthCancelBundleMethod:       prx.EthCancelBundlePublic,
		EthSendRawTransactionMethod: prx.EthSendRawTransactionPublic,
		BidSubsidiseBlockMethod:     prx.BidSubsidiseBlockPublic,
	},
		rpcserver.JSONRPCHandlerOpts{
			ServerName:                       "public_server",
			Log:                              prx.Log,
			MaxRequestBodySizeBytes:          maxRequestBodySizeBytes,
			VerifyRequestSignatureFromHeader: true,
		},
	)

	return handler, err
}

func (prx *ReceiverProxy) LocalJSONRPCHandler(maxRequestBodySizeBytes int64) (*rpcserver.JSONRPCHandler, error) {
	landingPageHTML, err := prx.prepHTML()
	if err != nil {
		return nil, err
	}

	handler, err := rpcserver.NewJSONRPCHandler(rpcserver.Methods{
		EthSendBundleMethod:         prx.EthSendBundleLocal,
		MevSendBundleMethod:         prx.MevSendBundleLocal,
		EthCancelBundleMethod:       prx.EthCancelBundleLocal,
		EthSendRawTransactionMethod: prx.EthSendRawTransactionLocal,
		BidSubsidiseBlockMethod:     prx.BidSubsidiseBlockLocal,
	},
		rpcserver.JSONRPCHandlerOpts{
			ServerName:                       "local_server",
			Log:                              prx.Log,
			MaxRequestBodySizeBytes:          maxRequestBodySizeBytes,
			VerifyRequestSignatureFromHeader: true,
			GetResponseContent:               landingPageHTML,
		},
	)

	return handler, err
}

func (prx *ReceiverProxy) ValidateSigner(ctx context.Context, req *ParsedRequest, publicEndpoint bool) error {
	req.signer = rpcserver.GetSigner(ctx)
	if !publicEndpoint {
		req.peerName = "local-request"
		return nil
	}

	prx.Log.Debug("Received signed request on a public endpoint", slog.Any("signer", req.signer))

	if req.signer == prx.FlashbotsSignerAddress {
		req.peerName = FlashbotsPeerName
		return nil
	}

	prx.peersMu.RLock()
	defer prx.peersMu.RUnlock()
	found := false
	peerName := ""
	for _, peer := range prx.lastFetchedPeers {
		if req.signer == peer.OrderflowProxy.EcdsaPubkeyAddress {
			found = true
			peerName = peer.Name
			break
		}
	}
	if !found {
		return errUnknownPeer
	}
	req.peerName = peerName
	return nil
}

func (prx *ReceiverProxy) EthSendBundle(ctx context.Context, ethSendBundle rpctypes.EthSendBundleArgs, publicEndpoint bool) error {
	parsedRequest := ParsedRequest{
		publicEndpoint: publicEndpoint,
		ethSendBundle:  &ethSendBundle,
		method:         EthSendBundleMethod,
	}

	err := prx.ValidateSigner(ctx, &parsedRequest, publicEndpoint)
	if err != nil {
		return err
	}

	err = ValidateEthSendBundle(&ethSendBundle, publicEndpoint)
	if err != nil {
		return err
	}

	if !publicEndpoint {
		ethSendBundle.SigningAddress = &parsedRequest.signer
		// when receiving orderflow directly we default to v2 (currently latest version)
		// for non-publicEndpoint we set v1 explicitly on sender_proxy, it might be a bit confusing
		if ethSendBundle.Version == nil || *ethSendBundle.Version == "" {
			version := rpctypes.BundleVersionV2
			ethSendBundle.Version = &version
		}
		if ethSendBundle.ReplacementUUID != nil {
			timestampInt := apiNow().UnixMicro()
			var timestamp uint64
			if timestampInt < 0 {
				timestamp = 0
			} else {
				timestamp = uint64(timestampInt)
			}

			if ethSendBundle.ReplacementNonce == nil {
				ethSendBundle.ReplacementNonce = &timestamp
			}
		}
	}

	uniqueKey := ethSendBundle.UniqueKey()
	parsedRequest.requestArgUniqueKey = &uniqueKey

	return prx.HandleParsedRequest(ctx, parsedRequest)
}

func (prx *ReceiverProxy) EthSendBundlePublic(ctx context.Context, ethSendBundle rpctypes.EthSendBundleArgs) error {
	return prx.EthSendBundle(ctx, ethSendBundle, true)
}

func (prx *ReceiverProxy) EthSendBundleLocal(ctx context.Context, ethSendBundle rpctypes.EthSendBundleArgs) error {
	return prx.EthSendBundle(ctx, ethSendBundle, false)
}

func (prx *ReceiverProxy) MevSendBundle(ctx context.Context, mevSendBundle rpctypes.MevSendBundleArgs, publicEndpoint bool) error {
	parsedRequest := ParsedRequest{
		publicEndpoint: publicEndpoint,
		mevSendBundle:  &mevSendBundle,
		method:         MevSendBundleMethod,
	}

	err := prx.ValidateSigner(ctx, &parsedRequest, publicEndpoint)
	if err != nil {
		return err
	}

	err = ValidateMevSendBundle(&mevSendBundle, publicEndpoint)
	if err != nil {
		return err
	}

	if !publicEndpoint {
		mevSendBundle.Metadata = &rpctypes.MevBundleMetadata{
			Signer: &parsedRequest.signer,
		}
		if mevSendBundle.ReplacementUUID != "" {
			replUUID, err := uuid.Parse(mevSendBundle.ReplacementUUID)
			if err != nil {
				return errors.Join(errUUIDParse, err)
			}
			replacementKey := replacementNonceKey{
				uuid:   replUUID,
				signer: parsedRequest.signer,
			}
			// this is not atomic but the normal user will not send multiple replacements in parallel
			nonce, ok := prx.replacementNonceRLU.Peek(replacementKey)
			if ok {
				nonce += 1
			} else {
				nonce = 0
			}
			prx.replacementNonceRLU.Add(replacementKey, nonce)
			mevSendBundle.Metadata.ReplacementNonce = &nonce

			if len(mevSendBundle.Body) == 0 {
				cancelled := true
				mevSendBundle.Metadata.Cancelled = &cancelled
			}
		}
	}

	// @note: unique key filterst same requests and it can interact with cancellations (you can't cancel multiple times per block)
	uniqueKey := mevSendBundle.UniqueKey()
	parsedRequest.requestArgUniqueKey = &uniqueKey

	return prx.HandleParsedRequest(ctx, parsedRequest)
}

func (prx *ReceiverProxy) MevSendBundlePublic(ctx context.Context, mevSendBundle rpctypes.MevSendBundleArgs) error {
	return prx.MevSendBundle(ctx, mevSendBundle, true)
}

func (prx *ReceiverProxy) MevSendBundleLocal(ctx context.Context, mevSendBundle rpctypes.MevSendBundleArgs) error {
	return prx.MevSendBundle(ctx, mevSendBundle, false)
}

func (prx *ReceiverProxy) EthCancelBundle(ctx context.Context, ethCancelBundle rpctypes.EthCancelBundleArgs, publicEndpoint bool) error {
	parsedRequest := ParsedRequest{
		publicEndpoint:  publicEndpoint,
		ethCancelBundle: &ethCancelBundle,
		method:          EthCancelBundleMethod,
	}

	err := prx.ValidateSigner(ctx, &parsedRequest, publicEndpoint)
	if err != nil {
		return err
	}

	err = ValidateEthCancelBundle(&ethCancelBundle, publicEndpoint)
	if err != nil {
		return err
	}

	if !publicEndpoint {
		ethCancelBundle.SigningAddress = &parsedRequest.signer
	}
	return prx.HandleParsedRequest(ctx, parsedRequest)
}

func (prx *ReceiverProxy) EthCancelBundlePublic(ctx context.Context, ethCancelBundle rpctypes.EthCancelBundleArgs) error {
	return prx.EthCancelBundle(ctx, ethCancelBundle, true)
}

func (prx *ReceiverProxy) EthCancelBundleLocal(ctx context.Context, ethCancelBundle rpctypes.EthCancelBundleArgs) error {
	return prx.EthCancelBundle(ctx, ethCancelBundle, false)
}

func (prx *ReceiverProxy) EthSendRawTransaction(ctx context.Context, ethSendRawTransaction rpctypes.EthSendRawTransactionArgs, publicEndpoint bool) error {
	parsedRequest := ParsedRequest{
		publicEndpoint:        publicEndpoint,
		ethSendRawTransaction: &ethSendRawTransaction,
		method:                EthSendRawTransactionMethod,
	}
	err := prx.ValidateSigner(ctx, &parsedRequest, publicEndpoint)
	if err != nil {
		return err
	}

	uniqueKey := ethSendRawTransaction.UniqueKey()
	parsedRequest.requestArgUniqueKey = &uniqueKey

	return prx.HandleParsedRequest(ctx, parsedRequest)
}

func (prx *ReceiverProxy) EthSendRawTransactionPublic(ctx context.Context, ethSendRawTransaction rpctypes.EthSendRawTransactionArgs) error {
	return prx.EthSendRawTransaction(ctx, ethSendRawTransaction, true)
}

func (prx *ReceiverProxy) EthSendRawTransactionLocal(ctx context.Context, ethSendRawTransaction rpctypes.EthSendRawTransactionArgs) error {
	return prx.EthSendRawTransaction(ctx, ethSendRawTransaction, false)
}

func (prx *ReceiverProxy) BidSubsidiseBlock(ctx context.Context, bidSubsidiseBlock rpctypes.BidSubsisideBlockArgs, publicEndpoint bool) error {
	if !publicEndpoint {
		return errSubsidyWrongEndpoint
	}

	parsedRequest := ParsedRequest{
		publicEndpoint:    publicEndpoint,
		bidSubsidiseBlock: &bidSubsidiseBlock,
		method:            BidSubsidiseBlockMethod,
	}

	err := prx.ValidateSigner(ctx, &parsedRequest, publicEndpoint)
	if err != nil {
		return err
	}

	if parsedRequest.signer != prx.FlashbotsSignerAddress {
		return errSubsidyWrongCaller
	}

	uniqueKey := bidSubsidiseBlock.UniqueKey()
	parsedRequest.requestArgUniqueKey = &uniqueKey

	return prx.HandleParsedRequest(ctx, parsedRequest)
}

func (prx *ReceiverProxy) BidSubsidiseBlockPublic(ctx context.Context, bidSubsidiseBlock rpctypes.BidSubsisideBlockArgs) error {
	return prx.BidSubsidiseBlock(ctx, bidSubsidiseBlock, true)
}

func (prx *ReceiverProxy) BidSubsidiseBlockLocal(ctx context.Context, bidSubsidiseBlock rpctypes.BidSubsisideBlockArgs) error {
	return prx.BidSubsidiseBlock(ctx, bidSubsidiseBlock, false)
}

type ParsedRequest struct {
	publicEndpoint        bool
	signer                common.Address
	method                string
	peerName              string
	receivedAt            time.Time
	requestArgUniqueKey   *uuid.UUID
	ethSendBundle         *rpctypes.EthSendBundleArgs
	mevSendBundle         *rpctypes.MevSendBundleArgs
	ethCancelBundle       *rpctypes.EthCancelBundleArgs
	ethSendRawTransaction *rpctypes.EthSendRawTransactionArgs
	bidSubsidiseBlock     *rpctypes.BidSubsisideBlockArgs
}

func (prx *ReceiverProxy) HandleParsedRequest(ctx context.Context, parsedRequest ParsedRequest) error {
	ctx, cancel := context.WithTimeout(ctx, handleParsedRequestTimeout)
	defer cancel()

	parsedRequest.receivedAt = apiNow()
	prx.Log.Debug("Received request", slog.Bool("isPublicEndpoint", parsedRequest.publicEndpoint), slog.String("method", parsedRequest.method))
	if parsedRequest.publicEndpoint {
		incAPIIncomingRequestsByPeer(parsedRequest.peerName)
	}
	if parsedRequest.requestArgUniqueKey != nil {
		if prx.requestUniqueKeysRLU.Contains(*parsedRequest.requestArgUniqueKey) {
			incAPIDuplicateRequestsByPeer(parsedRequest.peerName)
			return nil
		}
		prx.requestUniqueKeysRLU.Add(*parsedRequest.requestArgUniqueKey, struct{}{})
	}
	if !parsedRequest.publicEndpoint {
		err := prx.localAPIRateLimiter.Wait(ctx)
		if err != nil {
			incAPILocalRateLimits()
			return errors.Join(errRateLimiting, err)
		}
	}
	select {
	case <-ctx.Done():
		prx.Log.Error("Shared queue is stalling")
	case prx.shareQueue <- &parsedRequest:
	}
	if !parsedRequest.publicEndpoint {
		select {
		case <-ctx.Done():
			prx.Log.Error("Archive queue is stalling")
		case prx.archiveQueue <- &parsedRequest:
		}
	}
	return nil
}

func (prx *ReceiverProxy) prepHTML() ([]byte, error) {
	templ, err := template.New("index").Parse(landingPageHTML)
	if err != nil {
		return nil, err
	}

	htmlData := struct {
		Cert string
	}{
		Cert: string(prx.PublicCertPEM),
	}
	htmlBytes := bytes.Buffer{}
	err = templ.Execute(&htmlBytes, htmlData)
	if err != nil {
		return nil, err
	}

	return htmlBytes.Bytes(), nil
}
