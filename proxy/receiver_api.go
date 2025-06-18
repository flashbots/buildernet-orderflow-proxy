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
	errUnknownPeer          = errors.New("unknown peers can't send to the system address")
	errSubsidyWrongEndpoint = errors.New("subsidy can only be called on system API")
	errSubsidyWrongCaller   = errors.New("subsidy can only be called by Flashbots")
	errRateLimiting         = errors.New("requests to user API are rate limited")

	errUUIDParse = errors.New("failed to parse UUID")

	apiNow = time.Now

	handleParsedRequestTimeout = time.Second * 1
)

func (prx *ReceiverProxy) SystemJSONRPCHandler(maxRequestBodySizeBytes int64) (*rpcserver.JSONRPCHandler, error) {
	handler, err := rpcserver.NewJSONRPCHandler(rpcserver.Methods{
		EthSendBundleMethod:         prx.EthSendBundleSystem,
		MevSendBundleMethod:         prx.MevSendBundleSystem,
		EthCancelBundleMethod:       prx.EthCancelBundleSystem,
		EthSendRawTransactionMethod: prx.EthSendRawTransactionSystem,
		BidSubsidiseBlockMethod:     prx.BidSubsidiseBlockSystem,
	},
		rpcserver.JSONRPCHandlerOpts{
			ServerName:                       "system_server",
			Log:                              prx.Log,
			MaxRequestBodySizeBytes:          maxRequestBodySizeBytes,
			VerifyRequestSignatureFromHeader: true,
		},
	)

	return handler, err
}

func (prx *ReceiverProxy) UserJSONRPCHandler(maxRequestBodySizeBytes int64) (*rpcserver.JSONRPCHandler, error) {
	landingPageHTML, err := prx.prepHTML()
	if err != nil {
		return nil, err
	}

	handler, err := rpcserver.NewJSONRPCHandler(rpcserver.Methods{
		EthSendBundleMethod:         prx.EthSendBundleUser,
		MevSendBundleMethod:         prx.MevSendBundleUser,
		EthCancelBundleMethod:       prx.EthCancelBundleUser,
		EthSendRawTransactionMethod: prx.EthSendRawTransactionUser,
		BidSubsidiseBlockMethod:     prx.BidSubsidiseBlockUser,
	},
		rpcserver.JSONRPCHandlerOpts{
			ServerName:                       "user_server",
			Log:                              prx.Log,
			MaxRequestBodySizeBytes:          maxRequestBodySizeBytes,
			VerifyRequestSignatureFromHeader: true,
			GetResponseContent:               landingPageHTML,
		},
	)

	return handler, err
}

func (prx *ReceiverProxy) ValidateSigner(ctx context.Context, req *ParsedRequest, systemEndpoint bool) error {
	req.signer = rpcserver.GetSigner(ctx)
	if !systemEndpoint {
		req.peerName = "user-request"
		return nil
	}

	prx.Log.Debug("Received signed request on a system endpoint", slog.Any("signer", req.signer))

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

func (prx *ReceiverProxy) EthSendBundle(ctx context.Context, ethSendBundle rpctypes.EthSendBundleArgs, systemEndpoint bool) error {
	startAt := time.Now()
	parsedRequest := ParsedRequest{
		systemEndpoint: systemEndpoint,
		ethSendBundle:  &ethSendBundle,
		method:         EthSendBundleMethod,
		size:           rpcserver.GetRequestSize(ctx),
	}

	err := prx.ValidateSigner(ctx, &parsedRequest, systemEndpoint)
	if err != nil {
		return err
	}

	_, err = EnsureReplacementUUID(&ethSendBundle)
	if err != nil {
		return err
	}

	err = ValidateEthSendBundle(&ethSendBundle, systemEndpoint)
	if err != nil {
		return err
	}

	incRequestDurationStep(time.Since(startAt), parsedRequest.method, "", "validation")
	startAt = time.Now()

	// For direct orderflow we extract signing address from header
	// We also default to v2 bundle version if it's not set
	if !systemEndpoint {
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

	incRequestDurationStep(time.Since(startAt), parsedRequest.method, "", "add_fields")

	return prx.HandleParsedRequest(ctx, parsedRequest)
}

func (prx *ReceiverProxy) EthSendBundleSystem(ctx context.Context, ethSendBundle rpctypes.EthSendBundleArgs) error {
	return prx.EthSendBundle(ctx, ethSendBundle, true)
}

func (prx *ReceiverProxy) EthSendBundleUser(ctx context.Context, ethSendBundle rpctypes.EthSendBundleArgs) error {
	return prx.EthSendBundle(ctx, ethSendBundle, false)
}

func (prx *ReceiverProxy) MevSendBundle(ctx context.Context, mevSendBundle rpctypes.MevSendBundleArgs, systemEndpoint bool) error {
	startAt := time.Now()
	parsedRequest := ParsedRequest{
		systemEndpoint: systemEndpoint,
		mevSendBundle:  &mevSendBundle,
		method:         MevSendBundleMethod,
		size:           rpcserver.GetRequestSize(ctx),
	}

	err := prx.ValidateSigner(ctx, &parsedRequest, systemEndpoint)
	if err != nil {
		return err
	}

	err = ValidateMevSendBundle(&mevSendBundle, systemEndpoint)
	if err != nil {
		return err
	}

	incRequestDurationStep(time.Since(startAt), parsedRequest.method, "", "validation")
	startAt = time.Now()

	if !systemEndpoint {
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

	incRequestDurationStep(time.Since(startAt), parsedRequest.method, "", "add_fields")

	return prx.HandleParsedRequest(ctx, parsedRequest)
}

func (prx *ReceiverProxy) MevSendBundleSystem(ctx context.Context, mevSendBundle rpctypes.MevSendBundleArgs) error {
	return prx.MevSendBundle(ctx, mevSendBundle, true)
}

func (prx *ReceiverProxy) MevSendBundleUser(ctx context.Context, mevSendBundle rpctypes.MevSendBundleArgs) error {
	return prx.MevSendBundle(ctx, mevSendBundle, false)
}

func (prx *ReceiverProxy) EthCancelBundle(ctx context.Context, ethCancelBundle rpctypes.EthCancelBundleArgs, systemEndpoint bool) error {
	parsedRequest := ParsedRequest{
		systemEndpoint:  systemEndpoint,
		ethCancelBundle: &ethCancelBundle,
		method:          EthCancelBundleMethod,
		size:            rpcserver.GetRequestSize(ctx),
	}

	err := prx.ValidateSigner(ctx, &parsedRequest, systemEndpoint)
	if err != nil {
		return err
	}

	err = ValidateEthCancelBundle(&ethCancelBundle, systemEndpoint)
	if err != nil {
		return err
	}

	if !systemEndpoint {
		ethCancelBundle.SigningAddress = &parsedRequest.signer
	}
	return prx.HandleParsedRequest(ctx, parsedRequest)
}

func (prx *ReceiverProxy) EthCancelBundleSystem(ctx context.Context, ethCancelBundle rpctypes.EthCancelBundleArgs) error {
	return prx.EthCancelBundle(ctx, ethCancelBundle, true)
}

func (prx *ReceiverProxy) EthCancelBundleUser(ctx context.Context, ethCancelBundle rpctypes.EthCancelBundleArgs) error {
	return prx.EthCancelBundle(ctx, ethCancelBundle, false)
}

func (prx *ReceiverProxy) EthSendRawTransaction(ctx context.Context, ethSendRawTransaction rpctypes.EthSendRawTransactionArgs, systemEndpoint bool) error {
	parsedRequest := ParsedRequest{
		systemEndpoint:        systemEndpoint,
		ethSendRawTransaction: &ethSendRawTransaction,
		method:                EthSendRawTransactionMethod,
		size:                  rpcserver.GetRequestSize(ctx),
	}
	err := prx.ValidateSigner(ctx, &parsedRequest, systemEndpoint)
	if err != nil {
		return err
	}

	uniqueKey := ethSendRawTransaction.UniqueKey()
	parsedRequest.requestArgUniqueKey = &uniqueKey

	return prx.HandleParsedRequest(ctx, parsedRequest)
}

func (prx *ReceiverProxy) EthSendRawTransactionSystem(ctx context.Context, ethSendRawTransaction rpctypes.EthSendRawTransactionArgs) error {
	return prx.EthSendRawTransaction(ctx, ethSendRawTransaction, true)
}

func (prx *ReceiverProxy) EthSendRawTransactionUser(ctx context.Context, ethSendRawTransaction rpctypes.EthSendRawTransactionArgs) error {
	return prx.EthSendRawTransaction(ctx, ethSendRawTransaction, false)
}

func (prx *ReceiverProxy) BidSubsidiseBlock(ctx context.Context, bidSubsidiseBlock rpctypes.BidSubsisideBlockArgs, systemEndpoint bool) error {
	if !systemEndpoint {
		return errSubsidyWrongEndpoint
	}

	parsedRequest := ParsedRequest{
		systemEndpoint:    systemEndpoint,
		bidSubsidiseBlock: &bidSubsidiseBlock,
		method:            BidSubsidiseBlockMethod,
		size:              rpcserver.GetRequestSize(ctx),
	}

	err := prx.ValidateSigner(ctx, &parsedRequest, systemEndpoint)
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

func (prx *ReceiverProxy) BidSubsidiseBlockSystem(ctx context.Context, bidSubsidiseBlock rpctypes.BidSubsisideBlockArgs) error {
	return prx.BidSubsidiseBlock(ctx, bidSubsidiseBlock, true)
}

func (prx *ReceiverProxy) BidSubsidiseBlockUser(ctx context.Context, bidSubsidiseBlock rpctypes.BidSubsisideBlockArgs) error {
	return prx.BidSubsidiseBlock(ctx, bidSubsidiseBlock, false)
}

type ParsedRequest struct {
	systemEndpoint        bool
	signer                common.Address
	method                string
	peerName              string
	size                  int
	receivedAt            time.Time
	requestArgUniqueKey   *uuid.UUID
	ethSendBundle         *rpctypes.EthSendBundleArgs
	mevSendBundle         *rpctypes.MevSendBundleArgs
	ethCancelBundle       *rpctypes.EthCancelBundleArgs
	ethSendRawTransaction *rpctypes.EthSendRawTransactionArgs
	bidSubsidiseBlock     *rpctypes.BidSubsisideBlockArgs

	serializedJSONRPCRequest []byte
	signatureHeader          string
}

func (prx *ReceiverProxy) HandleParsedRequest(ctx context.Context, parsedRequest ParsedRequest) error {
	startAt := time.Now()
	ctx, cancel := context.WithTimeout(ctx, handleParsedRequestTimeout)
	defer cancel()

	parsedRequest.receivedAt = apiNow()
	prx.Log.Debug("Received request", slog.Bool("isSystemEndpoint", parsedRequest.systemEndpoint), slog.String("method", parsedRequest.method))
	if parsedRequest.systemEndpoint {
		incAPIIncomingRequestsByPeer(parsedRequest.peerName)
	}
	if parsedRequest.requestArgUniqueKey != nil {
		if prx.requestUniqueKeysRLU.Contains(*parsedRequest.requestArgUniqueKey) {
			incAPIDuplicateRequestsByPeer(parsedRequest.peerName)
			return nil
		}
		prx.requestUniqueKeysRLU.Add(*parsedRequest.requestArgUniqueKey, struct{}{})
	}

	incRequestDurationStep(time.Since(startAt), parsedRequest.method, "", "validation")
	startAt = time.Now()

	if !parsedRequest.systemEndpoint {
		err := prx.userAPIRateLimiter.Wait(ctx)
		if err != nil {
			incAPIUserRateLimits()
			return errors.Join(errRateLimiting, err)
		}
	}

	incRequestDurationStep(time.Since(startAt), parsedRequest.method, "", "rate_limiting")
	startAt = time.Now()

	err := SerializeParsedRequestForSharing(&parsedRequest, prx.OrderflowSigner)
	if err != nil {
		prx.Log.Warn("Failed to serialize request for sharing", slog.Any("error", err))
	}

	incRequestDurationStep(time.Since(startAt), parsedRequest.method, "", "serialize_parsed_request")
	startAt = time.Now()

	select {
	case <-ctx.Done():
		prx.Log.Error("Shared queue is stalling")
	case prx.shareQueue <- &parsedRequest:
	}

	incRequestDurationStep(time.Since(startAt), parsedRequest.method, "", "share_queue")
	startAt = time.Now()

	if !parsedRequest.systemEndpoint {
		select {
		case <-ctx.Done():
			prx.Log.Error("Archive queue is stalling")
		case prx.archiveQueue <- &parsedRequest:
		}
	}

	incRequestDurationStep(time.Since(startAt), parsedRequest.method, "", "archive_queue")
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
