package proxy

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/flashbots/go-utils/jsonrpc"
	"github.com/flashbots/go-utils/rpcclient"
	"github.com/flashbots/go-utils/signature"
	"github.com/goccy/go-json"
	"github.com/valyala/fasthttp"
)

var (
	ShareWorkerQueueSize = 10000
	requestTimeout       = time.Second * 10

	errUnknownRequestType = errors.New("unknown request type for sharing")
)

const (
	bigRequestSize = 50_000
)

type ShareQueue struct {
	name        string
	log         *slog.Logger
	queue       chan *ParsedRequest
	updatePeers chan []ConfighubBuilder
	signer      *signature.Signer
	// if > 0 share queue will spawn multiple senders per peer
	workersPerPeer int
}

type shareQueuePeer struct {
	ch       chan *ParsedRequest
	name     string
	client   *fasthttp.Client
	conf     ConfighubBuilder
	endpoint string
}

func newShareQueuePeer(name string, client *fasthttp.Client, conf ConfighubBuilder, endpoint string) shareQueuePeer {
	return shareQueuePeer{
		ch:       make(chan *ParsedRequest, ShareWorkerQueueSize),
		name:     name,
		client:   client,
		conf:     conf,
		endpoint: endpoint,
	}
}

func (p *shareQueuePeer) Close() {
	close(p.ch)
}

func (p *shareQueuePeer) SendRequest(log *slog.Logger, request *ParsedRequest) {
	select {
	case p.ch <- request:
	default:
		log.Error("Peer is stalling on requests", slog.String("peer", p.name))
		incShareQueuePeerStallingErrors(p.name)
	}
}

func (sq *ShareQueue) Run() {
	workersPerPeer := 1
	if sq.workersPerPeer > 0 {
		workersPerPeer = sq.workersPerPeer
	}
	var peers []shareQueuePeer
	for {
		select {
		case req, more := <-sq.queue:
			sq.log.Debug("Share queue received a request", slog.String("name", sq.name), slog.String("method", req.method))
			if !more {
				sq.log.Info("Share queue closing, queue channel closed")
				return
			}
			if !req.systemEndpoint {
				for _, peer := range peers {
					peer.SendRequest(sq.log, req)
				}
			}
		case newPeers, more := <-sq.updatePeers:
			if !more {
				sq.log.Info("Share queue closing, peer channel closed")
				return
			}

			var peersToKeep []shareQueuePeer
			var peersToClose []shareQueuePeer

		PeerLoop:
			for _, peer := range peers {
				for _, npi := range newPeers {
					if peer.conf == npi {
						// peer found do not close
						peersToKeep = append(peersToKeep, peer)
						continue PeerLoop
					}
				}
				peersToClose = append(peersToClose, peer)
			}

			var newPeersToOpen []ConfighubBuilder
		NewPeerLoop:
			for _, npi := range newPeers {
				for _, peer := range peersToKeep {
					if peer.conf == npi {
						continue NewPeerLoop
					}
				}
				newPeersToOpen = append(newPeersToOpen, npi)
			}

			for _, peer := range peersToClose {
				peer.Close()
			}

			peers = peersToKeep
			for _, info := range newPeersToOpen {
				// don't send to yourself
				if info.OrderflowProxy.EcdsaPubkeyAddress == sq.signer.Address() {
					continue
				}
				client, err := NewFastHTTPClient([]byte(info.TLSCert()), workersPerPeer)
				if err != nil {
					sq.log.Error("Failed to create a peer client3", slog.Any("error", err))
					shareQueueInternalErrors.Inc()
					continue
				}

				sq.log.Info("Created client for peer", slog.String("peer", info.Name), slog.String("name", sq.name))
				newPeer := newShareQueuePeer(info.Name, client, info, info.SystemAPIAddress())
				peers = append(peers, newPeer)
				for worker := range workersPerPeer {
					go sq.proxyRequests(&newPeer, worker)
				}
			}
		}
	}
}

type LocalBuilderSender struct {
	logger   *slog.Logger
	client   *fasthttp.Client
	endpoint string
}

func NewLocalBuilderSender(logger *slog.Logger, endpoint string, maxOpenConnections int) (LocalBuilderSender, error) {
	logger = logger.With(slog.String("peer", "local-builder"))

	client, err := NewFastHTTPClient(nil, maxOpenConnections)
	if err != nil {
		return LocalBuilderSender{}, err
	}

	return LocalBuilderSender{
		logger, client, endpoint,
	}, nil
}

func (s *LocalBuilderSender) SendRequest(req *ParsedRequest) error {
	request := fasthttp.AcquireRequest()
	request.SetRequestURI(s.endpoint)
	request.Header.SetMethod(http.MethodPost)
	request.Header.SetContentTypeBytes([]byte("application/json"))
	defer fasthttp.ReleaseRequest(request)

	return sendShareRequest(s.logger, req, request, s.client, "local-builder")
}

func sendShareRequest(logger *slog.Logger, req *ParsedRequest, request *fasthttp.Request, client *fasthttp.Client, peerName string) error {
	if req.serializedJSONRPCRequest == nil {
		logger.Debug("Skip sharing request that is not serialized properly")
		return nil
	}

	timeInQueue := time.Since(req.receivedAt)

	request.Header.Set(signature.HTTPHeader, req.signatureHeader)
	request.SetBodyRaw(req.serializedJSONRPCRequest)

	resp := fasthttp.AcquireResponse()
	start := time.Now()
	err := client.DoTimeout(request, resp, requestTimeout)
	requestDuration := time.Since(start)
	timeE2E := timeInQueue + requestDuration

	isBig := req.size >= bigRequestSize
	timeShareQueuePeerQueueDuration(peerName, timeInQueue, req.method, req.systemEndpoint, isBig)
	timeShareQueuePeerRPCDuration(peerName, requestDuration.Milliseconds(), isBig)
	timeShareQueuePeerE2EDuration(peerName, timeE2E, req.method, req.systemEndpoint, isBig)

	logSendErrorLevel := slog.LevelDebug
	if peerName == "local-builder" {
		logSendErrorLevel = slog.LevelWarn
	}
	if err != nil {
		logger.Log(context.Background(), logSendErrorLevel, "Error while proxying request", slog.Any("error", err))
		incShareQueuePeerRPCErrors(peerName)
	} else {
		var parsedResp jsonrpc.JSONRPCResponse
		err = json.Unmarshal(resp.Body(), &parsedResp)
		if err != nil {
			logger.Log(context.Background(), logSendErrorLevel, "Error parsing response while proxying", slog.Any("error", err))
			incShareQueuePeerRPCErrors(peerName)
		} else if parsedResp.Error != nil {
			logger.Log(context.Background(), logSendErrorLevel, "Error returned from target while proxying", slog.Any("error", parsedResp.Error))
			incShareQueuePeerRPCErrors(peerName)
		}
	}
	fasthttp.ReleaseResponse(resp)

	return nil
}

func (sq *ShareQueue) proxyRequests(peer *shareQueuePeer, worker int) {
	proxiedRequestCount := 0
	logger := sq.log.With(slog.String("peer", peer.name), slog.String("name", sq.name), slog.Int("worker", worker))
	logger.Info("Started proxying requests to peer")
	defer func() {
		logger.Info("Stopped proxying requets to peer", slog.Int("proxiedRequestCount", proxiedRequestCount))
	}()

	request := fasthttp.AcquireRequest()
	request.SetRequestURI(peer.endpoint)
	request.Header.SetMethod(http.MethodPost)
	request.Header.SetContentTypeBytes([]byte("application/json"))
	defer fasthttp.ReleaseRequest(request)

	for {
		req, more := <-peer.ch
		if !more {
			return
		}

		if req.serializedJSONRPCRequest == nil {
			logger.Debug("Skip sharing request that is not serialized properly")
			continue
		}

		err := sendShareRequest(logger, req, request, peer.client, peer.name)
		if err != nil {
			logger.Debug("Failed to proxy a request", slog.Any("error", err))
		}

		proxiedRequestCount += 1
		logger.Debug("Message proxied")
	}
}

func SerializeParsedRequestForSharing(req *ParsedRequest, signer *signature.Signer) error {
	var (
		method string
		data   any
	)
	if req.ethSendBundle != nil {
		method = EthSendBundleMethod
		data = req.ethSendBundle
	} else if req.mevSendBundle != nil {
		method = MevSendBundleMethod
		data = req.mevSendBundle
	} else if req.ethCancelBundle != nil {
		method = EthCancelBundleMethod
		data = req.ethCancelBundle
	} else if req.ethSendRawTransaction != nil {
		method = EthSendRawTransactionMethod
		data = req.ethSendRawTransaction
	} else if req.bidSubsidiseBlock != nil {
		method = BidSubsidiseBlockMethod
		data = req.bidSubsidiseBlock
	} else {
		return errUnknownRequestType
	}

	request := rpcclient.NewRequestWithID(0, method, data)

	ser, err := json.Marshal(request)
	if err != nil {
		return err
	}

	req.serializedJSONRPCRequest = ser

	if signer != nil {
		header, err := signer.Create(ser)
		if err != nil {
			return err
		}
		req.signatureHeader = header
	}

	return nil
}
