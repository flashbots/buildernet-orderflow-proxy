package proxy

import (
	"context"
	"log/slog"
	"time"

	"github.com/flashbots/go-utils/rpcclient"
	"github.com/flashbots/go-utils/signature"
)

var (
	ShareWorkerQueueSize = 10000
	requestTimeout       = time.Second * 10
)

const (
	bigRequestSize = 50_000
)

type ShareQueue struct {
	name         string
	log          *slog.Logger
	queue        chan *ParsedRequest
	updatePeers  chan []ConfighubBuilder
	localBuilder rpcclient.RPCClient
	signer       *signature.Signer
	// if > 0 share queue will spawn multiple senders per peer
	workersPerPeer int
}

type shareQueuePeer struct {
	ch     chan *ParsedRequest
	name   string
	client rpcclient.RPCClient
	conf   ConfighubBuilder
}

func newShareQueuePeer(name string, client rpcclient.RPCClient, conf ConfighubBuilder) shareQueuePeer {
	return shareQueuePeer{
		ch:     make(chan *ParsedRequest, ShareWorkerQueueSize),
		name:   name,
		client: client,
		conf:   conf,
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
	var (
		localBuilder *shareQueuePeer
		peers        []shareQueuePeer
	)
	if sq.localBuilder != nil {
		builderPeer := newShareQueuePeer("local-builder", sq.localBuilder, ConfighubBuilder{})
		localBuilder = &builderPeer
		for worker := range workersPerPeer {
			go sq.proxyRequests(localBuilder, worker)
		}
		defer localBuilder.Close()
	}
	for {
		select {
		case req, more := <-sq.queue:
			sq.log.Debug("Share queue received a request", slog.String("name", sq.name), slog.String("method", req.method))
			if !more {
				sq.log.Info("Share queue closing, queue channel closed")
				return
			}
			if localBuilder != nil {
				localBuilder.SendRequest(sq.log, req)
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
				client, err := RPCClientWithCertAndSigner(info.SystemAPIAddress(), []byte(info.TLSCert()), sq.signer, workersPerPeer)
				if err != nil {
					sq.log.Error("Failed to create a peer client", slog.Any("error", err))
					shareQueueInternalErrors.Inc()
					continue
				}
				sq.log.Info("Created client for peer", slog.String("peer", info.Name), slog.String("name", sq.name))
				newPeer := newShareQueuePeer(info.Name, client, info)
				peers = append(peers, newPeer)
				for worker := range workersPerPeer {
					go sq.proxyRequests(&newPeer, worker)
				}
			}
		}
	}
}

func (sq *ShareQueue) proxyRequests(peer *shareQueuePeer, worker int) {
	proxiedRequestCount := 0
	logger := sq.log.With(slog.String("peer", peer.name), slog.String("name", sq.name), slog.Int("worker", worker))
	logger.Info("Started proxying requests to peer")
	defer func() {
		logger.Info("Stopped proxying requets to peer", slog.Int("proxiedRequestCount", proxiedRequestCount))
	}()
	for {
		req, more := <-peer.ch
		if !more {
			return
		}
		var (
			method string
			data   any
		)
		isBig := req.size > bigRequestSize
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
			logger.Error("Unknown request type", slog.String("method", req.method))
			shareQueueInternalErrors.Inc()
			continue
		}
		timeShareQueuePeerQueueDuration(peer.name, time.Since(req.receivedAt), method, req.systemEndpoint, isBig)
		start := time.Now()
		ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
		resp, err := peer.client.Call(ctx, method, data)
		cancel()
		timeShareQueuePeerRPCDuration(peer.name, time.Since(start).Milliseconds(), isBig)
		timeShareQueuePeerE2EDuration(peer.name, time.Since(req.receivedAt), method, req.systemEndpoint, isBig)
		logSendErrorLevel := slog.LevelDebug
		if peer.name == "local-builder" {
			logSendErrorLevel = slog.LevelWarn
		}
		if err != nil {
			logger.Log(context.Background(), logSendErrorLevel, "Error while proxying request", slog.Any("error", err))
			incShareQueuePeerRPCErrors(peer.name)
		}
		if resp != nil && resp.Error != nil {
			logger.Log(context.Background(), logSendErrorLevel, "Error returned from target while proxying", slog.Any("error", resp.Error))
			incShareQueuePeerRPCErrors(peer.name)
		}
		proxiedRequestCount += 1
		logger.Debug("Message proxied")
	}
}
