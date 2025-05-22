package proxy

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/flashbots/go-utils/cli"
	"github.com/flashbots/go-utils/rpcclient"
	"github.com/flashbots/go-utils/signature"
	"golang.org/x/net/http2"
)

var (
	DefaultOrderflowProxyPublicPort = "5544"
	DefaultHTTPCLientWriteBuffer    = cli.GetEnvInt("HTTP_CLIENT_WRITE_BUFFER", 64<<10) // 64 KiB
)

const DefaultLocalhostMaxIdleConn = 1000

var errCertificate = errors.New("failed to add certificate to pool")

func createTransportForSelfSignedCert(certPEM []byte, maxOpenConnections int) (*http.Transport, error) {
	certPool := x509.NewCertPool()
	if ok := certPool.AppendCertsFromPEM(certPEM); !ok {
		return nil, errCertificate
	}
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{
			RootCAs:    certPool,
			MinVersion: tls.VersionTLS12,
		},
		MaxConnsPerHost: maxOpenConnections * 2,
	}

	return tr, nil
}

func HTTPClientWithMaxConnections(maxOpenConnections int) *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:        maxOpenConnections,
			MaxIdleConnsPerHost: maxOpenConnections,
		},
	}
}

func HTTPClientLocalhost(maxOpenConnections int) *http.Client {
	localTransport := &http.Transport{
		MaxIdleConnsPerHost: maxOpenConnections,
		MaxIdleConns:        maxOpenConnections,
		DisableCompression:  true,
		IdleConnTimeout:     time.Minute * 2,
		// ---- kill delayed-ACK/Nagle ----
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			c, err := (&net.Dialer{
				LocalAddr: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)},
				KeepAlive: 30 * time.Second,
				Timeout:   1 * time.Second,
			}).DialContext(ctx, network, addr)
			if err == nil {
				_ = c.(*net.TCPConn).SetNoDelay(true) // <-- ACK immediately
			}
			return c, err
		},
		Proxy: nil,
	}
	_ = http2.ConfigureTransport(localTransport)
	localCl := http.Client{
		Transport: localTransport,
		Timeout:   10 * time.Second,
	}
	return &localCl
}

//nolint:ireturn
func RPCClientWithCertAndSigner(endpoint string, certPEM []byte, signer *signature.Signer, maxOpenConnections int) (rpcclient.RPCClient, error) {
	transport, err := createTransportForSelfSignedCert(certPEM, maxOpenConnections)
	if err != nil {
		return nil, err
	}
	transport.MaxIdleConns = maxOpenConnections * 2
	transport.MaxIdleConnsPerHost = maxOpenConnections * 2
	transport.WriteBufferSize = DefaultHTTPCLientWriteBuffer

	client := rpcclient.NewClientWithOpts(endpoint, &rpcclient.RPCClientOpts{
		HTTPClient: &http.Client{
			Transport: transport,
		},
		Signer: signer,
	})
	return client, nil
}

func OrderflowProxyURLFromIP(ip string) string {
	if strings.Contains(ip, ":") {
		return "https://" + ip
	} else {
		return "https://" + net.JoinHostPort(ip, DefaultOrderflowProxyPublicPort)
	}
}

type BlockNumberSource struct {
	client         rpcclient.RPCClient
	cacheMu        sync.RWMutex
	cacheTimestamp time.Time
	cachedNumber   uint64
}

func NewBlockNumberSource(endpoint string) *BlockNumberSource {
	client := rpcclient.NewClient(endpoint)
	return &BlockNumberSource{
		client: client,
	}
}

func (bs *BlockNumberSource) UpdateCachedBlockNumber() error {
	var numberHex hexutil.Uint64
	err := bs.client.CallFor(context.Background(), &numberHex, "eth_blockNumber")
	if err != nil {
		return err
	}
	bs.cacheMu.Lock()
	bs.cacheTimestamp = time.Now()
	bs.cachedNumber = uint64(numberHex)
	bs.cacheMu.Unlock()
	return nil
}

func (bs *BlockNumberSource) BlockNumber() (uint64, error) {
	bs.cacheMu.RLock()
	if time.Since(bs.cacheTimestamp) > time.Second*3 {
		bs.cacheMu.RUnlock()
		err := bs.UpdateCachedBlockNumber()
		if err != nil {
			return 0, err
		}
		bs.cacheMu.RLock()
	}
	res := bs.cachedNumber
	bs.cacheMu.RUnlock()
	return res, nil
}
