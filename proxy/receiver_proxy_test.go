package proxy

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/flashbots/go-utils/rpctypes"
	"github.com/flashbots/go-utils/signature"
	"github.com/stretchr/testify/require"
)

type RequestData struct {
	request *http.Request
	body    string
}

var (
	builderHub *httptest.Server

	builderHubPeers []ConfighubBuilder

	archiveServer         *httptest.Server
	archiveServerRequests chan *RequestData

	proxies []*OrderflowProxyTestSetup

	flashbotsSigner *signature.Signer
)

func ServeHTTPRequestToChan(channel chan *RequestData) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		defer r.Body.Close()

		channel <- &RequestData{body: string(body), request: r}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("{}"))
	}))
}

type OrderflowProxyTestSetup struct {
	proxy        *ReceiverProxy
	publicServer *http.Server
	localServer  *http.Server
	certServer   *httptest.Server

	ip                   string
	publicServerEndpoint string
	localServerEndpoint  string

	localBuilderServer   *httptest.Server
	localBuilderRequests chan *RequestData
}

func (setup *OrderflowProxyTestSetup) Close() {
	_ = setup.publicServer.Close()
	_ = setup.localServer.Close()
	setup.certServer.Close()
	setup.localBuilderServer.Close()
	setup.proxy.Stop()
}

func StartTestOrderflowProxy(name, certPath, certKeyPath string) (*OrderflowProxyTestSetup, error) {
	localBuilderRequests := make(chan *RequestData, 1)
	localBuilderServer := ServeHTTPRequestToChan(localBuilderRequests)

	proxy := createProxy(localBuilderServer.URL, name, certPath, certKeyPath)
	publicProxyServer := &http.Server{ //nolint:gosec
		Handler:   proxy.SystemHandler,
		TLSConfig: proxy.TLSConfig(),
	}
	publicListener, err := net.Listen("tcp", ":0") //nolint:gosec
	if err != nil {
		return nil, err
	}
	go publicProxyServer.ServeTLS(publicListener, "", "")                                                  //nolint: errcheck
	publicServerEndpoint := fmt.Sprintf("https://localhost:%d", publicListener.Addr().(*net.TCPAddr).Port) //nolint:forcetypeassert
	ip := fmt.Sprintf("127.0.0.1:%d", publicListener.Addr().(*net.TCPAddr).Port)                           //nolint:forcetypeassert

	localProxyServer := &http.Server{ //nolint:gosec
		Handler:   proxy.UserHandler,
		TLSConfig: proxy.TLSConfig(),
	}
	localListener, err := net.Listen("tcp", ":0") //nolint:gosec
	if err != nil {
		return nil, err
	}
	go localProxyServer.ServeTLS(localListener, "", "")                                                  //nolint:errcheck
	localServerEndpoint := fmt.Sprintf("https://localhost:%d", localListener.Addr().(*net.TCPAddr).Port) //nolint:forcetypeassert

	certProxyServer := httptest.NewServer(proxy.CertHandler)

	return &OrderflowProxyTestSetup{
		proxy:                proxy,
		publicServer:         publicProxyServer,
		localServer:          localProxyServer,
		certServer:           certProxyServer,
		publicServerEndpoint: publicServerEndpoint,
		localServerEndpoint:  localServerEndpoint,
		localBuilderServer:   localBuilderServer,
		localBuilderRequests: localBuilderRequests,
		ip:                   ip,
	}, nil
}

func check(err error) {
	if err != nil {
		panic(err)
	}
}

func TestMain(m *testing.M) {
	signer, err := signature.NewRandomSigner()
	if err != nil {
		panic(err)
	}
	flashbotsSigner = signer

	archiveServerRequests = make(chan *RequestData)
	builderHub = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		defer r.Body.Close()

		if r.URL.Path == "/api/l1-builder/v1/register_credentials/orderflow_proxy" {
			var req ConfighubOrderflowProxyCredentials
			err := json.Unmarshal(body, &req)
			if err != nil {
				w.WriteHeader(http.StatusBadRequest)
				_, _ = w.Write([]byte(err.Error()))
			}
			var (
				ip   string
				name string
			)
			for _, proxy := range proxies {
				if proxy.proxy.OrderflowSigner.Address() == req.EcdsaPubkeyAddress {
					ip = proxy.ip
					name = proxy.proxy.Name
					break
				}
			}
			builderHubPeers = append(builderHubPeers, ConfighubBuilder{
				Name:           name,
				IP:             ip,
				OrderflowProxy: req,
			})
		} else if r.URL.Path == "/api/l1-builder/v1/builders" {
			res, err := json.Marshal(builderHubPeers)
			if err != nil {
				panic(err)
			}
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(res)
		} else {
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer builderHub.Close()

	archiveServer = ServeHTTPRequestToChan(archiveServerRequests)
	defer archiveServer.Close()

	tempDirs := make([]string, 0)
	for i := range 3 {
		tempDir, err := os.MkdirTemp("", "orderflow-proxy-test")
		check(err)
		tempDirs = append(tempDirs, tempDir)
		certPath := path.Join(tempDir, "cert")
		keyPath := path.Join(tempDir, "key")

		proxy, err := StartTestOrderflowProxy(fmt.Sprintf("proxy:%d", i), certPath, keyPath)
		proxies = append(proxies, proxy)
		check(err)
	}

	defer func() {
		for _, prx := range proxies {
			prx.Close()
		}

		for _, dir := range tempDirs {
			_ = os.RemoveAll(dir)
		}
	}()

	os.Exit(m.Run())
}

func createProxy(localBuilder, name, certPath, certKeyPath string) *ReceiverProxy {
	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))
	proxy, err := NewReceiverProxy(ReceiverProxyConfig{
		ReceiverProxyConstantConfig: ReceiverProxyConstantConfig{
			Log:                    log,
			Name:                   name,
			FlashbotsSignerAddress: flashbotsSigner.Address(),
		},
		CertValidDuration: time.Hour * 24,
		CertHosts:         []string{"localhost", "127.0.0.1"},
		CertPath:          certPath,
		CertKeyPath:       certKeyPath,

		BuilderConfigHubEndpoint: builderHub.URL,
		ArchiveEndpoint:          archiveServer.URL,
		LocalBuilderEndpoint:     localBuilder,
		EthRPC:                   "eth-rpc-not-set",
		MaxUserRPS:               10,
	})
	if err != nil {
		panic(err)
	}
	return proxy
}

func TestPublishSecrets(t *testing.T) {
	err := proxies[0].proxy.RegisterSecrets(context.Background())
	require.NoError(t, err)
}

func TestServeCert(t *testing.T) {
	resp, err := http.Get(proxies[0].certServer.URL)
	require.NoError(t, err)
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	require.Equal(t, string(proxies[0].proxy.PublicCertPEM), string(body))
}

func expectRequest(t *testing.T, ch chan *RequestData) *RequestData {
	t.Helper()
	select {
	case req := <-ch:
		return req
	case <-time.After(time.Millisecond * 100):
		t.Fatal("Timeout while waiting for request")
		return nil
	}
}

func expectNoRequest(t *testing.T, ch chan *RequestData) {
	t.Helper()
	select {
	case req := <-ch:
		t.Fatal("Unexpected request", req)
	case <-time.After(time.Millisecond * 100):
	}
}

func proxiesUpdatePeers(t *testing.T) {
	t.Helper()
	for _, instance := range proxies {
		err := instance.proxy.RequestNewPeers()
		require.NoError(t, err)
	}
	time.Sleep(time.Millisecond * 50)
}

func proxiesFlushQueue() {
	for _, instance := range proxies {
		instance.proxy.FlushArchiveQueue()
	}
}

func TestProxyBundleRequestWithPeerUpdate(t *testing.T) {
	defer func() {
		proxiesFlushQueue()
		for {
			select {
			case <-time.After(time.Millisecond * 100):
				expectNoRequest(t, archiveServerRequests)
				return
			case <-archiveServerRequests:
			}
		}
	}()
	apiNow = func() time.Time {
		return time.Unix(1730000000, 0)
	}
	defer func() {
		apiNow = time.Now
	}()

	signer, err := signature.NewSignerFromHexPrivateKey("0xd63b3c447fdea415a05e4c0b859474d14105a88178efdf350bc9f7b05be3cc58")
	require.NoError(t, err)
	client, err := RPCClientWithCertAndSigner(proxies[0].localServerEndpoint, proxies[0].proxy.PublicCertPEM, signer, 1)
	require.NoError(t, err)

	expectedRequest := `{"method":"eth_sendBundle","params":[{"txs":null,"blockNumber":"0x3e8","version":"v2","signingAddress":"0x9349365494be4f6205e5d44bdc7ec7dcd134becf"}],"id":0,"jsonrpc":"2.0"}`

	// we start with no peers
	builderHubPeers = nil
	err = proxies[0].proxy.RegisterSecrets(context.Background())
	require.NoError(t, err)
	proxiesUpdatePeers(t)

	blockNumber := hexutil.Uint64(1000)
	resp, err := client.Call(context.Background(), EthSendBundleMethod, &rpctypes.EthSendBundleArgs{
		BlockNumber: &blockNumber,
	})
	require.NoError(t, err)
	require.Nil(t, resp.Error)

	builderRequest := expectRequest(t, proxies[0].localBuilderRequests)
	require.Equal(t, expectedRequest, builderRequest.body)
	expectNoRequest(t, proxies[1].localBuilderRequests)
	expectNoRequest(t, proxies[2].localBuilderRequests)

	slog.Info("Adding first peer")

	// add one more peer
	err = proxies[1].proxy.RegisterSecrets(context.Background())
	require.NoError(t, err)
	proxiesUpdatePeers(t)

	blockNumber = hexutil.Uint64(1001)
	_, err = client.Call(context.Background(), EthSendBundleMethod, &rpctypes.EthSendBundleArgs{
		BlockNumber: &blockNumber,
	})
	require.NoError(t, err)

	expectedRequest = `{"method":"eth_sendBundle","params":[{"txs":null,"blockNumber":"0x3e9","version":"v2","signingAddress":"0x9349365494be4f6205e5d44bdc7ec7dcd134becf"}],"id":0,"jsonrpc":"2.0"}`
	builderRequest = expectRequest(t, proxies[0].localBuilderRequests)
	require.Equal(t, expectedRequest, builderRequest.body)
	builderRequest = expectRequest(t, proxies[1].localBuilderRequests)
	require.Equal(t, expectedRequest, builderRequest.body)
	expectNoRequest(t, proxies[2].localBuilderRequests)

	// add another peer
	slog.Info("Adding second peer")

	err = proxies[2].proxy.RegisterSecrets(context.Background())
	require.NoError(t, err)
	proxiesUpdatePeers(t)

	blockNumber = hexutil.Uint64(1002)
	replacementUUID := "550e8400-e29b-41d4-a716-446655440000"
	_, err = client.Call(context.Background(), EthSendBundleMethod, &rpctypes.EthSendBundleArgs{
		BlockNumber:     &blockNumber,
		ReplacementUUID: &replacementUUID,
	})
	require.NoError(t, err)

	expectedRequest = `{"method":"eth_sendBundle","params":[{"txs":null,"blockNumber":"0x3ea","replacementUuid":"550e8400-e29b-41d4-a716-446655440000","version":"v2","replacementNonce":1730000000000000,"signingAddress":"0x9349365494be4f6205e5d44bdc7ec7dcd134becf"}],"id":0,"jsonrpc":"2.0"}`
	builderRequest = expectRequest(t, proxies[0].localBuilderRequests)
	require.Equal(t, expectedRequest, builderRequest.body)
	builderRequest = expectRequest(t, proxies[1].localBuilderRequests)
	require.Equal(t, expectedRequest, builderRequest.body)
	builderRequest = expectRequest(t, proxies[2].localBuilderRequests)
	require.Equal(t, expectedRequest, builderRequest.body)

	replacementNonce := uint64(8976)
	_, err = client.Call(context.Background(), EthSendBundleMethod, &rpctypes.EthSendBundleArgs{
		BlockNumber:      &blockNumber,
		ReplacementUUID:  &replacementUUID,
		ReplacementNonce: &replacementNonce,
	})
	require.NoError(t, err)

	expectedRequest = `{"method":"eth_sendBundle","params":[{"txs":null,"blockNumber":"0x3ea","replacementUuid":"550e8400-e29b-41d4-a716-446655440000","version":"v2","replacementNonce":8976,"signingAddress":"0x9349365494be4f6205e5d44bdc7ec7dcd134becf"}],"id":0,"jsonrpc":"2.0"}`
	builderRequest = expectRequest(t, proxies[0].localBuilderRequests)
	require.Equal(t, expectedRequest, builderRequest.body)
	builderRequest = expectRequest(t, proxies[1].localBuilderRequests)
	require.Equal(t, expectedRequest, builderRequest.body)
	builderRequest = expectRequest(t, proxies[2].localBuilderRequests)
	require.Equal(t, expectedRequest, builderRequest.body)
}

func TestProxySendToArchive(t *testing.T) {
	signer, err := signature.NewSignerFromHexPrivateKey("0xd63b3c447fdea415a05e4c0b859474d14105a88178efdf350bc9f7b05be3cc58")
	require.NoError(t, err)
	client, err := RPCClientWithCertAndSigner(proxies[0].localServerEndpoint, proxies[0].proxy.PublicCertPEM, signer, 1)
	require.NoError(t, err)

	// we start with no peers
	builderHubPeers = nil
	err = proxies[0].proxy.RegisterSecrets(context.Background())
	require.NoError(t, err)
	proxiesUpdatePeers(t)

	apiNow = func() time.Time {
		return time.Unix(1730000000, 0)
	}
	defer func() {
		apiNow = time.Now
	}()

	blockNumber := hexutil.Uint64(123)
	resp, err := client.Call(context.Background(), EthSendBundleMethod, &rpctypes.EthSendBundleArgs{
		BlockNumber: &blockNumber,
	})
	require.NoError(t, err)
	require.Nil(t, resp.Error)
	_ = expectRequest(t, proxies[0].localBuilderRequests)

	blockNumber = hexutil.Uint64(456)
	resp, err = client.Call(context.Background(), EthSendBundleMethod, &rpctypes.EthSendBundleArgs{
		BlockNumber: &blockNumber,
	})
	require.NoError(t, err)
	require.Nil(t, resp.Error)

	_ = expectRequest(t, proxies[0].localBuilderRequests)

	proxiesFlushQueue()
	archiveRequest := expectRequest(t, archiveServerRequests)
	expectedArchiveRequest := `{"method":"flashbots_newOrderEvents","params":[{"orderEvents":[{"eth_sendBundle":{"params":{"txs":null,"blockNumber":"0x7b","version":"v2","signingAddress":"0x9349365494be4f6205e5d44bdc7ec7dcd134becf"},"metadata":{"receivedAt":1730000000000}}},{"eth_sendBundle":{"params":{"txs":null,"blockNumber":"0x1c8","version":"v2","signingAddress":"0x9349365494be4f6205e5d44bdc7ec7dcd134becf"},"metadata":{"receivedAt":1730000000000}}}]}],"id":0,"jsonrpc":"2.0"}`
	require.Equal(t, expectedArchiveRequest, archiveRequest.body)
}

func createTestTx(i int) *hexutil.Bytes {
	privateKey, err := crypto.HexToECDSA("c7589782d55a642c8ced7794ddcb24b62d4ebefbb81001034cb46545ff80e39e")
	if err != nil {
		panic(err)
	}

	chainID := big.NewInt(1)
	txData := &types.DynamicFeeTx{
		ChainID:    chainID,
		Nonce:      uint64(i), //nolint:gosec
		GasTipCap:  big.NewInt(1),
		GasFeeCap:  big.NewInt(1),
		Gas:        21000,
		To:         &common.Address{},
		Value:      big.NewInt(0),
		Data:       nil,
		AccessList: nil,
	}
	tx, err := types.SignNewTx(privateKey, types.LatestSignerForChainID(big.NewInt(1)), txData)
	if err != nil {
		panic(err)
	}
	binary, err := tx.MarshalBinary()
	if err != nil {
		panic(err)
	}
	hexBytes := hexutil.Bytes(binary)
	return &hexBytes
}

func TestProxyShareBundleReplacementUUIDAndCancellation(t *testing.T) {
	defer func() {
		proxiesFlushQueue()
		for {
			select {
			case <-time.After(time.Millisecond * 100):
				expectNoRequest(t, archiveServerRequests)
				return
			case <-archiveServerRequests:
			}
		}
	}()

	signer, err := signature.NewSignerFromHexPrivateKey("0xd63b3c447fdea415a05e4c0b859474d14105a88178efdf350bc9f7b05be3cc58")
	require.NoError(t, err)
	client, err := RPCClientWithCertAndSigner(proxies[0].localServerEndpoint, proxies[0].proxy.PublicCertPEM, signer, 1)
	require.NoError(t, err)

	// we start with no peers
	builderHubPeers = nil
	err = proxies[0].proxy.RegisterSecrets(context.Background())
	require.NoError(t, err)
	proxiesUpdatePeers(t)

	// first call
	resp, err := client.Call(context.Background(), MevSendBundleMethod, &rpctypes.MevSendBundleArgs{
		Version:         "v0.1",
		ReplacementUUID: "550e8400-e29b-41d4-a716-446655440000",
		Inclusion: rpctypes.MevBundleInclusion{
			BlockNumber: 10,
		},
		Body: []rpctypes.MevBundleBody{
			{
				Tx: createTestTx(0),
			},
		},
	})
	require.NoError(t, err)
	require.Nil(t, resp.Error)

	expectedRequest := `{"method":"mev_sendBundle","params":[{"version":"v0.1","replacementUuid":"550e8400-e29b-41d4-a716-446655440000","inclusion":{"block":"0xa","maxBlock":"0x0"},"body":[{"tx":"0x02f862018001018252089400000000000000000000000000000000000000008080c001a05900a5ea3e4e07980b0d6276e2764b734be64d64c20f3eb87746c7ed1d72aa26a073f3a0877bd80098bd720afd1ba2c6d2d9c76d87cbc23f098d7be74902d9bfd4"}],"validity":{},"metadata":{"signer":"0x9349365494be4f6205e5d44bdc7ec7dcd134becf","replacementNonce":0}}],"id":0,"jsonrpc":"2.0"}`

	builderRequest := expectRequest(t, proxies[0].localBuilderRequests)
	require.Equal(t, expectedRequest, builderRequest.body)

	// second call
	resp, err = client.Call(context.Background(), MevSendBundleMethod, &rpctypes.MevSendBundleArgs{
		Version:         "v0.1",
		ReplacementUUID: "550e8400-e29b-41d4-a716-446655440000",
		Inclusion: rpctypes.MevBundleInclusion{
			BlockNumber: 10,
		},
		Body: []rpctypes.MevBundleBody{
			{
				Tx: createTestTx(1),
			},
		},
	})
	require.NoError(t, err)
	require.Nil(t, resp.Error)

	expectedRequest = `{"method":"mev_sendBundle","params":[{"version":"v0.1","replacementUuid":"550e8400-e29b-41d4-a716-446655440000","inclusion":{"block":"0xa","maxBlock":"0x0"},"body":[{"tx":"0x02f862010101018252089400000000000000000000000000000000000000008080c001a03b5edc6a7fe16f7c7bf25c56281b86107e742a922f900ac94293225b380fd5bea00f2ea6392842711064ca5c0fe12d812a60e8936ec8dc13ca95ecde8b262fd1fe"}],"validity":{},"metadata":{"signer":"0x9349365494be4f6205e5d44bdc7ec7dcd134becf","replacementNonce":1}}],"id":0,"jsonrpc":"2.0"}`

	builderRequest = expectRequest(t, proxies[0].localBuilderRequests)
	require.Equal(t, expectedRequest, builderRequest.body)

	// cancell
	resp, err = client.Call(context.Background(), MevSendBundleMethod, &rpctypes.MevSendBundleArgs{
		Version:         "v0.1",
		ReplacementUUID: "550e8400-e29b-41d4-a716-446655440000",
	})
	require.NoError(t, err)
	require.Nil(t, resp.Error)

	expectedRequest = `{"method":"mev_sendBundle","params":[{"version":"v0.1","replacementUuid":"550e8400-e29b-41d4-a716-446655440000","inclusion":{"block":"0x0","maxBlock":"0x0"},"body":null,"validity":{},"metadata":{"signer":"0x9349365494be4f6205e5d44bdc7ec7dcd134becf","replacementNonce":2,"cancelled":true}}],"id":0,"jsonrpc":"2.0"}`

	builderRequest = expectRequest(t, proxies[0].localBuilderRequests)
	require.Equal(t, expectedRequest, builderRequest.body)
}

func TestProxyBidSubsidiseBlockCall(t *testing.T) {
	defer func() {
		proxiesFlushQueue()
		for {
			select {
			case <-time.After(time.Millisecond * 100):
				expectNoRequest(t, archiveServerRequests)
				return
			case <-archiveServerRequests:
			}
		}
	}()

	client, err := RPCClientWithCertAndSigner(proxies[0].publicServerEndpoint, proxies[0].proxy.PublicCertPEM, flashbotsSigner, 1)
	require.NoError(t, err)

	expectedRequest := `{"method":"bid_subsidiseBlock","params":[1000],"id":0,"jsonrpc":"2.0"}`

	// we add all proxies to the list of peers
	builderHubPeers = nil
	for _, proxy := range proxies {
		err = proxy.proxy.RegisterSecrets(context.Background())
		require.NoError(t, err)
	}
	proxiesUpdatePeers(t)

	args := rpctypes.BidSubsisideBlockArgs(1000)
	resp, err := client.Call(context.Background(), BidSubsidiseBlockMethod, &args)
	require.NoError(t, err)
	require.Nil(t, resp.Error)

	builderRequest := expectRequest(t, proxies[0].localBuilderRequests)
	require.Equal(t, expectedRequest, builderRequest.body)
	expectNoRequest(t, proxies[1].localBuilderRequests)
	expectNoRequest(t, proxies[2].localBuilderRequests)
}

func TestBuilderNetRootCall(t *testing.T) {
	tempDir := t.TempDir()
	certPath := path.Join(tempDir, "cert")
	keyPath := path.Join(tempDir, "key")
	proxy, err := StartTestOrderflowProxy("1", certPath, keyPath)
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodGet, "/", nil)

	rr := httptest.NewRecorder()
	proxy.localServer.Handler.ServeHTTP(rr, req)
	respBody, err := io.ReadAll(rr.Body)
	require.NoError(t, err)
	require.Contains(t, string(respBody), "-----BEGIN CERTIFICATE-----")
}

func TestValidateLocalBundles(t *testing.T) {
	signer, err := signature.NewSignerFromHexPrivateKey("0xd63b3c447fdea415a05e4c0b859474d14105a88178efdf350bc9f7b05be3cc58")
	require.NoError(t, err)
	client, err := RPCClientWithCertAndSigner(proxies[0].localServerEndpoint, proxies[0].proxy.PublicCertPEM, signer, 1)
	require.NoError(t, err)

	pubClient, err := RPCClientWithCertAndSigner(proxies[0].publicServerEndpoint, proxies[0].proxy.PublicCertPEM, flashbotsSigner, 1)
	require.NoError(t, err)

	// we start with no peers
	builderHubPeers = nil
	err = proxies[0].proxy.RegisterSecrets(context.Background())
	require.NoError(t, err)
	proxiesUpdatePeers(t)

	apiNow = func() time.Time {
		return time.Unix(1730000000, 0)
	}
	defer func() {
		apiNow = time.Now
	}()

	blockNumber := hexutil.Uint64(123)
	invalidVersion := "v14"
	resp, err := client.Call(context.Background(), EthSendBundleMethod, &rpctypes.EthSendBundleArgs{
		BlockNumber: &blockNumber,
		Version:     &invalidVersion,
	})
	require.NoError(t, err)
	require.Equal(t, "invalid version", resp.Error.Message)
	expectNoRequest(t, proxies[0].localBuilderRequests)

	blockNumber = hexutil.Uint64(124)
	uid := "52840671-89dc-484f-80f6-401753513ba9"
	resp, err = client.Call(context.Background(), EthSendBundleMethod, &rpctypes.EthSendBundleArgs{
		BlockNumber: &blockNumber,
		UUID:        &uid,
	})
	require.NoError(t, err)
	require.Nil(t, resp.Error)
	rd := expectRequest(t, proxies[0].localBuilderRequests)
	require.JSONEq(
		t,
		`{"method":"eth_sendBundle","params":[{"txs":null,"blockNumber":"0x7c","replacementUuid":"52840671-89dc-484f-80f6-401753513ba9","version":"v2","replacementNonce":1730000000000000,"signingAddress":"0x9349365494be4f6205e5d44bdc7ec7dcd134becf"}],"id":0,"jsonrpc":"2.0"}`,
		rd.body,
	)

	blockNumber = hexutil.Uint64(123)
	invalidVersion = "v14"
	resp, err = pubClient.Call(context.Background(), EthSendBundleMethod, &rpctypes.EthSendBundleArgs{
		BlockNumber: &blockNumber,
		Version:     &invalidVersion,
	})
	require.NoError(t, err)
	require.Equal(t, "invalid version", resp.Error.Message)
	expectNoRequest(t, proxies[0].localBuilderRequests)

	blockNumber = hexutil.Uint64(123)
	resp, err = pubClient.Call(context.Background(), EthSendBundleMethod, &rpctypes.EthSendBundleArgs{
		BlockNumber: &blockNumber,
	})
	require.NoError(t, err)
	require.Equal(t, "version field should be set", resp.Error.Message)
	expectNoRequest(t, proxies[0].localBuilderRequests)

	blockNumber = hexutil.Uint64(125)
	uid = "4749af2a-1b99-45f2-a9bf-827cc0840964"
	version := rpctypes.BundleVersionV1
	resp, err = pubClient.Call(context.Background(), EthSendBundleMethod, &rpctypes.EthSendBundleArgs{
		BlockNumber: &blockNumber,
		Version:     &version,
		UUID:        &uid,
	})
	require.NoError(t, err)
	require.Nil(t, resp.Error)
	rd = expectRequest(t, proxies[0].localBuilderRequests)
	require.JSONEq(
		t,
		`{"method":"eth_sendBundle","params":[{"txs":null,"blockNumber":"0x7d","replacementUuid":"4749af2a-1b99-45f2-a9bf-827cc0840964","version":"v1"}],"id":0,"jsonrpc":"2.0"}`,
		rd.body,
	)

	proxiesFlushQueue()
}
