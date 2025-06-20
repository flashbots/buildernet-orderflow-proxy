package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net/http"
	"os"
	"slices"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/flashbots/go-utils/rpcclient"
	"github.com/flashbots/go-utils/signature"
	"github.com/urfave/cli/v2" // imports as package "cli"
)

var flags []cli.Flag = []cli.Flag{
	// input and output
	&cli.StringFlag{
		Name:    "local-orderflow-endpoint",
		Value:   "http://127.0.0.1",
		Usage:   "address to send orderflow to",
		EnvVars: []string{"LOCAL_ORDERPLOW_ENDPOINT"},
	},
	&cli.IntFlag{
		Name:    "local-receiver-server-port",
		Value:   8646,
		Usage:   "address to send orderflow to",
		EnvVars: []string{"LOCAL_RECEIVER_SERVER__PORT"},
	},
	&cli.IntFlag{
		Name:    "num-senders",
		Value:   50,
		Usage:   "Number of senders",
		EnvVars: []string{"NUM_SENDERS"},
	},
	&cli.IntFlag{
		Name:    "num-requests",
		Value:   50000,
		Usage:   "Number of requests",
		EnvVars: []string{"NUM_REQUESTS"},
	},
}

// test tx

func main() {
	app := &cli.App{
		Name:  "test-tx-sender",
		Usage: "send test transactions",
		Flags: flags,
		Action: func(cCtx *cli.Context) error {
			orderflowSigner, err := signature.NewRandomSigner()
			if err != nil {
				return err
			}
			slog.Info("Ordeflow signing address", "address", orderflowSigner.Address())

			localOrderflowEndpoint := cCtx.String("local-orderflow-endpoint")
			client := rpcclient.NewClientWithOpts(localOrderflowEndpoint, &rpcclient.RPCClientOpts{
				Signer: orderflowSigner,
			})
			slog.Info("Created client")

			receiverPort := cCtx.Int("local-receiver-server-port")

			senders := cCtx.Int("num-senders")
			requests := cCtx.Int("num-requests")

			return runE2ELatencyTest(client, receiverPort, senders, requests)
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}

type sharedState struct {
	sentAt     map[uint64]time.Time
	receivedAt map[uint64]time.Time
	mu         sync.Mutex
}

func (s *sharedState) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	receivedAt := time.Now()
	body, _ := io.ReadAll(r.Body)

	// serve builderhub API
	if r.URL.Path == "/api/l1-builder/v1/register_credentials/orderflow_proxy" {
		w.WriteHeader(http.StatusOK)
		return
	} else if r.URL.Path == "/api/l1-builder/v1/builders" {
		res, err := json.Marshal([]int{})
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(res)
		return
	}

	resp, err := json.Marshal(struct{}{})
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	_, _ = w.Write(resp)

	// forwarded request received
	type jsonRPCRequest struct {
		Params []hexutil.Bytes `json:"params"`
	}

	var request jsonRPCRequest
	err = json.Unmarshal(body, &request)
	if err != nil {
		return
	}
	if len(request.Params) != 1 {
		return
	}

	decoded := binary.BigEndian.Uint64(request.Params[0])

	s.mu.Lock()
	s.receivedAt[decoded] = receivedAt
	s.mu.Unlock()
}

func (s *sharedState) RunSender(client rpcclient.RPCClient, start, count int, wg *sync.WaitGroup) {
	defer wg.Done()
	for i := start; i < start+count; i += 1 {
		b := make([]byte, 8)
		//nolint:gosec
		binary.BigEndian.PutUint64(b, uint64(i))
		request := hexutil.Bytes(b)

		s.mu.Lock()
		//nolint:gosec
		s.sentAt[uint64(i)] = time.Now()
		s.mu.Unlock()
		// send eth_sendRawTransactions
		resp, err := client.Call(context.Background(), "eth_sendRawTransaction", request)
		if err != nil {
			slog.Error("RPC request failed", "error", err)
			continue
		}
		if resp.Error != nil {
			slog.Error("RPC returned error", "error", resp.Error)
			continue
		}
	}
}

func runE2ELatencyTest(client rpcclient.RPCClient, receiverPort, senders, requests int) error {
	state := &sharedState{
		sentAt:     make(map[uint64]time.Time),
		receivedAt: make(map[uint64]time.Time),
		mu:         sync.Mutex{},
	}

	//nolint:gosec
	receiverServer := &http.Server{
		Addr:    fmt.Sprintf("0.0.0.0:%d", receiverPort),
		Handler: state,
	}

	go func() {
		if err := receiverServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("Failed while listening to server", "error", err)
			os.Exit(1)
		}
	}()

	countPerSender := requests / senders

	slog.Info("Waiting for startup")
	time.Sleep(time.Second * 5)
	slog.Info("Sending started")

	start := time.Now()
	offset := 0
	var wg sync.WaitGroup
	for range senders {
		wg.Add(1)
		go state.RunSender(client, offset, countPerSender, &wg)
		offset += countPerSender
	}
	wg.Wait()
	sendingTime := time.Since(start)

	slog.Info("Waiting to finish")
	time.Sleep(time.Second * 1)

	state.mu.Lock()
	defer state.mu.Unlock()

	missedRequests := 0
	totalRequests := len(state.sentAt)
	rps := float64(totalRequests) / (float64(sendingTime.Milliseconds()) / 1000.0)

	values := make([]float64, 0, totalRequests)
	for request, sentAt := range state.sentAt {
		receivedAt, ok := state.receivedAt[request]
		if !ok {
			missedRequests += 1
			continue
		}
		diff := float64(receivedAt.Sub(sentAt).Microseconds()) / 1000.0
		values = append(values, diff)
	}
	slices.Sort(values)
	p50 := values[(len(values)*50)/100]
	p99 := values[(len(values)*99)/100]
	p100 := values[len(values)-1]

	missedPercentage := float64(missedRequests) / float64(totalRequests) * 100.0

	slog.Info("Results", "p50", p50, "p99", p99, "p100", p100, "totalReq", totalRequests, "missedRequests", missedRequests, "missedPercent", missedPercentage, "averageRPS", rps)

	return nil
}
