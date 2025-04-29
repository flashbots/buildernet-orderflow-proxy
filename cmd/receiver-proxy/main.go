package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/VictoriaMetrics/metrics"
	eth "github.com/ethereum/go-ethereum/common"
	"github.com/flashbots/tdx-orderflow-proxy/common"
	"github.com/flashbots/tdx-orderflow-proxy/proxy"
	"github.com/google/uuid"
	"github.com/urfave/cli/v2" // imports as package "cli"
)

const (
	flagUserListenAddr   = "user-listen-addr"
	flagSystemListenAddr = "system-listen-addr"
	flagCertListenAddr   = "cert-listen-addr"
	flagMaxUserRPS       = "max-user-requests-per-second"

	flagCertPath    = "cert-path"
	flagCertKeyPath = "cert-key-path"
)

var flags = []cli.Flag{
	// Servers config (NEW)
	&cli.StringFlag{
		Name:    flagUserListenAddr,
		Value:   "127.0.0.1:443",
		Usage:   "server to receive orderflow from external users/public",
		EnvVars: []string{"USER_LISTEN_ADDR"},
	},
	&cli.StringFlag{
		Name:    flagSystemListenAddr,
		Value:   "127.0.0.1:5544",
		Usage:   "server to receive orderflow from BuilderNet network",
		EnvVars: []string{"SYSTEM_LISTEN_ADDR"},
	},
	&cli.StringFlag{
		Name:    flagCertListenAddr,
		Value:   "127.0.0.1:14727",
		Usage:   "address to listen on for orderflow proxy serving its SSL certificate on /cert",
		EnvVars: []string{"CERT_LISTEN_ADDR"},
	},

	// Connections to Builder, BuilderHub, RPC and block-processor
	&cli.StringFlag{
		Name:    "builder-endpoint",
		Value:   "http://127.0.0.1:8645",
		Usage:   "address to send local ordeflow to",
		EnvVars: []string{"BUILDER_ENDPOINT"},
	},
	&cli.StringFlag{
		Name:    "rpc-endpoint",
		Value:   "http://127.0.0.1:8545",
		Usage:   "address of the node RPC that supports eth_blockNumber",
		EnvVars: []string{"RPC_ENDPOINT"},
	},
	&cli.StringFlag{
		Name:    "builder-confighub-endpoint",
		Value:   "http://127.0.0.1:14892",
		Usage:   "address of the builder config hub endpoint (directly or using the cvm-proxy)",
		EnvVars: []string{"BUILDER_CONFIGHUB_ENDPOINT"},
	},
	&cli.StringFlag{
		Name:    "orderflow-archive-endpoint",
		Value:   "http://127.0.0.1:14893",
		Usage:   "address of the orderflow archive endpoint (block-processor)",
		EnvVars: []string{"ORDERFLOW_ARCHIVE_ENDPOINT"},
	},

	// Various configs
	&cli.StringFlag{
		Name:    "flashbots-orderflow-signer-address",
		Value:   "0x5015Fa72E34f75A9eC64f44a4Fcf0837919D1bB7",
		Usage:   "orderflow from Flashbots will be signed with this address",
		EnvVars: []string{"FLASHBOTS_ORDERFLOW_SIGNER_ADDRESS"},
	},
	&cli.Int64Flag{
		Name:    "max-request-body-size-bytes",
		Value:   0,
		Usage:   "Maximum size of the request body, if 0 default will be used",
		EnvVars: []string{"MAX_REQUEST_BODY_SIZE_BYTES"},
	},
	&cli.IntFlag{
		Name:    "connections-per-peer",
		Value:   10,
		Usage:   "Number of parallel connections for each peer and archival RPC",
		EnvVars: []string{"CONN_PER_PEER"},
	},
	&cli.IntFlag{
		Name:    flagMaxUserRPS,
		Value:   0,
		Usage:   "Maximum number of unique user requests per second (set 0 to disable)",
		EnvVars: []string{"MAX_USER_RPS"},
	},

	// Certificate config
	&cli.DurationFlag{
		Name:    "cert-duration",
		Value:   time.Hour * 24 * 365,
		Usage:   "generated certificate duration",
		EnvVars: []string{"CERT_DURATION"},
	},
	&cli.StringSliceFlag{
		Name:    "cert-hosts",
		Value:   cli.NewStringSlice("127.0.0.1", "localhost"),
		Usage:   "generated certificate hosts",
		EnvVars: []string{"CERT_HOSTS"},
	},
	&cli.StringFlag{
		Name:    flagCertPath,
		Usage:   "path where to store the generated certificate",
		EnvVars: []string{"CERT_PATH"},
	},
	&cli.StringFlag{
		Name:    flagCertKeyPath,
		Usage:   "path where to store the generated certificate key",
		EnvVars: []string{"CERT_KEY_PATH"},
	},

	// Logging, metrics and debug
	&cli.StringFlag{
		Name:    "metrics-addr",
		Value:   "127.0.0.1:8090",
		Usage:   "address to listen on for Prometheus metrics (metrics are served on $metrics-addr/metrics)",
		EnvVars: []string{"METRICS_ADDR"},
	},
	&cli.BoolFlag{
		Name:    "log-json",
		Value:   false,
		Usage:   "log in JSON format",
		EnvVars: []string{"LOG_JSON"},
	},
	&cli.BoolFlag{
		Name:    "log-debug",
		Value:   false,
		Usage:   "log debug messages",
		EnvVars: []string{"LOG_DEBUG"},
	},
	&cli.BoolFlag{
		Name:    "log-uid",
		Value:   false,
		Usage:   "generate a uuid and add to all log messages",
		EnvVars: []string{"LOG_UID"},
	},
	&cli.StringFlag{
		Name:    "log-service",
		Value:   "tdx-orderflow-proxy-receiver",
		Usage:   "add 'service' tag to logs",
		EnvVars: []string{"LOG_SERVICE"},
	},
	&cli.BoolFlag{
		Name:    "pprof",
		Value:   false,
		Usage:   "enable pprof debug endpoint (pprof is served on $metrics-addr/debug/pprof/*)",
		EnvVars: []string{"PPROF"},
	},
}

func main() {
	app := &cli.App{
		Name:    "receiver-proxy",
		Usage:   "Serve API and metrics",
		Flags:   flags,
		Version: common.Version,
		Action:  runMain,
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}

func runMain(cCtx *cli.Context) error {
	logJSON := cCtx.Bool("log-json")
	logDebug := cCtx.Bool("log-debug")
	logUID := cCtx.Bool("log-uid")
	logService := cCtx.String("log-service")

	log := common.SetupLogger(&common.LoggingOpts{
		Debug:   logDebug,
		JSON:    logJSON,
		Service: logService,
		Version: common.Version,
	})

	if logUID {
		id := uuid.Must(uuid.NewRandom())
		log = log.With("uid", id.String())
	}

	exit := make(chan os.Signal, 1)
	signal.Notify(exit, os.Interrupt, syscall.SIGTERM)

	// metrics server
	go func() {
		metricsAddr := cCtx.String("metrics-addr")
		usePprof := cCtx.Bool("pprof")
		metricsMux := http.NewServeMux()
		metricsMux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
			metrics.WritePrometheus(w, true)
		})
		if usePprof {
			metricsMux.Handle("/debug/pprof/", http.HandlerFunc(pprof.Index))
			metricsMux.Handle("/debug/pprof/cmdline", http.HandlerFunc(pprof.Cmdline))
			metricsMux.Handle("/debug/pprof/profile", http.HandlerFunc(pprof.Profile))
			metricsMux.Handle("/debug/pprof/symbol", http.HandlerFunc(pprof.Symbol))
			metricsMux.Handle("/debug/pprof/trace", http.HandlerFunc(pprof.Trace))
		}

		metricsServer := &http.Server{
			Addr:              metricsAddr,
			ReadHeaderTimeout: 5 * time.Second,
			Handler:           metricsMux,
		}

		err := metricsServer.ListenAndServe()
		if err != nil {
			log.Error("Failed to start metrics server", "err", err)
		}
	}()

	builderEndpoint := cCtx.String("builder-endpoint")
	rpcEndpoint := cCtx.String("rpc-endpoint")

	builderConfigHubEndpoint := cCtx.String("builder-confighub-endpoint")
	archiveEndpoint := cCtx.String("orderflow-archive-endpoint")
	flashbotsSignerStr := cCtx.String("flashbots-orderflow-signer-address")
	flashbotsSignerAddress := eth.HexToAddress(flashbotsSignerStr)
	maxRequestBodySizeBytes := cCtx.Int64("max-request-body-size-bytes")
	connectionsPerPeer := cCtx.Int("connections-per-peer")

	maxUserRPS := cCtx.Int(flagMaxUserRPS)

	certDuration := cCtx.Duration("cert-duration")
	certHosts := cCtx.StringSlice("cert-hosts")
	certPath := cCtx.String(flagCertPath)
	certKeyPath := cCtx.String(flagCertKeyPath)
	if certPath == "" || certKeyPath == "" {
		return errors.New("cert-path and cert-key-path must be set")
	}

	proxyConfig := &proxy.ReceiverProxyConfig{
		ReceiverProxyConstantConfig: proxy.ReceiverProxyConstantConfig{Log: log, FlashbotsSignerAddress: flashbotsSignerAddress},
		CertValidDuration:           certDuration,
		CertHosts:                   certHosts,
		CertPath:                    certPath,
		CertKeyPath:                 certKeyPath,
		BuilderConfigHubEndpoint:    builderConfigHubEndpoint,
		ArchiveEndpoint:             archiveEndpoint,
		ArchiveConnections:          connectionsPerPeer,
		LocalBuilderEndpoint:        builderEndpoint,
		EthRPC:                      rpcEndpoint,
		MaxRequestBodySizeBytes:     maxRequestBodySizeBytes,
		ConnectionsPerPeer:          connectionsPerPeer,
		MaxUserRPS:                  maxUserRPS,
	}

	instance, err := proxy.NewReceiverProxy(*proxyConfig)
	if err != nil {
		log.Error("Failed to create proxy server", "err", err)
		return err
	}

	registerContext, registerCancel := context.WithCancel(context.Background())
	go func() {
		select {
		case <-exit:
			registerCancel()
		case <-registerContext.Done():
		}
	}()

	err = instance.RegisterSecrets(registerContext)
	registerCancel()
	if err != nil {
		log.Error("Failed to generate and publish secrets", "err", err)
		return err
	}

	userListenAddr := cCtx.String(flagUserListenAddr)
	systemListenAddr := cCtx.String(flagSystemListenAddr)
	certListenAddr := cCtx.String(flagCertListenAddr)

	servers, err := proxy.StartReceiverServers(instance, userListenAddr, systemListenAddr, certListenAddr)
	if err != nil {
		log.Error("Failed to start proxy server", "err", err)
		return err
	}

	log.Info("Started receiver proxy", "userListenAddress", userListenAddr, "systemListenAddress", systemListenAddr, "certListenAddress", certListenAddr)

	<-exit
	servers.Stop()
	return nil
}
