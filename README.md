# orderflow-proxy

[![Goreport status](https://goreportcard.com/badge/github.com/flashbots/tdx-orderflow-proxy)](https://goreportcard.com/report/github.com/flashbots/go-template)
[![Test status](https://github.com/flashbots/tdx-orderflow-proxy/actions/workflows/checks.yml/badge.svg?branch=main)](https://github.com/flashbots/go-template/actions?query=workflow%3A%22Checks%22)

## Getting started

**Build**

```bash
make build
```

There are two separate programs in this repo:
* receiver proxy that should be part of tdx image
* sender proxy that is part of infra that sends orderflow to all peers

## Run receiver proxy

Receiver proxy will:

* generate SSL certificate
* generate orderflow signer
* create 2 input servers serving TLS with that certificate (local-listen-addr, public-listen-addr)
* create 1 local http server serving /cert  (cert-listen-addr)
* create metrics server (metric-addr)
* proxy requests to local builder
* proxy local request to other builders in the network
* archive local requests by sending them to archive endpoint

Flags for the receiver proxy

```
./build/receiver-proxy -h
NAME:
   receiver-proxy - Serve API, and metrics

USAGE:
   receiver-proxy [global options] command [command options]

COMMANDS:
   help, h  Shows a list of commands or help for one command

GLOBAL OPTIONS:
   --local-listen-addr value                   address to listen on for orderflow proxy API for external users and local operator (default: "127.0.0.1:443") [$LOCAL_LISTEN_ADDR]
   --public-listen-addr value                  address to listen on for orderflow proxy API for other network participants (default: "127.0.0.1:5544") [$PUBLIC_LISTEN_ADDR]
   --cert-listen-addr value                    address to listen on for orderflow proxy serving its SSL certificate on /cert (default: "127.0.0.1:14727") [$CERT_LISTEN_ADDR]
   --builder-endpoint value                    address to send local orderflow to (default: "http://127.0.0.1:8645") [$BUILDER_ENDPOINT]
   --rpc-endpoint value                        address of the node RPC that supports eth_blockNumber (default: "http://127.0.0.1:8545") [$RPC_ENDPOINT]
   --builder-confighub-endpoint value          address of the builder config hub endpoint (directly or using the cvm-proxy) (default: "http://127.0.0.1:14892") [$BUILDER_CONFIGHUB_ENDPOINT]
   --orderflow-archive-endpoint value          address of the orderflow archive endpoint (block-processor) (default: "http://127.0.0.1:14893") [$ORDERFLOW_ARCHIVE_ENDPOINT]
   --flashbots-orderflow-signer-address value  orderflow from Flashbots will be signed with this address (default: "0x5015Fa72E34f75A9eC64f44a4Fcf0837919D1bB7") [$FLASHBOTS_ORDERFLOW_SIGNER_ADDRESS]
   --max-request-body-size-bytes value         Maximum size of the request body, if 0 default will be used (default: 0) [$MAX_REQUEST_BODY_SIZE_BYTES]
   --connections-per-peer value                Number of parallel connections for each peer and archival RPC (default: 10) [$CONN_PER_PEER]
   --max-local-requests-per-second value       Maximum number of unique local requests per second (default: 100) [$MAX_LOCAL_RPS]
   --cert-duration value                       generated certificate duration (default: 8760h0m0s) [$CERT_DURATION]
   --cert-hosts value [ --cert-hosts value ]   generated certificate hosts (default: "127.0.0.1", "localhost") [$CERT_HOSTS]
   --metrics-addr value                        address to listen on for Prometheus metrics (metrics are served on $metrics-addr/metrics) (default: "127.0.0.1:8090") [$METRICS_ADDR]
   --log-json                                  log in JSON format (default: false) [$LOG_JSON]
   --log-debug                                 log debug messages (default: false) [$LOG_DEBUG]
   --log-uid                                   generate a uuid and add to all log messages (default: false) [$LOG_UID]
   --log-service value                         add 'service' tag to logs (default: "tdx-orderflow-proxy-receiver") [$LOG_SERVICE]
   --pprof                                     enable pprof debug endpoint (pprof is served on $metrics-addr/debug/pprof/*) (default: false) [$PPROF]
   --help, -h                                  show help
```

Additional receiver configuration environment variables:

* `HTTP_READ_TIMEOUT_SEC` - timeout for reading the request, default 60 seconds
* `HTTP_WRITE_TIMEOUT_SEC` - timeout for writing the response, default 30 seconds
* `HTTP_IDLE_TIMEOUT_SEC` - timeout for idle connection, default 1 hour (3600 seconds)

## Run sender proxy

Sender proxy will:
* listen for http requests
* sign request with `orderflow-signer-key`
* proxy them to the peers received form builder config hub

```
./build/sender-proxy -h
NAME:
   sender-proxy - Serve API, and metrics

USAGE:
   sender-proxy [global options] command [command options]

COMMANDS:
   help, h  Shows a list of commands or help for one command

GLOBAL OPTIONS:
   --listen-address value               address to listen on for requests (default: "127.0.0.1:8080") [$LISTEN_ADDRESS]
   --builder-confighub-endpoint value   address of the builder config hub endpoint (directly or using the cvm-proxy) (default: "http://127.0.0.1:14892") [$BUILDER_CONFIGHUB_ENDPOINT]
   --orderflow-signer-key value         orderflow will be signed with this address (default: "0xfb5ad18432422a84514f71d63b45edf51165d33bef9c2bd60957a48d4c4cb68e") [$ORDERFLOW_SIGNER_KEY]
   --max-request-body-size-bytes value  Maximum size of the request body, if 0 default will be used (default: 0) [$MAX_REQUEST_BODY_SIZE_BYTES]
   --connections-per-peer value         Number of parallel connections for each peer (default: 10) [$CONN_PER_PEER]
   --metrics-addr value                 address to listen on for Prometheus metrics (metrics are served on $metrics-addr/metrics) (default: "127.0.0.1:8090") [$METRICS_ADDR]
   --log-json                           log in JSON format (default: false) [$LOG_JSON]
   --log-debug                          log debug messages (default: false) [$LOG_DEBUG]
   --log-uid                            generate a uuid and add to all log messages (default: false) [$LOG_UID]
   --log-service value                  add 'service' tag to logs (default: "tdx-orderflow-proxy-sender") [$LOG_SERVICE]
   --pprof                              enable pprof debug endpoint (pprof is served on $metrics-addr/debug/pprof/*) (default: false) [$PPROF]
   --help, -h                           show help
```
