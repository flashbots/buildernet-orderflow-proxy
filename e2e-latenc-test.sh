#!/bin/bash

make build

./build/receiver-proxy --cert-path=/tmp/cert --cert-key-path=/tmp/key --pprof --user-listen-addr 0.0.0.0:9976 --builder-endpoint http://127.0.0.1:7890 --builder-confighub-endpoint http://127.0.0.1:7890 --orderflow-archive-endpoint http://127.0.0.1:7890 --connections-per-peer 100 > /tmp/log.txt 2>&1 &

PROXY_PID=$!

./build/test-e2e-latency --local-orderflow-endpoint https://127.0.0.1:9976 --local-receiver-server-port 7890 --num-requests 100000 --num-senders 1000

# uncomment to send orderflow without proxy to see test setup overhead 
#./build/test-e2e-latency --local-orderflow-endpoint http://127.0.0.1:7890 --local-receiver-server-port 7890 --num-requests 100000 --num-senders 1000

#sleep 10
kill $PROXY_PID
