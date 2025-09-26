package main

import (
	"context"
	"errors"
	"log"
	"log/slog"
	"os"

	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/flashbots/go-utils/rpcclient"
	"github.com/flashbots/go-utils/rpctypes"
	"github.com/flashbots/go-utils/signature"
	"github.com/flashbots/tdx-orderflow-proxy/proxy"
	"github.com/google/uuid"
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
	&cli.StringFlag{
		Name:    "signer-private-key",
		Value:   "0x52da2727dd1180b547258c9ca7deb7f9576b2768f3f293b67f36505c85b2ddd0",
		Usage:   "signer of the requests",
		EnvVars: []string{"SIGNER_PRIVATE_KEY"},
	},
	&cli.StringFlag{
		Name:    "rpc-endpoint",
		Value:   "http://127.0.0.1:8545",
		Usage:   "address of the node RPC that supports eth_blockNumber",
		EnvVars: []string{"RPC_ENDPOINT"},
	},
}

// test tx

// tx with hash 0x40614141bf0c512efcaa2e742f79ce5e654c6658d5de77ca4f1154b5b52ae13a
var testTx = hexutil.MustDecode("0x1234")

func main() {
	app := &cli.App{
		Name:  "test-tx-sender",
		Usage: "send test transactions",
		Flags: flags,
		Action: func(cCtx *cli.Context) error {
			localOrderflowEndpoint := cCtx.String("local-orderflow-endpoint")
			signerPrivateKey := cCtx.String("signer-private-key")

			orderflowSigner, err := signature.NewSignerFromHexPrivateKey(signerPrivateKey)
			if err != nil {
				return err
			}
			slog.Info("Ordeflow signing address", "address", orderflowSigner.Address())

			client := rpcclient.NewClientWithOpts(localOrderflowEndpoint, &rpcclient.RPCClientOpts{
				Signer: orderflowSigner,
			})
			slog.Info("Created client")

			rpcEndpoint := cCtx.String("rpc-endpoint")
			blockNumberSource := proxy.NewBlockNumberSource(rpcEndpoint)
			block, err := blockNumberSource.BlockNumber()
			if err != nil {
				return err
			}
			slog.Info("Current block number", "block", block)

			// send eth_sendRawTransactions
			resp, err := client.Call(context.Background(), "eth_sendRawTransaction", hexutil.Bytes(testTx))
			if err != nil {
				return err
			}
			if resp.Error != nil {
				slog.Error("RPC returned error", "error", resp.Error)
				return errors.New("eth_sendRawTransaction failed")
			}
			slog.Info("Sent eth_sendRawTransaction")

			// send eth_sendBundle
			repacementUUID := uuid.New()
			slog.Info("Using the following replacement UUID", "value", repacementUUID)
			blockNumber := hexutil.Uint64(block)
			bundleArgs := rpctypes.EthSendBundleArgs{
				Txs:             []hexutil.Bytes{testTx},
				ReplacementUUID: &repacementUUID,
				BlockNumber:     &blockNumber,
			}

			bundleHash, bundleUUID, err := bundleArgs.Validate()
			if err != nil {
				return err
			}
			resp, err = client.Call(context.Background(), "eth_sendBundle", bundleArgs)
			if err != nil {
				return err
			}
			if resp.Error != nil {
				slog.Error("RPC returned error", "error", resp.Error)
				return errors.New("eth_sendBundle failed")
			}
			slog.Info("Sent eth_sendBundle", "bundleHash", bundleHash, "bundleUUID", bundleUUID)

			// send eth_cancelBundle

			resp, err = client.Call(context.Background(), "eth_cancelBundle", rpctypes.EthCancelBundleArgs{
				ReplacementUUID: repacementUUID,
			})
			if err != nil {
				return err
			}
			if resp.Error != nil {
				slog.Error("RPC returned error", "error", resp.Error)
				return errors.New("eth_cancelBundle failed")
			}
			slog.Info("Sent eth_cancelBundle")

			// send mev_sendBundle (normal bundle)
			sbundleArgs := rpctypes.MevSendBundleArgs{
				Version:         "v0.1",
				ReplacementUUID: &repacementUUID,
				Inclusion: rpctypes.MevBundleInclusion{
					BlockNumber: hexutil.Uint64(block),
				},
				Body: []rpctypes.MevBundleBody{
					{
						Tx:        (*hexutil.Bytes)(&testTx),
						CanRevert: true,
					},
				},
			}
			sbundleHash, err := sbundleArgs.Validate()
			if err != nil {
				return err
			}

			resp, err = client.Call(context.Background(), "mev_sendBundle", sbundleArgs)
			if err != nil {
				return err
			}
			if resp.Error != nil {
				slog.Error("RPC returned error", "error", resp.Error)
				return errors.New("mev_sendBundle (normal bundle) failed")
			}
			slog.Info("Sent mev_sendBundle (normal bundle)", "hash", sbundleHash)

			// send mev_sendBundle (cancellation bundle)
			sbundleCancelArgs := rpctypes.MevSendBundleArgs{
				Version:         "v0.1",
				ReplacementUUID: &repacementUUID,
			}
			resp, err = client.Call(context.Background(), "mev_sendBundle", sbundleCancelArgs)
			if err != nil {
				return err
			}
			if resp.Error != nil {
				slog.Error("RPC returned error", "error", resp.Error)
				return errors.New("mev_sendBundle (cancellation) failed")
			}
			slog.Info("Sent mev_sendBundle (cancellation)")

			return nil
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}
