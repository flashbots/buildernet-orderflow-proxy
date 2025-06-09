// Package proxy provides the main proxy server.
package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"

	"github.com/ethereum/go-ethereum/common"
)

type ConfighubOrderflowProxyCredentials struct {
	TLSCert            string         `json:"tls_cert"` // for backward compatibility
	EcdsaPubkeyAddress common.Address `json:"ecdsa_pubkey_address"`
}

type ConfighubInstanceData struct {
	TLSCert string `json:"tls_cert"`
}

type ConfighubBuilder struct {
	Name           string                             `json:"name"`
	IP             string                             `json:"ip"`
	DNSName        string                             `json:"dns_name"`
	OrderflowProxy ConfighubOrderflowProxyCredentials `json:"orderflow_proxy"`
	Instance       ConfighubInstanceData              `json:"instance"`
}

func (b *ConfighubBuilder) SystemAPIAddress() string {
	if b.DNSName != "" {
		return OrderflowProxyURLFromIPOrDNSName(b.DNSName)
	}
	return OrderflowProxyURLFromIPOrDNSName(b.IP)
}

func (b *ConfighubBuilder) TLSCert() string {
	if b.Instance.TLSCert != "" {
		return b.Instance.TLSCert
	}
	return b.OrderflowProxy.TLSCert
}

type BuilderConfigHub struct {
	log      *slog.Logger
	endpoint string
}

func NewBuilderConfigHub(log *slog.Logger, endpoint string) *BuilderConfigHub {
	return &BuilderConfigHub{
		log:      log,
		endpoint: endpoint,
	}
}

func (b *BuilderConfigHub) RegisterCredentials(ctx context.Context, info ConfighubOrderflowProxyCredentials) error {
	body, err := json.Marshal(info)
	if err != nil {
		return err
	}
	req, err := http.NewRequest(http.MethodPost, b.endpoint+"/api/l1-builder/v1/register_credentials/orderflow_proxy", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req = req.WithContext(ctx)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("builder config hub returned error, code: %d, body: %s", resp.StatusCode, string(respBody))
	}
	return nil
}

func (b *BuilderConfigHub) Builders(internal bool) (result []ConfighubBuilder, err error) {
	defer func() {
		if err != nil {
			confighubErrorsCounter.Inc()
			b.log.Error("Failed to fetch peer list from config hub", slog.Any("error", err))
		}
	}()

	var resp *http.Response
	url := b.endpoint
	if internal {
		url += "/api/internal/l1-builder/v1/builders"
	} else {
		url += "/api/l1-builder/v1/builders"
	}
	resp, err = http.Get(url) //nolint:gosec
	if err != nil {
		return
	}
	defer resp.Body.Close()
	var body []byte
	body, err = io.ReadAll(resp.Body)
	if err != nil {
		return
	}

	err = json.Unmarshal(body, &result)
	if err != nil {
		return nil, err
	}
	b.log.Info("Received list of peers from confighub", slog.Bool("internalEndpoint", internal), slog.Any("peers", result))
	return
}
