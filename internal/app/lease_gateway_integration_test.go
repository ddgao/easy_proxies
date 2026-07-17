package app

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"easy_proxies/internal/config"
	"easy_proxies/internal/lease"
	"easy_proxies/internal/monitor"
)

type configuredNodeDialer struct {
	address string
}

func (d configuredNodeDialer) DialContext(ctx context.Context, network, _ string) (net.Conn, error) {
	return (&net.Dialer{}).DialContext(ctx, network, d.address)
}

// TestStartLeaseService_UsesConfiguredGatewayAndManagementAPI 验证应用层生产接线：
// 配置决定真实 Gateway 监听地址，管理 Handler 申请的租约可通过 BoxManager 提供的固定节点拨号器访问目标。
func TestStartLeaseService_UsesConfiguredGatewayAndManagementAPI(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "through-configured-node")
	}))
	t.Cleanup(upstream.Close)
	upstreamURL, err := url.Parse(upstream.URL)
	if err != nil {
		t.Fatalf("parse upstream URL: %v", err)
	}

	gatewayPort := reservePort(t)
	cfg := &config.Config{
		Mode:       "pool",
		ExternalIP: "127.0.0.1",
		Listener: config.ListenerConfig{
			Address: "127.0.0.1",
			Port:    reservePort(t),
		},
		Management: config.ManagementConfig{ProbeTarget: upstream.URL},
		LeaseGateway: config.LeaseGatewayConfig{
			Enabled:             true,
			Listen:              "0.0.0.0",
			Port:                gatewayPort,
			APIToken:            "machine-token",
			MinReadyNodes:       1,
			ProbeExpectedStatus: http.StatusOK,
		},
		Nodes: []config.NodeConfig{{Name: "node-a", URI: upstream.URL}},
	}
	if err := cfg.NormalizeWithPortMap(nil); err != nil {
		t.Fatalf("normalize config: %v", err)
	}

	monitorManager, err := monitor.NewManager(monitor.Config{})
	if err != nil {
		t.Fatalf("new monitor manager: %v", err)
	}
	management := monitor.NewServer(monitor.Config{Enabled: true}, monitorManager, log.Default())
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	service, err := startLeaseService(ctx, cfg, management, []lease.Node{{
		Key:    cfg.Nodes[0].LeaseNodeKey(),
		Dialer: configuredNodeDialer{address: upstreamURL.Host},
	}})
	if err != nil {
		t.Fatalf("start lease service: %v", err)
	}
	waitLeaseReady(t, service)
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), time.Second)
		defer closeCancel()
		_ = service.Close(closeCtx)
	})

	controlServer := httptest.NewServer(management.Handler())
	t.Cleanup(controlServer.Close)
	req, err := http.NewRequest(http.MethodPost, controlServer.URL+"/api/proxy-leases", strings.NewReader(`{"label":"integration"}`))
	if err != nil {
		t.Fatalf("new acquire request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer machine-token")
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("acquire lease: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("acquire status = %d, body = %s", resp.StatusCode, body)
	}

	var grant struct {
		NodeKey  string `json:"node_key"`
		ProxyURL string `json:"proxy_url"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&grant); err != nil {
		t.Fatalf("decode grant: %v", err)
	}
	if grant.NodeKey != cfg.Nodes[0].LeaseNodeKey() {
		t.Fatalf("node_key = %q, want %q", grant.NodeKey, cfg.Nodes[0].LeaseNodeKey())
	}
	wantGateway := fmt.Sprintf("127.0.0.1:%d", gatewayPort)
	proxyURL, err := url.Parse(grant.ProxyURL)
	if err != nil {
		t.Fatalf("parse proxy URL: %v", err)
	}
	if proxyURL.Host != wantGateway {
		t.Fatalf("proxy host = %q, want %q", proxyURL.Host, wantGateway)
	}
	client := &http.Client{
		Transport: &http.Transport{Proxy: http.ProxyURL(proxyURL)},
		Timeout:   5 * time.Second,
	}
	proxyResp, err := client.Get("http://business.example/resource")
	if err != nil {
		t.Fatalf("request through Lease Gateway: %v", err)
	}
	defer proxyResp.Body.Close()
	body, err := io.ReadAll(proxyResp.Body)
	if err != nil {
		t.Fatalf("read proxy response: %v", err)
	}
	if string(body) != "through-configured-node" {
		t.Fatalf("proxy response = %q", body)
	}
}

func waitLeaseReady(t *testing.T, service *leaseService) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		if service.runtime.Snapshot().Ready {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("Lease Gateway did not become ready: %+v", service.runtime.Snapshot())
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func reservePort(t *testing.T) uint16 {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	defer listener.Close()
	return uint16(listener.Addr().(*net.TCPAddr).Port)
}
