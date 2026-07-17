package app

import (
	"context"
	"encoding/json"
	"io"
	"log"
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

// TestStartLeaseService_OnlyAllocatesValidatedNodes 验证租约节点必须先通过完整 HTTP 准入。
// 坏节点即使位于候选列表首位，也不能出现在申请响应或业务代理路径中。
func TestStartLeaseService_OnlyAllocatesValidatedNodes(t *testing.T) {
	badUpstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health/ready" {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		_, _ = io.WriteString(w, "served-by-bad-node")
	}))
	t.Cleanup(badUpstream.Close)
	goodUpstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health/ready" {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		_, _ = io.WriteString(w, "served-by-good-node")
	}))
	t.Cleanup(goodUpstream.Close)
	badURL, _ := url.Parse(badUpstream.URL)
	goodURL, _ := url.Parse(goodUpstream.URL)

	cfg := &config.Config{
		Mode: "pool",
		Listener: config.ListenerConfig{
			Address: "127.0.0.1",
			Port:    reservePort(t),
		},
		Management: config.ManagementConfig{
			ProbeTarget: "http://probe.example/health/ready",
		},
		LeaseGateway: config.LeaseGatewayConfig{
			Enabled:             true,
			Listen:              "127.0.0.1",
			Port:                reservePort(t),
			APIToken:            "machine-token",
			MinReadyNodes:       1,
			ProbeExpectedStatus: http.StatusNoContent,
			AcquireWaitTimeout:  20 * time.Millisecond,
		},
		Nodes: []config.NodeConfig{{Name: "placeholder", URI: "http://127.0.0.1:18080"}},
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
	service, err := startLeaseService(ctx, cfg, management, []lease.Node{
		{Key: "bad-node", Dialer: configuredNodeDialer{address: badURL.Host}},
		{Key: "good-node", Dialer: configuredNodeDialer{address: goodURL.Host}},
	})
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
	request, err := http.NewRequest(http.MethodPost, controlServer.URL+"/api/proxy-leases", strings.NewReader(`{"label":"validated-only"}`))
	if err != nil {
		t.Fatalf("new acquire request: %v", err)
	}
	request.Header.Set("Authorization", "Bearer machine-token")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("acquire lease: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(response.Body)
		t.Fatalf("acquire status = %d, body = %s", response.StatusCode, body)
	}
	var grant lease.Grant
	if err := json.NewDecoder(response.Body).Decode(&grant); err != nil {
		t.Fatalf("decode grant: %v", err)
	}
	if grant.NodeKey != "good-node" {
		t.Fatalf("allocated Node Key = %q, want good-node", grant.NodeKey)
	}
	proxyURL, err := url.Parse(grant.ProxyURL)
	if err != nil {
		t.Fatalf("parse proxy URL: %v", err)
	}
	client := &http.Client{Transport: &http.Transport{Proxy: http.ProxyURL(proxyURL)}}
	businessResponse, err := client.Get("http://business.example/resource")
	if err != nil {
		t.Fatalf("business request: %v", err)
	}
	defer businessResponse.Body.Close()
	body, err := io.ReadAll(businessResponse.Body)
	if err != nil {
		t.Fatalf("read business response: %v", err)
	}
	if string(body) != "served-by-good-node" {
		t.Fatalf("business response = %q", body)
	}
}

// TestStartLeaseService_InsufficientInitialCapacityKeepsGatewayAlive 验证首次准入不足时，
// Gateway 保持可观测但拒绝租约，不能把 Lease readiness 失败升级为整个进程启动失败。
func TestStartLeaseService_InsufficientInitialCapacityKeepsGatewayAlive(t *testing.T) {
	probeServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(probeServer.Close)
	probeURL, err := url.Parse(probeServer.URL)
	if err != nil {
		t.Fatalf("parse probe URL: %v", err)
	}

	cfg := &config.Config{
		Mode:       "pool",
		Listener:   config.ListenerConfig{Address: "127.0.0.1", Port: reservePort(t)},
		Management: config.ManagementConfig{ProbeTarget: "http://probe.example/health/ready"},
		LeaseGateway: config.LeaseGatewayConfig{
			Enabled:             true,
			Listen:              "127.0.0.1",
			Port:                reservePort(t),
			APIToken:            "machine-token",
			MinReadyNodes:       2,
			ProbeExpectedStatus: http.StatusNoContent,
			AcquireWaitTimeout:  20 * time.Millisecond,
		},
		Nodes: []config.NodeConfig{{Name: "placeholder", URI: "http://127.0.0.1:18080"}},
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
		Key: "only-node", Dialer: configuredNodeDialer{address: probeURL.Host},
	}})
	if err != nil {
		t.Fatalf("start Lease Gateway with insufficient initial capacity: %v", err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), time.Second)
		defer closeCancel()
		_ = service.Close(closeCtx)
	})
	controlServer := httptest.NewServer(management.Handler())
	t.Cleanup(controlServer.Close)

	request, err := http.NewRequest(http.MethodPost, controlServer.URL+"/api/proxy-leases", nil)
	if err != nil {
		t.Fatalf("new acquire request: %v", err)
	}
	request.Header.Set("Authorization", "Bearer machine-token")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("acquire while not ready: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("acquire status = %d, want 503", response.StatusCode)
	}

	deadline := time.Now().Add(time.Second)
	for {
		snapshot := service.runtime.Snapshot()
		if snapshot.Validation.Pending == 0 && snapshot.Validation.Validating == 0 && len(snapshot.RecentGenerations) > 0 {
			if snapshot.Ready || !snapshot.Enabled || snapshot.Validation.Ready != 1 {
				t.Fatalf("runtime snapshot after insufficient admission = %+v", snapshot)
			}
			if snapshot.RecentGenerations[0].BuildPhase != "FAILED" || snapshot.RecentGenerations[0].ID != "generation-1" {
				t.Fatalf("failed generation history = %+v", snapshot.RecentGenerations)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("initial validation did not finish: %+v", snapshot.Validation)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
