package app

import (
	"context"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"easy_proxies/internal/boxmgr"
	"easy_proxies/internal/config"
	"easy_proxies/internal/monitor"
)

// TestLeaseBoxGeneration_ProxiesWithoutLegacyRuntime 验证 Lease Generation 拥有独立 sing-box：
// 不启动 Legacy BoxManager 的 2323/24000+ 监听，也能经管理 API 和 Gateway 完成代理请求。
func TestLeaseBoxGeneration_ProxiesWithoutLegacyRuntime(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health/ready" {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		_, _ = io.WriteString(w, "lease-only-generation")
	}))
	t.Cleanup(target.Close)
	upstreamProxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodConnect {
			http.Error(w, "CONNECT required", http.StatusMethodNotAllowed)
			return
		}
		upstream, err := net.Dial("tcp", r.Host)
		if err != nil {
			http.Error(w, "dial target failed", http.StatusBadGateway)
			return
		}
		hijacker, ok := w.(http.Hijacker)
		if !ok {
			_ = upstream.Close()
			http.Error(w, "hijacking unavailable", http.StatusInternalServerError)
			return
		}
		client, buffered, err := hijacker.Hijack()
		if err != nil {
			_ = upstream.Close()
			return
		}
		_, _ = buffered.WriteString("HTTP/1.1 200 Connection Established\r\n\r\n")
		_ = buffered.Flush()
		go func() {
			defer client.Close()
			defer upstream.Close()
			go func() { _, _ = io.Copy(upstream, buffered) }()
			_, _ = io.Copy(client, upstream)
		}()
	}))
	t.Cleanup(upstreamProxy.Close)
	cfg := &config.Config{
		Mode:       "pool",
		Listener:   config.ListenerConfig{Address: "127.0.0.1", Port: reservePort(t)},
		Management: config.ManagementConfig{ProbeTarget: target.URL + "/health/ready"},
		LeaseGateway: config.LeaseGatewayConfig{
			Enabled:             true,
			Listen:              "127.0.0.1",
			Port:                reservePort(t),
			APIToken:            "machine-token",
			MinReadyNodes:       1,
			ProbeExpectedStatus: http.StatusNoContent,
		},
		Nodes: []config.NodeConfig{{Name: "lease-http-node", URI: upstreamProxy.URL}},
	}
	if err := cfg.NormalizeWithPortMap(nil); err != nil {
		t.Fatalf("normalize config: %v", err)
	}

	candidate, err := boxmgr.BuildLeaseCandidate(context.Background(), cfg, "generation-1")
	if err != nil {
		t.Fatalf("build Lease Candidate: %v", err)
	}
	monitorManager, err := monitor.NewManager(monitor.Config{})
	if err != nil {
		t.Fatalf("new monitor manager: %v", err)
	}
	management := monitor.NewServer(monitor.Config{Enabled: true}, monitorManager, log.Default())
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	service, err := startLeaseServiceWithCandidate(ctx, cfg, management, candidate)
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
	grant := acquireGenerationLease(t, controlServer.URL, "lease-only")
	if got := requestURLThroughLease(t, grant.ProxyURL, target.URL+"/business"); got != "lease-only-generation" {
		t.Fatalf("lease-only generation response = %q", got)
	}
}
