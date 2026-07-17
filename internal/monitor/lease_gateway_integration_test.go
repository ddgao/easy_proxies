package monitor

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"easy_proxies/internal/lease"
)

// fixedNodeDialer 将所有目标连接导向测试上游，用于在真实 HTTP 边界验证租约固定到指定 Node Key。
// 它只替代外部网络和上游代理，不替代 Lease Runtime、管理 Handler 或 Gateway。
type fixedNodeDialer struct {
	address string
}

func (d fixedNodeDialer) DialContext(ctx context.Context, network, _ string) (net.Conn, error) {
	return (&net.Dialer{}).DialContext(ctx, network, d.address)
}

// TestLeaseGateway_AcquiredProxyKeepsRequestsOnOneNode 验证最小租约闭环：
// 调用方申请一次租约后，可使用返回的代理 URL 连续访问业务目标，且两次请求都绑定同一节点。
func TestLeaseGateway_AcquiredProxyKeepsRequestsOnOneNode(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "served-by-node-a")
	}))
	t.Cleanup(upstream.Close)
	upstreamURL, err := url.Parse(upstream.URL)
	if err != nil {
		t.Fatalf("parse upstream URL: %v", err)
	}

	gatewayListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen gateway: %v", err)
	}

	runtime, err := lease.NewRuntime(lease.Options{
		APIToken:   "machine-token",
		ProxyURL:   "http://" + gatewayListener.Addr().String(),
		TokenBytes: 32,
		Nodes: []lease.Node{{
			Key:    "node-a",
			Dialer: fixedNodeDialer{address: upstreamURL.Host},
		}},
	})
	if err != nil {
		t.Fatalf("new lease runtime: %v", err)
	}
	t.Cleanup(func() {
		_ = runtime.Close()
	})

	gatewayServer := &http.Server{Handler: runtime.GatewayHandler()}
	go func() {
		_ = gatewayServer.Serve(gatewayListener)
	}()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = gatewayServer.Shutdown(ctx)
	})

	monitorManager, err := NewManager(Config{})
	if err != nil {
		t.Fatalf("new monitor manager: %v", err)
	}
	server := NewServer(Config{Enabled: true}, monitorManager, log.Default())
	server.SetLeaseController(runtime)
	controlServer := httptest.NewServer(server.Handler())
	t.Cleanup(controlServer.Close)

	req, err := http.NewRequest(http.MethodPost, controlServer.URL+"/api/proxy-leases", strings.NewReader(`{"label":"crawl-one"}`))
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
		NodeKey    string    `json:"node_key"`
		LeaseToken string    `json:"lease_token"`
		ProxyURL   string    `json:"proxy_url"`
		ExpiresAt  time.Time `json:"expires_at"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&grant); err != nil {
		t.Fatalf("decode lease grant: %v", err)
	}
	if grant.NodeKey != "node-a" {
		t.Fatalf("node_key = %q, want node-a", grant.NodeKey)
	}
	rawToken, err := base64.RawURLEncoding.DecodeString(grant.LeaseToken)
	if err != nil || len(rawToken) != 32 {
		t.Fatalf("lease token decoded bytes = %d, error = %v", len(rawToken), err)
	}
	remaining := time.Until(grant.ExpiresAt)
	if remaining < time.Minute+50*time.Second || remaining > 2*time.Minute {
		t.Fatalf("lease expiration remaining = %s, want about two minutes", remaining)
	}

	snapshotResp, err := http.Get(controlServer.URL + "/api/proxy-runtime")
	if err != nil {
		t.Fatalf("get lease runtime snapshot: %v", err)
	}
	snapshotBody, err := io.ReadAll(snapshotResp.Body)
	_ = snapshotResp.Body.Close()
	if err != nil {
		t.Fatalf("read lease runtime snapshot: %v", err)
	}
	if snapshotResp.StatusCode != http.StatusOK {
		t.Fatalf("snapshot status = %d, body = %s", snapshotResp.StatusCode, snapshotBody)
	}
	if strings.Contains(string(snapshotBody), grant.LeaseToken) || strings.Contains(string(snapshotBody), grant.ProxyURL) {
		t.Fatalf("snapshot leaked lease credentials: %s", snapshotBody)
	}
	var snapshot struct {
		Enabled      bool `json:"enabled"`
		ActiveLeases []struct {
			NodeKey          string `json:"node_key"`
			Label            string `json:"label"`
			TokenFingerprint string `json:"token_fingerprint"`
		} `json:"active_leases"`
	}
	if err := json.Unmarshal(snapshotBody, &snapshot); err != nil {
		t.Fatalf("decode lease runtime snapshot: %v", err)
	}
	if !snapshot.Enabled || len(snapshot.ActiveLeases) != 1 {
		t.Fatalf("snapshot = %+v", snapshot)
	}
	leaseSummary := snapshot.ActiveLeases[0]
	if leaseSummary.NodeKey != "node-a" || leaseSummary.Label != "crawl-one" || leaseSummary.TokenFingerprint == "" {
		t.Fatalf("lease summary = %+v", leaseSummary)
	}

	proxyURL, err := url.Parse(grant.ProxyURL)
	if err != nil {
		t.Fatalf("parse proxy URL: %v", err)
	}
	client := &http.Client{Transport: &http.Transport{Proxy: http.ProxyURL(proxyURL)}}
	for i := 0; i < 2; i++ {
		proxyResp, err := client.Get("http://business.example/resource")
		if err != nil {
			t.Fatalf("proxy request %d: %v", i+1, err)
		}
		body, readErr := io.ReadAll(proxyResp.Body)
		_ = proxyResp.Body.Close()
		if readErr != nil {
			t.Fatalf("read proxy response %d: %v", i+1, readErr)
		}
		if string(body) != "served-by-node-a" {
			t.Fatalf("proxy response %d = %q", i+1, body)
		}
	}

	wrongRelease, err := http.NewRequest(http.MethodDelete, controlServer.URL+"/api/proxy-leases", nil)
	if err != nil {
		t.Fatalf("new unauthorized release request: %v", err)
	}
	wrongRelease.Header.Set("Authorization", "Bearer wrong-machine-token")
	wrongRelease.Header.Set("X-Lease-Token", grant.LeaseToken)
	wrongResp, err := http.DefaultClient.Do(wrongRelease)
	if err != nil {
		t.Fatalf("unauthorized release: %v", err)
	}
	_ = wrongResp.Body.Close()
	if wrongResp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unauthorized release status = %d", wrongResp.StatusCode)
	}

	for i := 0; i < 2; i++ {
		releaseReq, err := http.NewRequest(http.MethodDelete, controlServer.URL+"/api/proxy-leases", nil)
		if err != nil {
			t.Fatalf("new release request %d: %v", i+1, err)
		}
		releaseReq.Header.Set("Authorization", "Bearer machine-token")
		releaseReq.Header.Set("X-Lease-Token", grant.LeaseToken)
		releaseResp, err := http.DefaultClient.Do(releaseReq)
		if err != nil {
			t.Fatalf("release request %d: %v", i+1, err)
		}
		_ = releaseResp.Body.Close()
		if releaseResp.StatusCode != http.StatusOK {
			t.Fatalf("release status %d = %d", i+1, releaseResp.StatusCode)
		}
	}

	releasedResp, err := client.Get("http://business.example/after-release")
	if err != nil {
		t.Fatalf("request after release: %v", err)
	}
	_ = releasedResp.Body.Close()
	if releasedResp.StatusCode != http.StatusProxyAuthRequired {
		t.Fatalf("request after release status = %d, want 407", releasedResp.StatusCode)
	}
}

// TestProxyLeases_OptionalLabelAllowsEmptyBody 验证租约标签确实可选，机器调用方无需发送占位 JSON。
func TestProxyLeases_OptionalLabelAllowsEmptyBody(t *testing.T) {
	runtime, err := lease.NewRuntime(lease.Options{
		APIToken: "machine-token",
		ProxyURL: "http://127.0.0.1:2330",
		Nodes: []lease.Node{{
			Key:    "node-a",
			Dialer: fixedNodeDialer{address: "127.0.0.1:1"},
		}},
	})
	if err != nil {
		t.Fatalf("new lease runtime: %v", err)
	}
	t.Cleanup(func() { _ = runtime.Close() })
	manager, err := NewManager(Config{})
	if err != nil {
		t.Fatalf("new monitor manager: %v", err)
	}
	server := NewServer(Config{Enabled: true}, manager, log.Default())
	server.SetLeaseController(runtime)
	longLabelRequest := httptest.NewRequest(http.MethodPost, "/api/proxy-leases", strings.NewReader(`{"label":"`+strings.Repeat("任", 65)+`"}`))
	longLabelRequest.Header.Set("Authorization", "Bearer machine-token")
	longLabelResponse := httptest.NewRecorder()
	server.Handler().ServeHTTP(longLabelResponse, longLabelRequest)
	if longLabelResponse.Code != http.StatusBadRequest {
		t.Fatalf("long-label acquire status = %d, body = %s", longLabelResponse.Code, longLabelResponse.Body.String())
	}

	request := httptest.NewRequest(http.MethodPost, "/api/proxy-leases", nil)
	request.Header.Set("Authorization", "Bearer machine-token")
	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusCreated {
		t.Fatalf("empty-body acquire status = %d, body = %s", response.Code, response.Body.String())
	}
}
