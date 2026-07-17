package lease_test

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"
	"time"

	"easy_proxies/internal/lease"
)

type connectNodeDialer struct {
	address string
}

type failingNodeDialer struct{}

func (failingNodeDialer) DialContext(context.Context, string, string) (net.Conn, error) {
	return nil, errors.New("node unavailable")
}

func (d connectNodeDialer) DialContext(ctx context.Context, network, _ string) (net.Conn, error) {
	return (&net.Dialer{}).DialContext(ctx, network, d.address)
}

// TestGateway_HTTPSConnectUsesLeasedNode 验证 HTTPS CONNECT 隧道只使用租约绑定的节点拨号器。
func TestGateway_HTTPSConnectUsesLeasedNode(t *testing.T) {
	tlsTarget := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "connected-through-node-a")
	}))
	t.Cleanup(tlsTarget.Close)
	targetURL, err := url.Parse(tlsTarget.URL)
	if err != nil {
		t.Fatalf("parse TLS target URL: %v", err)
	}

	gatewayListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen gateway: %v", err)
	}
	runtime, err := lease.NewRuntime(lease.Options{
		APIToken:         "machine-token",
		ProxyURL:         "http://" + gatewayListener.Addr().String(),
		MinReadyCapacity: 1,
		Nodes: []lease.Node{{
			Key:    "node-a",
			Dialer: connectNodeDialer{address: targetURL.Host},
		}},
	})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}
	t.Cleanup(func() { _ = runtime.Close() })
	gateway := &http.Server{Handler: runtime.GatewayHandler()}
	go func() { _ = gateway.Serve(gatewayListener) }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = gateway.Shutdown(ctx)
	})

	grant, err := runtime.Acquire(context.Background(), "https-task")
	if err != nil {
		t.Fatalf("acquire lease: %v", err)
	}
	proxyURL, err := url.Parse(grant.ProxyURL)
	if err != nil {
		t.Fatalf("parse proxy URL: %v", err)
	}
	client := &http.Client{Transport: &http.Transport{
		Proxy:           http.ProxyURL(proxyURL),
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, // 测试 TLS 服务使用临时自签名证书。
	}}
	response, err := client.Get("https://business.example/resource")
	if err != nil {
		t.Fatalf("HTTPS request through Gateway: %v", err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("read HTTPS response: %v", err)
	}
	if string(body) != "connected-through-node-a" {
		t.Fatalf("HTTPS response = %q", body)
	}
}

// TestGateway_DrainTimeoutClosesResidualConnectTunnel 验证排空上限会主动终止残留隧道，
// 防止 Node Key 归还后旧连接仍继续占用同一上游节点。
func TestGateway_DrainTimeoutClosesResidualConnectTunnel(t *testing.T) {
	requestStarted := make(chan struct{})
	releaseTarget := make(chan struct{})
	tlsTarget := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		close(requestStarted)
		<-releaseTarget
		_, _ = io.WriteString(w, "late-response")
	}))
	t.Cleanup(func() {
		close(releaseTarget)
		tlsTarget.Close()
	})
	targetURL, err := url.Parse(tlsTarget.URL)
	if err != nil {
		t.Fatalf("parse TLS target URL: %v", err)
	}
	gatewayListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen gateway: %v", err)
	}
	runtime, err := lease.NewRuntime(lease.Options{
		APIToken:     "machine-token",
		ProxyURL:     "http://" + gatewayListener.Addr().String(),
		DrainTimeout: 40 * time.Millisecond,
		Nodes: []lease.Node{{
			Key: "node-a", Dialer: connectNodeDialer{address: targetURL.Host},
		}},
	})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}
	t.Cleanup(func() { _ = runtime.Close() })
	gateway := &http.Server{Handler: runtime.GatewayHandler()}
	go func() { _ = gateway.Serve(gatewayListener) }()
	t.Cleanup(func() { _ = gateway.Shutdown(context.Background()) })
	grant, err := runtime.Acquire(context.Background(), "forced-drain")
	if err != nil {
		t.Fatalf("acquire lease: %v", err)
	}
	proxyURL, err := url.Parse(grant.ProxyURL)
	if err != nil {
		t.Fatalf("parse proxy URL: %v", err)
	}
	client := &http.Client{Transport: &http.Transport{
		Proxy:           http.ProxyURL(proxyURL),
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}}
	requestResult := make(chan error, 1)
	go func() {
		response, requestErr := client.Get("https://business.example/blocked")
		if response != nil {
			_ = response.Body.Close()
		}
		requestResult <- requestErr
	}()
	select {
	case <-requestStarted:
	case <-time.After(time.Second):
		t.Fatal("HTTPS request did not establish tunnel")
	}
	if err := runtime.Release(context.Background(), grant.LeaseToken); err != nil {
		t.Fatalf("release lease: %v", err)
	}
	select {
	case requestErr := <-requestResult:
		if requestErr == nil {
			t.Fatal("residual tunnel completed instead of being force-closed")
		}
	case <-time.After(300 * time.Millisecond):
		t.Fatal("residual tunnel remained open after drain timeout")
	}
}

// TestGateway_ConfirmedNodeFailureBreaksLease 验证代理失败后的独立复检状态流转：
// 复检确认节点不可用后原租约成为 Broken，后续请求不会切换其他节点。
func TestGateway_ConfirmedNodeFailureBreaksLease(t *testing.T) {
	gatewayListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen gateway: %v", err)
	}
	runtime, err := lease.NewRuntime(lease.Options{
		APIToken:         "machine-token",
		ProxyURL:         "http://" + gatewayListener.Addr().String(),
		MinReadyCapacity: 1,
		NodeRecheck: func(context.Context, lease.Node) bool {
			return false
		},
		Nodes: []lease.Node{{Key: "broken-node", Dialer: failingNodeDialer{}}},
	})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}
	t.Cleanup(func() { _ = runtime.Close() })
	gateway := &http.Server{Handler: runtime.GatewayHandler()}
	go func() { _ = gateway.Serve(gatewayListener) }()
	t.Cleanup(func() { _ = gateway.Shutdown(context.Background()) })
	grant, err := runtime.Acquire(context.Background(), "broken")
	if err != nil {
		t.Fatalf("acquire lease: %v", err)
	}
	proxyURL, err := url.Parse(grant.ProxyURL)
	if err != nil {
		t.Fatalf("parse proxy URL: %v", err)
	}
	client := &http.Client{Transport: &http.Transport{Proxy: http.ProxyURL(proxyURL)}}
	response, err := client.Get("http://business.example/fails")
	if err != nil {
		t.Fatalf("request through failed node: %v", err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusBadGateway {
		t.Fatalf("failed request status = %d", response.StatusCode)
	}
	deadline := time.Now().Add(time.Second)
	for {
		snapshot := runtime.Snapshot()
		if len(snapshot.RecentLeases) == 1 && string(snapshot.RecentLeases[0].State) == "BROKEN" && snapshot.Degraded {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("lease did not become Broken: active=%+v recent=%+v", snapshot.ActiveLeases, snapshot.RecentLeases)
		}
		time.Sleep(5 * time.Millisecond)
	}
	secondResponse, err := client.Get("http://business.example/again")
	if err != nil {
		t.Fatalf("request with Broken token: %v", err)
	}
	_ = secondResponse.Body.Close()
	if secondResponse.StatusCode != http.StatusProxyAuthRequired {
		t.Fatalf("Broken token status = %d, want 407", secondResponse.StatusCode)
	}
}

// TestGateway_DrainingLeaseBecomesBrokenWhenNodeIsBlocked 验证节点故障状态覆盖排空阶段：
// 已停止接收新连接的租约仍绑定真实节点，节点被全局阻断后必须转为 Broken。
func TestGateway_DrainingLeaseBecomesBrokenWhenNodeIsBlocked(t *testing.T) {
	requestStarted := make(chan struct{})
	releaseTarget := make(chan struct{})
	var releaseOnce sync.Once
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		close(requestStarted)
		<-releaseTarget
		_, _ = io.WriteString(w, "done")
	}))
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(releaseTarget) })
		target.Close()
	})
	targetURL, err := url.Parse(target.URL)
	if err != nil {
		t.Fatalf("parse target URL: %v", err)
	}
	gatewayListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen gateway: %v", err)
	}
	runtime, err := lease.NewRuntime(lease.Options{
		APIToken: "machine-token", ProxyURL: "http://" + gatewayListener.Addr().String(),
		Nodes: []lease.Node{{Key: "node-a", Dialer: connectNodeDialer{address: targetURL.Host}}},
	})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}
	t.Cleanup(func() { _ = runtime.Close() })
	gateway := &http.Server{Handler: runtime.GatewayHandler()}
	go func() { _ = gateway.Serve(gatewayListener) }()
	t.Cleanup(func() { _ = gateway.Shutdown(context.Background()) })
	grant, err := runtime.Acquire(context.Background(), "draining-then-broken")
	if err != nil {
		t.Fatalf("acquire lease: %v", err)
	}
	proxyURL, err := url.Parse(grant.ProxyURL)
	if err != nil {
		t.Fatalf("parse proxy URL: %v", err)
	}
	requestDone := make(chan error, 1)
	go func() {
		response, requestErr := (&http.Client{Transport: &http.Transport{Proxy: http.ProxyURL(proxyURL)}}).Get("http://business.example/blocked")
		if response != nil {
			_ = response.Body.Close()
		}
		requestDone <- requestErr
	}()
	select {
	case <-requestStarted:
	case <-time.After(time.Second):
		t.Fatal("proxy request did not reach target")
	}
	if err := runtime.Release(context.Background(), grant.LeaseToken); err != nil {
		t.Fatalf("release lease: %v", err)
	}
	if err := runtime.BlockNode(context.Background(), "node-a", "confirmed unavailable"); err != nil {
		t.Fatalf("block node: %v", err)
	}
	snapshot := runtime.Snapshot()
	becameBroken := len(snapshot.ActiveLeases) == 1 && snapshot.ActiveLeases[0].State == lease.LeaseStateBroken
	releaseOnce.Do(func() { close(releaseTarget) })
	select {
	case <-requestDone:
	case <-time.After(time.Second):
		t.Fatal("proxy request did not finish")
	}
	if !becameBroken {
		t.Fatalf("draining lease after node block = %+v", snapshot.ActiveLeases)
	}
}
