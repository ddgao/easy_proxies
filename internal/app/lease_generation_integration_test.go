package app

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"easy_proxies/internal/config"
	"easy_proxies/internal/lease"
	"easy_proxies/internal/monitor"
)

// TestLeaseGenerationPromotion_KeepsOldLeaseAndRoutesNewLeaseToCandidate 验证代际提升的外部契约：
// 已签发 Lease Token 固定在旧代际，新申请只进入提升后的 Active Generation。
func TestLeaseGenerationPromotion_KeepsOldLeaseAndRoutesNewLeaseToCandidate(t *testing.T) {
	newGenerationUpstream := func(body string) (*httptest.Server, *url.URL) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/health/ready" {
				w.WriteHeader(http.StatusNoContent)
				return
			}
			_, _ = io.WriteString(w, body)
		}))
		parsed, err := url.Parse(server.URL)
		if err != nil {
			server.Close()
			t.Fatalf("parse upstream URL: %v", err)
		}
		return server, parsed
	}
	oldServer, oldURL := newGenerationUpstream("old-generation")
	t.Cleanup(oldServer.Close)
	newServer, newURL := newGenerationUpstream("new-generation")
	t.Cleanup(newServer.Close)

	cfg := &config.Config{
		Mode:       "pool",
		Listener:   config.ListenerConfig{Address: "127.0.0.1", Port: reservePort(t)},
		Management: config.ManagementConfig{ProbeTarget: "http://probe.example/health/ready"},
		LeaseGateway: config.LeaseGatewayConfig{
			Enabled:             true,
			Listen:              "127.0.0.1",
			Port:                reservePort(t),
			APIToken:            "machine-token",
			MinReadyNodes:       1,
			ProbeExpectedStatus: http.StatusNoContent,
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
		Key: "old-node", Dialer: configuredNodeDialer{address: oldURL.Host},
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

	oldGrant := acquireGenerationLease(t, controlServer.URL, "old-task")
	if err := service.Promote(ctx, lease.Candidate{
		ID: "generation-2",
		Nodes: []lease.Node{{
			Key: "new-node", Dialer: configuredNodeDialer{address: newURL.Host},
		}},
	}); err != nil {
		t.Fatalf("promote candidate: %v", err)
	}
	newGrant := acquireGenerationLease(t, controlServer.URL, "new-task")
	snapshot := readGenerationSnapshot(t, controlServer.URL)
	var draining *lease.GenerationSummary
	for index := range snapshot.Generations {
		if snapshot.Generations[index].Role == lease.GenerationRoleDraining {
			draining = &snapshot.Generations[index]
			break
		}
	}
	if draining == nil || draining.RetiredNodeCount != 1 || draining.RemainingLeases != 1 || draining.DrainDeadline.IsZero() {
		t.Fatalf("draining generation summary = %+v", draining)
	}

	if oldGrant.GenerationID == newGrant.GenerationID {
		t.Fatalf("old and new leases share generation %q", oldGrant.GenerationID)
	}
	if got := requestThroughLease(t, oldGrant.ProxyURL); got != "old-generation" {
		t.Fatalf("old lease response = %q", got)
	}
	if got := requestThroughLease(t, newGrant.ProxyURL); got != "new-generation" {
		t.Fatalf("new lease response = %q", got)
	}
}

// TestLeaseGenerationPromotion_RevalidatesNodeThatReappearsAfterRemoval 验证退休节点重新出现的准入边界：
// 相同 Node Key 在后续代际重新出现时必须再次访问探测目标，不能复用旧代际健康状态。
func TestLeaseGenerationPromotion_RevalidatesNodeThatReappearsAfterRemoval(t *testing.T) {
	var returningProbeCount atomic.Int32
	returningServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health/ready" {
			returningProbeCount.Add(1)
			w.WriteHeader(http.StatusNoContent)
			return
		}
		_, _ = io.WriteString(w, "returning")
	}))
	t.Cleanup(returningServer.Close)
	returningURL, err := url.Parse(returningServer.URL)
	if err != nil {
		t.Fatalf("parse returning upstream: %v", err)
	}
	otherServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health/ready" {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		_, _ = io.WriteString(w, "other")
	}))
	t.Cleanup(otherServer.Close)
	otherURL, err := url.Parse(otherServer.URL)
	if err != nil {
		t.Fatalf("parse other upstream: %v", err)
	}
	cfg := generationTestConfig(t)
	management := generationManagementServer(t)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	service, err := startLeaseService(ctx, cfg, management, []lease.Node{{
		Key: "returning-node", Dialer: configuredNodeDialer{address: returningURL.Host},
	}})
	if err != nil {
		t.Fatalf("start lease service: %v", err)
	}
	waitLeaseReady(t, service)
	t.Cleanup(func() { _ = service.Close(context.Background()) })
	initialProbeCount := returningProbeCount.Load()
	if initialProbeCount == 0 {
		t.Fatal("returning node was not validated in initial generation")
	}
	if err := service.Promote(ctx, lease.Candidate{
		ID: "generation-2", Nodes: []lease.Node{{Key: "other-node", Dialer: configuredNodeDialer{address: otherURL.Host}}},
	}); err != nil {
		t.Fatalf("promote generation without returning node: %v", err)
	}
	if err := service.Promote(ctx, lease.Candidate{
		ID: "generation-3", Nodes: []lease.Node{{Key: "returning-node", Dialer: configuredNodeDialer{address: returningURL.Host}}},
	}); err != nil {
		t.Fatalf("promote generation with returning node: %v", err)
	}
	if got := returningProbeCount.Load(); got <= initialProbeCount {
		t.Fatalf("returning node probe count = %d, want more than initial %d", got, initialProbeCount)
	}
	snapshot := service.runtime.Snapshot()
	if len(snapshot.Generations) != 1 || snapshot.Generations[0].ID != "generation-3" || snapshot.Generations[0].Role != lease.GenerationRoleActive {
		t.Fatalf("generation after returning node promotion = %+v", snapshot.Generations)
	}
}

// TestLeaseGenerationPromotion_ExposesCandidateWhileValidationIsRunning 验证候选代在探测期间可观测，
// 管理员能够区分正在服务的 Active 与尚未准入的 Candidate。
func TestLeaseGenerationPromotion_ExposesCandidateWhileValidationIsRunning(t *testing.T) {
	oldServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(oldServer.Close)
	oldURL, err := url.Parse(oldServer.URL)
	if err != nil {
		t.Fatalf("parse old URL: %v", err)
	}

	probeStarted := make(chan struct{})
	releaseProbe := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-releaseProbe:
		default:
			close(releaseProbe)
		}
	})
	var startOnce sync.Once
	candidateServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		startOnce.Do(func() { close(probeStarted) })
		<-releaseProbe
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(candidateServer.Close)
	candidateURL, err := url.Parse(candidateServer.URL)
	if err != nil {
		t.Fatalf("parse candidate URL: %v", err)
	}

	cfg := generationTestConfig(t)
	management := generationManagementServer(t)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	service, err := startLeaseService(ctx, cfg, management, []lease.Node{{
		Key: "old-node", Dialer: configuredNodeDialer{address: oldURL.Host},
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

	promoteDone := make(chan error, 1)
	go func() {
		promoteDone <- service.Promote(ctx, lease.Candidate{
			ID: "generation-2",
			Nodes: []lease.Node{{
				Key: "candidate-node", Dialer: configuredNodeDialer{address: candidateURL.Host},
			}},
		})
	}()
	select {
	case <-probeStarted:
	case <-time.After(time.Second):
		t.Fatal("candidate validation did not start")
	}

	snapshot := readGenerationSnapshot(t, controlServer.URL)
	roles := make(map[lease.GenerationRole]string)
	for _, generation := range snapshot.Generations {
		roles[generation.Role] = generation.ID
		if generation.Role == lease.GenerationRoleCandidate && generation.BuildPhase != "PREFLIGHT" {
			t.Fatalf("candidate build phase = %q", generation.BuildPhase)
		}
	}
	if roles[lease.GenerationRoleActive] != "generation-1" || roles[lease.GenerationRoleCandidate] != "generation-2" {
		t.Fatalf("generation roles during validation = %+v", snapshot.Generations)
	}
	for _, node := range snapshot.Nodes {
		if node.GenerationID == "generation-2" && node.NodeKey == "candidate-node" && node.Ready {
			t.Fatalf("validating candidate node was reported ready: %+v", node)
		}
	}

	close(releaseProbe)
	if err := <-promoteDone; err != nil {
		t.Fatalf("promote candidate: %v", err)
	}
}

// TestLeaseGenerationPromotion_FailedCandidateKeepsActiveLease 验证候选代未达到健康门槛时，
// 只销毁候选资源，当前 Active 的既有租约和代理链路继续可用。
func TestLeaseGenerationPromotion_FailedCandidateKeepsActiveLease(t *testing.T) {
	oldServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health/ready" {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		_, _ = io.WriteString(w, "still-active")
	}))
	t.Cleanup(oldServer.Close)
	oldURL, err := url.Parse(oldServer.URL)
	if err != nil {
		t.Fatalf("parse old URL: %v", err)
	}
	failedServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	t.Cleanup(failedServer.Close)
	failedURL, err := url.Parse(failedServer.URL)
	if err != nil {
		t.Fatalf("parse failed URL: %v", err)
	}

	cfg := generationTestConfig(t)
	management := generationManagementServer(t)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	service, err := startLeaseService(ctx, cfg, management, []lease.Node{{
		Key: "old-node", Dialer: configuredNodeDialer{address: oldURL.Host},
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
	oldGrant := acquireGenerationLease(t, controlServer.URL, "active-task")

	var candidateCloseCount atomic.Int32
	promoteCtx, promoteCancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer promoteCancel()
	err = service.Promote(promoteCtx, lease.Candidate{
		ID: "generation-2",
		Nodes: []lease.Node{{
			Key: "failed-node", Dialer: configuredNodeDialer{address: failedURL.Host},
		}},
		Close: func() error {
			candidateCloseCount.Add(1)
			return nil
		},
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("promote error = %v, want deadline exceeded", err)
	}
	if candidateCloseCount.Load() != 1 {
		t.Fatalf("candidate close count = %d, want 1", candidateCloseCount.Load())
	}
	if got := requestThroughLease(t, oldGrant.ProxyURL); got != "still-active" {
		t.Fatalf("old lease response after failed candidate = %q", got)
	}
	snapshot := readGenerationSnapshot(t, controlServer.URL)
	if len(snapshot.Generations) != 1 || snapshot.Generations[0].ID != "generation-1" || snapshot.Generations[0].Role != lease.GenerationRoleActive {
		t.Fatalf("generations after failed candidate = %+v", snapshot.Generations)
	}
}

// TestLeaseGenerationRefresh_UsesLatestPendingSnapshotAfterDraining 验证两代上限期间刷新只暂存，
// Draining 退出后自动从最新配置构建一个 Candidate，不创建第三个并存代际。
func TestLeaseGenerationRefresh_UsesLatestPendingSnapshotAfterDraining(t *testing.T) {
	newUpstream := func(body string) (*httptest.Server, *url.URL) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/health/ready" {
				w.WriteHeader(http.StatusNoContent)
				return
			}
			_, _ = io.WriteString(w, body)
		}))
		parsed, err := url.Parse(server.URL)
		if err != nil {
			server.Close()
			t.Fatalf("parse upstream URL: %v", err)
		}
		return server, parsed
	}
	oldServer, oldURL := newUpstream("generation-1")
	t.Cleanup(oldServer.Close)
	activeServer, activeURL := newUpstream("generation-2")
	t.Cleanup(activeServer.Close)
	pendingServer, pendingURL := newUpstream("generation-3")
	t.Cleanup(pendingServer.Close)

	cfg := generationTestConfig(t)
	management := generationManagementServer(t)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	service, err := startLeaseService(ctx, cfg, management, []lease.Node{{
		Key: "old-node", Dialer: configuredNodeDialer{address: oldURL.Host},
	}})
	if err != nil {
		t.Fatalf("start lease service: %v", err)
	}
	waitLeaseReady(t, service)
	t.Cleanup(func() { _ = service.Close(context.Background()) })
	controlServer := httptest.NewServer(management.Handler())
	t.Cleanup(controlServer.Close)
	oldGrant := acquireGenerationLease(t, controlServer.URL, "old")
	if err := service.Promote(ctx, lease.Candidate{
		ID:    "generation-2",
		Nodes: []lease.Node{{Key: "active-node", Dialer: configuredNodeDialer{address: activeURL.Host}}},
	}); err != nil {
		t.Fatalf("promote generation-2: %v", err)
	}
	service.buildCandidate = func(_ context.Context, _ *config.Config, generationID string) (lease.Candidate, error) {
		return lease.Candidate{
			ID:    generationID,
			Nodes: []lease.Node{{Key: "pending-node", Dialer: configuredNodeDialer{address: pendingURL.Host}}},
		}, nil
	}
	if err := service.RefreshLeaseGeneration(ctx, cfg); err != nil {
		t.Fatalf("queue pending refresh: %v", err)
	}
	snapshot := readGenerationSnapshot(t, controlServer.URL)
	if len(snapshot.Generations) != 2 || !snapshot.PendingRefresh {
		t.Fatalf("snapshot while draining = %+v", snapshot)
	}
	releaseGenerationLease(t, controlServer.URL, oldGrant.LeaseToken)
	deadline := time.Now().Add(2 * time.Second)
	for {
		snapshot = readGenerationSnapshot(t, controlServer.URL)
		if len(snapshot.Generations) == 1 && snapshot.Generations[0].ID == "generation-3" && snapshot.Generations[0].Role == lease.GenerationRoleActive {
			if snapshot.PendingRefresh {
				t.Fatalf("pending refresh remained set after promotion: %+v", snapshot)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("pending generation was not promoted: %+v", snapshot.Generations)
		}
		time.Sleep(10 * time.Millisecond)
	}
	grant := acquireGenerationLease(t, controlServer.URL, "after-drain")
	if got := requestThroughLease(t, grant.ProxyURL); got != "generation-3" {
		t.Fatalf("pending generation response = %q", got)
	}
}

func releaseGenerationLease(t *testing.T, controlURL, leaseToken string) {
	t.Helper()
	request, err := http.NewRequest(http.MethodDelete, controlURL+"/api/proxy-leases", nil)
	if err != nil {
		t.Fatalf("new release request: %v", err)
	}
	request.Header.Set("Authorization", "Bearer machine-token")
	request.Header.Set("X-Lease-Token", leaseToken)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("release lease: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("release status = %d", response.StatusCode)
	}
}

func generationTestConfig(t *testing.T) *config.Config {
	t.Helper()
	cfg := &config.Config{
		Mode:       "pool",
		Listener:   config.ListenerConfig{Address: "127.0.0.1", Port: reservePort(t)},
		Management: config.ManagementConfig{ProbeTarget: "http://probe.example/health/ready"},
		LeaseGateway: config.LeaseGatewayConfig{
			Enabled:             true,
			Listen:              "127.0.0.1",
			Port:                reservePort(t),
			APIToken:            "machine-token",
			MinReadyNodes:       1,
			ProbeExpectedStatus: http.StatusNoContent,
		},
		Nodes: []config.NodeConfig{{Name: "placeholder", URI: "http://127.0.0.1:18080"}},
	}
	if err := cfg.NormalizeWithPortMap(nil); err != nil {
		t.Fatalf("normalize config: %v", err)
	}
	return cfg
}

func generationManagementServer(t *testing.T) *monitor.Server {
	t.Helper()
	monitorManager, err := monitor.NewManager(monitor.Config{})
	if err != nil {
		t.Fatalf("new monitor manager: %v", err)
	}
	return monitor.NewServer(monitor.Config{Enabled: true}, monitorManager, log.Default())
}

func readGenerationSnapshot(t *testing.T, controlURL string) lease.Snapshot {
	t.Helper()
	response, err := http.Get(controlURL + "/api/proxy-runtime")
	if err != nil {
		t.Fatalf("get runtime snapshot: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("runtime snapshot status = %d", response.StatusCode)
	}
	var snapshot lease.Snapshot
	if err := json.NewDecoder(response.Body).Decode(&snapshot); err != nil {
		t.Fatalf("decode runtime snapshot: %v", err)
	}
	return snapshot
}

func acquireGenerationLease(t *testing.T, controlURL, label string) lease.Grant {
	t.Helper()
	request, err := http.NewRequest(http.MethodPost, controlURL+"/api/proxy-leases", strings.NewReader(`{"label":"`+label+`"}`))
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
	return grant
}

func requestThroughLease(t *testing.T, rawProxyURL string) string {
	return requestURLThroughLease(t, rawProxyURL, "http://business.example/resource")
}

func requestURLThroughLease(t *testing.T, rawProxyURL, targetURL string) string {
	t.Helper()
	proxyURL, err := url.Parse(rawProxyURL)
	if err != nil {
		t.Fatalf("parse proxy URL: %v", err)
	}
	client := &http.Client{Transport: &http.Transport{Proxy: http.ProxyURL(proxyURL)}}
	response, err := client.Get(targetURL)
	if err != nil {
		t.Fatalf("request through lease: %v", err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("read lease response: %v", err)
	}
	return string(body)
}
