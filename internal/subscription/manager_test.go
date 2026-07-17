package subscription

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"easy_proxies/internal/config"
)

type recoveredLeaseCapacity struct {
	calls atomic.Int32
}

func (r *recoveredLeaseCapacity) RefreshLeaseGeneration(context.Context, *config.Config) error {
	return nil
}

func (r *recoveredLeaseCapacity) RecoverDegradedCapacity(context.Context) (bool, error) {
	r.calls.Add(1)
	return true, nil
}

// TestRefreshNow_ConcurrentCallersShareOneRefresh 验证刷新协调器的 single-flight 契约：
// 并发调用者共享同一次订阅抓取和最终结果，不重复构建运行时。
func TestRefreshNow_ConcurrentCallersShareOneRefresh(t *testing.T) {
	proxyServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(proxyServer.Close)
	requestStarted := make(chan struct{})
	releaseRequest := make(chan struct{})
	var fetchCount atomic.Int32
	subscriptionServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if fetchCount.Add(1) == 1 {
			close(requestStarted)
		}
		<-releaseRequest
		_, _ = w.Write([]byte(proxyServer.URL + "#subscription-node\n"))
	}))
	t.Cleanup(subscriptionServer.Close)

	cfg := &config.Config{
		Mode:       "pool",
		Listener:   config.ListenerConfig{Address: "127.0.0.1", Port: 2323},
		Pool:       config.PoolConfig{Mode: "sequential"},
		Management: config.ManagementConfig{ProbeTarget: proxyServer.URL},
		SubscriptionRefresh: config.SubscriptionRefreshConfig{
			Enabled: true, Interval: time.Hour, Timeout: time.Second,
			HealthCheckTimeout: time.Second, MinAvailableNodes: 1,
		},
		Subscriptions: []string{subscriptionServer.URL},
		Nodes:         []config.NodeConfig{{Name: "initial-node", URI: proxyServer.URL}},
		NodesFile:     filepath.Join(t.TempDir(), "nodes.txt"),
	}
	manager := New(cfg, subscriptionTestReloader{})
	manager.Start()
	t.Cleanup(manager.Stop)

	results := make(chan error, 2)
	go func() { results <- manager.RefreshNow() }()
	select {
	case <-requestStarted:
	case <-time.After(time.Second):
		t.Fatal("first subscription fetch did not start")
	}
	go func() { results <- manager.RefreshNow() }()
	time.Sleep(20 * time.Millisecond)
	close(releaseRequest)
	for i := 0; i < 2; i++ {
		if err := <-results; err != nil {
			t.Fatalf("RefreshNow caller %d: %v", i+1, err)
		}
	}
	if fetchCount.Load() != 1 {
		t.Fatalf("subscription fetch count = %d, want 1", fetchCount.Load())
	}
}

// TestDegradedRecovery_UsesCappedBackoffAndResetsAfterSuccess 验证降级恢复失败后递增退避，
// 成功刷新后清零退避状态。
func TestDegradedRecovery_UsesCappedBackoffAndResetsAfterSuccess(t *testing.T) {
	proxyServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(proxyServer.Close)
	var mu sync.Mutex
	var attempts []time.Time
	subscriptionServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		attempts = append(attempts, time.Now())
		attempt := len(attempts)
		mu.Unlock()
		if attempt < 3 {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		_, _ = w.Write([]byte(proxyServer.URL + "#recovered\n"))
	}))
	t.Cleanup(subscriptionServer.Close)
	cfg := &config.Config{
		Mode: "pool",
		SubscriptionRefresh: config.SubscriptionRefreshConfig{
			Timeout: 100 * time.Millisecond, HealthCheckTimeout: 100 * time.Millisecond,
		},
		Subscriptions: []string{subscriptionServer.URL},
		NodesFile:     filepath.Join(t.TempDir(), "nodes.txt"),
	}
	manager := New(cfg, subscriptionTestReloader{}, WithRecoveryBackoff([]time.Duration{
		20 * time.Millisecond, 40 * time.Millisecond, 80 * time.Millisecond,
	}))
	t.Cleanup(manager.Stop)
	manager.TriggerDegradedRecovery()
	deadline := time.Now().Add(time.Second)
	for {
		mu.Lock()
		count := len(attempts)
		attemptTimes := append([]time.Time(nil), attempts...)
		mu.Unlock()
		status := manager.Status()
		if count == 3 && status.LastError == "" && status.BackoffAttempt == 0 {
			if attemptTimes[1].Sub(attemptTimes[0]) < 15*time.Millisecond || attemptTimes[2].Sub(attemptTimes[1]) < 35*time.Millisecond {
				t.Fatalf("recovery intervals = %s, %s", attemptTimes[1].Sub(attemptTimes[0]), attemptTimes[2].Sub(attemptTimes[1]))
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("recovery did not succeed: attempts=%d status=%+v", count, status)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestDegradedRecovery_RechecksFailedNodesBeforeFetchingSubscriptions 验证容量复检优先级：
// 当前代际恢复到门槛后不得再抓取订阅或构建 Candidate。
func TestDegradedRecovery_RechecksFailedNodesBeforeFetchingSubscriptions(t *testing.T) {
	var subscriptionRequests atomic.Int32
	subscriptionServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		subscriptionRequests.Add(1)
		_, _ = w.Write([]byte("ss://unused"))
	}))
	t.Cleanup(subscriptionServer.Close)

	cfg := &config.Config{
		Subscriptions:       []string{subscriptionServer.URL},
		SubscriptionRefresh: config.SubscriptionRefreshConfig{Timeout: time.Second},
	}
	recoverer := &recoveredLeaseCapacity{}
	manager := New(cfg, subscriptionTestReloader{}, WithRecoveryBackoff([]time.Duration{time.Millisecond}))
	manager.SetLeaseGenerationRefresher(recoverer)
	t.Cleanup(manager.Stop)
	manager.TriggerDegradedRecovery()

	deadline := time.Now().Add(time.Second)
	for recoverer.calls.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if recoverer.calls.Load() != 1 {
		t.Fatalf("degraded recheck calls = %d", recoverer.calls.Load())
	}
	if subscriptionRequests.Load() != 0 {
		t.Fatalf("subscription requests after capacity recovered = %d", subscriptionRequests.Load())
	}
}

// TestRefreshCoordinator_CoalescesConfigChangesToLatestRevision 验证刷新进行中连续修改配置时，
// 当前修订完成后只执行最新修订，中间订阅地址不会被逐个重放。
func TestRefreshCoordinator_CoalescesConfigChangesToLatestRevision(t *testing.T) {
	proxyServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(proxyServer.Close)
	started := make(chan struct{})
	release := make(chan struct{})
	var firstRequests atomic.Int32
	first := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if firstRequests.Add(1) == 1 {
			close(started)
		}
		<-release
		_, _ = w.Write([]byte(proxyServer.URL + "#first\n"))
	}))
	t.Cleanup(first.Close)
	var middleRequests atomic.Int32
	middle := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		middleRequests.Add(1)
		_, _ = w.Write([]byte(proxyServer.URL + "#middle\n"))
	}))
	t.Cleanup(middle.Close)
	var latestRequests atomic.Int32
	latest := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		latestRequests.Add(1)
		_, _ = w.Write([]byte(proxyServer.URL + "#latest\n"))
	}))
	t.Cleanup(latest.Close)

	cfg := &config.Config{
		Mode: "pool", Subscriptions: []string{first.URL}, NodesFile: filepath.Join(t.TempDir(), "nodes.txt"),
		SubscriptionRefresh: config.SubscriptionRefreshConfig{Timeout: time.Second, HealthCheckTimeout: time.Second, Interval: time.Hour},
	}
	manager := New(cfg, subscriptionTestReloader{})
	t.Cleanup(manager.Stop)
	firstResult := make(chan error, 1)
	go func() { firstResult <- manager.RefreshNow() }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("initial refresh did not start")
	}
	manager.UpdateConfig([]string{middle.URL}, false, time.Hour)
	manager.UpdateConfig([]string{latest.URL}, false, time.Hour)
	close(release)
	if err := <-firstResult; err != nil {
		t.Fatalf("initial refresh: %v", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for latestRequests.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if firstRequests.Load() != 1 || middleRequests.Load() != 0 || latestRequests.Load() != 1 {
		t.Fatalf("refresh requests first=%d middle=%d latest=%d", firstRequests.Load(), middleRequests.Load(), latestRequests.Load())
	}
	if manager.Status().ConfigRevision != 3 {
		t.Fatalf("config revision = %d, want 3", manager.Status().ConfigRevision)
	}
}

type subscriptionTestReloader struct{}

func (subscriptionTestReloader) CurrentPortMap() map[string]uint16 { return nil }

func (subscriptionTestReloader) ReloadWithPortMap(*config.Config, map[string]uint16) error {
	return nil
}

func TestCreateNewConfig_PreservesInlineNodes(t *testing.T) {
	// Setup base config with inline nodes
	baseCfg := &config.Config{
		Mode: "pool",
		Nodes: []config.NodeConfig{
			{
				Name:   "inline-node-1",
				URI:    "ss://test1@example.com:8388",
				Source: config.NodeSourceInline,
			},
			{
				Name:   "inline-node-2",
				URI:    "ss://test2@example.com:8389",
				Source: config.NodeSourceInline,
			},
		},
	}

	mgr := &Manager{
		baseCfg: baseCfg,
	}

	// Subscription nodes
	subNodes := []config.NodeConfig{
		{Name: "sub-node-1", URI: "ss://sub1@example.com:8390"},
		{Name: "sub-node-2", URI: "ss://sub2@example.com:8391"},
	}

	// Create new config
	newCfg := mgr.createNewConfig(subNodes)

	// Verify inline nodes are preserved
	if len(newCfg.Nodes) != 4 {
		t.Fatalf("expected 4 nodes (2 inline + 2 subscription), got %d", len(newCfg.Nodes))
	}

	// Verify inline nodes come first
	if newCfg.Nodes[0].Name != "inline-node-1" {
		t.Errorf("expected first node to be inline-node-1, got %s", newCfg.Nodes[0].Name)
	}
	if newCfg.Nodes[0].Source != config.NodeSourceInline {
		t.Errorf("expected first node source to be inline, got %s", newCfg.Nodes[0].Source)
	}

	if newCfg.Nodes[1].Name != "inline-node-2" {
		t.Errorf("expected second node to be inline-node-2, got %s", newCfg.Nodes[1].Name)
	}
	if newCfg.Nodes[1].Source != config.NodeSourceInline {
		t.Errorf("expected second node source to be inline, got %s", newCfg.Nodes[1].Source)
	}

	// Verify subscription nodes come after inline nodes
	if newCfg.Nodes[2].Name != "sub-node-1" {
		t.Errorf("expected third node to be sub-node-1, got %s", newCfg.Nodes[2].Name)
	}
	if newCfg.Nodes[2].Source != config.NodeSourceSubscription {
		t.Errorf("expected third node source to be subscription, got %s", newCfg.Nodes[2].Source)
	}

	if newCfg.Nodes[3].Name != "sub-node-2" {
		t.Errorf("expected fourth node to be sub-node-2, got %s", newCfg.Nodes[3].Name)
	}
	if newCfg.Nodes[3].Source != config.NodeSourceSubscription {
		t.Errorf("expected fourth node source to be subscription, got %s", newCfg.Nodes[3].Source)
	}
}

func TestCreateNewConfig_OnlySubscriptionNodes(t *testing.T) {
	// Setup base config without inline nodes
	baseCfg := &config.Config{
		Mode:  "pool",
		Nodes: []config.NodeConfig{},
	}

	mgr := &Manager{
		baseCfg: baseCfg,
	}

	// Subscription nodes
	subNodes := []config.NodeConfig{
		{Name: "sub-node-1", URI: "ss://sub1@example.com:8390"},
		{Name: "sub-node-2", URI: "ss://sub2@example.com:8391"},
	}

	// Create new config
	newCfg := mgr.createNewConfig(subNodes)

	// Verify only subscription nodes exist
	if len(newCfg.Nodes) != 2 {
		t.Fatalf("expected 2 nodes, got %d", len(newCfg.Nodes))
	}

	for i, node := range newCfg.Nodes {
		if node.Source != config.NodeSourceSubscription {
			t.Errorf("node %d: expected source to be subscription, got %s", i, node.Source)
		}
	}
}

func TestCreateNewConfig_SubscriptionSourceMarked(t *testing.T) {
	baseCfg := &config.Config{
		Mode:  "pool",
		Nodes: []config.NodeConfig{},
	}

	mgr := &Manager{
		baseCfg: baseCfg,
	}

	// Subscription nodes without source set
	subNodes := []config.NodeConfig{
		{Name: "sub-node-1", URI: "ss://sub1@example.com:8390"},
	}

	newCfg := mgr.createNewConfig(subNodes)

	// Verify source is set to subscription
	if newCfg.Nodes[0].Source != config.NodeSourceSubscription {
		t.Errorf("expected source to be subscription, got %s", newCfg.Nodes[0].Source)
	}
}
