package lease

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"
)

type unavailableDialer struct{}

func (unavailableDialer) DialContext(context.Context, string, string) (net.Conn, error) {
	return nil, errors.New("not used")
}

type manualTimer struct {
	callback func()
	stopped  bool
}

func (t *manualTimer) Stop() bool {
	wasActive := !t.stopped
	t.stopped = true
	return wasActive
}

func (t *manualTimer) Fire() {
	if !t.stopped {
		t.stopped = true
		t.callback()
	}
}

// TestRuntime_LeaseTTLUsesInjectedTimer 验证 TTL 边界可以由外部时钟确定性推进，
// 不依赖真实 sleep，也不会在到期前释放 Node Key。
func TestRuntime_LeaseTTLUsesInjectedTimer(t *testing.T) {
	var timers []*manualTimer
	runtime, err := NewRuntime(Options{
		APIToken: "machine-token", ProxyURL: "http://127.0.0.1:2330", TTL: 2 * time.Minute,
		AfterFunc: func(_ time.Duration, callback func()) Timer {
			timer := &manualTimer{callback: callback}
			timers = append(timers, timer)
			return timer
		},
		Nodes: []Node{{Key: "node-a", Dialer: unavailableDialer{}}},
	})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}
	t.Cleanup(func() { _ = runtime.Close() })
	if _, err := runtime.Acquire(context.Background(), "controlled-ttl"); err != nil {
		t.Fatalf("acquire lease: %v", err)
	}
	if len(timers) != 1 || len(runtime.Snapshot().ActiveLeases) != 1 {
		t.Fatalf("timers=%d snapshot=%+v", len(timers), runtime.Snapshot())
	}
	timers[0].Fire()
	if len(runtime.Snapshot().ActiveLeases) != 0 || runtime.Snapshot().IdleNodeCount != 1 {
		t.Fatalf("snapshot after controlled TTL = %+v", runtime.Snapshot())
	}
}

// TestRuntime_ExpiresOldLeaseAndRetiresDrainingGeneration 验证调用方忘记释放时，
// TTL 到点也会自动回收占用并关闭已经排空的旧代际资源。
func TestRuntime_ExpiresOldLeaseAndRetiresDrainingGeneration(t *testing.T) {
	oldClosed := make(chan struct{})
	runtime, err := NewRuntime(Options{
		APIToken:     "machine-token",
		ProxyURL:     "http://127.0.0.1:2330",
		TTL:          40 * time.Millisecond,
		GenerationID: "generation-1",
		GenerationClose: func() error {
			close(oldClosed)
			return nil
		},
		Nodes: []Node{{Key: "old-node", Dialer: unavailableDialer{}}},
	})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}
	t.Cleanup(func() { _ = runtime.Close() })
	if _, err := runtime.Acquire(context.Background(), "expires"); err != nil {
		t.Fatalf("acquire old lease: %v", err)
	}
	validation := &ValidationRun{total: 1, minReady: 1}
	if err := runtime.BeginCandidate("generation-2", 1, validation); err != nil {
		t.Fatalf("begin candidate: %v", err)
	}
	if err := runtime.Promote(Candidate{
		ID: "generation-2", Nodes: []Node{{Key: "new-node", Dialer: unavailableDialer{}}},
	}, []Node{{Key: "new-node", Dialer: unavailableDialer{}}}, validation); err != nil {
		t.Fatalf("promote candidate: %v", err)
	}

	select {
	case <-oldClosed:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("draining generation was not retired at lease expiry")
	}
	snapshot := runtime.Snapshot()
	if len(snapshot.ActiveLeases) != 0 || len(snapshot.Generations) != 1 || snapshot.Generations[0].ID != "generation-2" {
		t.Fatalf("snapshot after lease expiry = %+v", snapshot)
	}
}

// TestRuntime_GenerationDrainTimeoutAutomaticallyForceClosesResidualLeases 验证代际排空硬边界：
// 到达 generation_drain_timeout 后无需管理员操作，也会关闭旧代际并记录受影响租约数。
func TestRuntime_GenerationDrainTimeoutAutomaticallyForceClosesResidualLeases(t *testing.T) {
	var timers []*manualTimer
	oldClosed := 0
	runtime, err := NewRuntime(Options{
		APIToken: "machine-token", ProxyURL: "http://127.0.0.1:2330", GenerationID: "generation-1",
		GenerationClose: func() error {
			oldClosed++
			return nil
		},
		AfterFunc: func(_ time.Duration, callback func()) Timer {
			timer := &manualTimer{callback: callback}
			timers = append(timers, timer)
			return timer
		},
		Nodes: []Node{{Key: "old-node", Dialer: unavailableDialer{}}},
	})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}
	t.Cleanup(func() { _ = runtime.Close() })
	grant, err := runtime.Acquire(context.Background(), "residual")
	if err != nil {
		t.Fatalf("acquire old lease: %v", err)
	}
	connection, admission := runtime.connectionForToken(grant.LeaseToken)
	if admission != connectionAdmissionAccepted {
		t.Fatalf("connection admission = %v", admission)
	}
	connectionClosed := 0
	if !connection.RegisterCloser(func() { connectionClosed++ }) {
		t.Fatal("register generation connection closer")
	}
	validation := &ValidationRun{total: 1, minReady: 1}
	if err := runtime.BeginCandidate("generation-2", 1, validation); err != nil {
		t.Fatalf("begin candidate: %v", err)
	}
	newNode := Node{Key: "new-node", Dialer: unavailableDialer{}}
	if err := runtime.Promote(Candidate{ID: "generation-2", Nodes: []Node{newNode}}, []Node{newNode}, validation); err != nil {
		t.Fatalf("promote candidate: %v", err)
	}
	if len(timers) != 2 {
		t.Fatalf("timer count = %d, want lease TTL and generation drain timers", len(timers))
	}
	timers[1].Fire()
	snapshot := runtime.Snapshot()
	if oldClosed != 1 || connectionClosed != 1 || len(snapshot.ActiveLeases) != 0 || len(snapshot.Generations) != 1 || snapshot.Generations[0].ID != "generation-2" {
		t.Fatalf("snapshot after generation timeout = %+v, oldClosed=%d connectionClosed=%d", snapshot, oldClosed, connectionClosed)
	}
	if snapshot.GatewayMetrics.ActiveConnections != 0 || len(snapshot.InvariantAlerts) != 0 {
		t.Fatalf("gateway state after generation timeout = %+v alerts=%v", snapshot.GatewayMetrics, snapshot.InvariantAlerts)
	}
	events := runtime.EventSnapshot(0, "GENERATION_DRAIN_TIMEOUT")
	if len(events) != 1 || events[0].AffectedLeases != 1 || events[0].AffectedConnections != 1 {
		t.Fatalf("generation timeout events = %+v", events)
	}
	connection.Release()
}

// TestRuntime_DoesNotAllocateSameNodeKeyAcrossGenerations 验证 Node Key 占用表跨代际共享，
// 旧租约释放前，新 Active 即使包含同一节点也不能把它再次出租。
func TestRuntime_DoesNotAllocateSameNodeKeyAcrossGenerations(t *testing.T) {
	runtime, err := NewRuntime(Options{
		APIToken:           "machine-token",
		ProxyURL:           "http://127.0.0.1:2330",
		AcquireWaitTimeout: 20 * time.Millisecond,
		GenerationID:       "generation-1",
		Nodes:              []Node{{Key: "shared-node", Dialer: unavailableDialer{}}},
	})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}
	t.Cleanup(func() { _ = runtime.Close() })
	oldGrant, err := runtime.Acquire(context.Background(), "old")
	if err != nil {
		t.Fatalf("acquire old lease: %v", err)
	}
	validation := &ValidationRun{total: 1, minReady: 1}
	if err := runtime.BeginCandidate("generation-2", 1, validation); err != nil {
		t.Fatalf("begin candidate: %v", err)
	}
	sharedNode := Node{Key: "shared-node", Dialer: unavailableDialer{}}
	if err := runtime.Promote(Candidate{ID: "generation-2", Nodes: []Node{sharedNode}}, []Node{sharedNode}, validation); err != nil {
		t.Fatalf("promote candidate: %v", err)
	}
	if _, err := runtime.Acquire(context.Background(), "must-wait"); !errors.Is(err, ErrAcquireTimeout) {
		t.Fatalf("acquire duplicate Node Key error = %v", err)
	}
	if err := runtime.Release(context.Background(), oldGrant.LeaseToken); err != nil {
		t.Fatalf("release old lease: %v", err)
	}
	newGrant, err := runtime.Acquire(context.Background(), "new")
	if err != nil {
		t.Fatalf("acquire after old release: %v", err)
	}
	if newGrant.GenerationID != "generation-2" || newGrant.NodeKey != "shared-node" {
		t.Fatalf("new grant = %+v", newGrant)
	}
}

// TestRuntime_AllowsSameNodeKeyAcrossGenerationsForDifferentConflictDomains 验证跨代际占用以冲突域为边界：
// 旧代际租约只阻止同域复用 Node Key，不阻止其他冲突域从新 Active 取得相同节点。
func TestRuntime_AllowsSameNodeKeyAcrossGenerationsForDifferentConflictDomains(t *testing.T) {
	runtime, err := NewRuntime(Options{
		APIToken:           "machine-token",
		ProxyURL:           "http://127.0.0.1:2330",
		AcquireWaitTimeout: 20 * time.Millisecond,
		GenerationID:       "generation-1",
		Nodes:              []Node{{Key: "shared-node", Dialer: unavailableDialer{}}},
	})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}
	t.Cleanup(func() { _ = runtime.Close() })
	oldGrant, err := runtime.AcquireLease(context.Background(), AcquireRequest{Label: "old", ConflictKey: "account:100"})
	if err != nil {
		t.Fatalf("acquire old lease: %v", err)
	}
	validation := &ValidationRun{total: 1, minReady: 1}
	if err := runtime.BeginCandidate("generation-2", 1, validation); err != nil {
		t.Fatalf("begin candidate: %v", err)
	}
	sharedNode := Node{Key: "shared-node", Dialer: unavailableDialer{}}
	if err := runtime.Promote(Candidate{ID: "generation-2", Nodes: []Node{sharedNode}}, []Node{sharedNode}, validation); err != nil {
		t.Fatalf("promote candidate: %v", err)
	}
	otherDomain, err := runtime.AcquireLease(context.Background(), AcquireRequest{Label: "other", ConflictKey: "account:200"})
	if err != nil || otherDomain.NodeKey != "shared-node" {
		t.Fatalf("acquire other conflict domain = %+v, error = %v", otherDomain, err)
	}
	if _, err := runtime.AcquireLease(context.Background(), AcquireRequest{Label: "same", ConflictKey: "account:100"}); !errors.Is(err, ErrAcquireTimeout) {
		t.Fatalf("same conflict domain duplicate Node Key error = %v", err)
	}
	if alerts := runtime.Snapshot().InvariantAlerts; len(alerts) != 0 {
		t.Fatalf("cross-domain sharing raised invariant alerts: %v", alerts)
	}
	if err := runtime.Release(context.Background(), oldGrant.LeaseToken); err != nil {
		t.Fatalf("release old lease: %v", err)
	}
	afterRelease, err := runtime.AcquireLease(context.Background(), AcquireRequest{Label: "same-after-release", ConflictKey: "account:100"})
	if err != nil || afterRelease.NodeKey != "shared-node" {
		t.Fatalf("acquire same domain after release = %+v, error = %v", afterRelease, err)
	}
}

// TestRuntime_BlockNodeBreaksLeasesAcrossConflictDomains 验证冲突域不隔离物理节点故障。
func TestRuntime_BlockNodeBreaksLeasesAcrossConflictDomains(t *testing.T) {
	runtime, err := NewRuntime(Options{
		APIToken: "machine-token", ProxyURL: "http://127.0.0.1:2330",
		Nodes: []Node{{Key: "shared-node", Dialer: unavailableDialer{}}},
	})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}
	t.Cleanup(func() { _ = runtime.Close() })
	for _, conflictKey := range []string{"account:100", "account:200"} {
		if _, err := runtime.AcquireLease(context.Background(), AcquireRequest{ConflictKey: conflictKey}); err != nil {
			t.Fatalf("acquire %s: %v", conflictKey, err)
		}
	}
	beforeBlock := runtime.Snapshot()
	if len(beforeBlock.Nodes) != 1 || beforeBlock.Nodes[0].ActiveLeaseCount != 2 {
		t.Fatalf("shared node lease count = %+v", beforeBlock.Nodes)
	}
	if err := runtime.BlockNode(context.Background(), "shared-node", "operator request"); err != nil {
		t.Fatalf("block shared node: %v", err)
	}
	snapshot := runtime.Snapshot()
	if len(snapshot.ActiveLeases) != 0 || len(snapshot.RecentLeases) != 2 {
		t.Fatalf("snapshot after shared node block = %+v", snapshot)
	}
	for _, recent := range snapshot.RecentLeases {
		if recent.State != LeaseStateBroken || recent.NodeKey != "shared-node" {
			t.Fatalf("recent lease after shared node block = %+v", recent)
		}
	}
}
