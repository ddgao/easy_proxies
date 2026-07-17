package monitor

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"easy_proxies/internal/lease"
)

// TestHealthEndpoints_DistinguishLivenessFromLeaseReadiness 验证健康端点不会把进程存活
// 与 Lease Gateway 可分配容量混为一谈。
func TestHealthEndpoints_DistinguishLivenessFromLeaseReadiness(t *testing.T) {
	manager, err := NewManager(Config{})
	if err != nil {
		t.Fatalf("new monitor manager: %v", err)
	}
	server := NewServer(Config{Enabled: true, Password: "web-password"}, manager, log.Default())
	httpServer := httptest.NewServer(server.Handler())
	t.Cleanup(httpServer.Close)

	liveResponse, err := http.Get(httpServer.URL + "/health/live")
	if err != nil {
		t.Fatalf("get liveness: %v", err)
	}
	_ = liveResponse.Body.Close()
	if liveResponse.StatusCode != http.StatusOK {
		t.Fatalf("liveness status = %d", liveResponse.StatusCode)
	}
	readyResponse, err := http.Get(httpServer.URL + "/health/ready")
	if err != nil {
		t.Fatalf("get readiness: %v", err)
	}
	var notReady struct {
		Ready bool `json:"ready"`
	}
	decodeErr := json.NewDecoder(readyResponse.Body).Decode(&notReady)
	_ = readyResponse.Body.Close()
	if decodeErr != nil {
		t.Fatalf("decode readiness: %v", decodeErr)
	}
	if readyResponse.StatusCode != http.StatusServiceUnavailable || notReady.Ready {
		t.Fatalf("readiness without runtime status=%d body=%+v", readyResponse.StatusCode, notReady)
	}

	runtime, err := lease.NewRuntime(lease.Options{
		APIToken: "machine-token", ProxyURL: "http://127.0.0.1:2330",
		Nodes: []lease.Node{{Key: "node-a", Dialer: fixedNodeDialer{address: "127.0.0.1:1"}}},
	})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}
	t.Cleanup(func() { _ = runtime.Close() })
	server.SetLeaseController(runtime)
	readyResponse, err = http.Get(httpServer.URL + "/health/ready")
	if err != nil {
		t.Fatalf("get ready runtime: %v", err)
	}
	defer readyResponse.Body.Close()
	if readyResponse.StatusCode != http.StatusOK {
		t.Fatalf("ready runtime status = %d", readyResponse.StatusCode)
	}
	_ = readyResponse.Body.Close()
	holder, err := runtime.Acquire(context.Background(), "default-holder")
	if err != nil {
		t.Fatalf("acquire default holder: %v", err)
	}
	readyResponse, err = http.Get(httpServer.URL + "/health/ready")
	if err != nil {
		t.Fatalf("get readiness while default domain is occupied: %v", err)
	}
	_ = readyResponse.Body.Close()
	if readyResponse.StatusCode != http.StatusOK {
		t.Fatalf("readiness while another conflict domain remains allocatable = %d", readyResponse.StatusCode)
	}
	if err := runtime.Release(context.Background(), holder.LeaseToken); err != nil {
		t.Fatalf("release default holder: %v", err)
	}
	if err := runtime.BlockNode(context.Background(), "node-a", "health regression"); err != nil {
		t.Fatalf("block only node: %v", err)
	}
	readyResponse, err = http.Get(httpServer.URL + "/health/ready")
	if err != nil {
		t.Fatalf("get readiness after block: %v", err)
	}
	defer readyResponse.Body.Close()
	if readyResponse.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("readiness after blocking only node = %d", readyResponse.StatusCode)
	}
}

type fixedSubscriptionRefresher struct {
	status SubscriptionStatus
}

func (f fixedSubscriptionRefresher) RefreshNow() error                          { return nil }
func (f fixedSubscriptionRefresher) Status() SubscriptionStatus                 { return f.status }
func (f fixedSubscriptionRefresher) UpdateConfig([]string, bool, time.Duration) {}
func (f fixedSubscriptionRefresher) UpdateConfigAndRefresh([]string, bool, time.Duration) error {
	return nil
}

// TestProxyRuntime_ContainsProcessAndRefreshState 验证统一快照直接包含进程、租约和刷新状态，
// WebUI 不需要拼接多个非同时刻接口来推导是否可以安全分配。
func TestProxyRuntime_ContainsProcessAndRefreshState(t *testing.T) {
	controlURL, runtime := newAllocatorControlServer(t, []string{"node-a"})
	manager, err := NewManager(Config{})
	if err != nil {
		t.Fatalf("new monitor manager: %v", err)
	}
	server := NewServer(Config{Enabled: true}, manager, log.Default())
	server.SetLeaseController(runtime)
	server.SetSubscriptionRefresher(fixedSubscriptionRefresher{status: SubscriptionStatus{
		Phase: "FETCHING", TriggerReason: "manual", ConfigRevision: 7, SharedWaiters: 2,
	}})
	httpServer := httptest.NewServer(server.Handler())
	t.Cleanup(httpServer.Close)
	_ = controlURL

	response, err := http.Get(httpServer.URL + "/api/proxy-runtime")
	if err != nil {
		t.Fatalf("get unified runtime: %v", err)
	}
	defer response.Body.Close()
	var snapshot lease.Snapshot
	if err := json.NewDecoder(response.Body).Decode(&snapshot); err != nil {
		t.Fatalf("decode unified runtime: %v", err)
	}
	if !snapshot.Live || snapshot.Refresh.Phase != "FETCHING" || snapshot.Refresh.TriggerReason != "manual" || snapshot.Refresh.ConfigRevision != 7 || snapshot.Refresh.SharedWaiters != 2 {
		t.Fatalf("unified runtime snapshot = %+v", snapshot)
	}
}

// TestProxyEvents_StreamsMonotonicLeaseEvents 验证 SSE 事件具有单调序号，
// 客户端可以从公开事件边界观察租约创建。
func TestProxyEvents_StreamsMonotonicLeaseEvents(t *testing.T) {
	controlURL, _ := newAllocatorControlServer(t, []string{"node-a"})
	request, err := http.NewRequest(http.MethodGet, controlURL+"/api/proxy-events?after=0", nil)
	if err != nil {
		t.Fatalf("new event request: %v", err)
	}
	ctx, cancel := context.WithCancel(request.Context())
	defer cancel()
	response, err := http.DefaultClient.Do(request.WithContext(ctx))
	if err != nil {
		t.Fatalf("open event stream: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK || response.Header.Get("Content-Type") != "text/event-stream" {
		t.Fatalf("event stream status=%d content-type=%q", response.StatusCode, response.Header.Get("Content-Type"))
	}
	_ = acquireLeaseThroughAPI(t, controlURL, "event-test")
	eventResult := make(chan lease.RuntimeEvent, 1)
	go func() {
		scanner := bufio.NewScanner(response.Body)
		for scanner.Scan() {
			line := scanner.Text()
			if !strings.HasPrefix(line, "data: ") {
				continue
			}
			var event lease.RuntimeEvent
			if json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &event) == nil {
				eventResult <- event
				return
			}
		}
	}()
	select {
	case event := <-eventResult:
		if event.Sequence != 1 || event.Type != "LEASE_ACQUIRED" {
			t.Fatalf("first runtime event = %+v", event)
		}
	case <-time.After(time.Second):
		t.Fatal("lease event was not streamed")
	}
}

// TestProxyRuntime_PaginatesLeasesInStableOrder 验证租约视图由服务端分页，
// 并保持分配顺序稳定，避免 map 遍历导致页面跳动。
func TestProxyRuntime_PaginatesLeasesInStableOrder(t *testing.T) {
	controlURL, _ := newAllocatorControlServer(t, []string{"node-a", "node-b", "node-c"})
	_ = acquireLeaseThroughAPI(t, controlURL, "first")
	_ = acquireLeaseThroughAPI(t, controlURL, "second")
	_ = acquireLeaseThroughAPI(t, controlURL, "third")
	response, err := http.Get(controlURL + "/api/proxy-runtime?lease_state=ACTIVE&limit=2")
	if err != nil {
		t.Fatalf("get paginated runtime: %v", err)
	}
	defer response.Body.Close()
	var page struct {
		ActiveLeases []struct {
			Label string `json:"label"`
		} `json:"active_leases"`
		LeaseNextCursor string `json:"lease_next_cursor"`
	}
	if err := json.NewDecoder(response.Body).Decode(&page); err != nil {
		t.Fatalf("decode runtime page: %v", err)
	}
	if len(page.ActiveLeases) != 2 || page.ActiveLeases[0].Label != "first" || page.ActiveLeases[1].Label != "second" || page.LeaseNextCursor == "" {
		t.Fatalf("runtime page = %+v", page)
	}
}

// TestProxyRuntime_PaginatesAndFiltersNodes 验证节点视图使用 Generation ID 与 Node Key
// 作为稳定身份，并由服务端按业务状态筛选和分页。
func TestProxyRuntime_PaginatesAndFiltersNodes(t *testing.T) {
	controlURL, _ := newAllocatorControlServer(t, []string{"node-c", "node-a", "node-b"})
	response, err := http.Get(controlURL + "/api/proxy-runtime?node_state=idle&node_limit=2")
	if err != nil {
		t.Fatalf("get paginated nodes: %v", err)
	}
	defer response.Body.Close()
	var page struct {
		Nodes          []lease.NodeSummary `json:"lease_nodes"`
		NodeNextCursor string              `json:"node_next_cursor"`
	}
	if err := json.NewDecoder(response.Body).Decode(&page); err != nil {
		t.Fatalf("decode node page: %v", err)
	}
	if len(page.Nodes) != 2 || page.Nodes[0].NodeKey != "node-a" || page.Nodes[1].NodeKey != "node-b" || page.NodeNextCursor == "" {
		t.Fatalf("node page = %+v", page)
	}
}

// TestProxyEvents_ExpiredCursorRequiresSnapshotResync 验证事件保留窗口已经越过客户端游标时，
// 服务端明确要求先读取原子快照，避免客户端把缺失事件误认为连续时间线。
func TestProxyEvents_ExpiredCursorRequiresSnapshotResync(t *testing.T) {
	controlURL, runtime := newAllocatorControlServer(t, []string{"node-a"})
	for index := 0; index < 501; index++ {
		if err := runtime.PauseAllocation(context.Background(), "fill event window"); err != nil {
			t.Fatalf("pause allocation: %v", err)
		}
		if err := runtime.ResumeAllocation(context.Background(), "fill event window"); err != nil {
			t.Fatalf("resume allocation: %v", err)
		}
	}

	request, err := http.NewRequest(http.MethodGet, controlURL+"/api/proxy-events?after=1", nil)
	if err != nil {
		t.Fatalf("new expired cursor request: %v", err)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("get expired cursor: %v", err)
	}
	defer response.Body.Close()
	var failure struct {
		Code   string `json:"code"`
		Oldest uint64 `json:"oldest_sequence"`
		Latest uint64 `json:"latest_sequence"`
	}
	if err := json.NewDecoder(response.Body).Decode(&failure); err != nil {
		t.Fatalf("decode expired cursor response: %v", err)
	}
	if response.StatusCode != http.StatusConflict || failure.Code != "EVENT_CURSOR_EXPIRED" || failure.Oldest <= 1 || failure.Latest < failure.Oldest {
		t.Fatalf("expired cursor status=%d failure=%+v", response.StatusCode, failure)
	}

	snapshotResponse, err := http.Get(controlURL + "/api/proxy-runtime")
	if err != nil {
		t.Fatalf("get resync snapshot: %v", err)
	}
	defer snapshotResponse.Body.Close()
	var snapshot lease.Snapshot
	if err := json.NewDecoder(snapshotResponse.Body).Decode(&snapshot); err != nil {
		t.Fatalf("decode resync snapshot: %v", err)
	}
	if snapshot.EventOldestSequence != failure.Oldest || snapshot.EventLatestSequence != failure.Latest {
		t.Fatalf("snapshot event bounds=%d..%d, failure=%d..%d", snapshot.EventOldestSequence, snapshot.EventLatestSequence, failure.Oldest, failure.Latest)
	}
}

// TestProxyAdmin_PauseRequiresConfirmationAndBlocksOnlyNewLeases 验证高影响操作的确认边界，
// 暂停期间已有租约保留，新申请收到稳定维护状态，恢复后可继续申请。
func TestProxyAdmin_PauseRequiresConfirmationAndBlocksOnlyNewLeases(t *testing.T) {
	controlURL, _ := newAllocatorControlServer(t, []string{"node-a", "node-b"})
	_ = acquireLeaseThroughAPI(t, controlURL, "existing")
	status := postAdminAction(t, controlURL, "/api/proxy-admin/pause", map[string]any{
		"reason": "maintenance", "confirm": false,
	})
	if status != http.StatusBadRequest {
		t.Fatalf("unconfirmed pause status = %d", status)
	}
	auditResponse, err := http.Get(controlURL + "/api/proxy-audit?type=ADMIN_ALLOCATION_PAUSE_REJECTED")
	if err != nil {
		t.Fatalf("get rejected pause audit: %v", err)
	}
	var rejectedAudit struct {
		Events []lease.RuntimeEvent `json:"events"`
	}
	decodeErr := json.NewDecoder(auditResponse.Body).Decode(&rejectedAudit)
	_ = auditResponse.Body.Close()
	if decodeErr != nil || len(rejectedAudit.Events) != 1 || rejectedAudit.Events[0].Result != "rejected" || rejectedAudit.Events[0].Error == "" {
		t.Fatalf("rejected pause audit=%+v error=%v", rejectedAudit, decodeErr)
	}
	status = postAdminAction(t, controlURL, "/api/proxy-admin/pause", map[string]any{
		"reason": "maintenance", "confirm": true,
	})
	if status != http.StatusOK {
		t.Fatalf("confirmed pause status = %d", status)
	}
	payload, _ := json.Marshal(map[string]string{"label": "paused"})
	request, _ := http.NewRequest(http.MethodPost, controlURL+"/api/proxy-leases", bytes.NewReader(payload))
	request.Header.Set("Authorization", "Bearer machine-token")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("acquire while paused: %v", err)
	}
	var failure struct {
		Code string `json:"code"`
	}
	decodeErr = json.NewDecoder(response.Body).Decode(&failure)
	_ = response.Body.Close()
	if decodeErr != nil || response.StatusCode != http.StatusServiceUnavailable || failure.Code != "ALLOCATION_PAUSED" {
		t.Fatalf("paused acquire status=%d failure=%+v error=%v", response.StatusCode, failure, decodeErr)
	}
	status = postAdminAction(t, controlURL, "/api/proxy-admin/resume", map[string]any{
		"reason": "maintenance complete", "confirm": true,
	})
	if status != http.StatusOK {
		t.Fatalf("resume status = %d", status)
	}
	grant := acquireLeaseThroughAPI(t, controlURL, "after-resume")
	if grant.NodeKey != "node-b" {
		t.Fatalf("Node Key after resume = %q", grant.NodeKey)
	}
}

// TestProxyAdmin_RevokeLeaseByFingerprint 验证管理员使用脱敏指纹撤销租约，
// 管理接口和审计链路都不需要 Lease Token 明文。
func TestProxyAdmin_RevokeLeaseByFingerprint(t *testing.T) {
	controlURL, _ := newAllocatorControlServer(t, []string{"node-a", "node-b"})
	_ = acquireLeaseThroughAPI(t, controlURL, "revoke-me")
	_ = acquireLeaseThroughAPI(t, controlURL, "keep-me")
	response, err := http.Get(controlURL + "/api/proxy-runtime")
	if err != nil {
		t.Fatalf("get runtime: %v", err)
	}
	var snapshot lease.Snapshot
	decodeErr := json.NewDecoder(response.Body).Decode(&snapshot)
	_ = response.Body.Close()
	if decodeErr != nil {
		t.Fatalf("decode runtime: %v", decodeErr)
	}
	var fingerprint string
	for _, currentLease := range snapshot.ActiveLeases {
		if currentLease.Label == "revoke-me" {
			fingerprint = currentLease.TokenFingerprint
		}
	}
	if fingerprint == "" {
		t.Fatal("revoke target fingerprint not found")
	}
	status := postAdminAction(t, controlURL, "/api/proxy-admin/revoke-lease", map[string]any{
		"target": fingerprint, "reason": "stuck caller", "confirm": true,
	})
	if status != http.StatusOK {
		t.Fatalf("revoke status = %d", status)
	}
	response, err = http.Get(controlURL + "/api/proxy-runtime")
	if err != nil {
		t.Fatalf("get runtime after revoke: %v", err)
	}
	decodeErr = json.NewDecoder(response.Body).Decode(&snapshot)
	_ = response.Body.Close()
	if decodeErr != nil {
		t.Fatalf("decode runtime after revoke: %v", decodeErr)
	}
	if len(snapshot.ActiveLeases) != 1 || snapshot.ActiveLeases[0].Label != "keep-me" {
		t.Fatalf("active leases after revoke = %+v", snapshot.ActiveLeases)
	}
}

// TestProxyAdmin_AbortCandidateKeepsActiveGeneration 验证管理员中止候选代时，
// Candidate 资源被关闭，当前 Active 不发生变化。
func TestProxyAdmin_AbortCandidateKeepsActiveGeneration(t *testing.T) {
	controlURL, runtime := newAllocatorControlServer(t, []string{"node-a"})
	probeServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(probeServer.Close)
	probeURL, err := url.Parse(probeServer.URL)
	if err != nil {
		t.Fatalf("parse probe URL: %v", err)
	}
	candidateNode := lease.Node{Key: "candidate-node", Dialer: fixedNodeDialer{address: probeURL.Host}}
	validation, err := lease.StartValidation(context.Background(), []lease.Node{candidateNode}, lease.ValidationOptions{
		PrimaryTarget: probeServer.URL, ExpectedStatus: http.StatusNoContent, MinReady: 1,
	})
	if err != nil {
		t.Fatalf("start candidate validation: %v", err)
	}
	if err := runtime.BeginCandidate("generation-2", 1, validation); err != nil {
		t.Fatalf("begin candidate: %v", err)
	}
	candidateClosed := make(chan struct{})
	if err := runtime.AttachCandidateCloser("generation-2", func() error {
		close(candidateClosed)
		return nil
	}); err != nil {
		t.Fatalf("attach candidate closer: %v", err)
	}
	status := postAdminAction(t, controlURL, "/api/proxy-admin/abort-candidate", map[string]any{
		"reason": "bad subscription", "confirm": true,
	})
	if status != http.StatusOK {
		t.Fatalf("abort candidate status = %d", status)
	}
	select {
	case <-candidateClosed:
	case <-time.After(time.Second):
		t.Fatal("candidate resources were not closed")
	}
	snapshot := runtime.Snapshot()
	if len(snapshot.Generations) != 1 || snapshot.Generations[0].Role != lease.GenerationRoleActive {
		t.Fatalf("generations after abort = %+v", snapshot.Generations)
	}
}

// TestProxyAdmin_ForceClosesOnlyTargetDrainingGeneration 验证强制关闭排空代不会影响当前 Active。
func TestProxyAdmin_ForceClosesOnlyTargetDrainingGeneration(t *testing.T) {
	controlURL, runtime := newAllocatorControlServer(t, []string{"old-node"})
	_ = acquireLeaseThroughAPI(t, controlURL, "old-lease")
	probeServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(probeServer.Close)
	probeURL, _ := url.Parse(probeServer.URL)
	newNode := lease.Node{Key: "new-node", Dialer: fixedNodeDialer{address: probeURL.Host}}
	validation, err := lease.StartValidation(context.Background(), []lease.Node{newNode}, lease.ValidationOptions{
		PrimaryTarget: probeServer.URL, ExpectedStatus: http.StatusNoContent, MinReady: 1,
	})
	if err != nil {
		t.Fatalf("start validation: %v", err)
	}
	readyNodes, err := validation.WaitMinimum(context.Background(), 1)
	if err != nil {
		t.Fatalf("wait validation: %v", err)
	}
	if err := runtime.BeginCandidate("generation-2", 1, validation); err != nil {
		t.Fatalf("begin candidate: %v", err)
	}
	if err := runtime.Promote(lease.Candidate{ID: "generation-2", Nodes: []lease.Node{newNode}}, readyNodes, validation); err != nil {
		t.Fatalf("promote candidate: %v", err)
	}
	status := postAdminAction(t, controlURL, "/api/proxy-admin/force-close-generation", map[string]any{
		"target": "generation-1", "reason": "drain deadline exceeded", "confirm": true,
	})
	if status != http.StatusOK {
		t.Fatalf("force close generation status = %d", status)
	}
	snapshot := runtime.Snapshot()
	if len(snapshot.Generations) != 1 || snapshot.Generations[0].ID != "generation-2" || snapshot.Generations[0].Role != lease.GenerationRoleActive || len(snapshot.ActiveLeases) != 0 {
		t.Fatalf("runtime after force close = %+v", snapshot)
	}
}

// TestProxyAudit_RecordsReasonOperatorAndResult 验证高影响操作生成可筛选审计记录。
func TestProxyAudit_RecordsReasonOperatorAndResult(t *testing.T) {
	controlURL, _ := newAllocatorControlServer(t, []string{"node-a"})
	status := postAdminAction(t, controlURL, "/api/proxy-admin/pause", map[string]any{
		"reason": "planned maintenance", "confirm": true,
	})
	if status != http.StatusOK {
		t.Fatalf("pause status = %d", status)
	}
	response, err := http.Get(controlURL + "/api/proxy-audit?type=ADMIN_ALLOCATION_PAUSED")
	if err != nil {
		t.Fatalf("get proxy audit: %v", err)
	}
	defer response.Body.Close()
	var payload struct {
		Events []lease.RuntimeEvent `json:"events"`
	}
	if err := json.NewDecoder(response.Body).Decode(&payload); err != nil {
		t.Fatalf("decode proxy audit: %v", err)
	}
	if len(payload.Events) != 1 || payload.Events[0].Reason != "planned maintenance" || payload.Events[0].Operator != "webui" || payload.Events[0].Result != "success" {
		t.Fatalf("audit events = %+v", payload.Events)
	}
}

// TestProxyAdmin_UnauthorizedActionIsRejectedAndAudited 验证人工处置只接受 WebUI 管理会话，
// Lease API Token 或匿名请求不能复用管理权限，拒绝结果仍进入脱敏审计时间线。
func TestProxyAdmin_UnauthorizedActionIsRejectedAndAudited(t *testing.T) {
	runtime, err := lease.NewRuntime(lease.Options{
		APIToken: "machine-token", ProxyURL: "http://127.0.0.1:2330",
		Nodes: []lease.Node{{Key: "node-a", Dialer: fixedNodeDialer{address: "127.0.0.1:1"}}},
	})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}
	t.Cleanup(func() { _ = runtime.Close() })
	manager, err := NewManager(Config{})
	if err != nil {
		t.Fatalf("new monitor manager: %v", err)
	}
	server := NewServer(Config{Enabled: true, Password: "admin-password"}, manager, log.Default())
	server.SetLeaseController(runtime)
	httpServer := httptest.NewServer(server.Handler())
	t.Cleanup(httpServer.Close)

	payload := bytes.NewBufferString(`{"reason":"unauthorized","confirm":true}`)
	request, _ := http.NewRequest(http.MethodPost, httpServer.URL+"/api/proxy-admin/pause", payload)
	request.Header.Set("Authorization", "Bearer machine-token")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("post unauthorized action: %v", err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unauthorized action status = %d", response.StatusCode)
	}

	loginResponse, err := http.Post(httpServer.URL+"/api/auth", "application/json", strings.NewReader(`{"password":"admin-password"}`))
	if err != nil {
		t.Fatalf("login: %v", err)
	}
	_ = loginResponse.Body.Close()
	if len(loginResponse.Cookies()) == 0 {
		t.Fatal("login did not return session cookie")
	}
	auditRequest, _ := http.NewRequest(http.MethodGet, httpServer.URL+"/api/proxy-audit?type=ADMIN_AUTHORIZATION_REJECTED", nil)
	auditRequest.AddCookie(loginResponse.Cookies()[0])
	auditResponse, err := http.DefaultClient.Do(auditRequest)
	if err != nil {
		t.Fatalf("get authorization audit: %v", err)
	}
	defer auditResponse.Body.Close()
	var audit struct {
		Events []lease.RuntimeEvent `json:"events"`
	}
	if err := json.NewDecoder(auditResponse.Body).Decode(&audit); err != nil {
		t.Fatalf("decode authorization audit: %v", err)
	}
	if len(audit.Events) != 1 || audit.Events[0].Target != "/api/proxy-admin/pause" || audit.Events[0].Result != "rejected" {
		t.Fatalf("authorization audit = %+v", audit.Events)
	}
}

func postAdminAction(t *testing.T, controlURL, path string, payload map[string]any) int {
	t.Helper()
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal admin action: %v", err)
	}
	response, err := http.Post(controlURL+path, "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("post admin action: %v", err)
	}
	defer response.Body.Close()
	if !strings.HasPrefix(response.Header.Get("Content-Type"), "application/json") {
		t.Fatalf("admin action content type = %q", response.Header.Get("Content-Type"))
	}
	return response.StatusCode
}
