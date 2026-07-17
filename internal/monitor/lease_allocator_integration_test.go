package monitor

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"easy_proxies/internal/lease"
)

// TestProxyLeases_DifferentConflictDomainsCanShareFirstNode 验证动态调用方无需注册身份：
// 不同租约冲突域拥有独立空闲节点队列，因此都可以取得各自队首的同一 Node Key。
func TestProxyLeases_DifferentConflictDomainsCanShareFirstNode(t *testing.T) {
	controlURL, _ := newAllocatorControlServer(t, []string{"node-a", "node-b"})
	first := acquireLeaseThroughAPIWithConflict(t, controlURL, "first", "account:100")
	second := acquireLeaseThroughAPIWithConflict(t, controlURL, "second", "account:200")
	if first.NodeKey != "node-a" || second.NodeKey != "node-a" {
		t.Fatalf("different conflict domains got nodes %q and %q, want node-a and node-a", first.NodeKey, second.NodeKey)
	}
}

// TestProxyLeases_ConflictDomainReusesNodesInReleaseFIFOOrder 验证同一冲突域的严格空闲 FIFO：
// 当初始节点已经耗尽时，后续租约必须按节点完成排空的先后顺序获得节点。
func TestProxyLeases_ConflictDomainReusesNodesInReleaseFIFOOrder(t *testing.T) {
	controlURL, _ := newAllocatorControlServer(t, []string{"node-a", "node-b", "node-c"})
	first := acquireLeaseThroughAPIWithConflict(t, controlURL, "first", "account:100")
	second := acquireLeaseThroughAPIWithConflict(t, controlURL, "second", "account:100")
	third := acquireLeaseThroughAPIWithConflict(t, controlURL, "third", "account:100")
	if first.NodeKey != "node-a" || second.NodeKey != "node-b" || third.NodeKey != "node-c" {
		t.Fatalf("initial FIFO nodes = %q, %q, %q", first.NodeKey, second.NodeKey, third.NodeKey)
	}

	releaseLeaseThroughAPI(t, controlURL, second.LeaseToken)
	releaseLeaseThroughAPI(t, controlURL, first.LeaseToken)
	fourth := acquireLeaseThroughAPIWithConflict(t, controlURL, "fourth", "account:100")
	fifth := acquireLeaseThroughAPIWithConflict(t, controlURL, "fifth", "account:100")
	if fourth.NodeKey != "node-b" || fifth.NodeKey != "node-a" {
		t.Fatalf("released FIFO nodes = %q, %q, want node-b, node-a", fourth.NodeKey, fifth.NodeKey)
	}
}

// TestProxyLeases_ConflictKeyIsRedactedFromRuntimeSnapshot 验证冲突键按敏感业务标识处理：
// 管理快照只暴露不可反查的短指纹，不能包含申请请求中的原文。
func TestProxyLeases_ConflictKeyIsRedactedFromRuntimeSnapshot(t *testing.T) {
	controlURL, _ := newAllocatorControlServer(t, []string{"node-a"})
	conflictKey := "account:sensitive-100"
	_ = acquireLeaseThroughAPIWithConflict(t, controlURL, "redacted", conflictKey)

	response, err := http.Get(controlURL + "/api/proxy-runtime")
	if err != nil {
		t.Fatalf("get runtime snapshot: %v", err)
	}
	body, err := io.ReadAll(response.Body)
	_ = response.Body.Close()
	if err != nil {
		t.Fatalf("read runtime snapshot: %v", err)
	}
	if bytes.Contains(body, []byte(conflictKey)) {
		t.Fatalf("runtime snapshot leaked conflict_key: %s", body)
	}
	var snapshot struct {
		ActiveLeases []struct {
			ConflictFingerprint string `json:"conflict_fingerprint"`
		} `json:"active_leases"`
	}
	if err := json.Unmarshal(body, &snapshot); err != nil {
		t.Fatalf("decode runtime snapshot: %v", err)
	}
	if len(snapshot.ActiveLeases) != 1 || snapshot.ActiveLeases[0].ConflictFingerprint == "" {
		t.Fatalf("active lease conflict fingerprint = %+v", snapshot.ActiveLeases)
	}
}

// TestProxyLeases_ConflictKeyNormalizationAndValidation 固化机器字段契约：
// 仅去除首尾空白、保持大小写敏感，并按 UTF-8 字节数限制为 128 字节。
func TestProxyLeases_ConflictKeyNormalizationAndValidation(t *testing.T) {
	controlURL, _ := newAllocatorControlServer(t, []string{"node-a", "node-b"})
	first := acquireLeaseThroughAPIWithConflict(t, controlURL, "trimmed", " account:100 ")
	second := acquireLeaseThroughAPIWithConflict(t, controlURL, "same", "account:100")
	differentCase := acquireLeaseThroughAPIWithConflict(t, controlURL, "case-sensitive", "Account:100")
	if first.NodeKey != "node-a" || second.NodeKey != "node-b" || differentCase.NodeKey != "node-a" {
		t.Fatalf("normalized conflict allocation = %q, %q, %q", first.NodeKey, second.NodeKey, differentCase.NodeKey)
	}

	request, err := http.NewRequest(http.MethodPost, controlURL+"/api/proxy-leases",
		strings.NewReader(`{"conflict_key":"`+strings.Repeat("x", 129)+`"}`))
	if err != nil {
		t.Fatalf("new invalid conflict request: %v", err)
	}
	request.Header.Set("Authorization", "Bearer machine-token")
	request.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("invalid conflict request: %v", err)
	}
	defer response.Body.Close()
	var failure struct {
		Code string `json:"code"`
	}
	if err := json.NewDecoder(response.Body).Decode(&failure); err != nil {
		t.Fatalf("decode invalid conflict response: %v", err)
	}
	if response.StatusCode != http.StatusBadRequest || failure.Code != "INVALID_CONFLICT_KEY" {
		t.Fatalf("invalid conflict response status=%d code=%q", response.StatusCode, failure.Code)
	}
}

// TestProxyLeases_InactiveDynamicConflictDomainRestartsFromInitialQueue 验证动态域状态及时回收：
// 最后一个租约结束后再次使用相同冲突键，应创建新队列并从初始节点顺序开始。
func TestProxyLeases_InactiveDynamicConflictDomainRestartsFromInitialQueue(t *testing.T) {
	controlURL, _ := newAllocatorControlServer(t, []string{"node-a", "node-b"})
	first := acquireLeaseThroughAPIWithConflict(t, controlURL, "first", "task:dynamic")
	if first.NodeKey != "node-a" {
		t.Fatalf("first dynamic domain Node Key = %q", first.NodeKey)
	}
	releaseLeaseThroughAPI(t, controlURL, first.LeaseToken)
	second := acquireLeaseThroughAPIWithConflict(t, controlURL, "second", "task:dynamic")
	if second.NodeKey != "node-a" {
		t.Fatalf("recreated dynamic domain Node Key = %q, want node-a", second.NodeKey)
	}
}

// TestProxyLeases_ReleasedNodeReturnsToQueueTail 验证空闲节点队列的外部顺序：
// 已释放节点回到队尾，不能在尚有未使用节点时立即绕回复用。
func TestProxyLeases_ReleasedNodeReturnsToQueueTail(t *testing.T) {
	controlURL, _ := newAllocatorControlServer(t, []string{"node-a", "node-b"})
	first := acquireLeaseThroughAPI(t, controlURL, "first")
	if first.NodeKey != "node-a" {
		t.Fatalf("first Node Key = %q, want node-a", first.NodeKey)
	}
	releaseLeaseThroughAPI(t, controlURL, first.LeaseToken)
	second := acquireLeaseThroughAPI(t, controlURL, "second")
	if second.NodeKey != "node-b" {
		t.Fatalf("Node Key after releasing node-a = %q, want node-b", second.NodeKey)
	}
}

// TestProxyLeases_WaitsForReleasedNode 验证空闲节点耗尽时申请进入等待队列，
// 节点释放后由等待中的申请获得，而不是立即返回 503。
func TestProxyLeases_WaitsForReleasedNode(t *testing.T) {
	controlURL, _ := newAllocatorControlServer(t, []string{"node-a"})
	holder := acquireLeaseThroughAPI(t, controlURL, "holder")

	type acquireResult struct {
		grant allocatorGrant
		err   error
	}
	result := make(chan acquireResult, 1)
	go func() {
		grant, err := acquireLeaseThroughAPIResult(controlURL, "waiter")
		result <- acquireResult{grant: grant, err: err}
	}()

	deadline := time.Now().Add(time.Second)
	for {
		response, err := http.Get(controlURL + "/api/proxy-runtime")
		if err != nil {
			t.Fatalf("get runtime snapshot: %v", err)
		}
		var snapshot struct {
			WaiterCount int `json:"waiter_count"`
		}
		decodeErr := json.NewDecoder(response.Body).Decode(&snapshot)
		_ = response.Body.Close()
		if decodeErr != nil {
			t.Fatalf("decode runtime snapshot: %v", decodeErr)
		}
		if snapshot.WaiterCount == 1 {
			break
		}
		select {
		case early := <-result:
			t.Fatalf("waiting acquire returned before release: grant=%+v error=%v", early.grant, early.err)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("acquire request did not enter waiter queue")
		}
		time.Sleep(5 * time.Millisecond)
	}

	releaseLeaseThroughAPI(t, controlURL, holder.LeaseToken)
	select {
	case acquired := <-result:
		if acquired.err != nil {
			t.Fatalf("waiting acquire failed: %v", acquired.err)
		}
		if acquired.grant.NodeKey != "node-a" {
			t.Fatalf("waiting acquire Node Key = %q", acquired.grant.NodeKey)
		}
	case <-time.After(time.Second):
		t.Fatal("waiting acquire was not satisfied after release")
	}
}

// TestProxyLeases_WaitersAreSatisfiedInFIFOOrder 验证多个并发等待者严格按入队顺序获得节点。
func TestProxyLeases_WaitersAreSatisfiedInFIFOOrder(t *testing.T) {
	controlURL, _ := newAllocatorControlServer(t, []string{"node-a"})
	holder := acquireLeaseThroughAPI(t, controlURL, "holder")
	type result struct {
		grant allocatorGrant
		err   error
	}
	firstResult := make(chan result, 1)
	secondResult := make(chan result, 1)
	go func() {
		grant, err := acquireLeaseThroughAPIResult(controlURL, "first-waiter")
		firstResult <- result{grant: grant, err: err}
	}()
	waitForWaiterCount(t, controlURL, 1)
	go func() {
		grant, err := acquireLeaseThroughAPIResult(controlURL, "second-waiter")
		secondResult <- result{grant: grant, err: err}
	}()
	waitForWaiterCount(t, controlURL, 2)
	releaseLeaseThroughAPI(t, controlURL, holder.LeaseToken)
	var first result
	select {
	case first = <-firstResult:
		if first.err != nil {
			t.Fatalf("first waiter failed: %v", first.err)
		}
	case <-secondResult:
		t.Fatal("second waiter was satisfied before first waiter")
	case <-time.After(time.Second):
		t.Fatal("first waiter was not satisfied")
	}
	releaseLeaseThroughAPI(t, controlURL, first.grant.LeaseToken)
	select {
	case second := <-secondResult:
		if second.err != nil || second.grant.NodeKey != "node-a" {
			t.Fatalf("second waiter result = %+v", second)
		}
	case <-time.After(time.Second):
		t.Fatal("second waiter was not satisfied")
	}
}

// TestProxyLeases_WaitTimeoutReturnsStableErrorCode 验证等待超时的机器可读契约。
func TestProxyLeases_WaitTimeoutReturnsStableErrorCode(t *testing.T) {
	controlURL, _ := newAllocatorControlServerWithOptions(t, []string{"node-a"}, lease.Options{
		AcquireWaitTimeout: 30 * time.Millisecond,
	})
	_ = acquireLeaseThroughAPI(t, controlURL, "holder")

	payload, err := json.Marshal(map[string]string{"label": "timeout"})
	if err != nil {
		t.Fatalf("marshal acquire payload: %v", err)
	}
	request, err := http.NewRequest(http.MethodPost, controlURL+"/api/proxy-leases", bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("new acquire request: %v", err)
	}
	request.Header.Set("Authorization", "Bearer machine-token")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("acquire timeout request: %v", err)
	}
	defer response.Body.Close()
	var failure struct {
		Code string `json:"code"`
	}
	if err := json.NewDecoder(response.Body).Decode(&failure); err != nil {
		t.Fatalf("decode timeout response: %v", err)
	}
	if response.StatusCode != http.StatusServiceUnavailable || failure.Code != "LEASE_ACQUIRE_TIMEOUT" {
		t.Fatalf("timeout response status=%d code=%q", response.StatusCode, failure.Code)
	}
}

// TestProxyNodes_BlockAndRevalidateBeforeReturningToQueue 验证人工阻断跨租约生效，
// 解除阻断后必须复检，并按恢复时刻回到空闲队列尾部。
func TestProxyNodes_BlockAndRevalidateBeforeReturningToQueue(t *testing.T) {
	controlURL, _ := newAllocatorControlServerWithOptions(t, []string{"node-a", "node-b"}, lease.Options{
		NodeRecheck: func(context.Context, lease.Node) bool { return true },
	})
	first := acquireLeaseThroughAPI(t, controlURL, "uses-a")
	postNodeControlAction(t, controlURL, "/api/proxy-nodes/block", "node-a")
	response, err := http.Get(controlURL + "/api/proxy-runtime")
	if err != nil {
		t.Fatalf("get runtime after block: %v", err)
	}
	var blockedSnapshot struct {
		BlockedNodeKeys []string `json:"blocked_node_keys"`
		RecentLeases    []struct {
			NodeKey string `json:"node_key"`
			State   string `json:"state"`
		} `json:"recent_leases"`
	}
	decodeErr := json.NewDecoder(response.Body).Decode(&blockedSnapshot)
	_ = response.Body.Close()
	if decodeErr != nil {
		t.Fatalf("decode blocked snapshot: %v", decodeErr)
	}
	if len(blockedSnapshot.BlockedNodeKeys) != 1 || blockedSnapshot.BlockedNodeKeys[0] != "node-a" ||
		len(blockedSnapshot.RecentLeases) != 1 || blockedSnapshot.RecentLeases[0].State != "BROKEN" {
		t.Fatalf("blocked snapshot = %+v", blockedSnapshot)
	}
	second := acquireLeaseThroughAPI(t, controlURL, "uses-b")
	if second.NodeKey != "node-b" {
		t.Fatalf("Node Key after blocking node-a = %q", second.NodeKey)
	}
	releaseLeaseThroughAPI(t, controlURL, second.LeaseToken)
	postNodeControlAction(t, controlURL, "/api/proxy-nodes/unblock", "node-a")
	third := acquireLeaseThroughAPI(t, controlURL, "queue-order")
	if third.NodeKey != "node-b" {
		t.Fatalf("Node Key after revalidating node-a = %q, want released node-b first", third.NodeKey)
	}
	_ = first
}

// TestProxyNodes_RecoveredUnseenNodeMovesBehindExistingIdleNodes 验证严格 FIFO 不受延迟扫描影响：
// 尚未从初始队列取出的节点被阻断并恢复后，也必须按恢复时刻进入队尾。
func TestProxyNodes_RecoveredUnseenNodeMovesBehindExistingIdleNodes(t *testing.T) {
	controlURL, runtime := newAllocatorControlServerWithOptions(t, []string{"node-a", "node-b", "node-c"}, lease.Options{
		NodeRecheck: func(context.Context, lease.Node) bool { return true },
	})
	first := acquireLeaseThroughAPIWithConflict(t, controlURL, "first", "account:100")
	if first.NodeKey != "node-a" {
		t.Fatalf("first Node Key = %q", first.NodeKey)
	}
	postNodeControlAction(t, controlURL, "/api/proxy-nodes/block", "node-b")
	postNodeControlAction(t, controlURL, "/api/proxy-nodes/unblock", "node-b")
	deadline := time.Now().Add(time.Second)
	for {
		snapshot := runtime.Snapshot()
		ready := false
		for _, node := range snapshot.Nodes {
			if node.NodeKey == "node-b" && node.Ready {
				ready = true
				break
			}
		}
		if ready {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("node-b did not recover")
		}
		time.Sleep(5 * time.Millisecond)
	}
	second := acquireLeaseThroughAPIWithConflict(t, controlURL, "second", "account:100")
	if second.NodeKey != "node-c" {
		t.Fatalf("Node Key after node-b recovery = %q, want existing idle node-c", second.NodeKey)
	}
}

// TestProxyLeases_ReleaseDrainsExistingTunnelBeforeReusingNode 验证释放的两阶段语义：
// 旧 Token 立即停止新连接，已有 CONNECT 隧道结束前 Node Key 仍保持独占。
func TestProxyLeases_ReleaseDrainsExistingTunnelBeforeReusingNode(t *testing.T) {
	upstreamListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen upstream: %v", err)
	}
	t.Cleanup(func() { _ = upstreamListener.Close() })
	go func() {
		for {
			connection, acceptErr := upstreamListener.Accept()
			if acceptErr != nil {
				return
			}
			go func() {
				defer connection.Close()
				_, _ = io.Copy(io.Discard, connection)
			}()
		}
	}()

	gatewayListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen gateway: %v", err)
	}
	runtime, err := lease.NewRuntime(lease.Options{
		APIToken: "machine-token",
		ProxyURL: "http://" + gatewayListener.Addr().String(),
		Nodes: []lease.Node{{
			Key: "node-a", Dialer: fixedNodeDialer{address: upstreamListener.Addr().String()},
		}},
	})
	if err != nil {
		t.Fatalf("new runtime: %v", err)
	}
	t.Cleanup(func() { _ = runtime.Close() })
	gatewayServer := &http.Server{Handler: runtime.GatewayHandler()}
	go func() { _ = gatewayServer.Serve(gatewayListener) }()
	t.Cleanup(func() { _ = gatewayServer.Shutdown(context.Background()) })
	manager, err := NewManager(Config{})
	if err != nil {
		t.Fatalf("new monitor manager: %v", err)
	}
	management := NewServer(Config{Enabled: true}, manager, log.Default())
	management.SetLeaseController(runtime)
	controlServer := httptest.NewServer(management.Handler())
	t.Cleanup(controlServer.Close)

	holder := acquireLeaseThroughAPI(t, controlServer.URL, "holder")
	tunnel, err := net.Dial("tcp", gatewayListener.Addr().String())
	if err != nil {
		t.Fatalf("dial Lease Gateway: %v", err)
	}
	t.Cleanup(func() { _ = tunnel.Close() })
	authorization := base64.StdEncoding.EncodeToString([]byte("lease:" + holder.LeaseToken))
	if _, err := fmt.Fprintf(tunnel, "CONNECT business.example:443 HTTP/1.1\r\nHost: business.example:443\r\nProxy-Authorization: Basic %s\r\n\r\n", authorization); err != nil {
		t.Fatalf("write CONNECT request: %v", err)
	}
	connectResponse, err := http.ReadResponse(bufio.NewReader(tunnel), &http.Request{Method: http.MethodConnect})
	if err != nil {
		t.Fatalf("read CONNECT response: %v", err)
	}
	_ = connectResponse.Body.Close()
	if connectResponse.StatusCode != http.StatusOK {
		t.Fatalf("CONNECT status = %d", connectResponse.StatusCode)
	}

	releaseLeaseThroughAPI(t, controlServer.URL, holder.LeaseToken)
	otherDomain := acquireLeaseThroughAPIWithConflict(t, controlServer.URL, "other-domain", "account:200")
	if otherDomain.NodeKey != "node-a" {
		t.Fatalf("other conflict domain Node Key while holder drains = %q", otherDomain.NodeKey)
	}
	result := make(chan error, 1)
	go func() {
		_, acquireErr := acquireLeaseThroughAPIResult(controlServer.URL, "waiter")
		result <- acquireErr
	}()
	deadline := time.Now().Add(time.Second)
	for {
		response, getErr := http.Get(controlServer.URL + "/api/proxy-runtime")
		if getErr != nil {
			t.Fatalf("get runtime snapshot: %v", getErr)
		}
		var snapshot struct {
			WaiterCount int `json:"waiter_count"`
		}
		decodeErr := json.NewDecoder(response.Body).Decode(&snapshot)
		_ = response.Body.Close()
		if decodeErr != nil {
			t.Fatalf("decode runtime snapshot: %v", decodeErr)
		}
		if snapshot.WaiterCount == 1 {
			break
		}
		select {
		case early := <-result:
			t.Fatalf("node reused while tunnel was active: %v", early)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("new acquire did not wait for active tunnel")
		}
		time.Sleep(5 * time.Millisecond)
	}

	_ = tunnel.Close()
	select {
	case acquireErr := <-result:
		if acquireErr != nil {
			t.Fatalf("acquire after tunnel drain: %v", acquireErr)
		}
	case <-time.After(time.Second):
		t.Fatal("node was not released after tunnel drained")
	}
	releaseLeaseThroughAPI(t, controlServer.URL, otherDomain.LeaseToken)
}

type allocatorGrant struct {
	NodeKey    string `json:"node_key"`
	LeaseToken string `json:"lease_token"`
	ProxyURL   string `json:"proxy_url"`
}

func newAllocatorControlServer(t *testing.T, nodeKeys []string) (string, *lease.Runtime) {
	return newAllocatorControlServerWithOptions(t, nodeKeys, lease.Options{})
}

func newAllocatorControlServerWithOptions(t *testing.T, nodeKeys []string, options lease.Options) (string, *lease.Runtime) {
	t.Helper()
	nodes := make([]lease.Node, 0, len(nodeKeys))
	for _, nodeKey := range nodeKeys {
		nodes = append(nodes, lease.Node{Key: nodeKey, Dialer: fixedNodeDialer{address: "127.0.0.1:1"}})
	}
	options.APIToken = "machine-token"
	options.ProxyURL = "http://127.0.0.1:2330"
	options.Nodes = nodes
	runtime, err := lease.NewRuntime(options)
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
	controlServer := httptest.NewServer(server.Handler())
	t.Cleanup(controlServer.Close)
	return controlServer.URL, runtime
}

func acquireLeaseThroughAPI(t *testing.T, controlURL, label string) allocatorGrant {
	t.Helper()
	grant, err := acquireLeaseThroughAPIResultWithConflict(controlURL, label, "")
	if err != nil {
		t.Fatal(err)
	}
	return grant
}

func acquireLeaseThroughAPIWithConflict(t *testing.T, controlURL, label, conflictKey string) allocatorGrant {
	t.Helper()
	grant, err := acquireLeaseThroughAPIResultWithConflict(controlURL, label, conflictKey)
	if err != nil {
		t.Fatal(err)
	}
	return grant
}

func acquireLeaseThroughAPIResult(controlURL, label string) (allocatorGrant, error) {
	return acquireLeaseThroughAPIResultWithConflict(controlURL, label, "")
}

func acquireLeaseThroughAPIResultWithConflict(controlURL, label, conflictKey string) (allocatorGrant, error) {
	payload, err := json.Marshal(map[string]string{"label": label, "conflict_key": conflictKey})
	if err != nil {
		return allocatorGrant{}, err
	}
	request, err := http.NewRequest(http.MethodPost, controlURL+"/api/proxy-leases", bytes.NewReader(payload))
	if err != nil {
		return allocatorGrant{}, err
	}
	request.Header.Set("Authorization", "Bearer machine-token")
	request.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return allocatorGrant{}, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(response.Body)
		return allocatorGrant{}, fmt.Errorf("acquire status = %d, body = %s", response.StatusCode, body)
	}
	var grant allocatorGrant
	if err := json.NewDecoder(response.Body).Decode(&grant); err != nil {
		return allocatorGrant{}, err
	}
	return grant, nil
}

func releaseLeaseThroughAPI(t *testing.T, controlURL, leaseToken string) {
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
	if !strings.HasPrefix(response.Header.Get("Content-Type"), "application/json") {
		t.Fatalf("node action content type = %q", response.Header.Get("Content-Type"))
	}
}

func waitForWaiterCount(t *testing.T, controlURL string, expected int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		response, err := http.Get(controlURL + "/api/proxy-runtime")
		if err != nil {
			t.Fatalf("get runtime snapshot: %v", err)
		}
		var snapshot struct {
			WaiterCount int `json:"waiter_count"`
		}
		decodeErr := json.NewDecoder(response.Body).Decode(&snapshot)
		_ = response.Body.Close()
		if decodeErr != nil {
			t.Fatalf("decode runtime snapshot: %v", decodeErr)
		}
		if snapshot.WaiterCount == expected {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("waiter count = %d, want %d", snapshot.WaiterCount, expected)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func postNodeControlAction(t *testing.T, controlURL, path, nodeKey string) {
	t.Helper()
	payload, err := json.Marshal(map[string]any{"node_key": nodeKey, "reason": "operator request", "confirm": true})
	if err != nil {
		t.Fatalf("marshal node action: %v", err)
	}
	response, err := http.Post(controlURL+path, "application/json", bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("post node action: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(response.Body)
		t.Fatalf("node action status = %d, body = %s", response.StatusCode, body)
	}
}
