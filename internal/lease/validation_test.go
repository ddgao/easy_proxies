package lease

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type recordingDialer struct {
	address string
	calls   atomic.Int32
}

func (d *recordingDialer) DialContext(ctx context.Context, network, _ string) (net.Conn, error) {
	d.calls.Add(1)
	return (&net.Dialer{}).DialContext(ctx, network, d.address)
}

// TestValidation_PrimaryFailureFallsBackWithExactPathAndStatus 验证主备目标使用完整 URL，
// 且请求必须经过当前 Node Key 的注入拨号器。
func TestValidation_PrimaryFailureFallsBackWithExactPathAndStatus(t *testing.T) {
	var mu sync.Mutex
	var paths []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		paths = append(paths, r.URL.Path)
		mu.Unlock()
		if r.URL.Path == "/fallback/ready" {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	t.Cleanup(server.Close)
	serverURL, _ := url.Parse(server.URL)
	dialer := &recordingDialer{address: serverURL.Host}

	run, err := StartValidation(context.Background(), []Node{{Key: "node-a", Dialer: dialer}}, ValidationOptions{
		PrimaryTarget:  "http://primary.example/wrong/path",
		FallbackTarget: "http://fallback.example/fallback/ready",
		ExpectedStatus: http.StatusNoContent,
		MinReady:       1,
		Concurrency:    1,
		RetryDelay:     time.Millisecond,
	})
	if err != nil {
		t.Fatalf("start validation: %v", err)
	}
	ready, err := run.WaitMinimum(context.Background(), 1)
	if err != nil {
		t.Fatalf("wait minimum: %v", err)
	}
	if len(ready) != 1 || ready[0].Key != "node-a" {
		t.Fatalf("ready nodes = %+v", ready)
	}
	mu.Lock()
	gotPaths := append([]string(nil), paths...)
	mu.Unlock()
	if len(gotPaths) != 2 || gotPaths[0] != "/wrong/path" || gotPaths[1] != "/fallback/ready" {
		t.Fatalf("probe paths = %v", gotPaths)
	}
	if dialer.calls.Load() != 2 {
		t.Fatalf("Node Key dial count = %d, want 2", dialer.calls.Load())
	}
}

// TestValidation_UsesConfiguredTLSAndExpectedStatus 验证 HTTPS 握手和期望状态均参与准入判断。
func TestValidation_UsesConfiguredTLSAndExpectedStatus(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/tls-ready" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	t.Cleanup(server.Close)
	serverURL, _ := url.Parse(server.URL)
	dialer := &recordingDialer{address: serverURL.Host}

	run, err := StartValidation(context.Background(), []Node{{Key: "tls-node", Dialer: dialer}}, ValidationOptions{
		PrimaryTarget:  "https://probe.example/tls-ready",
		ExpectedStatus: http.StatusAccepted,
		MinReady:       1,
		Concurrency:    1,
		SkipCertVerify: true,
	})
	if err != nil {
		t.Fatalf("start TLS validation: %v", err)
	}
	if _, err := run.WaitMinimum(context.Background(), 1); err != nil {
		t.Fatalf("wait TLS validation: %v", err)
	}
	if dialer.calls.Load() == 0 {
		t.Fatal("TLS probe did not use Node Key dialer")
	}
}

// TestValidation_RetriesWholeRoundAfterDelay 验证首轮全部失败后不会立即重试。
func TestValidation_RetriesWholeRoundAfterDelay(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if requests.Add(1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(server.Close)
	serverURL, _ := url.Parse(server.URL)
	dialer := &recordingDialer{address: serverURL.Host}
	retryDelay := 40 * time.Millisecond
	started := time.Now()
	run, err := StartValidation(context.Background(), []Node{{Key: "retry-node", Dialer: dialer}}, ValidationOptions{
		PrimaryTarget:  "http://probe.example/ready",
		ExpectedStatus: http.StatusNoContent,
		MinReady:       1,
		Concurrency:    1,
		RetryDelay:     retryDelay,
	})
	if err != nil {
		t.Fatalf("start validation: %v", err)
	}
	if _, err := run.WaitMinimum(context.Background(), 1); err != nil {
		t.Fatalf("wait retry validation: %v", err)
	}
	if elapsed := time.Since(started); elapsed < retryDelay {
		t.Fatalf("retry elapsed = %s, want at least %s", elapsed, retryDelay)
	}
	if requests.Load() != 2 || dialer.calls.Load() != 2 {
		t.Fatalf("request count = %d, dial count = %d", requests.Load(), dialer.calls.Load())
	}
}

// TestValidation_ReturnsAtFiftiethReadyAndContinuesSlowTail 验证达到门槛立即准入，
// 未完成节点仍在后台验证并按完成顺序从 Ready 通道进入队尾。
func TestValidation_ReturnsAtFiftiethReadyAndContinuesSlowTail(t *testing.T) {
	readyServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(readyServer.Close)
	readyURL, _ := url.Parse(readyServer.URL)

	slowStarted := make(chan struct{})
	releaseSlow := make(chan struct{})
	var slowOnce sync.Once
	slowServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		slowOnce.Do(func() { close(slowStarted) })
		<-releaseSlow
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(slowServer.Close)
	slowURL, _ := url.Parse(slowServer.URL)

	nodes := make([]Node, 0, 51)
	for i := 0; i < 50; i++ {
		nodes = append(nodes, Node{Key: "ready-" + strconv.Itoa(i), Dialer: &recordingDialer{address: readyURL.Host}})
	}
	nodes = append(nodes, Node{Key: "slow-tail", Dialer: &recordingDialer{address: slowURL.Host}})
	run, err := StartValidation(context.Background(), nodes, ValidationOptions{
		PrimaryTarget:  "http://probe.example/ready",
		ExpectedStatus: http.StatusNoContent,
		MinReady:       50,
		Concurrency:    51,
	})
	if err != nil {
		t.Fatalf("start validation: %v", err)
	}
	select {
	case <-slowStarted:
	case <-time.After(time.Second):
		t.Fatal("slow tail did not start")
	}
	waitCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	ready, err := run.WaitMinimum(waitCtx, 50)
	if err != nil {
		t.Fatalf("wait fiftieth ready: %v", err)
	}
	for _, node := range ready {
		if node.Key == "slow-tail" {
			t.Fatal("slow tail entered initial ready threshold")
		}
	}
	close(releaseSlow)
	select {
	case node := <-run.Ready():
		if node.Key != "slow-tail" {
			t.Fatalf("tail node = %q, want slow-tail", node.Key)
		}
	case <-time.After(time.Second):
		t.Fatal("slow tail did not continue after admission")
	}
}
