package config

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestFetchSubscriptionNodesConcurrentDedupesAndRedacts(t *testing.T) {
	var active atomic.Int32
	release := make(chan struct{})
	var releaseOnce sync.Once

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if active.Add(1) == 2 {
			releaseOnce.Do(func() { close(release) })
		}
		select {
		case <-release:
		case <-time.After(750 * time.Millisecond):
			http.Error(w, "requests were not concurrent", http.StatusGatewayTimeout)
			return
		}
		switch r.URL.Path {
		case "/one":
			_, _ = w.Write([]byte(strings.Join([]string{
				"vless://00000000-0000-0000-0000-000000000001@example.com:443?type=ws#A",
				"vless://00000000-0000-0000-0000-000000000001@example.com:443?type=ws#B",
			}, "\n")))
		case "/two":
			_, _ = w.Write([]byte("trojan://pw@example.org:443#C\n"))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	var logs []string
	nodes, stats := FetchSubscriptionNodes(context.Background(), []string{
		server.URL + "/one?token=secret-1",
		server.URL + "/one?token=secret-1",
		server.URL + "/two?token=secret-2",
	}, SubscriptionFetchOptions{
		Timeout:     2 * time.Second,
		Concurrency: 2,
		Client:      server.Client(),
		Loggerf: func(format string, args ...any) {
			logs = append(logs, fmt.Sprintf(format, args...))
		},
	})

	if len(nodes) != 2 {
		t.Fatalf("expected 2 unique nodes after dedupe, got %d: %#v", len(nodes), nodes)
	}
	if stats.RequestedURLs != 3 || stats.UniqueURLs != 2 || stats.DedupedURLs != 1 {
		t.Fatalf("unexpected URL stats: %+v", stats)
	}
	if stats.Successful != 2 || stats.Failed != 0 || stats.Nodes != 3 || stats.DedupedNodes != 1 {
		t.Fatalf("unexpected fetch stats: %+v", stats)
	}
	if got := active.Load(); got != 2 {
		t.Fatalf("expected both unique subscriptions to be fetched concurrently, active=%d", got)
	}
	joinedLogs := strings.Join(logs, "\n")
	if strings.Contains(joinedLogs, "secret-") {
		t.Fatalf("subscription logger leaked query token: %s", joinedLogs)
	}
}

func TestRedactURLRemovesCredentialsQueryAndFragment(t *testing.T) {
	redacted := RedactURL("https://user:pass@example.com/path?token=secret#frag")
	if strings.Contains(redacted, "user") || strings.Contains(redacted, "pass") || strings.Contains(redacted, "secret") || strings.Contains(redacted, "frag") {
		t.Fatalf("URL was not redacted: %s", redacted)
	}
	if redacted != "https://example.com/...?redacted=1" {
		t.Fatalf("unexpected redacted URL: %s", redacted)
	}
}

func TestFetchSubscriptionNodesCountsFailuresWhenContextCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	nodes, stats := FetchSubscriptionNodes(ctx, []string{
		"http://example.invalid/one",
		"http://example.invalid/two",
	}, SubscriptionFetchOptions{Concurrency: 2})

	if len(nodes) != 0 {
		t.Fatalf("expected no nodes after canceled context, got %d", len(nodes))
	}
	if stats.UniqueURLs != 2 || stats.Failed != 2 {
		t.Fatalf("expected both unique URLs to be counted as failed, got %+v", stats)
	}
	if stats.LastError == nil || !strings.Contains(stats.LastError.Error(), context.Canceled.Error()) {
		t.Fatalf("expected context cancellation as last error, got %v", stats.LastError)
	}
}

func TestParseClashYAML_HTTPProxySupported(t *testing.T) {
	content := `proxies:
  - name: "http-node"
    type: "http"
    server: proxy.example.com
    port: 8080
    username: "user"
    password: "pass"
  - name: "https-node"
    type: "https"
    server: secure.example.com
    port: 443
`

	nodes, err := parseClashYAML(content)
	if err != nil {
		t.Fatalf("parse clash yaml: %v", err)
	}
	if len(nodes) != 2 {
		t.Fatalf("expected 2 HTTP proxy nodes, got %d: %#v", len(nodes), nodes)
	}
	if nodes[0].URI != "http://user:pass@proxy.example.com:8080#http-node" {
		t.Fatalf("unexpected HTTP URI: %q", nodes[0].URI)
	}
	if nodes[1].URI != "https://secure.example.com:443#https-node" {
		t.Fatalf("unexpected HTTPS URI: %q", nodes[1].URI)
	}
}

func TestShouldUseCachedSubscriptionNodesOnPartialRegression(t *testing.T) {
	fetched := []NodeConfig{{URI: "trojan://pw@new.example.com:443#new"}}
	cached := []NodeConfig{
		{URI: "trojan://pw@old1.example.com:443#old1"},
		{URI: "trojan://pw@old2.example.com:443#old2"},
	}
	useCached, reason := shouldUseCachedSubscriptionNodes(fetched, cached, nil, SubscriptionFetchStats{Failed: 1})
	if !useCached {
		t.Fatalf("expected cached nodes to protect against partial refresh regression, reason=%q", reason)
	}
}

func TestShouldUseFetchedSubscriptionNodesWhenBetterThanCache(t *testing.T) {
	fetched := []NodeConfig{
		{URI: "trojan://pw@new1.example.com:443#new1"},
		{URI: "trojan://pw@new2.example.com:443#new2"},
	}
	cached := []NodeConfig{{URI: "trojan://pw@old.example.com:443#old"}}
	useCached, reason := shouldUseCachedSubscriptionNodes(fetched, cached, nil, SubscriptionFetchStats{Failed: 1})
	if useCached {
		t.Fatalf("expected larger fetched node set to be used, reason=%q", reason)
	}
}
