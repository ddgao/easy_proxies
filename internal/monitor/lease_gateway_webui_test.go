package monitor

import (
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestWebUI_ShowsLeaseGatewaySummary 验证首页具备最小租约观测入口，且数据来自脱敏运行时摘要 API。
func TestWebUI_ShowsLeaseGatewaySummary(t *testing.T) {
	manager, err := NewManager(Config{})
	if err != nil {
		t.Fatalf("new monitor manager: %v", err)
	}
	server := NewServer(Config{Enabled: true}, manager, log.Default())
	request := httptest.NewRequest(http.MethodGet, "/", nil)
	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("index status = %d", response.Code)
	}
	body, err := io.ReadAll(response.Result().Body)
	if err != nil {
		t.Fatalf("read index: %v", err)
	}
	html := string(body)
	for _, expected := range []string{
		"leaseGatewayStatus",
		"activeLeaseCount",
		"leaseGatewayConnections",
		"leaseGatewayRejections",
		"gateway_metrics",
		"max_connections",
		"connection_rejections",
		"activeGenerationID",
		"leaseValidationStatus",
		"leaseGenerationRows",
		"leaseSummaryRows",
		"idleLeaseNodeCount",
		"leaseWaiterCount",
		"leaseUtilization",
		"leaseDegradedStatus",
		"leaseRefreshStatus",
		"lastProxyEventSequence",
		"event_latest_sequence",
		"pending_refresh",
		"refresh(); startProxyEvents();",
		"leaseNodeMoreBtn",
		"node_limit=100",
		"proxyEventSource",
		"scheduleLeaseRuntimeRefresh",
		"pauseLeaseAllocationBtn",
		"leaseAuditRows",
		"fetch('/api/proxy-runtime?",
	} {
		if !strings.Contains(html, expected) {
			t.Fatalf("WebUI missing %q", expected)
		}
	}
	if strings.Contains(html, "proxyEventSource.onmessage = event => {\n") && strings.Contains(html, "if (sequence) lastProxyEventSequence = sequence;\n        fetchLeaseRuntime();") {
		t.Fatal("SSE 每条事件仍直接刷新完整运行快照，严格准入期间会形成请求风暴")
	}
}
