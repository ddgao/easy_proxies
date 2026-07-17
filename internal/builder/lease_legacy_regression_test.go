package builder

import (
	"encoding/json"
	"testing"

	"easy_proxies/internal/config"
)

// TestBuild_LeaseGatewayDoesNotChangeLegacyEntryPoints 验证开启独立 Lease Gateway
// 不会修改 Legacy 的候选集、轮换方式、Sticky 或 multi-port 监听配置。
func TestBuild_LeaseGatewayDoesNotChangeLegacyEntryPoints(t *testing.T) {
	testCases := []struct {
		name      string
		mode      string
		poolMode  string
		sticky    bool
		multiPort bool
	}{
		{name: "sequential", mode: "pool", poolMode: "sequential"},
		{name: "random", mode: "pool", poolMode: "random"},
		{name: "balance", mode: "pool", poolMode: "balance"},
		{name: "latency", mode: "pool", poolMode: "latency"},
		{name: "sticky", mode: "pool", poolMode: "sequential", sticky: true},
		{name: "multi-port", mode: "multi-port", poolMode: "sequential", multiPort: true},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			base := &config.Config{
				Mode:      testCase.mode,
				Listener:  config.ListenerConfig{Address: "127.0.0.1", Port: 2323, Username: "legacy-user", Password: "legacy-password"},
				Pool:      config.PoolConfig{Mode: testCase.poolMode},
				MultiPort: config.MultiPortConfig{Address: "127.0.0.1", Username: "legacy-user", Password: "legacy-password"},
				Sticky:    config.StickyConfig{Enabled: testCase.sticky, Port: 2324},
				Nodes: []config.NodeConfig{
					{Name: "node-a", URI: "http://127.0.0.1:18080", Port: 24000},
					{Name: "node-b", URI: "http://127.0.0.1:18081", Port: 24001},
				},
			}
			withoutLease, err := Build(base)
			if err != nil {
				t.Fatalf("build legacy config: %v", err)
			}
			withLeaseCfg := *base
			withLeaseCfg.LeaseGateway = config.LeaseGatewayConfig{
				Enabled: true, Listen: "127.0.0.1", Port: 2330, APIToken: "machine-token", MinReadyNodes: 1,
			}
			withLease, err := Build(&withLeaseCfg)
			if err != nil {
				t.Fatalf("build config with Lease Gateway: %v", err)
			}
			legacyJSON, _ := json.Marshal(withoutLease)
			leaseJSON, _ := json.Marshal(withLease)
			if string(legacyJSON) != string(leaseJSON) {
				t.Fatalf("Lease Gateway changed Legacy build for %s", testCase.name)
			}
		})
	}
}
