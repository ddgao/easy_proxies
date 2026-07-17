package config

import (
	"strings"
	"testing"
)

// TestNodeConfig_LeaseNodeKeyIsStableAndCredentialSafe 验证 Lease 对外 Node Key
// 不包含代理 URI 凭据，同时保持查询参数重排前后的稳定身份。
func TestNodeConfig_LeaseNodeKeyIsStableAndCredentialSafe(t *testing.T) {
	first := NodeConfig{URI: "vless://secret-uuid@example.com:443?security=tls&type=ws#first"}
	second := NodeConfig{URI: "vless://secret-uuid@example.com:443?type=ws&security=tls#second"}
	firstKey := first.LeaseNodeKey()
	if firstKey == "" || firstKey != second.LeaseNodeKey() {
		t.Fatalf("Lease Node Keys are not stable: %q != %q", firstKey, second.LeaseNodeKey())
	}
	if strings.Contains(firstKey, "secret-uuid") || strings.Contains(firstKey, "example.com") || strings.Contains(firstKey, "vless") {
		t.Fatalf("Lease Node Key leaked URI material: %q", firstKey)
	}
}

// TestConfig_LeaseGatewayIsOptInAndRejectsUnsafeSettings 验证 Lease Gateway 的配置兼容边界：
// 旧配置保持关闭，显式启用时必须具备机器令牌和独立监听端口。
func TestConfig_LeaseGatewayIsOptInAndRejectsUnsafeSettings(t *testing.T) {
	newConfig := func() *Config {
		return &Config{
			Mode:       "pool",
			Management: ManagementConfig{ProbeTarget: "http://probe.example/generate_204"},
			Nodes:      []NodeConfig{{Name: "node-a", URI: "http://127.0.0.1:18080"}},
		}
	}

	legacy := newConfig()
	if err := legacy.NormalizeWithPortMap(nil); err != nil {
		t.Fatalf("normalize legacy config: %v", err)
	}
	if legacy.LeaseGateway.Enabled {
		t.Fatal("legacy config unexpectedly enabled Lease Gateway")
	}

	missingToken := newConfig()
	missingToken.LeaseGateway.Enabled = true
	if err := missingToken.NormalizeWithPortMap(nil); err == nil || !strings.Contains(err.Error(), "api_token") {
		t.Fatalf("missing api_token error = %v", err)
	}

	valid := newConfig()
	valid.LeaseGateway.Enabled = true
	valid.LeaseGateway.APIToken = "machine-token"
	if err := valid.NormalizeWithPortMap(nil); err != nil {
		t.Fatalf("normalize enabled config: %v", err)
	}
	if valid.LeaseGateway.Listen != "127.0.0.1" || valid.LeaseGateway.Port != 2330 {
		t.Fatalf("gateway listen = %s:%d, want 127.0.0.1:2330", valid.LeaseGateway.Listen, valid.LeaseGateway.Port)
	}

	conflict := newConfig()
	conflict.LeaseGateway.Enabled = true
	conflict.LeaseGateway.APIToken = "machine-token"
	conflict.LeaseGateway.Port = 2323
	if err := conflict.NormalizeWithPortMap(nil); err == nil || !strings.Contains(err.Error(), "conflicts with listener.port") {
		t.Fatalf("listener port conflict error = %v", err)
	}

	managementDisabled := newConfig()
	disabled := false
	managementDisabled.Management.Enabled = &disabled
	managementDisabled.LeaseGateway.Enabled = true
	managementDisabled.LeaseGateway.APIToken = "machine-token"
	if err := managementDisabled.NormalizeWithPortMap(nil); err == nil || !strings.Contains(err.Error(), "management.enabled") {
		t.Fatalf("disabled management error = %v", err)
	}

	managementConflict := newConfig()
	managementConflict.Management.Listen = "127.0.0.1:2330"
	managementConflict.LeaseGateway.Enabled = true
	managementConflict.LeaseGateway.APIToken = "machine-token"
	if err := managementConflict.NormalizeWithPortMap(nil); err == nil || !strings.Contains(err.Error(), "management.listen") {
		t.Fatalf("management port conflict error = %v", err)
	}
}
