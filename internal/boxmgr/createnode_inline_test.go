package boxmgr

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"easy_proxies/internal/config"
	"easy_proxies/internal/monitor"
)

// TestCreateNode_AlwaysInlineEvenWithSubscription is the regression test for the
// "WebUI-added node lost after subscription refresh" bug (issue #29). A node
// added through the WebUI is an explicit user configuration and must be stored
// as an inline node in config.yaml, even when subscriptions are configured.
// Classifying it as a subscription/file source routed it to nodes.txt, which the
// next subscription refresh overwrites — silently dropping the node.
func TestCreateNode_AlwaysInlineEvenWithSubscription(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.yaml")

	// Minimal on-disk config with a subscription configured. SaveNodes reads this
	// file back to preserve structure, so it must exist.
	if err := os.WriteFile(cfgPath, []byte(`mode: pool
subscriptions:
  - https://example.com/sub
nodes: []
`), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg := &config.Config{
		Mode:          "pool",
		Subscriptions: []string{"https://example.com/sub"},
		Nodes:         []config.NodeConfig{},
	}
	cfg.SetFilePath(cfgPath)

	m := New(cfg, monitor.Config{})

	created, err := m.CreateNode(context.Background(), config.NodeConfig{
		Name: "ManualNode",
		URI:  "vless://uuid-a@a.example.com:443?type=ws&security=tls",
	})
	if err != nil {
		t.Fatalf("CreateNode: %v", err)
	}

	if created.Source != config.NodeSourceInline {
		t.Errorf("WebUI-added node source = %q, want %q (must survive subscription refresh)",
			created.Source, config.NodeSourceInline)
	}

	// The node must be persisted as an inline node in config.yaml, not nodes.txt.
	data, err := os.ReadFile(cfgPath)
	if err != nil {
		t.Fatalf("read config back: %v", err)
	}
	if !strings.Contains(string(data), "a.example.com") {
		t.Errorf("config.yaml should contain the inline node URI, got:\n%s", data)
	}
}
