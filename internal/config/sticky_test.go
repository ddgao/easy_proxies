package config

import "testing"

func TestNormalizeSticky(t *testing.T) {
	tests := []struct {
		name        string
		mode        string
		listenPort  uint16
		stickyEn    bool
		stickyPort  uint16
		nodePorts   []uint16
		wantErr     bool
		wantEnabled bool
		wantPort    uint16
	}{
		{
			name:     "disabled is a no-op",
			mode:     "pool",
			stickyEn: false,
		},
		{
			name:        "pool mode auto-assigns listener.port+1",
			mode:        "pool",
			listenPort:  2323,
			stickyEn:    true,
			wantEnabled: true,
			wantPort:    2324,
		},
		{
			name:        "explicit port is kept",
			mode:        "pool",
			listenPort:  2323,
			stickyEn:    true,
			stickyPort:  9000,
			wantEnabled: true,
			wantPort:    9000,
		},
		{
			name:       "conflict with listener port errors",
			mode:       "pool",
			listenPort: 2323,
			stickyEn:   true,
			stickyPort: 2323,
			wantErr:    true,
		},
		{
			name:        "non-pool mode disables sticky silently",
			mode:        "multi-port",
			listenPort:  2323,
			stickyEn:    true,
			wantEnabled: false,
		},
		{
			name:       "conflict with node port errors",
			mode:       "hybrid",
			listenPort: 2323,
			stickyEn:   true,
			stickyPort: 24001,
			nodePorts:  []uint16{24000, 24001},
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Config{Mode: tt.mode}
			c.Listener.Port = tt.listenPort
			c.Sticky.Enabled = tt.stickyEn
			c.Sticky.Port = tt.stickyPort
			for _, p := range tt.nodePorts {
				c.Nodes = append(c.Nodes, NodeConfig{Port: p})
			}

			err := c.normalizeSticky()
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if c.Sticky.Enabled != tt.wantEnabled {
				t.Errorf("Sticky.Enabled = %v, want %v", c.Sticky.Enabled, tt.wantEnabled)
			}
			if tt.wantEnabled && c.Sticky.Port != tt.wantPort {
				t.Errorf("Sticky.Port = %d, want %d", c.Sticky.Port, tt.wantPort)
			}
		})
	}
}
