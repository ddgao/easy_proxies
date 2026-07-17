package subscription

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"easy_proxies/internal/config"
	"easy_proxies/internal/monitor"
)

// Logger defines logging interface.
type Logger interface {
	Infof(format string, args ...any)
	Warnf(format string, args ...any)
	Errorf(format string, args ...any)
}

// Option configures the Manager.
type Option func(*Manager)

// LeaseGenerationRefresher 在 Legacy reload 前构建、验证并提升 Lease Candidate。
type LeaseGenerationRefresher interface {
	RefreshLeaseGeneration(ctx context.Context, cfg *config.Config) error
}

// DegradedCapacityRecoverer 在完整拉取订阅前复检当前代际的失败节点。
// 返回 true 表示容量已恢复到准入门槛，此轮不应再构建 Candidate。
type DegradedCapacityRecoverer interface {
	RecoverDegradedCapacity(ctx context.Context) (bool, error)
}

// RuntimeReloader 是订阅模块应用新配置的 sing-box 运行引擎边界。
type RuntimeReloader interface {
	CurrentPortMap() map[string]uint16
	ReloadWithPortMap(cfg *config.Config, portMap map[string]uint16) error
}

// WithLogger sets a custom logger.
func WithLogger(l Logger) Option {
	return func(m *Manager) { m.logger = l }
}

// WithRecoveryBackoff 注入降级恢复退避序列；生产默认按分钟递增并封顶 30 分钟。
func WithRecoveryBackoff(delays []time.Duration) Option {
	return func(m *Manager) {
		m.recoveryBackoff = append([]time.Duration(nil), delays...)
	}
}

// Manager handles periodic subscription refresh.
type Manager struct {
	mu sync.RWMutex

	baseCfg        *config.Config
	boxMgr         RuntimeReloader
	logger         Logger
	httpClient     *http.Client // Custom HTTP client with connection pooling
	leaseRefresher LeaseGenerationRefresher

	status          monitor.SubscriptionStatus
	ctx             context.Context
	cancel          context.CancelFunc
	loopMu          sync.Mutex
	loopCancel      context.CancelFunc
	refreshMu       sync.Mutex // prevents concurrent refreshes
	flightMu        sync.Mutex
	flight          *refreshFlight
	recoveryMu      sync.Mutex
	recoveryBackoff []time.Duration
	recoveryAttempt int
	recoveryTimer   *time.Timer
	recoveryRunning bool
	manualRefresh   chan struct{}

	// Track nodes.txt content hash to detect modifications
	lastSubHash      string    // Hash of nodes.txt content after last subscription refresh
	lastNodesModTime time.Time // Last known modification time of nodes.txt
}

type refreshFlight struct {
	done     chan struct{}
	err      error
	revision uint64
}

// SetLeaseGenerationRefresher 绑定独立 Lease Runtime 的代际刷新入口。
func (m *Manager) SetLeaseGenerationRefresher(refresher LeaseGenerationRefresher) {
	m.mu.Lock()
	m.leaseRefresher = refresher
	m.mu.Unlock()
}

// New creates a SubscriptionManager.
func New(cfg *config.Config, boxMgr RuntimeReloader, opts ...Option) *Manager {
	ctx, cancel := context.WithCancel(context.Background())

	// Create optimized HTTP client with connection pooling
	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   10,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		ResponseHeaderTimeout: 10 * time.Second,
	}

	httpClient := &http.Client{
		Transport: transport,
		Timeout:   60 * time.Second, // Overall timeout
	}

	m := &Manager{
		baseCfg:       cfg,
		boxMgr:        boxMgr,
		ctx:           ctx,
		cancel:        cancel,
		manualRefresh: make(chan struct{}, 1),
		httpClient:    httpClient,
		status:        monitor.SubscriptionStatus{ConfigRevision: 1},
	}
	for _, opt := range opts {
		opt(m)
	}
	if m.logger == nil {
		m.logger = defaultLogger{}
	}
	if len(m.recoveryBackoff) == 0 {
		m.recoveryBackoff = []time.Duration{time.Minute, 2 * time.Minute, 4 * time.Minute, 8 * time.Minute, 16 * time.Minute, 30 * time.Minute}
	}
	return m
}

// Start begins the periodic refresh loop.
func (m *Manager) Start() {
	m.mu.RLock()
	enabled := m.baseCfg.SubscriptionRefresh.Enabled
	hasSubscriptions := len(m.baseCfg.Subscriptions) > 0
	interval := m.baseCfg.SubscriptionRefresh.Interval
	m.mu.RUnlock()
	if !enabled {
		m.logger.Infof("subscription refresh disabled")
		return
	}
	if !hasSubscriptions {
		m.logger.Infof("no subscriptions configured, refresh disabled")
		return
	}
	m.logger.Infof("starting subscription refresh, interval: %s", interval)
	m.restartRefreshLoop(interval, enabled)
}

// Stop stops the periodic refresh.
func (m *Manager) Stop() {
	m.loopMu.Lock()
	if m.loopCancel != nil {
		m.loopCancel()
		m.loopCancel = nil
	}
	m.loopMu.Unlock()
	if m.cancel != nil {
		m.cancel()
	}

	// Close idle connections
	if m.httpClient != nil {
		m.httpClient.CloseIdleConnections()
	}
	m.recoveryMu.Lock()
	if m.recoveryTimer != nil {
		m.recoveryTimer.Stop()
	}
	m.recoveryMu.Unlock()
}

// TriggerDegradedRecovery 在节点复检仍不足容量时启动统一恢复流程。
// 重复触发会合并到当前执行或已经安排的下一次退避重试。
func (m *Manager) TriggerDegradedRecovery() {
	m.recoveryMu.Lock()
	if m.recoveryRunning || m.recoveryTimer != nil {
		m.recoveryMu.Unlock()
		return
	}
	m.recoveryRunning = true
	m.recoveryMu.Unlock()
	go m.runDegradedRecovery()
}

func (m *Manager) runDegradedRecovery() {
	var err error
	recovered := false
	m.mu.RLock()
	recoverer, canRecover := m.leaseRefresher.(DegradedCapacityRecoverer)
	m.mu.RUnlock()
	if canRecover {
		m.mu.RLock()
		timeout := m.baseCfg.SubscriptionRefresh.HealthCheckTimeout
		m.mu.RUnlock()
		if timeout <= 0 {
			timeout = 30 * time.Second
		}
		ctx, cancel := context.WithTimeout(m.ctx, timeout)
		recovered, err = recoverer.RecoverDegradedCapacity(ctx)
		cancel()
	}
	if err == nil && !recovered {
		err = m.RefreshNow()
	}
	m.recoveryMu.Lock()
	m.recoveryRunning = false
	if err == nil {
		m.recoveryAttempt = 0
		m.recoveryTimer = nil
		m.mu.Lock()
		m.status.TriggerReason = "degraded"
		m.status.BackoffAttempt = 0
		m.status.NextRetry = time.Time{}
		m.mu.Unlock()
		m.recoveryMu.Unlock()
		return
	}
	index := m.recoveryAttempt
	if index >= len(m.recoveryBackoff) {
		index = len(m.recoveryBackoff) - 1
	}
	delay := m.recoveryBackoff[index]
	m.recoveryAttempt++
	nextRetry := time.Now().Add(delay)
	m.mu.Lock()
	m.status.TriggerReason = "degraded"
	m.status.BackoffAttempt = m.recoveryAttempt
	m.status.NextRetry = nextRetry
	m.mu.Unlock()
	m.recoveryTimer = time.AfterFunc(delay, func() {
		m.recoveryMu.Lock()
		m.recoveryTimer = nil
		if m.ctx.Err() != nil {
			m.recoveryMu.Unlock()
			return
		}
		m.recoveryRunning = true
		m.recoveryMu.Unlock()
		m.runDegradedRecovery()
	})
	m.recoveryMu.Unlock()
}

// UpdateConfig hot-reloads subscription URLs and refresh settings without restart.
func (m *Manager) UpdateConfig(urls []string, enabled bool, interval time.Duration) {
	m.mu.Lock()
	m.baseCfg.Subscriptions = urls
	m.baseCfg.SubscriptionRefresh.Enabled = enabled
	if interval > 0 {
		m.baseCfg.SubscriptionRefresh.Interval = interval
	}
	m.status.ConfigRevision++
	currentInterval := m.baseCfg.SubscriptionRefresh.Interval
	persistCfg := cloneConfig(m.baseCfg)
	m.mu.Unlock()

	// Persist to config.yaml
	if err := persistCfg.SaveSettings(); err != nil {
		m.logger.Errorf("failed to save subscription config: %v", err)
	}

	if len(urls) == 0 {
		m.restartRefreshLoop(currentInterval, false)
		m.logger.Infof("no subscription URLs configured, skipping refresh")
		return
	}

	m.logger.Infof("subscription config updated: %d URLs, enabled=%v, interval=%s", len(urls), enabled, currentInterval)
	m.restartRefreshLoop(currentInterval, enabled)
	// 配置变更始终触发一次刷新；若已有旧修订运行，该调用会等待并只执行最新修订。
	go func() {
		if err := m.RefreshNow(); err != nil {
			m.logger.Errorf("refresh after config update failed: %v", err)
		}
	}()
}

// UpdateConfigAndRefresh updates subscription config and synchronously waits for
// the first refresh to complete before returning. This ensures the caller (WebUI API)
// can confirm the update took effect.
func (m *Manager) UpdateConfigAndRefresh(urls []string, enabled bool, interval time.Duration) error {
	m.UpdateConfig(urls, enabled, interval)

	if len(urls) == 0 {
		return nil
	}

	return m.RefreshNow()
}

// RefreshNow triggers an immediate refresh.
func (m *Manager) RefreshNow() error {
	m.recoveryMu.Lock()
	if m.recoveryTimer != nil {
		m.recoveryTimer.Stop()
		m.recoveryTimer = nil
	}
	m.recoveryMu.Unlock()
	m.mu.RLock()
	timeout := m.baseCfg.SubscriptionRefresh.Timeout
	healthCheckTimeout := m.baseCfg.SubscriptionRefresh.HealthCheckTimeout
	m.mu.RUnlock()
	if timeout <= 0 {
		timeout = 30 * time.Second
	}

	ctx, cancel := context.WithTimeout(m.ctx, timeout+healthCheckTimeout)
	defer cancel()
	err := m.runRefresh(ctx)
	if err == nil {
		m.resetRecoveryBackoff()
	}
	return err
}

func (m *Manager) resetRecoveryBackoff() {
	m.recoveryMu.Lock()
	m.recoveryAttempt = 0
	m.recoveryMu.Unlock()
	m.mu.Lock()
	m.status.BackoffAttempt = 0
	m.status.NextRetry = time.Time{}
	m.mu.Unlock()
}

// runRefresh 让所有并发触发共享同一次抓取、构建和提升结果。
func (m *Manager) runRefresh(ctx context.Context) error {
	m.mu.RLock()
	requestedRevision := m.status.ConfigRevision
	m.mu.RUnlock()
	m.flightMu.Lock()
	if current := m.flight; current != nil {
		m.mu.Lock()
		m.status.SharedWaiters++
		m.mu.Unlock()
		m.flightMu.Unlock()
		defer func() {
			m.mu.Lock()
			m.status.SharedWaiters--
			m.mu.Unlock()
		}()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-current.done:
			if requestedRevision > current.revision {
				return m.runRefresh(ctx)
			}
			return current.err
		}
	}
	current := &refreshFlight{done: make(chan struct{}), revision: requestedRevision}
	m.flight = current
	m.flightMu.Unlock()
	m.mu.Lock()
	m.status.Phase = "FETCHING"
	refreshCfg := cloneConfig(m.baseCfg)
	m.mu.Unlock()

	m.doRefresh(ctx, refreshCfg)
	status := m.Status()
	if status.LastError != "" {
		current.err = fmt.Errorf("refresh failed: %s", status.LastError)
	}
	m.mu.Lock()
	if current.err != nil {
		m.status.Phase = "FAILED"
	} else {
		m.status.Phase = "COMPLETE"
	}
	m.mu.Unlock()
	m.flightMu.Lock()
	close(current.done)
	m.flight = nil
	m.flightMu.Unlock()
	return current.err
}

// Status returns the current refresh status.
func (m *Manager) Status() monitor.SubscriptionStatus {
	m.mu.RLock()
	status := m.status
	m.mu.RUnlock()

	// Check if nodes have been modified since last refresh
	status.NodesModified = m.CheckNodesModified()
	return status
}

// restartRefreshLoop 只重启定时触发器，不取消正在执行的刷新 flight。
// 这样配置修订发生在刷新中途时，旧修订可以完整结束，再由协调器执行最新修订。
func (m *Manager) restartRefreshLoop(interval time.Duration, autoEnabled bool) {
	m.loopMu.Lock()
	if m.loopCancel != nil {
		m.loopCancel()
		m.loopCancel = nil
	}
	if !autoEnabled || interval <= 0 {
		m.loopMu.Unlock()
		return
	}
	loopCtx, cancel := context.WithCancel(m.ctx)
	m.loopCancel = cancel
	m.loopMu.Unlock()
	go m.refreshLoop(loopCtx, interval)
}

// refreshLoop runs the periodic refresh.
func (m *Manager) refreshLoop(loopCtx context.Context, interval time.Duration) {
	autoEnabled := true

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	if autoEnabled {
		// Update next refresh time only when auto-refresh is enabled
		m.mu.Lock()
		m.status.NextRefresh = time.Now().Add(interval)
		m.mu.Unlock()
	}

	for {
		select {
		case <-loopCtx.Done():
			return
		case <-ticker.C:
			// Only do periodic refresh when auto-refresh is enabled
			if !autoEnabled {
				continue
			}
			_ = m.runRefresh(loopCtx)
			m.mu.Lock()
			m.status.NextRefresh = time.Now().Add(interval)
			m.mu.Unlock()
		case <-m.manualRefresh:
			// Always honor manual/immediate refresh regardless of enabled flag
			_ = m.runRefresh(m.ctx)
			if autoEnabled {
				ticker.Reset(interval)
				m.mu.Lock()
				m.status.NextRefresh = time.Now().Add(interval)
				m.mu.Unlock()
			}
		}
	}
}

// doRefresh performs a single refresh operation.
func (m *Manager) doRefresh(ctx context.Context, refreshCfg *config.Config) {
	// Prevent concurrent refreshes
	if !m.refreshMu.TryLock() {
		m.logger.Warnf("refresh already in progress, skipping")
		return
	}
	defer m.refreshMu.Unlock()

	m.mu.Lock()
	m.status.IsRefreshing = true
	m.mu.Unlock()

	defer func() {
		m.mu.Lock()
		m.status.IsRefreshing = false
		m.status.RefreshCount++
		m.mu.Unlock()
	}()

	m.logger.Infof("starting subscription refresh")

	// Fetch nodes from all subscriptions
	nodes, err := m.fetchSubscriptions(ctx, refreshCfg)
	if err != nil {
		m.logger.Errorf("fetch subscriptions failed: %v", err)
		m.mu.Lock()
		m.status.LastError = err.Error()
		m.status.LastRefresh = time.Now()
		m.mu.Unlock()
		return
	}

	if len(nodes) == 0 {
		m.logger.Warnf("no nodes fetched from subscriptions")
		m.mu.Lock()
		m.status.LastError = "no nodes fetched"
		m.status.LastRefresh = time.Now()
		m.mu.Unlock()
		return
	}

	m.logger.Infof("fetched %d nodes from subscriptions", len(nodes))

	// Write subscription nodes to nodes.txt
	nodesFilePath := getNodesFilePath(refreshCfg)
	if err := m.writeNodesToFile(nodesFilePath, nodes); err != nil {
		m.logger.Errorf("failed to write nodes.txt: %v", err)
		m.mu.Lock()
		m.status.LastError = fmt.Sprintf("write nodes.txt: %v", err)
		m.status.LastRefresh = time.Now()
		m.mu.Unlock()
		return
	}
	m.logger.Infof("written %d nodes to %s", len(nodes), nodesFilePath)

	// Update hash and mod time after writing
	newHash := m.computeNodesHash(nodes)
	m.mu.Lock()
	m.lastSubHash = newHash
	if info, err := os.Stat(nodesFilePath); err == nil {
		m.lastNodesModTime = info.ModTime()
	} else {
		m.lastNodesModTime = time.Now()
	}
	m.status.NodesModified = false
	m.mu.Unlock()

	// Get current port mapping to preserve existing node ports
	portMap := m.boxMgr.CurrentPortMap()

	// Create new config with updated nodes
	newCfg := createNewConfig(refreshCfg, nodes)

	m.mu.RLock()
	leaseRefresher := m.leaseRefresher
	m.mu.RUnlock()
	if leaseRefresher != nil {
		if err := leaseRefresher.RefreshLeaseGeneration(ctx, newCfg); err != nil {
			m.logger.Errorf("Lease Generation refresh failed: %v", err)
			m.mu.Lock()
			m.status.LastError = fmt.Sprintf("Lease Generation refresh: %v", err)
			m.status.LastRefresh = time.Now()
			m.mu.Unlock()
			return
		}
	}

	// Trigger BoxManager reload with port preservation
	if err := m.boxMgr.ReloadWithPortMap(newCfg, portMap); err != nil {
		m.logger.Errorf("reload failed: %v", err)
		m.mu.Lock()
		m.status.LastError = err.Error()
		m.status.LastRefresh = time.Now()
		m.mu.Unlock()
		return
	}

	m.mu.Lock()
	m.status.LastRefresh = time.Now()
	m.status.NodeCount = len(nodes)
	m.status.LastError = ""
	m.mu.Unlock()

	m.logger.Infof("subscription refresh completed, %d nodes active", len(nodes))
}

// getNodesFilePath returns the path to nodes.txt.
func (m *Manager) getNodesFilePath() string {
	m.mu.RLock()
	cfg := cloneConfig(m.baseCfg)
	m.mu.RUnlock()
	return getNodesFilePath(cfg)
}

func getNodesFilePath(cfg *config.Config) string {
	if cfg.NodesFile != "" {
		return cfg.NodesFile
	}
	return filepath.Join(filepath.Dir(cfg.FilePath()), "nodes.txt")
}

// writeNodesToFile writes nodes to a file (one URI per line).
func (m *Manager) writeNodesToFile(path string, nodes []config.NodeConfig) error {
	var lines []string
	for _, node := range nodes {
		lines = append(lines, node.URI)
	}
	content := strings.Join(lines, "\n")
	if len(lines) > 0 {
		content += "\n"
	}
	return os.WriteFile(path, []byte(content), 0o644)
}

// computeNodesHash computes a hash of node URIs for change detection.
func (m *Manager) computeNodesHash(nodes []config.NodeConfig) string {
	var uris []string
	for _, node := range nodes {
		uris = append(uris, node.URI)
	}
	content := strings.Join(uris, "\n")
	hash := sha256.Sum256([]byte(content))
	return hex.EncodeToString(hash[:])
}

// CheckNodesModified checks if nodes.txt has been modified since last refresh.
// Uses file modification time as a fast path to avoid unnecessary file reads.
func (m *Manager) CheckNodesModified() bool {
	m.mu.RLock()
	lastHash := m.lastSubHash
	lastMod := m.lastNodesModTime
	m.mu.RUnlock()

	if lastHash == "" {
		return false // No previous refresh, can't determine modification
	}

	nodesFilePath := m.getNodesFilePath()

	// Fast path: check modification time first
	info, err := os.Stat(nodesFilePath)
	if err != nil {
		return false // File doesn't exist or can't stat
	}
	modTime := info.ModTime()
	if !modTime.After(lastMod) {
		return false // File hasn't been modified
	}

	// Slow path: file was modified, compute hash
	data, err := os.ReadFile(nodesFilePath)
	if err != nil {
		return false // File doesn't exist or can't read
	}

	// Parse nodes from file content
	var nodes []config.NodeConfig
	lines := strings.Split(string(data), "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if config.IsProxyURI(line) {
			nodes = append(nodes, config.NodeConfig{URI: line})
		}
	}

	currentHash := m.computeNodesHash(nodes)
	changed := currentHash != lastHash

	// Update cached mod time
	m.mu.Lock()
	m.lastNodesModTime = modTime
	m.mu.Unlock()

	return changed
}

// MarkNodesModified updates the modification status.
func (m *Manager) MarkNodesModified() {
	m.mu.Lock()
	m.status.NodesModified = true
	m.mu.Unlock()
}

// fetchAllSubscriptions fetches nodes from all configured subscription URLs.
func (m *Manager) fetchAllSubscriptions() ([]config.NodeConfig, error) {
	m.mu.RLock()
	cfg := cloneConfig(m.baseCfg)
	m.mu.RUnlock()
	return m.fetchSubscriptions(m.ctx, cfg)
}

func (m *Manager) fetchSubscriptions(ctx context.Context, cfg *config.Config) ([]config.NodeConfig, error) {
	timeout := cfg.SubscriptionRefresh.Timeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	nodes, stats := config.FetchSubscriptionNodes(ctx, cfg.Subscriptions, config.SubscriptionFetchOptions{
		Timeout:     timeout,
		Concurrency: cfg.SubscriptionRefresh.FetchConcurrency,
		Client:      m.httpClient,
		Loggerf: func(format string, args ...any) {
			m.logger.Infof(format, args...)
		},
	})
	if stats.DedupedURLs > 0 || stats.DedupedNodes > 0 {
		m.logger.Infof("subscription dedupe summary: urls=%d, nodes=%d", stats.DedupedURLs, stats.DedupedNodes)
	}
	if cachedNodes, err := config.LoadNodesFromFile(getNodesFilePath(cfg)); err == nil && len(cachedNodes) > 0 {
		if len(nodes) == 0 {
			m.logger.Warnf("using %d cached subscription nodes because refresh returned no usable nodes", len(cachedNodes))
			return cachedNodes, nil
		}
		if stats.Failed > 0 && len(nodes) < len(cachedNodes) {
			m.logger.Warnf("keeping %d cached subscription nodes because partial refresh only returned %d nodes", len(cachedNodes), len(nodes))
			return cachedNodes, nil
		}
	}
	if len(nodes) == 0 && stats.LastError != nil {
		return nil, stats.LastError
	}
	return nodes, nil
}

// createNewConfig creates a new config with updated nodes while preserving other settings.
func (m *Manager) createNewConfig(nodes []config.NodeConfig) *config.Config {
	m.mu.RLock()
	baseCfg := cloneConfig(m.baseCfg)
	m.mu.RUnlock()
	return createNewConfig(baseCfg, nodes)
}

func createNewConfig(baseCfg *config.Config, nodes []config.NodeConfig) *config.Config {
	newCfg := *baseCfg

	// Mark all subscription nodes with proper source
	for i := range nodes {
		nodes[i].Source = config.NodeSourceSubscription
	}

	// Preserve inline nodes from base config (nodes defined directly in config.yaml)
	var inlineNodes []config.NodeConfig
	for _, node := range baseCfg.Nodes {
		if node.Source == config.NodeSourceInline {
			inlineNodes = append(inlineNodes, node)
		}
	}

	// Merge inline nodes with subscription nodes: inline nodes first, then subscription nodes
	mergedNodes := make([]config.NodeConfig, 0, len(inlineNodes)+len(nodes))
	mergedNodes = append(mergedNodes, inlineNodes...)
	mergedNodes = append(mergedNodes, nodes...)

	// Port and credential assignment is owned by NormalizeWithPortMap (invoked
	// via ReloadWithPortMap): it preserves the port of any node whose stable
	// identity is unchanged and assigns fresh, collision-free ports to the rest.
	// Pre-assigning sequential ports here would override that preservation and
	// could collide with a preserved port, so it is intentionally left to the
	// normalize step.

	// Process node names
	for i := range mergedNodes {
		mergedNodes[i].Name = strings.TrimSpace(mergedNodes[i].Name)
		mergedNodes[i].URI = strings.TrimSpace(mergedNodes[i].URI)

		// Auto-extract name from URI if not provided
		if mergedNodes[i].Name == "" {
			mergedNodes[i].Name = config.ExtractNodeName(mergedNodes[i].URI)
		}
		if mergedNodes[i].Name == "" {
			mergedNodes[i].Name = fmt.Sprintf("node-%d", i)
		}
	}

	newCfg.Nodes = mergedNodes
	return &newCfg
}

// cloneConfig 固化一次刷新使用的配置修订，避免运行中配置变更污染当前 flight。
func cloneConfig(source *config.Config) *config.Config {
	if source == nil {
		return &config.Config{}
	}
	cloned := *source
	cloned.Subscriptions = append([]string(nil), source.Subscriptions...)
	cloned.Nodes = append([]config.NodeConfig(nil), source.Nodes...)
	return &cloned
}

type defaultLogger struct{}

func (defaultLogger) Infof(format string, args ...any) {
	log.Printf("[subscription] "+format, args...)
}

func (defaultLogger) Warnf(format string, args ...any) {
	log.Printf("[subscription] WARN: "+format, args...)
}

func (defaultLogger) Errorf(format string, args ...any) {
	log.Printf("[subscription] ERROR: "+format, args...)
}
