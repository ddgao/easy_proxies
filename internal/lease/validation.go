package lease

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"
	"time"
)

// ValidationOptions 定义运行代际的节点准入规则。
// PrimaryTarget 必填，FallbackTarget 可选；任一目标返回 ExpectedStatus 即通过本轮验证。
type ValidationOptions struct {
	PrimaryTarget  string
	FallbackTarget string
	ExpectedStatus int
	MinReady       int
	Concurrency    int
	ProbeTimeout   time.Duration
	RetryDelay     time.Duration
	SkipCertVerify bool
}

// ValidationSnapshot 是管理 API 可安全展示的节点验证进度。
type ValidationSnapshot struct {
	Total      int    `json:"total"`
	MinReady   int    `json:"min_ready"`
	Pending    int    `json:"pending"`
	Validating int    `json:"validating"`
	Ready      int    `json:"ready"`
	Failed     int    `json:"failed"`
	LastError  string `json:"last_error,omitempty"`
}

// ValidationRun 代表一次运行代际验证。Ready 通道按节点完成验证的真实顺序输出，
// 调用方达到最小容量后可以立即提升，剩余节点继续在后台输出。
type ValidationRun struct {
	ready      chan Node
	done       chan struct{}
	total      int
	minReady   int
	validating atomic.Int64
	readyCount atomic.Int64
	failed     atomic.Int64
	completed  atomic.Bool
	errMu      sync.RWMutex
	lastError  string
	stateMu    sync.RWMutex
	nodeStates map[string]string
}

// StartValidation 启动节点验证，不阻塞等待全部节点完成。
func StartValidation(ctx context.Context, nodes []Node, opts ValidationOptions) (*ValidationRun, error) {
	targets, err := validationTargets(opts)
	if err != nil {
		return nil, err
	}
	if opts.MinReady <= 0 {
		opts.MinReady = 50
	}
	if len(nodes) == 0 {
		return nil, errors.New("candidate has no nodes")
	}
	if opts.Concurrency <= 0 {
		opts.Concurrency = 32
	}
	if opts.Concurrency > len(nodes) {
		opts.Concurrency = len(nodes)
	}
	if opts.ProbeTimeout <= 0 {
		opts.ProbeTimeout = 8 * time.Second
	}
	if opts.RetryDelay <= 0 {
		opts.RetryDelay = 5 * time.Second
	}

	run := &ValidationRun{
		ready:      make(chan Node, len(nodes)),
		done:       make(chan struct{}),
		total:      len(nodes),
		minReady:   opts.MinReady,
		nodeStates: make(map[string]string, len(nodes)),
	}
	for _, node := range nodes {
		run.nodeStates[node.Key] = "PENDING"
	}
	jobs := make(chan Node)
	var workers sync.WaitGroup
	workers.Add(opts.Concurrency)
	for i := 0; i < opts.Concurrency; i++ {
		go func() {
			defer workers.Done()
			for node := range jobs {
				run.setNodeState(node.Key, "VALIDATING")
				run.validating.Add(1)
				err := validateNode(ctx, node, targets, opts)
				run.validating.Add(-1)
				if err != nil {
					run.failed.Add(1)
					run.setLastError(err.Error())
					run.setNodeState(node.Key, "FAILED")
					continue
				}
				run.readyCount.Add(1)
				run.setNodeState(node.Key, "READY")
				select {
				case <-ctx.Done():
					return
				case run.ready <- node:
				}
			}
		}()
	}
	go func() {
		defer close(jobs)
		for _, node := range nodes {
			select {
			case <-ctx.Done():
				return
			case jobs <- node:
			}
		}
	}()
	go func() {
		workers.Wait()
		run.completed.Store(true)
		close(run.ready)
		close(run.done)
	}()
	return run, nil
}

// ValidateNode 使用与代际准入相同的主备目标、TLS、状态码和复测规则复检单个节点。
func ValidateNode(ctx context.Context, node Node, opts ValidationOptions) error {
	targets, err := validationTargets(opts)
	if err != nil {
		return err
	}
	if opts.ProbeTimeout <= 0 {
		opts.ProbeTimeout = 8 * time.Second
	}
	if opts.RetryDelay <= 0 {
		opts.RetryDelay = 5 * time.Second
	}
	return validateNode(ctx, node, targets, opts)
}

// WaitMinimum 按 Ready 完成顺序收集节点，到达门槛立即返回，不等待慢尾节点。
func (r *ValidationRun) WaitMinimum(ctx context.Context, minimum int) ([]Node, error) {
	ready := make([]Node, 0, minimum)
	for len(ready) < minimum {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case node, ok := <-r.ready:
			if !ok {
				snapshot := r.Snapshot()
				err := fmt.Errorf("only %d/%d nodes ready, need %d: %s", snapshot.Ready, snapshot.Total, minimum, snapshot.LastError)
				r.setLastError(err.Error())
				return nil, err
			}
			ready = append(ready, node)
		}
	}
	return ready, nil
}

// Ready 返回达到门槛后仍继续完成验证的节点流。
func (r *ValidationRun) Ready() <-chan Node {
	return r.ready
}

// Snapshot 返回原子进度视图。
func (r *ValidationRun) Snapshot() ValidationSnapshot {
	ready := int(r.readyCount.Load())
	failed := int(r.failed.Load())
	validating := int(r.validating.Load())
	pending := r.total - ready - failed - validating
	if pending < 0 {
		pending = 0
	}
	r.errMu.RLock()
	lastError := r.lastError
	r.errMu.RUnlock()
	return ValidationSnapshot{Total: r.total, MinReady: r.minReady, Pending: pending, Validating: validating, Ready: ready, Failed: failed, LastError: lastError}
}

func (r *ValidationRun) setLastError(message string) {
	r.errMu.Lock()
	r.lastError = message
	r.errMu.Unlock()
}

// NodeState 返回单个 Node Key 在本轮准入中的状态。
func (r *ValidationRun) NodeState(nodeKey string) string {
	r.stateMu.RLock()
	defer r.stateMu.RUnlock()
	return r.nodeStates[nodeKey]
}

func (r *ValidationRun) setNodeState(nodeKey, state string) {
	r.stateMu.Lock()
	r.nodeStates[nodeKey] = state
	r.stateMu.Unlock()
}

type validationTarget struct {
	url            *url.URL
	expectedStatus int
}

func validationTargets(opts ValidationOptions) ([]validationTarget, error) {
	if opts.ExpectedStatus == 0 {
		opts.ExpectedStatus = http.StatusNoContent
	}
	if opts.ExpectedStatus < 100 || opts.ExpectedStatus > 599 {
		return nil, fmt.Errorf("invalid expected probe status %d", opts.ExpectedStatus)
	}
	rawTargets := []string{opts.PrimaryTarget}
	if opts.FallbackTarget != "" {
		rawTargets = append(rawTargets, opts.FallbackTarget)
	}
	targets := make([]validationTarget, 0, len(rawTargets))
	for _, raw := range rawTargets {
		parsed, err := url.Parse(raw)
		if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
			return nil, fmt.Errorf("invalid probe target %q", raw)
		}
		targets = append(targets, validationTarget{url: parsed, expectedStatus: opts.ExpectedStatus})
	}
	return targets, nil
}

func validateNode(ctx context.Context, node Node, targets []validationTarget, opts ValidationOptions) error {
	if err := validateRound(ctx, node, targets, opts); err == nil {
		return nil
	}
	timer := time.NewTimer(opts.RetryDelay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
	}
	return validateRound(ctx, node, targets, opts)
}

func validateRound(ctx context.Context, node Node, targets []validationTarget, opts ValidationOptions) error {
	var failures []error
	for _, target := range targets {
		transport := &http.Transport{
			Proxy:               nil,
			DialContext:         node.Dialer.DialContext,
			DisableKeepAlives:   true,
			TLSClientConfig:     &tls.Config{InsecureSkipVerify: opts.SkipCertVerify},
			TLSHandshakeTimeout: opts.ProbeTimeout,
		}
		client := &http.Client{Transport: transport, Timeout: opts.ProbeTimeout}
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, target.url.String(), nil)
		if err != nil {
			transport.CloseIdleConnections()
			failures = append(failures, err)
			continue
		}
		response, err := client.Do(request)
		if err == nil {
			_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 32*1024))
			_ = response.Body.Close()
		}
		transport.CloseIdleConnections()
		if err == nil && response.StatusCode == target.expectedStatus {
			return nil
		}
		if err != nil {
			reason := "request failed"
			if errors.Is(err, context.DeadlineExceeded) {
				reason = "request timed out"
			}
			failures = append(failures, fmt.Errorf("%s %s", safeValidationTarget(target.url), reason))
		} else {
			failures = append(failures, fmt.Errorf("%s returned HTTP %d, expected %d", safeValidationTarget(target.url), response.StatusCode, target.expectedStatus))
		}
	}
	return fmt.Errorf("validate Node Key %s: %w", node.Key, errors.Join(failures...))
}

func safeValidationTarget(target *url.URL) string {
	if target == nil {
		return "probe target"
	}
	return target.Scheme + "://" + target.Host
}
