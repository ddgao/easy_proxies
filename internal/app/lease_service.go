package app

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"easy_proxies/internal/boxmgr"
	"easy_proxies/internal/config"
	"easy_proxies/internal/lease"
	"easy_proxies/internal/monitor"
)

// leaseService 管理稳定 Lease Gateway 监听器和进程内租约运行时的共同生命周期。
// 它不拥有 NodeDialer；底层 sing-box 的关闭顺序由 app.Run 保证晚于 Gateway 停止。
type leaseService struct {
	runtime            *lease.Runtime
	server             *http.Server
	serveDone          chan error
	ctx                context.Context
	cancel             context.CancelFunc
	cfg                *config.Config
	promoteMu          sync.Mutex
	generationSequence uint64
	buildCandidate     func(context.Context, *config.Config, string) (lease.Candidate, error)
	pendingCfg         *config.Config
	pendingWake        chan struct{}
	once               sync.Once
	closeErr           error
}

// RecoverDegradedCapacity 复检当前 Active 的失败节点，容量恢复后避免无意义的订阅重抓和代际重建。
func (s *leaseService) RecoverDegradedCapacity(ctx context.Context) (bool, error) {
	if s == nil || s.runtime == nil {
		return false, errors.New("Lease Runtime is not available")
	}
	return s.runtime.RecoverDegradedCapacity(ctx)
}

// startLeaseService 根据配置启动真实 Gateway，并将同一个 Runtime 绑定到管理 API。
// 节点拨号器来自独立 Lease-only 代际，管理层不依赖 sing-box 内部对象。
func startLeaseService(ctx context.Context, cfg *config.Config, management *monitor.Server, nodes []lease.Node) (*leaseService, error) {
	return startLeaseServiceWithCandidate(ctx, cfg, management, lease.Candidate{ID: "generation-1", Nodes: nodes})
}

// startLeaseServiceWithCandidate 验证并接管初始 Lease-only Candidate 的生命周期。
func startLeaseServiceWithCandidate(ctx context.Context, cfg *config.Config, management *monitor.Server, candidate lease.Candidate) (*leaseService, error) {
	if cfg == nil || !cfg.LeaseGateway.Enabled {
		return nil, errors.New("Lease Gateway is not enabled")
	}
	if management == nil {
		return nil, errors.New("Lease Gateway requires management server")
	}
	serviceCtx, serviceCancel := context.WithCancel(ctx)
	address := net.JoinHostPort(cfg.LeaseGateway.Listen, strconv.Itoa(int(cfg.LeaseGateway.Port)))
	listener, err := net.Listen("tcp", address)
	if err != nil {
		serviceCancel()
		return nil, fmt.Errorf("listen Lease Gateway on %s: %w", address, err)
	}

	candidateCtx, managedCandidate := manageCandidate(serviceCtx, candidate)
	validation, err := lease.StartValidation(candidateCtx, managedCandidate.Nodes, lease.ValidationOptions{
		PrimaryTarget:  cfg.Management.ProbeTarget,
		FallbackTarget: cfg.LeaseGateway.ProbeFallbackTarget,
		ExpectedStatus: cfg.LeaseGateway.ProbeExpectedStatus,
		MinReady:       cfg.LeaseGateway.MinReadyNodes,
		Concurrency:    cfg.ProbeConcurrencyOrDefault(),
		ProbeTimeout:   cfg.SubscriptionRefresh.HealthCheckTimeout,
		RetryDelay:     5 * time.Second,
		SkipCertVerify: cfg.SkipCertVerify,
	})
	if err != nil {
		serviceCancel()
		_ = listener.Close()
		return nil, errors.Join(fmt.Errorf("start Lease Gateway validation: %w", err), closeCandidate(managedCandidate))
	}

	proxyHost := strings.TrimSpace(cfg.ExternalIP)
	if proxyHost == "" {
		proxyHost = cfg.LeaseGateway.Listen
		if ip := net.ParseIP(proxyHost); ip != nil && ip.IsUnspecified() {
			// 通配监听地址只用于绑定套接字，调用方需要一个实际可连接的返回地址。
			proxyHost = "127.0.0.1"
		}
	}
	proxyURL := (&url.URL{
		Scheme: "http",
		Host:   net.JoinHostPort(proxyHost, strconv.Itoa(int(cfg.LeaseGateway.Port))),
	}).String()
	runtime, err := lease.NewPendingRuntime(lease.Options{
		APIToken:               cfg.LeaseGateway.APIToken,
		ProxyURL:               proxyURL,
		TokenBytes:             32,
		MaxConnections:         cfg.LeaseGateway.MaxConnections,
		GenerationID:           managedCandidate.ID,
		GenerationClose:        managedCandidate.Close,
		NodeRecheck:            leaseNodeRechecker(cfg),
		MinReadyCapacity:       cfg.LeaseGateway.MinReadyNodes,
		AcquireWaitTimeout:     cfg.LeaseGateway.AcquireWaitTimeout,
		DrainTimeout:           cfg.LeaseGateway.DrainTimeout,
		GenerationDrainTimeout: cfg.LeaseGateway.GenerationDrainTimeout,
	}, managedCandidate, validation)
	if err != nil {
		serviceCancel()
		_ = listener.Close()
		return nil, errors.Join(fmt.Errorf("create Lease Runtime: %w", err), closeCandidate(managedCandidate))
	}

	service := &leaseService{
		runtime: runtime,
		server: &http.Server{
			Handler:           runtime.GatewayHandler(),
			ReadHeaderTimeout: 10 * time.Second,
			IdleTimeout:       60 * time.Second,
			MaxHeaderBytes:    32 << 10,
		},
		serveDone:          make(chan error, 1),
		ctx:                serviceCtx,
		cancel:             serviceCancel,
		cfg:                cfg,
		generationSequence: 1,
		buildCandidate:     boxmgr.BuildLeaseCandidate,
		pendingWake:        make(chan struct{}, 1),
	}
	go func() {
		readyNodes, waitErr := validation.WaitMinimum(serviceCtx, cfg.LeaseGateway.MinReadyNodes)
		if waitErr != nil {
			runtime.DiscardCandidate(managedCandidate.ID)
			_ = closeCandidate(managedCandidate)
			return
		}
		if promoteErr := runtime.Promote(managedCandidate, readyNodes, validation); promoteErr != nil {
			runtime.DiscardCandidate(managedCandidate.ID)
			_ = closeCandidate(managedCandidate)
			return
		}
		for node := range validation.Ready() {
			runtime.AddReadyNode(managedCandidate.ID, node)
		}
	}()
	go service.runPendingRefresh()
	management.SetLeaseController(runtime)
	go func() {
		err := service.server.Serve(listener)
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		service.serveDone <- err
	}()
	go func() {
		<-serviceCtx.Done()
		_ = service.Close(context.Background())
	}()
	return service, nil
}

// Promote 构建并验证 Candidate，达到最小容量后原子提升。
// 验证失败或运行时拒绝提升时必须关闭 Candidate，当前 Active 保持不变。
func (s *leaseService) Promote(ctx context.Context, candidate lease.Candidate) error {
	if s == nil || s.runtime == nil {
		return errors.New("Lease Gateway is not running")
	}
	s.promoteMu.Lock()
	defer s.promoteMu.Unlock()
	if err := s.promoteLocked(ctx, candidate, s.cfg); err != nil {
		return err
	}
	const generationPrefix = "generation-"
	if strings.HasPrefix(candidate.ID, generationPrefix) {
		if sequence, err := strconv.ParseUint(strings.TrimPrefix(candidate.ID, generationPrefix), 10, 64); err == nil && sequence > s.generationSequence {
			s.generationSequence = sequence
		}
	}
	return nil
}

// RefreshLeaseGeneration 根据最新订阅配置构建独立 Candidate，并在健康门槛满足后提升。
// Candidate 构建或验证失败只关闭候选代，当前 Active 及其既有租约保持不变。
func (s *leaseService) RefreshLeaseGeneration(ctx context.Context, nextCfg *config.Config) error {
	if s == nil || s.runtime == nil {
		return errors.New("Lease Gateway is not running")
	}
	if nextCfg == nil {
		return errors.New("Lease Generation config is required")
	}

	s.promoteMu.Lock()
	defer s.promoteMu.Unlock()
	if !s.runtime.CanBeginCandidate() {
		// 排空期间只保留最后一次配置快照，较旧刷新结果会被后来的调用覆盖。
		s.pendingCfg = nextCfg
		s.runtime.SetPendingRefresh(true)
		select {
		case s.pendingWake <- struct{}{}:
		default:
		}
		return nil
	}
	return s.refreshLeaseGenerationLocked(ctx, nextCfg)
}

func (s *leaseService) refreshLeaseGenerationLocked(ctx context.Context, nextCfg *config.Config) error {
	s.generationSequence++
	generationID := fmt.Sprintf("generation-%d", s.generationSequence)
	candidate, err := s.buildCandidate(s.ctx, nextCfg, generationID)
	if err != nil {
		return fmt.Errorf("build Lease Candidate %s: %w", generationID, err)
	}
	if err := s.promoteLocked(ctx, candidate, nextCfg); err != nil {
		return fmt.Errorf("promote Lease Candidate %s: %w", generationID, err)
	}
	s.cfg = nextCfg
	s.runtime.SetNodeRecheck(leaseNodeRechecker(nextCfg))
	return nil
}

func leaseNodeRechecker(cfg *config.Config) lease.NodeRecheckFunc {
	return func(ctx context.Context, node lease.Node) bool {
		return lease.ValidateNode(ctx, node, lease.ValidationOptions{
			PrimaryTarget: cfg.Management.ProbeTarget, FallbackTarget: cfg.LeaseGateway.ProbeFallbackTarget,
			ExpectedStatus: cfg.LeaseGateway.ProbeExpectedStatus,
			ProbeTimeout:   cfg.SubscriptionRefresh.HealthCheckTimeout, RetryDelay: 5 * time.Second,
			SkipCertVerify: cfg.SkipCertVerify,
		}) == nil
	}
}

// runPendingRefresh 在 Draining 退出后消费最后保存的订阅快照。
func (s *leaseService) runPendingRefresh() {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-s.pendingWake:
		case <-ticker.C:
		}
		s.promoteMu.Lock()
		if s.pendingCfg == nil || !s.runtime.CanBeginCandidate() {
			s.promoteMu.Unlock()
			continue
		}
		pendingCfg := s.pendingCfg
		s.pendingCfg = nil
		s.runtime.SetPendingRefresh(false)
		_ = s.refreshLeaseGenerationLocked(s.ctx, pendingCfg)
		s.promoteMu.Unlock()
	}
}

// promoteLocked 执行单个候选代的探测和原子提升；调用方必须持有 promoteMu。
func (s *leaseService) promoteLocked(ctx context.Context, candidate lease.Candidate, validationCfg *config.Config) error {
	candidateCtx, managedCandidate := manageCandidate(s.ctx, candidate)
	validation, err := lease.StartValidation(candidateCtx, managedCandidate.Nodes, lease.ValidationOptions{
		PrimaryTarget:  validationCfg.Management.ProbeTarget,
		FallbackTarget: validationCfg.LeaseGateway.ProbeFallbackTarget,
		ExpectedStatus: validationCfg.LeaseGateway.ProbeExpectedStatus,
		MinReady:       validationCfg.LeaseGateway.MinReadyNodes,
		Concurrency:    validationCfg.ProbeConcurrencyOrDefault(),
		ProbeTimeout:   validationCfg.SubscriptionRefresh.HealthCheckTimeout,
		RetryDelay:     5 * time.Second,
		SkipCertVerify: validationCfg.SkipCertVerify,
	})
	if err != nil {
		return errors.Join(err, closeCandidate(managedCandidate))
	}
	if err := s.runtime.BeginCandidate(managedCandidate.ID, len(managedCandidate.Nodes), validation); err != nil {
		return errors.Join(err, closeCandidate(managedCandidate))
	}
	if err := s.runtime.AttachCandidateCloser(managedCandidate.ID, managedCandidate.Close); err != nil {
		s.runtime.DiscardCandidate(managedCandidate.ID)
		return errors.Join(err, closeCandidate(managedCandidate))
	}
	if err := s.runtime.AttachCandidateNodes(managedCandidate.ID, managedCandidate.Nodes); err != nil {
		s.runtime.DiscardCandidate(managedCandidate.ID)
		return errors.Join(err, closeCandidate(managedCandidate))
	}
	readyNodes, err := validation.WaitMinimum(ctx, validationCfg.LeaseGateway.MinReadyNodes)
	if err != nil {
		s.runtime.DiscardCandidate(managedCandidate.ID)
		return errors.Join(err, closeCandidate(managedCandidate))
	}
	if err := s.runtime.Promote(managedCandidate, readyNodes, validation); err != nil {
		s.runtime.DiscardCandidate(managedCandidate.ID)
		return errors.Join(err, closeCandidate(managedCandidate))
	}
	go func(generationID string) {
		for node := range validation.Ready() {
			s.runtime.AddReadyNode(generationID, node)
		}
	}(managedCandidate.ID)
	return nil
}

// manageCandidate 将验证协程和候选 sing-box 绑定为同一生命周期。
// 关闭顺序固定为先取消探测、再关闭拨号器，避免探测协程继续使用已销毁的出站对象。
func manageCandidate(parent context.Context, candidate lease.Candidate) (context.Context, lease.Candidate) {
	candidateCtx, cancel := context.WithCancel(parent)
	originalClose := candidate.Close
	var once sync.Once
	var closeErr error
	candidate.Close = func() error {
		once.Do(func() {
			cancel()
			if originalClose != nil {
				closeErr = originalClose()
			}
		})
		return closeErr
	}
	return candidateCtx, candidate
}

func closeCandidate(candidate lease.Candidate) error {
	if candidate.Close == nil {
		return nil
	}
	return candidate.Close()
}

// SetDegradedTrigger 将 Lease Runtime 的容量告警接入订阅恢复协调器。
func (s *leaseService) SetDegradedTrigger(trigger func()) {
	if s != nil && s.runtime != nil {
		s.runtime.SetDegradedTrigger(trigger)
	}
}

// Close 停止新代理连接后使全部 Lease Token 失效；重复调用保持幂等。
func (s *leaseService) Close(ctx context.Context) error {
	if s == nil {
		return nil
	}
	s.once.Do(func() {
		s.cancel()
		shutdownErr := s.server.Shutdown(ctx)
		serveErr := <-s.serveDone
		runtimeErr := s.runtime.Close()
		s.closeErr = errors.Join(serveErr, shutdownErr, runtimeErr)
	})
	return s.closeErr
}
