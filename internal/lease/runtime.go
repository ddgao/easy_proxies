// Package lease 提供租约冲突域内的节点独占及稳定 HTTP 代理入口运行时能力。
//
// 当前包只负责 Lease Gateway 的调用方契约，不接管 Legacy 代理入口。节点拨号器由
// 上游运行时代际注入，使后续代际切换可以替换底层 sing-box，而不改变租约控制接口。
package lease

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

const gatewayUsername = "lease"

var (
	// ErrNoNodeAvailable 表示当前没有可分配的空闲节点；后续分配器会在此基础上增加 FIFO 等待。
	ErrNoNodeAvailable = errors.New("no available proxy node")
	// ErrInvalidLabel 表示租约标签超过允许的非敏感观测字段边界。
	ErrInvalidLabel = errors.New("lease label must not exceed 64 characters")
	// ErrInvalidConflictKey 表示租约冲突键超过机器字段的输入边界。
	ErrInvalidConflictKey = errors.New("lease conflict key must be valid UTF-8 and not exceed 128 bytes")
	// ErrGenerationLimit 表示已有排空代际时不能提升第三个运行代际。
	ErrGenerationLimit = errors.New("a draining generation is still active")
	// ErrAcquireTimeout 表示申请在等待边界内没有获得空闲节点。
	ErrAcquireTimeout = errors.New("lease acquire wait timeout")
	// ErrAllocationPaused 表示管理员暂停了新租约分配。
	ErrAllocationPaused = errors.New("lease allocation is paused")
)

// GenerationRole 表示运行代际在租约分配链路中的职责。
type GenerationRole string

const (
	// GenerationRoleActive 表示唯一允许分配新租约的当前代际。
	GenerationRoleActive GenerationRole = "ACTIVE"
	// GenerationRoleCandidate 表示正在验证、尚不允许分配租约的候选代际。
	GenerationRoleCandidate GenerationRole = "CANDIDATE"
	// GenerationRoleDraining 表示只服务既有租约、不再接收新租约的排空代际。
	GenerationRoleDraining GenerationRole = "DRAINING"
)

// LeaseState 表示租约是否仍接受新代理连接。
type LeaseState string

const (
	// LeaseStateActive 表示租约有效并接受新连接。
	LeaseStateActive LeaseState = "ACTIVE"
	// LeaseStateDraining 表示租约已释放或到期，只等待已有连接结束。
	LeaseStateDraining LeaseState = "DRAINING"
	// LeaseStateBroken 表示绑定节点复检失败，租约永久拒绝新连接。
	LeaseStateBroken LeaseState = "BROKEN"
)

// NodeDialer 表示一个已经绑定到确定 Node Key 的 TCP 拨号能力。
// 实现不得在失败时切换到其他节点，否则会破坏一个业务任务固定节点的租约语义。
type NodeDialer interface {
	DialContext(ctx context.Context, network, address string) (net.Conn, error)
}

// Node 是 Lease Runtime 可分配的最小节点描述。
type Node struct {
	Key    string
	Dialer NodeDialer
}

// NodeRecheckFunc 在代理失败后独立验证同一个 Node Key，不得替换节点。
type NodeRecheckFunc func(context.Context, Node) bool

// Timer 是运行时 TTL 与排空边界所需的最小定时器接口。
type Timer interface {
	Stop() bool
}

// Options 定义 Lease Runtime 的稳定入口、机器凭据和初始节点集合。
type Options struct {
	APIToken               string
	ProxyURL               string
	TokenBytes             int
	TTL                    time.Duration
	AcquireWaitTimeout     time.Duration
	DrainTimeout           time.Duration
	GenerationDrainTimeout time.Duration
	NodeRecheck            NodeRecheckFunc
	MinReadyCapacity       int
	DegradedTrigger        func()
	Now                    func() time.Time
	AfterFunc              func(time.Duration, func()) Timer
	Nodes                  []Node
	GenerationID           string
	GenerationClose        func() error
}

// Candidate 是已经构建但尚未提升的候选运行代际。
type Candidate struct {
	ID    string
	Nodes []Node
	Close func() error
}

// Grant 是申请成功后仅返回一次的租约凭据。
// ProxyURL 已包含 Gateway Basic 凭据，可直接交给标准 HTTP 客户端使用。
type Grant struct {
	NodeKey      string    `json:"node_key"`
	LeaseToken   string    `json:"lease_token"`
	ProxyURL     string    `json:"proxy_url"`
	Label        string    `json:"label,omitempty"`
	ExpiresAt    time.Time `json:"expires_at"`
	GenerationID string    `json:"generation_id"`
}

// AcquireRequest 是管理面提交给租约运行时的申请参数。
// ConflictKey 原文只在本次调用中存在，运行时仅保留其哈希和脱敏指纹。
type AcquireRequest struct {
	Label       string
	ConflictKey string
}

// LeaseSummary 是不含可用凭据的租约观测信息。
type LeaseSummary struct {
	TokenFingerprint    string     `json:"token_fingerprint"`
	ConflictFingerprint string     `json:"conflict_fingerprint"`
	NodeKey             string     `json:"node_key"`
	Label               string     `json:"label,omitempty"`
	ExpiresAt           time.Time  `json:"expires_at"`
	GenerationID        string     `json:"generation_id"`
	State               LeaseState `json:"state"`
	ActiveConnections   int        `json:"active_connections"`
}

// GenerationSummary 是运行时快照中的代际角色和容量摘要。
type GenerationSummary struct {
	ID                   string         `json:"id"`
	Role                 GenerationRole `json:"role"`
	BuildPhase           string         `json:"build_phase"`
	NodeCount            int            `json:"node_count"`
	ReadyCount           int            `json:"ready_count"`
	FailedCount          int            `json:"failed_count"`
	PendingCount         int            `json:"pending_count"`
	AddedNodeCount       int            `json:"added_node_count"`
	UnchangedNodeCount   int            `json:"unchanged_node_count"`
	ErrorSummary         string         `json:"error_summary,omitempty"`
	RetiredNodeCount     int            `json:"retired_node_count"`
	RemainingLeases      int            `json:"remaining_leases"`
	RemainingConnections int            `json:"remaining_connections"`
	DrainDeadline        time.Time      `json:"drain_deadline,omitempty"`
}

// RuntimeEvent 是租约运行时对外发布的有序、脱敏状态事件。
type RuntimeEvent struct {
	Sequence            uint64    `json:"sequence"`
	Time                time.Time `json:"time"`
	Type                string    `json:"type"`
	GenerationID        string    `json:"generation_id,omitempty"`
	NodeKey             string    `json:"node_key,omitempty"`
	Fingerprint         string    `json:"lease_fingerprint,omitempty"`
	Reason              string    `json:"reason,omitempty"`
	Operator            string    `json:"operator,omitempty"`
	Target              string    `json:"target,omitempty"`
	Result              string    `json:"result,omitempty"`
	BeforeState         string    `json:"before_state,omitempty"`
	Error               string    `json:"error,omitempty"`
	AffectedLeases      int       `json:"affected_leases,omitempty"`
	AffectedConnections int       `json:"affected_connections,omitempty"`
}

// NodeSummary 以 Generation ID 与 Node Key 组合描述节点在租约运行时中的状态。
type NodeSummary struct {
	GenerationID     string         `json:"generation_id"`
	NodeKey          string         `json:"node_key"`
	Role             GenerationRole `json:"generation_role"`
	Ready            bool           `json:"ready"`
	Idle             bool           `json:"idle"`
	Leased           bool           `json:"leased"`
	ActiveLeaseCount int            `json:"active_lease_count"`
	Unavailable      bool           `json:"unavailable"`
	Blocked          bool           `json:"blocked"`
	Retired          bool           `json:"retired"`
	ValidationState  string         `json:"validation_state,omitempty"`
}

// GatewayMetrics 是 Lease Gateway 独立于 Legacy 流量的累计指标。
type GatewayMetrics struct {
	AuthenticationFailures uint64 `json:"authentication_failures"`
	InvalidTokens          uint64 `json:"invalid_tokens"`
	ProxyFailures          uint64 `json:"proxy_failures"`
	NodeRechecks           uint64 `json:"node_rechecks"`
	ActiveConnections      int    `json:"active_connections"`
}

// RefreshSummary 是统一运行时快照中的订阅刷新协调状态。
// 它只包含观测信息，不携带订阅 URL、节点凭据或其他敏感配置。
type RefreshSummary struct {
	LastRefresh    time.Time `json:"last_refresh,omitempty"`
	NextRefresh    time.Time `json:"next_refresh,omitempty"`
	NodeCount      int       `json:"node_count"`
	LastError      string    `json:"last_error,omitempty"`
	RefreshCount   int       `json:"refresh_count"`
	IsRefreshing   bool      `json:"is_refreshing"`
	TriggerReason  string    `json:"trigger_reason,omitempty"`
	BackoffAttempt int       `json:"backoff_attempt"`
	NextRetry      time.Time `json:"next_retry,omitempty"`
	Phase          string    `json:"phase,omitempty"`
	ConfigRevision uint64    `json:"config_revision"`
	SharedWaiters  int       `json:"shared_waiters"`
}

// Snapshot 描述当前最小 Lease Runtime 状态，供管理 API 和 WebUI 安全展示。
type Snapshot struct {
	Live                 bool `json:"live"`
	Enabled              bool `json:"enabled"`
	Ready                bool `json:"ready"`
	Degraded             bool `json:"degraded"`
	ReadyNodeCount       int  `json:"ready_node_count"`
	DefaultIdleNodeCount int  `json:"default_idle_node_count"`
	// IdleNodeCount 是 DefaultIdleNodeCount 的兼容别名；空闲节点必须相对于冲突域理解。
	IdleNodeCount        int                 `json:"idle_node_count"`
	WaiterCount          int                 `json:"waiter_count"`
	AcquireFailures      uint64              `json:"acquire_failures"`
	ActiveLeases         []LeaseSummary      `json:"active_leases"`
	RecentLeases         []LeaseSummary      `json:"recent_leases"`
	Validation           ValidationSnapshot  `json:"validation"`
	Generations          []GenerationSummary `json:"generations"`
	RecentGenerations    []GenerationSummary `json:"recent_generations"`
	BlockedNodeKeys      []string            `json:"blocked_node_keys"`
	LeaseNextCursor      string              `json:"lease_next_cursor,omitempty"`
	NodeNextCursor       string              `json:"node_next_cursor,omitempty"`
	GenerationNextCursor string              `json:"generation_next_cursor,omitempty"`
	EventOldestSequence  uint64              `json:"event_oldest_sequence"`
	EventLatestSequence  uint64              `json:"event_latest_sequence"`
	AllocationPaused     bool                `json:"allocation_paused"`
	PendingRefresh       bool                `json:"pending_refresh"`
	Nodes                []NodeSummary       `json:"lease_nodes"`
	InvariantAlerts      []string            `json:"invariant_alerts"`
	GatewayMetrics       GatewayMetrics      `json:"gateway_metrics"`
	Refresh              RefreshSummary      `json:"refresh"`
}

// Controller 是管理 HTTP 层依赖的租约控制边界。
// 管理层只使用该接口，不接触 Token 哈希表或节点拨号器等运行时内部状态。
type Controller interface {
	AuthenticateAPIToken(token string) bool
	AcquireLease(ctx context.Context, request AcquireRequest) (Grant, error)
	Release(ctx context.Context, token string) error
	BlockNode(ctx context.Context, nodeKey, reason string) error
	UnblockNode(ctx context.Context, nodeKey, reason string) error
	PauseAllocation(ctx context.Context, reason string) error
	ResumeAllocation(ctx context.Context, reason string) error
	RevokeLease(ctx context.Context, fingerprint, reason string) error
	AbortCandidate(ctx context.Context, reason string) error
	ForceCloseGeneration(ctx context.Context, generationID, reason string) error
	Snapshot() Snapshot
	SubscribeEvents(ctx context.Context, after uint64) <-chan RuntimeEvent
	EventSnapshot(after uint64, eventType string) []RuntimeEvent
	EventBounds() (oldest uint64, latest uint64)
	UpdateOperationalState(live bool, refresh RefreshSummary)
	RecordAuditEvent(event RuntimeEvent)
}

type leaseRecord struct {
	node              Node
	label             string
	hash              [sha256.Size]byte
	expiresAt         time.Time
	generationID      string
	expiryTimer       Timer
	drainTimer        Timer
	state             LeaseState
	activeConnections int
	connections       map[uint64]func()
	conflictDomain    conflictDomainKey
}

type acquireResult struct {
	grant Grant
	err   error
}

type leaseWaiter struct {
	label          string
	conflictDomain conflictDomainKey
	result         chan acquireResult
}

type conflictDomainKey struct {
	defaultDomain bool
	hash          [sha256.Size]byte
}

type conflictDomainState struct {
	generationID    string
	initialLimit    int
	initialCursor   int
	initialExcluded map[string]struct{}
	releasedQueue   []string
	queued          map[string]struct{}
	occupied        map[string]struct{}
	waiterCount     int
}

type connectionUse struct {
	runtime *Runtime
	hash    [sha256.Size]byte
	id      uint64
	dialer  NodeDialer
	once    sync.Once
}

func (u *connectionUse) RegisterCloser(closer func()) bool {
	return u.runtime.registerConnectionCloser(u.hash, u.id, closer)
}

func (u *connectionUse) Release() {
	u.once.Do(func() { u.runtime.releaseConnection(u.hash, u.id) })
}

func (u *connectionUse) ReportFailure() {
	u.runtime.reportNodeFailure(u.hash)
}

type generationState struct {
	id                 string
	role               GenerationRole
	nodeCount          int
	close              func() error
	validation         *ValidationRun
	nodeKeys           map[string]struct{}
	readyNodeKeys      map[string]struct{}
	addedNodeCount     int
	unchangedNodeCount int
	retiredNodeCount   int
	retiredNodeKeys    map[string]struct{}
	drainDeadline      time.Time
	drainTimer         Timer
}

// Runtime 管理进程内租约、冲突域内 Node Key 独占关系和 Gateway 路由。
// 租约不持久化，进程关闭后所有 Lease Token 都会失效。
type Runtime struct {
	mu                     sync.RWMutex
	apiToken               string
	proxyURL               *url.URL
	tokenBytes             int
	ttl                    time.Duration
	acquireWaitTimeout     time.Duration
	drainTimeout           time.Duration
	generationDrainTimeout time.Duration
	now                    func() time.Time
	afterFunc              func(time.Duration, func()) Timer
	nodes                  []Node
	conflictDomains        map[conflictDomainKey]*conflictDomainState
	nodeLeaseCounts        map[string]int
	leases                 map[[sha256.Size]byte]*leaseRecord
	recentLeases           []LeaseSummary
	waiters                []*leaseWaiter
	acquireFailures        uint64
	nextConnectionID       uint64
	nodeRecheck            NodeRecheckFunc
	rechecking             map[string]struct{}
	unavailable            map[string]struct{}
	blocked                map[string]struct{}
	minReadyCapacity       int
	degradedTrigger        func()
	degradedNotified       bool
	allocationPaused       bool
	pendingRefresh         bool
	events                 []RuntimeEvent
	nextEventSequence      uint64
	subscribers            map[uint64]chan RuntimeEvent
	nextSubscriberID       uint64
	gatewayMetrics         GatewayMetrics
	processLive            bool
	refresh                RefreshSummary
	validation             *ValidationRun
	activeGeneration       string
	generations            map[string]*generationState
	recentGenerations      []GenerationSummary
	closed                 bool
}

// AddReadyNode 将后台完成验证的节点追加到当前可分配列表尾部。
// 相同 Node Key 只保留一个条目，避免重复验证结果破坏独占语义。
func (r *Runtime) AddReadyNode(generationID string, node Node) {
	if node.Key == "" || node.Dialer == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return
	}
	if generationID != r.activeGeneration {
		return
	}
	for _, existing := range r.nodes {
		if existing.Key == node.Key {
			return
		}
	}
	r.nodes = append(r.nodes, node)
	for _, domain := range r.conflictDomains {
		r.enqueueDomainNodeLocked(domain, node.Key)
	}
	if generation := r.generations[generationID]; generation != nil {
		generation.nodeCount = len(r.nodes)
		if generation.readyNodeKeys == nil {
			generation.readyNodeKeys = make(map[string]struct{})
		}
		generation.readyNodeKeys[node.Key] = struct{}{}
	}
	r.recordEventLocked(RuntimeEvent{Type: "NODE_VALIDATED_READY", GenerationID: generationID, NodeKey: node.Key})
	r.dispatchWaitersLocked()
}

// BeginCandidate 注册正在验证的候选代，供管理 API 展示探测进度。
// 候选节点不会写入 Active 分配队列；已有 Draining 时拒绝第三代并存。
func (r *Runtime) BeginCandidate(generationID string, nodeCount int, validation *ValidationRun) error {
	if strings.TrimSpace(generationID) == "" || nodeCount <= 0 || validation == nil {
		return errors.New("candidate generation requires ID, nodes and validation")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return errors.New("lease runtime is closed")
	}
	// 新一轮刷新前先回收已到期租约，避免已经没有有效租约的 Draining 阻塞后续 Candidate。
	r.removeExpiredLocked(r.now())
	for _, generation := range r.generations {
		if generation.role == GenerationRoleDraining || generation.role == GenerationRoleCandidate {
			return ErrGenerationLimit
		}
	}
	if _, exists := r.generations[generationID]; exists {
		return fmt.Errorf("generation %q already exists", generationID)
	}
	r.generations[generationID] = &generationState{
		id: generationID, role: GenerationRoleCandidate, nodeCount: nodeCount, validation: validation,
	}
	r.validation = validation
	r.recordEventLocked(RuntimeEvent{Type: "CANDIDATE_STARTED", GenerationID: generationID})
	return nil
}

// CanBeginCandidate 返回当前是否允许构建下一候选代；用于刷新协调器落实两代上限。
func (r *Runtime) CanBeginCandidate() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.closed {
		return false
	}
	for _, generation := range r.generations {
		if generation.role == GenerationRoleCandidate || generation.role == GenerationRoleDraining {
			return false
		}
	}
	return true
}

// SetNodeRecheck 更新当前运行时的严格节点复检边界。
func (r *Runtime) SetNodeRecheck(recheck NodeRecheckFunc) {
	r.mu.Lock()
	r.nodeRecheck = recheck
	r.mu.Unlock()
}

// SetDegradedTrigger 绑定 Active 容量不足后的统一恢复协调入口。
func (r *Runtime) SetDegradedTrigger(trigger func()) {
	r.mu.Lock()
	r.degradedTrigger = trigger
	r.mu.Unlock()
}

// SetPendingRefresh 标记两代上限期间是否暂存了最新订阅快照。
func (r *Runtime) SetPendingRefresh(pending bool) {
	r.mu.Lock()
	if r.pendingRefresh != pending {
		eventType := "REFRESH_SNAPSHOT_PENDING"
		if !pending {
			eventType = "REFRESH_SNAPSHOT_CONSUMED"
		}
		r.recordEventLocked(RuntimeEvent{Type: eventType})
	}
	r.pendingRefresh = pending
	r.mu.Unlock()
}

// UpdateOperationalState 把进程与刷新协调器状态纳入 Runtime 原子快照。
// 刷新阶段或配置修订变化时同时产生统一时间线事件。
func (r *Runtime) UpdateOperationalState(live bool, refresh RefreshSummary) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.refresh.Phase != refresh.Phase || r.refresh.ConfigRevision != refresh.ConfigRevision || r.refresh.LastError != refresh.LastError {
		result := "success"
		if refresh.LastError != "" {
			result = "failed"
		}
		r.recordEventLocked(RuntimeEvent{
			Type: "REFRESH_STATE_CHANGED", Reason: refresh.TriggerReason, Result: result,
			Target: fmt.Sprintf("revision-%d", refresh.ConfigRevision), Error: refresh.LastError,
		})
	}
	r.processLive = live
	r.refresh = refresh
}

// RecordAuditEvent 接收管理 HTTP 边界产生的拒绝或失败事件。
// 调用方只能传入脱敏目标，序号和时间由 Runtime 统一生成。
func (r *Runtime) RecordAuditEvent(event RuntimeEvent) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if event.Operator == "" {
		event.Operator = "webui"
	}
	r.recordEventLocked(event)
}

// RecoverDegradedCapacity 复检当前 Active 中已确认不可用的节点。
// 只恢复同一个 Node Key；被人工阻断、已离开 Active 或复检失败的节点不会入队。
func (r *Runtime) RecoverDegradedCapacity(ctx context.Context) (bool, error) {
	r.mu.RLock()
	recheck := r.nodeRecheck
	activeGeneration := r.activeGeneration
	nodes := make([]Node, 0, len(r.unavailable))
	if recheck != nil {
		for _, node := range r.nodes {
			if _, unavailable := r.unavailable[node.Key]; !unavailable {
				continue
			}
			if _, blocked := r.blocked[node.Key]; blocked {
				continue
			}
			nodes = append(nodes, node)
		}
	}
	r.mu.RUnlock()

	for _, node := range nodes {
		if err := ctx.Err(); err != nil {
			return false, err
		}
		available := recheck(ctx, node)
		r.mu.Lock()
		r.gatewayMetrics.NodeRechecks++
		if available && r.activeGeneration == activeGeneration {
			if _, blocked := r.blocked[node.Key]; !blocked {
				delete(r.unavailable, node.Key)
				r.enqueueNodeForAllDomainsLocked(node.Key)
				r.recordEventLocked(RuntimeEvent{Type: "NODE_RECHECK_RECOVERED", GenerationID: activeGeneration, NodeKey: node.Key})
			}
		} else {
			r.recordEventLocked(RuntimeEvent{Type: "NODE_RECHECK_FAILED", GenerationID: activeGeneration, NodeKey: node.Key})
		}
		r.mu.Unlock()
	}

	r.mu.Lock()
	recovered := r.readyCapacityLocked() >= r.minReadyCapacity
	if recovered {
		r.degradedNotified = false
		r.dispatchWaitersLocked()
	}
	r.mu.Unlock()
	return recovered, nil
}

// SubscribeEvents 从指定序号之后开始重放并订阅实时事件。
func (r *Runtime) SubscribeEvents(ctx context.Context, after uint64) <-chan RuntimeEvent {
	r.mu.Lock()
	backlog := make([]RuntimeEvent, 0)
	for _, event := range r.events {
		if event.Sequence > after {
			backlog = append(backlog, event)
		}
	}
	channel := make(chan RuntimeEvent, len(backlog)+128)
	for _, event := range backlog {
		channel <- event
	}
	r.nextSubscriberID++
	subscriberID := r.nextSubscriberID
	r.subscribers[subscriberID] = channel
	r.mu.Unlock()
	go func() {
		<-ctx.Done()
		r.mu.Lock()
		if current, exists := r.subscribers[subscriberID]; exists {
			delete(r.subscribers, subscriberID)
			close(current)
		}
		r.mu.Unlock()
	}()
	return channel
}

// EventSnapshot 返回指定游标后的稳定事件副本，可用于审计筛选和 SSE 缺口恢复。
func (r *Runtime) EventSnapshot(after uint64, eventType string) []RuntimeEvent {
	r.mu.RLock()
	defer r.mu.RUnlock()
	result := make([]RuntimeEvent, 0)
	for _, event := range r.events {
		if event.Sequence <= after || (eventType != "" && event.Type != eventType) {
			continue
		}
		result = append(result, event)
	}
	return result
}

// EventBounds 返回当前内存事件窗口的首尾序号。
// 客户端游标早于该窗口时必须先读取快照，不能静默跳过已经淘汰的事件。
func (r *Runtime) EventBounds() (uint64, uint64) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.events) == 0 {
		return 0, r.nextEventSequence
	}
	return r.events[0].Sequence, r.nextEventSequence
}

func (r *Runtime) recordEventLocked(event RuntimeEvent) {
	r.nextEventSequence++
	event.Sequence = r.nextEventSequence
	event.Time = r.now()
	r.events = append(r.events, event)
	if len(r.events) > 1000 {
		r.events = append([]RuntimeEvent(nil), r.events[len(r.events)-1000:]...)
	}
	for _, subscriber := range r.subscribers {
		select {
		case subscriber <- event:
		default:
		}
	}
}

// DiscardCandidate 移除验证失败或被取消的候选代，不改变当前 Active。
func (r *Runtime) DiscardCandidate(generationID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if generation := r.generations[generationID]; generation != nil && generation.role == GenerationRoleCandidate {
		r.rememberGenerationLocked(generation, "FAILED", 0, 0)
		delete(r.generations, generationID)
		r.recordEventLocked(RuntimeEvent{Type: "CANDIDATE_DISCARDED", GenerationID: generationID, Result: "failed"})
	}
}

// AttachCandidateCloser 把 Candidate 的取消和 sing-box 关闭函数交给运行时人工处置路径。
func (r *Runtime) AttachCandidateCloser(generationID string, closer func() error) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	generation := r.generations[generationID]
	if generation == nil || generation.role != GenerationRoleCandidate {
		return errors.New("candidate generation not found")
	}
	generation.close = closer
	return nil
}

// AttachCandidateNodes 补充候选代 Node Key 视图，节点仍不会进入 Active 分配队列。
func (r *Runtime) AttachCandidateNodes(generationID string, nodes []Node) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	generation := r.generations[generationID]
	if generation == nil || generation.role != GenerationRoleCandidate {
		return errors.New("candidate generation not found")
	}
	generation.nodeKeys = nodeKeySet(nodes)
	if active := r.generations[r.activeGeneration]; active != nil {
		for nodeKey := range generation.nodeKeys {
			if _, unchanged := active.nodeKeys[nodeKey]; unchanged {
				generation.unchangedNodeCount++
			} else {
				generation.addedNodeCount++
			}
		}
	} else {
		generation.addedNodeCount = len(generation.nodeKeys)
	}
	return nil
}

// AbortCandidate 幂等中止当前候选代，不修改 Active。
func (r *Runtime) AbortCandidate(_ context.Context, reason string) error {
	r.mu.Lock()
	var candidateID string
	var closeCandidate func() error
	for generationID, generation := range r.generations {
		if generation.role != GenerationRoleCandidate {
			continue
		}
		candidateID = generationID
		closeCandidate = generation.close
		r.rememberGenerationLocked(generation, "CLOSED", 0, 0)
		delete(r.generations, generationID)
		break
	}
	if candidateID == "" {
		r.mu.Unlock()
		return nil
	}
	if active := r.generations[r.activeGeneration]; active != nil {
		r.validation = active.validation
	}
	r.recordEventLocked(RuntimeEvent{Type: "ADMIN_CANDIDATE_ABORTED", GenerationID: candidateID, Target: candidateID, Reason: reason, Operator: "webui", Result: "success", BeforeState: "CANDIDATE"})
	r.mu.Unlock()
	if closeCandidate != nil {
		return closeCandidate()
	}
	return nil
}

// ForceCloseGeneration 强制关闭指定 Draining 代际及其残留租约和连接。
func (r *Runtime) ForceCloseGeneration(_ context.Context, generationID, reason string) error {
	r.mu.Lock()
	generation := r.generations[generationID]
	if generation == nil || generation.role != GenerationRoleDraining {
		r.mu.Unlock()
		return errors.New("draining generation not found")
	}
	affectedLeases := 0
	affectedConnections := 0
	for _, record := range r.leases {
		if record.generationID == generationID {
			affectedLeases++
			affectedConnections += record.activeConnections
		}
	}
	r.recordEventLocked(RuntimeEvent{Type: "ADMIN_DRAINING_FORCE_CLOSED", GenerationID: generationID, Target: generationID, Reason: reason, Operator: "webui", Result: "success", BeforeState: "DRAINING", AffectedLeases: affectedLeases, AffectedConnections: affectedConnections})
	r.mu.Unlock()
	r.forceCloseGenerationWithEvent(generationID, "")
	return nil
}

// Promote 原子提升已经达到准入门槛的 Candidate。
// 旧 leaseRecord 继续持有原 NodeDialer，新租约只从新的 Active Generation 分配。
func (r *Runtime) Promote(candidate Candidate, readyNodes []Node, validation *ValidationRun) error {
	if strings.TrimSpace(candidate.ID) == "" || len(readyNodes) == 0 {
		return errors.New("candidate generation requires ID and ready nodes")
	}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return errors.New("lease runtime is closed")
	}
	for _, generation := range r.generations {
		if generation.role == GenerationRoleDraining {
			r.mu.Unlock()
			return ErrGenerationLimit
		}
	}
	candidateGeneration := r.generations[candidate.ID]
	if candidateGeneration == nil || candidateGeneration.role != GenerationRoleCandidate {
		r.mu.Unlock()
		return fmt.Errorf("generation %q is not a candidate", candidate.ID)
	}
	oldID := r.activeGeneration
	oldGeneration := r.generations[oldID]
	if oldGeneration != nil {
		oldGeneration.role = GenerationRoleDraining
		oldGeneration.drainDeadline = r.now().Add(r.generationDrainTimeout)
		oldGeneration.retiredNodeCount = countRetiredNodes(oldGeneration.nodeKeys, nodeKeySet(candidate.Nodes))
		oldGeneration.retiredNodeKeys = retiredNodeKeys(oldGeneration.nodeKeys, nodeKeySet(candidate.Nodes))
		oldGeneration.drainTimer = r.afterFunc(r.generationDrainTimeout, func() {
			r.forceCloseGeneration(oldID)
		})
	}
	candidateGeneration.role = GenerationRoleActive
	candidateGeneration.nodeCount = len(readyNodes)
	candidateGeneration.close = candidate.Close
	candidateGeneration.validation = validation
	candidateGeneration.nodeKeys = nodeKeySet(candidate.Nodes)
	candidateGeneration.readyNodeKeys = nodeKeySet(readyNodes)
	r.activeGeneration = candidate.ID
	r.nodes = append([]Node(nil), readyNodes...)
	for _, domain := range r.conflictDomains {
		r.resetConflictDomainQueueLocked(domain)
	}
	r.validation = validation
	r.degradedNotified = false
	r.recordEventLocked(RuntimeEvent{Type: "GENERATION_PROMOTED", GenerationID: candidate.ID})
	r.recordEventLocked(RuntimeEvent{Type: "VALIDATION_THRESHOLD_REACHED", GenerationID: candidate.ID, Result: "success"})
	r.dispatchWaitersLocked()
	var closeOld func() error
	if oldID != "" {
		closeOld = r.retireDrainedGenerationLocked(oldID)
	}
	r.mu.Unlock()
	if closeOld != nil {
		return closeOld()
	}
	return nil
}

// NewPendingRuntime 创建尚未达到首次准入门槛的运行时。
// 稳定 Gateway 可以先启动并返回 503；Candidate 达标前不存在可分配的 Active 节点。
func NewPendingRuntime(opts Options, candidate Candidate, validation *ValidationRun) (*Runtime, error) {
	if strings.TrimSpace(candidate.ID) == "" || len(candidate.Nodes) == 0 || validation == nil {
		return nil, errors.New("initial candidate requires ID, nodes and validation")
	}
	opts.Nodes = candidate.Nodes
	opts.GenerationID = candidate.ID
	opts.GenerationClose = candidate.Close
	runtime, err := NewRuntime(opts)
	if err != nil {
		return nil, err
	}
	runtime.nodes = nil
	runtime.activeGeneration = ""
	runtime.generations[candidate.ID].role = GenerationRoleCandidate
	runtime.generations[candidate.ID].validation = validation
	runtime.validation = validation
	return runtime, nil
}

// NewRuntime 创建 Lease Runtime，并在监听服务启动前校验机器凭据、Gateway URL 和节点拨号边界。
func NewRuntime(opts Options) (*Runtime, error) {
	if strings.TrimSpace(opts.APIToken) == "" {
		return nil, errors.New("lease API token is required")
	}
	proxyURL, err := url.Parse(opts.ProxyURL)
	if err != nil || proxyURL.Scheme != "http" || proxyURL.Host == "" {
		return nil, fmt.Errorf("invalid lease proxy URL %q", opts.ProxyURL)
	}
	if opts.TokenBytes == 0 {
		opts.TokenBytes = 32
	}
	if opts.TokenBytes < 32 {
		return nil, errors.New("lease token must contain at least 32 random bytes")
	}
	if opts.TTL <= 0 {
		opts.TTL = 2 * time.Minute
	}
	if opts.AcquireWaitTimeout <= 0 {
		opts.AcquireWaitTimeout = 10 * time.Second
	}
	if opts.DrainTimeout <= 0 {
		opts.DrainTimeout = 30 * time.Second
	}
	if opts.GenerationDrainTimeout <= 0 {
		opts.GenerationDrainTimeout = 2*time.Minute + 30*time.Second
	}
	if opts.MinReadyCapacity <= 0 {
		opts.MinReadyCapacity = 1
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if opts.AfterFunc == nil {
		opts.AfterFunc = func(delay time.Duration, callback func()) Timer {
			return time.AfterFunc(delay, callback)
		}
	}
	if strings.TrimSpace(opts.GenerationID) == "" {
		opts.GenerationID = "generation-1"
	}
	if len(opts.Nodes) == 0 {
		return nil, ErrNoNodeAvailable
	}
	seen := make(map[string]struct{}, len(opts.Nodes))
	for _, node := range opts.Nodes {
		if strings.TrimSpace(node.Key) == "" || node.Dialer == nil {
			return nil, errors.New("lease node requires key and dialer")
		}
		if _, exists := seen[node.Key]; exists {
			return nil, fmt.Errorf("duplicate lease node key %q", node.Key)
		}
		seen[node.Key] = struct{}{}
	}

	return &Runtime{
		apiToken:               opts.APIToken,
		proxyURL:               proxyURL,
		tokenBytes:             opts.TokenBytes,
		ttl:                    opts.TTL,
		acquireWaitTimeout:     opts.AcquireWaitTimeout,
		drainTimeout:           opts.DrainTimeout,
		generationDrainTimeout: opts.GenerationDrainTimeout,
		nodeRecheck:            opts.NodeRecheck,
		minReadyCapacity:       opts.MinReadyCapacity,
		degradedTrigger:        opts.DegradedTrigger,
		processLive:            true,
		now:                    opts.Now,
		afterFunc:              opts.AfterFunc,
		nodes:                  append([]Node(nil), opts.Nodes...),
		conflictDomains:        make(map[conflictDomainKey]*conflictDomainState),
		nodeLeaseCounts:        make(map[string]int, len(opts.Nodes)),
		leases:                 make(map[[sha256.Size]byte]*leaseRecord),
		rechecking:             make(map[string]struct{}),
		unavailable:            make(map[string]struct{}),
		blocked:                make(map[string]struct{}),
		subscribers:            make(map[uint64]chan RuntimeEvent),
		activeGeneration:       opts.GenerationID,
		generations: map[string]*generationState{
			opts.GenerationID: {
				id: opts.GenerationID, role: GenerationRoleActive, nodeCount: len(opts.Nodes),
				close: opts.GenerationClose, nodeKeys: nodeKeySet(opts.Nodes),
				readyNodeKeys: nodeKeySet(opts.Nodes),
			},
		},
	}, nil
}

// AuthenticateAPIToken 使用常量时间比较验证长期机器令牌，避免与 WebUI Session 混用。
func (r *Runtime) AuthenticateAPIToken(token string) bool {
	if r == nil || len(token) != len(r.apiToken) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(token), []byte(r.apiToken)) == 1
}

// Acquire 为旧的进程内调用点保留默认冲突域入口。
func (r *Runtime) Acquire(ctx context.Context, label string) (Grant, error) {
	return r.AcquireLease(ctx, AcquireRequest{Label: label})
}

// AcquireLease 为调用方在指定租约冲突域中分配一个固定 Node Key。
func (r *Runtime) AcquireLease(ctx context.Context, request AcquireRequest) (Grant, error) {
	label := strings.TrimSpace(request.Label)
	if utf8.RuneCountInString(label) > 64 {
		return Grant{}, ErrInvalidLabel
	}
	conflictDomain, err := normalizeConflictKey(request.ConflictKey)
	if err != nil {
		return Grant{}, err
	}

	if ctx == nil {
		ctx = context.Background()
	}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return Grant{}, errors.New("lease runtime is closed")
	}
	if r.allocationPaused {
		r.acquireFailures++
		r.mu.Unlock()
		return Grant{}, ErrAllocationPaused
	}
	now := r.now()
	r.removeExpiredLocked(now)
	domain := r.conflictDomainLocked(conflictDomain)
	if domain.waiterCount == 0 {
		grant, err := r.tryAcquireLocked(label, conflictDomain, now)
		if !errors.Is(err, ErrNoNodeAvailable) {
			r.mu.Unlock()
			return grant, err
		}
	}
	waiter := &leaseWaiter{label: label, conflictDomain: conflictDomain, result: make(chan acquireResult, 1)}
	r.waiters = append(r.waiters, waiter)
	domain.waiterCount++
	r.dispatchWaitersLocked()
	r.mu.Unlock()

	timer := time.NewTimer(r.acquireWaitTimeout)
	defer timer.Stop()
	select {
	case result := <-waiter.result:
		return result.grant, result.err
	case <-ctx.Done():
		if r.removeWaiter(waiter) {
			return Grant{}, ctx.Err()
		}
		result := <-waiter.result
		return result.grant, result.err
	case <-timer.C:
		if r.removeWaiter(waiter) {
			return Grant{}, ErrAcquireTimeout
		}
		result := <-waiter.result
		return result.grant, result.err
	}
}

func (r *Runtime) tryAcquireLocked(label string, conflictDomain conflictDomainKey, now time.Time) (Grant, error) {
	domain := r.conflictDomainLocked(conflictDomain)
	selected := r.nextNodeForDomainLocked(domain)
	if selected.Dialer == nil {
		return Grant{}, ErrNoNodeAvailable
	}

	token, hash, err := newToken(r.tokenBytes)
	if err != nil {
		return Grant{}, fmt.Errorf("generate lease token: %w", err)
	}
	expiresAt := now.Add(r.ttl)
	record := &leaseRecord{
		node: selected, label: label, hash: hash, expiresAt: expiresAt,
		generationID: r.activeGeneration, state: LeaseStateActive, connections: make(map[uint64]func()),
		conflictDomain: conflictDomain,
	}
	r.leases[hash] = record
	domain.occupied[selected.Key] = struct{}{}
	r.nodeLeaseCounts[selected.Key]++
	r.recordEventLocked(RuntimeEvent{
		Type: "LEASE_ACQUIRED", GenerationID: r.activeGeneration, NodeKey: selected.Key,
		Fingerprint: base64.RawURLEncoding.EncodeToString(hash[:6]),
	})
	// TTL 是租约资源回收的硬边界，不能依赖调用方后续再次访问或主动释放来触发清理。
	record.expiryTimer = r.afterFunc(r.ttl, func() { r.expireLease(hash) })

	proxyURL := *r.proxyURL
	proxyURL.User = url.UserPassword(gatewayUsername, token)
	return Grant{
		NodeKey:      selected.Key,
		LeaseToken:   token,
		ProxyURL:     proxyURL.String(),
		Label:        label,
		ExpiresAt:    expiresAt,
		GenerationID: r.activeGeneration,
	}, nil
}

func (r *Runtime) removeWaiter(target *leaseWaiter) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for index, waiter := range r.waiters {
		if waiter != target {
			continue
		}
		r.waiters = append(r.waiters[:index], r.waiters[index+1:]...)
		r.waiterRemovedLocked(target)
		r.acquireFailures++
		return true
	}
	return false
}

// dispatchWaitersLocked 按冲突域内 FIFO 顺序分配等待者；一个耗尽的冲突域不阻塞其他域。
func (r *Runtime) dispatchWaitersLocked() {
	for index := 0; index < len(r.waiters); {
		waiter := r.waiters[index]
		grant, err := r.tryAcquireLocked(waiter.label, waiter.conflictDomain, r.now())
		if errors.Is(err, ErrNoNodeAvailable) {
			index++
			continue
		}
		r.waiters = append(r.waiters[:index], r.waiters[index+1:]...)
		r.waiterRemovedLocked(waiter)
		waiter.result <- acquireResult{grant: grant, err: err}
	}
}

func (r *Runtime) conflictDomainLocked(key conflictDomainKey) *conflictDomainState {
	domain := r.conflictDomains[key]
	if domain == nil {
		domain = &conflictDomainState{occupied: make(map[string]struct{})}
		r.resetConflictDomainQueueLocked(domain)
		r.conflictDomains[key] = domain
	}
	return domain
}

// resetConflictDomainQueueLocked 以当前 Active 的节点顺序创建共享初始队列视图。
// 已占用或全局不可用的节点不会留在初始队列，后续恢复或排空完成时再进入队尾。
func (r *Runtime) resetConflictDomainQueueLocked(domain *conflictDomainState) {
	domain.generationID = r.activeGeneration
	domain.initialLimit = len(r.nodes)
	domain.initialCursor = 0
	domain.initialExcluded = make(map[string]struct{}, len(domain.occupied)+len(r.unavailable)+len(r.blocked))
	domain.releasedQueue = nil
	domain.queued = make(map[string]struct{})
	for nodeKey := range domain.occupied {
		domain.initialExcluded[nodeKey] = struct{}{}
	}
	for nodeKey := range r.unavailable {
		domain.initialExcluded[nodeKey] = struct{}{}
	}
	for nodeKey := range r.blocked {
		domain.initialExcluded[nodeKey] = struct{}{}
	}
}

func (r *Runtime) nextNodeForDomainLocked(domain *conflictDomainState) Node {
	if domain.generationID != r.activeGeneration {
		r.resetConflictDomainQueueLocked(domain)
	}
	for domain.initialCursor < domain.initialLimit {
		node := r.nodes[domain.initialCursor]
		domain.initialCursor++
		if _, excluded := domain.initialExcluded[node.Key]; excluded {
			continue
		}
		if _, unavailable := r.unavailable[node.Key]; unavailable {
			domain.initialExcluded[node.Key] = struct{}{}
			continue
		}
		if _, blocked := r.blocked[node.Key]; blocked {
			domain.initialExcluded[node.Key] = struct{}{}
			continue
		}
		if _, occupied := domain.occupied[node.Key]; occupied {
			domain.initialExcluded[node.Key] = struct{}{}
			continue
		}
		return node
	}
	for len(domain.releasedQueue) > 0 {
		nodeKey := domain.releasedQueue[0]
		domain.releasedQueue = domain.releasedQueue[1:]
		delete(domain.queued, nodeKey)
		node, exists := r.activeNodeLocked(nodeKey)
		if !exists || r.nodeUnavailableLocked(nodeKey) {
			continue
		}
		if _, occupied := domain.occupied[nodeKey]; occupied {
			continue
		}
		return node
	}
	return Node{}
}

func (r *Runtime) waiterRemovedLocked(waiter *leaseWaiter) {
	domain := r.conflictDomains[waiter.conflictDomain]
	if domain == nil {
		return
	}
	if domain.waiterCount > 0 {
		domain.waiterCount--
	}
	r.cleanupConflictDomainLocked(waiter.conflictDomain, domain)
}

func (r *Runtime) cleanupConflictDomainLocked(key conflictDomainKey, domain *conflictDomainState) {
	// 默认域需要延续旧调用方的跨请求 FIFO；动态域空闲后立即回收，避免冲突键无限积累。
	if !key.defaultDomain && domain != nil && domain.waiterCount == 0 && len(domain.occupied) == 0 {
		delete(r.conflictDomains, key)
	}
}

func (r *Runtime) nodeOccupiedInDomainLocked(key conflictDomainKey, nodeKey string) bool {
	domain := r.conflictDomains[key]
	if domain == nil {
		return false
	}
	_, occupied := domain.occupied[nodeKey]
	return occupied
}

func (r *Runtime) nodeLeaseCountLocked(nodeKey string) int {
	return r.nodeLeaseCounts[nodeKey]
}

func (r *Runtime) decrementNodeLeaseCountLocked(nodeKey string) {
	if r.nodeLeaseCounts[nodeKey] <= 1 {
		delete(r.nodeLeaseCounts, nodeKey)
		return
	}
	r.nodeLeaseCounts[nodeKey]--
}

func (r *Runtime) activeNodeLocked(nodeKey string) (Node, bool) {
	for _, node := range r.nodes {
		if node.Key == nodeKey {
			return node, true
		}
	}
	return Node{}, false
}

func (r *Runtime) nodeUnavailableLocked(nodeKey string) bool {
	if _, unavailable := r.unavailable[nodeKey]; unavailable {
		return true
	}
	_, blocked := r.blocked[nodeKey]
	return blocked
}

func (r *Runtime) enqueueDomainNodeLocked(domain *conflictDomainState, nodeKey string) {
	if domain == nil || domain.generationID != r.activeGeneration || r.nodeUnavailableLocked(nodeKey) {
		return
	}
	if _, exists := r.activeNodeLocked(nodeKey); !exists {
		return
	}
	if _, occupied := domain.occupied[nodeKey]; occupied {
		return
	}
	if _, queued := domain.queued[nodeKey]; queued {
		return
	}
	domain.releasedQueue = append(domain.releasedQueue, nodeKey)
	domain.queued[nodeKey] = struct{}{}
}

func (r *Runtime) enqueueNodeForAllDomainsLocked(nodeKey string) {
	for _, domain := range r.conflictDomains {
		r.enqueueDomainNodeLocked(domain, nodeKey)
	}
}

func (r *Runtime) excludeNodeFromAllDomainsLocked(nodeKey string) {
	for _, domain := range r.conflictDomains {
		domain.initialExcluded[nodeKey] = struct{}{}
		if _, queued := domain.queued[nodeKey]; !queued {
			continue
		}
		filtered := domain.releasedQueue[:0]
		for _, queuedNodeKey := range domain.releasedQueue {
			if queuedNodeKey != nodeKey {
				filtered = append(filtered, queuedNodeKey)
			}
		}
		domain.releasedQueue = filtered
		delete(domain.queued, nodeKey)
	}
}

// Release 幂等释放租约；未知 Token 不泄露租约是否曾经存在。
func (r *Runtime) Release(_ context.Context, token string) error {
	hash := sha256.Sum256([]byte(token))
	r.mu.Lock()
	closeGeneration := r.beginDrainLeaseLocked(hash, LeaseStateDraining)
	r.mu.Unlock()
	if closeGeneration != nil {
		return closeGeneration()
	}
	return nil
}

// PauseAllocation 暂停新租约和等待者分配，不影响已有租约连接。
func (r *Runtime) PauseAllocation(_ context.Context, reason string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.allocationPaused {
		return nil
	}
	r.allocationPaused = true
	for _, waiter := range r.waiters {
		waiter.result <- acquireResult{err: ErrAllocationPaused}
		r.waiterRemovedLocked(waiter)
		r.acquireFailures++
	}
	r.waiters = nil
	r.recordEventLocked(RuntimeEvent{Type: "ADMIN_ALLOCATION_PAUSED", Reason: reason, Operator: "webui", Result: "success", BeforeState: "RUNNING"})
	return nil
}

// ResumeAllocation 恢复新租约分配并按 FIFO 满足后续等待者。
func (r *Runtime) ResumeAllocation(_ context.Context, reason string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.allocationPaused {
		return nil
	}
	r.allocationPaused = false
	r.recordEventLocked(RuntimeEvent{Type: "ADMIN_ALLOCATION_RESUMED", Reason: reason, Operator: "webui", Result: "success", BeforeState: "PAUSED"})
	r.dispatchWaitersLocked()
	return nil
}

// RevokeLease 使用管理快照中的脱敏指纹撤销租约，不要求管理员接触 Lease Token。
func (r *Runtime) RevokeLease(_ context.Context, fingerprint, reason string) error {
	fingerprint = strings.TrimSpace(fingerprint)
	if fingerprint == "" {
		return errors.New("lease fingerprint is required")
	}
	r.mu.Lock()
	var targetHash [sha256.Size]byte
	found := false
	for hash := range r.leases {
		if base64.RawURLEncoding.EncodeToString(hash[:6]) != fingerprint {
			continue
		}
		if found {
			r.mu.Unlock()
			return errors.New("lease fingerprint is ambiguous")
		}
		targetHash = hash
		found = true
	}
	if !found {
		r.mu.Unlock()
		return errors.New("lease not found")
	}
	record := r.leases[targetHash]
	beforeState := record.state
	affectedConnections := record.activeConnections
	closeGeneration := r.beginDrainLeaseLocked(targetHash, LeaseStateDraining)
	r.recordEventLocked(RuntimeEvent{
		Type: "ADMIN_LEASE_REVOKED", GenerationID: record.generationID,
		NodeKey: record.node.Key, Fingerprint: fingerprint, Target: fingerprint,
		Reason: reason, Operator: "webui", Result: "success", BeforeState: string(beforeState),
		AffectedLeases: 1, AffectedConnections: affectedConnections,
	})
	r.mu.Unlock()
	if closeGeneration != nil {
		return closeGeneration()
	}
	return nil
}

// BlockNode 按稳定 Node Key 跨代际阻断节点，并使绑定租约进入 Broken 排空。
func (r *Runtime) BlockNode(_ context.Context, nodeKey, reason string) error {
	nodeKey = strings.TrimSpace(nodeKey)
	if nodeKey == "" {
		return errors.New("node key is required")
	}
	r.mu.Lock()
	r.blocked[nodeKey] = struct{}{}
	r.unavailable[nodeKey] = struct{}{}
	r.excludeNodeFromAllDomainsLocked(nodeKey)
	var generationClosers []func() error
	for hash, record := range r.leases {
		if record.node.Key != nodeKey || record.state == LeaseStateBroken {
			continue
		}
		if closeGeneration := r.beginDrainLeaseLocked(hash, LeaseStateBroken); closeGeneration != nil {
			generationClosers = append(generationClosers, closeGeneration)
		}
	}
	r.recordEventLocked(RuntimeEvent{Type: "ADMIN_NODE_BLOCKED", NodeKey: nodeKey, Target: nodeKey, Reason: reason, Operator: "webui", Result: "success", BeforeState: "AVAILABLE"})
	r.mu.Unlock()
	for _, closeGeneration := range generationClosers {
		_ = closeGeneration()
	}
	return nil
}

// UnblockNode 解除人工阻断，但只有复检成功后节点才回到当前空闲队列尾部。
func (r *Runtime) UnblockNode(ctx context.Context, nodeKey, reason string) error {
	nodeKey = strings.TrimSpace(nodeKey)
	if nodeKey == "" {
		return errors.New("node key is required")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	r.mu.Lock()
	delete(r.blocked, nodeKey)
	r.recordEventLocked(RuntimeEvent{Type: "ADMIN_NODE_UNBLOCKED", NodeKey: nodeKey, Target: nodeKey, Reason: reason, Operator: "webui", Result: "success", BeforeState: "BLOCKED"})
	var node Node
	for _, current := range r.nodes {
		if current.Key == nodeKey {
			node = current
			break
		}
	}
	if node.Dialer == nil || r.nodeRecheck == nil {
		r.mu.Unlock()
		return nil
	}
	if _, running := r.rechecking[nodeKey]; running {
		r.mu.Unlock()
		return nil
	}
	r.rechecking[nodeKey] = struct{}{}
	recheck := r.nodeRecheck
	r.mu.Unlock()
	go func() {
		available := recheck(ctx, node)
		r.mu.Lock()
		defer r.mu.Unlock()
		delete(r.rechecking, nodeKey)
		if !available {
			return
		}
		if _, blocked := r.blocked[nodeKey]; blocked {
			return
		}
		delete(r.unavailable, nodeKey)
		r.enqueueNodeForAllDomainsLocked(nodeKey)
		if r.readyCapacityLocked() >= r.minReadyCapacity {
			r.degradedNotified = false
		}
		r.dispatchWaitersLocked()
	}()
	return nil
}

// Snapshot 返回不包含 Lease Token 明文的运行时快照。
func (r *Runtime) Snapshot() Snapshot {
	r.mu.RLock()
	defer r.mu.RUnlock()
	snapshot := Snapshot{
		Live:              r.processLive,
		Enabled:           !r.closed,
		Ready:             !r.closed && r.activeGeneration != "" && len(r.nodes) > 0,
		ActiveLeases:      make([]LeaseSummary, 0, len(r.leases)),
		RecentLeases:      append([]LeaseSummary(nil), r.recentLeases...),
		WaiterCount:       len(r.waiters),
		AcquireFailures:   r.acquireFailures,
		AllocationPaused:  r.allocationPaused,
		PendingRefresh:    r.pendingRefresh,
		Refresh:           r.refresh,
		RecentGenerations: append([]GenerationSummary(nil), r.recentGenerations...),
	}
	if len(r.events) > 0 {
		snapshot.EventOldestSequence = r.events[0].Sequence
	}
	snapshot.EventLatestSequence = r.nextEventSequence
	snapshot.ReadyNodeCount = r.readyCapacityLocked()
	snapshot.Degraded = r.activeGeneration != "" && snapshot.ReadyNodeCount < r.minReadyCapacity
	for _, node := range r.nodes {
		if r.nodeOccupiedInDomainLocked(defaultConflictDomainKey(), node.Key) {
			continue
		}
		if _, unavailable := r.unavailable[node.Key]; unavailable {
			continue
		}
		if _, blocked := r.blocked[node.Key]; blocked {
			continue
		}
		snapshot.DefaultIdleNodeCount++
	}
	snapshot.IdleNodeCount = snapshot.DefaultIdleNodeCount
	snapshot.Ready = !r.closed && r.activeGeneration != "" && snapshot.ReadyNodeCount > 0
	if r.validation != nil {
		snapshot.Validation = r.validation.Snapshot()
	}
	for nodeKey := range r.blocked {
		snapshot.BlockedNodeKeys = append(snapshot.BlockedNodeKeys, nodeKey)
	}
	sort.Strings(snapshot.BlockedNodeKeys)
	for _, record := range r.leases {
		snapshot.GatewayMetrics.ActiveConnections += record.activeConnections
		snapshot.ActiveLeases = append(snapshot.ActiveLeases, LeaseSummary{
			TokenFingerprint:    base64.RawURLEncoding.EncodeToString(record.hash[:6]),
			ConflictFingerprint: conflictFingerprint(record.conflictDomain),
			NodeKey:             record.node.Key,
			Label:               record.label,
			ExpiresAt:           record.expiresAt,
			GenerationID:        record.generationID,
			State:               record.state,
			ActiveConnections:   record.activeConnections,
		})
	}
	snapshot.GatewayMetrics.AuthenticationFailures = r.gatewayMetrics.AuthenticationFailures
	snapshot.GatewayMetrics.InvalidTokens = r.gatewayMetrics.InvalidTokens
	snapshot.GatewayMetrics.ProxyFailures = r.gatewayMetrics.ProxyFailures
	snapshot.GatewayMetrics.NodeRechecks = r.gatewayMetrics.NodeRechecks
	sort.Slice(snapshot.ActiveLeases, func(i, j int) bool {
		if snapshot.ActiveLeases[i].ExpiresAt.Equal(snapshot.ActiveLeases[j].ExpiresAt) {
			return snapshot.ActiveLeases[i].TokenFingerprint < snapshot.ActiveLeases[j].TokenFingerprint
		}
		return snapshot.ActiveLeases[i].ExpiresAt.Before(snapshot.ActiveLeases[j].ExpiresAt)
	})
	generationIDs := make([]string, 0, len(r.generations))
	for generationID := range r.generations {
		generationIDs = append(generationIDs, generationID)
	}
	sort.Strings(generationIDs)
	for _, generationID := range generationIDs {
		generation := r.generations[generationID]
		buildPhase := "ACTIVE"
		if generation.role == GenerationRoleCandidate {
			buildPhase = "PREFLIGHT"
		} else if generation.role == GenerationRoleDraining {
			buildPhase = "DRAINING"
		}
		var readyCount, failedCount, pendingCount int
		var errorSummary string
		if generation.validation != nil {
			validation := generation.validation.Snapshot()
			readyCount = validation.Ready
			failedCount = validation.Failed
			pendingCount = validation.Pending + validation.Validating
			errorSummary = validation.LastError
			if generation.role == GenerationRoleCandidate && generation.validation.completed.Load() {
				if validation.Ready < validation.MinReady {
					buildPhase = "FAILED"
				}
			}
		}
		remainingLeases := 0
		remainingConnections := 0
		for _, record := range r.leases {
			if record.generationID == generationID {
				remainingLeases++
				remainingConnections += record.activeConnections
			}
		}
		snapshot.Generations = append(snapshot.Generations, GenerationSummary{
			ID: generation.id, Role: generation.role, BuildPhase: buildPhase,
			NodeCount: generation.nodeCount, ReadyCount: readyCount, FailedCount: failedCount,
			PendingCount: pendingCount, AddedNodeCount: generation.addedNodeCount, UnchangedNodeCount: generation.unchangedNodeCount,
			ErrorSummary:     errorSummary,
			RetiredNodeCount: generation.retiredNodeCount,
			RemainingLeases:  remainingLeases, RemainingConnections: remainingConnections,
			DrainDeadline: generation.drainDeadline,
		})
	}
	for _, generationID := range generationIDs {
		generation := r.generations[generationID]
		nodeKeys := make([]string, 0, len(generation.nodeKeys))
		for nodeKey := range generation.nodeKeys {
			nodeKeys = append(nodeKeys, nodeKey)
		}
		sort.Strings(nodeKeys)
		for _, nodeKey := range nodeKeys {
			_, unavailable := r.unavailable[nodeKey]
			_, blocked := r.blocked[nodeKey]
			activeLeaseCount := r.nodeLeaseCountLocked(nodeKey)
			leased := activeLeaseCount > 0
			defaultOccupied := r.nodeOccupiedInDomainLocked(defaultConflictDomainKey(), nodeKey)
			_, retired := generation.retiredNodeKeys[nodeKey]
			_, validatedReady := generation.readyNodeKeys[nodeKey]
			validationState := ""
			if generation.validation != nil {
				validationState = generation.validation.NodeState(nodeKey)
				if validationState != "" {
					validatedReady = validationState == "READY"
					unavailable = unavailable || validationState == "FAILED"
				}
			}
			snapshot.Nodes = append(snapshot.Nodes, NodeSummary{
				GenerationID: generationID, NodeKey: nodeKey, Role: generation.role,
				Ready: validatedReady && !unavailable && !blocked, Idle: generation.role == GenerationRoleActive && validatedReady && !unavailable && !blocked && !defaultOccupied,
				Leased: leased, ActiveLeaseCount: activeLeaseCount, Unavailable: unavailable, Blocked: blocked, Retired: retired, ValidationState: validationState,
			})
		}
	}
	snapshot.InvariantAlerts = r.invariantAlertsLocked()
	return snapshot
}

func (r *Runtime) invariantAlertsLocked() []string {
	alerts := make([]string, 0)
	queueNodes := make(map[string]struct{}, len(r.nodes))
	for _, node := range r.nodes {
		if _, duplicate := queueNodes[node.Key]; duplicate {
			alerts = append(alerts, "duplicate queue Node Key: "+node.Key)
		}
		queueNodes[node.Key] = struct{}{}
	}
	activeCount := 0
	for _, generation := range r.generations {
		if generation.role == GenerationRoleActive {
			activeCount++
		}
	}
	if r.activeGeneration != "" && activeCount != 1 {
		alerts = append(alerts, fmt.Sprintf("expected one Active Generation, found %d", activeCount))
	}
	if len(r.generations) > 2 {
		alerts = append(alerts, fmt.Sprintf("generation limit exceeded: %d", len(r.generations)))
	}
	leaseOwned := make(map[conflictDomainKey]map[string]struct{})
	leaseCounts := make(map[string]int)
	for _, record := range r.leases {
		if _, exists := r.generations[record.generationID]; !exists {
			alerts = append(alerts, "orphan lease: "+record.generationID)
		}
		domainOwned := leaseOwned[record.conflictDomain]
		if domainOwned == nil {
			domainOwned = make(map[string]struct{})
			leaseOwned[record.conflictDomain] = domainOwned
		}
		if _, duplicate := domainOwned[record.node.Key]; duplicate {
			alerts = append(alerts, "duplicate Node Key ownership: "+record.node.Key)
		}
		domainOwned[record.node.Key] = struct{}{}
		leaseCounts[record.node.Key]++
	}
	for nodeKey, count := range r.nodeLeaseCounts {
		if leaseCounts[nodeKey] != count {
			alerts = append(alerts, fmt.Sprintf("Node Key lease count mismatch: %s runtime=%d leases=%d", nodeKey, count, leaseCounts[nodeKey]))
		}
		delete(leaseCounts, nodeKey)
	}
	for nodeKey, count := range leaseCounts {
		alerts = append(alerts, fmt.Sprintf("missing Node Key lease count: %s leases=%d", nodeKey, count))
	}
	for domainKey, domain := range r.conflictDomains {
		for nodeKey := range domain.occupied {
			if _, exists := leaseOwned[domainKey][nodeKey]; !exists {
				alerts = append(alerts, "orphan occupied Node Key: "+nodeKey)
			}
		}
	}
	sort.Strings(alerts)
	return alerts
}

// GatewayHandler 返回稳定 HTTP 代理入口，普通 HTTP 和 CONNECT 都使用租约绑定的 NodeDialer。
func (r *Runtime) GatewayHandler() http.Handler {
	return &gatewayHandler{runtime: r}
}

// Close 使全部进程内 Token 失效，并释放 Node Key 占用记录。
func (r *Runtime) Close() error {
	r.mu.Lock()
	r.closed = true
	for subscriberID, subscriber := range r.subscribers {
		close(subscriber)
		delete(r.subscribers, subscriberID)
	}
	for _, waiter := range r.waiters {
		waiter.result <- acquireResult{err: errors.New("lease runtime is closed")}
	}
	r.waiters = nil
	for _, record := range r.leases {
		if record.expiryTimer != nil {
			record.expiryTimer.Stop()
		}
		if record.drainTimer != nil {
			record.drainTimer.Stop()
		}
	}
	clear(r.leases)
	clear(r.conflictDomains)
	clear(r.nodeLeaseCounts)
	closers := make([]func() error, 0, len(r.generations))
	for _, generation := range r.generations {
		if generation.drainTimer != nil {
			generation.drainTimer.Stop()
		}
		if generation.close != nil {
			closers = append(closers, generation.close)
		}
	}
	clear(r.generations)
	r.mu.Unlock()
	var closeErrors []error
	for _, closeGeneration := range closers {
		closeErrors = append(closeErrors, closeGeneration())
	}
	return errors.Join(closeErrors...)
}

func (r *Runtime) connectionForToken(token string) (*connectionUse, bool) {
	hash := sha256.Sum256([]byte(token))
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil, false
	}
	record, exists := r.leases[hash]
	if !exists || record.state != LeaseStateActive {
		r.mu.Unlock()
		return nil, false
	}
	if !r.now().Before(record.expiresAt) {
		closeGeneration := r.beginDrainLeaseLocked(hash, LeaseStateDraining)
		r.mu.Unlock()
		if closeGeneration != nil {
			go func() { _ = closeGeneration() }()
		}
		return nil, false
	}
	r.nextConnectionID++
	connectionID := r.nextConnectionID
	record.activeConnections++
	record.connections[connectionID] = nil
	use := &connectionUse{runtime: r, hash: hash, id: connectionID, dialer: record.node.Dialer}
	r.mu.Unlock()
	return use, true
}

func (r *Runtime) reportNodeFailure(hash [sha256.Size]byte) {
	r.mu.Lock()
	r.gatewayMetrics.ProxyFailures++
	record := r.leases[hash]
	if record == nil || record.state != LeaseStateActive {
		r.mu.Unlock()
		return
	}
	r.recordEventLocked(RuntimeEvent{Type: "PROXY_REQUEST_FAILED", GenerationID: record.generationID, NodeKey: record.node.Key, Fingerprint: base64.RawURLEncoding.EncodeToString(record.hash[:6]), Result: "failed"})
	if r.nodeRecheck == nil {
		r.mu.Unlock()
		return
	}
	if _, running := r.rechecking[record.node.Key]; running {
		r.mu.Unlock()
		return
	}
	node := record.node
	r.rechecking[node.Key] = struct{}{}
	r.gatewayMetrics.NodeRechecks++
	r.recordEventLocked(RuntimeEvent{Type: "NODE_RECHECK_STARTED", GenerationID: record.generationID, NodeKey: node.Key})
	recheck := r.nodeRecheck
	r.mu.Unlock()

	go func() {
		available := recheck(context.Background(), node)
		r.mu.Lock()
		delete(r.rechecking, node.Key)
		if available {
			r.recordEventLocked(RuntimeEvent{Type: "NODE_RECHECK_RECOVERED", NodeKey: node.Key, Result: "success"})
			r.mu.Unlock()
			return
		}
		r.recordEventLocked(RuntimeEvent{Type: "NODE_RECHECK_FAILED", NodeKey: node.Key, Result: "failed"})
		r.unavailable[node.Key] = struct{}{}
		r.excludeNodeFromAllDomainsLocked(node.Key)
		var generationClosers []func() error
		for leaseHash, currentLease := range r.leases {
			if currentLease.node.Key != node.Key || currentLease.state == LeaseStateBroken {
				continue
			}
			if closeGeneration := r.beginDrainLeaseLocked(leaseHash, LeaseStateBroken); closeGeneration != nil {
				generationClosers = append(generationClosers, closeGeneration)
			}
		}
		triggerDegraded := r.readyCapacityLocked() < r.minReadyCapacity && !r.degradedNotified
		if triggerDegraded {
			r.degradedNotified = true
		}
		degradedTrigger := r.degradedTrigger
		r.mu.Unlock()
		for _, closeGeneration := range generationClosers {
			_ = closeGeneration()
		}
		if triggerDegraded && degradedTrigger != nil {
			degradedTrigger()
		}
	}()
}

func (r *Runtime) recordAuthenticationFailure(invalidToken bool) {
	r.mu.Lock()
	r.gatewayMetrics.AuthenticationFailures++
	if invalidToken {
		r.gatewayMetrics.InvalidTokens++
	}
	r.mu.Unlock()
}

func (r *Runtime) removeExpiredLocked(now time.Time) {
	for hash, record := range r.leases {
		if now.Before(record.expiresAt) {
			continue
		}
		if closeGeneration := r.beginDrainLeaseLocked(hash, LeaseStateDraining); closeGeneration != nil {
			go func() { _ = closeGeneration() }()
		}
	}
	r.dispatchWaitersLocked()
}

func (r *Runtime) expireLease(hash [sha256.Size]byte) {
	r.mu.Lock()
	closeGeneration := r.beginDrainLeaseLocked(hash, LeaseStateDraining)
	r.mu.Unlock()
	if closeGeneration != nil {
		_ = closeGeneration()
	}
}

func (r *Runtime) registerConnectionCloser(hash [sha256.Size]byte, connectionID uint64, closer func()) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	record := r.leases[hash]
	if record == nil {
		return false
	}
	if _, exists := record.connections[connectionID]; !exists {
		return false
	}
	record.connections[connectionID] = closer
	return true
}

func (r *Runtime) releaseConnection(hash [sha256.Size]byte, connectionID uint64) {
	r.mu.Lock()
	record := r.leases[hash]
	if record == nil {
		r.mu.Unlock()
		return
	}
	delete(record.connections, connectionID)
	if record.activeConnections > 0 {
		record.activeConnections--
	}
	var closeGeneration func() error
	if record.state != LeaseStateActive && record.activeConnections == 0 {
		closeGeneration = r.finalizeLeaseLocked(hash, record)
	}
	r.mu.Unlock()
	if closeGeneration != nil {
		_ = closeGeneration()
	}
}

func (r *Runtime) beginDrainLeaseLocked(hash [sha256.Size]byte, state LeaseState) func() error {
	record := r.leases[hash]
	if record == nil {
		return nil
	}
	transitioned := false
	if record.state == LeaseStateActive {
		record.state = state
		transitioned = true
		if record.expiryTimer != nil {
			record.expiryTimer.Stop()
		}
	} else if record.state == LeaseStateDraining && state == LeaseStateBroken {
		record.state = LeaseStateBroken
		transitioned = true
	}
	if transitioned {
		eventType := "LEASE_DRAINING"
		if state == LeaseStateBroken {
			eventType = "LEASE_BROKEN"
		}
		r.recordEventLocked(RuntimeEvent{
			Type: eventType, GenerationID: record.generationID, NodeKey: record.node.Key,
			Fingerprint: base64.RawURLEncoding.EncodeToString(record.hash[:6]),
		})
	}
	if record.activeConnections == 0 {
		return r.finalizeLeaseLocked(hash, record)
	}
	if record.drainTimer == nil {
		record.drainTimer = r.afterFunc(r.drainTimeout, func() { r.forceFinalizeLease(hash) })
	}
	return nil
}

func (r *Runtime) forceFinalizeLease(hash [sha256.Size]byte) {
	r.mu.Lock()
	record := r.leases[hash]
	var closeGeneration func() error
	var connectionClosers []func()
	if record != nil && record.state != LeaseStateActive {
		connectionClosers = make([]func(), 0, len(record.connections))
		for _, closer := range record.connections {
			if closer != nil {
				connectionClosers = append(connectionClosers, closer)
			}
		}
		closeGeneration = r.finalizeLeaseLocked(hash, record)
	}
	r.mu.Unlock()
	for _, closeConnection := range connectionClosers {
		closeConnection()
	}
	if closeGeneration != nil {
		_ = closeGeneration()
	}
}

func (r *Runtime) finalizeLeaseLocked(hash [sha256.Size]byte, record *leaseRecord) func() error {
	if record.expiryTimer != nil {
		record.expiryTimer.Stop()
	}
	if record.drainTimer != nil {
		record.drainTimer.Stop()
	}
	if record.state == LeaseStateBroken {
		r.recentLeases = append(r.recentLeases, LeaseSummary{
			TokenFingerprint:    base64.RawURLEncoding.EncodeToString(record.hash[:6]),
			ConflictFingerprint: conflictFingerprint(record.conflictDomain),
			NodeKey:             record.node.Key, Label: record.label, ExpiresAt: record.expiresAt,
			GenerationID: record.generationID, State: record.state,
		})
		if len(r.recentLeases) > 100 {
			r.recentLeases = append([]LeaseSummary(nil), r.recentLeases[len(r.recentLeases)-100:]...)
		}
	}
	delete(r.leases, hash)
	r.decrementNodeLeaseCountLocked(record.node.Key)
	domain := r.conflictDomains[record.conflictDomain]
	if domain != nil {
		delete(domain.occupied, record.node.Key)
		r.enqueueDomainNodeLocked(domain, record.node.Key)
		r.cleanupConflictDomainLocked(record.conflictDomain, domain)
	}
	closeGeneration := r.retireDrainedGenerationLocked(record.generationID)
	r.dispatchWaitersLocked()
	return closeGeneration
}

func (r *Runtime) readyCapacityLocked() int {
	ready := 0
	for _, node := range r.nodes {
		if _, unavailable := r.unavailable[node.Key]; unavailable {
			continue
		}
		if _, blocked := r.blocked[node.Key]; blocked {
			continue
		}
		ready++
	}
	return ready
}

func (r *Runtime) retireDrainedGenerationLocked(generationID string) func() error {
	generation := r.generations[generationID]
	if generation == nil || generation.role != GenerationRoleDraining {
		return nil
	}
	for _, record := range r.leases {
		if record.generationID == generationID {
			return nil
		}
	}
	if generation.drainTimer != nil {
		generation.drainTimer.Stop()
	}
	r.rememberGenerationLocked(generation, "CLOSED", 0, 0)
	r.recordEventLocked(RuntimeEvent{Type: "GENERATION_DRAINED", GenerationID: generationID, Result: "success"})
	delete(r.generations, generationID)
	return generation.close
}

func (r *Runtime) forceCloseGeneration(generationID string) {
	r.forceCloseGenerationWithEvent(generationID, "GENERATION_DRAIN_TIMEOUT")
}

func (r *Runtime) forceCloseGenerationWithEvent(generationID, eventType string) {
	r.mu.Lock()
	generation := r.generations[generationID]
	if generation == nil || generation.role != GenerationRoleDraining {
		r.mu.Unlock()
		return
	}
	var connectionClosers []func()
	affectedLeases := 0
	affectedConnections := 0
	for hash, record := range r.leases {
		if record.generationID != generationID {
			continue
		}
		affectedLeases++
		affectedConnections += record.activeConnections
		if record.expiryTimer != nil {
			record.expiryTimer.Stop()
		}
		if record.drainTimer != nil {
			record.drainTimer.Stop()
		}
		for _, closer := range record.connections {
			if closer != nil {
				connectionClosers = append(connectionClosers, closer)
			}
		}
		delete(r.leases, hash)
		r.decrementNodeLeaseCountLocked(record.node.Key)
		if domain := r.conflictDomains[record.conflictDomain]; domain != nil {
			delete(domain.occupied, record.node.Key)
			r.enqueueDomainNodeLocked(domain, record.node.Key)
			r.cleanupConflictDomainLocked(record.conflictDomain, domain)
		}
	}
	if eventType != "" {
		r.recordEventLocked(RuntimeEvent{Type: eventType, GenerationID: generationID, Result: "forced", AffectedLeases: affectedLeases, AffectedConnections: affectedConnections})
	}
	r.rememberGenerationLocked(generation, "CLOSED", affectedLeases, affectedConnections)
	delete(r.generations, generationID)
	r.dispatchWaitersLocked()
	closeGeneration := generation.close
	r.mu.Unlock()
	for _, closeConnection := range connectionClosers {
		closeConnection()
	}
	if closeGeneration != nil {
		_ = closeGeneration()
	}
}

// rememberGenerationLocked 保存已经退出的代际摘要，不把历史记录计入两代存活上限。
func (r *Runtime) rememberGenerationLocked(generation *generationState, phase string, remainingLeases, remainingConnections int) {
	if generation == nil {
		return
	}
	summary := GenerationSummary{
		ID: generation.id, Role: generation.role, BuildPhase: phase, NodeCount: generation.nodeCount,
		RetiredNodeCount: generation.retiredNodeCount, RemainingLeases: remainingLeases,
		RemainingConnections: remainingConnections, AddedNodeCount: generation.addedNodeCount,
		UnchangedNodeCount: generation.unchangedNodeCount,
	}
	if generation.validation != nil {
		validation := generation.validation.Snapshot()
		summary.ReadyCount = validation.Ready
		summary.FailedCount = validation.Failed
		summary.PendingCount = validation.Pending + validation.Validating
		summary.ErrorSummary = validation.LastError
	}
	r.recentGenerations = append(r.recentGenerations, summary)
	if len(r.recentGenerations) > 20 {
		r.recentGenerations = append([]GenerationSummary(nil), r.recentGenerations[len(r.recentGenerations)-20:]...)
	}
}

func nodeKeySet(nodes []Node) map[string]struct{} {
	keys := make(map[string]struct{}, len(nodes))
	for _, node := range nodes {
		keys[node.Key] = struct{}{}
	}
	return keys
}

func countRetiredNodes(previous, next map[string]struct{}) int {
	count := 0
	for nodeKey := range previous {
		if _, exists := next[nodeKey]; !exists {
			count++
		}
	}
	return count
}

func retiredNodeKeys(previous, next map[string]struct{}) map[string]struct{} {
	retired := make(map[string]struct{})
	for nodeKey := range previous {
		if _, exists := next[nodeKey]; !exists {
			retired[nodeKey] = struct{}{}
		}
	}
	return retired
}

func newToken(size int) (string, [sha256.Size]byte, error) {
	raw := make([]byte, size)
	if _, err := rand.Read(raw); err != nil {
		return "", [sha256.Size]byte{}, err
	}
	token := base64.RawURLEncoding.EncodeToString(raw)
	return token, sha256.Sum256([]byte(token)), nil
}

func normalizeConflictKey(raw string) (conflictDomainKey, error) {
	normalized := strings.TrimSpace(raw)
	if normalized == "" {
		return defaultConflictDomainKey(), nil
	}
	if !utf8.ValidString(normalized) || len([]byte(normalized)) > 128 {
		return conflictDomainKey{}, ErrInvalidConflictKey
	}
	return conflictDomainKey{hash: sha256.Sum256([]byte(normalized))}, nil
}

func defaultConflictDomainKey() conflictDomainKey {
	return conflictDomainKey{defaultDomain: true}
}

func conflictFingerprint(key conflictDomainKey) string {
	if key.defaultDomain {
		return "default"
	}
	return base64.RawURLEncoding.EncodeToString(key.hash[:6])
}
