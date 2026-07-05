package pool

import (
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"easy_proxies/internal/monitor"
)

// sharedMemberState holds failure/blacklist state shared across all pool instances.
// This enables hybrid mode where pool and multi-port modes share the same node state.
type sharedMemberState struct {
	mu               sync.Mutex
	failures         int
	blacklisted      bool
	blacklistedUntil time.Time
	entry            atomic.Pointer[monitor.EntryHandle]
	active           atomic.Int32
}

// transientCooldown is how long a node is skipped after a transient failure
// (rate-limit / timeout / connection reset). Far shorter than the 24h blacklist
// used for permanent faults, because these errors usually clear on their own —
// e.g. a shared free node briefly rate-limited (HTTP 429) by its CDN.
const transientCooldown = 60 * time.Second

// isTransientError reports whether err looks like a temporary condition that
// should NOT count toward the permanent-blacklist threshold. Rate limiting
// (429), timeouts and connection resets fall here: the node is likely alive and
// will recover, so a full 24h ban would needlessly drain the healthy pool.
func isTransientError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	switch {
	case strings.Contains(msg, "429"),
		strings.Contains(msg, "too many requests"),
		strings.Contains(msg, "timeout"),
		strings.Contains(msg, "deadline exceeded"),
		strings.Contains(msg, "connection reset"),
		strings.Contains(msg, "reset by peer"),
		strings.Contains(msg, "temporarily"),
		strings.Contains(msg, "try again"),
		strings.Contains(msg, "service unavailable"),
		strings.Contains(msg, "503"):
		return true
	}
	return false
}

var sharedStateStore sync.Map // map[tag]*sharedMemberState

// acquireSharedState returns the shared state for a tag, creating if needed.
func acquireSharedState(tag string) *sharedMemberState {
	if v, ok := sharedStateStore.Load(tag); ok {
		return v.(*sharedMemberState)
	}
	state := &sharedMemberState{}
	actual, _ := sharedStateStore.LoadOrStore(tag, state)
	return actual.(*sharedMemberState)
}

// lookupSharedState returns the shared state if it exists.
func lookupSharedState(tag string) (*sharedMemberState, bool) {
	v, ok := sharedStateStore.Load(tag)
	if !ok {
		return nil, false
	}
	return v.(*sharedMemberState), true
}

// ResetSharedStateStore clears all shared state (used during config reload).
func ResetSharedStateStore() {
	sharedStateStore.Range(func(key, _ any) bool {
		sharedStateStore.Delete(key)
		return true
	})
	ResetDialerRegistry()
}

func (s *sharedMemberState) attachEntry(entry *monitor.EntryHandle) {
	if entry == nil {
		return
	}
	s.entry.Store(entry)
}

func (s *sharedMemberState) entryHandle() *monitor.EntryHandle {
	return s.entry.Load()
}

// recordFailure records a failure and decides whether to blacklist the node.
//
// Transient errors (rate-limit 429, timeouts, connection resets) do NOT count
// toward the permanent threshold; instead they impose a short cooldown so the
// node is briefly skipped and then retried automatically. Permanent errors
// (handshake/cert/protocol failures, 404, etc.) accumulate toward the threshold
// and trigger the full blacklist duration once it is reached.
//
// Returns: (current permanent-failure count, blacklisted, blacklist-until, transient).
func (s *sharedMemberState) recordFailure(cause error, threshold int, duration time.Duration) (int, bool, time.Time, bool) {
	transient := isTransientError(cause)

	s.mu.Lock()
	var count int
	triggered := false
	var until time.Time
	if transient {
		// Short cooldown only; do not accumulate toward the 24h blacklist.
		count = s.failures
		until = time.Now().Add(transientCooldown)
		s.blacklisted = true
		s.blacklistedUntil = until
	} else {
		s.failures++
		count = s.failures
		if s.failures >= threshold {
			triggered = true
			until = time.Now().Add(duration)
			s.failures = 0
			s.blacklisted = true
			s.blacklistedUntil = until
		}
	}
	s.mu.Unlock()

	if entry := s.entry.Load(); entry != nil {
		entry.RecordFailure(cause)
		if triggered || transient {
			entry.Blacklist(until)
		}
	}
	return count, triggered, until, transient
}

func (s *sharedMemberState) recordSuccess() {
	s.mu.Lock()
	s.failures = 0
	s.mu.Unlock()

	if entry := s.entry.Load(); entry != nil {
		entry.RecordSuccess()
	}
}

// isBlacklisted checks if the node is currently blacklisted, auto-clearing if expired.
func (s *sharedMemberState) isBlacklisted(now time.Time) bool {
	s.mu.Lock()
	expired := s.blacklisted && now.After(s.blacklistedUntil)
	if expired {
		s.blacklisted = false
		s.blacklistedUntil = time.Time{}
	}
	blacklisted := s.blacklisted
	s.mu.Unlock()

	if expired {
		if entry := s.entry.Load(); entry != nil {
			entry.ClearBlacklist()
		}
	}
	return blacklisted
}

// blacklistRemaining returns the remaining blacklist duration.
// Returns 0 if not blacklisted.
func (s *sharedMemberState) blacklistRemaining(now time.Time) time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.blacklisted {
		return 0
	}

	remaining := s.blacklistedUntil.Sub(now)
	if remaining < 0 {
		return 0
	}
	return remaining
}

func (s *sharedMemberState) forceRelease() {
	s.mu.Lock()
	s.failures = 0
	s.blacklisted = false
	s.blacklistedUntil = time.Time{}
	s.mu.Unlock()

	if entry := s.entry.Load(); entry != nil {
		entry.ClearBlacklist()
	}
}

func (s *sharedMemberState) incActive() {
	s.active.Add(1)
	if entry := s.entry.Load(); entry != nil {
		entry.IncActive()
	}
}

func (s *sharedMemberState) decActive() {
	s.active.Add(-1)
	if entry := s.entry.Load(); entry != nil {
		entry.DecActive()
	}
}

func (s *sharedMemberState) activeCount() int32 {
	return s.active.Load()
}

// releaseSharedMember clears blacklist state for a tag (called from release functions).
func releaseSharedMember(tag string) {
	if state, ok := lookupSharedState(tag); ok {
		state.forceRelease()
	}
}

// blacklistSharedMember manually blacklists a node in pool shared state.
func blacklistSharedMember(tag string, duration time.Duration) {
	if state, ok := lookupSharedState(tag); ok {
		until := time.Now().Add(duration)
		state.mu.Lock()
		state.blacklisted = true
		state.blacklistedUntil = until
		state.failures = 0
		state.mu.Unlock()
	}
}
