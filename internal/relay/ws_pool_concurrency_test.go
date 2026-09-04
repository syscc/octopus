package relay

import (
	"context"
	"errors"
	"testing"
	"time"

	dbmodel "github.com/bestruirui/octopus/internal/model"
	"github.com/bestruirui/octopus/internal/relay/stream"
	transformerModel "github.com/bestruirui/octopus/internal/transformer/model"
	"github.com/coder/websocket"
)

func newUnitWSPool() *wsPool {
	return &wsPool{
		conns:       make(map[wsPoolKey]*wsPoolEntry),
		inFlight:    make(map[wsPoolKey]int),
		unsupported: make(map[int]time.Time),
		health:      make(map[int]*wsChannelHealth),
		stopCh:      make(chan struct{}),
	}
}

func TestWSPoolRecordFailureForRequestSkipsCanceledContext(t *testing.T) {
	pool := newUnitWSPool()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	pool.recordWSFailureForRequest(ctx, 91, nil)
	pool.healthMu.RLock()
	_, recordedCanceled := pool.health[91]
	pool.healthMu.RUnlock()
	if recordedCanceled {
		t.Fatal("canceled request must not degrade WS transport health")
	}

	pool.recordWSFailureForRequest(context.Background(), 91, nil)
	pool.healthMu.RLock()
	health := pool.health[91]
	pool.healthMu.RUnlock()
	if health == nil || health.consecutiveFailures != 1 {
		t.Fatalf("active request failure must be recorded once, health=%+v", health)
	}
}

func TestWSPoolRecordFailureForRequestClassifiesCancellationCause(t *testing.T) {
	cases := []struct {
		name       string
		ctx        func() context.Context
		wantRecord bool
	}{
		{
			name: "ordinary cancellation",
			ctx: func() context.Context {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return ctx
			},
			wantRecord: false,
		},
		{
			name: "ordinary deadline",
			ctx: func() context.Context {
				ctx, cancel := context.WithTimeout(context.Background(), 0)
				cancel()
				return ctx
			},
			wantRecord: false,
		},
		{
			name: "local relay budget",
			ctx: func() context.Context {
				ctx, cancel := context.WithCancelCause(context.Background())
				cancel(errLocalRelayBudgetExceeded)
				return ctx
			},
			wantRecord: true,
		},
		{
			name: "first token timeout",
			ctx: func() context.Context {
				ctx, cancel := context.WithCancelCause(context.Background())
				cancel(errFirstTokenTimeout)
				return ctx
			},
			wantRecord: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pool := newUnitWSPool()
			pool.recordWSFailureForRequest(tc.ctx(), 92, nil)
			pool.healthMu.RLock()
			_, recorded := pool.health[92]
			pool.healthMu.RUnlock()
			if recorded != tc.wantRecord {
				t.Fatalf("recorded=%t, want %t", recorded, tc.wantRecord)
			}
		})
	}
}

func TestWSPoolRecordFailureForRequestPreservesUpstreamFailureOnCancellation(t *testing.T) {
	pool := newUnitWSPool()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	failure := errors.Join(context.Canceled, &wsUpstreamEventError{Status: 502, Type: "server_error"})

	pool.recordWSFailureForRequest(ctx, 93, failure)
	pool.healthMu.RLock()
	health := pool.health[93]
	pool.healthMu.RUnlock()
	if health == nil || health.consecutiveFailures != 1 {
		t.Fatalf("independent upstream failure must be recorded despite cancellation, health=%+v", health)
	}
}

func TestWSPoolRecordFailureForRequestPreservesJoinedTransportEvidence(t *testing.T) {
	pool := newUnitWSPool()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	transportFailure := newWSTransportError("ws read error", websocket.CloseError{Code: websocket.StatusInternalError, Reason: "provider detail"})
	failure := errors.Join(context.Canceled, stream.ErrDownstreamWriteFailed, transformerModel.ErrIncompleteUpstreamStream, transportFailure)
	stats := &wsPassthroughStats{DownstreamBroken: true, TerminalOutcome: transformerModel.PassthroughTerminalOutcomeFailed}
	if !hasIndependentWSTransportFailure(failure) || !shouldRecordWSPassthroughTransportFailure(stats, failure) {
		t.Fatal("independent WS transport evidence must survive downstream and cancellation joins")
	}
	pool.recordWSFailureForRequest(ctx, 94, failure)
	pool.healthMu.RLock()
	health := pool.health[94]
	pool.healthMu.RUnlock()
	if health == nil || health.consecutiveFailures != 1 {
		t.Fatalf("joined upstream transport failure must be recorded once despite cancellation, health=%+v", health)
	}
}

func TestWSTransportCleanCloseIsNotIndependentFailure(t *testing.T) {
	for _, status := range []websocket.StatusCode{websocket.StatusNormalClosure, websocket.StatusGoingAway} {
		err := newWSTransportError("ws read error", websocket.CloseError{Code: status})
		if hasIndependentWSTransportFailure(err) {
			t.Fatalf("clean close status %d was treated as an independent transport failure", status)
		}
		stats := &wsPassthroughStats{DownstreamBroken: true}
		if shouldRecordWSPassthroughTransportFailure(stats, err) {
			t.Fatalf("clean close status %d should remain health-neutral with downstream failure", status)
		}
	}
}

func TestWSPoolPreferredPreflightReservesConnection(t *testing.T) {
	pool := newUnitWSPool()
	key := wsPoolKey{channelID: 11, keyID: 12, headerSig: "sig"}
	pc := &pooledConn{
		id: "preferred", conn: &websocket.Conn{}, poolKey: key,
		createdAt: time.Now(), lastUsed: time.Now().Add(-wsHealthCheckIdle - time.Second),
	}
	pool.conns[key] = &wsPoolEntry{conns: []*pooledConn{pc}}

	started := make(chan struct{})
	release := make(chan struct{})
	pool.pingConn = func(context.Context, *websocket.Conn) error {
		close(started)
		<-release
		return nil
	}
	result := make(chan *pooledConn, 1)
	go func() { result <- pool.GetPreferred(key, pc.id) }()

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("preferred preflight did not start")
	}
	second := pool.Get(key)
	close(release)
	var first *pooledConn
	select {
	case first = <-result:
	case <-time.After(time.Second):
		t.Fatal("preferred preflight did not finish")
	}
	if second != nil {
		t.Fatalf("connection was acquired twice during preferred preflight: %#v", second)
	}
	if first != pc || !pc.busy || pc.queue != 1 {
		t.Fatalf("preferred reservation state is invalid: first=%p pc=%p busy=%t queue=%d", first, pc, pc.busy, pc.queue)
	}
	pool.Put(pc)
}

func TestWSPoolPruneDefersBusyExpiredConnection(t *testing.T) {
	pool := newUnitWSPool()
	key := wsPoolKey{channelID: 21, keyID: 22}
	pc := &pooledConn{
		id: "busy-prune", poolKey: key, busy: true, queue: 1,
		createdAt: time.Now().Add(-wsConnMaxAge - time.Minute), lastUsed: time.Now(),
	}
	entry := &wsPoolEntry{conns: []*pooledConn{pc}}
	pool.conns[key] = entry

	pool.mu.Lock()
	pool.pruneExpiredLocked(key, entry, time.Now())
	pool.mu.Unlock()
	if len(entry.conns) != 1 || entry.conns[0] != pc || !pc.retire || !pc.busy {
		t.Fatalf("busy expired connection was not deferred: %+v", pc)
	}
	pool.Put(pc)
	if count := pool.pooledConnCount(key); count != 0 {
		t.Fatalf("retired connection remained pooled after Put: %d", count)
	}
}

func TestWSPoolCleanupDefersBusyExpiredConnection(t *testing.T) {
	pool := newUnitWSPool()
	key := wsPoolKey{channelID: 31, keyID: 32}
	pc := &pooledConn{
		id: "busy-cleanup", poolKey: key, busy: true, queue: 1,
		createdAt: time.Now().Add(-wsConnMaxAge - time.Minute), lastUsed: time.Now(),
	}
	pool.conns[key] = &wsPoolEntry{conns: []*pooledConn{pc}}

	pool.cleanup()
	if count := pool.pooledConnCount(key); count != 1 || !pc.retire || !pc.busy {
		t.Fatalf("cleanup closed a busy expired connection: count=%d state=%+v", count, pc)
	}
	pool.Put(pc)
	if count := pool.pooledConnCount(key); count != 0 {
		t.Fatalf("cleanup-retired connection remained pooled after Put: %d", count)
	}
}

func TestWSPoolPreferredBusyIsNotBorrowed(t *testing.T) {
	pool := newUnitWSPool()
	key := wsPoolKey{channelID: 41, keyID: 42}
	preferred := &pooledConn{id: "preferred-busy", poolKey: key, busy: true, queue: 1, createdAt: time.Now(), lastUsed: time.Now()}
	other := &pooledConn{id: "other-idle", poolKey: key, createdAt: time.Now(), lastUsed: time.Now()}
	pool.conns[key] = &wsPoolEntry{conns: []*pooledConn{preferred, other}}
	if got := pool.GetPreferred(key, preferred.id); got != nil {
		t.Fatalf("busy preferred connection request borrowed another connection: %#v", got)
	}
	if !other.busy && other.queue == 0 {
		return
	}
	t.Fatalf("non-preferred connection was mutated: busy=%t queue=%d", other.busy, other.queue)
}

func TestTryUpstreamWSWaitsForBusyPreferredConnection(t *testing.T) {
	pool := newUnitWSPool()
	previousPool := wsUpstreamPool
	wsUpstreamPool = pool
	t.Cleanup(func() { wsUpstreamPool = previousPool })

	channel := &dbmodel.Channel{ID: 51, Type: 1}
	keyID := 52
	channelKey := "test-key"
	headers := buildUpstreamWSHeaders(nil, channel, channelKey)
	poolKey := newWSPoolKey(channel.ID, keyID, headers)
	preferred := &pooledConn{id: "preferred-busy", poolKey: poolKey, busy: true, queue: 1, createdAt: time.Now(), lastUsed: time.Now()}
	other := &pooledConn{id: "other-idle", poolKey: poolKey, createdAt: time.Now(), lastUsed: time.Now()}
	pool.conns[poolKey] = &wsPoolEntry{conns: []*pooledConn{preferred, other}}

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Millisecond)
	defer cancel()
	if got := TryUpstreamWSWithPreference(ctx, channel, "https://example.com/v1", channelKey, keyID, nil, preferred.id); got != nil {
		t.Fatalf("continuation borrowed or dialed another connection: %#v", got)
	}
	if other.busy || other.queue != 0 || pool.inFlight[poolKey] != 0 {
		t.Fatalf("waiting for preferred mutated another connection or dial state: other=%+v inFlight=%d", other, pool.inFlight[poolKey])
	}
}
