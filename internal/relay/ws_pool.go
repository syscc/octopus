package relay

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bestruirui/octopus/internal/helper"
	dbmodel "github.com/bestruirui/octopus/internal/model"
	"github.com/bestruirui/octopus/internal/transformer/outbound"
	"github.com/bestruirui/octopus/internal/utils/httpbody"
	"github.com/bestruirui/octopus/internal/utils/log"
	"github.com/coder/websocket"
)

const (
	wsConnMaxAge         = 55 * time.Minute // slightly less than 60-min limit
	wsConnIdleTimeout    = 5 * time.Minute
	wsPoolCleanupEvery   = 1 * time.Minute
	wsMaxConnsPerPoolKey = 8
	wsMaxIdlePerPoolKey  = 2
	wsQueueLimitPerConn  = 64
	wsAcquireTimeout     = 30 * time.Second
	wsHealthCheckIdle    = 90 * time.Second
	wsHealthCheckTimeout = 2 * time.Second

	wsHealthBackoffBase = 1 * time.Minute  // 首次失败退避
	wsHealthBackoffMax  = 5 * time.Minute  // 退避上限
	wsHealthStaleAfter  = 10 * time.Minute // 无失败多久后清理健康条目
)

// 上游 WS 单条消息的读上限与协议层 maxSSEEventSize 解耦：
// coder/websocket 的 SetReadLimit 会对上限做 n++，极端配置值（如 MaxInt）
// 会溢出成负数并被库当作“取消限制”，因此必须先钳制到独立硬上限。
const (
	wsUpstreamMaxReadLimitBytes     int64 = httpbody.MaxLLMResponseBodyBytes
	wsUpstreamDefaultReadLimitBytes int64 = 32 * 1024 * 1024
)

// upstreamWSReadLimit 把配置的事件上限钳制为安全的 socket 读上限：
// 非正值回落到安全默认，超过硬上限的值被截断，其余保持原值。
func upstreamWSReadLimit(configured int) int64 {
	if configured <= 0 {
		return wsUpstreamDefaultReadLimitBytes
	}
	if int64(configured) > wsUpstreamMaxReadLimitBytes {
		return wsUpstreamMaxReadLimitBytes
	}
	return int64(configured)
}

// wsUpstreamPool manages persistent WebSocket connections to upstream providers.
var wsUpstreamPool = newWSPool()

type wsPoolKey struct {
	channelID int
	keyID     int
	headerSig string
}

type pooledConn struct {
	id        string
	conn      *websocket.Conn
	createdAt time.Time
	lastUsed  time.Time
	busy      bool
	queue     int
	retire    bool
	poolKey   wsPoolKey
}

type wsPoolEntry struct {
	conns []*pooledConn
}

var wsConnIDCounter uint64

func nextWSConnID() string {
	id := atomic.AddUint64(&wsConnIDCounter, 1)
	return fmt.Sprintf("wsconn_%d_%d", time.Now().UnixNano(), id)
}

// wsChannelHealth tracks transient WS failures per channel for exponential backoff.
// Unlike the "unsupported" mechanism (for definitive 404/405/426/501), this handles
// unstable connections, timeouts, and other transient failures.
type wsChannelHealth struct {
	consecutiveFailures int
	lastFailure         time.Time
	skipUntil           time.Time
}

type wsPool struct {
	mu    sync.Mutex
	conns map[wsPoolKey]*wsPoolEntry
	// inFlight tracks Dial calls in progress per poolKey so concurrent cold
	// starts cannot exceed wsMaxConnsPerPoolKey.
	inFlight map[wsPoolKey]int
	pingConn func(context.Context, *websocket.Conn) error

	// Track channels that don't support WS to avoid repeated attempts
	unsupported   map[int]time.Time
	unsupportedMu sync.RWMutex

	// Track transient WS failures per channel for exponential backoff
	health   map[int]*wsChannelHealth
	healthMu sync.RWMutex

	stopCh chan struct{}
	once   sync.Once
}

func newWSPool() *wsPool {
	p := &wsPool{
		conns:    make(map[wsPoolKey]*wsPoolEntry),
		inFlight: make(map[wsPoolKey]int),
		pingConn: func(ctx context.Context, conn *websocket.Conn) error {
			return conn.Ping(ctx)
		},
		unsupported: make(map[int]time.Time),
		health:      make(map[int]*wsChannelHealth),
		stopCh:      make(chan struct{}),
	}
	go p.cleanupLoop()
	return p
}

// Get returns an existing idle connection or nil.
func (p *wsPool) Get(key wsPoolKey) *pooledConn {
	return p.GetPreferred(key, "")
}

// GetPreferred returns the preferred idle connection when available, otherwise
// the least-busy idle connection for the pool key.
func (p *wsPool) GetPreferred(key wsPoolKey, preferredConnID string) *pooledConn {
	p.mu.Lock()
	defer p.mu.Unlock()

	entry := p.conns[key]
	if entry == nil || len(entry.conns) == 0 {
		return nil
	}
	now := time.Now()
	p.pruneExpiredLocked(key, entry, now)
	if len(entry.conns) == 0 {
		delete(p.conns, key)
		return nil
	}
	if preferredConnID != "" {
		for _, pc := range entry.conns {
			if pc != nil && pc.id == preferredConnID && !pc.busy {
				// Reserve the connection before preflight releases p.mu for Ping.
				// Other acquisitions must never observe it as idle in that window.
				pc.busy = true
				pc.queue++
				if !p.preflightPreferredConnLocked(key, entry, pc, now) {
					pc.busy = false
					if pc.queue > 0 {
						pc.queue--
					}
					return nil
				}
				pc.lastUsed = now
				return pc
			}
		}
		// A continuation is connection-affine. If the preferred connection is
		// busy or absent, let the caller wait/fail instead of borrowing another
		// idle connection that does not own the previous_response_id.
		return nil
	}
	var selected *pooledConn
	for _, pc := range entry.conns {
		if pc == nil || pc.busy {
			continue
		}
		if selected == nil || pc.lastUsed.Before(selected.lastUsed) {
			selected = pc
		}
	}
	if selected == nil {
		return nil
	}
	selected.busy = true
	selected.queue++
	selected.lastUsed = now
	return selected
}

// Put returns a connection to the pool after use.
func (p *wsPool) Put(pc *pooledConn) {
	if pc == nil {
		return
	}
	p.mu.Lock()
	pc.busy = false
	if pc.queue > 0 {
		pc.queue--
	}
	pc.lastUsed = time.Now()
	entry := p.conns[pc.poolKey]
	if pc.retire || time.Since(pc.createdAt) > wsConnMaxAge {
		if entry != nil {
			for i, existing := range entry.conns {
				if existing == pc || (existing != nil && existing.id == pc.id) {
					entry.conns = append(entry.conns[:i], entry.conns[i+1:]...)
					break
				}
			}
			if len(entry.conns) == 0 {
				delete(p.conns, pc.poolKey)
			}
		}
		p.mu.Unlock()
		if pc.conn != nil {
			_ = pc.conn.Close(websocket.StatusGoingAway, "connection retired")
		}
		return
	}
	if entry == nil {
		entry = &wsPoolEntry{}
		p.conns[pc.poolKey] = entry
	}
	for _, existing := range entry.conns {
		if existing == pc || (existing != nil && existing.id == pc.id) {
			p.mu.Unlock()
			return
		}
	}
	entry.conns = append(entry.conns, pc)
	p.mu.Unlock()
}

// Remove removes and closes all connections for a pool key.
func (p *wsPool) Remove(key wsPoolKey) {
	p.mu.Lock()
	entry := p.conns[key]
	delete(p.conns, key)
	p.mu.Unlock()

	if entry == nil {
		return
	}
	for _, pc := range entry.conns {
		if pc != nil {
			_ = pc.conn.Close(websocket.StatusNormalClosure, "")
		}
	}
}

func (p *wsPool) RemoveConn(pc *pooledConn) {
	if pc == nil {
		return
	}
	p.mu.Lock()
	entry := p.conns[pc.poolKey]
	if entry != nil {
		for i, existing := range entry.conns {
			if existing == pc || (existing != nil && existing.id == pc.id) {
				entry.conns = append(entry.conns[:i], entry.conns[i+1:]...)
				break
			}
		}
		if len(entry.conns) == 0 {
			delete(p.conns, pc.poolKey)
		}
	}
	p.mu.Unlock()
	_ = pc.conn.Close(websocket.StatusNormalClosure, "")
}

func (p *wsPool) pooledConnCount(key wsPoolKey) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	count := 0
	if entry := p.conns[key]; entry != nil {
		count = len(entry.conns)
	}
	return count + p.inFlight[key]
}

func (p *wsPool) hasConnection(key wsPoolKey, connID string) bool {
	if connID == "" {
		return false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	entry := p.conns[key]
	if entry == nil {
		return false
	}
	for _, pc := range entry.conns {
		if pc != nil && pc.id == connID {
			return true
		}
	}
	return false
}

// reserveDial atomically checks the per-key cap and increments the in-flight
// counter. Returns true when a dial is allowed; the caller must invoke
// releaseDial exactly once after the dial completes (success or failure).
func (p *wsPool) reserveDial(key wsPoolKey) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	pooled := 0
	if entry := p.conns[key]; entry != nil {
		pooled = len(entry.conns)
	}
	if pooled+p.inFlight[key] >= wsMaxConnsPerPoolKey {
		return false
	}
	p.inFlight[key]++
	return true
}

func (p *wsPool) releaseDial(key wsPoolKey) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if c := p.inFlight[key]; c > 1 {
		p.inFlight[key] = c - 1
	} else {
		delete(p.inFlight, key)
	}
}

func (p *wsPool) preflightPreferredConnLocked(key wsPoolKey, entry *wsPoolEntry, pc *pooledConn, now time.Time) bool {
	if pc == nil || pc.conn == nil {
		return false
	}
	if now.Sub(pc.lastUsed) < wsHealthCheckIdle {
		return true
	}
	p.mu.Unlock()
	pingCtx, cancel := context.WithTimeout(context.Background(), wsHealthCheckTimeout)
	ping := p.pingConn
	if ping == nil {
		ping = func(ctx context.Context, conn *websocket.Conn) error { return conn.Ping(ctx) }
	}
	err := ping(pingCtx, pc.conn)
	cancel()
	p.mu.Lock()
	stillPooled := p.conns[key] == entry
	if stillPooled {
		stillPooled = false
		for _, existing := range entry.conns {
			if existing == pc || (existing != nil && existing.id == pc.id) {
				stillPooled = true
				break
			}
		}
	}
	if err == nil && stillPooled {
		pc.lastUsed = time.Now()
		return true
	}
	if !stillPooled {
		return false
	}
	log.Debugf("upstream WS preferred connection preflight failed (channel=%d, key=%d, conn_id=%s): %v", key.channelID, key.keyID, pc.id, err)
	if entry != nil {
		for i, existing := range entry.conns {
			if existing == pc || (existing != nil && existing.id == pc.id) {
				entry.conns = append(entry.conns[:i], entry.conns[i+1:]...)
				break
			}
		}
		if len(entry.conns) == 0 {
			delete(p.conns, key)
		}
	}
	_ = pc.conn.Close(websocket.StatusGoingAway, "preflight failed")
	return false
}

func (p *wsPool) pruneExpiredLocked(key wsPoolKey, entry *wsPoolEntry, now time.Time) {
	if entry == nil {
		return
	}
	kept := entry.conns[:0]
	for _, pc := range entry.conns {
		if pc == nil {
			continue
		}
		if now.Sub(pc.createdAt) > wsConnMaxAge {
			if pc.busy {
				pc.retire = true
				kept = append(kept, pc)
				continue
			}
			if pc.conn != nil {
				_ = pc.conn.Close(websocket.StatusGoingAway, "connection expired")
			}
			continue
		}
		kept = append(kept, pc)
	}
	entry.conns = kept
	if len(entry.conns) == 0 {
		delete(p.conns, key)
	}
}

// IsUnsupported checks if a channel is known to not support WS.
func (p *wsPool) IsUnsupported(channelID int) bool {
	p.unsupportedMu.RLock()
	defer p.unsupportedMu.RUnlock()

	t, ok := p.unsupported[channelID]
	if !ok {
		return false
	}
	// Re-check every 30 minutes
	return time.Since(t) < 30*time.Minute
}

// MarkUnsupported marks a channel as not supporting WS.
func (p *wsPool) MarkUnsupported(channelID int) {
	p.unsupportedMu.Lock()
	defer p.unsupportedMu.Unlock()
	p.unsupported[channelID] = time.Now()
}

// ShouldSkipWS returns true if the channel is in a health backoff period
// due to recent consecutive WS failures (transient errors, not definitive unsupported).
func (p *wsPool) ShouldSkipWS(channelID int) bool {
	p.healthMu.RLock()
	defer p.healthMu.RUnlock()

	h, ok := p.health[channelID]
	if !ok {
		return false
	}
	return time.Now().Before(h.skipUntil)
}

// RecordWSFailure increments the consecutive failure count for a channel
// and sets an exponential backoff period during which WS attempts are skipped.
func (p *wsPool) RecordWSFailure(channelID int) {
	p.healthMu.Lock()
	defer p.healthMu.Unlock()

	h, ok := p.health[channelID]
	if !ok {
		h = &wsChannelHealth{}
		p.health[channelID] = h
	}
	h.consecutiveFailures++
	now := time.Now()
	h.lastFailure = now
	h.skipUntil = now.Add(wsFailureBackoff(h.consecutiveFailures))
	log.Debugf("ws health: channel %d failure #%d, backoff until %v", channelID, h.consecutiveFailures, h.skipUntil.Format(time.TimeOnly))
}

// recordWSFailureForRequest keeps ordinary client cancellation from degrading
// the channel's WS transport health for later requests. Service-side relay
// budgets and first-token timeouts remain transport failures and are recorded,
// matching the shared cancellation classifier.
func (p *wsPool) recordWSFailureForRequest(ctx context.Context, channelID int, failure error) {
	if failure == nil {
		failure = contextError(ctx)
	}
	if p == nil || isClientCancellation(ctx, failure) {
		return
	}
	p.RecordWSFailure(channelID)
}

// RecordWSSuccess resets the failure counter for a channel after a successful WS stream.
func (p *wsPool) RecordWSSuccess(channelID int) {
	p.healthMu.Lock()
	defer p.healthMu.Unlock()
	delete(p.health, channelID)
}

// wsFailureBackoff returns the backoff duration based on consecutive failure count.
func wsFailureBackoff(failures int) time.Duration {
	switch {
	case failures <= 1:
		return wsHealthBackoffBase // 1min
	case failures == 2:
		return 2 * wsHealthBackoffBase // 2min
	default:
		return wsHealthBackoffMax // 5min cap
	}
}

// Dial creates a new WebSocket connection to the upstream. The caller must
// hold an in-flight reservation from reserveDial. On success the reservation
// is converted into a pooled entry atomically so concurrent dials see the new
// connection in pooledConnCount. On failure the reservation is released.
func (p *wsPool) Dial(ctx context.Context, key wsPoolKey, channel *dbmodel.Channel, baseUrl string, headers http.Header) (*pooledConn, bool, error) {
	// Build WS URL
	wsURL, err := buildWSURL(baseUrl)
	if err != nil {
		p.releaseDial(key)
		return nil, false, fmt.Errorf("invalid base url for ws: %w", err)
	}

	// Get HTTP client for proxy settings
	httpClient, err := helper.ChannelHTTPClientWithContext(ctx, channel)
	if err != nil {
		p.releaseDial(key)
		return nil, false, fmt.Errorf("failed to get http client: %w", err)
	}
	httpClient = cloneHTTPClientForWSDial(httpClient)

	dialCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	opts := &websocket.DialOptions{
		HTTPClient: httpClient,
		HTTPHeader: headers,
	}

	conn, response, err := websocket.Dial(dialCtx, wsURL, opts)
	if err != nil {
		p.releaseDial(key)
		return nil, shouldMarkWSUnsupported(response, err), err
	}

	// Set read limit high for large responses (e.g., image generation)
	conn.SetReadLimit(upstreamWSReadLimit(maxSSEEventSize))

	pc := &pooledConn{
		id:        nextWSConnID(),
		conn:      conn,
		createdAt: time.Now(),
		lastUsed:  time.Now(),
		busy:      true,
		queue:     1,
		poolKey:   key,
	}

	// Atomically convert the in-flight reservation into a pooled entry so
	// pooledConnCount stays consistent across concurrent dials. The pc is
	// busy=true so GetPreferred won't return it until Put toggles it.
	p.mu.Lock()
	if c := p.inFlight[key]; c > 1 {
		p.inFlight[key] = c - 1
	} else {
		delete(p.inFlight, key)
	}
	entry := p.conns[key]
	if entry == nil {
		entry = &wsPoolEntry{}
		p.conns[key] = entry
	}
	entry.conns = append(entry.conns, pc)
	p.mu.Unlock()

	return pc, false, nil
}

func buildUpstreamWSHeaders(clientHeaders http.Header, channel *dbmodel.Channel, key string) http.Header {
	headers := http.Header{}
	entries := collectNormalizedHeaderEntries(clientHeaders, func(name string) bool {
		return shouldProxyUpstreamWSHeader(name)
	})
	for _, entry := range entries {
		setHeaderValuesCaseInsensitive(headers, entry.name, entry.values)
	}
	userAgent := headerValuesCaseInsensitive(headers, "User-Agent")
	if len(userAgent) == 0 {
		setHeaderValuesCaseInsensitive(headers, "User-Agent", []string{""})
	} else {
		setHeaderValuesCaseInsensitive(headers, "User-Agent", userAgent[:1])
	}
	if channel != nil {
		applySafeWSChannelHeaders(headers, channel.CustomHeader)
	}
	applySelectedCredentialHeader(headers, outbound.OutboundTypeOpenAIResponse, key)
	setHeaderValuesCaseInsensitive(headers, "OpenAI-Beta", []string{"responses_websockets=2026-02-06"})
	return headers
}

func applySafeWSChannelHeaders(dst http.Header, headers []dbmodel.CustomHeader) {
	if dst == nil {
		return
	}
	for _, header := range headers {
		name := strings.TrimSpace(header.HeaderKey)
		if !shouldProxyUpstreamWSChannelHeader(name) {
			continue
		}
		setHeaderValuesCaseInsensitive(dst, name, []string{header.HeaderValue})
	}
}

func shouldProxyUpstreamWSHeader(name string) bool {
	lowerName := strings.ToLower(strings.TrimSpace(name))
	if lowerName == "" {
		return false
	}
	if isBlockedUpstreamHeader(lowerName) {
		return false
	}
	if strings.HasPrefix(lowerName, "sec-websocket-") {
		return false
	}
	return true
}

func shouldProxyUpstreamWSChannelHeader(name string) bool {
	lowerName := strings.ToLower(strings.TrimSpace(name))
	if lowerName == "" {
		return false
	}
	if isBlockedChannelHeader(lowerName) {
		return false
	}
	if strings.HasPrefix(lowerName, "sec-websocket-") {
		return false
	}
	return true
}

func newWSPoolKey(channelID, keyID int, headers http.Header) wsPoolKey {
	return wsPoolKey{channelID: channelID, keyID: keyID, headerSig: wsHeaderSignature(headers)}
}

func wsHeaderSignature(headers http.Header) string {
	if len(headers) == 0 {
		return ""
	}
	normalized := make(map[string][]string, len(headers))
	for key, values := range headers {
		name := strings.ToLower(strings.TrimSpace(key))
		if name == "" {
			continue
		}
		normalized[name] = append(normalized[name], values...)
	}
	keys := make([]string, 0, len(normalized))
	for key := range normalized {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	var builder strings.Builder
	for _, key := range keys {
		values := append([]string(nil), normalized[key]...)
		sort.Strings(values)
		builder.WriteString(key)
		builder.WriteByte('=')
		for i, value := range values {
			if i > 0 {
				builder.WriteByte(',')
			}
			builder.WriteString(value)
		}
		builder.WriteByte('\n')
	}
	digest := sha256.Sum256([]byte(builder.String()))
	return fmt.Sprintf("%x", digest[:])
}

func cloneHTTPClientForWSDial(httpClient *http.Client) *http.Client {
	if httpClient == nil {
		return nil
	}
	clonedClient := *httpClient
	if transport, ok := httpClient.Transport.(*http.Transport); ok && transport != nil {
		clonedTransport := transport.Clone()
		clonedTransport.DisableCompression = true
		clonedClient.Transport = clonedTransport
		return &clonedClient
	}
	if httpClient.Transport == nil {
		if defaultTransport, ok := http.DefaultTransport.(*http.Transport); ok {
			clonedTransport := defaultTransport.Clone()
			clonedTransport.DisableCompression = true
			clonedClient.Transport = clonedTransport
		}
	}
	return &clonedClient
}

// SendResponseCreate sends a response.create message on a WS connection.
func (p *wsPool) SendResponseCreate(ctx context.Context, pc *pooledConn, requestBody json.RawMessage) error {
	merged, err := buildWSResponseCreateMessage(requestBody)
	if err != nil {
		return err
	}
	return p.SendRaw(ctx, pc, merged)
}

func (p *wsPool) SendRaw(ctx context.Context, pc *pooledConn, payload []byte) error {
	if pc == nil || pc.conn == nil {
		return fmt.Errorf("ws connection is nil")
	}
	writeCtx, cancel := context.WithTimeout(ctx, wsWriteTimeout)
	defer cancel()
	return pc.conn.Write(writeCtx, websocket.MessageText, payload)
}

func buildWSResponseCreateMessage(requestBody json.RawMessage) ([]byte, error) {
	// Merge type field into the request body
	var bodyMap map[string]json.RawMessage
	if err := json.Unmarshal(requestBody, &bodyMap); err != nil {
		return nil, fmt.Errorf("failed to parse request body: %w", err)
	}
	bodyMap["type"] = json.RawMessage(`"response.create"`)

	// OpenAI Responses WS mode uses response.create with a Responses-shaped body.
	// Keep/force stream=true for Codex/OpenAI fidelity; background is not used in WS mode.
	bodyMap["stream"] = json.RawMessage(`true`)
	delete(bodyMap, "background")

	merged, err := json.Marshal(bodyMap)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal ws message: %w", err)
	}

	return merged, nil
}

func buildWSURL(baseUrl string) (string, error) {
	parsed, err := url.Parse(strings.TrimSuffix(baseUrl, "/"))
	if err != nil {
		return "", err
	}

	// Convert http(s) to ws(s)
	switch parsed.Scheme {
	case "https":
		parsed.Scheme = "wss"
	case "http":
		parsed.Scheme = "ws"
	case "wss", "ws":
		// already WS
	default:
		parsed.Scheme = "wss"
	}

	parsed.Path = parsed.Path + "/responses"
	return parsed.String(), nil
}

func shouldMarkWSUnsupported(response *http.Response, err error) bool {
	statusCode := 0
	if response != nil {
		statusCode = response.StatusCode
	}
	switch statusCode {
	case http.StatusNotFound, http.StatusMethodNotAllowed, http.StatusNotImplemented:
		return true
	}

	if err == nil {
		return false
	}

	message := strings.ToLower(err.Error())
	return strings.Contains(message, "status code 404") ||
		strings.Contains(message, "status code 405") ||
		strings.Contains(message, "status code 501") ||
		strings.Contains(message, " got 404") ||
		strings.Contains(message, " got 405") ||
		strings.Contains(message, " got 501")
}

func (p *wsPool) cleanupLoop() {
	ticker := time.NewTicker(wsPoolCleanupEvery)
	defer ticker.Stop()

	for {
		select {
		case <-p.stopCh:
			return
		case <-ticker.C:
			p.cleanup()
		}
	}
}

func (p *wsPool) cleanup() {
	var toClose []*pooledConn

	p.mu.Lock()
	now := time.Now()
	for key, entry := range p.conns {
		if entry == nil {
			delete(p.conns, key)
			continue
		}
		idleCount := 0
		for _, pc := range entry.conns {
			if pc != nil && !pc.busy {
				idleCount++
			}
		}
		kept := entry.conns[:0]
		for _, pc := range entry.conns {
			if pc == nil {
				continue
			}
			expired := now.Sub(pc.createdAt) > wsConnMaxAge
			if expired && pc.busy {
				pc.retire = true
			}
			shouldClose := expired && !pc.busy
			if !shouldClose && !pc.busy && now.Sub(pc.lastUsed) > wsConnIdleTimeout {
				shouldClose = true
			}
			if !shouldClose && !pc.busy && idleCount > wsMaxIdlePerPoolKey {
				shouldClose = true
				idleCount--
			}
			if shouldClose {
				toClose = append(toClose, pc)
				continue
			}
			kept = append(kept, pc)
		}
		entry.conns = kept
		if len(entry.conns) == 0 {
			delete(p.conns, key)
		}
	}
	p.mu.Unlock()

	for _, pc := range toClose {
		if pc.conn != nil {
			_ = pc.conn.Close(websocket.StatusGoingAway, "cleanup")
		}
	}

	// Clean up old unsupported entries
	p.unsupportedMu.Lock()
	for id, t := range p.unsupported {
		if now.Sub(t) > 30*time.Minute {
			delete(p.unsupported, id)
		}
	}
	p.unsupportedMu.Unlock()

	// Clean up stale health entries (no failure for wsHealthStaleAfter)
	p.healthMu.Lock()
	for id, h := range p.health {
		if now.Sub(h.lastFailure) > wsHealthStaleAfter {
			delete(p.health, id)
		}
	}
	p.healthMu.Unlock()
}

// Close shuts down the pool and all connections.
func (p *wsPool) Close() {
	p.once.Do(func() {
		close(p.stopCh)

		p.mu.Lock()
		defer p.mu.Unlock()

		for key, entry := range p.conns {
			if entry != nil {
				for _, pc := range entry.conns {
					if pc != nil {
						_ = pc.conn.Close(websocket.StatusGoingAway, "shutdown")
					}
				}
			}
			delete(p.conns, key)
		}
		for key := range p.inFlight {
			delete(p.inFlight, key)
		}
	})
}

// TryUpstreamWS attempts to get or create a WS connection for an upstream channel.
// Returns nil if the channel doesn't support WS or connection fails.
func TryUpstreamWS(ctx context.Context, channel *dbmodel.Channel, baseUrl, key string, keyID int, clientHeaders http.Header, forceRedial ...bool) *pooledConn {
	return TryUpstreamWSWithPreference(ctx, channel, baseUrl, key, keyID, clientHeaders, "", forceRedial...)
}

func TryUpstreamWSWithPreference(ctx context.Context, channel *dbmodel.Channel, baseUrl, key string, keyID int, clientHeaders http.Header, preferredConnID string, forceRedial ...bool) *pooledConn {
	if channel == nil || wsUpstreamPool == nil {
		return nil
	}
	if wsUpstreamPool.IsUnsupported(channel.ID) {
		return nil
	}
	if wsUpstreamPool.ShouldSkipWS(channel.ID) {
		log.Debugf("skipping upstream WS for channel %d (health backoff)", channel.ID)
		return nil
	}

	headers := buildUpstreamWSHeaders(clientHeaders, channel, key)
	poolKey := newWSPoolKey(channel.ID, keyID, headers)
	redial := len(forceRedial) > 0 && forceRedial[0]

	deadline := time.Now().Add(wsAcquireTimeout)
	for {
		waitingForPreferred := false
		if !redial {
			if pc := wsUpstreamPool.GetPreferred(poolKey, preferredConnID); pc != nil {
				return pc
			}
			if preferredConnID != "" {
				if !wsUpstreamPool.hasConnection(poolKey, preferredConnID) {
					return nil
				}
				waitingForPreferred = true
			}
		}
		redial = false
		if !waitingForPreferred && wsUpstreamPool.reserveDial(poolKey) {
			pc, unsupported, err := wsUpstreamPool.Dial(ctx, poolKey, channel, baseUrl, headers)
			if err != nil {
				if unsupported {
					log.Debugf("upstream WS dial failed for channel %d, marking unsupported: %v", channel.ID, err)
					wsUpstreamPool.MarkUnsupported(channel.ID)
				} else {
					log.Debugf("upstream WS dial failed for channel %d: %v", channel.ID, err)
					wsUpstreamPool.recordWSFailureForRequest(ctx, channel.ID, err)
				}
				return nil
			}
			return pc
		}
		if preferredConnID == "" || time.Now().After(deadline) || ctx.Err() != nil {
			return nil
		}
		log.Debugf("waiting for preferred upstream WS connection (channel=%d, key=%d, conn_id=%s)", channel.ID, keyID, preferredConnID)
		timer := time.NewTimer(50 * time.Millisecond)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return nil
		case <-timer.C:
		}
	}
}

// CloseUpstreamWSPool gracefully shuts down the upstream WS pool.
func CloseUpstreamWSPool() {
	wsUpstreamPool.Close()
}

func resetWSUpstreamPool() {
	if wsUpstreamPool != nil {
		wsUpstreamPool.Close()
	}
	wsUpstreamPool = newWSPool()
}
