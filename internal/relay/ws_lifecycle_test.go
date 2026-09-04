package relay

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bestruirui/octopus/internal/model"
	"github.com/bestruirui/octopus/internal/op"
	"github.com/bestruirui/octopus/internal/transformer/inbound"
	transformerModel "github.com/bestruirui/octopus/internal/transformer/model"
	"github.com/bestruirui/octopus/internal/transformer/outbound"
	"github.com/coder/websocket"
	"github.com/gin-gonic/gin"
)

// wsLifecycleServerEvents defines the behavior of a fake upstream WS server.
type wsLifecycleServerEvents struct {
	frames []string
	// skipInitialRead writes frames immediately after accept instead of
	// waiting for a response.create message first.
	skipInitialRead bool
	closeSignal     chan struct{} // signaled after the client closed the connection (server read error)
}

func newWSLifecycleServer(t *testing.T, ev wsLifecycleServerEvents) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close(websocket.StatusNormalClosure, "")
		if !ev.skipInitialRead {
			_, _, err = conn.Read(r.Context())
			if err != nil {
				if ev.closeSignal != nil {
					ev.closeSignal <- struct{}{}
				}
				return
			}
		}
		for _, frame := range ev.frames {
			if writeErr := conn.Write(r.Context(), websocket.MessageText, []byte(frame)); writeErr != nil {
				break
			}
		}
		// Block until the client closes the connection; the resulting read
		// error is the observable proof that the underlying socket closed.
		_, _, _ = conn.Read(r.Context())
		if ev.closeSignal != nil {
			ev.closeSignal <- struct{}{}
		}
	}))
	t.Cleanup(func() { server.Close() })
	return server
}

func newWSLifecycleTransformAttempt(t *testing.T, dbCtx context.Context, name, serverURL string, reqCtx context.Context) (*relayAttempt, *model.Channel, *gin.Context, *httptest.ResponseRecorder) {
	t.Helper()
	channel := &model.Channel{
		Name:     name,
		Type:     outbound.OutboundTypeOpenAIResponse,
		Enabled:  true,
		BaseUrls: []model.BaseUrl{{URL: serverURL + "/v1"}},
		Model:    "gpt-4o",
		Keys:     []model.ChannelKey{{Enabled: true, ChannelKey: "lifecycle-key"}},
	}
	if err := op.ChannelCreate(channel, dbCtx); err != nil {
		t.Fatalf("ChannelCreate failed: %v", err)
	}

	writer := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(writer)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil).WithContext(reqCtx)

	internalReq := &transformerModel.InternalLLMRequest{Model: "gpt-4o", Stream: boolPtr(true)}
	req := &relayRequest{
		c:               c,
		inAdapter:       inbound.Get(inbound.InboundTypeOpenAIResponse),
		internalRequest: internalReq,
		metrics:         NewRelayMetrics(1, "gpt-4o", nil, internalReq),
		apiKeyID:        1,
		requestModel:    "gpt-4o",
	}
	ra := &relayAttempt{relayRequest: req, outAdapter: outbound.Get(channel.Type), channel: channel, usedKey: channel.Keys[0]}
	return ra, channel, c, writer
}

func wsLifecyclePoolKey(channel *model.Channel, c *gin.Context) wsPoolKey {
	var headers http.Header
	if c != nil && c.Request != nil {
		headers = c.Request.Header
	}
	return newWSPoolKey(channel.ID, channel.Keys[0].ID, buildUpstreamWSHeaders(headers, channel, channel.Keys[0].ChannelKey))
}

// A structured upstream error frame mid-stream must remove the pooled
// connection (pool count zero, not retrievable via GetPreferred) and close the
// underlying socket.
func TestWSLifecycleTransformErrorFrameRemovesPooledConn(t *testing.T) {
	gin.SetMode(gin.TestMode)
	dbCtx := setupRelayTestDB(t)
	resetWSUpstreamPool()
	t.Cleanup(resetWSUpstreamPool)

	closedCh := make(chan struct{}, 1)
	server := newWSLifecycleServer(t, wsLifecycleServerEvents{
		frames: []string{
			`{"type":"response.created","response":{"id":"resp_err","model":"gpt-4o"}}`,
			`{"type":"response.output_text.delta","delta":"partial"}`,
			`{"type":"error","code":"server_error","message":"boom"}`,
		},
		closeSignal: closedCh,
	})

	ctx := context.Background()
	ra, channel, c, _ := newWSLifecycleTransformAttempt(t, dbCtx, "ws-lifecycle-error-frame", server.URL, ctx)
	key := wsLifecyclePoolKey(channel, c)

	status, err := ra.forwardViaWS(ctx)
	if err == nil {
		t.Fatalf("expected error after upstream error frame, got status=%d", status)
	}

	if got := wsUpstreamPool.GetPreferred(key, ""); got != nil {
		wsUpstreamPool.RemoveConn(got)
		t.Fatalf("expected pooled connection to be unavailable after error frame")
	}
	if count := wsUpstreamPool.pooledConnCount(key); count != 0 {
		t.Fatalf("expected pool count 0 after error frame, got %d", count)
	}
	select {
	case <-closedCh:
	case <-time.After(5 * time.Second):
		t.Fatalf("expected underlying ws connection to be closed by the error path")
	}
}

func TestWSLifecycleTransformStructuredFailurePreservesTerminal(t *testing.T) {
	gin.SetMode(gin.TestMode)
	dbCtx := setupRelayTestDB(t)
	resetWSUpstreamPool()
	t.Cleanup(resetWSUpstreamPool)

	server := newWSLifecycleServer(t, wsLifecycleServerEvents{
		frames: []string{
			`{"type":"response.created","response":{"id":"resp_failed","model":"gpt-4o"}}`,
			`{"type":"response.output_text.delta","delta":"partial"}`,
			`{"type":"response.failed","response":{"id":"resp_failed","model":"gpt-4o","status":"failed","error":{"code":500,"message":"boom","type":"server_error"}}}`,
		},
	})
	ctx := context.Background()
	ra, channel, c, writer := newWSLifecycleTransformAttempt(t, dbCtx, "ws-lifecycle-structured-failure", server.URL, ctx)
	key := wsLifecyclePoolKey(channel, c)
	status, err := ra.forwardViaWS(ctx)
	if err == nil {
		t.Fatalf("expected structured failure error, got status=%d", status)
	}
	body := writer.Body.String()
	if strings.Count(body, "response.failed") != 1 || !strings.Contains(body, "500") || strings.Contains(body, "response.incomplete") {
		t.Fatalf("expected one structured failure terminal, got %s", body)
	}
	if got := wsUpstreamPool.GetPreferred(key, ""); got != nil {
		wsUpstreamPool.RemoveConn(got)
		t.Fatal("structured failure connection must not return to pool")
	}
}

// A fully consumed transform stream must return the connection to the pool for
// reuse.
func TestWSLifecycleSuccessfulTransformStreamReturnsConnToPool(t *testing.T) {
	gin.SetMode(gin.TestMode)
	dbCtx := setupRelayTestDB(t)
	resetWSUpstreamPool()
	t.Cleanup(resetWSUpstreamPool)

	server := newWSLifecycleServer(t, wsLifecycleServerEvents{
		frames: []string{
			`{"type":"response.created","response":{"id":"resp_ok","model":"gpt-4o"}}`,
			`{"type":"response.output_text.delta","delta":"hello"}`,
			`{"type":"response.completed","response":{"id":"resp_ok","model":"gpt-4o","status":"completed","usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}}`,
		},
	})

	ctx := context.Background()
	ra, channel, c, _ := newWSLifecycleTransformAttempt(t, dbCtx, "ws-lifecycle-success", server.URL, ctx)
	key := wsLifecyclePoolKey(channel, c)

	status, err := ra.forwardViaWS(ctx)
	if err != nil || status != http.StatusOK {
		t.Fatalf("expected successful ws transform stream, got status=%d err=%v", status, err)
	}

	pc := wsUpstreamPool.GetPreferred(key, "")
	if pc == nil {
		t.Fatalf("expected pooled connection to be reusable after a fully consumed stream")
	}
	if count := wsUpstreamPool.pooledConnCount(key); count != 1 {
		t.Fatalf("expected pool count 1 after success, got %d", count)
	}
	wsUpstreamPool.RemoveConn(pc)
}

// Close/CloseWithError must be idempotent, and an error close after a normal
// Close must still retract the connection from the pool.
func TestWSLifecycleReaderCloseIdempotentAndErrorRetractsPooledConn(t *testing.T) {
	gin.SetMode(gin.TestMode)
	dbCtx := setupRelayTestDB(t)
	resetWSUpstreamPool()
	t.Cleanup(resetWSUpstreamPool)

	closedCh := make(chan struct{}, 4)
	server := newWSLifecycleServer(t, wsLifecycleServerEvents{closeSignal: closedCh})

	channel := &model.Channel{
		Name:     "ws-lifecycle-reader",
		Type:     outbound.OutboundTypeOpenAIResponse,
		Enabled:  true,
		BaseUrls: []model.BaseUrl{{URL: server.URL + "/v1"}},
		Model:    "gpt-4o",
		Keys:     []model.ChannelKey{{Enabled: true, ChannelKey: "lifecycle-key"}},
	}
	if err := op.ChannelCreate(channel, dbCtx); err != nil {
		t.Fatalf("ChannelCreate failed: %v", err)
	}
	key := wsLifecyclePoolKey(channel, nil)

	// Scenario A: normal Close first, then error close must retract.
	pc := TryUpstreamWS(context.Background(), channel, channel.GetBaseUrl(), channel.Keys[0].ChannelKey, channel.Keys[0].ID, nil, true)
	if pc == nil {
		t.Fatalf("expected ws dial to succeed")
	}
	reader := newWSUpstreamReader(pc, channel.ID, channel.Keys[0].ID)

	if err := reader.Close(); err != nil {
		t.Fatalf("first Close failed: %v", err)
	}
	if count := wsUpstreamPool.pooledConnCount(key); count != 1 {
		t.Fatalf("expected pool count 1 after Close, got %d", count)
	}
	if err := reader.Close(); err != nil {
		t.Fatalf("second Close failed: %v", err)
	}
	if count := wsUpstreamPool.pooledConnCount(key); count != 1 {
		t.Fatalf("expected repeated Close to be a no-op, got pool count %d", count)
	}

	reader.CloseWithError() // must retract the already-pooled connection
	if got := wsUpstreamPool.GetPreferred(key, ""); got != nil {
		wsUpstreamPool.RemoveConn(got)
		t.Fatalf("expected pooled connection retracted after CloseWithError")
	}
	if count := wsUpstreamPool.pooledConnCount(key); count != 0 {
		t.Fatalf("expected pool count 0 after CloseWithError, got %d", count)
	}
	reader.CloseWithError() // idempotent
	if count := wsUpstreamPool.pooledConnCount(key); count != 0 {
		t.Fatalf("expected repeated CloseWithError to be a no-op, got pool count %d", count)
	}
	if err := reader.Close(); err != nil {
		t.Fatalf("Close after CloseWithError failed: %v", err)
	}
	if count := wsUpstreamPool.pooledConnCount(key); count != 0 {
		t.Fatalf("expected Close after CloseWithError to not re-pool a closed connection, got %d", count)
	}

	// Scenario B: error close first must never pool the connection.
	pc2 := TryUpstreamWS(context.Background(), channel, channel.GetBaseUrl(), channel.Keys[0].ChannelKey, channel.Keys[0].ID, nil, true)
	if pc2 == nil {
		t.Fatalf("expected second ws dial to succeed")
	}
	reader2 := newWSUpstreamReader(pc2, channel.ID, channel.Keys[0].ID)
	reader2.CloseWithError()
	if err := reader2.Close(); err != nil {
		t.Fatalf("Close after CloseWithError failed: %v", err)
	}
	if count := wsUpstreamPool.pooledConnCount(key); count != 0 {
		t.Fatalf("expected pool count 0 after error-first close, got %d", count)
	}

	// Both underlying connections must have been closed.
	for i := 0; i < 2; i++ {
		select {
		case <-closedCh:
		case <-time.After(5 * time.Second):
			t.Fatalf("expected underlying ws connection %d to be closed", i+1)
		}
	}
}

// A downstream cancellation mid-stream must remove and close the pooled
// connection.
func TestWSLifecycleClientCancelRemovesPooledConn(t *testing.T) {
	gin.SetMode(gin.TestMode)
	dbCtx := setupRelayTestDB(t)
	resetWSUpstreamPool()
	t.Cleanup(resetWSUpstreamPool)

	closedCh := make(chan struct{}, 1)
	server := newWSLifecycleServer(t, wsLifecycleServerEvents{
		frames: []string{
			`{"type":"response.created","response":{"id":"resp_cancel","model":"gpt-4o"}}`,
			`{"type":"response.output_text.delta","delta":"partial"}`,
		},
		closeSignal: closedCh,
	})

	ctx, cancel := context.WithCancel(context.Background())
	ra, channel, c, _ := newWSLifecycleTransformAttempt(t, dbCtx, "ws-lifecycle-cancel", server.URL, ctx)
	key := wsLifecyclePoolKey(channel, c)

	type result struct {
		status int
		err    error
	}
	done := make(chan result, 1)
	go func() {
		status, err := ra.forwardViaWS(ctx)
		done <- result{status: status, err: err}
	}()

	// Give the stream time to deliver the partial payload, then cancel.
	time.Sleep(150 * time.Millisecond)
	cancel()

	select {
	case res := <-done:
		if res.err == nil {
			t.Fatalf("expected error after client cancellation, got status=%d", res.status)
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("forwardViaWS did not return after cancellation")
	}

	if got := wsUpstreamPool.GetPreferred(key, ""); got != nil {
		wsUpstreamPool.RemoveConn(got)
		t.Fatalf("expected pooled connection to be unavailable after cancellation")
	}
	if count := wsUpstreamPool.pooledConnCount(key); count != 0 {
		t.Fatalf("expected pool count 0 after cancellation, got %d", count)
	}
	select {
	case <-closedCh:
	case <-time.After(5 * time.Second):
		t.Fatalf("expected underlying ws connection to be closed after cancellation")
	}
}

// Close must not return the connection to the pool while a ReadEvent is still
// blocked on the socket; the put happens only after the read drained.
func TestWSLifecycleCloseWaitsForInFlightReadBeforePoolPut(t *testing.T) {
	gin.SetMode(gin.TestMode)
	dbCtx := setupRelayTestDB(t)
	resetWSUpstreamPool()
	t.Cleanup(resetWSUpstreamPool)

	server := newWSLifecycleServer(t, wsLifecycleServerEvents{
		frames: []string{
			`{"type":"response.created","response":{"id":"resp_join","model":"gpt-4o"}}`,
		},
		skipInitialRead: true,
	})

	channel := &model.Channel{
		Name:     "ws-lifecycle-join",
		Type:     outbound.OutboundTypeOpenAIResponse,
		Enabled:  true,
		BaseUrls: []model.BaseUrl{{URL: server.URL + "/v1"}},
		Model:    "gpt-4o",
		Keys:     []model.ChannelKey{{Enabled: true, ChannelKey: "lifecycle-key"}},
	}
	if err := op.ChannelCreate(channel, dbCtx); err != nil {
		t.Fatalf("ChannelCreate failed: %v", err)
	}
	key := wsLifecyclePoolKey(channel, nil)

	pc := TryUpstreamWS(context.Background(), channel, channel.GetBaseUrl(), channel.Keys[0].ChannelKey, channel.Keys[0].ID, nil, true)
	if pc == nil {
		t.Fatalf("expected ws dial to succeed")
	}
	reader := newWSUpstreamReader(pc, channel.ID, channel.Keys[0].ID)

	readCtx, cancelRead := context.WithCancel(context.Background())
	defer cancelRead()
	firstRead := make(chan error, 1)
	secondRead := make(chan error, 1)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, err := reader.ReadEvent(readCtx)
		firstRead <- err
		// Second read blocks: the server stays silent after the first frame.
		_, err = reader.ReadEvent(readCtx)
		secondRead <- err
	}()

	select {
	case err := <-firstRead:
		if err != nil {
			t.Fatalf("expected first ReadEvent to succeed, got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("first ReadEvent did not return")
	}

	// Let the reader goroutine enter the blocking second read.
	time.Sleep(50 * time.Millisecond)

	closeDone := make(chan struct{})
	go func() {
		_ = reader.Close()
		close(closeDone)
	}()

	// While the second read is blocked, Close must not put the connection back.
	// GetPreferred only returns idle connections, so it observes the Put.
	deadline := time.Now().Add(300 * time.Millisecond)
	for time.Now().Before(deadline) {
		if got := wsUpstreamPool.GetPreferred(key, ""); got != nil {
			wsUpstreamPool.RemoveConn(got)
			t.Fatalf("connection returned to pool while a read is still in flight")
		}
		time.Sleep(10 * time.Millisecond)
	}

	cancelRead()
	select {
	case <-closeDone:
	case <-time.After(5 * time.Second):
		t.Fatalf("Close did not return after in-flight read drained")
	}
	select {
	case <-secondRead:
	case <-time.After(5 * time.Second):
		t.Fatalf("in-flight ReadEvent did not return after context cancel")
	}
	wg.Wait()
	got := wsUpstreamPool.GetPreferred(key, "")
	if got == nil {
		t.Fatalf("expected connection pooled after in-flight read drained")
	}
	wsUpstreamPool.RemoveConn(got)
}
