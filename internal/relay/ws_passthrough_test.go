package relay

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bestruirui/octopus/internal/model"
	"github.com/bestruirui/octopus/internal/op"
	"github.com/bestruirui/octopus/internal/outlierwindow"
	"github.com/bestruirui/octopus/internal/relay/balancer"
	"github.com/bestruirui/octopus/internal/relay/stream"
	"github.com/bestruirui/octopus/internal/transformer/inbound"
	transformerModel "github.com/bestruirui/octopus/internal/transformer/model"
	"github.com/bestruirui/octopus/internal/transformer/outbound"
	"github.com/coder/websocket"
	"github.com/gin-gonic/gin"
)

// passthroughScriptedWriter provides deterministic partial/failing writes for
// passthrough lifecycle tests without involving a downstream socket.
type passthroughScriptedWriter struct {
	header  http.Header
	data    []byte
	n       int
	err     error
	writes  int
	written bool
	closeFn func()
}

func (w *passthroughScriptedWriter) Write(data []byte) (int, error) {
	w.writes++
	w.data = append(w.data[:0], data...)
	if w.n > 0 {
		w.written = true
	}
	return w.n, w.err
}
func (w *passthroughScriptedWriter) Flush()        {}
func (w *passthroughScriptedWriter) Written() bool { return w.written }
func (w *passthroughScriptedWriter) Header() http.Header {
	if w.header == nil {
		w.header = make(http.Header)
	}
	return w.header
}
func (w *passthroughScriptedWriter) WriteHeader(int) {}

func (w *passthroughScriptedWriter) CloseWithError() {
	if w.closeFn != nil {
		w.closeFn()
	}
}

func TestWSPassthroughFirstTokenTimeout(t *testing.T) {
	clientConn, serverConn := newTestWSConnPair(t)
	t.Cleanup(func() {
		clientConn.CloseNow()
		serverConn.CloseNow()
	})
	stream := true
	writer := &passthroughScriptedWriter{}
	ra := &relayAttempt{
		relayRequest: &relayRequest{
			internalRequest: &transformerModel.InternalLLMRequest{Stream: &stream},
			streamWriter:    writer,
		},
		firstTokenTimeOutSec: 1,
	}
	started := time.Now()
	_, err := ra.handleWSPassthroughStream(context.Background(), &pooledConn{conn: clientConn})
	if !errors.Is(err, errFirstTokenTimeout) {
		t.Fatalf("expected first token timeout, got %v", err)
	}
	if elapsed := time.Since(started); elapsed > 2*time.Second {
		t.Fatalf("first token timeout exceeded budget: %v", elapsed)
	}
	if ra.streamPayloadWritten.Load() || writer.Written() {
		t.Fatal("first token timeout must not mark downstream payload written")
	}
}

func TestWSPassthroughDownstreamBreakDrainsWithoutUpstreamFailure(t *testing.T) {
	clientConn, serverConn := newTestWSConnPair(t)
	t.Cleanup(func() {
		clientConn.CloseNow()
		serverConn.CloseNow()
	})
	closed := false
	writer := &passthroughScriptedWriter{err: errors.New("broken pipe"), closeFn: func() { closed = true }}
	ra := &relayAttempt{
		relayRequest: &relayRequest{streamWriter: writer},
		channel:      &model.Channel{ID: 41},
		usedKey:      model.ChannelKey{ID: 42},
	}
	writeDone := make(chan error, 1)
	go func() {
		if err := serverConn.Write(context.Background(), websocket.MessageText, []byte(`{"type":"response.output_text.delta","delta":"partial"}`)); err != nil {
			writeDone <- err
			return
		}
		writeDone <- serverConn.Write(context.Background(), websocket.MessageText, []byte(`{"type":"error","status":429,"error":{"code":"rate_limit","message":"slow down"}}`))
	}()
	stats, err := ra.handleWSPassthroughStream(context.Background(), &pooledConn{conn: clientConn})
	if !errors.Is(err, errWSPassthroughDownstreamClosed) {
		t.Fatalf("expected downstream-closed sentinel, got %v", err)
	}
	if writeErr := <-writeDone; writeErr != nil {
		t.Fatalf("write upstream frames: %v", writeErr)
	}
	if stats == nil || !stats.DownstreamBroken || strings.Contains(err.Error(), "before first event") || errors.Is(err, transformerModel.ErrIncompleteUpstreamStream) {
		t.Fatalf("downstream break was misclassified: stats=%+v err=%v", stats, err)
	}
	if !ra.streamPayloadWritten.Load() {
		t.Fatal("downstream break must block retry/failover")
	}
	if closed {
		t.Fatal("classified downstream break should drain without a second close attempt")
	}
}

func TestWriteWSPassthroughDownstreamReportsPartialVisibility(t *testing.T) {
	writer := &passthroughScriptedWriter{n: 1, err: errors.New("partial write")}
	written, err := writeWSPassthroughDownstream(context.Background(), writer, []byte(`{"type":"response.created"}`))
	if err == nil || !written || !writer.Written() {
		t.Fatalf("partial write visibility lost: written=%t writer=%t err=%v", written, writer.Written(), err)
	}
}

func TestWriteWSPassthroughDownstreamCompactsMultilineJSON(t *testing.T) {
	writer := &passthroughScriptedWriter{n: 64}
	written, err := writeWSPassthroughDownstream(context.Background(), writer, []byte("{\n  \"type\": \"response.completed\"\n}"))
	if err != nil || !written {
		t.Fatalf("multiline write failed: written=%t err=%v", written, err)
	}
	if strings.Count(string(writer.data), "\n") != 2 || !strings.Contains(string(writer.data), `{"type":"response.completed"}`) {
		t.Fatalf("multiline JSON was not compacted into one SSE data line: %q", writer.data)
	}
}

func TestObserveWSPassthroughFailedWithoutErrorObject(t *testing.T) {
	stats := &wsPassthroughStats{}
	observeWSPassthroughEvent(stats, []byte(`{"type":"response.failed","response":{"id":"resp_failed","status":"failed"}}`))
	if stats.Error == nil || stats.ResponseID != "resp_failed" || stats.Error.Message == "" {
		t.Fatalf("response.failed without error object was treated as success: %+v", stats)
	}
}

func TestWSPassthroughUnknownPartialWriteClosesDownstream(t *testing.T) {
	clientConn, serverConn := newTestWSConnPair(t)
	t.Cleanup(func() {
		clientConn.CloseNow()
		serverConn.CloseNow()
	})
	closed := false
	writer := &passthroughScriptedWriter{n: 1, err: errors.New("custom write failure"), closeFn: func() { closed = true }}
	ra := &relayAttempt{
		relayRequest: &relayRequest{streamWriter: writer},
		channel:      &model.Channel{ID: 51},
		usedKey:      model.ChannelKey{ID: 52},
	}
	writeDone := make(chan error, 1)
	go func() {
		writeDone <- serverConn.Write(context.Background(), websocket.MessageText, []byte(`{"type":"response.output_text.delta","delta":"partial"}`))
	}()
	_, err := ra.handleWSPassthroughStream(context.Background(), &pooledConn{conn: clientConn})
	if err == nil || !closed || !ra.streamPayloadWritten.Load() {
		t.Fatalf("unknown partial write did not close/commit downstream: closed=%t written=%t err=%v", closed, ra.streamPayloadWritten.Load(), err)
	}
	if writeErr := <-writeDone; writeErr != nil {
		t.Fatalf("write upstream frame: %v", writeErr)
	}
}

func TestWSPassthroughApplicationErrorsDoNotBackoffTransport(t *testing.T) {
	applicationErr := &wsUpstreamEventError{Status: http.StatusTooManyRequests, Code: "rate_limit", Message: "slow down"}
	if shouldRecordWSPassthroughTransportFailure(&wsPassthroughStats{Error: applicationErr}, applicationErr) {
		t.Fatal("application-level response.failed must not count as a WS transport failure")
	}
	if !shouldRecordWSPassthroughTransportFailure(&wsPassthroughStats{}, errors.New("ws read error: connection reset by peer")) {
		t.Fatal("transport break must count as a WS transport failure")
	}
}

func TestWSPassthroughSyntheticTerminalWriteFailureMarksDownstreamBroken(t *testing.T) {
	closed := false
	writer := &passthroughScriptedWriter{n: 1, err: errors.New("terminal write failed"), closeFn: func() { closed = true }}
	ra := &relayAttempt{
		relayRequest: &relayRequest{streamWriter: writer},
		channel:      &model.Channel{ID: 61},
		usedKey:      model.ChannelKey{ID: 62},
	}
	stats := &wsPassthroughStats{Error: &wsUpstreamEventError{Code: "server_error", Message: "boom"}}
	drop := false
	ra.writeWSPassthroughSyntheticTerminal(context.Background(), writer, stats, &drop, ra.buildWSPassthroughFailureTerminal)
	if !drop || !stats.DownstreamBroken || !closed || !ra.streamPayloadWritten.Load() {
		t.Fatalf("terminal write failure not committed as downstream break: drop=%t stats=%+v closed=%t written=%t", drop, stats, closed, ra.streamPayloadWritten.Load())
	}
}

type wsPassthroughSequenceWriter struct {
	passthroughScriptedWriter
	failAt int
}

func (w *wsPassthroughSequenceWriter) Write(data []byte) (int, error) {
	w.writes++
	w.data = append(w.data[:0], data...)
	if w.writes == w.failAt {
		return 0, errors.New("synthetic terminal write failed")
	}
	w.written = true
	return len(data), nil
}

func TestWSPassthroughSyntheticTerminalFailureRetainsIncompleteEvidence(t *testing.T) {
	ctx := setupRelayTestDB(t)
	if err := op.SettingSetInt(model.SettingKeyCircuitBreakerThreshold, 1); err != nil {
		t.Fatalf("set circuit threshold: %v", err)
	}
	const (
		channelID = 7601
		keyID     = 7602
		modelName = "synthetic-terminal-incomplete-model"
	)
	balancer.ResetStateByChannel(channelID)
	outlierwindow.Clear(channelID)
	t.Cleanup(func() {
		balancer.ResetStateByChannel(channelID)
		outlierwindow.Clear(channelID)
	})

	clientConn, serverConn := newTestWSConnPair(t)
	t.Cleanup(func() {
		clientConn.CloseNow()
		serverConn.CloseNow()
	})
	closed := false
	writer := &wsPassthroughSequenceWriter{
		passthroughScriptedWriter: passthroughScriptedWriter{closeFn: func() { closed = true }},
		failAt:                    2,
	}
	ra := &relayAttempt{
		relayRequest: &relayRequest{streamWriter: writer},
		channel:      &model.Channel{ID: channelID},
		usedKey:      model.ChannelKey{ID: keyID},
	}
	writeDone := make(chan error, 1)
	go func() {
		if err := serverConn.Write(context.Background(), websocket.MessageText, []byte(`{"type":"response.output_text.delta","delta":"partial"}`)); err != nil {
			writeDone <- err
			return
		}
		writeDone <- serverConn.Close(websocket.StatusInternalError, "upstream failed")
	}()

	stats, err := ra.handleWSPassthroughStream(ctx, &pooledConn{conn: clientConn})
	if writeErr := <-writeDone; writeErr != nil {
		t.Fatalf("write/close upstream frame: %v", writeErr)
	}
	if stats == nil || !stats.DownstreamBroken || !ra.streamPayloadWritten.Load() || !closed {
		t.Fatalf("expected downstream commitment after synthetic terminal failure: stats=%+v written=%t closed=%t", stats, ra.streamPayloadWritten.Load(), closed)
	}
	if stats.TerminalOutcome != transformerModel.PassthroughTerminalOutcomeNone {
		t.Fatalf("failed synthetic terminal must not commit a public outcome: %s", stats.TerminalOutcome)
	}
	if !errors.Is(err, transformerModel.ErrIncompleteUpstreamStream) {
		t.Fatalf("upstream abnormal close evidence was lost with failed synthetic terminal: %v", err)
	}
	if writer.writes != 2 {
		t.Fatalf("expected one payload and one synthetic-terminal write, got %d", writer.writes)
	}

	joinedErr := errors.Join(errWSPassthroughDownstreamClosed, err)
	if !errors.Is(joinedErr, stream.ErrDownstreamWriteFailed) || !errors.Is(joinedErr, transformerModel.ErrIncompleteUpstreamStream) {
		t.Fatalf("joined attempt error lost one of its origins: %v", joinedErr)
	}
	if !shouldRecordWSPassthroughTransportFailure(stats, joinedErr) {
		t.Fatal("independent abnormal upstream WS close must remain transport-accountable")
	}
	wsUpstreamPool.recordWSFailureForRequest(ctx, channelID, joinedErr)
	result := attemptResult{Written: true, StatusCode: http.StatusBadGateway, Err: joinedErr, UpstreamErr: joinedErr}
	recordIncompleteUpstreamFailure(channelID, keyID, modelName, result)
	recordWrittenStructuredUpstreamFailure(channelID, keyID, modelName, false, result)
	if _, recorded := recordFinalAttemptFailure(channelID, keyID, modelName, false, false, result); recorded {
		t.Fatal("joined downstream/incomplete error must be owned by dedicated accounting")
	}
	if tripped, _ := balancer.IsTripped(channelID, keyID, modelName); !tripped {
		t.Fatal("incomplete upstream evidence must trip channel circuit health")
	}
	if health := outlierwindow.Evaluate(channelID, time.Now()); health.Samples != 1 || health.Failures != 1 {
		t.Fatalf("incomplete upstream evidence must be recorded exactly once: %+v", health)
	}
	wsUpstreamPool.healthMu.RLock()
	health, ok := wsUpstreamPool.health[channelID]
	wsUpstreamPool.healthMu.RUnlock()
	if !ok || health.consecutiveFailures != 1 {
		t.Fatalf("abnormal upstream WS close must enter transport backoff once: ok=%t health=%+v", ok, health)
	}
}

func TestForwardViaWSPassthroughNormalizesPayloadAndRecordsMetrics(t *testing.T) {
	gin.SetMode(gin.TestMode)
	ctx := setupRelayTestDB(t)
	if err := op.SettingSetString(model.SettingKeyResponsesWSEnabled, "true"); err != nil {
		t.Fatalf("SettingSetString responses ws enabled failed: %v", err)
	}
	if err := op.SettingSetString(model.SettingKeyResponsesWSDefaultMode, "passthrough"); err != nil {
		t.Fatalf("SettingSetString responses ws mode failed: %v", err)
	}

	payloadCh := make(chan map[string]json.RawMessage, 1)
	wsServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("OpenAI-Beta"); got != "responses_websockets=2026-02-06" {
			t.Errorf("expected OpenAI-Beta header, got %q", got)
		}
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close(websocket.StatusNormalClosure, "")
		_, data, err := conn.Read(r.Context())
		if err != nil {
			return
		}
		var payload map[string]json.RawMessage
		if err := json.Unmarshal(data, &payload); err != nil {
			t.Errorf("decode ws payload: %v", err)
			return
		}
		payloadCh <- payload
		frames := []string{
			`{"type":"response.created","response":{"id":"resp_passthrough","model":"upstream-model"}}`,
			`{"type":"response.output_text.delta","delta":"hello"}`,
			`{"type":"response.completed","response":{"id":"resp_passthrough","model":"upstream-model","status":"completed","usage":{"input_tokens":3,"output_tokens":2,"total_tokens":5}}}`,
		}
		for _, frame := range frames {
			if err := conn.Write(r.Context(), websocket.MessageText, []byte(frame)); err != nil {
				return
			}
		}
	}))
	defer wsServer.Close()

	channel := &model.Channel{
		Name:     "ws-passthrough",
		Type:     outbound.OutboundTypeOpenAIResponse,
		Enabled:  true,
		BaseUrls: []model.BaseUrl{{URL: wsServer.URL + "/v1"}},
		Model:    "upstream-model",
		Keys:     []model.ChannelKey{{Enabled: true, ChannelKey: "ws-key"}},
		WSMode:   model.ChannelWSModePassthrough,
	}
	if err := op.ChannelCreate(channel, ctx); err != nil {
		t.Fatalf("ChannelCreate failed: %v", err)
	}
	group := &model.Group{Name: "ws-passthrough-group", Mode: model.GroupModeFailover}
	if err := op.GroupCreate(group, ctx); err != nil {
		t.Fatalf("GroupCreate failed: %v", err)
	}
	if err := op.GroupItemAdd(&model.GroupItem{GroupID: group.ID, ChannelID: channel.ID, ModelName: "upstream-model"}, ctx); err != nil {
		t.Fatalf("GroupItemAdd failed: %v", err)
	}

	clientConn, serverConn := newTestWSConnPair(t)
	defer clientConn.Close(websocket.StatusNormalClosure, "")
	defer serverConn.Close(websocket.StatusNormalClosure, "")

	rawBody := []byte(`{"type":"response.create","model":"client-model","input":"hello","stream":false,"background":true}`)
	internalReq := &transformerModel.InternalLLMRequest{Model: "upstream-model", Stream: boolPtr(true), RawAPIFormat: transformerModel.APIFormatOpenAIResponse}
	req := &relayRequest{
		ctx:             context.Background(),
		inAdapter:       inbound.Get(inbound.InboundTypeOpenAIResponse),
		internalRequest: internalReq,
		metrics:         NewRelayMetrics(1, "client-model", nil, internalReq),
		apiKeyID:        1,
		requestModel:    "client-model",
		streamWriter:    NewWSStreamWriter(context.Background(), serverConn),
		rawBody:         rawBody,
	}
	ra := &relayAttempt{relayRequest: req, outAdapter: outbound.Get(channel.Type), channel: channel, usedKey: channel.Keys[0]}

	status, err := ra.forwardViaWS(context.Background())
	if err != nil || status != http.StatusOK {
		t.Fatalf("forwardViaWS passthrough failed: status=%d err=%v", status, err)
	}

	select {
	case payload := <-payloadCh:
		if got := string(payload["type"]); got != `"response.create"` {
			t.Fatalf("expected response.create envelope, got %s", payload["type"])
		}
		if got := string(payload["model"]); got != `"upstream-model"` {
			t.Fatalf("expected upstream model in payload, got %s", got)
		}
		if got := string(payload["stream"]); got != "true" {
			t.Fatalf("expected stream=true, got %s", got)
		}
		if _, ok := payload["background"]; ok {
			t.Fatalf("expected background to be removed: %#v", payload)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for upstream payload")
	}

	if req.metrics.Stats.InputToken != 3 || req.metrics.Stats.OutputToken != 2 {
		t.Fatalf("expected usage to be collected, got %+v", req.metrics.Stats)
	}
	if req.metrics.InternalResponse == nil || req.metrics.InternalResponse.ID != "resp_passthrough" {
		t.Fatalf("expected internal response metadata, got %+v", req.metrics.InternalResponse)
	}
	if req.metrics.UsedWS != true {
		t.Fatalf("expected ws transport to be recorded")
	}
	if req.metrics.WSExecMode == nil || *req.metrics.WSExecMode != model.RelayLogWSExecModePassthrough {
		t.Fatalf("expected passthrough execution mode, got %+v", req.metrics.WSExecMode)
	}
}

func TestWSPassthroughModelRewriteRecursive(t *testing.T) {
	ra := &relayAttempt{relayRequest: &relayRequest{requestModel: "client-model", internalRequest: &transformerModel.InternalLLMRequest{Model: "upstream-model"}}}
	raw := []byte(`{"type":"response.completed","model":"upstream-model","response":{"model":"upstream-model","output":[{"model":"upstream-model"}]}}`)
	rewritten := ra.rewriteWSPassthroughDownstreamModel(raw)
	if strings.Contains(string(rewritten), "upstream-model") {
		t.Fatalf("expected all model fields to be rewritten: %s", rewritten)
	}
	if count := strings.Count(string(rewritten), "client-model"); count != 3 {
		t.Fatalf("expected 3 rewritten model fields, got %d in %s", count, rewritten)
	}
}

func TestWSPassthroughWriterSkipsDoneAndForwardsUsage(t *testing.T) {
	clientConn, serverConn := newTestWSConnPair(t)
	defer clientConn.Close(websocket.StatusNormalClosure, "")
	defer serverConn.Close(websocket.StatusNormalClosure, "")

	writer := NewWSStreamWriter(context.Background(), serverConn)
	payload := strings.Join([]string{
		`data: {"type":"response.output_text.delta","delta":"hello"}`,
		"",
		`data: {"type":"response.completed","response":{"status":"completed"}}`,
		"",
		"data: [DONE]",
		"",
	}, "\n")
	if _, err := writer.Write([]byte(payload)); err != nil {
		t.Fatalf("writer.Write failed: %v", err)
	}

	readCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var events [][]byte
	for i := 0; i < 2; i++ {
		_, data, err := clientConn.Read(readCtx)
		if err != nil {
			t.Fatalf("read event %d failed: %v", i, err)
		}
		events = append(events, data)
	}
	if strings.Contains(string(events[0]), "[DONE]") || strings.Contains(string(events[1]), "[DONE]") {
		t.Fatalf("[DONE] must not be forwarded over responses websocket: %q %q", events[0], events[1])
	}
	if !strings.Contains(string(events[0]), "response.output_text.delta") || !strings.Contains(string(events[1]), "response.completed") {
		t.Fatalf("unexpected websocket events: %q %q", events[0], events[1])
	}
}

func TestWSPassthroughRewriteDoesNotTouchDifferentModel(t *testing.T) {
	ra := &relayAttempt{relayRequest: &relayRequest{requestModel: "client-model", internalRequest: &transformerModel.InternalLLMRequest{Model: "upstream-model"}}}
	raw := []byte(`{"type":"response.completed","response":{"model":"other-model"}}`)
	rewritten := ra.rewriteWSPassthroughDownstreamModel(raw)
	if string(rewritten) != string(raw) {
		t.Fatalf("unexpected rewrite for different model: %s", rewritten)
	}
}

func TestWSPassthroughRequestPayloadUsesExactRawBody(t *testing.T) {
	stream := false
	request := &transformerModel.InternalLLMRequest{Model: "upstream-model", Stream: &stream, RawRequest: []byte(`{"type":"response.create","model":"client-model","input":"hello","background":true,"metadata":{"keep":true}}`)}
	ra := &relayAttempt{relayRequest: &relayRequest{internalRequest: request, rawBody: request.RawRequest}}
	payload, err := ra.buildWSPassthroughRequestPayload()
	if err != nil {
		t.Fatalf("buildWSPassthroughRequestPayload failed: %v", err)
	}
	var decoded map[string]json.RawMessage
	if err := json.Unmarshal(payload, &decoded); err != nil {
		t.Fatalf("decode payload failed: %v", err)
	}
	if string(decoded["type"]) != `"response.create"` {
		t.Fatalf("expected response.create envelope: %s", payload)
	}
	if _, ok := decoded["background"]; ok {
		t.Fatalf("expected unsupported background field removed: %s", payload)
	}
	if string(decoded["stream"]) != "true" {
		t.Fatalf("expected stream true, got %s", decoded["stream"])
	}
	if string(decoded["model"]) != `"upstream-model"` {
		t.Fatalf("expected upstream model, got %s", decoded["model"])
	}
	if string(decoded["metadata"]) != `{"keep":true}` {
		t.Fatalf("expected raw metadata preserved, got %s", decoded["metadata"])
	}
}

func TestWSPassthroughPartialFailureWritesSingleTerminal(t *testing.T) {
	for _, tc := range []struct {
		name         string
		finish       func(*websocket.Conn) error
		wantTerminal string
	}{
		{
			name: "structured upstream error",
			finish: func(conn *websocket.Conn) error {
				return conn.Write(context.Background(), websocket.MessageText, []byte(`{"type":"error","status":400,"error":{"code":"bad_request","type":"invalid_request_error","message":"boom"}}`))
			},
			wantTerminal: "response.failed",
		},
		{
			name: "transport break",
			finish: func(conn *websocket.Conn) error {
				conn.CloseNow()
				return nil
			},
			wantTerminal: "response.failed",
		},
		{
			name: "clean eof without terminal",
			finish: func(conn *websocket.Conn) error {
				return conn.Close(websocket.StatusNormalClosure, "")
			},
			wantTerminal: "response.incomplete",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clientConn, serverConn := newTestWSConnPair(t)
			defer clientConn.Close(websocket.StatusNormalClosure, "")
			defer serverConn.Close(websocket.StatusNormalClosure, "")

			req := &relayRequest{
				ctx:          context.Background(),
				requestModel: "client-model",
				streamWriter: NewWSStreamWriter(context.Background(), serverConn),
				metrics:      NewRelayMetrics(1, "client-model", nil, &transformerModel.InternalLLMRequest{Model: "upstream-model"}),
			}
			ra := &relayAttempt{
				relayRequest: req,
				channel:      &model.Channel{ID: 1},
				usedKey:      model.ChannelKey{ID: 1},
			}
			upstreamClient, upstreamServer := newTestWSConnPair(t)
			defer upstreamClient.Close(websocket.StatusNormalClosure, "")
			defer upstreamServer.Close(websocket.StatusNormalClosure, "")
			pc := &pooledConn{id: "partial", conn: upstreamClient, poolKey: wsPoolKey{channelID: 1, keyID: 1}}

			done := make(chan error, 1)
			go func() {
				_, err := ra.handleWSPassthroughStream(context.Background(), pc)
				done <- err
			}()

			created := []byte(`{"type":"response.created","response":{"id":"resp_partial","model":"upstream-model","status":"in_progress"}}`)
			delta := []byte(`{"type":"response.output_text.delta","response_id":"resp_partial","delta":"hello"}`)
			if err := upstreamServer.Write(context.Background(), websocket.MessageText, created); err != nil {
				t.Fatalf("write created: %v", err)
			}
			if err := upstreamServer.Write(context.Background(), websocket.MessageText, delta); err != nil {
				t.Fatalf("write delta: %v", err)
			}
			if err := tc.finish(upstreamServer); err != nil {
				t.Fatalf("finish upstream: %v", err)
			}

			select {
			case err := <-done:
				if err == nil {
					t.Fatal("expected partial failure error")
				}
				if tc.wantTerminal == "response.incomplete" && !errors.Is(err, transformerModel.ErrIncompleteUpstreamStream) {
					t.Fatalf("expected incomplete stream error, got %v", err)
				}
				if tc.name == "structured upstream error" && errors.Is(err, transformerModel.ErrIncompleteUpstreamStream) {
					t.Fatalf("structured response.failed must remain a typed failure, got %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("timed out waiting for passthrough failure")
			}

			readCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			types := make([]string, 0, 3)
			for i := 0; i < 3; i++ {
				_, data, err := clientConn.Read(readCtx)
				if err != nil {
					t.Fatalf("read downstream event %d: %v (types=%v)", i, err, types)
				}
				var event struct {
					Type string `json:"type"`
				}
				if err := json.Unmarshal(data, &event); err != nil {
					t.Fatalf("decode downstream event %q: %v", data, err)
				}
				types = append(types, event.Type)
			}
			if types[0] != "response.created" || types[1] != "response.output_text.delta" || types[2] != tc.wantTerminal {
				t.Fatalf("expected two payload events and one synthetic terminal, got %v", types)
			}
			terminalCount := 0
			for _, eventType := range types {
				if eventType == "response.failed" || eventType == "response.incomplete" {
					terminalCount++
				}
				if eventType == "error" {
					t.Fatalf("raw upstream error must not be forwarded after payload: %v", types)
				}
			}
			if terminalCount != 1 {
				t.Fatalf("expected exactly one terminal event, got %v", types)
			}
		})
	}
}

func TestWSPassthroughDownstreamWriteFailureStopsWSRecovery(t *testing.T) {
	ctx := setupRelayTestDB(t)
	resetWSUpstreamPool()
	t.Cleanup(resetWSUpstreamPool)
	if err := op.SettingSetString(model.SettingKeyResponsesWSEnabled, "true"); err != nil {
		t.Fatalf("enable upstream Responses WS: %v", err)
	}
	if err := op.SettingSetString(model.SettingKeyResponsesWSDefaultMode, "passthrough"); err != nil {
		t.Fatalf("set upstream Responses WS mode: %v", err)
	}
	if err := op.SettingSetInt(model.SettingKeyCircuitBreakerThreshold, 1); err != nil {
		t.Fatalf("set circuit threshold: %v", err)
	}

	newUpstream := func(hits *atomic.Int32) *httptest.Server {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/v1/responses" {
				http.NotFound(w, r)
				return
			}
			conn, err := websocket.Accept(w, r, nil)
			if err != nil {
				return
			}
			defer conn.CloseNow()
			upstreamCtx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
			defer cancel()
			if _, _, err := conn.Read(upstreamCtx); err != nil {
				return
			}
			hits.Add(1)
			_ = conn.Write(upstreamCtx, websocket.MessageText, []byte(
				`{"type":"response.created","response":{"id":"resp_downstream_write","model":"upstream-model","status":"in_progress"}}`,
			))
		}))
		t.Cleanup(server.Close)
		return server
	}

	var firstHits, secondHits atomic.Int32
	firstUpstream := newUpstream(&firstHits)
	secondUpstream := newUpstream(&secondHits)
	newChannel := func(name, baseURL string) *model.Channel {
		return &model.Channel{
			Name: name, Type: outbound.OutboundTypeOpenAIResponse, Enabled: true,
			BaseUrls: []model.BaseUrl{{URL: baseURL + "/v1"}}, Model: "upstream-model",
			Keys:   []model.ChannelKey{{Enabled: true, ChannelKey: "ws-key"}},
			WSMode: model.ChannelWSModePassthrough,
		}
	}
	first := newChannel("ws-downstream-write-first", firstUpstream.URL)
	second := newChannel("ws-downstream-write-second", secondUpstream.URL)
	if err := op.ChannelCreate(first, ctx); err != nil {
		t.Fatalf("create first channel: %v", err)
	}
	if err := op.ChannelCreate(second, ctx); err != nil {
		t.Fatalf("create second channel: %v", err)
	}
	group := &model.Group{Name: "ws-downstream-write-group", Mode: model.GroupModeFailover, RetryEnabled: true, MaxRetries: 2}
	if err := op.GroupCreate(group, ctx); err != nil {
		t.Fatalf("create group: %v", err)
	}
	if err := op.GroupItemAdd(&model.GroupItem{GroupID: group.ID, ChannelID: first.ID, ModelName: "upstream-model", Priority: 1}, ctx); err != nil {
		t.Fatalf("add first group item: %v", err)
	}
	if err := op.GroupItemAdd(&model.GroupItem{GroupID: group.ID, ChannelID: second.ID, ModelName: "upstream-model", Priority: 2}, ctx); err != nil {
		t.Fatalf("add second group item: %v", err)
	}
	for _, channel := range []*model.Channel{first, second} {
		balancer.ResetStateByChannel(channel.ID)
		outlierwindow.Clear(channel.ID)
		defer balancer.ResetStateByChannel(channel.ID)
		defer outlierwindow.Clear(channel.ID)
	}

	downstreamClient, downstreamServer := newTestWSConnPair(t)
	t.Cleanup(func() {
		downstreamClient.CloseNow()
		downstreamServer.CloseNow()
	})
	runCtx, cancelRun := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelRun()
	rawBody := []byte(`{"model":"ws-downstream-write-group","input":"hello","stream":true}`)
	inAdapter := inbound.Get(inbound.InboundTypeOpenAIResponse)
	internalRequest, err := inAdapter.TransformRequest(runCtx, rawBody)
	if err != nil {
		t.Fatalf("parse internal request: %v", err)
	}
	req, loadedGroup, err := newWSRelayRequest(
		runCtx, downstreamServer, inAdapter, 1, internalRequest.Model,
		cloneInternalRequest(internalRequest), cloneInternalRequest(internalRequest), nil, rawBody,
	)
	if err != nil {
		t.Fatalf("create WS relay request: %v", err)
	}
	writer := &passthroughScriptedWriter{err: errors.New("synthetic zero-byte downstream failure")}
	req.streamWriter = writer

	result := runWSRelay(runCtx, req, loadedGroup)
	if !errors.Is(result.Err, stream.ErrDownstreamWriteFailed) || !result.Written || result.Success || result.ResetConversation {
		t.Fatalf("downstream write failure was not terminal: %+v", result)
	}
	if writer.Written() {
		t.Fatal("zero-byte writer unexpectedly reported visible payload")
	}
	if writer.writes != 1 {
		t.Fatalf("downstream write failure triggered %d stream writes, want one", writer.writes)
	}
	if got := firstHits.Load(); got != 1 {
		t.Fatalf("downstream failure retried or reconnected the first channel: hits=%d", got)
	}
	if got := secondHits.Load(); got != 0 {
		t.Fatalf("downstream failure reached failover channel: hits=%d", got)
	}
	if req.metrics.WSRecovery != nil && (*req.metrics.WSRecovery == model.RelayLogWSRecoveryReconnect || *req.metrics.WSRecovery == model.RelayLogWSRecoveryReplay) {
		t.Fatalf("downstream failure triggered WS recovery: %v", *req.metrics.WSRecovery)
	}

	finalized := finalizeWSRelay(runCtx, downstreamServer, req, result)
	if !finalized.Written {
		t.Fatal("finalization lost downstream commitment")
	}
	readCtx, cancelRead := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancelRead()
	if _, data, readErr := downstreamClient.Read(readCtx); readErr == nil {
		t.Fatalf("finalization wrote an extra downstream frame: %s", data)
	} else if !errors.Is(readErr, context.DeadlineExceeded) {
		t.Fatalf("expected no downstream error frame, got read error: %v", readErr)
	}

	wsUpstreamPool.healthMu.RLock()
	_, wsFailureRecorded := wsUpstreamPool.health[first.ID]
	wsUpstreamPool.healthMu.RUnlock()
	if wsFailureRecorded || wsUpstreamPool.ShouldSkipWS(first.ID) {
		t.Fatal("downstream failure entered WS transport backoff")
	}
	if tripped, _ := balancer.IsTripped(first.ID, first.Keys[0].ID, "upstream-model"); tripped {
		t.Fatal("downstream failure tripped the upstream circuit")
	}
	if stats := outlierwindow.Evaluate(first.ID, time.Now()); stats.Samples != 0 {
		t.Fatalf("downstream failure entered outlier health: %+v", stats)
	}
}

func newTestWSConnPair(t *testing.T) (*websocket.Conn, *websocket.Conn) {
	t.Helper()
	serverConnCh := make(chan *websocket.Conn, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		serverConnCh <- conn
		<-r.Context().Done()
		_ = conn.Close(websocket.StatusNormalClosure, "")
	}))
	t.Cleanup(server.Close)
	clientConn, _, err := websocket.Dial(context.Background(), "ws"+strings.TrimPrefix(server.URL, "http"), nil)
	if err != nil {
		t.Fatalf("dial test ws pair failed: %v", err)
	}
	select {
	case serverConn := <-serverConnCh:
		return clientConn, serverConn
	case <-time.After(5 * time.Second):
		clientConn.Close(websocket.StatusNormalClosure, "")
		t.Fatalf("timed out waiting for server side websocket")
		return nil, nil
	}
}
