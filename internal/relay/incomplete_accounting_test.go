package relay

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
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
	"github.com/gin-gonic/gin"
)

func TestRecordIncompleteUpstreamFailureAccountsWSHealth(t *testing.T) {
	setupRelayTestDB(t)
	if err := op.SettingSetInt(model.SettingKeyCircuitBreakerThreshold, 1); err != nil {
		t.Fatalf("set circuit threshold: %v", err)
	}

	const (
		channelID = 7001
		keyID     = 7002
		modelName = "incomplete-ws-health-model"
	)
	outlierwindow.Clear(channelID)
	t.Cleanup(func() {
		balancer.Reset()
		outlierwindow.Clear(channelID)
	})

	result := attemptResult{
		UpstreamErr: transformerModel.ErrIncompleteUpstreamStream,
		StatusCode:  http.StatusOK,
	}
	recordIncompleteUpstreamFailure(channelID, keyID, modelName, result)
	recordWrittenStructuredUpstreamFailure(channelID, keyID, modelName, false, result)
	if _, recorded := recordFinalAttemptFailure(channelID, keyID, modelName, false, false, result); recorded {
		t.Fatal("incomplete failure must be owned by the incomplete accounting path")
	}

	if tripped, _ := balancer.IsTripped(channelID, keyID, modelName); !tripped {
		t.Fatal("incomplete WS stream must count as a hard circuit failure")
	}
	stats := outlierwindow.Evaluate(channelID, time.Now())
	if stats.Samples != 1 || stats.Failures != 1 {
		t.Fatalf("incomplete WS stream must be recorded in outlier health, got %+v", stats)
	}
}

func TestRecordIncompleteUpstreamFailureSkipsClientCancellation(t *testing.T) {
	setupRelayTestDB(t)
	if err := op.SettingSetInt(model.SettingKeyCircuitBreakerThreshold, 1); err != nil {
		t.Fatalf("set circuit threshold: %v", err)
	}
	const channelID, keyID = 7101, 7102
	const modelName = "canceled-incomplete-model"
	balancer.ResetStateByChannel(channelID)
	outlierwindow.Clear(channelID)
	recordIncompleteUpstreamFailure(channelID, keyID, modelName, attemptResult{
		Canceled: true, Written: true,
		UpstreamErr: transformerModel.ErrIncompleteUpstreamStream,
		StatusCode:  http.StatusOK,
	})
	if tripped, _ := balancer.IsTripped(channelID, keyID, modelName); tripped {
		t.Fatal("client cancellation must not trip upstream circuit health")
	}
	if stats := outlierwindow.Evaluate(channelID, time.Now()); stats.Samples != 0 {
		t.Fatalf("client cancellation must not enter outlier health: %+v", stats)
	}
}

func TestConcurrentCancellationDoesNotMaskHTTPFailureHealth(t *testing.T) {
	setupRelayTestDB(t)
	if err := op.SettingSetInt(model.SettingKeyCircuitBreakerThreshold, 1); err != nil {
		t.Fatalf("set circuit threshold: %v", err)
	}
	const channelID, keyID = 7103, 7104
	const modelName = "concurrent-cancel-http-failure-model"
	balancer.ResetStateByChannel(channelID)
	outlierwindow.Clear(channelID)
	t.Cleanup(func() {
		balancer.ResetStateByChannel(channelID)
		outlierwindow.Clear(channelID)
	})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	upstreamErr := newUpstreamHTTPError(http.StatusInternalServerError, []byte(`{"error":{"type":"server_error"}}`))
	result := attemptResult{
		Canceled:    isClientCancellation(ctx, upstreamErr),
		Err:         fmt.Errorf("channel failed: %w", upstreamErr),
		UpstreamErr: upstreamErr,
		StatusCode:  http.StatusInternalServerError,
	}
	if result.Canceled {
		t.Fatal("typed HTTP failure must not be classified as client cancellation")
	}
	if _, recorded := recordFinalAttemptFailure(channelID, keyID, modelName, false, false, result); !recorded {
		t.Fatal("typed HTTP failure must enter final upstream health accounting")
	}
	if tripped, _ := balancer.IsTripped(channelID, keyID, modelName); !tripped {
		t.Fatal("typed HTTP failure must trip hard circuit health")
	}
	stats := outlierwindow.Evaluate(channelID, time.Now())
	if stats.Samples != 1 || stats.Failures != 1 {
		t.Fatalf("typed HTTP failure must produce exactly one outlier sample, got %+v", stats)
	}
}

func TestRecordWrittenStructuredUpstreamFailureAccountsHealthOnce(t *testing.T) {
	setupRelayTestDB(t)
	if err := op.SettingSetInt(model.SettingKeyCircuitBreakerThreshold, 1); err != nil {
		t.Fatalf("set circuit threshold: %v", err)
	}
	const channelID, keyID = 7201, 7202
	const modelName = "written-structured-model"
	balancer.ResetStateByChannel(channelID)
	outlierwindow.Clear(channelID)
	result := attemptResult{
		Written: true, StatusCode: http.StatusInternalServerError,
		UpstreamErr: &wsUpstreamEventError{Status: http.StatusInternalServerError, Code: "server_error", Message: "boom"},
	}
	recordIncompleteUpstreamFailure(channelID, keyID, modelName, result)
	recordWrittenStructuredUpstreamFailure(channelID, keyID, modelName, false, result)
	if tripped, _ := balancer.IsTripped(channelID, keyID, modelName); !tripped {
		t.Fatal("written structured upstream failure must count toward circuit health")
	}
	if stats := outlierwindow.Evaluate(channelID, time.Now()); stats.Samples != 1 || stats.Failures != 1 {
		t.Fatalf("written structured failure must be recorded exactly once: %+v", stats)
	}
}

// WS transform 路径的上游中断会产生
// errors.Join(读错误(携带结构化 WS error), ErrIncompleteUpstreamStream)。
// recordIncompleteUpstreamFailure 按硬失败记账后，
// recordWrittenStructuredUpstreamFailure 必须跳过，避免同一次最终尝试
// 被计入两次熔断/outlier。
func TestJoinedStructuredIncompleteErrorCountedExactlyOnce(t *testing.T) {
	setupRelayTestDB(t)
	if err := op.SettingSetInt(model.SettingKeyCircuitBreakerThreshold, 1); err != nil {
		t.Fatalf("set circuit threshold: %v", err)
	}
	const channelID, keyID = 7301, 7302
	const modelName = "joined-structured-incomplete-model"
	balancer.ResetStateByChannel(channelID)
	outlierwindow.Clear(channelID)
	t.Cleanup(func() {
		balancer.ResetStateByChannel(channelID)
		outlierwindow.Clear(channelID)
	})
	result := attemptResult{
		Written: true, StatusCode: http.StatusBadGateway,
		UpstreamErr: errors.Join(
			fmt.Errorf("stream read error: %w", &wsUpstreamEventError{Status: http.StatusBadGateway, Code: "server_error", Message: "boom"}),
			transformerModel.ErrIncompleteUpstreamStream,
		),
	}
	recordIncompleteUpstreamFailure(channelID, keyID, modelName, result)
	recordWrittenStructuredUpstreamFailure(channelID, keyID, modelName, false, result)
	if tripped, _ := balancer.IsTripped(channelID, keyID, modelName); !tripped {
		t.Fatal("joined structured+incomplete error must count toward circuit health")
	}
	if stats := outlierwindow.Evaluate(channelID, time.Now()); stats.Samples != 1 || stats.Failures != 1 {
		t.Fatalf("joined structured+incomplete error must be recorded exactly once: %+v", stats)
	}
}

func TestWrittenStructuredFailureWithoutTerminalIsNotCountedTwice(t *testing.T) {
	setupRelayTestDB(t)
	if err := op.SettingSetInt(model.SettingKeyCircuitBreakerThreshold, 3); err != nil {
		t.Fatalf("set circuit threshold: %v", err)
	}
	const channelID, keyID = 7401, 7402
	const modelName = "written-structured-no-terminal-model"
	balancer.ResetStateByChannel(channelID)
	outlierwindow.Clear(channelID)
	t.Cleanup(func() {
		balancer.ResetStateByChannel(channelID)
		outlierwindow.Clear(channelID)
	})
	result := attemptResult{
		Written: true, StatusCode: http.StatusBadGateway,
		UpstreamErr: &wsUpstreamEventError{Status: http.StatusBadGateway, Code: "server_error", Message: "fixture"},
	}

	recordIncompleteUpstreamFailure(channelID, keyID, modelName, result)
	recordWrittenStructuredUpstreamFailure(channelID, keyID, modelName, false, result)
	_, recorded := recordFinalAttemptFailure(channelID, keyID, modelName, false, false, result)
	if !recorded {
		t.Fatal("written structured failure should remain visible to route-learning callers")
	}
	if stats := outlierwindow.Evaluate(channelID, time.Now()); stats.Samples != 1 || stats.Failures != 1 {
		t.Fatalf("written structured failure was counted more than once: %+v", stats)
	}
}

func TestDownstreamWriteFailureIsHealthNeutralAcrossAccountingPaths(t *testing.T) {
	setupRelayTestDB(t)
	if err := op.SettingSetInt(model.SettingKeyCircuitBreakerThreshold, 1); err != nil {
		t.Fatalf("set circuit threshold: %v", err)
	}
	const channelID, keyID = 7501, 7502
	const modelName = "downstream-write-neutral-model"
	balancer.ResetStateByChannel(channelID)
	outlierwindow.Clear(channelID)
	t.Cleanup(func() {
		balancer.ResetStateByChannel(channelID)
		outlierwindow.Clear(channelID)
	})
	result := attemptResult{
		Written:     true,
		StatusCode:  http.StatusBadGateway,
		Err:         errors.Join(stream.ErrDownstreamWriteFailed, errors.New("client closed")),
		UpstreamErr: stream.ErrDownstreamWriteFailed,
	}
	recordIncompleteUpstreamFailure(channelID, keyID, modelName, result)
	recordWrittenStructuredUpstreamFailure(channelID, keyID, modelName, false, result)
	if _, recorded := recordFinalAttemptFailure(channelID, keyID, modelName, false, false, result); recorded {
		t.Fatal("pure downstream write failure must not remain an upstream health failure")
	}
	if tripped, _ := balancer.IsTripped(channelID, keyID, modelName); tripped {
		t.Fatal("pure downstream write failure must not trip the circuit")
	}
	if stats := outlierwindow.Evaluate(channelID, time.Now()); stats.Samples != 0 {
		t.Fatalf("pure downstream write failure entered outlier health: %+v", stats)
	}
}

func TestIncompleteEvidenceSurvivesDownstreamTerminalWriteFailure(t *testing.T) {
	setupRelayTestDB(t)
	if err := op.SettingSetInt(model.SettingKeyCircuitBreakerThreshold, 1); err != nil {
		t.Fatalf("set circuit threshold: %v", err)
	}
	const channelID, keyID = 7551, 7552
	const modelName = "incomplete-with-downstream-write-model"
	balancer.ResetStateByChannel(channelID)
	outlierwindow.Clear(channelID)
	t.Cleanup(func() {
		balancer.ResetStateByChannel(channelID)
		outlierwindow.Clear(channelID)
	})
	result := attemptResult{
		Written: true, StatusCode: http.StatusBadGateway,
		Err: errors.Join(stream.ErrDownstreamWriteFailed, errors.New("terminal delivery failed")),
		UpstreamErr: errors.Join(
			fmt.Errorf("upstream connection reset"),
			transformerModel.ErrIncompleteUpstreamStream,
			stream.ErrDownstreamWriteFailed,
		),
	}

	recordIncompleteUpstreamFailure(channelID, keyID, modelName, result)
	recordWrittenStructuredUpstreamFailure(channelID, keyID, modelName, false, result)
	if _, recorded := recordFinalAttemptFailure(channelID, keyID, modelName, false, false, result); recorded {
		t.Fatal("incomplete evidence must be owned by the incomplete accounting path")
	}
	if tripped, _ := balancer.IsTripped(channelID, keyID, modelName); !tripped {
		t.Fatal("incomplete upstream evidence must trip the circuit even when terminal delivery fails")
	}
	if stats := outlierwindow.Evaluate(channelID, time.Now()); stats.Samples != 1 || stats.Failures != 1 {
		t.Fatalf("incomplete+downstream error must be recorded exactly once: %+v", stats)
	}
}

func TestDownstreamWriteFailureCannotTriggerProtocolOrWSRecovery(t *testing.T) {
	writeErr := errors.Join(
		stream.ErrDownstreamWriteFailed,
		fmt.Errorf("ws stream ended before first event: route not found"),
	)
	request := &transformerModel.InternalLLMRequest{RawAPIFormat: transformerModel.APIFormatOpenAIResponse}
	channel := &model.Channel{Type: outbound.OutboundTypeOpenAIChat}

	if shouldTryProtocolFallbackForAttempt(request, channel, outbound.OutboundTypeOpenAIResponse, http.StatusNotFound, writeErr) {
		t.Fatal("downstream write failure must not trigger protocol fallback")
	}
	if shouldReconnectUpstreamWSBeforeReplay(writeErr) {
		t.Fatal("downstream write failure must not trigger WS replay reconnect")
	}
	if isContinuationTransportFailure(writeErr) {
		t.Fatal("downstream write failure must not reset a continuation")
	}
}

type zeroWriteResponseWriter struct {
	header        http.Header
	err           error
	wait          <-chan struct{}
	onSuccess     func()
	succeedBefore int
	writes        int
}

func (w *zeroWriteResponseWriter) Header() http.Header {
	if w.header == nil {
		w.header = make(http.Header)
	}
	return w.header
}

func (w *zeroWriteResponseWriter) Write(data []byte) (int, error) {
	w.writes++
	if w.writes <= w.succeedBefore {
		if w.onSuccess != nil {
			w.onSuccess()
		}
		return len(data), nil
	}
	if w.wait != nil {
		<-w.wait
	}
	return 0, w.err
}

func (*zeroWriteResponseWriter) WriteHeader(int) {}
func (*zeroWriteResponseWriter) Flush()          {}

func TestHTTPDownstreamWriteFailureStopsRetryAndFailover(t *testing.T) {
	gin.SetMode(gin.TestMode)
	ctx := setupRelayTestDB(t)
	if err := op.SettingSetInt(model.SettingKeyCircuitBreakerThreshold, 1); err != nil {
		t.Fatalf("set circuit threshold: %v", err)
	}

	streamBody := strings.Join([]string{
		`data: {"id":"downstream-write","object":"chat.completion.chunk","created":1,"model":"upstream-model","choices":[{"index":0,"delta":{"role":"assistant","content":"answer"}}]}`,
		"",
		`data: {"id":"downstream-write","object":"chat.completion.chunk","created":1,"model":"upstream-model","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}`,
		"",
		`data: [DONE]`,
		"",
	}, "\n")
	firstUpstream := newStreamingMockChannelServer(t, "/v1/chat/completions", streamBody)
	secondUpstream := newStreamingMockChannelServer(t, "/v1/chat/completions", streamBody)

	first := createChannel("downstream-write-first", outbound.OutboundTypeOpenAIChat, firstUpstream.url)
	second := createChannel("downstream-write-second", outbound.OutboundTypeOpenAIChat, secondUpstream.url)
	if err := op.ChannelCreate(first, ctx); err != nil {
		t.Fatalf("create first channel: %v", err)
	}
	if err := op.ChannelCreate(second, ctx); err != nil {
		t.Fatalf("create second channel: %v", err)
	}
	group := &model.Group{Name: "downstream-write-group", Mode: model.GroupModeFailover, RetryEnabled: true, MaxRetries: 2}
	if err := op.GroupCreate(group, ctx); err != nil {
		t.Fatalf("create group: %v", err)
	}
	if err := op.GroupItemAdd(&model.GroupItem{GroupID: group.ID, ChannelID: first.ID, ModelName: "upstream-model", Priority: 1}, ctx); err != nil {
		t.Fatalf("add first group item: %v", err)
	}
	if err := op.GroupItemAdd(&model.GroupItem{GroupID: group.ID, ChannelID: second.ID, ModelName: "upstream-model", Priority: 2}, ctx); err != nil {
		t.Fatalf("add second group item: %v", err)
	}
	outlierwindow.Clear(first.ID)

	writer := &zeroWriteResponseWriter{err: errors.New("synthetic downstream writer failure")}
	c, _ := gin.CreateTestContext(writer)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(
		`{"model":"downstream-write-group","messages":[{"role":"user","content":"hello"}],"stream":true}`,
	))
	c.Request.Header.Set("Content-Type", "application/json")
	Handler(inbound.InboundTypeOpenAIChat, c)

	if got := firstUpstream.hits.Load(); got != 1 {
		t.Fatalf("downstream failure retried the same channel: hits=%d", got)
	}
	if got := secondUpstream.hits.Load(); got != 0 {
		t.Fatalf("downstream failure reached failover channel: hits=%d", got)
	}
	if writer.writes != 1 {
		t.Fatalf("downstream failure triggered %d writes, want exactly one", writer.writes)
	}
	if tripped, _ := balancer.IsTripped(first.ID, first.Keys[0].ID, "upstream-model"); tripped {
		t.Fatal("downstream failure tripped the upstream circuit")
	}
	if stats := outlierwindow.Evaluate(first.ID, time.Now()); stats.Samples != 0 {
		t.Fatalf("downstream failure entered outlier health: %+v", stats)
	}
}

func TestFirstTokenTimeoutDoesNotMaskBlockedDownstreamWriteFailure(t *testing.T) {
	gin.SetMode(gin.TestMode)
	ctx := setupRelayTestDB(t)
	if err := op.SettingSetInt(model.SettingKeyCircuitBreakerThreshold, 1); err != nil {
		t.Fatalf("set circuit threshold: %v", err)
	}

	budgetExpired := make(chan struct{})
	serverStop := make(chan struct{})
	var signalOnce sync.Once
	var stopOnce sync.Once
	signalBudgetExpiry := func() {
		signalOnce.Do(func() { close(budgetExpired) })
	}
	stopServer := func() {
		stopOnce.Do(func() { close(serverStop) })
	}

	var firstHits atomic.Int32
	firstServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		firstHits.Add(1)
		if r.URL.Path != "/v1/chat/completions" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = fmt.Fprint(w, `data: {"id":"blocked-first-token","object":"chat.completion.chunk","created":1,"model":"upstream-model","choices":[{"index":0,"delta":{"role":"assistant","content":"answer"}}]}`+"\n\n")
		w.(http.Flusher).Flush()
		select {
		case <-r.Context().Done():
		case <-serverStop:
		}
		signalBudgetExpiry()
	}))
	t.Cleanup(func() {
		stopServer()
		firstServer.Close()
	})

	streamBody := strings.Join([]string{
		`data: {"id":"fallback","object":"chat.completion.chunk","created":1,"model":"upstream-model","choices":[{"index":0,"delta":{"role":"assistant","content":"fallback"}}]}`,
		"",
		`data: [DONE]`,
		"",
	}, "\n")
	secondUpstream := newStreamingMockChannelServer(t, "/v1/chat/completions", streamBody)

	first := createChannel("blocked-first-token-write", outbound.OutboundTypeOpenAIChat, firstServer.URL+"/v1")
	second := createChannel("blocked-first-token-fallback", outbound.OutboundTypeOpenAIChat, secondUpstream.url)
	if err := op.ChannelCreate(first, ctx); err != nil {
		t.Fatalf("create first channel: %v", err)
	}
	if err := op.ChannelCreate(second, ctx); err != nil {
		t.Fatalf("create second channel: %v", err)
	}
	group := &model.Group{
		Name: "blocked-first-token-write-group", Mode: model.GroupModeFailover,
		RetryEnabled: true, MaxRetries: 2, FirstTokenTimeOut: 1,
	}
	if err := op.GroupCreate(group, ctx); err != nil {
		t.Fatalf("create group: %v", err)
	}
	if err := op.GroupItemAdd(&model.GroupItem{GroupID: group.ID, ChannelID: first.ID, ModelName: "upstream-model", Priority: 1}, ctx); err != nil {
		t.Fatalf("add first group item: %v", err)
	}
	if err := op.GroupItemAdd(&model.GroupItem{GroupID: group.ID, ChannelID: second.ID, ModelName: "upstream-model", Priority: 2}, ctx); err != nil {
		t.Fatalf("add second group item: %v", err)
	}
	outlierwindow.Clear(first.ID)
	t.Cleanup(func() {
		balancer.ResetStateByChannel(first.ID)
		outlierwindow.Clear(first.ID)
	})

	writer := &zeroWriteResponseWriter{
		err:  errors.New("synthetic blocked downstream writer failure"),
		wait: budgetExpired,
	}
	c, _ := gin.CreateTestContext(writer)
	c.Set("api_key_id", 751)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(
		`{"model":"blocked-first-token-write-group","messages":[{"role":"user","content":"hello"}],"stream":true}`,
	))
	c.Request.Header.Set("Content-Type", "application/json")

	handlerDone := make(chan struct{})
	go func() {
		defer close(handlerDone)
		Handler(inbound.InboundTypeOpenAIChat, c)
	}()
	select {
	case <-handlerDone:
	case <-time.After(5 * time.Second):
		stopServer()
		select {
		case <-handlerDone:
		case <-time.After(2 * time.Second):
			t.Fatal("handler remained blocked after releasing the downstream writer")
		}
		t.Fatal("handler did not finish after the first-token budget expired")
	}

	if got := firstHits.Load(); got != 1 {
		t.Fatalf("blocked downstream write retried first channel: hits=%d", got)
	}
	if got := secondUpstream.hits.Load(); got != 0 {
		t.Fatalf("first-token timeout masked downstream failure and reached fallback: hits=%d", got)
	}
	if writer.writes != 1 {
		t.Fatalf("masked downstream failure triggered %d writes, want one", writer.writes)
	}
	if tripped, _ := balancer.IsTripped(first.ID, first.Keys[0].ID, "upstream-model"); tripped {
		t.Fatal("blocked downstream write tripped upstream circuit health")
	}
	if stats := outlierwindow.Evaluate(first.ID, time.Now()); stats.Samples != 0 {
		t.Fatalf("blocked downstream write entered outlier health: %+v", stats)
	}
	if stats := op.StatsChannelGet(first.ID); stats.RequestSuccess != 0 || stats.RequestFailed != 0 {
		t.Fatalf("blocked downstream write changed channel success/failure stats: %+v", stats)
	}
	if sticky := balancer.GetSticky(751, group.Name, time.Minute); sticky != nil {
		t.Fatalf("blocked downstream write established sticky routing: %#v", sticky)
	}
}

func TestHTTPNonStreamingDownstreamWriteFailureIsNotSuccess(t *testing.T) {
	gin.SetMode(gin.TestMode)
	ctx := setupRelayTestDB(t)
	if err := op.SettingSetInt(model.SettingKeyCircuitBreakerThreshold, 1); err != nil {
		t.Fatalf("set circuit threshold: %v", err)
	}

	responseBody := `{"id":"nonstream-write","object":"chat.completion","created":1,"model":"upstream-model","choices":[{"index":0,"message":{"role":"assistant","content":"answer"},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}`
	firstUpstream := newMockChannelServer(t, "/v1/chat/completions", http.StatusOK, responseBody)
	secondUpstream := newMockChannelServer(t, "/v1/chat/completions", http.StatusOK, responseBody)

	first := createChannel("nonstream-write-first", outbound.OutboundTypeOpenAIChat, firstUpstream.url)
	second := createChannel("nonstream-write-second", outbound.OutboundTypeOpenAIChat, secondUpstream.url)
	if err := op.ChannelCreate(first, ctx); err != nil {
		t.Fatalf("create first channel: %v", err)
	}
	if err := op.ChannelCreate(second, ctx); err != nil {
		t.Fatalf("create second channel: %v", err)
	}
	group := &model.Group{Name: "nonstream-write-group", Mode: model.GroupModeFailover, RetryEnabled: true, MaxRetries: 2}
	if err := op.GroupCreate(group, ctx); err != nil {
		t.Fatalf("create group: %v", err)
	}
	if err := op.GroupItemAdd(&model.GroupItem{GroupID: group.ID, ChannelID: first.ID, ModelName: "upstream-model", Priority: 1}, ctx); err != nil {
		t.Fatalf("add first group item: %v", err)
	}
	if err := op.GroupItemAdd(&model.GroupItem{GroupID: group.ID, ChannelID: second.ID, ModelName: "upstream-model", Priority: 2}, ctx); err != nil {
		t.Fatalf("add second group item: %v", err)
	}
	outlierwindow.Clear(first.ID)
	t.Cleanup(func() {
		balancer.ResetStateByChannel(first.ID)
		outlierwindow.Clear(first.ID)
	})

	writer := &zeroWriteResponseWriter{err: errors.New("synthetic nonstream downstream writer failure")}
	c, _ := gin.CreateTestContext(writer)
	c.Set("api_key_id", 752)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(
		`{"model":"nonstream-write-group","messages":[{"role":"user","content":"hello"}]}`,
	))
	c.Request.Header.Set("Content-Type", "application/json")
	Handler(inbound.InboundTypeOpenAIChat, c)

	if got := firstUpstream.hits.Load(); got != 1 {
		t.Fatalf("nonstream downstream failure retried first channel: hits=%d", got)
	}
	if got := secondUpstream.hits.Load(); got != 0 {
		t.Fatalf("nonstream downstream failure reached fallback channel: hits=%d", got)
	}
	if writer.writes != 1 {
		t.Fatalf("nonstream downstream failure triggered %d writes, want one", writer.writes)
	}
	if tripped, _ := balancer.IsTripped(first.ID, first.Keys[0].ID, "upstream-model"); tripped {
		t.Fatal("nonstream downstream failure tripped upstream circuit health")
	}
	if stats := outlierwindow.Evaluate(first.ID, time.Now()); stats.Samples != 0 {
		t.Fatalf("nonstream downstream failure entered outlier health: %+v", stats)
	}
	if stats := op.StatsChannelGet(first.ID); stats.RequestSuccess != 0 || stats.RequestFailed != 0 {
		t.Fatalf("nonstream downstream failure changed channel success/failure stats: %+v", stats)
	}
	if sticky := balancer.GetSticky(752, group.Name, time.Minute); sticky != nil {
		t.Fatalf("nonstream downstream failure established sticky routing: %#v", sticky)
	}
	saved, err := op.ChannelGet(first.ID, ctx)
	if err != nil || len(saved.Keys) != 1 {
		t.Fatalf("load first channel after attempt: %v", err)
	}
	if got := saved.Keys[0].StatusCode; got != http.StatusOK {
		t.Fatalf("selected key status = %d, want upstream status %d", got, http.StatusOK)
	}
}

func TestHTTPPassthroughDownstreamWriteFailurePreservesUpstreamStatus(t *testing.T) {
	gin.SetMode(gin.TestMode)
	ctx := setupRelayTestDB(t)
	if err := op.SettingSetInt(model.SettingKeyCircuitBreakerThreshold, 1); err != nil {
		t.Fatalf("set circuit threshold: %v", err)
	}

	// This body is intentionally not a transformable Responses object. The raw
	// passthrough path accepts it, so the test cannot silently exercise the
	// standard transformed-response path instead.
	responseBody := `{"opaque":"passthrough-only"}`
	firstUpstream := newMockChannelServer(t, "/v1/responses", http.StatusAccepted, responseBody)
	secondUpstream := newMockChannelServer(t, "/v1/responses", http.StatusOK, responseBody)
	first := createChannel("passthrough-write-first", outbound.OutboundTypeOpenAIResponse, firstUpstream.url)
	second := createChannel("passthrough-write-second", outbound.OutboundTypeOpenAIResponse, secondUpstream.url)
	if err := op.ChannelCreate(first, ctx); err != nil {
		t.Fatalf("create first channel: %v", err)
	}
	if err := op.ChannelCreate(second, ctx); err != nil {
		t.Fatalf("create second channel: %v", err)
	}
	group := &model.Group{Name: "passthrough-write-group", Mode: model.GroupModeFailover, RetryEnabled: true, MaxRetries: 2}
	if err := op.GroupCreate(group, ctx); err != nil {
		t.Fatalf("create group: %v", err)
	}
	if err := op.GroupItemAdd(&model.GroupItem{GroupID: group.ID, ChannelID: first.ID, ModelName: "upstream-model", Priority: 1}, ctx); err != nil {
		t.Fatalf("add first group item: %v", err)
	}
	if err := op.GroupItemAdd(&model.GroupItem{GroupID: group.ID, ChannelID: second.ID, ModelName: "upstream-model", Priority: 2}, ctx); err != nil {
		t.Fatalf("add second group item: %v", err)
	}
	outlierwindow.Clear(first.ID)
	t.Cleanup(func() {
		balancer.ResetStateByChannel(first.ID)
		outlierwindow.Clear(first.ID)
	})

	writer := &zeroWriteResponseWriter{err: errors.New("synthetic passthrough downstream writer failure")}
	c, _ := gin.CreateTestContext(writer)
	c.Set("api_key_id", 754)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(
		`{"model":"passthrough-write-group","input":"hello"}`,
	))
	c.Request.Header.Set("Content-Type", "application/json")
	Handler(inbound.InboundTypeOpenAIResponse, c)

	if got := firstUpstream.hits.Load(); got != 1 {
		t.Fatalf("passthrough downstream failure retried first channel: hits=%d", got)
	}
	if got := secondUpstream.hits.Load(); got != 0 {
		t.Fatalf("passthrough downstream failure reached fallback channel: hits=%d", got)
	}
	if writer.writes != 1 {
		t.Fatalf("passthrough downstream failure triggered %d writes, want one", writer.writes)
	}
	if tripped, _ := balancer.IsTripped(first.ID, first.Keys[0].ID, "upstream-model"); tripped {
		t.Fatal("passthrough downstream failure tripped upstream circuit health")
	}
	if stats := outlierwindow.Evaluate(first.ID, time.Now()); stats.Samples != 0 {
		t.Fatalf("passthrough downstream failure entered outlier health: %+v", stats)
	}
	if stats := op.StatsChannelGet(first.ID); stats.RequestSuccess != 0 || stats.RequestFailed != 0 {
		t.Fatalf("passthrough downstream failure changed channel success/failure stats: %+v", stats)
	}
	if sticky := balancer.GetSticky(754, group.Name, time.Minute); sticky != nil {
		t.Fatalf("passthrough downstream failure established sticky routing: %#v", sticky)
	}
	saved, err := op.ChannelGet(first.ID, ctx)
	if err != nil || len(saved.Keys) != 1 {
		t.Fatalf("load first channel after attempt: %v", err)
	}
	if got := saved.Keys[0].StatusCode; got != http.StatusAccepted {
		t.Fatalf("selected key status = %d, want upstream status %d", got, http.StatusAccepted)
	}
}

func TestHTTPPassthroughTerminalWriteFailurePreservesUpstreamEvidence(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, tc := range []struct {
		name     string
		terminal string
	}{
		{
			name:     "failed",
			terminal: `data: {"type":"response.failed","response":{"status":"failed","error":{"code":"server_error","message":"private provider detail"}}}`,
		},
		{
			name:     "incomplete",
			terminal: `data: {"type":"response.incomplete","response":{"status":"incomplete"}}`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := setupRelayTestDB(t)
			if err := op.SettingSetInt(model.SettingKeyCircuitBreakerThreshold, 1); err != nil {
				t.Fatalf("set circuit threshold: %v", err)
			}

			payloadDelivered := make(chan struct{})
			var payloadOnce sync.Once
			signalPayload := func() {
				payloadOnce.Do(func() { close(payloadDelivered) })
			}
			newUpstream := func() *mockChannelServer {
				s := &mockChannelServer{hits: &atomic.Int32{}}
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					s.hits.Add(1)
					if r.URL.Path != "/v1/responses" {
						http.NotFound(w, r)
						return
					}
					w.Header().Set("Content-Type", "text/event-stream")
					_, _ = fmt.Fprint(w, `data: {"type":"response.output_text.delta","delta":"partial"}`+"\n\n")
					w.(http.Flusher).Flush()
					select {
					case <-payloadDelivered:
					case <-r.Context().Done():
						return
					}
					_, _ = fmt.Fprint(w, tc.terminal+"\n\n")
					w.(http.Flusher).Flush()
				}))
				t.Cleanup(server.Close)
				s.url = server.URL + "/v1"
				return s
			}
			firstUpstream := newUpstream()
			secondUpstream := newUpstream()
			first := createChannel("terminal-write-first-"+tc.name, outbound.OutboundTypeOpenAIResponse, firstUpstream.url)
			second := createChannel("terminal-write-second-"+tc.name, outbound.OutboundTypeOpenAIResponse, secondUpstream.url)
			if err := op.ChannelCreate(first, ctx); err != nil {
				t.Fatalf("create first channel: %v", err)
			}
			if err := op.ChannelCreate(second, ctx); err != nil {
				t.Fatalf("create second channel: %v", err)
			}
			group := &model.Group{Name: "terminal-write-group-" + tc.name, Mode: model.GroupModeFailover, RetryEnabled: true, MaxRetries: 2}
			if err := op.GroupCreate(group, ctx); err != nil {
				t.Fatalf("create group: %v", err)
			}
			if err := op.GroupItemAdd(&model.GroupItem{GroupID: group.ID, ChannelID: first.ID, ModelName: "upstream-model", Priority: 1}, ctx); err != nil {
				t.Fatalf("add first group item: %v", err)
			}
			if err := op.GroupItemAdd(&model.GroupItem{GroupID: group.ID, ChannelID: second.ID, ModelName: "upstream-model", Priority: 2}, ctx); err != nil {
				t.Fatalf("add second group item: %v", err)
			}
			outlierwindow.Clear(first.ID)
			t.Cleanup(func() {
				balancer.ResetStateByChannel(first.ID)
				outlierwindow.Clear(first.ID)
			})

			writer := &zeroWriteResponseWriter{
				err:           errors.New("synthetic passthrough terminal writer failure"),
				onSuccess:     signalPayload,
				succeedBefore: 1,
			}
			c, _ := gin.CreateTestContext(writer)
			c.Set("api_key_id", 756)
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(
				`{"model":"`+group.Name+`","input":"hello","stream":true}`,
			))
			c.Request.Header.Set("Content-Type", "application/json")
			Handler(inbound.InboundTypeOpenAIResponse, c)

			if got := firstUpstream.hits.Load(); got != 1 {
				t.Fatalf("terminal write failure retried first channel: hits=%d", got)
			}
			if got := secondUpstream.hits.Load(); got != 0 {
				t.Fatalf("terminal write failure reached fallback channel: hits=%d", got)
			}
			if writer.writes != 2 {
				t.Fatalf("terminal write failure performed %d writes, want payload plus terminal", writer.writes)
			}
			if tripped, _ := balancer.IsTripped(first.ID, first.Keys[0].ID, "upstream-model"); !tripped {
				t.Fatal("typed upstream terminal did not trip circuit health")
			}
			if stats := outlierwindow.Evaluate(first.ID, time.Now()); stats.Samples != 1 || stats.Failures != 1 {
				t.Fatalf("typed terminal must enter outlier health exactly once: %+v", stats)
			}
			if stats := op.StatsChannelGet(first.ID); stats.RequestSuccess != 0 || stats.RequestFailed != 1 {
				t.Fatalf("typed terminal channel stats = %+v, want one upstream failure", stats)
			}
			if sticky := balancer.GetSticky(756, group.Name, time.Minute); sticky != nil {
				t.Fatalf("terminal write failure established sticky routing: %#v", sticky)
			}
			saved, err := op.ChannelGet(first.ID, ctx)
			if err != nil || len(saved.Keys) != 1 {
				t.Fatalf("load first channel after attempt: %v", err)
			}
			if got := saved.Keys[0].StatusCode; got != http.StatusOK {
				t.Fatalf("selected key status = %d, want upstream status %d", got, http.StatusOK)
			}
		})
	}
}

func TestIncompleteStreamTerminalWriteFailureCountsUpstreamOnce(t *testing.T) {
	gin.SetMode(gin.TestMode)
	ctx := setupRelayTestDB(t)
	if err := op.SettingSetInt(model.SettingKeyCircuitBreakerThreshold, 1); err != nil {
		t.Fatalf("set circuit threshold: %v", err)
	}

	incompleteBody := strings.Join([]string{
		`data: {"id":"incomplete-write","object":"chat.completion.chunk","created":1,"model":"upstream-model","choices":[{"index":0,"delta":{"role":"assistant","content":"partial"}}]}`,
		"",
	}, "\n")
	completeBody := strings.Join([]string{
		`data: {"id":"unused-fallback","object":"chat.completion.chunk","created":1,"model":"upstream-model","choices":[{"index":0,"delta":{"role":"assistant","content":"fallback"}}]}`,
		"",
		`data: [DONE]`,
		"",
	}, "\n")
	firstUpstream := newStreamingMockChannelServer(t, "/v1/chat/completions", incompleteBody)
	secondUpstream := newStreamingMockChannelServer(t, "/v1/chat/completions", completeBody)

	first := createChannel("incomplete-terminal-write-first", outbound.OutboundTypeOpenAIChat, firstUpstream.url)
	second := createChannel("incomplete-terminal-write-second", outbound.OutboundTypeOpenAIChat, secondUpstream.url)
	if err := op.ChannelCreate(first, ctx); err != nil {
		t.Fatalf("create first channel: %v", err)
	}
	if err := op.ChannelCreate(second, ctx); err != nil {
		t.Fatalf("create second channel: %v", err)
	}
	group := &model.Group{Name: "incomplete-terminal-write-group", Mode: model.GroupModeFailover, RetryEnabled: true, MaxRetries: 2}
	if err := op.GroupCreate(group, ctx); err != nil {
		t.Fatalf("create group: %v", err)
	}
	if err := op.GroupItemAdd(&model.GroupItem{GroupID: group.ID, ChannelID: first.ID, ModelName: "upstream-model", Priority: 1}, ctx); err != nil {
		t.Fatalf("add first group item: %v", err)
	}
	if err := op.GroupItemAdd(&model.GroupItem{GroupID: group.ID, ChannelID: second.ID, ModelName: "upstream-model", Priority: 2}, ctx); err != nil {
		t.Fatalf("add second group item: %v", err)
	}
	outlierwindow.Clear(first.ID)
	t.Cleanup(func() {
		balancer.ResetStateByChannel(first.ID)
		outlierwindow.Clear(first.ID)
	})

	writer := &zeroWriteResponseWriter{
		err:           errors.New("synthetic incomplete terminal writer failure"),
		succeedBefore: 1,
	}
	c, _ := gin.CreateTestContext(writer)
	c.Set("api_key_id", 753)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(
		`{"model":"incomplete-terminal-write-group","messages":[{"role":"user","content":"hello"}],"stream":true}`,
	))
	c.Request.Header.Set("Content-Type", "application/json")
	Handler(inbound.InboundTypeOpenAIChat, c)

	if got := firstUpstream.hits.Load(); got != 1 {
		t.Fatalf("incomplete stream retried first channel after terminal write failed: hits=%d", got)
	}
	if got := secondUpstream.hits.Load(); got != 0 {
		t.Fatalf("incomplete stream reached fallback after terminal write failed: hits=%d", got)
	}
	if writer.writes != 2 {
		t.Fatalf("incomplete stream performed %d downstream writes, want payload plus one terminal", writer.writes)
	}
	if tripped, _ := balancer.IsTripped(first.ID, first.Keys[0].ID, "upstream-model"); !tripped {
		t.Fatal("incomplete upstream evidence did not trip circuit health")
	}
	if stats := outlierwindow.Evaluate(first.ID, time.Now()); stats.Samples != 1 || stats.Failures != 1 {
		t.Fatalf("incomplete stream must enter outlier health exactly once: %+v", stats)
	}
	if stats := op.StatsChannelGet(first.ID); stats.RequestSuccess != 0 || stats.RequestFailed != 1 {
		t.Fatalf("incomplete stream channel stats = %+v, want one upstream failure", stats)
	}
	if sticky := balancer.GetSticky(753, group.Name, time.Minute); sticky != nil {
		t.Fatalf("incomplete stream established sticky routing: %#v", sticky)
	}
	saved, err := op.ChannelGet(first.ID, ctx)
	if err != nil || len(saved.Keys) != 1 {
		t.Fatalf("load first channel after attempt: %v", err)
	}
	if got := saved.Keys[0].StatusCode; got != http.StatusOK {
		t.Fatalf("selected key status = %d, want upstream status %d", got, http.StatusOK)
	}
}

func TestResetConversationSkipsWrittenStructuredHealthAccounting(t *testing.T) {
	setupRelayTestDB(t)
	if err := op.SettingSetInt(model.SettingKeyCircuitBreakerThreshold, 1); err != nil {
		t.Fatalf("set circuit threshold: %v", err)
	}
	const channelID, keyID = 7601, 7602
	const modelName = "reset-conversation-model"
	balancer.ResetStateByChannel(channelID)
	outlierwindow.Clear(channelID)
	t.Cleanup(func() {
		balancer.ResetStateByChannel(channelID)
		outlierwindow.Clear(channelID)
	})
	result := attemptResult{
		Written: true, ResetConversation: true, StatusCode: http.StatusConflict,
		Err: errors.Join(stream.ErrDownstreamWriteFailed, errors.New("client unavailable")),
		UpstreamErr: errors.Join(
			&wsUpstreamEventError{Status: http.StatusConflict, Code: "conversation_reset", Message: "fixture"},
			transformerModel.ErrIncompleteUpstreamStream,
			stream.ErrDownstreamWriteFailed,
		),
	}
	recordIncompleteUpstreamFailure(channelID, keyID, modelName, result)
	recordWrittenStructuredUpstreamFailure(channelID, keyID, modelName, false, result)
	if _, recorded := recordFinalAttemptFailure(channelID, keyID, modelName, false, false, result); recorded {
		t.Fatal("reset-conversation outcome must not be recorded as an upstream failure")
	}
	if tripped, _ := balancer.IsTripped(channelID, keyID, modelName); tripped {
		t.Fatal("reset-conversation outcome must not trip the circuit")
	}
	if stats := outlierwindow.Evaluate(channelID, time.Now()); stats.Samples != 0 {
		t.Fatalf("reset-conversation outcome entered outlier health: %+v", stats)
	}
}
