package relay

import (
	"context"
	"errors"
	"io"
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
	transformerModel "github.com/bestruirui/octopus/internal/transformer/model"
	"github.com/bestruirui/octopus/internal/transformer/outbound"
	"github.com/gin-gonic/gin"
)

func setupImagesAccountingChannel(t *testing.T, ctx context.Context, upstreamURL, groupName string) (*model.Channel, *model.ChannelKey) {
	t.Helper()
	channel := &model.Channel{
		Name:     groupName + "-channel",
		Type:     outbound.OutboundTypeOpenAIChat,
		Enabled:  true,
		BaseUrls: []model.BaseUrl{{URL: upstreamURL + "/v1"}},
		Model:    "images-model",
		Keys:     []model.ChannelKey{{Enabled: true, ChannelKey: "images-key"}},
	}
	if err := op.ChannelCreate(channel, ctx); err != nil {
		t.Fatalf("ChannelCreate failed: %v", err)
	}
	group := &model.Group{Name: groupName, Mode: model.GroupModeFailover}
	if err := op.GroupCreate(group, ctx); err != nil {
		t.Fatalf("GroupCreate failed: %v", err)
	}
	if err := op.GroupItemAdd(&model.GroupItem{GroupID: group.ID, ChannelID: channel.ID, ModelName: "images-model", Priority: 1, Weight: 1}, ctx); err != nil {
		t.Fatalf("GroupItemAdd failed: %v", err)
	}
	saved, err := op.ChannelGet(channel.ID, ctx)
	if err != nil || len(saved.Keys) == 0 {
		t.Fatalf("ChannelGet failed: %v", err)
	}
	return channel, &saved.Keys[0]
}

// 客户端在 SSE 流中途断开：不算上游失败，也不算上游成功。
func TestImagesClientCancellationNotUpstreamFailure(t *testing.T) {
	gin.SetMode(gin.TestMode)
	ctx := setupRelayTestDB(t)
	if err := op.SettingSetInt(model.SettingKeyCircuitBreakerThreshold, 1); err != nil {
		t.Fatalf("set circuit threshold: %v", err)
	}

	upstreamStop := make(chan struct{})
	upstreamHit := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// 读完请求体：触发服务端后台读，客户端断开后 r.Context() 才会取消
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("event: image_generation.partial_output\ndata: {\"type\":\"image_generation.partial_output\"}\n\n"))
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		close(upstreamHit)
		select {
		case <-r.Context().Done():
		case <-upstreamStop:
		}
	}))
	t.Cleanup(func() {
		close(upstreamStop)
		upstream.Close()
	})

	channel, key := setupImagesAccountingChannel(t, ctx, upstream.URL, "images-cancel-group")
	outlierwindow.Clear(channel.ID)
	t.Cleanup(func() {
		balancer.ResetStateByChannel(channel.ID)
		outlierwindow.Clear(channel.ID)
	})

	reqCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Set("api_key_id", 44)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/images/generations",
		strings.NewReader(`{"model":"images-cancel-group","stream":true}`)).WithContext(reqCtx)
	c.Request.Header.Set("Content-Type", "application/json")

	handlerDone := make(chan struct{})
	go func() {
		defer close(handlerDone)
		ImagesHandler("/images/generations", c)
	}()

	select {
	case <-upstreamHit:
	case <-time.After(5 * time.Second):
		t.Fatal("images request never reached upstream")
	}
	// 等待首个 SSE 事件写到下游（不能轮询 recorder，与 handler 并发访问会 data race）
	time.Sleep(200 * time.Millisecond)

	cancel()
	select {
	case <-handlerDone:
	case <-time.After(5 * time.Second):
		t.Fatal("images handler did not return after cancellation")
	}

	if tripped, _ := balancer.IsTripped(channel.ID, key.ID, "images-model"); tripped {
		t.Fatal("client cancellation must not trip upstream circuit health")
	}
	if stats := outlierwindow.Evaluate(channel.ID, time.Now()); stats.Samples != 0 {
		t.Fatalf("client cancellation must not enter outlier health: %+v", stats)
	}
}

// 真实上游失败（非 2xx）：计熔断硬失败 + outlier 失败样本。
func TestImagesRealUpstreamFailureAccountsHealth(t *testing.T) {
	gin.SetMode(gin.TestMode)
	ctx := setupRelayTestDB(t)
	if err := op.SettingSetInt(model.SettingKeyCircuitBreakerThreshold, 1); err != nil {
		t.Fatalf("set circuit threshold: %v", err)
	}

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "upstream exploded", http.StatusInternalServerError)
	}))
	defer upstream.Close()

	channel, key := setupImagesAccountingChannel(t, ctx, upstream.URL, "images-fail-group")
	outlierwindow.Clear(channel.ID)
	t.Cleanup(func() {
		balancer.ResetStateByChannel(channel.ID)
		outlierwindow.Clear(channel.ID)
	})

	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Set("api_key_id", 45)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/images/generations",
		strings.NewReader(`{"model":"images-fail-group"}`))
	c.Request.Header.Set("Content-Type", "application/json")

	ImagesHandler("/images/generations", c)

	if tripped, _ := balancer.IsTripped(channel.ID, key.ID, "images-model"); !tripped {
		t.Fatal("real upstream failure must count as a hard circuit failure")
	}
	stats := outlierwindow.Evaluate(channel.ID, time.Now())
	if stats.Samples != 1 || stats.Failures != 1 {
		t.Fatalf("real upstream failure must be recorded in outlier health, got %+v", stats)
	}
}

func TestImagesUsesHealthyAlternateKeyWhenPreferredKeyIsTripped(t *testing.T) {
	gin.SetMode(gin.TestMode)
	ctx := setupRelayTestDB(t)
	if err := op.SettingSetInt(model.SettingKeyCircuitBreakerThreshold, 1); err != nil {
		t.Fatalf("set circuit threshold: %v", err)
	}

	var upstreamHits atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamHits.Add(1)
		if got := r.Header.Get("Authorization"); got != "Bearer healthy-images-key" {
			http.Error(w, "unexpected credential", http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":[{"url":"https://example.invalid/image.png"}]}`))
	}))
	defer upstream.Close()

	const groupName = "images-alternate-key-group"
	channel := &model.Channel{
		Name:     groupName + "-channel",
		Type:     outbound.OutboundTypeOpenAIChat,
		Enabled:  true,
		BaseUrls: []model.BaseUrl{{URL: upstream.URL + "/v1"}},
		Model:    "images-model",
		Keys: []model.ChannelKey{
			{Enabled: true, ChannelKey: "tripped-images-key", TotalCost: 0},
			{Enabled: true, ChannelKey: "healthy-images-key", TotalCost: 1},
		},
	}
	if err := op.ChannelCreate(channel, ctx); err != nil {
		t.Fatalf("ChannelCreate failed: %v", err)
	}
	group := &model.Group{Name: groupName, Mode: model.GroupModeFailover}
	if err := op.GroupCreate(group, ctx); err != nil {
		t.Fatalf("GroupCreate failed: %v", err)
	}
	if err := op.GroupItemAdd(&model.GroupItem{GroupID: group.ID, ChannelID: channel.ID, ModelName: "images-model", Priority: 1, Weight: 1}, ctx); err != nil {
		t.Fatalf("GroupItemAdd failed: %v", err)
	}
	saved, err := op.ChannelGet(channel.ID, ctx)
	if err != nil || len(saved.Keys) != 2 {
		t.Fatalf("ChannelGet failed or returned wrong keys: err=%v keys=%d", err, len(saved.Keys))
	}
	var trippedKey, healthyKey model.ChannelKey
	for _, key := range saved.Keys {
		switch key.ChannelKey {
		case "tripped-images-key":
			trippedKey = key
		case "healthy-images-key":
			healthyKey = key
		}
	}
	if trippedKey.ID == 0 || healthyKey.ID == 0 {
		t.Fatalf("failed to identify persisted keys: tripped=%d healthy=%d", trippedKey.ID, healthyKey.ID)
	}

	balancer.ResetStateByChannel(channel.ID)
	outlierwindow.Clear(channel.ID)
	t.Cleanup(func() {
		balancer.ResetStateByChannel(channel.ID)
		outlierwindow.Clear(channel.ID)
	})
	balancer.RecordFailure(channel.ID, trippedKey.ID, "images-model", balancer.FailureHard)
	if tripped, _ := balancer.IsTripped(channel.ID, trippedKey.ID, "images-model"); !tripped {
		t.Fatal("test setup failed to trip the preferred key")
	}
	if tripped, _ := balancer.IsTripped(channel.ID, healthyKey.ID, "images-model"); tripped {
		t.Fatal("test setup unexpectedly tripped the alternate key")
	}

	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Set("api_key_id", 145)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/images/generations",
		strings.NewReader(`{"model":"images-alternate-key-group"}`))
	c.Request.Header.Set("Content-Type", "application/json")

	ImagesHandler("/images/generations", c)

	if recorder.Code != http.StatusOK {
		t.Fatalf("images status = %d, want 200; body=%s", recorder.Code, recorder.Body.String())
	}
	if got := upstreamHits.Load(); got != 1 {
		t.Fatalf("healthy alternate key request count = %d, want 1", got)
	}
	if tripped, _ := balancer.IsTripped(channel.ID, healthyKey.ID, "images-model"); tripped {
		t.Fatal("successful alternate key must remain healthy")
	}
}

// 下游写断开（客户端连接已坏）：不算上游失败，也不进 outlier。
type failingImagesResponseWriter struct {
	header   http.Header
	writeErr error
}

func (w *failingImagesResponseWriter) Header() http.Header {
	if w.header == nil {
		w.header = http.Header{}
	}
	return w.header
}

func (w *failingImagesResponseWriter) Write(p []byte) (int, error) { return 0, w.writeErr }

func (w *failingImagesResponseWriter) WriteHeader(int) {}

func TestImagesDownstreamWriteBreakNotUpstreamFailure(t *testing.T) {
	gin.SetMode(gin.TestMode)
	ctx := setupRelayTestDB(t)
	if err := op.SettingSetInt(model.SettingKeyCircuitBreakerThreshold, 1); err != nil {
		t.Fatalf("set circuit threshold: %v", err)
	}

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":[{"b64_json":"omitted"}]}`))
	}))
	defer upstream.Close()

	channel, key := setupImagesAccountingChannel(t, ctx, upstream.URL, "images-writebreak-group")
	outlierwindow.Clear(channel.ID)
	t.Cleanup(func() {
		balancer.ResetStateByChannel(channel.ID)
		outlierwindow.Clear(channel.ID)
	})

	c, _ := gin.CreateTestContext(&failingImagesResponseWriter{writeErr: errors.New("broken pipe")})
	c.Set("api_key_id", 46)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/images/generations",
		strings.NewReader(`{"model":"images-writebreak-group"}`))
	c.Request.Header.Set("Content-Type", "application/json")

	ImagesHandler("/images/generations", c)

	if tripped, _ := balancer.IsTripped(channel.ID, key.ID, "images-model"); tripped {
		t.Fatal("downstream write break must not trip upstream circuit health")
	}
	if stats := outlierwindow.Evaluate(channel.ID, time.Now()); stats.Samples != 0 {
		t.Fatalf("downstream write break must not enter outlier health: %+v", stats)
	}
}

func addImagesAccountingFallbackChannel(t *testing.T, ctx context.Context, upstreamURL, groupName, channelName string, priority int) (*model.Channel, *model.ChannelKey) {
	t.Helper()
	group, err := op.GroupGetEnabledMap(groupName, ctx)
	if err != nil {
		t.Fatalf("GroupGetEnabledMap failed: %v", err)
	}
	channel := &model.Channel{
		Name:     channelName,
		Type:     outbound.OutboundTypeOpenAIChat,
		Enabled:  true,
		BaseUrls: []model.BaseUrl{{URL: upstreamURL + "/v1"}},
		Model:    "images-model",
		Keys:     []model.ChannelKey{{Enabled: true, ChannelKey: "images-key"}},
	}
	if err := op.ChannelCreate(channel, ctx); err != nil {
		t.Fatalf("ChannelCreate failed: %v", err)
	}
	if err := op.GroupItemAdd(&model.GroupItem{GroupID: group.ID, ChannelID: channel.ID, ModelName: "images-model", Priority: priority, Weight: 1}, ctx); err != nil {
		t.Fatalf("GroupItemAdd failed: %v", err)
	}
	saved, err := op.ChannelGet(channel.ID, ctx)
	if err != nil || len(saved.Keys) == 0 {
		t.Fatalf("ChannelGet failed: %v", err)
	}
	return channel, &saved.Keys[0]
}

func TestProxySSERequiresExplicitImageTerminal(t *testing.T) {
	gin.SetMode(gin.TestMode)
	partial := "event: image_generation.partial_output\ndata: {\"type\":\"image_generation.partial_output\"}\n\n"
	completed := "event: image_generation.completed\ndata: {\"type\":\"image_generation.completed\",\"usage\":{\"input_tokens\":2,\"output_tokens\":3,\"total_tokens\":5}}\n\n"
	failed := "event: image_generation.failed\ndata: {\"type\":\"image_generation.failed\",\"error\":{\"message\":\"private provider detail\"}}\n\n"

	tests := []struct {
		name            string
		body            string
		wantWritten     bool
		wantSSEHeaders  bool
		wantEmpty       bool
		wantIncomplete  bool
		wantOutcome     transformerModel.PassthroughTerminalOutcome
		wantUsage       *imagesUsage
		forbiddenOutput string
	}{
		{name: "empty", wantEmpty: true},
		{name: "partial clean eof", body: partial, wantWritten: true, wantSSEHeaders: true, wantIncomplete: true},
		{name: "completed", body: partial + completed, wantWritten: true, wantSSEHeaders: true, wantUsage: &imagesUsage{InputTokens: 2, OutputTokens: 3, TotalTokens: 5}},
		{name: "failed before payload", body: failed, wantOutcome: transformerModel.PassthroughTerminalOutcomeFailed, forbiddenOutput: "private provider detail"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(recorder)
			upstream := &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
				Body:       io.NopCloser(strings.NewReader(tc.body)),
			}
			usage, written, err := proxySSE(context.Background(), c, upstream, 0, newImagesRelayMetrics(0, "fixture"), nil)
			for header, want := range map[string]string{
				"Content-Type":      "text/event-stream",
				"Cache-Control":     "no-cache",
				"Connection":        "keep-alive",
				"X-Accel-Buffering": "no",
			} {
				got := recorder.Header().Get(header)
				if tc.wantSSEHeaders && got != want {
					t.Fatalf("%s = %q, want %q", header, got, want)
				}
				if !tc.wantSSEHeaders && got != "" {
					t.Fatalf("suppressed/empty stream set tentative %s=%q", header, got)
				}
			}
			if written != tc.wantWritten {
				t.Fatalf("written = %t, want %t (err=%v body=%q)", written, tc.wantWritten, err, recorder.Body.String())
			}
			if tc.wantEmpty && !errors.Is(err, stream.ErrEmptyUpstreamStream) {
				t.Fatalf("empty stream error = %v, want ErrEmptyUpstreamStream", err)
			}
			if tc.wantIncomplete && !errors.Is(err, transformerModel.ErrIncompleteUpstreamStream) {
				t.Fatalf("partial EOF error = %v, want ErrIncompleteUpstreamStream", err)
			}
			if tc.wantOutcome != transformerModel.PassthroughTerminalOutcomeNone {
				var terminalErr *stream.PassthroughTerminalError
				if !errors.As(err, &terminalErr) || terminalErr.Outcome != tc.wantOutcome {
					t.Fatalf("terminal error = %v, want outcome %q", err, tc.wantOutcome)
				}
			}
			if tc.wantUsage != nil && (usage == nil || *usage != *tc.wantUsage) {
				t.Fatalf("usage = %#v, want %#v", usage, tc.wantUsage)
			}
			if tc.name == "completed" && err != nil {
				t.Fatalf("completed stream failed: %v", err)
			}
			if tc.forbiddenOutput != "" && strings.Contains(recorder.Body.String(), tc.forbiddenOutput) {
				t.Fatalf("suppressed failure exposed provider detail: %q", recorder.Body.String())
			}
		})
	}
}

func TestImagesEmptySSEFailsOverToCompletedStream(t *testing.T) {
	gin.SetMode(gin.TestMode)
	ctx := setupRelayTestDB(t)
	if err := op.SettingSetInt(model.SettingKeyCircuitBreakerThreshold, 1); err != nil {
		t.Fatalf("set circuit threshold: %v", err)
	}

	firstUpstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
	}))
	defer firstUpstream.Close()
	var fallbackHits atomic.Int32
	fallbackUpstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fallbackHits.Add(1)
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "event: image_generation.completed\ndata: {\"type\":\"image_generation.completed\",\"usage\":{\"input_tokens\":1,\"output_tokens\":2,\"total_tokens\":3}}\n\n")
	}))
	defer fallbackUpstream.Close()

	first, firstKey := setupImagesAccountingChannel(t, ctx, firstUpstream.URL, "images-empty-failover-group")
	fallback, _ := addImagesAccountingFallbackChannel(t, ctx, fallbackUpstream.URL, "images-empty-failover-group", "images-empty-fallback", 2)
	for _, channelID := range []int{first.ID, fallback.ID} {
		outlierwindow.Clear(channelID)
		t.Cleanup(func() {
			balancer.ResetStateByChannel(channelID)
			outlierwindow.Clear(channelID)
		})
	}

	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Set("api_key_id", 47)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/images/generations", strings.NewReader(`{"model":"images-empty-failover-group","stream":true}`))
	c.Request.Header.Set("Content-Type", "application/json")
	ImagesHandler("/images/generations", c)

	if fallbackHits.Load() != 1 || !strings.Contains(recorder.Body.String(), "image_generation.completed") {
		t.Fatalf("empty stream did not fail over to completed stream: hits=%d body=%q", fallbackHits.Load(), recorder.Body.String())
	}
	if tripped, _ := balancer.IsTripped(first.ID, firstKey.ID, "images-model"); !tripped {
		t.Fatal("empty upstream stream did not count as a hard failure")
	}
	if stats := outlierwindow.Evaluate(first.ID, time.Now()); stats.Samples != 1 || stats.Failures != 1 {
		t.Fatalf("empty stream upstream health = %+v, want one failure", stats)
	}
	if stats := outlierwindow.Evaluate(fallback.ID, time.Now()); stats.Samples != 1 || stats.Failures != 0 {
		t.Fatalf("completed fallback health = %+v, want one success", stats)
	}
	sticky := balancer.GetSticky(47, "images-empty-failover-group", time.Minute)
	if sticky == nil || sticky.ChannelID != fallback.ID {
		t.Fatalf("completed fallback did not establish sticky routing: %#v", sticky)
	}
}

func TestImagesPartialSSEEOFAccountsFailureWithoutFailover(t *testing.T) {
	gin.SetMode(gin.TestMode)
	ctx := setupRelayTestDB(t)
	if err := op.SettingSetInt(model.SettingKeyCircuitBreakerThreshold, 1); err != nil {
		t.Fatalf("set circuit threshold: %v", err)
	}

	partialUpstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "event: image_generation.partial_output\ndata: {\"type\":\"image_generation.partial_output\",\"b64_json\":\"partial\"}\n\n")
	}))
	defer partialUpstream.Close()
	var fallbackHits atomic.Int32
	fallbackUpstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fallbackHits.Add(1)
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "event: image_generation.completed\ndata: {\"type\":\"image_generation.completed\"}\n\n")
	}))
	defer fallbackUpstream.Close()

	first, firstKey := setupImagesAccountingChannel(t, ctx, partialUpstream.URL, "images-partial-eof-group")
	fallback, _ := addImagesAccountingFallbackChannel(t, ctx, fallbackUpstream.URL, "images-partial-eof-group", "images-partial-fallback", 2)
	for _, channelID := range []int{first.ID, fallback.ID} {
		outlierwindow.Clear(channelID)
		t.Cleanup(func() {
			balancer.ResetStateByChannel(channelID)
			outlierwindow.Clear(channelID)
		})
	}

	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Set("api_key_id", 48)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/images/generations", strings.NewReader(`{"model":"images-partial-eof-group","stream":true}`))
	c.Request.Header.Set("Content-Type", "application/json")
	ImagesHandler("/images/generations", c)

	if fallbackHits.Load() != 0 {
		t.Fatalf("partial visible stream failed over %d times", fallbackHits.Load())
	}
	body := recorder.Body.String()
	if !strings.Contains(body, "image_generation.partial_output") || strings.Contains(body, "image_generation.completed") {
		t.Fatalf("partial stream body = %q", body)
	}
	if tripped, _ := balancer.IsTripped(first.ID, firstKey.ID, "images-model"); !tripped {
		t.Fatal("partial EOF did not count as a hard upstream failure")
	}
	if stats := outlierwindow.Evaluate(first.ID, time.Now()); stats.Samples != 1 || stats.Failures != 1 {
		t.Fatalf("partial EOF upstream health = %+v, want exactly one failure", stats)
	}
	if stats := op.StatsChannelGet(first.ID); stats.RequestSuccess != 0 || stats.RequestFailed != 1 {
		t.Fatalf("partial EOF channel stats = %+v, want one failure", stats)
	}
	if sticky := balancer.GetSticky(48, "images-partial-eof-group", time.Minute); sticky != nil {
		t.Fatalf("partial EOF established sticky routing: %#v", sticky)
	}
}

func TestImagesAllFailedDoesNotReuseAttemptResponseHeaders(t *testing.T) {
	gin.SetMode(gin.TestMode)
	tests := []struct {
		name            string
		stream          bool
		upstreamContent string
	}{
		{name: "stream", stream: true, upstreamContent: "text/event-stream"},
		{name: "non-stream", upstreamContent: "image/png"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := setupRelayTestDB(t)
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", tc.upstreamContent)
				w.WriteHeader(http.StatusOK)
			}))
			t.Cleanup(upstream.Close)

			groupName := "images-empty-final-" + tc.name
			channel, _ := setupImagesAccountingChannel(t, ctx, upstream.URL, groupName)
			outlierwindow.Clear(channel.ID)
			t.Cleanup(func() {
				balancer.ResetStateByChannel(channel.ID)
				outlierwindow.Clear(channel.ID)
			})

			requestBody := `{"model":"` + groupName + `"}`
			if tc.stream {
				requestBody = `{"model":"` + groupName + `","stream":true}`
			}
			recorder := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(recorder)
			c.Set("api_key_id", 49)
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/images/generations", strings.NewReader(requestBody))
			c.Request.Header.Set("Content-Type", "application/json")
			ImagesHandler("/images/generations", c)

			if recorder.Code != http.StatusBadGateway {
				t.Fatalf("empty upstream status = %d, want 502", recorder.Code)
			}
			if got := recorder.Header().Get("Content-Type"); !strings.HasPrefix(got, "application/json") {
				t.Fatalf("final error Content-Type = %q, want application/json", got)
			}
			for _, header := range []string{"Cache-Control", "Connection", "X-Accel-Buffering"} {
				if got := recorder.Header().Get(header); got != "" {
					t.Fatalf("final JSON error retained tentative %s=%q", header, got)
				}
			}
		})
	}
}

func TestImagesAllFailedPreservesFinalUpstreamStatusAndRetryAfter(t *testing.T) {
	gin.SetMode(gin.TestMode)
	tests := []struct {
		name           string
		status         int
		retryAfter     string
		wantRetryAfter string
	}{
		{name: "bad request", status: http.StatusBadRequest, retryAfter: "7"},
		{name: "rate limited", status: http.StatusTooManyRequests, retryAfter: "120", wantRetryAfter: "60"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := setupRelayTestDB(t)
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/problem+json")
				w.Header().Set("Retry-After", tc.retryAfter)
				w.WriteHeader(tc.status)
				_, _ = io.WriteString(w, `{"error":{"message":"private upstream image failure"}}`)
			}))
			t.Cleanup(upstream.Close)

			groupName := "images-final-status-" + strings.ReplaceAll(tc.name, " ", "-")
			channel, _ := setupImagesAccountingChannel(t, ctx, upstream.URL, groupName)
			outlierwindow.Clear(channel.ID)
			t.Cleanup(func() {
				balancer.ResetStateByChannel(channel.ID)
				outlierwindow.Clear(channel.ID)
			})

			recorder := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(recorder)
			c.Set("api_key_id", 50)
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/images/generations", strings.NewReader(`{"model":"`+groupName+`"}`))
			c.Request.Header.Set("Content-Type", "application/json")
			ImagesHandler("/images/generations", c)

			if recorder.Code != tc.status {
				t.Fatalf("final status = %d, want %d", recorder.Code, tc.status)
			}
			if got := recorder.Header().Get("Retry-After"); got != tc.wantRetryAfter {
				t.Fatalf("Retry-After = %q, want %q", got, tc.wantRetryAfter)
			}
			if got := recorder.Header().Get("Content-Type"); !strings.HasPrefix(got, "application/json") {
				t.Fatalf("final error Content-Type = %q, want application/json", got)
			}
			if strings.Contains(recorder.Body.String(), "private upstream image failure") {
				t.Fatalf("final error exposed upstream body: %q", recorder.Body.String())
			}
		})
	}
}

func TestImagesCompletedUsageSurvivesDownstreamWriteFailure(t *testing.T) {
	gin.SetMode(gin.TestMode)
	ctx := setupRelayTestDB(t)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "event: image_generation.completed\ndata: {\"type\":\"image_generation.completed\",\"usage\":{\"input_tokens\":2,\"output_tokens\":3,\"total_tokens\":5}}\n\n")
	}))
	t.Cleanup(upstream.Close)

	channel, key := setupImagesAccountingChannel(t, ctx, upstream.URL, "images-usage-write-failure-group")
	outlierwindow.Clear(channel.ID)
	t.Cleanup(func() {
		balancer.ResetStateByChannel(channel.ID)
		outlierwindow.Clear(channel.ID)
	})

	c, _ := gin.CreateTestContext(&failingImagesResponseWriter{writeErr: errors.New("synthetic downstream write failure")})
	c.Set("api_key_id", 51)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/images/generations", strings.NewReader(`{"model":"images-usage-write-failure-group","stream":true}`))
	c.Request.Header.Set("Content-Type", "application/json")
	ImagesHandler("/images/generations", c)

	if tripped, _ := balancer.IsTripped(channel.ID, key.ID, "images-model"); tripped {
		t.Fatal("downstream write failure with usage tripped upstream health")
	}
	if stats := outlierwindow.Evaluate(channel.ID, time.Now()); stats.Samples != 0 {
		t.Fatalf("downstream write failure entered outlier health: %+v", stats)
	}
	stats := op.StatsChannelGet(channel.ID)
	if stats.InputToken != 2 || stats.OutputToken != 3 || stats.RequestFailed != 0 {
		t.Fatalf("channel usage after downstream failure = %+v, want input=2 output=3 and health-neutral request count", stats)
	}
	logs, err := op.RelayLogList(ctx, nil, nil, nil, 1, 10)
	if err != nil || len(logs) == 0 {
		t.Fatalf("load relay log: logs=%d err=%v", len(logs), err)
	}
	if logs[0].InputTokens != 2 || logs[0].OutputTokens != 3 || logs[0].Success {
		t.Fatalf("relay log usage after downstream failure = %+v", logs[0])
	}
}

func TestUsageScannerAcceptsWhitespaceAcrossChunks(t *testing.T) {
	scanner := newUsageScanner()
	for _, chunk := range []string{
		`{"data":[{"b64_json":"omitted"}],"us`,
		"age\" \n\t",
		": \r\n {\"input_tokens\":2,",
		`"output_tokens":3,"total_tokens":5}}`,
	} {
		scanner.Feed([]byte(chunk))
	}
	usage := scanner.Usage()
	if usage == nil || usage.InputTokens != 2 || usage.OutputTokens != 3 || usage.TotalTokens != 5 {
		t.Fatalf("usage with JSON whitespace/split chunks = %#v", usage)
	}
}

func TestImagesAttemptUsageResetClearsPreviousValues(t *testing.T) {
	metrics := newImagesRelayMetrics(0, "fixture")
	metrics.SetUsageFromImages("first-model", imagesUsage{InputTokens: 2, OutputTokens: 3})
	metrics.Stats.InputCost = 12
	metrics.Stats.OutputCost = 34
	metrics.ResponseContent = "stale response"

	metrics.resetAttemptUsage("second-model")
	if metrics.ActualModel != "second-model" || metrics.Stats.InputToken != 0 || metrics.Stats.OutputToken != 0 ||
		metrics.Stats.InputCost != 0 || metrics.Stats.OutputCost != 0 || metrics.ResponseContent != "" {
		t.Fatalf("stale image attempt usage survived reset: model=%q stats=%+v response=%q", metrics.ActualModel, metrics.Stats, metrics.ResponseContent)
	}
}

type shortImagesResponseWriter struct {
	header http.Header
}

func (w *shortImagesResponseWriter) Header() http.Header {
	if w.header == nil {
		w.header = http.Header{}
	}
	return w.header
}

func (w *shortImagesResponseWriter) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	return len(p) - 1, nil
}

func (w *shortImagesResponseWriter) WriteHeader(int) {}

func TestProxyNonStreamPreservesShortWriteEvidenceAndUsage(t *testing.T) {
	writer := &shortImagesResponseWriter{}
	c, _ := gin.CreateTestContext(writer)
	upstream := &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(`{"usage":{"input_tokens":2,"output_tokens":3,"total_tokens":5}}`)),
	}

	usage, written, err := proxyNonStream(c, upstream)
	if !written {
		t.Fatal("partial downstream write was not reported as visible")
	}
	if !errors.Is(err, stream.ErrDownstreamWriteFailed) || !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("short write error = %v, want downstream sentinel and io.ErrShortWrite", err)
	}
	if usage == nil || usage.InputTokens != 2 || usage.OutputTokens != 3 || usage.TotalTokens != 5 {
		t.Fatalf("usage after short write = %#v", usage)
	}
}

func TestImagesFinalErrorUsesSSEAfterEarlyHeartbeatCommit(t *testing.T) {
	gin.SetMode(gin.TestMode)
	ctx := setupRelayTestDB(t)
	setHeartbeatSettings(t, "1", "1")
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(1200 * time.Millisecond)
		w.Header().Set("Content-Type", "application/problem+json")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = io.WriteString(w, `{"error":{"message":"private delayed upstream failure"}}`)
	}))
	t.Cleanup(upstream.Close)

	channel, _ := setupImagesAccountingChannel(t, ctx, upstream.URL, "images-heartbeat-final-error-group")
	outlierwindow.Clear(channel.ID)
	t.Cleanup(func() {
		balancer.ResetStateByChannel(channel.ID)
		outlierwindow.Clear(channel.ID)
	})

	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Set("api_key_id", 52)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/images/generations", strings.NewReader(`{"model":"images-heartbeat-final-error-group","stream":true}`))
	c.Request.Header.Set("Content-Type", "application/json")
	ImagesHandler("/images/generations", c)

	if recorder.Code != http.StatusOK {
		t.Fatalf("committed SSE status = %d, want 200", recorder.Code)
	}
	if got := recorder.Header().Get("Content-Type"); got != "text/event-stream" {
		t.Fatalf("committed SSE Content-Type = %q", got)
	}
	body := recorder.Body.String()
	if !strings.Contains(body, ":\n\n") || strings.Count(body, "event: error") != 1 || !strings.Contains(body, `"code":429`) {
		t.Fatalf("committed SSE final error framing = %q", body)
	}
	if strings.Contains(body, "private delayed upstream failure") || strings.HasPrefix(strings.TrimSpace(body), "{") {
		t.Fatalf("committed SSE response leaked/mixed JSON error: %q", body)
	}
}

func TestImagesRestartsHeartbeatAcrossEmptyStreamFailover(t *testing.T) {
	gin.SetMode(gin.TestMode)
	ctx := setupRelayTestDB(t)
	setHeartbeatSettings(t, "1", "1")
	emptyUpstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(emptyUpstream.Close)
	var fallbackHits atomic.Int32
	slowUpstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fallbackHits.Add(1)
		time.Sleep(1200 * time.Millisecond)
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "event: image_generation.completed\ndata: {\"type\":\"image_generation.completed\"}\n\n")
	}))
	t.Cleanup(slowUpstream.Close)

	first, _ := setupImagesAccountingChannel(t, ctx, emptyUpstream.URL, "images-heartbeat-failover-group")
	fallback, _ := addImagesAccountingFallbackChannel(t, ctx, slowUpstream.URL, "images-heartbeat-failover-group", "images-heartbeat-failover-second", 2)
	for _, channelID := range []int{first.ID, fallback.ID} {
		outlierwindow.Clear(channelID)
		t.Cleanup(func() {
			balancer.ResetStateByChannel(channelID)
			outlierwindow.Clear(channelID)
		})
	}

	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Set("api_key_id", 53)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/images/generations", strings.NewReader(`{"model":"images-heartbeat-failover-group","stream":true}`))
	c.Request.Header.Set("Content-Type", "application/json")
	ImagesHandler("/images/generations", c)

	if fallbackHits.Load() != 1 {
		t.Fatalf("fallback attempts = %d, want 1", fallbackHits.Load())
	}
	body := recorder.Body.String()
	if !strings.Contains(body, ":\n\n") || !strings.Contains(body, "image_generation.completed") {
		t.Fatalf("heartbeat was not resumed across failover: %q", body)
	}
	if strings.Contains(body, "event: error") {
		t.Fatalf("successful fallback appended an error frame: %q", body)
	}
}
