package relay

import (
	"context"
	"encoding/json"
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
	"github.com/bestruirui/octopus/internal/transformer/outbound"
	"github.com/gin-gonic/gin"
)

func TestResponsesCompactUsesCandidateModelForUpstreamAndMetrics(t *testing.T) {
	gin.SetMode(gin.TestMode)
	ctx := setupRelayTestDB(t)

	var upstreamModel string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var payload map[string]any
		if err := json.Unmarshal(body, &payload); err != nil {
			t.Errorf("decode compact request: %v", err)
		}
		upstreamModel, _ = payload["model"].(string)
		if payload["metadata"] == nil {
			t.Errorf("compact request lost unknown metadata field: %s", body)
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Set-Cookie", "provider-secret=must-not-leak")
		_, _ = w.Write([]byte(`{"id":"compact-response","object":"response.compaction","created_at":1,"output":[],"usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}`))
	}))
	defer upstream.Close()

	channel := &model.Channel{
		Name:     "compact-mapped-channel",
		Type:     outbound.OutboundTypeOpenAIResponse,
		Enabled:  true,
		BaseUrls: []model.BaseUrl{{URL: upstream.URL + "/v1"}},
		Model:    "candidate-compact-model",
		Keys:     []model.ChannelKey{{Enabled: true, ChannelKey: "compact-mapped-key"}},
	}
	if err := op.ChannelCreate(channel, ctx); err != nil {
		t.Fatalf("ChannelCreate failed: %v", err)
	}
	group := &model.Group{Name: "compact-request-alias", Mode: model.GroupModeFailover}
	if err := op.GroupCreate(group, ctx); err != nil {
		t.Fatalf("GroupCreate failed: %v", err)
	}
	if err := op.GroupItemAdd(&model.GroupItem{
		GroupID: group.ID, ChannelID: channel.ID, ModelName: "candidate-compact-model", Priority: 1, Weight: 1,
	}, ctx); err != nil {
		t.Fatalf("GroupItemAdd failed: %v", err)
	}

	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Set("api_key_id", 401)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses/compact", strings.NewReader(
		`{"model":"compact-request-alias","input":"hello","metadata":{"keep":true}}`,
	))
	c.Request.Header.Set("Content-Type", "application/json")

	HandleResponsesCompact(c)

	if recorder.Code != http.StatusOK {
		t.Fatalf("compact request status = %d, body=%s", recorder.Code, recorder.Body.String())
	}
	if upstreamModel != "candidate-compact-model" {
		t.Fatalf("upstream model = %q, want candidate model", upstreamModel)
	}
	if recorder.Header().Get("Set-Cookie") != "" {
		t.Fatalf("upstream Set-Cookie leaked to compact client: %q", recorder.Header().Get("Set-Cookie"))
	}
	logs, err := op.RelayLogList(ctx, nil, nil, nil, 1, 10)
	if err != nil {
		t.Fatalf("RelayLogList failed: %v", err)
	}
	if len(logs) == 0 || logs[0].ActualModelName != "candidate-compact-model" {
		t.Fatalf("compact actual model log = %#v, want candidate model", logs)
	}
}

func TestResponsesCompactCountsEachRetryAndUsesCandidateHealthKey(t *testing.T) {
	gin.SetMode(gin.TestMode)
	ctx := setupRelayTestDB(t)
	if err := op.SettingSetInt(model.SettingKeyCircuitBreakerThreshold, 1); err != nil {
		t.Fatalf("set circuit threshold: %v", err)
	}

	var hits atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error":{"message":"provider failure"}}`))
	}))
	defer upstream.Close()

	channel := &model.Channel{
		Name:     "compact-retry-channel",
		Type:     outbound.OutboundTypeOpenAIResponse,
		Enabled:  true,
		BaseUrls: []model.BaseUrl{{URL: upstream.URL + "/v1"}},
		Model:    "candidate-retry-model",
		Keys:     []model.ChannelKey{{Enabled: true, ChannelKey: "compact-retry-key"}},
	}
	if err := op.ChannelCreate(channel, ctx); err != nil {
		t.Fatalf("ChannelCreate failed: %v", err)
	}
	group := &model.Group{
		Name: "compact-retry-alias", Mode: model.GroupModeFailover, RetryEnabled: true, MaxRetries: 2,
	}
	if err := op.GroupCreate(group, ctx); err != nil {
		t.Fatalf("GroupCreate failed: %v", err)
	}
	if err := op.GroupItemAdd(&model.GroupItem{
		GroupID: group.ID, ChannelID: channel.ID, ModelName: "candidate-retry-model", Priority: 1, Weight: 1,
	}, ctx); err != nil {
		t.Fatalf("GroupItemAdd failed: %v", err)
	}
	saved, err := op.ChannelGet(channel.ID, ctx)
	if err != nil || len(saved.Keys) != 1 {
		t.Fatalf("ChannelGet failed: %v", err)
	}
	keyID := saved.Keys[0].ID
	outlierwindow.Clear(channel.ID)
	t.Cleanup(func() {
		balancer.ResetStateByChannel(channel.ID)
		outlierwindow.Clear(channel.ID)
	})

	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Set("api_key_id", 402)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses/compact", strings.NewReader(
		`{"model":"compact-retry-alias","input":"hello"}`,
	))
	c.Request.Header.Set("Content-Type", "application/json")

	HandleResponsesCompact(c)

	if hits.Load() != 2 {
		t.Fatalf("upstream attempts = %d, want two retries", hits.Load())
	}
	if recorder.Code != http.StatusInternalServerError {
		t.Fatalf("compact failure status = %d, want %d", recorder.Code, http.StatusInternalServerError)
	}
	stats := op.StatsChannelGet(channel.ID)
	if stats.RequestFailed != 2 || stats.WaitTime < 0 {
		t.Fatalf("channel stats = %+v, want two failed attempts with wait time", stats)
	}
	if tripped, _ := balancer.IsTripped(channel.ID, keyID, "candidate-retry-model"); !tripped {
		t.Fatal("candidate model health key must be tripped after final failure")
	}
	if tripped, _ := balancer.IsTripped(channel.ID, keyID, "compact-retry-alias"); tripped {
		t.Fatal("request alias must not receive compact health accounting")
	}
	if sample := outlierwindow.Evaluate(channel.ID, time.Now()); sample.Samples != 1 {
		t.Fatalf("outlier samples = %+v, want one final channel failure", sample)
	}
}

func TestResponsesCompactDownstreamWriteFailureStopsRetryAndFailover(t *testing.T) {
	gin.SetMode(gin.TestMode)
	ctx := setupRelayTestDB(t)
	if err := op.SettingSetInt(model.SettingKeyCircuitBreakerThreshold, 1); err != nil {
		t.Fatalf("set circuit threshold: %v", err)
	}

	compactBody := `{"id":"compact-write","object":"response.compaction","created_at":1,"output":[],"usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}`
	var firstHits atomic.Int32
	firstUpstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		firstHits.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(compactBody))
	}))
	defer firstUpstream.Close()
	var secondHits atomic.Int32
	secondUpstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		secondHits.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(compactBody))
	}))
	defer secondUpstream.Close()

	first := &model.Channel{
		Name: "compact-write-first", Type: outbound.OutboundTypeOpenAIResponse, Enabled: true,
		BaseUrls: []model.BaseUrl{{URL: firstUpstream.URL + "/v1"}}, Model: "compact-upstream-model",
		Keys: []model.ChannelKey{{Enabled: true, ChannelKey: "compact-write-first-key"}},
	}
	second := &model.Channel{
		Name: "compact-write-second", Type: outbound.OutboundTypeOpenAIResponse, Enabled: true,
		BaseUrls: []model.BaseUrl{{URL: secondUpstream.URL + "/v1"}}, Model: "compact-upstream-model",
		Keys: []model.ChannelKey{{Enabled: true, ChannelKey: "compact-write-second-key"}},
	}
	if err := op.ChannelCreate(first, ctx); err != nil {
		t.Fatalf("create first channel: %v", err)
	}
	if err := op.ChannelCreate(second, ctx); err != nil {
		t.Fatalf("create second channel: %v", err)
	}
	group := &model.Group{Name: "compact-write-group", Mode: model.GroupModeFailover, RetryEnabled: true, MaxRetries: 2}
	if err := op.GroupCreate(group, ctx); err != nil {
		t.Fatalf("create group: %v", err)
	}
	if err := op.GroupItemAdd(&model.GroupItem{GroupID: group.ID, ChannelID: first.ID, ModelName: "compact-upstream-model", Priority: 1, Weight: 1}, ctx); err != nil {
		t.Fatalf("add first group item: %v", err)
	}
	if err := op.GroupItemAdd(&model.GroupItem{GroupID: group.ID, ChannelID: second.ID, ModelName: "compact-upstream-model", Priority: 2, Weight: 1}, ctx); err != nil {
		t.Fatalf("add second group item: %v", err)
	}
	outlierwindow.Clear(first.ID)
	t.Cleanup(func() {
		balancer.ResetStateByChannel(first.ID)
		outlierwindow.Clear(first.ID)
	})

	writer := &zeroWriteResponseWriter{err: errors.New("synthetic compact downstream writer failure")}
	c, _ := gin.CreateTestContext(writer)
	c.Set("api_key_id", 403)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses/compact", strings.NewReader(
		`{"model":"compact-write-group","input":"hello"}`,
	))
	c.Request.Header.Set("Content-Type", "application/json")
	HandleResponsesCompact(c)

	if got := firstHits.Load(); got != 1 {
		t.Fatalf("compact downstream failure retried first channel: hits=%d", got)
	}
	if got := secondHits.Load(); got != 0 {
		t.Fatalf("compact downstream failure reached fallback channel: hits=%d", got)
	}
	if writer.writes != 1 {
		t.Fatalf("compact downstream failure triggered %d writes, want one", writer.writes)
	}
	if tripped, _ := balancer.IsTripped(first.ID, first.Keys[0].ID, "compact-upstream-model"); tripped {
		t.Fatal("compact downstream failure tripped upstream circuit health")
	}
	if stats := outlierwindow.Evaluate(first.ID, time.Now()); stats.Samples != 0 {
		t.Fatalf("compact downstream failure entered outlier health: %+v", stats)
	}
	if stats := op.StatsChannelGet(first.ID); stats.RequestSuccess != 0 || stats.RequestFailed != 0 {
		t.Fatalf("compact downstream failure changed channel success/failure stats: %+v", stats)
	}
	if sticky := balancer.GetSticky(403, group.Name, time.Minute); sticky != nil {
		t.Fatalf("compact downstream failure established sticky routing: %#v", sticky)
	}
}

func TestBuildResponsesCompactRequestRewritesOnlyModel(t *testing.T) {
	channel := &model.Channel{
		BaseUrls: []model.BaseUrl{{URL: "https://example.invalid/v1"}},
	}
	req, err := buildResponsesCompactRequest(context.Background(), channel, "key", "mapped-model", []byte(
		`{"model":"request-model","input":"hello","metadata":{"keep":true}}`,
	))
	if err != nil {
		t.Fatalf("build compact request: %v", err)
	}
	body, err := io.ReadAll(req.Body)
	if err != nil {
		t.Fatalf("read compact request: %v", err)
	}
	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatalf("decode compact request: %v", err)
	}
	if payload["model"] != "mapped-model" || payload["metadata"] == nil || payload["input"] != "hello" {
		t.Fatalf("rewritten compact payload = %#v", payload)
	}
}
