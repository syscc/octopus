package relay

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bestruirui/octopus/internal/model"
	"github.com/bestruirui/octopus/internal/op"
	"github.com/bestruirui/octopus/internal/relay/balancer"
	"github.com/bestruirui/octopus/internal/transformer/inbound"
	"github.com/bestruirui/octopus/internal/transformer/outbound"
	"github.com/gin-gonic/gin"
)

func TestRelayRestartsHeartbeatAcrossEmptyStreamFailover(t *testing.T) {
	gin.SetMode(gin.TestMode)
	ctx := setupRelayTestDB(t)
	setHeartbeatSettings(t, "1", "1")

	var firstHits atomic.Int32
	firstUpstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		firstHits.Add(1)
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(firstUpstream.Close)

	var secondHits atomic.Int32
	secondUpstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		secondHits.Add(1)
		time.Sleep(1500 * time.Millisecond)
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = fmt.Fprint(w, strings.Join([]string{
			`data: {"id":"heartbeat-fallback","object":"chat.completion.chunk","created":1,"model":"fb-model","choices":[{"index":0,"delta":{"role":"assistant","content":"ok"}}]}`,
			"",
			`data: {"id":"heartbeat-fallback","object":"chat.completion.chunk","created":1,"model":"fb-model","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}`,
			"",
			`data: [DONE]`,
			"",
		}, "\n"))
	}))
	t.Cleanup(secondUpstream.Close)

	first := createChannel("ordinary-heartbeat-empty-first", outbound.OutboundTypeOpenAIChat, firstUpstream.URL+"/v1")
	second := createChannel("ordinary-heartbeat-empty-second", outbound.OutboundTypeOpenAIChat, secondUpstream.URL+"/v1")
	if err := op.ChannelCreate(first, ctx); err != nil {
		t.Fatalf("create first channel: %v", err)
	}
	if err := op.ChannelCreate(second, ctx); err != nil {
		t.Fatalf("create second channel: %v", err)
	}
	for _, channelID := range []int{first.ID, second.ID} {
		balancer.ResetStateByChannel(channelID)
		t.Cleanup(func() { balancer.ResetStateByChannel(channelID) })
	}

	group := &model.Group{Name: "ordinary-heartbeat-empty-failover-group", Mode: model.GroupModeFailover}
	if err := op.GroupCreate(group, ctx); err != nil {
		t.Fatalf("create group: %v", err)
	}
	if err := op.GroupItemAdd(&model.GroupItem{GroupID: group.ID, ChannelID: first.ID, ModelName: "fb-model", Priority: 1}, ctx); err != nil {
		t.Fatalf("add first group item: %v", err)
	}
	if err := op.GroupItemAdd(&model.GroupItem{GroupID: group.ID, ChannelID: second.ID, ModelName: "fb-model", Priority: 2}, ctx); err != nil {
		t.Fatalf("add second group item: %v", err)
	}

	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Set("api_key_id", 805)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"ordinary-heartbeat-empty-failover-group","messages":[{"role":"user","content":"hello"}],"stream":true}`))
	c.Request.Header.Set("Content-Type", "application/json")
	Handler(inbound.InboundTypeOpenAIChat, c)

	if firstHits.Load() != 1 || secondHits.Load() != 1 {
		t.Fatalf("expected one empty attempt and one fallback attempt, got first=%d second=%d", firstHits.Load(), secondHits.Load())
	}
	body := recorder.Body.String()
	if !strings.Contains(body, "ok") {
		t.Fatalf("fallback response was not delivered: %q", body)
	}
	if !strings.Contains(body, ":\n\n") {
		t.Fatalf("expected an early heartbeat while fallback headers were delayed: %q", body)
	}
	if strings.Contains(body, "event: error") {
		t.Fatalf("successful fallback appended an error event: %q", body)
	}
}

func TestRelayEmptyStreamFailoverKeepsCommittedSSEErrorContentType(t *testing.T) {
	gin.SetMode(gin.TestMode)
	ctx := setupRelayTestDB(t)
	setHeartbeatSettings(t, "1", "1")

	firstUpstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(firstUpstream.Close)

	secondUpstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(1500 * time.Millisecond)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = fmt.Fprint(w, `{"error":"temporarily unavailable"}`)
	}))
	t.Cleanup(secondUpstream.Close)

	first := createChannel("ordinary-heartbeat-error-first", outbound.OutboundTypeOpenAIChat, firstUpstream.URL+"/v1")
	second := createChannel("ordinary-heartbeat-error-second", outbound.OutboundTypeOpenAIChat, secondUpstream.URL+"/v1")
	if err := op.ChannelCreate(first, ctx); err != nil {
		t.Fatalf("create first channel: %v", err)
	}
	if err := op.ChannelCreate(second, ctx); err != nil {
		t.Fatalf("create second channel: %v", err)
	}
	group := &model.Group{Name: "ordinary-heartbeat-error-failover-group", Mode: model.GroupModeFailover}
	if err := op.GroupCreate(group, ctx); err != nil {
		t.Fatalf("create group: %v", err)
	}
	if err := op.GroupItemAdd(&model.GroupItem{GroupID: group.ID, ChannelID: first.ID, ModelName: "fb-model", Priority: 1}, ctx); err != nil {
		t.Fatalf("add first group item: %v", err)
	}
	if err := op.GroupItemAdd(&model.GroupItem{GroupID: group.ID, ChannelID: second.ID, ModelName: "fb-model", Priority: 2}, ctx); err != nil {
		t.Fatalf("add second group item: %v", err)
	}

	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Set("api_key_id", 806)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"ordinary-heartbeat-error-failover-group","messages":[{"role":"user","content":"hello"}],"stream":true}`))
	c.Request.Header.Set("Content-Type", "application/json")
	Handler(inbound.InboundTypeOpenAIChat, c)

	body := recorder.Body.String()
	if got := recorder.Header().Get("Content-Type"); got != "text/event-stream" {
		t.Fatalf("committed fallback error Content-Type = %q, want text/event-stream", got)
	}
	if strings.Count(body, ":\n\n") == 0 || strings.Count(body, "event: error") != 1 {
		t.Fatalf("expected heartbeat plus one SSE error event, got %q", body)
	}
	if strings.HasPrefix(strings.TrimSpace(body), "{") || strings.Contains(body, `{"error":"temporarily unavailable"}`) {
		t.Fatalf("final error mixed upstream/JSON content into SSE response: %q", body)
	}
}
