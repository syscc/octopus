package relay

import (
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
	"github.com/bestruirui/octopus/internal/transformer/inbound"
	"github.com/bestruirui/octopus/internal/transformer/outbound"
	"github.com/gin-gonic/gin"
)

func TestHandlerPassthroughStructuredFailureAfterPayloadStopsFailover(t *testing.T) {
	gin.SetMode(gin.TestMode)
	tests := []struct {
		name          string
		inboundType   inbound.InboundType
		outboundType  outbound.OutboundType
		path          string
		request       string
		stream        string
		fallback      string
		terminal      string
		privateDetail string
	}{
		{
			name:          "openai responses",
			inboundType:   inbound.InboundTypeOpenAIResponse,
			outboundType:  outbound.OutboundTypeOpenAIResponse,
			path:          "/v1/responses",
			request:       `{"model":"%s","input":"hello","stream":true}`,
			stream:        "data: {\"type\":\"response.output_text.delta\",\"delta\":\"partial\"}\n\ndata: {\"type\":\"response.failed\",\"response\":{\"status\":\"failed\",\"error\":{\"code\":\"server_error\",\"message\":\"responses-private-terminal-detail\"}}}\n\n",
			fallback:      "data: {\"type\":\"response.output_text.delta\",\"delta\":\"fallback\"}\n\ndata: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"output\":[]}}\n\n",
			terminal:      `"type":"response.failed"`,
			privateDetail: "responses-private-terminal-detail",
		},
		{
			name:          "anthropic messages",
			inboundType:   inbound.InboundTypeAnthropic,
			outboundType:  outbound.OutboundTypeAnthropic,
			path:          "/v1/messages",
			request:       `{"model":"%s","max_tokens":8,"messages":[{"role":"user","content":"hello"}],"stream":true}`,
			stream:        "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_partial\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"upstream-model\",\"content\":[],\"usage\":{\"input_tokens\":1,\"output_tokens\":0}}}\n\nevent: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"partial\"}}\n\nevent: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"api_error\",\"message\":\"anthropic-private-terminal-detail\"}}\n\n",
			fallback:      "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_fallback\",\"type\":\"message\",\"role\":\"assistant\",\"model\":\"upstream-model\",\"content\":[],\"usage\":{\"input_tokens\":1,\"output_tokens\":0}}}\n\nevent: message_stop\ndata: {\"type\":\"message_stop\"}\n\n",
			terminal:      "event: error",
			privateDetail: "anthropic-private-terminal-detail",
		},
	}

	for index, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := setupRelayTestDB(t)
			if err := op.SettingSetInt(model.SettingKeyCircuitBreakerThreshold, 1); err != nil {
				t.Fatalf("set circuit threshold: %v", err)
			}

			var firstHits atomic.Int32
			firstUpstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				firstHits.Add(1)
				if r.URL.Path != tc.path {
					http.NotFound(w, r)
					return
				}
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = w.Write([]byte(tc.stream))
			}))
			t.Cleanup(firstUpstream.Close)

			var fallbackHits atomic.Int32
			fallbackUpstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				fallbackHits.Add(1)
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = w.Write([]byte(tc.fallback))
			}))
			t.Cleanup(fallbackUpstream.Close)

			groupName := "structured-sse-terminal-" + strings.ReplaceAll(tc.name, " ", "-")
			first := createChannel(groupName+"-first", tc.outboundType, firstUpstream.URL+"/v1")
			fallback := createChannel(groupName+"-fallback", tc.outboundType, fallbackUpstream.URL+"/v1")
			if err := op.ChannelCreate(first, ctx); err != nil {
				t.Fatalf("create first channel: %v", err)
			}
			if err := op.ChannelCreate(fallback, ctx); err != nil {
				t.Fatalf("create fallback channel: %v", err)
			}
			group := &model.Group{Name: groupName, Mode: model.GroupModeFailover, RetryEnabled: true, MaxRetries: 2}
			if err := op.GroupCreate(group, ctx); err != nil {
				t.Fatalf("create group: %v", err)
			}
			if err := op.GroupItemAdd(&model.GroupItem{GroupID: group.ID, ChannelID: first.ID, ModelName: "upstream-model", Priority: 1}, ctx); err != nil {
				t.Fatalf("add first group item: %v", err)
			}
			if err := op.GroupItemAdd(&model.GroupItem{GroupID: group.ID, ChannelID: fallback.ID, ModelName: "upstream-model", Priority: 2}, ctx); err != nil {
				t.Fatalf("add fallback group item: %v", err)
			}
			for _, channelID := range []int{first.ID, fallback.ID} {
				outlierwindow.Clear(channelID)
				t.Cleanup(func() {
					balancer.ResetStateByChannel(channelID)
					outlierwindow.Clear(channelID)
				})
			}

			recorder := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(recorder)
			apiKeyID := 940 + index
			c.Set("api_key_id", apiKeyID)
			c.Request = httptest.NewRequest(http.MethodPost, tc.path, strings.NewReader(strings.Replace(tc.request, "%s", groupName, 1)))
			c.Request.Header.Set("Content-Type", "application/json")
			Handler(tc.inboundType, c)

			if got := recorder.Body.String(); got != tc.stream {
				t.Fatalf("structured terminal stream changed or gained a generic error:\n got: %q\nwant: %q", got, tc.stream)
			}
			if got := strings.Count(recorder.Body.String(), tc.terminal); got != 1 {
				t.Fatalf("terminal marker count = %d, want 1 in %q", got, recorder.Body.String())
			}
			if got := firstHits.Load(); got != 1 {
				t.Fatalf("first channel hits = %d, want 1", got)
			}
			if got := fallbackHits.Load(); got != 0 {
				t.Fatalf("visible structured failure reached fallback channel %d times", got)
			}
			if tripped, _ := balancer.IsTripped(first.ID, first.Keys[0].ID, "upstream-model"); !tripped {
				t.Fatal("structured terminal failure did not trip circuit health")
			}
			if stats := outlierwindow.Evaluate(first.ID, time.Now()); stats.Samples != 1 || stats.Failures != 1 {
				t.Fatalf("structured terminal outlier health = %+v, want one failure", stats)
			}
			if stats := op.StatsChannelGet(first.ID); stats.RequestSuccess != 0 || stats.RequestFailed != 1 {
				t.Fatalf("structured terminal channel stats = %+v, want one failure", stats)
			}
			if sticky := balancer.GetSticky(apiKeyID, groupName, time.Minute); sticky != nil {
				t.Fatalf("structured terminal failure established sticky routing: %#v", sticky)
			}
			logs, err := op.RelayLogList(ctx, nil, nil, nil, 1, 10)
			if err != nil || len(logs) == 0 {
				t.Fatalf("load relay log: logs=%d err=%v", len(logs), err)
			}
			if strings.Contains(logs[0].Error, tc.privateDetail) {
				t.Fatalf("relay log error exposed provider terminal detail: %q", logs[0].Error)
			}
		})
	}
}
