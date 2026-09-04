package relay

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
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

func TestOrphanResponsesToolOutputRequiresNativeUpstream(t *testing.T) {
	toolCallID := "call_orphan"
	orphan := &transformerModel.InternalLLMRequest{
		RawAPIFormat: transformerModel.APIFormatOpenAIResponse,
		Messages: []transformerModel.Message{{
			Role: "tool", ToolCallID: &toolCallID,
			Content: transformerModel.MessageContent{Content: stringPtr("result")},
		}},
	}
	if !requiresNativeResponsesUpstream(orphan) {
		t.Fatal("orphan tool output must require a native Responses upstream")
	}
	if canFallbackToOutbound(orphan, outbound.OutboundTypeOpenAIChat) {
		t.Fatal("orphan tool output must not be converted to Chat")
	}

	paired := &transformerModel.InternalLLMRequest{
		RawAPIFormat: transformerModel.APIFormatOpenAIResponse,
		Messages: []transformerModel.Message{
			{
				Role: "assistant",
				ToolCalls: []transformerModel.ToolCall{{
					ID: toolCallID, Type: "function",
					Function: transformerModel.FunctionCall{Name: "lookup", Arguments: `{}`},
				}},
			},
			{
				Role: "tool", ToolCallID: &toolCallID,
				Content: transformerModel.MessageContent{Content: stringPtr("result")},
			},
		},
	}
	if requiresNativeResponsesUpstream(paired) {
		t.Fatal("a tool output paired with its assistant tool call should remain Chat-convertible")
	}
	if !canFallbackToOutbound(paired, outbound.OutboundTypeOpenAIChat) {
		t.Fatal("paired tool call and output should allow Chat conversion")
	}
}

func TestHandlerDoesNotSendOrphanToolOutputToChatOnlyChannel(t *testing.T) {
	gin.SetMode(gin.TestMode)
	ctx := setupRelayTestDB(t)

	var upstreamHits atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamHits.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"unexpected","object":"chat.completion","choices":[{"message":{"role":"assistant","content":"unsafe"}}]}`))
	}))
	t.Cleanup(upstream.Close)

	channel := createChannel("orphan-tool-output-chat-only", outbound.OutboundTypeOpenAIChat, upstream.URL+"/v1")
	channel.OpenAIProtocolMode = model.OpenAIProtocolModeChatOnly
	if err := op.ChannelCreate(channel, ctx); err != nil {
		t.Fatalf("create Chat-only channel: %v", err)
	}
	group := &model.Group{Name: "orphan-tool-output-group", Mode: model.GroupModeFailover}
	if err := op.GroupCreate(group, ctx); err != nil {
		t.Fatalf("create group: %v", err)
	}
	if err := op.GroupItemAdd(&model.GroupItem{GroupID: group.ID, ChannelID: channel.ID, ModelName: "fb-model"}, ctx); err != nil {
		t.Fatalf("add group item: %v", err)
	}

	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{
		"model":"orphan-tool-output-group",
		"input":[{"type":"function_call_output","call_id":"call_orphan","output":"result"}]
	}`))
	c.Request.Header.Set("Content-Type", "application/json")
	Handler(inbound.InboundTypeOpenAIResponse, c)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("expected native Responses capability error, got %d body=%s", recorder.Code, recorder.Body.String())
	}
	if upstreamHits.Load() != 0 {
		t.Fatalf("orphan tool output reached Chat-only upstream %d times", upstreamHits.Load())
	}
}

func TestHTTPContinuationDoesNotAppendErrorAfterPartialSSE(t *testing.T) {
	gin.SetMode(gin.TestMode)
	ctx := setupRelayTestDB(t)

	var firstRequests atomic.Int32
	upstreamWS := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		firstRequests.Add(1)
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		if _, _, err := conn.Read(r.Context()); err != nil {
			return
		}
		_ = conn.Write(r.Context(), websocket.MessageText, []byte(`{"type":"response.created","response":{"id":"resp_http_partial","object":"response","model":"upstream-model","status":"in_progress","output":[]}}`))
		_ = conn.Write(r.Context(), websocket.MessageText, []byte(`{"type":"response.output_text.delta","response":{"id":"resp_http_partial","model":"upstream-model"},"delta":"partial-http"}`))
		conn.CloseNow()
	}))
	t.Cleanup(upstreamWS.Close)

	var secondHits atomic.Int32
	second := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		secondHits.Add(1)
		http.Error(w, `{"error":"must not fail over"}`, http.StatusServiceUnavailable)
	}))
	t.Cleanup(second.Close)

	firstChannel := createChannel("http-partial-continuation-first", outbound.OutboundTypeOpenAIResponse, upstreamWS.URL+"/v1")
	secondChannel := createChannel("http-partial-continuation-second", outbound.OutboundTypeOpenAIResponse, second.URL+"/v1")
	if err := op.ChannelCreate(firstChannel, ctx); err != nil {
		t.Fatalf("create first channel: %v", err)
	}
	if err := op.ChannelCreate(secondChannel, ctx); err != nil {
		t.Fatalf("create second channel: %v", err)
	}
	group := &model.Group{Name: "http-partial-continuation-group", Mode: model.GroupModeFailover, SessionKeepTime: 60}
	if err := op.GroupCreate(group, ctx); err != nil {
		t.Fatalf("create group: %v", err)
	}
	if err := op.GroupItemAdd(&model.GroupItem{GroupID: group.ID, ChannelID: firstChannel.ID, ModelName: "upstream-model", Priority: 1}, ctx); err != nil {
		t.Fatalf("add first group item: %v", err)
	}
	if err := op.GroupItemAdd(&model.GroupItem{GroupID: group.ID, ChannelID: secondChannel.ID, ModelName: "second-model", Priority: 2}, ctx); err != nil {
		t.Fatalf("add second group item: %v", err)
	}

	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Set("api_key_id", 911)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{
		"model":"http-partial-continuation-group",
		"previous_response_id":"resp_previous",
		"input":"next",
		"stream":true
	}`))
	c.Request.Header.Set("Content-Type", "application/json")
	Handler(inbound.InboundTypeOpenAIResponse, c)

	body := recorder.Body.String()
	if !strings.Contains(body, "partial-http") {
		t.Fatalf("expected partial SSE payload, got %s", body)
	}
	for _, forbidden := range []string{"event: error", `"code":409`, "conversation_restart_required", "上游连续会话已中断", "all_channels_failed"} {
		if strings.Contains(body, forbidden) {
			t.Fatalf("partial SSE contains an appended non-protocol error %q: %s", forbidden, body)
		}
	}
	if firstRequests.Load() != 1 {
		t.Fatalf("partial continuation should issue one upstream request, got %d", firstRequests.Load())
	}
	if secondHits.Load() != 0 {
		t.Fatalf("partial continuation must not fail over after payload, second hits=%d", secondHits.Load())
	}
}

func TestWSResponseCreateDoesNotReplayAfterPassthroughPayload(t *testing.T) {
	gin.SetMode(gin.TestMode)
	ctx := setupRelayTestDB(t)
	if err := op.SettingSetString(model.SettingKeyResponsesWSEnabled, "true"); err != nil {
		t.Fatalf("enable Responses WS: %v", err)
	}
	if err := op.SettingSetString(model.SettingKeyResponsesWSDefaultMode, "passthrough"); err != nil {
		t.Fatalf("set Responses WS passthrough mode: %v", err)
	}

	var upstreamRequests atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamRequests.Add(1)
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		if _, _, err := conn.Read(r.Context()); err != nil {
			return
		}
		_ = conn.Write(r.Context(), websocket.MessageText, []byte(`{"type":"response.created","response":{"id":"resp_ws_partial","object":"response","model":"upstream-model","status":"in_progress","output":[]}}`))
		_ = conn.Write(r.Context(), websocket.MessageText, []byte(`{"type":"response.output_text.delta","response":{"id":"resp_ws_partial","model":"upstream-model"},"delta":"partial-ws"}`))
		conn.CloseNow()
	}))
	t.Cleanup(upstream.Close)

	channel := createChannel("ws-partial-no-replay", outbound.OutboundTypeOpenAIResponse, upstream.URL+"/v1")
	channel.WSMode = model.ChannelWSModePassthrough
	if err := op.ChannelCreate(channel, ctx); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	group := &model.Group{Name: "ws-partial-no-replay-group", Mode: model.GroupModeFailover, SessionKeepTime: 60}
	if err := op.GroupCreate(group, ctx); err != nil {
		t.Fatalf("create group: %v", err)
	}
	if err := op.GroupItemAdd(&model.GroupItem{GroupID: group.ID, ChannelID: channel.ID, ModelName: "upstream-model"}, ctx); err != nil {
		t.Fatalf("add group item: %v", err)
	}

	clientConn, serverConn := newTestWSConnPair(t)
	t.Cleanup(func() {
		_ = clientConn.Close(websocket.StatusNormalClosure, "")
		_ = serverConn.Close(websocket.StatusNormalClosure, "")
	})

	state := &wsConversationState{
		DownstreamSessionID: "downstream-partial-session",
		RequestModel:        group.Name,
		ChannelID:           channel.ID,
		ChannelKeyID:        channel.Keys[0].ID,
		LastResponseID:      "resp_previous",
		ReplayWindowItems: json.RawMessage(`[
			{"type":"message","role":"user","content":[{"type":"input_text","text":"previous"}]}
		]`),
		Transcript: []transformerModel.Message{{
			Role: "user", Content: transformerModel.MessageContent{Content: stringPtr("previous")},
		}},
		LastOutboundType:    outbound.OutboundTypeOpenAIResponse,
		LastOutboundTypeSet: true,
	}
	request := []byte(`{
		"type":"response.create",
		"model":"ws-partial-no-replay-group",
		"previous_response_id":"resp_previous",
		"input":"next"
	}`)
	resultingState := processWSResponseCreate(context.Background(), serverConn, request, 912, "", state.DownstreamSessionID, state)
	if resultingState != nil {
		t.Fatalf("broken native continuation should clear conversation state, got %+v", resultingState)
	}

	readCtx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	var eventTypes []string
	for len(eventTypes) < 8 {
		_, data, err := clientConn.Read(readCtx)
		if err != nil {
			break
		}
		var event struct {
			Type string `json:"type"`
		}
		if err := json.Unmarshal(data, &event); err != nil {
			t.Fatalf("decode downstream WS frame %q: %v", data, err)
		}
		eventTypes = append(eventTypes, event.Type)
	}

	if upstreamRequests.Load() != 1 {
		t.Fatalf("partial WS continuation was replayed; upstream requests=%d events=%v", upstreamRequests.Load(), eventTypes)
	}
	if len(eventTypes) != 3 || eventTypes[0] != "response.created" || eventTypes[1] != "response.output_text.delta" || eventTypes[2] != "response.failed" {
		t.Fatalf("expected partial payload plus one protocol failure terminal, got %v", eventTypes)
	}
	for _, eventType := range eventTypes {
		if eventType == "error" {
			t.Fatalf("partial WS stream must not append a generic error frame: %v", eventTypes)
		}
	}
}
