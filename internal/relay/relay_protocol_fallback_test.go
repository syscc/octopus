package relay

import (
	"encoding/json"
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
	"github.com/bestruirui/octopus/internal/transformer/inbound"
	"github.com/bestruirui/octopus/internal/transformer/outbound"
	"github.com/gin-gonic/gin"
)

// 本文件覆盖协议回落的既有语义回归：
// 1) /v1/chat/completions 下游优先走原生 Chat 通道，可回退到 Responses 通道并转回 Chat。
// 2) /v1/responses 下游优先走原生 Responses 通道，可回退到 Chat 通道并转回 Responses。
// 3) 原生 Responses 不可转换能力（image_generation 工具 / control 字段）仅允许 Responses 通道。
// 同协议优先只影响候选排序，不破坏 failover / 重试 / 粘性。

// mockChannelServer 模拟只支持一个 OpenAI 端点的上游，并记录所有请求。
type mockChannelServer struct {
	hits  *atomic.Int32
	url   string
	mu    sync.Mutex
	paths []string
}

func newMockChannelServer(t *testing.T, supportedPath string, status int, body string) *mockChannelServer {
	t.Helper()
	s := &mockChannelServer{hits: &atomic.Int32{}}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.hits.Add(1)
		s.mu.Lock()
		s.paths = append(s.paths, r.URL.Path)
		s.mu.Unlock()
		if r.URL.Path != supportedPath {
			http.Error(w, `{"error":"endpoint not found"}`, http.StatusNotFound)
			return
		}
		if status == 0 {
			status = http.StatusOK
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(server.Close)
	s.url = server.URL + "/v1"
	return s
}

func newStreamingMockChannelServer(t *testing.T, supportedPath, body string) *mockChannelServer {
	t.Helper()
	s := &mockChannelServer{hits: &atomic.Int32{}}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.hits.Add(1)
		s.mu.Lock()
		s.paths = append(s.paths, r.URL.Path)
		s.mu.Unlock()
		if r.URL.Path != supportedPath {
			http.Error(w, `{"error":"endpoint not found"}`, http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = fmt.Fprint(w, body)
	}))
	t.Cleanup(server.Close)
	s.url = server.URL + "/v1"
	return s
}

func (s *mockChannelServer) requestedPaths() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.paths...)
}

func createChannel(name string, ct outbound.OutboundType, url string) *model.Channel {
	return &model.Channel{
		Name:     name,
		Type:     ct,
		Enabled:  true,
		BaseUrls: []model.BaseUrl{{URL: url}},
		Model:    "fb-model",
		Keys:     []model.ChannelKey{{Enabled: true, ChannelKey: "k"}},
	}
}

func TestChatAndResponsesProtocolPreferenceOverridesGroupPriority(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, tc := range []struct {
		name        string
		inboundType inbound.InboundType
		path        string
		body        string
		preferred   outbound.OutboundType
	}{
		{
			name:        "chat prefers chat",
			inboundType: inbound.InboundTypeOpenAIChat,
			path:        "/v1/chat/completions",
			body:        `{"model":"protocol-priority-chat","messages":[{"role":"user","content":"hello"}]}`,
			preferred:   outbound.OutboundTypeOpenAIChat,
		},
		{
			name:        "responses prefers responses",
			inboundType: inbound.InboundTypeOpenAIResponse,
			path:        "/v1/responses",
			body:        `{"model":"protocol-priority-responses","input":"hello"}`,
			preferred:   outbound.OutboundTypeOpenAIResponse,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := setupRelayTestDB(t)
			chat := newMockChannelServer(t, "/v1/chat/completions", http.StatusOK, `{"id":"c1","object":"chat.completion","model":"m","choices":[{"index":0,"message":{"role":"assistant","content":"ok"}}]}`)
			responses := newMockChannelServer(t, "/v1/responses", http.StatusOK, `{"id":"r1","object":"response","status":"completed","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"ok"}]}]}`)
			chatChannel := createChannel("priority-chat", outbound.OutboundTypeOpenAIChat, chat.url)
			responsesChannel := createChannel("priority-responses", outbound.OutboundTypeOpenAIResponse, responses.url)
			if err := op.ChannelCreate(chatChannel, ctx); err != nil {
				t.Fatalf("create chat channel: %v", err)
			}
			if err := op.ChannelCreate(responsesChannel, ctx); err != nil {
				t.Fatalf("create responses channel: %v", err)
			}
			groupName := "protocol-priority-chat"
			if tc.inboundType == inbound.InboundTypeOpenAIResponse {
				groupName = "protocol-priority-responses"
			}
			group := &model.Group{Name: groupName, Mode: model.GroupModeFailover}
			if err := op.GroupCreate(group, ctx); err != nil {
				t.Fatalf("create group: %v", err)
			}
			// Give the non-preferred protocol the better group priority. The
			// protocol preference must reverse this order for the request.
			first, second := chatChannel, responsesChannel
			if tc.preferred == outbound.OutboundTypeOpenAIChat {
				first, second = responsesChannel, chatChannel
			}
			if err := op.GroupItemAdd(&model.GroupItem{GroupID: group.ID, ChannelID: first.ID, ModelName: "fb-model", Priority: 1}, ctx); err != nil {
				t.Fatalf("add first item: %v", err)
			}
			if err := op.GroupItemAdd(&model.GroupItem{GroupID: group.ID, ChannelID: second.ID, ModelName: "fb-model", Priority: 2}, ctx); err != nil {
				t.Fatalf("add second item: %v", err)
			}

			recorder := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(recorder)
			c.Request = httptest.NewRequest(http.MethodPost, tc.path, strings.NewReader(tc.body))
			c.Request.Header.Set("Content-Type", "application/json")
			Handler(tc.inboundType, c)
			if recorder.Code != http.StatusOK {
				t.Fatalf("expected success, got %d body %s", recorder.Code, recorder.Body.String())
			}
			if tc.preferred == outbound.OutboundTypeOpenAIChat {
				if chat.hits.Load() != 1 || responses.hits.Load() != 0 {
					t.Fatalf("expected only Chat channel, got chat=%d responses=%d", chat.hits.Load(), responses.hits.Load())
				}
			} else if responses.hits.Load() != 1 || chat.hits.Load() != 0 {
				t.Fatalf("expected only Responses channel, got chat=%d responses=%d", chat.hits.Load(), responses.hits.Load())
			}
		})
	}
}

func TestChatPrefersChatOverResponses(t *testing.T) {
	gin.SetMode(gin.TestMode)
	ctx := setupRelayTestDB(t)

	chat := newMockChannelServer(t, "/v1/chat/completions", http.StatusOK, `{"id":"c1","object":"chat.completion","created":1,"model":"m","choices":[{"index":0,"message":{"role":"assistant","content":"chat-ok"}}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}`)
	responses := newMockChannelServer(t, "/v1/responses", http.StatusOK, `{"id":"r1","object":"response","status":"completed","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"resp-ok"}]}],"usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}`)

	chatChannel := createChannel("fb-chat", outbound.OutboundTypeOpenAIChat, chat.url)
	if err := op.ChannelCreate(chatChannel, ctx); err != nil {
		t.Fatalf("create chat channel: %v", err)
	}
	responsesChannel := createChannel("fb-responses", outbound.OutboundTypeOpenAIResponse, responses.url)
	if err := op.ChannelCreate(responsesChannel, ctx); err != nil {
		t.Fatalf("create responses channel: %v", err)
	}

	group := &model.Group{Name: "fb-chat-group", Mode: model.GroupModeFailover}
	if err := op.GroupCreate(group, ctx); err != nil {
		t.Fatalf("create group: %v", err)
	}
	if err := op.GroupItemAdd(&model.GroupItem{GroupID: group.ID, ChannelID: chatChannel.ID, ModelName: "fb-model", Priority: 1}, ctx); err != nil {
		t.Fatalf("add chat item: %v", err)
	}
	if err := op.GroupItemAdd(&model.GroupItem{GroupID: group.ID, ChannelID: responsesChannel.ID, ModelName: "fb-model", Priority: 2}, ctx); err != nil {
		t.Fatalf("add responses item: %v", err)
	}

	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Set("api_key_id", 71)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions",
		strings.NewReader(`{"model":"fb-chat-group","messages":[{"role":"user","content":"hello"}]}`))
	c.Request.Header.Set("Content-Type", "application/json")
	Handler(inbound.InboundTypeOpenAIChat, c)

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body %s", recorder.Code, recorder.Body.String())
	}
	if chat.hits.Load() != 1 {
		t.Fatalf("expected native chat channel to be preferred and hit once, got %d", chat.hits.Load())
	}
	if responses.hits.Load() != 0 {
		t.Fatalf("expected responses channel NOT hit when chat healthy, got %d", responses.hits.Load())
	}
	if !strings.Contains(recorder.Body.String(), `"object":"chat.completion"`) || !strings.Contains(recorder.Body.String(), `"content":"chat-ok"`) {
		t.Fatalf("expected chat.completion client body, got %s", recorder.Body.String())
	}
}

func TestChatFallsBackToResponsesWhenChatFails(t *testing.T) {
	gin.SetMode(gin.TestMode)
	ctx := setupRelayTestDB(t)

	chat := newMockChannelServer(t, "/v1/chat/completions", http.StatusServiceUnavailable, `{"error":"down"}`)
	responses := newMockChannelServer(t, "/v1/responses", http.StatusOK, `{"id":"r1","object":"response","status":"completed","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"fallback-ok"}]}],"usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}`)

	chatChannel := createChannel("fbc-chat", outbound.OutboundTypeOpenAIChat, chat.url)
	if err := op.ChannelCreate(chatChannel, ctx); err != nil {
		t.Fatalf("create chat channel: %v", err)
	}
	responsesChannel := createChannel("fbc-responses", outbound.OutboundTypeOpenAIResponse, responses.url)
	if err := op.ChannelCreate(responsesChannel, ctx); err != nil {
		t.Fatalf("create responses channel: %v", err)
	}

	group := &model.Group{Name: "fbc-group", Mode: model.GroupModeFailover}
	if err := op.GroupCreate(group, ctx); err != nil {
		t.Fatalf("create group: %v", err)
	}
	if err := op.GroupItemAdd(&model.GroupItem{GroupID: group.ID, ChannelID: chatChannel.ID, ModelName: "fb-model", Priority: 1}, ctx); err != nil {
		t.Fatalf("add chat item: %v", err)
	}
	if err := op.GroupItemAdd(&model.GroupItem{GroupID: group.ID, ChannelID: responsesChannel.ID, ModelName: "fb-model", Priority: 2}, ctx); err != nil {
		t.Fatalf("add responses item: %v", err)
	}

	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Set("api_key_id", 72)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions",
		strings.NewReader(`{"model":"fbc-group","messages":[{"role":"user","content":"hello"}]}`))
	c.Request.Header.Set("Content-Type", "application/json")
	Handler(inbound.InboundTypeOpenAIChat, c)

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected 200 via responses fallback, got %d body %s", recorder.Code, recorder.Body.String())
	}
	if chat.hits.Load() != 1 {
		t.Fatalf("expected chat channel attempted first, got %d", chat.hits.Load())
	}
	if responses.hits.Load() != 2 {
		t.Fatalf("expected responses channel to try chat then fall back to responses, got %d", responses.hits.Load())
	}
	paths := responses.requestedPaths()
	if len(paths) != 2 || paths[0] != "/v1/chat/completions" || paths[1] != "/v1/responses" {
		t.Fatalf("expected downstream Chat protocol first then Responses fallback, got %v", paths)
	}
	if !strings.Contains(recorder.Body.String(), `"object":"chat.completion"`) || !strings.Contains(recorder.Body.String(), `"content":"fallback-ok"`) {
		t.Fatalf("expected chat.completion client body from responses upstream, got %s", recorder.Body.String())
	}
}

func TestResponsesPrefersResponsesOverChat(t *testing.T) {
	gin.SetMode(gin.TestMode)
	ctx := setupRelayTestDB(t)

	responses := newMockChannelServer(t, "/v1/responses", http.StatusOK, `{"id":"r1","object":"response","status":"completed","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"resp-ok"}]}],"usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}`)
	chat := newMockChannelServer(t, "/v1/chat/completions", http.StatusOK, `{"id":"c1","object":"chat.completion","created":1,"model":"m","choices":[{"index":0,"message":{"role":"assistant","content":"chat-ok"}}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}`)

	responsesChannel := createChannel("rpc-responses", outbound.OutboundTypeOpenAIResponse, responses.url)
	if err := op.ChannelCreate(responsesChannel, ctx); err != nil {
		t.Fatalf("create responses channel: %v", err)
	}
	chatChannel := createChannel("rpc-chat", outbound.OutboundTypeOpenAIChat, chat.url)
	if err := op.ChannelCreate(chatChannel, ctx); err != nil {
		t.Fatalf("create chat channel: %v", err)
	}

	group := &model.Group{Name: "rpc-group", Mode: model.GroupModeFailover}
	if err := op.GroupCreate(group, ctx); err != nil {
		t.Fatalf("create group: %v", err)
	}
	if err := op.GroupItemAdd(&model.GroupItem{GroupID: group.ID, ChannelID: responsesChannel.ID, ModelName: "fb-model", Priority: 1}, ctx); err != nil {
		t.Fatalf("add responses item: %v", err)
	}
	if err := op.GroupItemAdd(&model.GroupItem{GroupID: group.ID, ChannelID: chatChannel.ID, ModelName: "fb-model", Priority: 2}, ctx); err != nil {
		t.Fatalf("add chat item: %v", err)
	}

	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Set("api_key_id", 73)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses",
		strings.NewReader(`{"model":"rpc-group","input":"hello"}`))
	c.Request.Header.Set("Content-Type", "application/json")
	Handler(inbound.InboundTypeOpenAIResponse, c)

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body %s", recorder.Code, recorder.Body.String())
	}
	if responses.hits.Load() != 1 {
		t.Fatalf("expected native responses channel preferred, got %d", responses.hits.Load())
	}
	if chat.hits.Load() != 0 {
		t.Fatalf("expected chat channel NOT hit when responses healthy, got %d", chat.hits.Load())
	}
	if !strings.Contains(recorder.Body.String(), `"object":"response"`) || !strings.Contains(recorder.Body.String(), `"status":"completed"`) {
		t.Fatalf("expected responses client body, got %s", recorder.Body.String())
	}
}

func TestResponsesFallsBackToChatWhenResponsesFails(t *testing.T) {
	gin.SetMode(gin.TestMode)
	ctx := setupRelayTestDB(t)

	responses := newMockChannelServer(t, "/v1/responses", http.StatusServiceUnavailable, `{"error":"down"}`)
	chat := newMockChannelServer(t, "/v1/chat/completions", http.StatusOK, `{"id":"c1","object":"chat.completion","created":1,"model":"m","choices":[{"index":0,"message":{"role":"assistant","content":"fallback-ok"}}],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}`)

	responsesChannel := createChannel("rfc-responses", outbound.OutboundTypeOpenAIResponse, responses.url)
	if err := op.ChannelCreate(responsesChannel, ctx); err != nil {
		t.Fatalf("create responses channel: %v", err)
	}
	chatChannel := createChannel("rfc-chat", outbound.OutboundTypeOpenAIChat, chat.url)
	if err := op.ChannelCreate(chatChannel, ctx); err != nil {
		t.Fatalf("create chat channel: %v", err)
	}

	group := &model.Group{Name: "rfc-group", Mode: model.GroupModeFailover}
	if err := op.GroupCreate(group, ctx); err != nil {
		t.Fatalf("create group: %v", err)
	}
	if err := op.GroupItemAdd(&model.GroupItem{GroupID: group.ID, ChannelID: responsesChannel.ID, ModelName: "fb-model", Priority: 1}, ctx); err != nil {
		t.Fatalf("add responses item: %v", err)
	}
	if err := op.GroupItemAdd(&model.GroupItem{GroupID: group.ID, ChannelID: chatChannel.ID, ModelName: "fb-model", Priority: 2}, ctx); err != nil {
		t.Fatalf("add chat item: %v", err)
	}

	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Set("api_key_id", 74)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses",
		strings.NewReader(`{"model":"rfc-group","input":"hello"}`))
	c.Request.Header.Set("Content-Type", "application/json")
	Handler(inbound.InboundTypeOpenAIResponse, c)

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected 200 via chat fallback, got %d body %s", recorder.Code, recorder.Body.String())
	}
	if responses.hits.Load() != 1 {
		t.Fatalf("expected responses channel attempted first, got %d", responses.hits.Load())
	}
	if chat.hits.Load() != 2 {
		t.Fatalf("expected chat channel to try responses then fall back to chat, got %d", chat.hits.Load())
	}
	paths := chat.requestedPaths()
	if len(paths) != 2 || paths[0] != "/v1/responses" || paths[1] != "/v1/chat/completions" {
		t.Fatalf("expected downstream Responses protocol first then Chat fallback, got %v", paths)
	}
	if !strings.Contains(recorder.Body.String(), `"object":"response"`) || !strings.Contains(recorder.Body.String(), `"status":"completed"`) || !strings.Contains(recorder.Body.String(), `"fallback-ok"`) {
		t.Fatalf("expected responses client body from chat upstream, got %s", recorder.Body.String())
	}
	var parsed map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &parsed); err != nil {
		t.Fatalf("expected valid JSON responses body, got error %v", err)
	}
}

func TestChatStreamConvertsResponsesUpstream(t *testing.T) {
	gin.SetMode(gin.TestMode)
	ctx := setupRelayTestDB(t)

	upstream := newStreamingMockChannelServer(t, "/v1/responses", strings.Join([]string{
		`data: {"type":"response.created","response":{"id":"resp_stream","model":"fb-model","status":"in_progress","output":[]}}`,
		"",
		`data: {"type":"response.output_text.delta","delta":"hello"}`,
		"",
		`data: {"type":"response.completed","response":{"id":"resp_stream","model":"fb-model","status":"completed","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"hello"}]}],"usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}}`,
		"",
		`data: [DONE]`,
		"",
	}, "\n"))
	channel := createChannel("stream-chat-via-responses", outbound.OutboundTypeOpenAIResponse, upstream.url)
	if err := op.ChannelCreate(channel, ctx); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	group := &model.Group{Name: "stream-chat-via-responses-group", Mode: model.GroupModeFailover}
	if err := op.GroupCreate(group, ctx); err != nil {
		t.Fatalf("create group: %v", err)
	}
	if err := op.GroupItemAdd(&model.GroupItem{GroupID: group.ID, ChannelID: channel.ID, ModelName: "fb-model"}, ctx); err != nil {
		t.Fatalf("add group item: %v", err)
	}

	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"stream-chat-via-responses-group","messages":[{"role":"user","content":"hello"}],"stream":true}`))
	c.Request.Header.Set("Content-Type", "application/json")
	Handler(inbound.InboundTypeOpenAIChat, c)

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body %s", recorder.Code, recorder.Body.String())
	}
	body := recorder.Body.String()
	if !strings.Contains(body, `"object":"chat.completion.chunk"`) || !strings.Contains(body, `"content":"hello"`) {
		t.Fatalf("expected Chat SSE converted from Responses upstream, got %s", body)
	}
	if paths := upstream.requestedPaths(); len(paths) != 2 || paths[0] != "/v1/chat/completions" || paths[1] != "/v1/responses" {
		t.Fatalf("expected Chat endpoint probe then Responses fallback, got %v", paths)
	}
}

func TestResponsesStreamConvertsChatUpstream(t *testing.T) {
	gin.SetMode(gin.TestMode)
	ctx := setupRelayTestDB(t)

	upstream := newStreamingMockChannelServer(t, "/v1/chat/completions", strings.Join([]string{
		`data: {"id":"chat_stream","object":"chat.completion.chunk","created":1,"model":"fb-model","choices":[{"index":0,"delta":{"role":"assistant","content":"hello"}}]}`,
		"",
		`data: {"id":"chat_stream","object":"chat.completion.chunk","created":1,"model":"fb-model","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}`,
		"",
		`data: {"id":"chat_stream","object":"chat.completion.chunk","created":1,"model":"fb-model","choices":[],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}`,
		"",
		`data: [DONE]`,
		"",
	}, "\n"))
	channel := createChannel("stream-responses-via-chat", outbound.OutboundTypeOpenAIChat, upstream.url)
	if err := op.ChannelCreate(channel, ctx); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	group := &model.Group{Name: "stream-responses-via-chat-group", Mode: model.GroupModeFailover}
	if err := op.GroupCreate(group, ctx); err != nil {
		t.Fatalf("create group: %v", err)
	}
	if err := op.GroupItemAdd(&model.GroupItem{GroupID: group.ID, ChannelID: channel.ID, ModelName: "fb-model"}, ctx); err != nil {
		t.Fatalf("add group item: %v", err)
	}

	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"stream-responses-via-chat-group","input":"hello","stream":true}`))
	c.Request.Header.Set("Content-Type", "application/json")
	Handler(inbound.InboundTypeOpenAIResponse, c)

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body %s", recorder.Code, recorder.Body.String())
	}
	body := recorder.Body.String()
	for _, eventType := range []string{"response.created", "response.output_text.delta", "response.completed"} {
		if !strings.Contains(body, `"type":"`+eventType+`"`) {
			t.Fatalf("expected %s in Responses SSE converted from Chat upstream, got %s", eventType, body)
		}
	}
	if paths := upstream.requestedPaths(); len(paths) != 2 || paths[0] != "/v1/responses" || paths[1] != "/v1/chat/completions" {
		t.Fatalf("expected Responses endpoint probe then Chat fallback, got %v", paths)
	}
}

// 原生 Responses 不可转换能力（image_generation 工具）在无 Responses 通道时应明确失败，
// Chat 通道不得被静默降级尝试。
func TestNativeResponsesToolImageGenerationRequiresResponsesChannel(t *testing.T) {
	gin.SetMode(gin.TestMode)
	ctx := setupRelayTestDB(t)

	chat := newMockChannelServer(t, "/v1/chat/completions", http.StatusOK, `{"id":"c1","object":"chat.completion","created":1,"model":"m","choices":[{"index":0,"message":{"role":"assistant","content":"x"}}]}`)
	chatChannel := createChannel("img-chat", outbound.OutboundTypeOpenAIChat, chat.url)
	if err := op.ChannelCreate(chatChannel, ctx); err != nil {
		t.Fatalf("create chat channel: %v", err)
	}
	group := &model.Group{Name: "img-group", Mode: model.GroupModeFailover}
	if err := op.GroupCreate(group, ctx); err != nil {
		t.Fatalf("create group: %v", err)
	}
	if err := op.GroupItemAdd(&model.GroupItem{GroupID: group.ID, ChannelID: chatChannel.ID, ModelName: "fb-model", Priority: 1}, ctx); err != nil {
		t.Fatalf("add chat item: %v", err)
	}

	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Set("api_key_id", 75)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses",
		strings.NewReader(`{"model":"img-group","input":"draw","tools":[{"type":"image_generation","name":"img_gen"}]}`))
	c.Request.Header.Set("Content-Type", "application/json")
	Handler(inbound.InboundTypeOpenAIResponse, c)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("expected image_generation native tool to be rejected without responses channel, got %d body %s", recorder.Code, recorder.Body.String())
	}
	if chat.hits.Load() != 1 {
		t.Fatalf("expected exactly one native Responses capability probe, got %d hits", chat.hits.Load())
	}
	paths := chat.requestedPaths()
	if len(paths) != 1 || paths[0] != "/v1/responses" {
		t.Fatalf("expected native request to probe only /v1/responses and never fall back to chat, got %v", paths)
	}
	if !strings.Contains(recorder.Body.String(), "仅支持 OpenAI Responses 通道直通") {
		t.Fatalf("expected clear passthrough-only error, got %s", recorder.Body.String())
	}
}

// Responses 原生控制字段（如 background）无法用 Chat 表达，在无 Responses 通道时应明确失败。
func TestNativeResponsesControlFieldRequiresResponsesChannel(t *testing.T) {
	gin.SetMode(gin.TestMode)
	ctx := setupRelayTestDB(t)

	chat := newMockChannelServer(t, "/v1/chat/completions", http.StatusOK, `{"id":"c1","object":"chat.completion","created":1,"model":"m","choices":[{"index":0,"message":{"role":"assistant","content":"x"}}]}`)
	chatChannel := createChannel("ctl-chat", outbound.OutboundTypeOpenAIChat, chat.url)
	if err := op.ChannelCreate(chatChannel, ctx); err != nil {
		t.Fatalf("create chat channel: %v", err)
	}
	group := &model.Group{Name: "ctl-group", Mode: model.GroupModeFailover}
	if err := op.GroupCreate(group, ctx); err != nil {
		t.Fatalf("create group: %v", err)
	}
	if err := op.GroupItemAdd(&model.GroupItem{GroupID: group.ID, ChannelID: chatChannel.ID, ModelName: "fb-model", Priority: 1}, ctx); err != nil {
		t.Fatalf("add chat item: %v", err)
	}

	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Set("api_key_id", 76)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses",
		strings.NewReader(`{"model":"ctl-group","input":"hello","background":true}`))
	c.Request.Header.Set("Content-Type", "application/json")
	Handler(inbound.InboundTypeOpenAIResponse, c)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("expected native responses control field to be rejected without responses channel, got %d body %s", recorder.Code, recorder.Body.String())
	}
	if chat.hits.Load() != 1 {
		t.Fatalf("expected exactly one native Responses capability probe, got %d hits", chat.hits.Load())
	}
	paths := chat.requestedPaths()
	if len(paths) != 1 || paths[0] != "/v1/responses" {
		t.Fatalf("expected native request to probe only /v1/responses and never fall back to chat, got %v", paths)
	}
	if !strings.Contains(recorder.Body.String(), "仅支持 OpenAI Responses 通道直通") {
		t.Fatalf("expected clear passthrough-only error, got %s", recorder.Body.String())
	}
}

func TestChatToResponsesFallbackUsesIsolatedRequestSnapshot(t *testing.T) {
	gin.SetMode(gin.TestMode)
	ctx := setupRelayTestDB(t)

	var responsesPayload map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/chat/completions" {
			http.Error(w, `{"error":"endpoint not found"}`, http.StatusNotFound)
			return
		}
		if r.URL.Path != "/v1/responses" {
			http.NotFound(w, r)
			return
		}
		if err := json.NewDecoder(r.Body).Decode(&responsesPayload); err != nil {
			t.Fatalf("decode responses fallback request: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"r1","object":"response","status":"completed","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"ok"}]}]}`))
	}))
	defer server.Close()

	channel := createChannel("isolated-fallback", outbound.OutboundTypeOpenAIChat, server.URL+"/v1")
	if err := op.ChannelCreate(channel, ctx); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	group := &model.Group{Name: "isolated-fallback-group", Mode: model.GroupModeFailover}
	if err := op.GroupCreate(group, ctx); err != nil {
		t.Fatalf("create group: %v", err)
	}
	if err := op.GroupItemAdd(&model.GroupItem{GroupID: group.ID, ChannelID: channel.ID, ModelName: "fb-model"}, ctx); err != nil {
		t.Fatalf("add group item: %v", err)
	}

	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{
		"model":"isolated-fallback-group",
		"messages":[
			{"role":"assistant","content":"prior","reasoning_content":"summary","reasoning_signature":"enc-sig"},
			{"role":"user","content":"hello"}
		]
	}`))
	c.Request.Header.Set("Content-Type", "application/json")
	Handler(inbound.InboundTypeOpenAIChat, c)

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected successful Responses fallback, got %d body %s", recorder.Code, recorder.Body.String())
	}
	input, ok := responsesPayload["input"].([]any)
	if !ok {
		t.Fatalf("expected Responses input array, got %#v", responsesPayload["input"])
	}
	foundSignature := false
	for _, rawItem := range input {
		item, ok := rawItem.(map[string]any)
		if ok && item["type"] == "reasoning" && item["encrypted_content"] == "enc-sig" {
			foundSignature = true
			break
		}
	}
	if !foundSignature {
		t.Fatalf("expected reasoning signature to survive Chat endpoint failure, got %#v", input)
	}
}

func TestResponsesToChatFallbackPreservesExistingReplayStates(t *testing.T) {
	gin.SetMode(gin.TestMode)
	ctx := setupRelayTestDB(t)
	resetResponsesReplayStore()
	defer resetResponsesReplayStore()

	upstream := newMockChannelServer(t, "/v1/chat/completions", http.StatusOK, `{"id":"c1","object":"chat.completion","model":"m","choices":[{"index":0,"message":{"role":"assistant","content":"ok"}}]}`)
	channel := createChannel("replay-preserving-chat", outbound.OutboundTypeOpenAIChat, upstream.url)
	if err := op.ChannelCreate(channel, ctx); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	group := &model.Group{Name: "replay-preserving-group", Mode: model.GroupModeFailover}
	if err := op.GroupCreate(group, ctx); err != nil {
		t.Fatalf("create group: %v", err)
	}
	if err := op.GroupItemAdd(&model.GroupItem{GroupID: group.ID, ChannelID: channel.ID, ModelName: "fb-model"}, ctx); err != nil {
		t.Fatalf("add group item: %v", err)
	}

	for _, responseID := range []string{"resp_a", "resp_b"} {
		storeResponsesReplayState(88, group.ID, group.Name, &wsConversationState{
			RequestModel:   group.Name,
			ChannelID:      channel.ID,
			LastResponseID: responseID,
		}, time.Minute)
	}

	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Set("api_key_id", 88)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"replay-preserving-group","input":"hello"}`))
	c.Request.Header.Set("Content-Type", "application/json")
	Handler(inbound.InboundTypeOpenAIResponse, c)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected successful Chat fallback, got %d body %s", recorder.Code, recorder.Body.String())
	}
	for _, responseID := range []string{"resp_a", "resp_b"} {
		if state := loadResponsesReplayState(88, group.ID, group.Name, responseID); state == nil {
			t.Fatalf("expected unrelated replay state %s to remain", responseID)
		}
	}
}

func TestNativeResponsesWithoutAvailableKeyReturnsServiceUnavailable(t *testing.T) {
	gin.SetMode(gin.TestMode)
	ctx := setupRelayTestDB(t)

	channel := createChannel("native-no-key", outbound.OutboundTypeOpenAIResponse, "https://unused.invalid/v1")
	channel.Keys = nil
	if err := op.ChannelCreate(channel, ctx); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	group := &model.Group{Name: "native-no-key-group", Mode: model.GroupModeFailover}
	if err := op.GroupCreate(group, ctx); err != nil {
		t.Fatalf("create group: %v", err)
	}
	if err := op.GroupItemAdd(&model.GroupItem{GroupID: group.ID, ChannelID: channel.ID, ModelName: "fb-model"}, ctx); err != nil {
		t.Fatalf("add group item: %v", err)
	}

	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{
		"model":"native-no-key-group",
		"input":"hello",
		"background":true
	}`))
	c.Request.Header.Set("Content-Type", "application/json")
	Handler(inbound.InboundTypeOpenAIResponse, c)

	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected operational 503 instead of capability 400, got %d body %s", recorder.Code, recorder.Body.String())
	}
}
