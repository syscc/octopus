package relay

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bestruirui/octopus/internal/model"
	"github.com/bestruirui/octopus/internal/op"
	"github.com/bestruirui/octopus/internal/transformer/outbound"
	"github.com/coder/websocket"
)

func TestWSResponsesFallsBackToChatAndEmitsResponsesEvents(t *testing.T) {
	ctx := setupRelayTestDB(t)
	if err := op.SettingSetString(model.SettingKeyResponsesWSEnabled, "false"); err != nil {
		t.Fatalf("disable upstream Responses WS: %v", err)
	}

	var mu sync.Mutex
	paths := make([]string, 0, 2)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		paths = append(paths, r.URL.Path)
		mu.Unlock()
		if r.URL.Path == "/v1/responses" {
			http.Error(w, `{"error":"endpoint not found"}`, http.StatusNotFound)
			return
		}
		if r.URL.Path != "/v1/chat/completions" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte(strings.Join([]string{
			`data: {"id":"chat-ws","object":"chat.completion.chunk","created":1,"model":"upstream-model","choices":[{"index":0,"delta":{"role":"assistant","content":"hello"}}]}`,
			"",
			`data: {"id":"chat-ws","object":"chat.completion.chunk","created":1,"model":"upstream-model","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}`,
			"",
			`data: {"id":"chat-ws","object":"chat.completion.chunk","created":1,"model":"upstream-model","choices":[],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}`,
			"",
			`data: [DONE]`,
			"",
		}, "\n")))
	}))
	defer upstream.Close()

	channel := &model.Channel{
		Name:     "ws-chat-fallback",
		Type:     outbound.OutboundTypeOpenAIChat,
		Enabled:  true,
		BaseUrls: []model.BaseUrl{{URL: upstream.URL + "/v1"}},
		Model:    "upstream-model",
		Keys:     []model.ChannelKey{{Enabled: true, ChannelKey: "ws-key"}},
	}
	if err := op.ChannelCreate(channel, ctx); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	group := &model.Group{Name: "ws-fallback-group", Mode: model.GroupModeFailover}
	if err := op.GroupCreate(group, ctx); err != nil {
		t.Fatalf("create group: %v", err)
	}
	if err := op.GroupItemAdd(&model.GroupItem{GroupID: group.ID, ChannelID: channel.ID, ModelName: "upstream-model"}, ctx); err != nil {
		t.Fatalf("add group item: %v", err)
	}

	clientConn, serverConn := newTestWSConnPair(t)
	defer clientConn.Close(websocket.StatusNormalClosure, "")
	defer serverConn.Close(websocket.StatusNormalClosure, "")

	state := processWSResponseCreate(
		context.Background(),
		serverConn,
		[]byte(`{"type":"response.create","model":"ws-fallback-group","input":"hello"}`),
		1,
		"",
		"ws-fallback-session",
		nil,
	)
	if state != nil {
		t.Fatalf("expected Chat fallback not to create a native Responses conversation state, got %#v", state)
	}

	readCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var eventTypes []string
	for {
		_, data, err := clientConn.Read(readCtx)
		if err != nil {
			t.Fatalf("read downstream WS event: %v (events=%v)", err, eventTypes)
		}
		var event struct {
			Type string `json:"type"`
		}
		if err := json.Unmarshal(data, &event); err != nil {
			t.Fatalf("decode downstream WS event %q: %v", data, err)
		}
		eventTypes = append(eventTypes, event.Type)
		if event.Type == "response.completed" {
			break
		}
	}
	if len(eventTypes) < 3 || eventTypes[0] != "response.created" || eventTypes[1] != "response.in_progress" {
		t.Fatalf("expected Responses lifecycle events after Chat fallback, got %v", eventTypes)
	}

	mu.Lock()
	gotPaths := append([]string(nil), paths...)
	mu.Unlock()
	if len(gotPaths) != 2 || gotPaths[0] != "/v1/responses" || gotPaths[1] != "/v1/chat/completions" {
		t.Fatalf("expected Responses then Chat upstream paths, got %v", gotPaths)
	}
}
