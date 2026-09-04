package relay

import (
	"context"
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
	"github.com/bestruirui/octopus/internal/outlierwindow"
	"github.com/bestruirui/octopus/internal/relay/balancer"
	"github.com/bestruirui/octopus/internal/transformer/outbound"
	"github.com/coder/websocket"
)

func TestWSProtocolUnavailableStillRecordsHealthFailure(t *testing.T) {
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

	var hits atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/responses" {
			http.NotFound(w, r)
			return
		}
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		readCtx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		if _, _, err := conn.Read(readCtx); err != nil {
			return
		}
		hits.Add(1)
		_ = conn.Write(readCtx, websocket.MessageText, []byte(`{"type":"error","error":{"type":"invalid_request_error","message":"Invalid URL (POST /v1/responses)"}}`))
	}))
	t.Cleanup(upstream.Close)

	channel := &model.Channel{
		Name: "ws-native-health", Type: outbound.OutboundTypeOpenAIResponse, Enabled: true,
		BaseUrls: []model.BaseUrl{{URL: upstream.URL + "/v1"}}, Model: "upstream-model",
		Keys: []model.ChannelKey{{Enabled: true, ChannelKey: "ws-key"}},
	}
	if err := op.ChannelCreate(channel, ctx); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	group := &model.Group{Name: "ws-native-health-group", Mode: model.GroupModeFailover}
	if err := op.GroupCreate(group, ctx); err != nil {
		t.Fatalf("create group: %v", err)
	}
	if err := op.GroupItemAdd(&model.GroupItem{GroupID: group.ID, ChannelID: channel.ID, ModelName: "upstream-model"}, ctx); err != nil {
		t.Fatalf("add group item: %v", err)
	}
	balancer.ResetStateByChannel(channel.ID)
	outlierwindow.Clear(channel.ID)
	t.Cleanup(func() {
		balancer.ResetStateByChannel(channel.ID)
		outlierwindow.Clear(channel.ID)
	})

	clientConn, serverConn := newTestWSConnPair(t)
	t.Cleanup(func() {
		_ = clientConn.Close(websocket.StatusNormalClosure, "")
		_ = serverConn.Close(websocket.StatusNormalClosure, "")
	})
	state := processWSResponseCreate(
		context.Background(), serverConn,
		[]byte(`{"type":"response.create","model":"ws-native-health-group","input":"hello","background":true}`),
		1, "", "ws-native-health-session", nil,
	)
	if state != nil {
		t.Fatalf("native capability failure must not create replay state: %#v", state)
	}
	if got := hits.Load(); got != 1 {
		t.Fatalf("expected exactly one Responses WS probe, got %d", got)
	}
	stats := outlierwindow.Evaluate(channel.ID, time.Now())
	if stats.Samples != 1 || stats.Failures != 1 {
		t.Fatalf("WS capability failure must count in outlier health, got %+v", stats)
	}
	if tripped, _ := balancer.IsTripped(channel.ID, channel.Keys[0].ID, "upstream-model"); !tripped {
		t.Fatalf("WS capability failure must count in circuit health")
	}
}

func TestWSResponsesFallsBackToChatAndContinuesWithLocalTranscript(t *testing.T) {
	ctx := setupRelayTestDB(t)
	if err := op.SettingSetString(model.SettingKeyResponsesWSEnabled, "false"); err != nil {
		t.Fatalf("disable upstream Responses WS: %v", err)
	}

	var mu sync.Mutex
	paths := make([]string, 0, 4)
	chatPayloads := make([]map[string]any, 0, 2)
	responsesPayloads := make([]map[string]any, 0, 2)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		paths = append(paths, r.URL.Path)
		mu.Unlock()
		if r.URL.Path == "/v1/responses" {
			var payload map[string]any
			if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			mu.Lock()
			responsesPayloads = append(responsesPayloads, payload)
			mu.Unlock()
			http.Error(w, `{"error":"endpoint not found"}`, http.StatusNotFound)
			return
		}
		if r.URL.Path != "/v1/chat/completions" {
			http.NotFound(w, r)
			return
		}

		var payload map[string]any
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		mu.Lock()
		chatPayloads = append(chatPayloads, payload)
		turn := len(chatPayloads)
		mu.Unlock()

		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte(strings.Join([]string{
			fmt.Sprintf(`data: {"id":"chat-ws-%d","object":"chat.completion.chunk","created":1,"model":"upstream-model","choices":[{"index":0,"delta":{"role":"assistant","content":"answer %d"}}]}`, turn, turn),
			"",
			fmt.Sprintf(`data: {"id":"chat-ws-%d","object":"chat.completion.chunk","created":1,"model":"upstream-model","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}`, turn),
			"",
			fmt.Sprintf(`data: {"id":"chat-ws-%d","object":"chat.completion.chunk","created":1,"model":"upstream-model","choices":[],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}`, turn),
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

	readCompleted := func() []string {
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
				return eventTypes
			}
		}
	}

	state := processWSResponseCreate(
		context.Background(),
		serverConn,
		[]byte(`{"type":"response.create","model":"ws-fallback-group","input":"hello"}`),
		1,
		"",
		"ws-fallback-session",
		nil,
	)
	firstEvents := readCompleted()
	if len(firstEvents) < 3 || firstEvents[0] != "response.created" || firstEvents[1] != "response.in_progress" {
		t.Fatalf("expected Responses lifecycle events after Chat fallback, got %v", firstEvents)
	}
	if state == nil || !state.LastOutboundTypeSet || state.LastOutboundType != outbound.OutboundTypeOpenAIChat {
		t.Fatalf("expected Chat fallback state, got %#v", state)
	}
	if state.LastResponseID == "" || len(state.Transcript) != 2 {
		t.Fatalf("expected first turn and local response id to be stored, got %#v", state)
	}
	firstResponseID := state.LastResponseID

	state = processWSResponseCreate(
		context.Background(),
		serverConn,
		[]byte(fmt.Sprintf(`{"type":"response.create","model":"ws-fallback-group","previous_response_id":%q,"input":"second"}`, firstResponseID)),
		1,
		"",
		"ws-fallback-session",
		state,
	)
	secondEvents := readCompleted()
	if len(secondEvents) < 3 || secondEvents[0] != "response.created" || secondEvents[1] != "response.in_progress" {
		t.Fatalf("expected second Responses lifecycle after local replay, got %v", secondEvents)
	}
	if state == nil || len(state.Transcript) != 4 || state.LastOutboundType != outbound.OutboundTypeOpenAIChat {
		t.Fatalf("expected two complete Chat-backed turns, got %#v", state)
	}

	lastResponseID := state.LastResponseID
	state = processWSResponseCreate(
		context.Background(),
		serverConn,
		[]byte(fmt.Sprintf(`{"type":"response.create","model":"ws-fallback-group","previous_response_id":%q,"input":"third","background":true}`, lastResponseID)),
		1,
		"",
		"ws-fallback-session",
		state,
	)
	if state != nil {
		t.Fatalf("expected unsafe local replay to clear conversation state, got %#v", state)
	}
	errorCtx, cancelError := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelError()
	_, errorData, err := clientConn.Read(errorCtx)
	if err != nil {
		t.Fatalf("read replay rejection: %v", err)
	}
	var replayError struct {
		Type   string `json:"type"`
		Status int    `json:"status"`
		Error  struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal(errorData, &replayError); err != nil {
		t.Fatalf("decode replay rejection %q: %v", errorData, err)
	}
	if replayError.Type != "error" || replayError.Status != http.StatusConflict || replayError.Error.Code != "conversation_restart_required" {
		t.Fatalf("unexpected replay rejection: %+v", replayError)
	}

	mu.Lock()
	gotPaths := append([]string(nil), paths...)
	gotChatPayloads := append([]map[string]any(nil), chatPayloads...)
	gotResponsesPayloads := append([]map[string]any(nil), responsesPayloads...)
	mu.Unlock()
	wantPaths := []string{"/v1/responses", "/v1/chat/completions", "/v1/chat/completions"}
	if len(gotPaths) != len(wantPaths) {
		t.Fatalf("unexpected upstream path count: got %v want %v", gotPaths, wantPaths)
	}
	for index := range wantPaths {
		if gotPaths[index] != wantPaths[index] {
			t.Fatalf("unexpected upstream paths: got %v want %v", gotPaths, wantPaths)
		}
	}
	if len(gotChatPayloads) != 2 {
		t.Fatalf("expected two Chat payloads, got %d", len(gotChatPayloads))
	}
	if len(gotResponsesPayloads) != 1 {
		t.Fatalf("expected the first turn to probe Responses once and later turns to reuse the learned result, got %d", len(gotResponsesPayloads))
	}
	learnedChannel, err := op.ChannelGet(channel.ID, ctx)
	if err != nil {
		t.Fatalf("reload learned channel: %v", err)
	}
	if learnedChannel.OpenAIResponsesCapability != model.OpenAIProtocolCapabilityUnsupported ||
		learnedChannel.OpenAIChatCapability != model.OpenAIProtocolCapabilitySupported {
		t.Fatalf("expected persisted Chat-only automatic capability, got chat=%q responses=%q", learnedChannel.OpenAIChatCapability, learnedChannel.OpenAIResponsesCapability)
	}
	secondPayload, err := json.Marshal(gotChatPayloads[1])
	if err != nil {
		t.Fatalf("marshal second Chat payload: %v", err)
	}
	if strings.Contains(string(secondPayload), firstResponseID) || strings.Contains(string(secondPayload), "previous_response_id") {
		t.Fatalf("local response id must not reach Chat upstream: %s", secondPayload)
	}
	for _, expected := range []string{"hello", "answer 1", "second"} {
		if !strings.Contains(string(secondPayload), expected) {
			t.Fatalf("expected second Chat payload to contain %q, got %s", expected, secondPayload)
		}
	}
}

func TestWSResponsesEndpointErrorFallsBackToChatAndPersists(t *testing.T) {
	ctx := setupRelayTestDB(t)
	resetWSUpstreamPool()
	t.Cleanup(resetWSUpstreamPool)
	if err := op.SettingSetString(model.SettingKeyResponsesWSEnabled, "true"); err != nil {
		t.Fatalf("enable upstream Responses WS: %v", err)
	}
	if err := op.SettingSetString(model.SettingKeyResponsesWSDefaultMode, "passthrough"); err != nil {
		t.Fatalf("set default upstream Responses WS mode: %v", err)
	}

	var mu sync.Mutex
	wsHits := 0
	chatHits := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/responses":
			conn, err := websocket.Accept(w, r, nil)
			if err != nil {
				return
			}
			defer conn.CloseNow()
			readCtx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
			defer cancel()
			if _, _, err := conn.Read(readCtx); err != nil {
				return
			}
			mu.Lock()
			wsHits++
			mu.Unlock()
			_ = conn.Write(readCtx, websocket.MessageText, []byte(`{"type":"error","error":{"type":"invalid_request_error","message":"Invalid URL (POST /v1/responses)"}}`))
		case "/v1/chat/completions":
			mu.Lock()
			chatHits++
			turn := chatHits
			mu.Unlock()
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = fmt.Fprint(w, strings.Join([]string{
				fmt.Sprintf(`data: {"id":"chat-ws-fallback-%d","object":"chat.completion.chunk","created":1,"model":"upstream-model","choices":[{"index":0,"delta":{"role":"assistant","content":"answer %d"}}]}`, turn, turn),
				"",
				fmt.Sprintf(`data: {"id":"chat-ws-fallback-%d","object":"chat.completion.chunk","created":1,"model":"upstream-model","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}`, turn),
				"",
				fmt.Sprintf(`data: {"id":"chat-ws-fallback-%d","object":"chat.completion.chunk","created":1,"model":"upstream-model","choices":[],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}`, turn),
				"",
				"data: [DONE]",
				"",
			}, "\n"))
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(upstream.Close)

	channel := &model.Channel{
		Name:     "ws-structured-capability-fallback",
		Type:     outbound.OutboundTypeOpenAIResponse,
		Enabled:  true,
		BaseUrls: []model.BaseUrl{{URL: upstream.URL + "/v1"}},
		Model:    "upstream-model",
		Keys:     []model.ChannelKey{{Enabled: true, ChannelKey: "ws-key"}},
	}
	if err := op.ChannelCreate(channel, ctx); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	group := &model.Group{Name: "ws-structured-capability-group", Mode: model.GroupModeFailover}
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
	readCompleted := func() {
		readCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		for {
			_, data, err := clientConn.Read(readCtx)
			if err != nil {
				t.Fatalf("read downstream WS event: %v", err)
			}
			var event struct {
				Type string `json:"type"`
			}
			if err := json.Unmarshal(data, &event); err != nil {
				t.Fatalf("decode downstream WS event %q: %v", data, err)
			}
			if event.Type == "response.completed" {
				return
			}
		}
	}

	for _, input := range []string{"first", "second"} {
		state := processWSResponseCreate(
			context.Background(),
			serverConn,
			[]byte(fmt.Sprintf(`{"type":"response.create","model":"ws-structured-capability-group","input":%q}`, input)),
			1,
			"",
			"ws-structured-capability-session-"+input,
			nil,
		)
		if state == nil || state.LastOutboundType != outbound.OutboundTypeOpenAIChat {
			t.Fatalf("expected Chat-backed state for %s turn, got %#v", input, state)
		}
		readCompleted()
	}

	mu.Lock()
	gotWSHits, gotChatHits := wsHits, chatHits
	mu.Unlock()
	if gotWSHits != 1 || gotChatHits != 2 {
		t.Fatalf("expected one WS Responses probe and two Chat requests, got ws=%d chat=%d", gotWSHits, gotChatHits)
	}
	learned, err := op.ChannelGet(channel.ID, ctx)
	if err != nil {
		t.Fatalf("reload channel: %v", err)
	}
	if learned.OpenAIResponsesCapability != model.OpenAIProtocolCapabilityUnsupported ||
		learned.OpenAIChatCapability != model.OpenAIProtocolCapabilitySupported {
		t.Fatalf("expected persisted Chat-only automatic capability, got chat=%q responses=%q", learned.OpenAIChatCapability, learned.OpenAIResponsesCapability)
	}
}

func TestNewWSRelayRequestRejectsNilExecutionRequest(t *testing.T) {
	req, group, err := newWSRelayRequest(context.Background(), nil, nil, 1, "model", nil, nil, nil, nil)
	if err == nil || !strings.Contains(err.Error(), "internal request is nil") {
		t.Fatalf("expected nil execution request to be rejected, got req=%#v group=%#v err=%v", req, group, err)
	}
}
