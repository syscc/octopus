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
	"github.com/bestruirui/octopus/internal/outlierwindow"
	"github.com/bestruirui/octopus/internal/relay/balancer"
	"github.com/bestruirui/octopus/internal/transformer/inbound"
	"github.com/bestruirui/octopus/internal/transformer/outbound"
	"github.com/gin-gonic/gin"
)

func TestResponsesChatPartialEOFWritesIncompleteWithoutFailoverOrReplay(t *testing.T) {
	gin.SetMode(gin.TestMode)
	ctx := setupRelayTestDB(t)
	if err := op.SettingSetInt(model.SettingKeyCircuitBreakerThreshold, 1); err != nil {
		t.Fatalf("set circuit breaker threshold: %v", err)
	}
	resetResponsesReplayStore()
	t.Cleanup(resetResponsesReplayStore)

	var firstMu sync.Mutex
	firstPaths := make([]string, 0, 2)
	firstServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		firstMu.Lock()
		firstPaths = append(firstPaths, r.URL.Path)
		firstMu.Unlock()
		switch r.URL.Path {
		case "/v1/responses":
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			_, _ = fmt.Fprint(w, `{"error":{"message":"responses unsupported","type":"invalid_request_error","code":"RESPONSES_MODEL_NOT_SUPPORTED"}}`)
		case "/v1/chat/completions":
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = fmt.Fprint(w, "data: {\"id\":\"chat_partial\",\"object\":\"chat.completion.chunk\",\"created\":21,\"model\":\"chat-model\",\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"partial answer\"}}]}\n\n")
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(firstServer.Close)

	var secondHits atomic.Int32
	secondServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		secondHits.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, `{"id":"unexpected","object":"response","status":"completed","output":[]}`)
	}))
	t.Cleanup(secondServer.Close)

	firstChannel := createChannel("partial-eof-chat-first", outbound.OutboundTypeOpenAIChat, firstServer.URL+"/v1")
	secondChannel := createChannel("partial-eof-chat-second", outbound.OutboundTypeOpenAIChat, secondServer.URL+"/v1")
	if err := op.ChannelCreate(firstChannel, ctx); err != nil {
		t.Fatalf("create first channel: %v", err)
	}
	if err := op.ChannelCreate(secondChannel, ctx); err != nil {
		t.Fatalf("create second channel: %v", err)
	}
	if len(firstChannel.Keys) == 0 {
		t.Fatal("expected first channel key")
	}
	balancer.ResetStateByChannel(firstChannel.ID)
	outlierwindow.Clear(firstChannel.ID)
	t.Cleanup(func() {
		balancer.ResetStateByChannel(firstChannel.ID)
		outlierwindow.Clear(firstChannel.ID)
	})
	group := &model.Group{Name: "partial-eof-chat-group", Mode: model.GroupModeFailover}
	if err := op.GroupCreate(group, ctx); err != nil {
		t.Fatalf("create group: %v", err)
	}
	if err := op.GroupItemAdd(&model.GroupItem{GroupID: group.ID, ChannelID: firstChannel.ID, ModelName: "chat-model", Priority: 1}, ctx); err != nil {
		t.Fatalf("add first item: %v", err)
	}
	if err := op.GroupItemAdd(&model.GroupItem{GroupID: group.ID, ChannelID: secondChannel.ID, ModelName: "second-model", Priority: 2}, ctx); err != nil {
		t.Fatalf("add second item: %v", err)
	}

	const apiKeyID = 701
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Set("api_key_id", apiKeyID)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"partial-eof-chat-group","input":"hello","stream":true}`))
	c.Request.Header.Set("Content-Type", "application/json")
	Handler(inbound.InboundTypeOpenAIResponse, c)

	body := recorder.Body.String()
	assertSingleIncompleteStream(t, body, "partial answer")
	if secondHits.Load() != 0 {
		t.Fatalf("must not fail over after payload was written, second hits=%d", secondHits.Load())
	}
	if tripped, _ := balancer.IsTripped(firstChannel.ID, firstChannel.Keys[0].ID, "chat-model"); !tripped {
		t.Fatal("transform incomplete stream did not count as a hard circuit failure")
	}
	if stats := outlierwindow.Evaluate(firstChannel.ID, time.Now()); stats.Samples != 1 || stats.Failures != 1 {
		t.Fatalf("transform incomplete health = %+v, want exactly one failure", stats)
	}
	firstMu.Lock()
	gotPaths := append([]string(nil), firstPaths...)
	firstMu.Unlock()
	if len(gotPaths) != 2 || gotPaths[0] != "/v1/responses" || gotPaths[1] != "/v1/chat/completions" {
		t.Fatalf("expected Responses probe then Chat fallback, got %v", gotPaths)
	}
	if state := loadResponsesReplayState(apiKeyID, group.ID, group.Name, "chat_partial"); state != nil {
		t.Fatalf("incomplete response must not be saved for replay: %+v", state)
	}
}

func TestResponsesPassthroughPartialEOFWritesIncompleteWithoutFailoverOrReplay(t *testing.T) {
	gin.SetMode(gin.TestMode)
	ctx := setupRelayTestDB(t)
	if err := op.SettingSetInt(model.SettingKeyCircuitBreakerThreshold, 1); err != nil {
		t.Fatalf("set circuit breaker threshold failed: %v", err)
	}
	resetResponsesReplayStore()
	t.Cleanup(resetResponsesReplayStore)

	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/responses" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = fmt.Fprint(w, strings.Join([]string{
			`data: {"type":"response.created","sequence_number":10,"response":{"id":"resp_native_partial","object":"response","model":"native-model","created_at":31,"status":"in_progress","output":[]}}`,
			"",
			`data: {"type":"response.output_item.added","sequence_number":11,"output_index":0,"item":{"id":"msg_upstream","type":"message","role":"assistant","status":"in_progress","content":[]}}`,
			"",
			`data: {"type":"response.content_part.added","sequence_number":12,"item_id":"msg_upstream","output_index":0,"content_index":0,"part":{"type":"output_text","text":""}}`,
			"",
			`data: {"type":"response.output_text.delta","sequence_number":13,"item_id":"msg_upstream","output_index":0,"content_index":0,"delta":"native partial"}`,
			"",
			`data: {"type":"response.output_text.done","sequence_number":14,"item_id":"msg_upstream","output_index":0,"content_index":0,"text":"native partial"}`,
			"",
			`data: {"type":"response.content_part.done","sequence_number":15,"item_id":"msg_upstream","output_index":0,"content_index":0,"part":{"type":"output_text","text":"native partial"}}`,
			"",
			`data: {"type":"response.output_item.done","sequence_number":16,"output_index":0,"item":{"id":"msg_upstream","type":"message","role":"assistant","status":"completed","content":[{"type":"output_text","text":"native partial"}]}}`,
			"",
			`data: [DONE]`,
			"",
		}, "\n"))
	}))
	t.Cleanup(upstream.Close)

	var secondHits atomic.Int32
	secondServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		secondHits.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, `{"id":"unexpected","object":"response","status":"completed","output":[]}`)
	}))
	t.Cleanup(secondServer.Close)

	firstChannel := createChannel("partial-eof-native-first", outbound.OutboundTypeOpenAIResponse, upstream.URL+"/v1")
	secondChannel := createChannel("partial-eof-native-second", outbound.OutboundTypeOpenAIResponse, secondServer.URL+"/v1")
	if err := op.ChannelCreate(firstChannel, ctx); err != nil {
		t.Fatalf("create first channel: %v", err)
	}
	if err := op.ChannelCreate(secondChannel, ctx); err != nil {
		t.Fatalf("create second channel: %v", err)
	}
	group := &model.Group{Name: "partial-eof-native-group", Mode: model.GroupModeFailover}
	if err := op.GroupCreate(group, ctx); err != nil {
		t.Fatalf("create group: %v", err)
	}
	if err := op.GroupItemAdd(&model.GroupItem{GroupID: group.ID, ChannelID: firstChannel.ID, ModelName: "native-model", Priority: 1}, ctx); err != nil {
		t.Fatalf("add first item: %v", err)
	}
	if err := op.GroupItemAdd(&model.GroupItem{GroupID: group.ID, ChannelID: secondChannel.ID, ModelName: "second-model", Priority: 2}, ctx); err != nil {
		t.Fatalf("add second item: %v", err)
	}

	const apiKeyID = 702
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Set("api_key_id", apiKeyID)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"partial-eof-native-group","input":"hello","stream":true}`))
	c.Request.Header.Set("Content-Type", "application/json")
	Handler(inbound.InboundTypeOpenAIResponse, c)

	body := recorder.Body.String()
	assertSingleIncompleteStream(t, body, "native partial")
	if sequence := incompleteSequenceNumber(t, body); sequence <= 16 {
		t.Fatalf("synthetic terminal sequence must continue after upstream sequence 16, got %d in %s", sequence, body)
	}
	if count := strings.Count(body, `"type":"response.output_item.done"`); count != 1 {
		t.Fatalf("upstream completed item must not be closed twice, got %d events in %s", count, body)
	}
	if itemID := incompleteOutputItemID(t, body); itemID != "msg_upstream" {
		t.Fatalf("incomplete terminal must preserve upstream item id, got %q in %s", itemID, body)
	}
	if secondHits.Load() != 0 {
		t.Fatalf("must not fail over after passthrough payload was written, second hits=%d", secondHits.Load())
	}
	if len(firstChannel.Keys) == 0 {
		t.Fatal("expected first channel key")
	}
	if tripped, _ := balancer.IsTripped(firstChannel.ID, firstChannel.Keys[0].ID, "native-model"); !tripped {
		t.Fatal("incomplete upstream stream must count as a hard circuit failure")
	}
	if state := loadResponsesReplayState(apiKeyID, group.ID, group.Name, "resp_native_partial"); state != nil {
		t.Fatalf("incomplete passthrough response must not be saved for replay: %+v", state)
	}
}

func assertSingleIncompleteStream(t *testing.T, body, expectedText string) {
	t.Helper()
	if !strings.Contains(body, expectedText) {
		t.Fatalf("expected partial text %q in stream: %s", expectedText, body)
	}
	if count := strings.Count(body, `"type":"response.incomplete"`); count != 1 {
		t.Fatalf("expected exactly one response.incomplete, got %d in %s", count, body)
	}
	for _, forbidden := range []string{`"type":"response.completed"`, `"type":"response.failed"`, "data: [DONE]"} {
		if strings.Contains(body, forbidden) {
			t.Fatalf("incomplete stream contains forbidden terminal %q: %s", forbidden, body)
		}
	}
}

func incompleteSequenceNumber(t *testing.T, body string) int {
	t.Helper()
	for _, line := range strings.Split(body, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "data:") {
			continue
		}
		var event struct {
			Type           string `json:"type"`
			SequenceNumber int    `json:"sequence_number"`
		}
		if err := json.Unmarshal([]byte(strings.TrimSpace(strings.TrimPrefix(line, "data:"))), &event); err == nil && event.Type == "response.incomplete" {
			return event.SequenceNumber
		}
	}
	t.Fatalf("response.incomplete event not found: %s", body)
	return -1
}

func incompleteOutputItemID(t *testing.T, body string) string {
	t.Helper()
	for _, line := range strings.Split(body, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "data:") {
			continue
		}
		var event struct {
			Type     string `json:"type"`
			Response struct {
				Output []struct {
					ID string `json:"id"`
				} `json:"output"`
			} `json:"response"`
		}
		if err := json.Unmarshal([]byte(strings.TrimSpace(strings.TrimPrefix(line, "data:"))), &event); err == nil && event.Type == "response.incomplete" {
			if len(event.Response.Output) == 0 {
				t.Fatalf("response.incomplete has no output: %s", body)
			}
			return event.Response.Output[0].ID
		}
	}
	t.Fatalf("response.incomplete event not found: %s", body)
	return ""
}
