package relay

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/bestruirui/octopus/internal/model"
	"github.com/bestruirui/octopus/internal/op"
	"github.com/bestruirui/octopus/internal/transformer/inbound"
	transformerModel "github.com/bestruirui/octopus/internal/transformer/model"
	"github.com/bestruirui/octopus/internal/transformer/outbound"
	"github.com/gin-gonic/gin"
)

func TestResponsesFallbackResetsToResponsesForNextCandidate(t *testing.T) {
	gin.SetMode(gin.TestMode)
	ctx := setupRelayTestDB(t)

	var firstMu sync.Mutex
	firstPaths := make([]string, 0, 2)
	firstServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		firstMu.Lock()
		firstPaths = append(firstPaths, r.URL.Path)
		firstMu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/v1/responses":
			w.WriteHeader(http.StatusBadRequest)
			_, _ = fmt.Fprint(w, `{"error":{"message":"responses unsupported","type":"invalid_request_error","code":"RESPONSES_MODEL_NOT_SUPPORTED"}}`)
		case "/v1/chat/completions":
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = fmt.Fprint(w, `{"error":{"message":"first channel unavailable"}}`)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(firstServer.Close)

	var secondMu sync.Mutex
	secondPaths := make([]string, 0, 1)
	secondServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		secondMu.Lock()
		secondPaths = append(secondPaths, r.URL.Path)
		secondMu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path != "/v1/responses" {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = fmt.Fprint(w, `{"error":{"message":"unexpected chat probe"}}`)
			return
		}
		_, _ = fmt.Fprint(w, `{"id":"resp_second","object":"response","status":"completed","model":"second-model","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"second-responses-ok"}]}],"usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}`)
	}))
	t.Cleanup(secondServer.Close)

	firstChannel := createChannel("candidate-reset-first", outbound.OutboundTypeOpenAIChat, firstServer.URL+"/v1")
	secondChannel := createChannel("candidate-reset-second", outbound.OutboundTypeOpenAIChat, secondServer.URL+"/v1")
	if err := op.ChannelCreate(firstChannel, ctx); err != nil {
		t.Fatalf("create first channel: %v", err)
	}
	if err := op.ChannelCreate(secondChannel, ctx); err != nil {
		t.Fatalf("create second channel: %v", err)
	}
	group := &model.Group{Name: "candidate-reset-group", Mode: model.GroupModeFailover}
	if err := op.GroupCreate(group, ctx); err != nil {
		t.Fatalf("create group: %v", err)
	}
	if err := op.GroupItemAdd(&model.GroupItem{GroupID: group.ID, ChannelID: firstChannel.ID, ModelName: "first-model", Priority: 1}, ctx); err != nil {
		t.Fatalf("add first group item: %v", err)
	}
	if err := op.GroupItemAdd(&model.GroupItem{GroupID: group.ID, ChannelID: secondChannel.ID, ModelName: "second-model", Priority: 2}, ctx); err != nil {
		t.Fatalf("add second group item: %v", err)
	}

	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"candidate-reset-group","input":"hello"}`))
	c.Request.Header.Set("Content-Type", "application/json")
	Handler(inbound.InboundTypeOpenAIResponse, c)

	if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), "second-responses-ok") {
		t.Fatalf("expected second candidate Responses success, status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	firstMu.Lock()
	gotFirstPaths := append([]string(nil), firstPaths...)
	firstMu.Unlock()
	secondMu.Lock()
	gotSecondPaths := append([]string(nil), secondPaths...)
	secondMu.Unlock()
	if len(gotFirstPaths) != 2 || gotFirstPaths[0] != "/v1/responses" || gotFirstPaths[1] != "/v1/chat/completions" {
		t.Fatalf("expected first candidate Responses then Chat, got %v", gotFirstPaths)
	}
	if len(gotSecondPaths) != 1 || gotSecondPaths[0] != "/v1/responses" {
		t.Fatalf("expected next candidate to reset to Responses, got %v", gotSecondPaths)
	}
}

func TestResponsesInboundUsageDoesNotLeakAcrossCandidates(t *testing.T) {
	gin.SetMode(gin.TestMode)
	ctx := setupRelayTestDB(t)

	first := newStreamingMockChannelServer(t, "/v1/chat/completions", strings.Join([]string{
		`data: {"id":"first-usage","object":"chat.completion.chunk","created":11,"model":"first-model","choices":[],"usage":{"prompt_tokens":777,"completion_tokens":333,"total_tokens":1110}}`,
		"",
	}, "\n"))
	second := newStreamingMockChannelServer(t, "/v1/chat/completions", strings.Join([]string{
		`data: {"id":"second","object":"chat.completion.chunk","created":22,"model":"second-model","choices":[{"index":0,"delta":{"role":"assistant","content":"ok"}}]}`,
		"",
		`data: {"id":"second","object":"chat.completion.chunk","created":22,"model":"second-model","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}`,
		"",
		`data: [DONE]`,
		"",
	}, "\n"))

	firstChannel := createChannel("usage-isolation-first", outbound.OutboundTypeOpenAIChat, first.url)
	secondChannel := createChannel("usage-isolation-second", outbound.OutboundTypeOpenAIChat, second.url)
	if err := op.ChannelCreate(firstChannel, ctx); err != nil {
		t.Fatalf("create first channel: %v", err)
	}
	if err := op.ChannelCreate(secondChannel, ctx); err != nil {
		t.Fatalf("create second channel: %v", err)
	}
	group := &model.Group{Name: "usage-isolation-group", Mode: model.GroupModeFailover}
	if err := op.GroupCreate(group, ctx); err != nil {
		t.Fatalf("create group: %v", err)
	}
	if err := op.GroupItemAdd(&model.GroupItem{GroupID: group.ID, ChannelID: firstChannel.ID, ModelName: "first-model", Priority: 1}, ctx); err != nil {
		t.Fatalf("add first group item: %v", err)
	}
	if err := op.GroupItemAdd(&model.GroupItem{GroupID: group.ID, ChannelID: secondChannel.ID, ModelName: "second-model", Priority: 2}, ctx); err != nil {
		t.Fatalf("add second group item: %v", err)
	}

	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"usage-isolation-group","input":"hello","stream":true}`))
	c.Request.Header.Set("Content-Type", "application/json")
	Handler(inbound.InboundTypeOpenAIResponse, c)

	body := recorder.Body.String()
	if recorder.Code != http.StatusOK || !strings.Contains(body, `"text":"ok"`) {
		t.Fatalf("expected second channel to complete, status=%d body=%s", recorder.Code, body)
	}
	for _, leaked := range []string{`"input_tokens":777`, `"output_tokens":333`, `"total_tokens":1110`} {
		if strings.Contains(body, leaked) {
			t.Fatalf("failed candidate usage leaked into successful response (%s): %s", leaked, body)
		}
	}
	if got := first.requestedPaths(); len(got) != 2 || got[0] != "/v1/responses" || got[1] != "/v1/chat/completions" {
		t.Fatalf("unexpected first candidate paths: %v", got)
	}
	if got := second.requestedPaths(); len(got) != 2 || got[0] != "/v1/responses" || got[1] != "/v1/chat/completions" {
		t.Fatalf("unexpected second candidate paths: %v", got)
	}
}

func TestBeginNetworkAttemptCreatesFreshResponsesInbound(t *testing.T) {
	calls := 0
	req := &relayRequest{
		inAdapter: inbound.Get(inbound.InboundTypeOpenAIResponse),
		newInboundAdapter: func() transformerModel.Inbound {
			calls++
			return inbound.Get(inbound.InboundTypeOpenAIResponse)
		},
	}
	truncation := "auto"
	internalRequest := &transformerModel.InternalLLMRequest{Truncation: &truncation}
	original := req.inAdapter
	if err := req.beginNetworkAttempt(internalRequest); err != nil {
		t.Fatalf("first beginNetworkAttempt failed: %v", err)
	}
	first := req.inAdapter
	if err := req.beginNetworkAttempt(internalRequest); err != nil {
		t.Fatalf("second beginNetworkAttempt failed: %v", err)
	}
	second := req.inAdapter
	if calls != 2 || first == original || second == first {
		t.Fatalf("expected a fresh adapter per network attempt, calls=%d original=%p first=%p second=%p", calls, original, first, second)
	}

	body, err := second.TransformResponse(context.Background(), &transformerModel.InternalLLMResponse{
		ID:     "resp_initializer",
		Object: "response",
		Model:  "test-model",
	})
	if err != nil {
		t.Fatalf("TransformResponse failed: %v", err)
	}
	if !strings.Contains(string(body), `"truncation":"auto"`) {
		t.Fatalf("fresh adapter did not retain request-bound truncation: %s", body)
	}
}
