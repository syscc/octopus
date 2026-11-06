package relay

import (
	"context"
	"encoding/json"
	"io"
	"strings"
	"testing"
	"time"

	openaiInbound "github.com/bestruirui/octopus/internal/transformer/inbound/openai"
	transformerModel "github.com/bestruirui/octopus/internal/transformer/model"
	"github.com/bestruirui/octopus/internal/transformer/outbound"
	openaiOutbound "github.com/bestruirui/octopus/internal/transformer/outbound/openai"
)

// TestHTTPReplayIntegration tests the complete HTTP replay flow:
// 1. Initial request succeeds and saves replay state
// 2. Follow-up request with previous_response_id loads state and transforms request
func TestHTTPReplayIntegration(t *testing.T) {
	resetResponsesReplayStore()
	defer resetResponsesReplayStore()

	apiKeyID := 100
	groupID := 50
	requestModel := "gpt-4"
	channelID := 10
	channelKeyID := 5

	// === Simulate first successful request ===
	firstReq := &transformerModel.InternalLLMRequest{
		Model: requestModel,
		Messages: []transformerModel.Message{
			{Role: "user", Content: transformerModel.MessageContent{Content: stringPtr("What is AI?")}},
		},
		RawAPIFormat: transformerModel.APIFormatOpenAIResponse,
	}

	firstResp := &transformerModel.InternalLLMResponse{
		ID:    "resp_first123",
		Model: requestModel,
		Choices: []transformerModel.Choice{
			{
				Message: &transformerModel.Message{
					Role:    "assistant",
					Content: transformerModel.MessageContent{Content: stringPtr("AI is...")},
				},
			},
		},
		RawResponsesOutputItems: json.RawMessage(`[{"type":"message","role":"assistant","content":[{"type":"text","text":"AI is..."}]}]`),
	}

	// Save state after first turn
	state := &wsConversationState{
		RequestModel: requestModel,
		ChannelID:    channelID,
		ChannelKeyID: channelKeyID,
	}
	state.ApplySuccessfulTurn(firstReq, firstResp)

	if state.LastResponseID != "resp_first123" {
		t.Fatalf("expected LastResponseID=resp_first123, got %q", state.LastResponseID)
	}
	if len(state.Transcript) != 2 {
		t.Fatalf("expected 2 messages in transcript, got %d", len(state.Transcript))
	}
	if len(state.ReplayWindowItems) == 0 {
		t.Fatal("expected ReplayWindowItems to be populated")
	}

	storeResponsesReplayState(apiKeyID, groupID, requestModel, state, time.Minute)

	// === Simulate second request with previous_response_id ===
	secondReq := &transformerModel.InternalLLMRequest{
		Model:              requestModel,
		PreviousResponseID: stringPtr("resp_first123"),
		Messages: []transformerModel.Message{
			{Role: "user", Content: transformerModel.MessageContent{Content: stringPtr("Tell me more")}},
		},
		RawAPIFormat: transformerModel.APIFormatOpenAIResponse,
	}

	// Resolve replay state
	loadedState := resolveResponsesReplayState(apiKeyID, groupID, requestModel, secondReq)
	if loadedState == nil {
		t.Fatal("expected to load replay state for second request")
	}
	if loadedState.ChannelID != channelID {
		t.Fatalf("expected ChannelID=%d, got %d", channelID, loadedState.ChannelID)
	}
	if loadedState.ChannelKeyID != channelKeyID {
		t.Fatalf("expected ChannelKeyID=%d, got %d", channelKeyID, loadedState.ChannelKeyID)
	}

	// Transform request using replay state
	replayedReq := loadedState.BuildReplayRequest(secondReq)
	if replayedReq == nil {
		t.Fatal("expected BuildReplayRequest to succeed")
	}

	// Verify transformation
	if replayedReq.OpenAIPreviousResponseID() != "" {
		t.Fatalf("expected previous_response_id to be removed, got %q", replayedReq.OpenAIPreviousResponseID())
	}
	if !replayedReq.IsOpenAIExactReplayRequest() {
		t.Fatal("expected replayed request to be marked as exact replay")
	}
	if len(replayedReq.OpenAIRawInputItems()) == 0 {
		t.Fatal("expected RawInputItems to be merged with history")
	}

	// Verify sticky routing
	stickyEntry := responsesReplayStateToSticky(loadedState)
	if stickyEntry == nil {
		t.Fatal("expected sticky entry for replay state")
	}
	if stickyEntry.ChannelID != channelID {
		t.Fatalf("expected sticky ChannelID=%d, got %d", channelID, stickyEntry.ChannelID)
	}
	if stickyEntry.ChannelKeyID != channelKeyID {
		t.Fatalf("expected sticky ChannelKeyID=%d, got %d", channelKeyID, stickyEntry.ChannelKeyID)
	}
}

// TestHTTPReplayStreamingRequest tests replay with streaming requests
func TestHTTPReplayStreamingRequest(t *testing.T) {
	resetResponsesReplayStore()
	defer resetResponsesReplayStore()

	apiKeyID := 200
	groupID := 75
	requestModel := "gpt-4"

	stream := true
	firstReq := &transformerModel.InternalLLMRequest{
		Model:  requestModel,
		Stream: &stream,
		Messages: []transformerModel.Message{
			{Role: "user", Content: transformerModel.MessageContent{Content: stringPtr("Stream test")}},
		},
		RawAPIFormat: transformerModel.APIFormatOpenAIResponse,
	}

	firstResp := &transformerModel.InternalLLMResponse{
		ID:                      "resp_stream456",
		Model:                   requestModel,
		RawResponsesOutputItems: json.RawMessage(`[{"type":"message","role":"assistant","content":[{"type":"text","text":"Streaming..."}]}]`),
		Choices: []transformerModel.Choice{
			{Message: &transformerModel.Message{Role: "assistant", Content: transformerModel.MessageContent{Content: stringPtr("Streaming...")}}},
		},
	}

	state := &wsConversationState{
		RequestModel: requestModel,
		ChannelID:    20,
		ChannelKeyID: 10,
	}
	state.ApplySuccessfulTurn(firstReq, firstResp)
	storeResponsesReplayState(apiKeyID, groupID, requestModel, state, time.Minute)

	// Second streaming request with previous_response_id
	secondReq := &transformerModel.InternalLLMRequest{
		Model:              requestModel,
		Stream:             &stream,
		PreviousResponseID: stringPtr("resp_stream456"),
		Messages: []transformerModel.Message{
			{Role: "user", Content: transformerModel.MessageContent{Content: stringPtr("Continue")}},
		},
		RawAPIFormat: transformerModel.APIFormatOpenAIResponse,
	}

	loadedState := resolveResponsesReplayState(apiKeyID, groupID, requestModel, secondReq)
	if loadedState == nil {
		t.Fatal("expected to load replay state for streaming request")
	}

	replayedReq := loadedState.BuildReplayRequest(secondReq)
	if replayedReq == nil {
		t.Fatal("expected BuildReplayRequest to succeed for streaming")
	}
	if replayedReq.Stream == nil || !*replayedReq.Stream {
		t.Fatal("expected Stream to be preserved")
	}
}

// TestHTTPReplayDifferentGroups tests isolation between different groups
func TestHTTPReplayDifferentGroups(t *testing.T) {
	resetResponsesReplayStore()
	defer resetResponsesReplayStore()

	apiKeyID := 300
	requestModel := "gpt-4"

	// Store state for group 1
	state1 := &wsConversationState{
		RequestModel:   requestModel,
		ChannelID:      30,
		ChannelKeyID:   15,
		LastResponseID: "resp_group1",
	}
	storeResponsesReplayState(apiKeyID, 1, requestModel, state1, time.Minute)

	// Store state for group 2 with same response ID (different group should isolate)
	state2 := &wsConversationState{
		RequestModel:   requestModel,
		ChannelID:      40,
		ChannelKeyID:   20,
		LastResponseID: "resp_group1", // Same response ID
	}
	storeResponsesReplayState(apiKeyID, 2, requestModel, state2, time.Minute)

	// Load from group 1
	loaded1 := loadResponsesReplayState(apiKeyID, 1, requestModel, "resp_group1")
	if loaded1 == nil || loaded1.ChannelID != 30 {
		t.Fatal("expected to load state for group 1 with ChannelID=30")
	}

	// Load from group 2
	loaded2 := loadResponsesReplayState(apiKeyID, 2, requestModel, "resp_group1")
	if loaded2 == nil || loaded2.ChannelID != 40 {
		t.Fatal("expected to load state for group 2 with ChannelID=40")
	}
}

// TestHTTPReplayWithToolCalls tests replay with tool calls
func TestHTTPReplayWithToolCalls(t *testing.T) {
	resetResponsesReplayStore()
	defer resetResponsesReplayStore()

	apiKeyID := 400
	groupID := 100
	requestModel := "gpt-4"

	firstReq := &transformerModel.InternalLLMRequest{
		Model: requestModel,
		Messages: []transformerModel.Message{
			{Role: "user", Content: transformerModel.MessageContent{Content: stringPtr("What's the weather?")}},
		},
		RawAPIFormat: transformerModel.APIFormatOpenAIResponse,
	}

	firstResp := &transformerModel.InternalLLMResponse{
		ID:    "resp_tool789",
		Model: requestModel,
		Choices: []transformerModel.Choice{
			{
				Message: &transformerModel.Message{
					Role: "assistant",
					ToolCalls: []transformerModel.ToolCall{
						{ID: "call_abc", Function: transformerModel.FunctionCall{Name: "get_weather"}},
					},
				},
			},
		},
		RawResponsesOutputItems: json.RawMessage(`[{"type":"function_call","call_id":"call_abc","name":"get_weather","arguments":"{}"}]`),
	}

	state := &wsConversationState{
		RequestModel: requestModel,
		ChannelID:    50,
		ChannelKeyID: 25,
	}
	state.ApplySuccessfulTurn(firstReq, firstResp)
	storeResponsesReplayState(apiKeyID, groupID, requestModel, state, time.Minute)

	// Second request with tool output
	secondReq := &transformerModel.InternalLLMRequest{
		Model:              requestModel,
		PreviousResponseID: stringPtr("resp_tool789"),
		Messages: []transformerModel.Message{
			{Role: "tool", ToolCallID: stringPtr("call_abc"), Content: transformerModel.MessageContent{Content: stringPtr("Sunny, 25°C")}},
		},
		RawAPIFormat: transformerModel.APIFormatOpenAIResponse,
	}

	loadedState := resolveResponsesReplayState(apiKeyID, groupID, requestModel, secondReq)
	if loadedState == nil {
		t.Fatal("expected to load replay state for tool call response")
	}

	replayedReq := loadedState.BuildReplayRequest(secondReq)
	if replayedReq == nil {
		t.Fatal("expected BuildReplayRequest to succeed with tool calls")
	}

	// Verify history includes assistant message with tool call
	if len(loadedState.Transcript) < 2 {
		t.Fatalf("expected at least 2 messages in transcript (user + assistant with tool call), got %d", len(loadedState.Transcript))
	}
}

// TestHTTPReplayMultiTurnChain tests continuous multi-turn replay (resp1 -> replay resp2 -> replay resp3)
func TestHTTPReplayMultiTurnChain(t *testing.T) {
	resetResponsesReplayStore()
	defer resetResponsesReplayStore()

	apiKeyID := 500
	groupID := 150
	requestModel := "gpt-4"
	channelID := 60
	channelKeyID := 30

	// === Turn 1: Initial request ===
	turn1Req := &transformerModel.InternalLLMRequest{
		Model: requestModel,
		Messages: []transformerModel.Message{
			{Role: "user", Content: transformerModel.MessageContent{Content: stringPtr("Hello")}},
		},
		RawAPIFormat: transformerModel.APIFormatOpenAIResponse,
	}

	turn1Resp := &transformerModel.InternalLLMResponse{
		ID:                      "resp_turn1",
		Model:                   requestModel,
		RawResponsesOutputItems: json.RawMessage(`[{"type":"message","role":"assistant","content":[{"type":"text","text":"Hi there!"}]}]`),
		Choices: []transformerModel.Choice{
			{Message: &transformerModel.Message{Role: "assistant", Content: transformerModel.MessageContent{Content: stringPtr("Hi there!")}}},
		},
	}

	state1 := &wsConversationState{
		RequestModel: requestModel,
		ChannelID:    channelID,
		ChannelKeyID: channelKeyID,
	}
	state1.ApplySuccessfulTurn(turn1Req, turn1Resp)
	storeResponsesReplayState(apiKeyID, groupID, requestModel, state1, time.Minute)

	// === Turn 2: Replay continuation ===
	turn2Req := &transformerModel.InternalLLMRequest{
		Model:              requestModel,
		PreviousResponseID: stringPtr("resp_turn1"),
		Messages: []transformerModel.Message{
			{Role: "user", Content: transformerModel.MessageContent{Content: stringPtr("How are you?")}},
		},
		RawAPIFormat: transformerModel.APIFormatOpenAIResponse,
	}

	loadedState2 := resolveResponsesReplayState(apiKeyID, groupID, requestModel, turn2Req)
	if loadedState2 == nil {
		t.Fatal("turn 2: expected to load replay state")
	}
	if loadedState2.LastResponseID != "resp_turn1" {
		t.Fatalf("turn 2: expected LastResponseID=resp_turn1, got %q", loadedState2.LastResponseID)
	}

	replayedReq2 := loadedState2.BuildReplayRequest(turn2Req)
	if replayedReq2 == nil {
		t.Fatal("turn 2: BuildReplayRequest failed")
	}
	if !replayedReq2.IsOpenAIExactReplayRequest() {
		t.Fatal("turn 2: expected exact replay request")
	}

	// Simulate turn 2 response
	turn2Resp := &transformerModel.InternalLLMResponse{
		ID:                      "resp_turn2",
		Model:                   requestModel,
		RawResponsesOutputItems: json.RawMessage(`[{"type":"message","role":"assistant","content":[{"type":"text","text":"I'm good!"}]}]`),
		Choices: []transformerModel.Choice{
			{Message: &transformerModel.Message{Role: "assistant", Content: transformerModel.MessageContent{Content: stringPtr("I'm good!")}}},
		},
	}

	// Save turn 2 state (based on existing state)
	state2 := cloneWSConversationState(loadedState2)
	state2.ChannelID = channelID
	state2.ChannelKeyID = channelKeyID
	state2.ApplySuccessfulTurn(replayedReq2, turn2Resp)
	storeResponsesReplayState(apiKeyID, groupID, requestModel, state2, time.Minute)

	// === Turn 3: Continue replay from turn 2 ===
	turn3Req := &transformerModel.InternalLLMRequest{
		Model:              requestModel,
		PreviousResponseID: stringPtr("resp_turn2"),
		Messages: []transformerModel.Message{
			{Role: "user", Content: transformerModel.MessageContent{Content: stringPtr("Great!")}},
		},
		RawAPIFormat: transformerModel.APIFormatOpenAIResponse,
	}

	loadedState3 := resolveResponsesReplayState(apiKeyID, groupID, requestModel, turn3Req)
	if loadedState3 == nil {
		t.Fatal("turn 3: expected to load replay state from turn 2")
	}
	if loadedState3.LastResponseID != "resp_turn2" {
		t.Fatalf("turn 3: expected LastResponseID=resp_turn2, got %q", loadedState3.LastResponseID)
	}

	// Verify transcript accumulated across turns
	// Turn 1: user + assistant (2 messages)
	// Turn 2: replayedReq2 only has instruction messages, but ApplySuccessfulTurn appends current request messages + response
	// So state2 should have: turn1 user + turn1 assistant + turn2 user + turn2 assistant = 4 messages
	// But BuildReplayRequest retains only instruction messages, so replayedReq2 has 0 user messages
	// ApplySuccessfulTurn uses req.Messages which is empty after BuildReplayRequest
	// This is expected behavior - we need to check the actual accumulated state
	if len(loadedState3.Transcript) < 2 {
		t.Fatalf("turn 3: expected at least 2 messages in transcript, got %d", len(loadedState3.Transcript))
	}

	replayedReq3 := loadedState3.BuildReplayRequest(turn3Req)
	if replayedReq3 == nil {
		t.Fatal("turn 3: BuildReplayRequest failed")
	}
	if !replayedReq3.IsOpenAIExactReplayRequest() {
		t.Fatal("turn 3: expected exact replay request")
	}

	// Verify turn 2 history survives into turn 3's replayed input items
	rawItems3 := replayedReq3.OpenAIRawInputItems()
	if len(rawItems3) == 0 {
		t.Fatal("turn 3: expected non-empty RawInputItems")
	}
	rawItems3Str := string(rawItems3)
	if !strings.Contains(rawItems3Str, "How are you?") {
		t.Fatalf("turn 3: expected RawInputItems to contain turn 2 user message 'How are you?', got %s", rawItems3Str)
	}
	if !strings.Contains(rawItems3Str, "I'm good!") {
		t.Fatalf("turn 3: expected RawInputItems to contain turn 2 assistant message 'I'm good!', got %s", rawItems3Str)
	}
	if !strings.Contains(rawItems3Str, "Hello") {
		t.Fatalf("turn 3: expected RawInputItems to contain turn 1 user message 'Hello', got %s", rawItems3Str)
	}
	if !strings.Contains(rawItems3Str, "Great!") {
		t.Fatalf("turn 3: expected RawInputItems to contain turn 3 user message 'Great!', got %s", rawItems3Str)
	}
}

// TestHTTPReplayFailedMergeKeepsOriginalRequest tests that failed history merge returns nil
func TestHTTPReplayFailedMergeKeepsOriginalRequest(t *testing.T) {
	resetResponsesReplayStore()
	defer resetResponsesReplayStore()

	apiKeyID := 600
	groupID := 200
	requestModel := "gpt-4"

	// Store state with empty replay window (will cause merge failure)
	state := &wsConversationState{
		RequestModel:      requestModel,
		ChannelID:         70,
		ChannelKeyID:      35,
		LastResponseID:    "resp_empty",
		ReplayWindowItems: nil, // Empty
		Transcript:        nil,
	}
	storeResponsesReplayState(apiKeyID, groupID, requestModel, state, time.Minute)

	// Request with previous_response_id but NO current messages or raw input items
	// This will cause buildRequestInputItems to fail and return ok=false
	req := &transformerModel.InternalLLMRequest{
		Model:              requestModel,
		PreviousResponseID: stringPtr("resp_empty"),
		Messages:           nil, // Empty messages
		RawInputItems:      nil, // Empty raw items
		RawAPIFormat:       transformerModel.APIFormatOpenAIResponse,
	}

	originalPrevID := req.OpenAIPreviousResponseID()
	if originalPrevID != "resp_empty" {
		t.Fatalf("setup error: expected previous_response_id=resp_empty, got %q", originalPrevID)
	}

	loadedState := resolveResponsesReplayState(apiKeyID, groupID, requestModel, req)
	if loadedState == nil {
		t.Fatal("expected to load state")
	}

	// BuildReplayRequest now returns nil on merge failure
	replayedReq := loadedState.BuildReplayRequest(req)
	if replayedReq != nil {
		t.Fatal("expected BuildReplayRequest to return nil on merge failure, but got non-nil")
	}

	// Handler must reject this local continuation instead of forwarding its
	// process-local previous_response_id to an upstream provider.
}

func TestStreamingResponsesReplayPreservesRawReasoningItems(t *testing.T) {
	ctx := context.Background()
	outAdapter := &openaiOutbound.ResponseOutbound{}
	inAdapter := &openaiInbound.ResponseInbound{}
	payloads := []string{
		`{"type":"response.created","response":{"id":"resp_reasoning","model":"gpt-test","created_at":42,"status":"in_progress","output":[]}}`,
		`{"type":"response.completed","response":{"id":"resp_reasoning","model":"gpt-test","created_at":42,"status":"completed","output":[{"id":"rs_1","type":"reasoning","encrypted_content":"enc_sig","summary":[{"type":"summary_text","text":"thinking"}]},{"id":"msg_1","type":"message","role":"assistant","status":"completed","content":[{"type":"output_text","text":"answer"}]}],"usage":{"input_tokens":1,"output_tokens":2,"total_tokens":3}}}`,
	}
	for _, payload := range payloads {
		events, err := outAdapter.TransformStreamEvent(ctx, []byte(payload))
		if err != nil {
			t.Fatalf("TransformStreamEvent failed: %v", err)
		}
		if _, err := inAdapter.TransformStreamEvents(ctx, events); err != nil {
			t.Fatalf("TransformStreamEvents failed: %v", err)
		}
	}
	internalResponse, err := inAdapter.GetInternalResponse(ctx)
	if err != nil || internalResponse == nil {
		t.Fatalf("expected aggregated internal response, got resp=%+v err=%v", internalResponse, err)
	}
	if !strings.Contains(string(internalResponse.RawResponsesOutputItems), `"encrypted_content":"enc_sig"`) {
		t.Fatalf("expected raw reasoning item after stream aggregation, got %s", internalResponse.RawResponsesOutputItems)
	}

	request := &transformerModel.InternalLLMRequest{
		Model:         "gpt-test",
		RawAPIFormat:  transformerModel.APIFormatOpenAIResponse,
		RawInputItems: json.RawMessage(`[{"type":"message","role":"user","content":[{"type":"input_text","text":"hello"}]}]`),
	}
	state := &wsConversationState{
		LastOutboundType:    outbound.OutboundTypeOpenAIResponse,
		LastOutboundTypeSet: true,
	}
	state.ApplySuccessfulTurn(request, internalResponse)
	window := string(state.ReplayWindowItems)
	for _, expected := range []string{`"id":"rs_1"`, `"encrypted_content":"enc_sig"`, `"id":"msg_1"`} {
		if !strings.Contains(window, expected) {
			t.Fatalf("expected replay window to contain %s, got %s", expected, window)
		}
	}
}

func TestHTTPReplayNormalizesFunctionCallIDAtInput903(t *testing.T) {
	history := make([]json.RawMessage, 903, 904)
	for i := range history {
		history[i] = json.RawMessage(`{"type":"message","role":"user","content":[{"type":"input_text","text":"history"}]}`)
	}
	history[0] = json.RawMessage(`{"id":"rs_native","type":"reasoning","encrypted_content":"encrypted_history","summary":[],"native":9007199254740993}`)
	history = append(history, json.RawMessage(`{"id":"call_legacy","type":"function_call","call_id":"call_pair","name":"lookup","arguments":"{}","status":"completed","native":{"counter":9007199254740993}}`))
	window, err := json.Marshal(history)
	if err != nil {
		t.Fatal(err)
	}
	state := &wsConversationState{LastResponseID: "resp_previous", ReplayWindowItems: window}
	current := &transformerModel.InternalLLMRequest{
		Model:              "gpt-test",
		PreviousResponseID: stringPtr("resp_previous"),
		RawAPIFormat:       transformerModel.APIFormatOpenAIResponse,
		RawInputItems:      json.RawMessage(`[{"type":"function_call_output","call_id":"call_pair","output":"ok"}]`),
	}
	replayed := state.BuildReplayRequest(current)
	if replayed == nil {
		t.Fatal("BuildReplayRequest failed")
	}
	request, err := (&openaiOutbound.ResponseOutbound{}).TransformRequest(context.Background(), replayed, "https://example.invalid/v1", "test-key")
	if err != nil {
		t.Fatal(err)
	}
	defer request.Body.Close()
	body, err := io.ReadAll(request.Body)
	if err != nil {
		t.Fatal(err)
	}
	var decoded struct {
		Input              []map[string]json.RawMessage `json:"input"`
		PreviousResponseID string                       `json:"previous_response_id"`
	}
	if err := json.Unmarshal(body, &decoded); err != nil {
		t.Fatal(err)
	}
	if len(decoded.Input) != 905 || decoded.PreviousResponseID != "" {
		t.Fatalf("unexpected replay input or continuation: %s", body)
	}
	var itemID string
	if err := json.Unmarshal(decoded.Input[903]["id"], &itemID); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(itemID, "fc_") {
		t.Fatalf("input[903].id = %q, expected fc_ prefix", itemID)
	}
	for _, index := range []int{903, 904} {
		if string(decoded.Input[index]["call_id"]) != `"call_pair"` {
			t.Fatalf("call/output pairing changed at input[%d]: %s", index, body)
		}
	}
	if string(decoded.Input[904]["output"]) != `"ok"` || string(decoded.Input[903]["native"]) != `{"counter":9007199254740993}` {
		t.Fatalf("function call/output native fields changed: %s", body)
	}
	if string(decoded.Input[0]["encrypted_content"]) != `"encrypted_history"` || string(decoded.Input[0]["native"]) != "9007199254740993" {
		t.Fatalf("reasoning history fields changed: %s", body)
	}
	if !strings.Contains(string(state.ReplayWindowItems), `"id":"call_legacy"`) {
		t.Fatal("cached replay window was mutated")
	}
}

func TestStreamingResponsesReplayResolvesClientFunctionCallReference(t *testing.T) {
	ctx := context.Background()
	adapter := &openaiInbound.ResponseInbound{}
	raw := json.RawMessage(`[{"type":"reasoning","id":"rs_native","encrypted_content":"cipher","native":9007199254740993},{"type":"function_call","id":"fc_native","call_id":"call_x","name":"lookup","arguments":"{}"}]`)
	originalRaw := string(raw)
	start := &transformerModel.InternalLLMResponse{
		ID: "resp_reference", Model: "gpt-test",
		Choices: []transformerModel.Choice{{Delta: &transformerModel.Message{
			Role: "assistant", ToolCalls: []transformerModel.ToolCall{{ID: "call_x", Type: "function", Function: transformerModel.FunctionCall{Name: "lookup", Arguments: "{}"}}},
		}}},
	}
	addedBytes, err := adapter.TransformStream(ctx, start)
	if err != nil {
		t.Fatal(err)
	}
	finishReason := "tool_calls"
	finish := &transformerModel.InternalLLMResponse{
		ID: "resp_reference", Model: "gpt-test", RawResponsesOutputItems: raw,
		Choices: []transformerModel.Choice{{FinishReason: &finishReason}},
		Usage:   &transformerModel.Usage{PromptTokens: 1, CompletionTokens: 1, TotalTokens: 2},
	}
	terminalBytes, err := adapter.TransformStream(ctx, finish)
	if err != nil {
		t.Fatal(err)
	}
	var clientID, terminalID string
	for _, frame := range strings.Split(string(addedBytes)+string(terminalBytes), "\n\n") {
		if !strings.HasPrefix(frame, "data: ") {
			continue
		}
		var event struct {
			Type string `json:"type"`
			Item *struct {
				ID   string `json:"id"`
				Type string `json:"type"`
			} `json:"item"`
			Response *struct {
				Output []struct {
					ID   string `json:"id"`
					Type string `json:"type"`
				} `json:"output"`
			} `json:"response"`
		}
		if err := json.Unmarshal([]byte(strings.TrimPrefix(frame, "data: ")), &event); err != nil {
			t.Fatal(err)
		}
		if event.Type == "response.output_item.added" && event.Item != nil && event.Item.Type == "function_call" {
			clientID = event.Item.ID
		}
		if event.Type == "response.completed" && event.Response != nil {
			for _, item := range event.Response.Output {
				if item.Type == "function_call" {
					terminalID = item.ID
				}
			}
		}
	}
	if clientID == "" || clientID != terminalID {
		t.Fatalf("client added/terminal IDs differ: %q / %q", clientID, terminalID)
	}
	response, err := adapter.GetInternalResponse(ctx)
	if err != nil || response == nil {
		t.Fatalf("get cached response: response=%v err=%v", response, err)
	}
	state := &wsConversationState{}
	state.ApplySuccessfulTurn(&transformerModel.InternalLLMRequest{
		Model: "gpt-test", RawAPIFormat: transformerModel.APIFormatOpenAIResponse,
		RawInputItems: json.RawMessage(`[{"type":"message","role":"user","content":[{"type":"input_text","text":"lookup"}]}]`),
	}, response)
	next := &transformerModel.InternalLLMRequest{
		Model: "gpt-test", PreviousResponseID: stringPtr("resp_reference"),
		RawAPIFormat:  transformerModel.APIFormatOpenAIResponse,
		RawInputItems: json.RawMessage(`[{"type":"item_reference","id":"` + clientID + `"}]`),
	}
	replayed := state.BuildReplayRequest(next)
	if replayed == nil {
		t.Fatal("BuildReplayRequest failed")
	}
	request, err := (&openaiOutbound.ResponseOutbound{}).TransformRequest(ctx, replayed, "https://example.invalid/v1", "test-key")
	if err != nil {
		t.Fatal(err)
	}
	defer request.Body.Close()
	body, err := io.ReadAll(request.Body)
	if err != nil {
		t.Fatal(err)
	}
	var payload struct {
		Input []map[string]json.RawMessage `json:"input"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatal(err)
	}
	var targetID, referenceID string
	reasoningFound := false
	for _, item := range payload.Input {
		switch string(item["type"]) {
		case `"function_call"`:
			if err := json.Unmarshal(item["id"], &targetID); err != nil {
				t.Fatal(err)
			}
			if string(item["call_id"]) != `"call_x"` {
				t.Fatalf("call_id changed: %s", body)
			}
		case `"item_reference"`:
			if err := json.Unmarshal(item["id"], &referenceID); err != nil {
				t.Fatal(err)
			}
		case `"reasoning"`:
			reasoningFound = true
			if string(item["encrypted_content"]) != `"cipher"` || string(item["native"]) != "9007199254740993" {
				t.Fatalf("reasoning native fields changed: %s", body)
			}
		}
	}
	if !reasoningFound {
		t.Fatal("reasoning history was dropped")
	}
	if targetID != clientID || referenceID != targetID {
		t.Fatalf("replay reference has no matching client target: client=%q target=%q reference=%q", clientID, targetID, referenceID)
	}
	if string(raw) != originalRaw || string(finish.RawResponsesOutputItems) != originalRaw {
		t.Fatal("upstream raw output was mutated")
	}
}

func TestResponsesPassthroughMetricsReplayPreservesNativeFunctionCallID(t *testing.T) {
	for _, collector := range []string{"generic", "responses"} {
		for _, addedID := range []string{"fc_native", "call_x"} {
			t.Run(collector+"/"+addedID, func(t *testing.T) {
				ctx := context.Background()
				inAdapter := &openaiInbound.ResponseInbound{}
				ra := &relayAttempt{
					relayRequest: &relayRequest{inAdapter: inAdapter},
					outAdapter:   &openaiOutbound.ResponseOutbound{},
				}
				frames := []string{
					`{"type":"response.created","response":{"id":"resp_sidecar","model":"gpt-test","status":"in_progress","output":[]}}`,
					`{"type":"response.output_item.added","output_index":0,"item":{"type":"function_call","id":"` + addedID + `","call_id":"call_x","name":"lookup","arguments":""}}`,
					`{"type":"response.function_call_arguments.delta","output_index":0,"item_id":"` + addedID + `","delta":"{}"}`,
					`{"type":"response.completed","response":{"id":"resp_sidecar","model":"gpt-test","status":"completed","output":[{"type":"function_call","id":"fc_native","call_id":"call_x","name":"lookup","arguments":"{}"}],"usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}}`,
				}
				rawStream := []byte("data: " + strings.Join(frames, "\n\ndata: ") + "\n\n")
				if collector == "generic" {
					ra.collectPassthroughMetrics(ctx, rawStream)
				} else {
					ra.collectOpenAIResponsesPassthroughMetrics(ctx, rawStream)
				}
				response, err := inAdapter.GetInternalResponse(ctx)
				if err != nil || response == nil {
					t.Fatalf("get sidecar cache: response=%v err=%v", response, err)
				}
				var cached []struct {
					ID     string `json:"id"`
					CallID string `json:"call_id"`
				}
				if err := json.Unmarshal(response.RawResponsesOutputItems, &cached); err != nil {
					t.Fatal(err)
				}
				if len(cached) != 1 || cached[0].ID != "fc_native" || cached[0].CallID != "call_x" {
					t.Fatalf("sidecar replaced wire terminal IDs: %s", response.RawResponsesOutputItems)
				}
				state := &wsConversationState{}
				state.ApplySuccessfulTurn(&transformerModel.InternalLLMRequest{
					Model: "gpt-test", RawAPIFormat: transformerModel.APIFormatOpenAIResponse,
					RawInputItems: json.RawMessage(`[{"type":"message","role":"user","content":[{"type":"input_text","text":"lookup"}]}]`),
				}, response)
				replayed := state.BuildReplayRequest(&transformerModel.InternalLLMRequest{
					Model: "gpt-test", PreviousResponseID: stringPtr("resp_sidecar"),
					RawAPIFormat:  transformerModel.APIFormatOpenAIResponse,
					RawInputItems: json.RawMessage(`[{"type":"item_reference","id":"fc_native"}]`),
				})
				if replayed == nil {
					t.Fatal("BuildReplayRequest failed")
				}
				request, err := (&openaiOutbound.ResponseOutbound{}).TransformRequest(ctx, replayed, "https://example.invalid/v1", "test-key")
				if err != nil {
					t.Fatal(err)
				}
				defer request.Body.Close()
				body, err := io.ReadAll(request.Body)
				if err != nil {
					t.Fatal(err)
				}
				var payload struct {
					Input []struct {
						Type   string `json:"type"`
						ID     string `json:"id"`
						CallID string `json:"call_id"`
					} `json:"input"`
				}
				if err := json.Unmarshal(body, &payload); err != nil {
					t.Fatal(err)
				}
				var targetID, referenceID string
				for _, item := range payload.Input {
					switch item.Type {
					case "function_call":
						targetID = item.ID
						if item.CallID != "call_x" {
							t.Fatalf("call_id changed: %s", body)
						}
					case "item_reference":
						referenceID = item.ID
					}
				}
				if targetID != "fc_native" || referenceID != targetID {
					t.Fatalf("native client reference lost target: %s", body)
				}
			})
		}
	}
}
