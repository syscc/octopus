package model

import "testing"

// Issue #65: the choice rebuild loops iterated idx 0..len(map)-1 over maps
// keyed by event/choice index, silently dropping every choice stored at a
// non-contiguous index (e.g. tool calls at index >= 1) and — in
// InternalResponseFromStreamEvents — collapsing the whole aggregate to nil,
// which ChatInbound then dereferenced.

func TestInternalResponseFromStreamEventsKeepsNonContiguousChoiceIndices(t *testing.T) {
	events := []StreamEvent{
		{Kind: StreamEventKindTextDelta, ID: "resp_1", Model: "gpt-test", Index: 1, Delta: &StreamDelta{Text: "hello"}},
	}
	resp := InternalResponseFromStreamEvents(events)
	if resp == nil {
		t.Fatalf("expected non-nil response for events at Index=1, got nil")
	}
	if len(resp.Choices) != 1 || resp.Choices[0].Index != 1 {
		t.Fatalf("expected single choice preserving Index=1, got %+v", resp.Choices)
	}
	if resp.Choices[0].Delta == nil || resp.Choices[0].Delta.Content.Content == nil || *resp.Choices[0].Delta.Content.Content != "hello" {
		t.Fatalf("expected text delta to survive, got %+v", resp.Choices[0].Delta)
	}
}

func TestInternalResponseFromStreamEventsKeepsPayloadBeforeDone(t *testing.T) {
	text := "hello"
	resp := InternalResponseFromStreamEvents([]StreamEvent{
		{Kind: StreamEventKindTextDelta, ID: "resp_1", Model: "gpt-test", Delta: &StreamDelta{Text: text}},
		{Kind: StreamEventKindMessageStop, ID: "resp_1", Model: "gpt-test", StopReason: FinishReasonStop},
		{Kind: StreamEventKindUsageDelta, ID: "resp_1", Model: "gpt-test", Usage: &Usage{PromptTokens: 1, CompletionTokens: 2, TotalTokens: 3}},
		{Kind: StreamEventKindDone},
	})
	if resp == nil || resp.Object == "[DONE]" {
		t.Fatalf("expected payload response rather than done marker, got %+v", resp)
	}
	if resp.Usage == nil || resp.Usage.TotalTokens != 3 {
		t.Fatalf("expected usage to survive Done event, got %+v", resp.Usage)
	}
	if len(resp.Choices) != 1 || resp.Choices[0].FinishReason == nil || *resp.Choices[0].FinishReason != "stop" {
		t.Fatalf("expected terminal choice to survive Done event, got %+v", resp.Choices)
	}
}

func TestInternalResponseFromStreamEventsMapsStandaloneDone(t *testing.T) {
	resp := InternalResponseFromStreamEvents([]StreamEvent{{Kind: StreamEventKindDone}})
	if resp == nil || resp.Object != "[DONE]" {
		t.Fatalf("expected standalone Done marker, got %+v", resp)
	}
}

func TestSplitStreamEventsAtTerminalPreservesPrefixAndErrorPrecedence(t *testing.T) {
	errorEvent := StreamEvent{Kind: StreamEventKindError, Error: &ResponseError{Detail: ErrorDetail{Code: "failed"}}}
	prefix, gotError, done := SplitStreamEventsAtTerminal([]StreamEvent{
		{Kind: StreamEventKindTextDelta, Delta: &StreamDelta{Text: "prefix"}},
		{Kind: StreamEventKindDone},
		{Kind: StreamEventKindTextDelta, Delta: &StreamDelta{Text: "discard"}},
		errorEvent,
	})
	if len(prefix) != 1 || prefix[0].Kind != StreamEventKindTextDelta {
		t.Fatalf("expected only payload prefix before Done, got %+v", prefix)
	}
	if gotError == nil || gotError.Error == nil || gotError.Error.Detail.Code != "failed" {
		t.Fatalf("expected later error to be retained, got %+v", gotError)
	}
	if done {
		t.Fatal("error must prevent a successful Done result")
	}

	prefix, gotError, done = SplitStreamEventsAtTerminal([]StreamEvent{
		{Kind: StreamEventKindTextDelta, Delta: &StreamDelta{Text: "prefix"}},
		errorEvent,
		{Kind: StreamEventKindDone},
	})
	if len(prefix) != 1 || gotError == nil || done {
		t.Fatalf("expected error cutoff before later Done, prefix=%+v error=%+v done=%t", prefix, gotError, done)
	}
}

func TestInternalResponseFromStreamEventsStillNilForNoContent(t *testing.T) {

	tests := []struct {
		name   string
		events []StreamEvent
	}{
		{name: "empty slice", events: []StreamEvent{}},
		{name: "usage_delta with nil Usage", events: []StreamEvent{{Kind: StreamEventKindUsageDelta, Usage: nil}}},
		{name: "error event with nil Error", events: []StreamEvent{{Kind: StreamEventKindError, Error: nil}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if resp := InternalResponseFromStreamEvents(tt.events); resp != nil {
				t.Fatalf("expected nil response, got %+v", resp)
			}
		})
	}
}

// A chat chunk carrying a parallel tool call (tool_calls[].index=1 on choice 0)
// must produce events keyed by the choice index, not the tool call index —
// otherwise the round-trip rebuild re-homes the call into a phantom choice 1
// (or, before issue #65 was fixed, returns nil and panics ChatInbound).
func TestStreamEventsFromInternalResponseUsesChoiceIndexForToolCalls(t *testing.T) {
	chunk := &InternalLLMResponse{
		ID:      "chatcmpl-1",
		Object:  "chat.completion.chunk",
		Model:   "gpt-test",
		Created: 42,
		Choices: []Choice{
			{
				Index: 0,
				Delta: &Message{
					ToolCalls: []ToolCall{
						{Index: 1, ID: "call_2", Type: "function", Function: FunctionCall{Name: "get_weather", Arguments: `{"city":"sf"}`}},
					},
				},
			},
		},
	}

	events := StreamEventsFromInternalResponse(chunk)
	if len(events) == 0 {
		t.Fatalf("expected events, got none")
	}
	for _, ev := range events {
		if ev.Kind == StreamEventKindToolCallDelta && ev.Index != 0 {
			t.Fatalf("tool call event must carry choice index 0, got Index=%d", ev.Index)
		}
		if ev.Created != 42 {
			t.Fatalf("stream event creation time = %d, want 42", ev.Created)
		}
	}

	rebuilt := InternalResponseFromStreamEvents(events)
	if rebuilt == nil {
		t.Fatalf("round-trip returned nil")
	}
	if len(rebuilt.Choices) != 1 || rebuilt.Choices[0].Index != 0 {
		t.Fatalf("expected single choice 0 after round-trip, got %+v", rebuilt.Choices)
	}
	if rebuilt.Created != 42 {
		t.Fatalf("round-trip creation time = %d, want 42", rebuilt.Created)
	}
	toolCalls := rebuilt.Choices[0].Delta.ToolCalls
	if len(toolCalls) != 1 || toolCalls[0].Index != 1 || toolCalls[0].ID != "call_2" {
		t.Fatalf("expected tool call index=1 preserved on choice 0, got %+v", toolCalls)
	}
}

func TestStreamAggregatorKeepsNonContiguousChoiceIndices(t *testing.T) {
	text := "hi"
	var agg StreamAggregator
	agg.Add(&InternalLLMResponse{
		ID:     "chatcmpl-1",
		Object: "chat.completion.chunk",
		Model:  "gpt-test",
		Choices: []Choice{
			{Index: 1, Delta: &Message{Content: MessageContent{Content: &text}}},
		},
	})
	resp := agg.BuildAndReset()
	if resp == nil {
		t.Fatalf("expected aggregated response, got nil")
	}
	if len(resp.Choices) != 1 || resp.Choices[0].Index != 1 {
		t.Fatalf("expected choice at index 1 to survive aggregation, got %+v", resp.Choices)
	}
}

func TestStreamAggregatorPreservesResponsesReplayMetadata(t *testing.T) {
	reasoning := "thinking"
	signature := "encrypted-signature"
	rawItems := []byte(`[{"id":"rs_1","type":"reasoning","encrypted_content":"encrypted-signature","summary":[{"type":"summary_text","text":"thinking"}]}]`)
	var agg StreamAggregator
	agg.Add(&InternalLLMResponse{
		ID:      "resp_stream",
		Created: 42,
		Choices: []Choice{{Index: 0, Delta: &Message{
			ReasoningContent: &reasoning,
			ReasoningBlocks:  []ReasoningBlock{{Kind: ReasoningBlockKindThinking, Text: reasoning}},
		}}},
	})
	agg.Add(&InternalLLMResponse{
		ID:                      "resp_stream",
		Created:                 42,
		RawResponsesOutputItems: rawItems,
		Choices: []Choice{{Index: 0, Delta: &Message{
			ReasoningSignature: &signature,
			ReasoningBlocks:    []ReasoningBlock{{Kind: ReasoningBlockKindSignature, Signature: signature}},
		}}},
	})

	resp := agg.BuildAndReset()
	if resp == nil || string(resp.RawResponsesOutputItems) != string(rawItems) {
		t.Fatalf("expected raw Responses output items to survive aggregation, got %+v", resp)
	}
	if resp.Created != 42 || len(resp.Choices) != 1 || resp.Choices[0].Message == nil {
		t.Fatalf("unexpected aggregated response: %+v", resp)
	}
	message := resp.Choices[0].Message
	if message.ReasoningSignature == nil || *message.ReasoningSignature != signature {
		t.Fatalf("expected reasoning signature to survive, got %+v", message)
	}
	if len(message.ReasoningBlocks) != 2 {
		t.Fatalf("expected both reasoning blocks to survive, got %+v", message.ReasoningBlocks)
	}
}
