package openai

import (
	"context"
	"strings"
	"testing"

	"github.com/bestruirui/octopus/internal/transformer/model"
)

// Issue #65 Bug 1: model.InternalResponseFromStreamEvents returns nil when the
// aggregate carries no choices, usage, or error, and TransformStreamEvents fed
// that nil straight into TransformStream, which dereferenced stream.Object.
// The relay only guards len(events)==0, so non-empty nil-producing sequences
// reached the panic in production (e.g. usage-less UsageDelta events).
func TestTransformStreamEventsNilAggregateDoesNotPanic(t *testing.T) {
	tests := []struct {
		name   string
		events []model.StreamEvent
	}{
		{name: "empty slice", events: []model.StreamEvent{}},
		{name: "usage_delta with nil Usage", events: []model.StreamEvent{{Kind: model.StreamEventKindUsageDelta, Usage: nil}}},
		{name: "error event with nil Error", events: []model.StreamEvent{{Kind: model.StreamEventKindError, Error: nil}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			inbound := &ChatInbound{}
			data, err := inbound.TransformStreamEvents(context.Background(), tt.events)
			if err != nil {
				t.Fatalf("expected nil error for nil aggregate, got %v", err)
			}
			if data != nil {
				t.Fatalf("expected nil data for nil aggregate, got %q", data)
			}
		})
	}
}

// A tool call stream as emitted by the fixed Responses outbound (choice index
// 0, dense tool index) must produce a forwardable chat chunk instead of the
// pre-fix nil panic / silent drop.
func TestTransformStreamEventsToolCallStreamProducesChunk(t *testing.T) {
	toolCall := model.ToolCall{
		Index: 0,
		ID:    "call_abc123",
		Type:  "function",
		Function: model.FunctionCall{
			Name: "get_weather",
		},
	}
	events := []model.StreamEvent{
		{Kind: model.StreamEventKindToolCallStart, ID: "resp_123", Model: "gpt-test", Index: 0, ToolCall: &toolCall},
		{Kind: model.StreamEventKindToolCallDelta, ID: "resp_123", Model: "gpt-test", Index: 0, ToolCall: &toolCall, Delta: &model.StreamDelta{Arguments: "{\"city\":"}},
	}

	inbound := &ChatInbound{}
	data, err := inbound.TransformStreamEvents(context.Background(), events)
	if err != nil {
		t.Fatalf("TransformStreamEvents: %v", err)
	}
	chunk := string(data)
	if !strings.HasPrefix(chunk, "data: ") {
		t.Fatalf("expected SSE data line, got %q", chunk)
	}
	if !strings.Contains(chunk, `"index":0`) || !strings.Contains(chunk, "get_weather") {
		t.Fatalf("expected chunk with choice 0 carrying the tool call, got %q", chunk)
	}
}

func TestChatInboundErrorWinsOverDoneInOneResponse(t *testing.T) {
	inbound := &ChatInbound{}
	output, err := inbound.TransformStream(context.Background(), &model.InternalLLMResponse{
		Object: "[DONE]",
		Error:  &model.ResponseError{Detail: model.ErrorDetail{Code: "upstream_failed", Message: "failed"}},
	})
	if err != nil {
		t.Fatalf("TransformStream returned error: %v", err)
	}
	if strings.Contains(string(output), "[DONE]") {
		t.Fatalf("error response must not be replaced by [DONE]: %q", output)
	}
	outcome, cause := inbound.StreamTerminalOutcome()
	if outcome != model.PassthroughTerminalOutcomeFailed || cause == nil {
		t.Fatalf("expected failed terminal outcome, got outcome=%q cause=%v", outcome, cause)
	}
}

func TestChatInboundDropsEventsAfterDoneWhenErrorAlsoPresent(t *testing.T) {
	inbound := &ChatInbound{}
	output, err := inbound.TransformStreamEvents(context.Background(), []model.StreamEvent{
		{Kind: model.StreamEventKindTextDelta, Delta: &model.StreamDelta{Text: "prefix"}},
		{Kind: model.StreamEventKindDone},
		{Kind: model.StreamEventKindTextDelta, Delta: &model.StreamDelta{Text: "must-drop"}},
		{Kind: model.StreamEventKindError, Error: &model.ResponseError{Detail: model.ErrorDetail{Code: "failed", Message: "boom"}}},
	})
	if err == nil {
		t.Fatal("expected semantic error from mixed batch")
	}
	text := string(output)
	if !strings.Contains(text, "prefix") || strings.Contains(text, "must-drop") || strings.Contains(text, "data: [DONE]") {
		t.Fatalf("unexpected mixed-batch output: %q", text)
	}
	outcome, _ := inbound.StreamTerminalOutcome()
	if outcome != model.PassthroughTerminalOutcomeFailed {
		t.Fatalf("expected failed outcome, got %q", outcome)
	}
}
