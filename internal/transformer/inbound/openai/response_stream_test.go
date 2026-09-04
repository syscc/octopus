package openai

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/samber/lo"

	"github.com/bestruirui/octopus/internal/transformer/model"
)

// --- helpers ---------------------------------------------------------------

func feedStream(t *testing.T, chunks []*model.InternalLLMResponse) []ResponsesStreamEvent {
	t.Helper()
	i := &ResponseInbound{}
	ctx := context.Background()

	var buf bytes.Buffer
	for _, c := range chunks {
		out, err := i.TransformStream(ctx, c)
		if err != nil {
			t.Fatalf("TransformStream failed: %v", err)
		}
		buf.Write(out)
	}

	return parseSSEEvents(t, buf.Bytes())
}

func parseSSEEvents(t *testing.T, raw []byte) []ResponsesStreamEvent {
	t.Helper()
	events := make([]ResponsesStreamEvent, 0)
	for _, line := range bytes.Split(raw, []byte("\n\n")) {
		line = bytes.TrimSpace(line)
		if len(line) == 0 {
			continue
		}
		payload := bytes.TrimPrefix(line, []byte("data: "))
		if bytes.Equal(payload, []byte("[DONE]")) {
			continue
		}
		var ev ResponsesStreamEvent
		if err := json.Unmarshal(payload, &ev); err != nil {
			t.Fatalf("failed to decode SSE event %q: %v", string(payload), err)
		}
		events = append(events, ev)
	}
	return events
}

func eventTypes(events []ResponsesStreamEvent) []string {
	out := make([]string, len(events))
	for i, e := range events {
		out[i] = e.Type
	}
	return out
}

func findEvent(events []ResponsesStreamEvent, t string) *ResponsesStreamEvent {
	for i := range events {
		if events[i].Type == t {
			return &events[i]
		}
	}
	return nil
}

func findItemDone(events []ResponsesStreamEvent, itemType string) *ResponsesItem {
	for i := range events {
		if events[i].Type != "response.output_item.done" {
			continue
		}
		if events[i].Item == nil {
			continue
		}
		if events[i].Item.Type == itemType {
			return events[i].Item
		}
	}
	return nil
}

func chunkWithDelta(model_ string, delta *model.Message) *model.InternalLLMResponse {
	return &model.InternalLLMResponse{
		ID:      "resp_test",
		Model:   model_,
		Object:  "chat.completion.chunk",
		Created: 123,
		Choices: []model.Choice{{Index: 0, Delta: delta}},
	}
}

func chunkWithFinish(model_, reason string) *model.InternalLLMResponse {
	r := reason
	return &model.InternalLLMResponse{
		ID:      "resp_test",
		Model:   model_,
		Object:  "chat.completion.chunk",
		Created: 123,
		Choices: []model.Choice{{Index: 0, Delta: &model.Message{}, FinishReason: &r}},
		Usage:   &model.Usage{PromptTokens: 10, CompletionTokens: 20, TotalTokens: 30},
	}
}

// --- tests -----------------------------------------------------------------

func TestStreamReasoningBlocksSingleSignature(t *testing.T) {
	chunks := []*model.InternalLLMResponse{
		chunkWithDelta("claude", &model.Message{
			ReasoningContent: lo.ToPtr("thinking..."),
		}),
		chunkWithDelta("claude", &model.Message{
			ReasoningBlocks: []model.ReasoningBlock{{
				Kind:      model.ReasoningBlockKindSignature,
				Signature: "sigA",
				Provider:  "anthropic",
			}},
		}),
		chunkWithFinish("claude", "stop"),
	}

	events := feedStream(t, chunks)

	item := findItemDone(events, "reasoning")
	if item == nil {
		t.Fatalf("reasoning item.done not found; got %v", eventTypes(events))
	}
	if item.EncryptedContent == nil || *item.EncryptedContent != "sigA" {
		t.Fatalf("expected encrypted_content=\"sigA\", got %v", item.EncryptedContent)
	}
	if findEvent(events, "response.reasoning.delta") == nil {
		t.Fatalf("expected response.reasoning.delta, got %v", eventTypes(events))
	}
	if done := findEvent(events, "response.reasoning.done"); done == nil || done.Text != "thinking..." {
		t.Fatalf("expected response.reasoning.done with full text, got %+v", done)
	}
}

func TestStreamReasoningBlocksMultipleSignatures(t *testing.T) {
	chunks := []*model.InternalLLMResponse{
		chunkWithDelta("claude", &model.Message{
			ReasoningContent: lo.ToPtr("block one"),
		}),
		chunkWithDelta("claude", &model.Message{
			ReasoningBlocks: []model.ReasoningBlock{{
				Kind: model.ReasoningBlockKindSignature, Signature: "sig1", Provider: "anthropic",
			}},
		}),
		chunkWithDelta("claude", &model.Message{
			ReasoningContent: lo.ToPtr("block two"),
		}),
		chunkWithDelta("claude", &model.Message{
			ReasoningBlocks: []model.ReasoningBlock{{
				Kind: model.ReasoningBlockKindSignature, Signature: "sig2", Provider: "anthropic",
			}},
		}),
		chunkWithFinish("claude", "stop"),
	}

	events := feedStream(t, chunks)

	item := findItemDone(events, "reasoning")
	if item == nil || item.EncryptedContent == nil {
		t.Fatalf("reasoning item with encrypted_content missing")
	}
	var decoded []string
	if err := json.Unmarshal([]byte(*item.EncryptedContent), &decoded); err != nil {
		t.Fatalf("multi-sig encrypted_content should be JSON array, got %q: %v", *item.EncryptedContent, err)
	}
	if len(decoded) != 2 || decoded[0] != "sig1" || decoded[1] != "sig2" {
		t.Fatalf("unexpected signatures: %v", decoded)
	}
}

func TestStreamReasoningBlocksRedacted(t *testing.T) {
	chunks := []*model.InternalLLMResponse{
		chunkWithDelta("claude", &model.Message{
			ReasoningBlocks: []model.ReasoningBlock{{
				Kind:     model.ReasoningBlockKindRedacted,
				Data:     "REDACTED_DATA",
				Provider: "anthropic",
			}},
		}),
		chunkWithFinish("claude", "stop"),
	}

	events := feedStream(t, chunks)

	if findEvent(events, "response.output_item.added") == nil {
		t.Fatalf("redacted block should open a reasoning item; events=%v", eventTypes(events))
	}
	if findItemDone(events, "reasoning") == nil {
		t.Fatalf("redacted block should close with reasoning output_item.done; events=%v", eventTypes(events))
	}
}

func TestStreamReasoningLegacyFallback(t *testing.T) {
	chunks := []*model.InternalLLMResponse{
		chunkWithDelta("openrouter", &model.Message{
			ReasoningContent: lo.ToPtr("legacy reasoning"),
		}),
		chunkWithDelta("openrouter", &model.Message{
			ReasoningSignature: lo.ToPtr("sigLegacy"),
		}),
		chunkWithFinish("openrouter", "stop"),
	}

	events := feedStream(t, chunks)

	item := findItemDone(events, "reasoning")
	if item == nil || item.EncryptedContent == nil {
		t.Fatalf("legacy reasoning path lost signature; events=%v", eventTypes(events))
	}
	if *item.EncryptedContent != "sigLegacy" {
		t.Fatalf("expected legacy signature verbatim, got %q", *item.EncryptedContent)
	}
}

func TestStreamRefusalEvents(t *testing.T) {
	chunks := []*model.InternalLLMResponse{
		chunkWithDelta("claude", &model.Message{Refusal: "I cannot"}),
		chunkWithDelta("claude", &model.Message{Refusal: " help with that."}),
		chunkWithFinish("claude", "refusal"),
	}

	events := feedStream(t, chunks)
	types := eventTypes(events)

	want := []string{
		"response.content_part.added",
		"response.refusal.delta",
		"response.refusal.delta",
	}
	for _, w := range want {
		if findEvent(events, w) == nil {
			t.Fatalf("expected event %s, got sequence %v", w, types)
		}
	}

	addPart := findEvent(events, "response.content_part.added")
	if addPart.Part == nil || addPart.Part.Type != "refusal" {
		t.Fatalf("content_part.added should be refusal, got %+v", addPart.Part)
	}

	refusalDone := findEvent(events, "response.refusal.done")
	if refusalDone == nil || refusalDone.Text != "I cannot help with that." {
		t.Fatalf("refusal.done text mismatch: got %v", refusalDone)
	}

	partDone := findEvent(events, "response.content_part.done")
	if partDone == nil || partDone.Part == nil || partDone.Part.Type != "refusal" {
		t.Fatalf("content_part.done should carry Type=refusal, got %+v", partDone)
	}

	item := findItemDone(events, "message")
	if item == nil || item.Content == nil || len(item.Content.Items) == 0 {
		t.Fatalf("message item.done missing; events=%v", types)
	}
	if item.Content.Items[0].Refusal == nil || *item.Content.Items[0].Refusal != "I cannot help with that." {
		t.Fatalf("item refusal content lost: %+v", item.Content.Items[0].Refusal)
	}
}

func TestStreamToolCallArgumentDeltaUsesStoredOutputIndex(t *testing.T) {
	chunks := []*model.InternalLLMResponse{
		chunkWithDelta("gpt-4o", &model.Message{
			ToolCalls: []model.ToolCall{{
				Index: 0,
				ID:    "call_a",
				Type:  "function",
				Function: model.FunctionCall{
					Name:      "first",
					Arguments: `{"a":`,
				},
			}},
		}),
		chunkWithDelta("gpt-4o", &model.Message{
			ToolCalls: []model.ToolCall{{
				Index: 1,
				ID:    "call_b",
				Type:  "function",
				Function: model.FunctionCall{
					Name:      "second",
					Arguments: `{"b":1}`,
				},
			}},
		}),
		chunkWithDelta("gpt-4o", &model.Message{
			ToolCalls: []model.ToolCall{{
				Index: 0,
				ID:    "call_a",
				Type:  "function",
				Function: model.FunctionCall{
					Arguments: `1}`,
				},
			}},
		}),
	}

	events := feedStream(t, chunks)
	var firstToolOutputIndex *int
	for _, event := range events {
		if event.Type == "response.output_item.added" && event.Item != nil && event.Item.CallID == "call_a" {
			firstToolOutputIndex = event.OutputIndex
			break
		}
	}
	if firstToolOutputIndex == nil {
		t.Fatalf("expected output_item.added for call_a; events=%v", eventTypes(events))
	}

	var lateDelta *ResponsesStreamEvent
	for i := range events {
		if events[i].Type == "response.function_call_arguments.delta" && events[i].ItemID != nil && *events[i].ItemID == "call_a" && events[i].Delta == `1}` {
			lateDelta = &events[i]
			break
		}
	}
	if lateDelta == nil {
		t.Fatalf("expected late argument delta for call_a; events=%v", eventTypes(events))
	}
	if lateDelta.OutputIndex == nil || *lateDelta.OutputIndex != *firstToolOutputIndex {
		t.Fatalf("late call_a delta output_index=%v, want %d", lateDelta.OutputIndex, *firstToolOutputIndex)
	}
}

func TestTransformStreamEventsSignatureOnlyStillOpensReasoningItem(t *testing.T) {
	i := &ResponseInbound{}
	ctx := context.Background()

	out, err := i.TransformStreamEvents(ctx, []model.StreamEvent{
		{
			Kind:  model.StreamEventKindMessageStart,
			ID:    "resp_sig_only",
			Model: "claude",
			Role:  "assistant",
		},
		{
			Kind:  model.StreamEventKindSignatureDelta,
			ID:    "resp_sig_only",
			Model: "claude",
			Delta: &model.StreamDelta{Signature: "sig_only"},
		},
		{
			Kind:       model.StreamEventKindMessageStop,
			ID:         "resp_sig_only",
			Model:      "claude",
			StopReason: model.FinishReasonStop,
		},
		{
			Kind:  model.StreamEventKindUsageDelta,
			ID:    "resp_sig_only",
			Model: "claude",
			Usage: &model.Usage{PromptTokens: 1, CompletionTokens: 1, TotalTokens: 2},
		},
	})
	if err != nil {
		t.Fatalf("TransformStreamEvents failed: %v", err)
	}

	events := parseSSEEvents(t, out)
	item := findItemDone(events, "reasoning")
	if item == nil || item.EncryptedContent == nil || *item.EncryptedContent != "sig_only" {
		t.Fatalf("expected reasoning output item with encrypted_content, got %+v", item)
	}
}

func TestTransformStreamMatchesStreamEventsProjection(t *testing.T) {
	chunks := []*model.InternalLLMResponse{
		chunkWithDelta("claude", &model.Message{
			Role:             "assistant",
			ReasoningContent: lo.ToPtr("thinking..."),
		}),
		chunkWithDelta("claude", &model.Message{
			ReasoningBlocks: []model.ReasoningBlock{{
				Kind:      model.ReasoningBlockKindSignature,
				Signature: "sigA",
				Provider:  "anthropic",
			}},
		}),
		chunkWithDelta("claude", &model.Message{
			Content: model.MessageContent{Content: lo.ToPtr("answer")},
			Refusal: " but not that part",
		}),
		chunkWithFinish("claude", "stop"),
	}

	streamInbound := &ResponseInbound{}
	eventInbound := &ResponseInbound{}
	ctx := context.Background()

	var streamBuf bytes.Buffer
	var eventBuf bytes.Buffer
	for _, chunk := range chunks {
		streamOut, err := streamInbound.TransformStream(ctx, chunk)
		if err != nil {
			t.Fatalf("TransformStream failed: %v", err)
		}
		streamBuf.Write(streamOut)

		eventOut, err := eventInbound.TransformStreamEvents(ctx, model.StreamEventsFromInternalResponse(chunk))
		if err != nil {
			t.Fatalf("TransformStreamEvents failed: %v", err)
		}
		eventBuf.Write(eventOut)
	}

	streamEvents := parseSSEEvents(t, streamBuf.Bytes())
	projectedEvents := parseSSEEvents(t, eventBuf.Bytes())
	if len(streamEvents) != len(projectedEvents) {
		t.Fatalf("event count mismatch: stream=%d projected=%d", len(streamEvents), len(projectedEvents))
	}
	for idx := range streamEvents {
		if streamEvents[idx].Type != projectedEvents[idx].Type {
			t.Fatalf("event[%d] type mismatch: %q vs %q", idx, streamEvents[idx].Type, projectedEvents[idx].Type)
		}
	}

	streamDone := findItemDone(streamEvents, "message")
	projectedDone := findItemDone(projectedEvents, "message")
	if streamDone == nil || projectedDone == nil {
		t.Fatalf("expected message output_item.done in both paths")
	}
	streamDone.ID = ""
	projectedDone.ID = ""
	gotStream, err := json.Marshal(streamDone)
	if err != nil {
		t.Fatalf("marshal stream item: %v", err)
	}
	gotProjected, err := json.Marshal(projectedDone)
	if err != nil {
		t.Fatalf("marshal projected item: %v", err)
	}
	if string(gotStream) != string(gotProjected) {
		t.Fatalf("message item mismatch:\nstream=%s\nprojected=%s", gotStream, gotProjected)
	}
}

func TestTransformStreamEventsCompletesWithoutUsage(t *testing.T) {
	inbound := &ResponseInbound{}
	ctx := context.Background()
	output, err := inbound.TransformStreamEvents(ctx, []model.StreamEvent{
		{Kind: model.StreamEventKindMessageStart, ID: "resp_no_usage", Model: "gpt-test", Role: "assistant"},
		{Kind: model.StreamEventKindTextDelta, ID: "resp_no_usage", Model: "gpt-test", Delta: &model.StreamDelta{Text: "hello"}},
		{Kind: model.StreamEventKindMessageStop, ID: "resp_no_usage", Model: "gpt-test", StopReason: model.FinishReasonStop},
		{Kind: model.StreamEventKindDone},
	})
	if err != nil {
		t.Fatalf("TransformStreamEvents failed: %v", err)
	}
	events := parseSSEEvents(t, output)
	completed := findEvent(events, "response.completed")
	if completed == nil || completed.Response == nil {
		t.Fatalf("expected response.completed without usage, got %v", eventTypes(events))
	}
	if completed.Response.Usage != nil {
		t.Fatalf("expected omitted usage, got %+v", completed.Response.Usage)
	}
	if len(completed.Response.Output) == 0 {
		t.Fatalf("expected terminal output items")
	}
}

func TestTransformStreamEventsStartsLifecycleWithoutRoleDelta(t *testing.T) {
	inbound := &ResponseInbound{}
	output, err := inbound.TransformStreamEvents(context.Background(), []model.StreamEvent{
		{Kind: model.StreamEventKindTextDelta, ID: "resp_no_role", Model: "gpt-test", Created: 42, Delta: &model.StreamDelta{Text: "hello"}},
		{Kind: model.StreamEventKindMessageStop, ID: "resp_no_role", Model: "gpt-test", Created: 42, StopReason: model.FinishReasonStop},
		{Kind: model.StreamEventKindDone},
	})
	if err != nil {
		t.Fatalf("TransformStreamEvents failed: %v", err)
	}
	events := parseSSEEvents(t, output)
	types := eventTypes(events)
	if len(types) < 3 || types[0] != "response.created" || types[1] != "response.in_progress" {
		t.Fatalf("expected lifecycle events before content without a role delta, got %v", types)
	}
	created := findEvent(events, "response.created")
	if created == nil || created.Response == nil || created.Response.ID != "resp_no_role" || created.Response.CreatedAt != 42 {
		t.Fatalf("unexpected response.created payload: %+v", created)
	}
	if findEvent(events, "response.completed") == nil {
		t.Fatalf("expected terminal event, got %v", types)
	}
}

func TestTransformStreamEventsWaitsForUsageBeforeTerminal(t *testing.T) {
	inbound := &ResponseInbound{}
	ctx := context.Background()
	first, err := inbound.TransformStreamEvents(ctx, []model.StreamEvent{
		{Kind: model.StreamEventKindMessageStart, ID: "resp_usage", Model: "gpt-test", Role: "assistant"},
		{Kind: model.StreamEventKindTextDelta, ID: "resp_usage", Model: "gpt-test", Delta: &model.StreamDelta{Text: "hello"}},
		{Kind: model.StreamEventKindMessageStop, ID: "resp_usage", Model: "gpt-test", StopReason: model.FinishReasonStop},
	})
	if err != nil {
		t.Fatalf("initial transform failed: %v", err)
	}
	if completed := findEvent(parseSSEEvents(t, first), "response.completed"); completed != nil {
		t.Fatalf("terminal event must wait for a possible usage chunk")
	}

	terminal, err := inbound.TransformStreamEvents(ctx, []model.StreamEvent{{
		Kind:  model.StreamEventKindUsageDelta,
		ID:    "resp_usage",
		Model: "gpt-test",
		Usage: &model.Usage{PromptTokens: 1, CompletionTokens: 2, TotalTokens: 3},
	}})
	if err != nil {
		t.Fatalf("usage transform failed: %v", err)
	}
	completed := findEvent(parseSSEEvents(t, terminal), "response.completed")
	if completed == nil || completed.Response == nil || completed.Response.Usage == nil || completed.Response.Usage.TotalTokens != 3 {
		t.Fatalf("expected terminal event with late usage, got %+v", completed)
	}
}

func TestTransformStreamEventsWaitsPastZeroUsagePlaceholder(t *testing.T) {
	inbound := &ResponseInbound{}
	ctx := context.Background()
	first, err := inbound.TransformStreamEvents(ctx, []model.StreamEvent{
		{Kind: model.StreamEventKindMessageStart, ID: "resp_placeholder", Model: "gpt-test", Role: "assistant"},
		{Kind: model.StreamEventKindTextDelta, ID: "resp_placeholder", Model: "gpt-test", Delta: &model.StreamDelta{Text: "hello"}},
		{Kind: model.StreamEventKindMessageStop, ID: "resp_placeholder", Model: "gpt-test", StopReason: model.FinishReasonStop},
		{Kind: model.StreamEventKindUsageDelta, ID: "resp_placeholder", Model: "gpt-test", Usage: &model.Usage{}},
	})
	if err != nil {
		t.Fatalf("placeholder transform failed: %v", err)
	}
	if completed := findEvent(parseSSEEvents(t, first), "response.completed"); completed != nil {
		t.Fatal("zero usage placeholder must not complete the response")
	}

	terminal, err := inbound.TransformStreamEvents(ctx, []model.StreamEvent{{
		Kind:  model.StreamEventKindUsageDelta,
		ID:    "resp_placeholder",
		Model: "gpt-test",
		Usage: &model.Usage{PromptTokens: 4, CompletionTokens: 5, TotalTokens: 9},
	}})
	if err != nil {
		t.Fatalf("final usage transform failed: %v", err)
	}
	completed := findEvent(parseSSEEvents(t, terminal), "response.completed")
	if completed == nil || completed.Response == nil || completed.Response.Usage == nil || completed.Response.Usage.TotalTokens != 9 {
		t.Fatalf("expected terminal event with final usage, got %+v", completed)
	}
}

func TestResponseInboundFinalizeStreamEmitsIncompleteAtPartialEOF(t *testing.T) {
	inbound := &ResponseInbound{}
	ctx := context.Background()
	first, err := inbound.TransformStreamEvents(ctx, []model.StreamEvent{
		{Kind: model.StreamEventKindMessageStart, ID: "resp_partial", Model: "gpt-test", Created: 9, Role: "assistant"},
		{Kind: model.StreamEventKindTextDelta, ID: "resp_partial", Model: "gpt-test", Created: 9, Delta: &model.StreamDelta{Text: "partial answer"}},
	})
	if err != nil {
		t.Fatalf("partial transform failed: %v", err)
	}
	if created := findEvent(parseSSEEvents(t, first), "response.created"); created == nil {
		t.Fatalf("expected response.created lifecycle before EOF, got %q", first)
	}

	terminal, err := inbound.FinalizeStream(ctx)
	if !errors.Is(err, model.ErrIncompleteUpstreamStream) {
		t.Fatalf("expected ErrIncompleteUpstreamStream, got %v", err)
	}
	events := parseSSEEvents(t, terminal)
	incomplete := findEvent(events, "response.incomplete")
	if incomplete == nil || incomplete.Response == nil {
		t.Fatalf("expected exactly one response.incomplete, got %v", eventTypes(events))
	}
	if count := len(filterEvents(events, "response.incomplete")); count != 1 {
		t.Fatalf("expected exactly one response.incomplete, got %d", count)
	}
	if findEvent(events, "response.completed") != nil || findEvent(events, "response.failed") != nil {
		t.Fatalf("must not emit other terminals, got %v", eventTypes(events))
	}
	if strings.Contains(string(terminal), "[DONE]") {
		t.Fatalf("must not emit [DONE], got %q", terminal)
	}
	if incomplete.Response.ID != "resp_partial" || incomplete.Response.CreatedAt != 9 {
		t.Fatalf("metadata lost: %+v", incomplete.Response)
	}
	if incomplete.Response.Status == nil || *incomplete.Response.Status != "incomplete" {
		t.Fatalf("expected status incomplete, got %+v", incomplete.Response.Status)
	}
	item := findItemDone(events, "message")
	if item == nil || item.Content == nil || len(item.Content.Items) == 0 {
		t.Fatalf("expected the open message item to be closed with accumulated text, got %v", eventTypes(events))
	}
	if item.Content.Items[0].Text == nil || *item.Content.Items[0].Text != "partial answer" {
		t.Fatalf("accumulated text lost: %+v", item.Content.Items[0].Text)
	}
	if incomplete.Response.Usage != nil {
		t.Fatalf("must not fabricate usage, got %+v", incomplete.Response.Usage)
	}

	again, err := inbound.FinalizeStream(ctx)
	if err != nil || len(again) != 0 {
		t.Fatalf("finalization must be idempotent, got %q err=%v", again, err)
	}
}

func TestResponseInboundFinalizeStreamEmptyAdapterStaysEmpty(t *testing.T) {
	inbound := &ResponseInbound{}
	terminal, err := inbound.FinalizeStream(context.Background())
	if err != nil || len(terminal) != 0 {
		t.Fatalf("empty adapter must finalize to nil so the processor reports an empty stream, got %q err=%v", terminal, err)
	}
}

func TestResponseInboundErrorWinsOverDoneInOneResponse(t *testing.T) {
	inbound := &ResponseInbound{}
	output, err := inbound.TransformStream(context.Background(), &model.InternalLLMResponse{
		Object: "[DONE]",
		ID:     "resp_failed",
		Model:  "gpt-test",
		Error: &model.ResponseError{Detail: model.ErrorDetail{
			Code:    "context_length_exceeded",
			Message: "too long",
		}},
	})
	if err != nil {
		t.Fatalf("TransformStream returned error: %v", err)
	}
	events := parseSSEEvents(t, output)
	if findEvent(events, "response.failed") == nil {
		t.Fatalf("expected response.failed, got %v", eventTypes(events))
	}
	if findEvent(events, "response.completed") != nil || findEvent(events, "response.incomplete") != nil {
		t.Fatalf("error-plus-Done must not complete the response, got %v", eventTypes(events))
	}
	outcome, cause := inbound.StreamTerminalOutcome()
	if outcome != model.PassthroughTerminalOutcomeFailed || cause == nil {
		t.Fatalf("expected failed outcome, got outcome=%q cause=%v", outcome, cause)
	}
}

func TestResponseInboundFinalizeStreamAfterExplicitFailedTerminal(t *testing.T) {
	inbound := &ResponseInbound{}
	ctx := context.Background()
	first, err := inbound.TransformStreamEvents(ctx, []model.StreamEvent{
		{Kind: model.StreamEventKindMessageStart, ID: "resp_failed", Model: "gpt-test", Role: "assistant"},
		{Kind: model.StreamEventKindError, ID: "resp_failed", Model: "gpt-test", Error: &model.ResponseError{Detail: model.ErrorDetail{Code: "context_length_exceeded", Message: "too long"}}},
	})
	if err != nil {
		t.Fatalf("failed terminal transform failed: %v", err)
	}
	if findEvent(parseSSEEvents(t, first), "response.failed") == nil {
		t.Fatalf("expected explicit response.failed, got %q", first)
	}
	again, err := inbound.FinalizeStream(ctx)
	if err != nil || len(again) != 0 {
		t.Fatalf("explicit terminal must not be duplicated, got %q err=%v", again, err)
	}
}

func TestResponseInboundFinalizeInterruptedStreamAlwaysIncomplete(t *testing.T) {
	inbound := &ResponseInbound{}
	ctx := context.Background()
	first, err := inbound.TransformStreamEvents(ctx, []model.StreamEvent{
		{Kind: model.StreamEventKindMessageStart, ID: "resp_interrupted", Model: "gpt-test", Created: 10, Role: "assistant"},
		{Kind: model.StreamEventKindTextDelta, ID: "resp_interrupted", Model: "gpt-test", Delta: &model.StreamDelta{Text: "partial"}},
		{Kind: model.StreamEventKindMessageStop, ID: "resp_interrupted", Model: "gpt-test", StopReason: model.FinishReasonStop},
	})
	if err != nil {
		t.Fatalf("partial interrupted transform failed: %v", err)
	}
	if findEvent(parseSSEEvents(t, first), "response.completed") != nil {
		t.Fatalf("finish signal before transport interruption must not complete before finalization: %q", first)
	}

	terminal, err := inbound.FinalizeInterruptedStream(ctx)
	if !errors.Is(err, model.ErrIncompleteUpstreamStream) {
		t.Fatalf("expected ErrIncompleteUpstreamStream, got %v", err)
	}
	events := parseSSEEvents(t, terminal)
	if findEvent(events, "response.incomplete") == nil || findEvent(events, "response.completed") != nil {
		t.Fatalf("interrupted stream must emit only response.incomplete, got %v", eventTypes(events))
	}
	again, err := inbound.FinalizeInterruptedStream(ctx)
	if err != nil || len(again) != 0 {
		t.Fatalf("interrupted finalization must be idempotent, got %q err=%v", again, err)
	}
}

func TestResponseInboundFinalizeIncompleteStreamPassthrough(t *testing.T) {
	inbound := &ResponseInbound{}
	ctx := context.Background()
	if _, err := inbound.TransformStreamEvents(ctx, []model.StreamEvent{
		{Kind: model.StreamEventKindMessageStart, ID: "resp_pt", Model: "fb-model", Created: 11, Role: "assistant"},
		{Kind: model.StreamEventKindTextDelta, ID: "resp_pt", Model: "fb-model", Created: 11, Delta: &model.StreamDelta{Text: "hi"}},
	}); err != nil {
		t.Fatalf("sidecar transform failed: %v", err)
	}
	raw := strings.Join([]string{
		`data: {"type":"response.created","sequence_number":7,"response":{"id":"resp_pt","model":"fb-model","created_at":11,"status":"in_progress","output":[]}}`,
		"",
		`data: {"type":"response.output_text.delta","sequence_number":8,"delta":"hi"}`,
		"",
	}, "\r\n")

	out, err := inbound.FinalizeIncompleteStream(ctx, []byte(raw))
	if !errors.Is(err, model.ErrIncompleteUpstreamStream) {
		t.Fatalf("expected ErrIncompleteUpstreamStream, got %v", err)
	}
	events := parseSSEEvents(t, out)
	incomplete := findEvent(events, "response.incomplete")
	if incomplete == nil || incomplete.Response == nil {
		t.Fatalf("expected one synthesized response.incomplete, got %q", out)
	}
	if incomplete.Response.ID != "resp_pt" || incomplete.Response.Model != "fb-model" || incomplete.Response.CreatedAt != 11 {
		t.Fatalf("snapshot metadata lost: %+v", incomplete.Response)
	}
	if incomplete.SequenceNumber <= 8 {
		t.Fatalf("synthetic terminal sequence must continue after upstream sequence 8, got %d", incomplete.SequenceNumber)
	}
	if !strings.Contains(string(out), `"text":"hi"`) {
		t.Fatalf("partial sidecar output must be preserved, got %q", out)
	}
	if strings.Contains(string(out), "[DONE]") {
		t.Fatalf("must not append [DONE], got %q", out)
	}

	terminalInbound := &ResponseInbound{}
	terminalRaw := raw + "\r\n" + `data: {"type":"response.completed","sequence_number":9,"response":{"id":"resp_pt","status":"completed"}}` + "\r\n\r\n"
	out, err = terminalInbound.FinalizeIncompleteStream(ctx, []byte(terminalRaw))
	if err != nil || len(out) != 0 {
		t.Fatalf("stream with an existing terminal must not be synthesized, got %q err=%v", out, err)
	}
}

func TestResponseInboundFinalizeIncompleteStreamAlignsOpenPassthroughItem(t *testing.T) {
	inbound := &ResponseInbound{}
	ctx := context.Background()
	if _, err := inbound.TransformStreamEvents(ctx, []model.StreamEvent{
		{Kind: model.StreamEventKindMessageStart, ID: "resp_open", Model: "fb-model", Created: 12, Role: "assistant"},
		{Kind: model.StreamEventKindTextDelta, ID: "resp_open", Model: "fb-model", Created: 12, Delta: &model.StreamDelta{Text: "partial"}},
	}); err != nil {
		t.Fatalf("sidecar transform failed: %v", err)
	}
	raw := strings.Join([]string{
		`data: {"type":"response.created","sequence_number":7,"response":{"id":"resp_open","model":"fb-model","created_at":12,"status":"in_progress","output":[]}}`,
		"",
		`data: {"type":"response.output_item.added","sequence_number":8,"output_index":3,"item":{"id":"msg_real","type":"message","role":"assistant","status":"in_progress","content":[]}}`,
		"",
		`data: {"type":"response.output_text.delta","sequence_number":9,"item_id":"msg_real","output_index":3,"content_index":0,"delta":"partial"}`,
		"",
	}, "\n")

	out, err := inbound.FinalizeIncompleteStream(ctx, []byte(raw))
	if !errors.Is(err, model.ErrIncompleteUpstreamStream) {
		t.Fatalf("expected ErrIncompleteUpstreamStream, got %v", err)
	}
	events := parseSSEEvents(t, out)
	for _, eventType := range []string{"response.output_text.done", "response.content_part.done"} {
		event := findEvent(events, eventType)
		if event == nil || event.ItemID == nil || *event.ItemID != "msg_real" || event.OutputIndex == nil || *event.OutputIndex != 3 {
			t.Fatalf("%s must use upstream item identity/index, got %+v", eventType, event)
		}
	}
	itemDone := findEvent(events, "response.output_item.done")
	if itemDone == nil || itemDone.Item == nil || itemDone.Item.ID != "msg_real" || itemDone.OutputIndex == nil || *itemDone.OutputIndex != 3 {
		t.Fatalf("output_item.done must use upstream item identity/index, got %+v", itemDone)
	}
	incomplete := findEvent(events, "response.incomplete")
	if incomplete == nil || incomplete.Response == nil || len(incomplete.Response.Output) != 1 || incomplete.Response.Output[0].ID != "msg_real" {
		t.Fatalf("terminal output must preserve upstream item identity, got %+v", incomplete)
	}
}

func TestResponseInboundInitializeResponseCopiesTruncation(t *testing.T) {
	base := "auto"
	request := &model.InternalLLMRequest{Truncation: &base}
	inbound := &ResponseInbound{}
	inbound.InitializeResponse(request)
	if inbound.truncation == nil || *inbound.truncation != "auto" {
		t.Fatalf("expected truncation to be copied, got %v", inbound.truncation)
	}
	*inbound.truncation = "other"
	if *request.Truncation != "auto" {
		t.Fatal("truncation must be copied without aliasing the request")
	}
	inbound.InitializeResponse(nil)
	if inbound.truncation == nil || *inbound.truncation != "other" {
		t.Fatal("nil request must be a no-op")
	}
}

func filterEvents(events []ResponsesStreamEvent, eventType string) []ResponsesStreamEvent {
	var out []ResponsesStreamEvent
	for _, e := range events {
		if e.Type == eventType {
			out = append(out, e)
		}
	}
	return out
}

func TestResponseInboundFinalizeStreamCompletesAtEOF(t *testing.T) {
	inbound := &ResponseInbound{}
	ctx := context.Background()
	first, err := inbound.TransformStreamEvents(ctx, []model.StreamEvent{
		{Kind: model.StreamEventKindMessageStart, ID: "resp_eof", Model: "gpt-test", Role: "assistant"},
		{Kind: model.StreamEventKindMessageStop, ID: "resp_eof", Model: "gpt-test", StopReason: model.FinishReasonStop},
	})
	if err != nil {
		t.Fatalf("initial transform failed: %v", err)
	}
	if completed := findEvent(parseSSEEvents(t, first), "response.completed"); completed != nil {
		t.Fatalf("terminal event must not be emitted before EOF")
	}

	terminal, err := inbound.FinalizeStream(ctx)
	if err != nil {
		t.Fatalf("FinalizeStream failed: %v", err)
	}
	completed := findEvent(parseSSEEvents(t, terminal), "response.completed")
	if completed == nil || completed.Response == nil {
		t.Fatalf("expected response.completed at EOF, got %q", terminal)
	}
	if completed.Response.Usage != nil {
		t.Fatalf("expected EOF terminal without fabricated usage, got %+v", completed.Response.Usage)
	}
	again, err := inbound.FinalizeStream(ctx)
	if err != nil || len(again) != 0 {
		t.Fatalf("expected idempotent finalization, got %q err=%v", again, err)
	}
}

func TestTransformStreamEventsLateUsageDoesNotDuplicateTerminal(t *testing.T) {
	inbound := &ResponseInbound{}
	ctx := context.Background()
	first, err := inbound.TransformStreamEvents(ctx, []model.StreamEvent{
		{Kind: model.StreamEventKindMessageStart, ID: "resp_late_usage", Model: "gpt-test", Role: "assistant"},
		{Kind: model.StreamEventKindMessageStop, ID: "resp_late_usage", Model: "gpt-test", StopReason: model.FinishReasonStop},
		{Kind: model.StreamEventKindDone},
	})
	if err != nil {
		t.Fatalf("initial terminal transform failed: %v", err)
	}
	if completed := findEvent(parseSSEEvents(t, first), "response.completed"); completed == nil {
		t.Fatalf("expected initial response.completed")
	}

	late, err := inbound.TransformStreamEvents(ctx, []model.StreamEvent{{
		Kind:  model.StreamEventKindUsageDelta,
		Usage: &model.Usage{PromptTokens: 1, CompletionTokens: 2, TotalTokens: 3},
	}})
	if err != nil {
		t.Fatalf("late usage transform failed: %v", err)
	}
	if len(late) != 0 {
		t.Fatalf("late usage must not emit a duplicate terminal event, got %q", late)
	}
}

func TestStreamTextThenRefusal(t *testing.T) {
	chunks := []*model.InternalLLMResponse{
		chunkWithDelta("claude", &model.Message{
			Content: model.MessageContent{Content: lo.ToPtr("partial answer...")},
		}),
		chunkWithDelta("claude", &model.Message{Refusal: "Actually I can't."}),
		chunkWithFinish("claude", "refusal"),
	}

	events := feedStream(t, chunks)
	types := eventTypes(events)

	// Expect text part fully opened-and-closed before refusal opens.
	textDoneIdx := -1
	refusalAddedIdx := -1
	for idx, e := range events {
		if e.Type == "response.output_text.done" && textDoneIdx == -1 {
			textDoneIdx = idx
		}
		if e.Type == "response.content_part.added" && e.Part != nil && e.Part.Type == "refusal" && refusalAddedIdx == -1 {
			refusalAddedIdx = idx
		}
	}
	if textDoneIdx == -1 || refusalAddedIdx == -1 {
		t.Fatalf("expected text.done then refusal part.added; got %v", types)
	}
	if textDoneIdx > refusalAddedIdx {
		t.Fatalf("text.done (%d) must precede refusal content_part.added (%d)", textDoneIdx, refusalAddedIdx)
	}

	item := findItemDone(events, "message")
	if item == nil || item.Content == nil {
		t.Fatalf("message item.done missing")
	}
	if len(item.Content.Items) != 2 {
		t.Fatalf("expected 2 content items (text + refusal), got %d: %+v", len(item.Content.Items), item.Content.Items)
	}
	if item.Content.Items[0].Type != "output_text" || item.Content.Items[1].Type != "refusal" {
		t.Fatalf("content items order wrong: %+v", item.Content.Items)
	}
	if item.Content.Items[0].Text == nil || !strings.Contains(*item.Content.Items[0].Text, "partial answer") {
		t.Fatalf("text content lost: %+v", item.Content.Items[0].Text)
	}
	if item.Content.Items[1].Refusal == nil || *item.Content.Items[1].Refusal != "Actually I can't." {
		t.Fatalf("refusal content lost: %+v", item.Content.Items[1].Refusal)
	}
}
