package openai

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/bestruirui/octopus/internal/transformer/model"
)

// 部分 payload 已写出后上游中断：ChatInbound 必须实现
// InboundInterruptedFinalizer，产出唯一的 [DONE] 终态并返回
// ErrIncompleteUpstreamStream 哨兵（relay 据此记硬失败且不 failover），
// 同时不得伪造 finish_reason 或 usage。
func TestChatInboundFinalizeInterruptedStreamEmitsDoneWithoutFabrication(t *testing.T) {
	inbound := &ChatInbound{}
	ctx := context.Background()

	content := "partial"
	chunk := &model.InternalLLMResponse{
		ID:     "chat_interrupted",
		Model:  "gpt-test",
		Object: "chat.completion.chunk",
		Choices: []model.Choice{{
			Index: 0,
			Delta: &model.Message{Role: "assistant", Content: model.MessageContent{Content: &content}},
		}},
	}
	if _, err := inbound.TransformStream(ctx, chunk); err != nil {
		t.Fatalf("TransformStream failed: %v", err)
	}

	terminal, err := inbound.FinalizeInterruptedStream(ctx)
	if !errors.Is(err, model.ErrIncompleteUpstreamStream) {
		t.Fatalf("expected ErrIncompleteUpstreamStream, got %v", err)
	}
	if !bytes.Equal(bytes.TrimSpace(terminal), []byte("data: [DONE]")) {
		t.Fatalf("interrupted chat stream must end with the [DONE] terminator, got %q", terminal)
	}

	// 幂等：第二次调用不再产出终态或错误
	again, err := inbound.FinalizeInterruptedStream(ctx)
	if err != nil || len(again) != 0 {
		t.Fatalf("interrupted finalization must be idempotent, got %q err=%v", again, err)
	}

	// 不得伪造 usage：聚合响应不应凭空产生 token 统计
	resp, err := inbound.GetInternalResponse(ctx)
	if err != nil {
		t.Fatalf("GetInternalResponse failed: %v", err)
	}
	if resp != nil && resp.Usage != nil && (resp.Usage.PromptTokens != 0 || resp.Usage.CompletionTokens != 0 || resp.Usage.TotalTokens != 0) {
		t.Fatalf("interrupted finalization must not fabricate usage, got %+v", resp.Usage)
	}
}

func TestChatInboundFinalizeStreamEmitsIncompleteAtCleanEOF(t *testing.T) {
	inbound := &ChatInbound{}
	ctx := context.Background()
	content := "partial"
	if _, err := inbound.TransformStream(ctx, &model.InternalLLMResponse{
		Object:  "chat.completion.chunk",
		Choices: []model.Choice{{Index: 0, Delta: &model.Message{Content: model.MessageContent{Content: &content}}}},
	}); err != nil {
		t.Fatalf("TransformStream failed: %v", err)
	}

	terminal, err := inbound.FinalizeStream(ctx)
	if !errors.Is(err, model.ErrIncompleteUpstreamStream) {
		t.Fatalf("expected ErrIncompleteUpstreamStream, got %v", err)
	}
	if string(bytes.TrimSpace(terminal)) != "data: [DONE]" {
		t.Fatalf("expected one Chat terminator, got %q", terminal)
	}
	again, err := inbound.FinalizeStream(ctx)
	if err != nil || len(again) != 0 {
		t.Fatalf("clean EOF finalization must be idempotent, got %q err=%v", again, err)
	}
}

func TestChatInboundTransformStreamEventsTracksPayloadForCleanEOF(t *testing.T) {
	inbound := &ChatInbound{}
	ctx := context.Background()
	output, err := inbound.TransformStreamEvents(ctx, []model.StreamEvent{{
		Kind:  model.StreamEventKindTextDelta,
		Index: 0,
		Delta: &model.StreamDelta{Text: "partial from events"},
	}})
	if err != nil {
		t.Fatalf("TransformStreamEvents failed: %v", err)
	}
	if !bytes.Contains(output, []byte("partial from events")) {
		t.Fatalf("expected event payload to be visible, got %q", output)
	}
	terminal, err := inbound.FinalizeStream(ctx)
	if !errors.Is(err, model.ErrIncompleteUpstreamStream) {
		t.Fatalf("expected event-driven clean EOF to be incomplete, got %v", err)
	}
	if string(bytes.TrimSpace(terminal)) != "data: [DONE]" {
		t.Fatalf("expected one Chat terminator after event-driven EOF, got %q", terminal)
	}
}

func TestChatInboundTransformStreamEventsNoopDoesNotStartStream(t *testing.T) {
	inbound := &ChatInbound{}
	ctx := context.Background()
	output, err := inbound.TransformStreamEvents(ctx, []model.StreamEvent{{Kind: model.StreamEventKindUsageDelta}})
	if err != nil || len(output) != 0 {
		t.Fatalf("metadata-only event should be ignored, output=%q err=%v", output, err)
	}
	terminal, err := inbound.FinalizeStream(ctx)
	if err != nil || len(terminal) != 0 {
		t.Fatalf("ignored event must not synthesize a terminal, output=%q err=%v", terminal, err)
	}
}

func TestChatInboundVisibleEventStartsIncompleteTracking(t *testing.T) {
	if !hasVisibleChatStreamEvent([]model.StreamEvent{{Kind: model.StreamEventKindContentBlockStop}}) {
		t.Fatal("fieldless lifecycle event should count as visible stream activity")
	}
	if hasVisibleChatStreamEvent([]model.StreamEvent{{Kind: model.StreamEventKindUsageDelta}}) {
		t.Fatal("metadata-only usage event must not start stream tracking")
	}

	inbound := &ChatInbound{}
	ctx := context.Background()
	output, err := inbound.TransformStreamEvents(ctx, []model.StreamEvent{{Kind: model.StreamEventKindContentBlockStop}})
	if err != nil || len(output) == 0 {
		t.Fatalf("visible lifecycle event should be forwarded as a Chat chunk, output=%q err=%v", output, err)
	}
	terminal, err := inbound.FinalizeStream(ctx)
	if !errors.Is(err, model.ErrIncompleteUpstreamStream) {
		t.Fatalf("visible event must make clean EOF incomplete, got %v", err)
	}
	if string(bytes.TrimSpace(terminal)) != "data: [DONE]" {
		t.Fatalf("expected one Chat terminator after visible event, got %q", terminal)
	}
}
