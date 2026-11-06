package openai

import (
	"context"
	"errors"
	"testing"

	"github.com/samber/lo"

	"github.com/bestruirui/octopus/internal/transformer/model"
)

func TestStreamMixedOutputItemsKeepAssignedOrder(t *testing.T) {
	for _, content := range []struct {
		name   string
		deltas []*model.Message
	}{
		{name: "text", deltas: []*model.Message{{Content: model.MessageContent{Content: lo.ToPtr("answer")}}}},
		{name: "reasoning", deltas: []*model.Message{{ReasoningContent: lo.ToPtr("thinking")}}},
		{name: "text_reasoning_text", deltas: []*model.Message{
			{Content: model.MessageContent{Content: lo.ToPtr("first")}},
			{ReasoningContent: lo.ToPtr("thinking")},
			{Content: model.MessageContent{Content: lo.ToPtr("last")}},
		}},
	} {
		for _, interrupted := range []bool{false, true} {
			name := content.name + "/completed"
			if interrupted {
				name = content.name + "/interrupted"
			}
			t.Run(name, func(t *testing.T) {
				ctx := context.Background()
				inbound := &ResponseInbound{}
				var events []ResponsesStreamEvent
				for _, call := range []model.ToolCall{
					{Index: 7, ID: "call_a", Function: model.FunctionCall{Name: "first", Arguments: `{"a":`}},
					{Index: 3, ID: "call_b", Function: model.FunctionCall{Name: "second", Arguments: `{"b":`}},
					{Index: 7, Function: model.FunctionCall{Arguments: `1}`}},
					{Index: 3, Function: model.FunctionCall{Arguments: `2}`}},
				} {
					out, err := inbound.TransformStream(ctx, chunkWithDelta("gpt", &model.Message{ToolCalls: []model.ToolCall{call}}))
					if err != nil {
						t.Fatal(err)
					}
					events = append(events, parseSSEEvents(t, out)...)
				}
				for _, delta := range content.deltas {
					out, err := inbound.TransformStream(ctx, chunkWithDelta("gpt", delta))
					if err != nil {
						t.Fatal(err)
					}
					events = append(events, parseSSEEvents(t, out)...)
				}
				var terminalBytes []byte
				var err error
				terminalType := "response.completed"
				if interrupted {
					terminalType = "response.incomplete"
					terminalBytes, err = inbound.FinalizeInterruptedStream(ctx)
					if !errors.Is(err, model.ErrIncompleteUpstreamStream) {
						t.Fatalf("unexpected interrupted error: %v", err)
					}
				} else {
					terminalBytes, err = inbound.TransformStream(ctx, chunkWithFinish("gpt", "tool_calls"))
					if err != nil {
						t.Fatal(err)
					}
				}
				events = append(events, parseSSEEvents(t, terminalBytes)...)
				added := make(map[int]ResponsesItem)
				done := make(map[int]ResponsesItem)
				for _, event := range events {
					if event.Item != nil && event.OutputIndex != nil {
						index := *event.OutputIndex
						switch event.Type {
						case "response.output_item.added":
							added[index] = *event.Item
						case "response.output_item.done":
							if event.Item.ID != added[index].ID {
								t.Fatalf("done[%d] ID %q != added ID %q", index, event.Item.ID, added[index].ID)
							}
							done[index] = *event.Item
						}
					}
					if event.ItemID != nil && event.OutputIndex != nil && *event.ItemID != added[*event.OutputIndex].ID {
						t.Fatalf("%s item ID/index differ from added", event.Type)
					}
				}
				terminal := findEvent(events, terminalType)
				if terminal == nil || terminal.Response == nil {
					t.Fatalf("missing %s", terminalType)
				}
				if len(added) != 2+len(content.deltas) || len(done) != len(added) || len(terminal.Response.Output) != len(added) {
					t.Fatalf("output count mismatch: added=%d done=%d terminal=%d", len(added), len(done), len(terminal.Response.Output))
				}
				for index, item := range terminal.Response.Output {
					if item.ID != added[index].ID || item.ID != done[index].ID {
						t.Fatalf("terminal[%d] %s ID %q != assigned ID %q", index, item.Type, item.ID, added[index].ID)
					}
					if item.Type == "function_call" && (item.Arguments != done[index].Arguments || item.CallID != added[index].CallID) {
						t.Fatalf("tool arguments/association changed at output[%d]", index)
					}
				}
			})
		}
	}
}

func TestStreamAdoptedOutputItemsKeepNativeOrder(t *testing.T) {
	inbound := &ResponseInbound{}
	first := ResponsesItem{ID: "fc_first", Type: "function_call", CallID: "call_first"}
	second := ResponsesItem{ID: "fc_second", Type: "function_call", CallID: "call_second"}
	inbound.recordCompletedOutputItem(0, first)
	inbound.recordCompletedOutputItem(1, second)
	inbound.adoptPassthroughCompletedOutput([]ResponsesItem{second, first})
	output := inbound.finalOutputItems()
	if len(output) != 2 || output[0].ID != second.ID || output[1].ID != first.ID {
		t.Fatalf("adopted native order changed: %+v", output)
	}
}
