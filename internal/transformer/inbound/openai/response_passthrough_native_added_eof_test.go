package openai

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/bestruirui/octopus/internal/transformer/model"
	outbound "github.com/bestruirui/octopus/internal/transformer/outbound/openai"
)

func TestPassthroughNativeAddedItemsFinalizeAtEOF(t *testing.T) {
	for _, tc := range []struct {
		name  string
		items []ResponsesItem
	}{
		{
			name: "message_and_reasoning",
			items: []ResponsesItem{
				{ID: "msg_open", Type: "message", Role: "assistant"},
				{ID: "rs_open", Type: "reasoning"},
			},
		},
		{
			name: "reasoning_and_function_call",
			items: []ResponsesItem{
				{ID: "rs_open", Type: "reasoning"},
				{ID: "fc_open", Type: "function_call", CallID: "call_open", Name: "lookup"},
			},
		},
		{
			name: "function_call_and_message",
			items: []ResponsesItem{
				{ID: "fc_open", Type: "function_call", CallID: "call_open", Name: "lookup"},
				{ID: "msg_open", Type: "message", Role: "assistant"},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			inbound := &ResponseInbound{}
			inbound.PreservePassthroughOutputIDs()
			outbound := &outbound.ResponseOutbound{}
			indices := []int{3, 8}
			sequence := 20
			var raw []byte

			feed := func(event map[string]any) {
				t.Helper()
				event["sequence_number"] = sequence
				sequence++
				body, err := json.Marshal(event)
				if err != nil {
					t.Fatal(err)
				}
				raw = append(raw, formatSSEData(body)...)
				semantic, err := outbound.TransformStreamEvent(ctx, body)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := inbound.TransformStreamEvents(ctx, semantic); err != nil {
					t.Fatal(err)
				}
			}

			feed(map[string]any{
				"type": "response.created",
				"response": map[string]any{
					"id":         "resp_added_eof",
					"model":      "test",
					"created_at": 11,
					"output":     []any{},
					"status":     "in_progress",
				},
			})
			openStatus := "in_progress"
			for index, item := range tc.items {
				item.Status = &openStatus
				feed(map[string]any{
					"type":         "response.output_item.added",
					"output_index": indices[index],
					"item":         item,
				})
			}

			tail, err := inbound.FinalizeIncompleteStream(ctx, raw)
			if !errors.Is(err, model.ErrIncompleteUpstreamStream) {
				t.Fatalf("EOF error = %v", err)
			}
			events := parseSSEEvents(t, tail)
			if findEvent(events, "response.output_item.added") != nil {
				t.Fatalf("EOF closing events must not add a synthetic item: %v", eventTypes(events))
			}

			done := map[string]*ResponsesStreamEvent{}
			doneCount := 0
			for _, event := range events {
				if event.Type == "response.output_item.done" {
					doneCount++
				}
			}
			for index := range tc.items {
				for _, event := range events {
					if event.Type == "response.output_item.done" && event.Item != nil && event.Item.ID == tc.items[index].ID {
						eventCopy := event
						done[event.Item.ID] = &eventCopy
					}
				}
				closed := done[tc.items[index].ID]
				if closed == nil || closed.Item.Type != tc.items[index].Type || closed.OutputIndex == nil || *closed.OutputIndex != indices[index] {
					t.Fatalf("item %q closing event = %+v, want type=%q output_index=%d", tc.items[index].ID, closed, tc.items[index].Type, indices[index])
				}
			}
			if doneCount != len(tc.items) || len(done) != len(tc.items) {
				t.Fatalf("unexpected output_item.done events: %+v", done)
			}

			incomplete := findEvent(events, "response.incomplete")
			if incomplete == nil || incomplete.Response == nil || len(incomplete.Response.Output) != len(tc.items) {
				t.Fatalf("incomplete output = %+v", incomplete)
			}
			for index, item := range incomplete.Response.Output {
				if item.ID != tc.items[index].ID || item.Type != tc.items[index].Type {
					t.Fatalf("incomplete output[%d] = %+v, want id=%q type=%q", index, item, tc.items[index].ID, tc.items[index].Type)
				}
			}
		})
	}
}
