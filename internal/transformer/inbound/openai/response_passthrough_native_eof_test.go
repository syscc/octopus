package openai

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/bestruirui/octopus/internal/transformer/model"
	outbound "github.com/bestruirui/octopus/internal/transformer/outbound/openai"
)

func TestPassthroughNativeInterleavedEOF(t *testing.T) {
	for _, types := range [][2]string{{"message", "message"}, {"reasoning", "reasoning"}, {"message", "reasoning"}, {"reasoning", "message"}, {"message", "function_call"}, {"function_call", "message"}, {"reasoning", "function_call"}, {"function_call", "reasoning"}, {"function_call", "function_call"}} {
		for _, tc := range []struct {
			index     int
			omitIndex bool
			offset    int
			gap       int
		}{{index: -1}, {index: 0}, {index: 1}, {index: 1, omitIndex: true}, {index: -1, offset: 1}, {index: -1, offset: 3, gap: 2}, {index: 0, offset: 3, gap: 2}, {index: 1, omitIndex: true, offset: 3, gap: 2}} {
			rawDoneIndex := tc.index
			name := fmt.Sprintf("%s_%s/done_%d/missing_index_%t/offset_%d/gap_%d", types[0], types[1], rawDoneIndex, tc.omitIndex, tc.offset, tc.gap)
			t.Run(name, func(t *testing.T) {
				ctx := context.Background()
				responseOutbound := &outbound.ResponseOutbound{}
				inbound := &ResponseInbound{}
				inbound.PreservePassthroughOutputIDs()
				ids := [2]string{"msg_one", "msg_two"}

				for index, typ := range types {
					if typ == "reasoning" {
						ids[index] = fmt.Sprintf("rs_%d", index)
					}
					if typ == "function_call" {
						ids[index] = fmt.Sprintf("fc_%d", index)
					}
				}
				indices := [2]int{tc.offset, tc.offset + 1 + tc.gap}
				sequence := 40
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
					semantic, err := responseOutbound.TransformStreamEvent(ctx, body)
					if err != nil {
						t.Fatal(err)
					}
					if _, err = inbound.TransformStreamEvents(ctx, semantic); err != nil {
						t.Fatal(err)
					}
				}
				feed(map[string]any{"type": "response.created", "response": map[string]any{"id": "resp_native_eof", "model": "test", "created_at": 11, "output": []any{}, "status": "in_progress"}})
				for index := 0; index < tc.offset; index++ {
					feed(map[string]any{"type": "response.output_item.added", "output_index": index, "item": map[string]any{"type": "reasoning", "id": fmt.Sprintf("rs_empty_%d", index), "status": "in_progress"}})
				}
				add := func(index int) {
					feed(map[string]any{"type": "response.output_item.added", "output_index": indices[index], "item": map[string]any{"type": types[index], "id": ids[index], "status": "in_progress", "call_id": fmt.Sprintf("call_%d", index), "name": "lookup"}})
				}
				delta := func(index int, value string) {
					// Tool added events alone exercise EOF index reservation.
					if types[index] == "function_call" {
						return
					}
					typ := "response.output_text.delta"
					if types[index] == "reasoning" {
						typ = "response.reasoning_summary_text.delta"
					}
					feed(map[string]any{"type": typ, "output_index": indices[index], "item_id": ids[index], "content_index": 0, "summary_index": 0, "delta": value})
				}
				add(0)
				delta(0, "one")
				add(1)
				delta(1, "other")
				delta(0, "two")
				values := [2]string{"onetwo", "other"}
				for index, typ := range types {
					if typ == "function_call" {
						values[index] = ""
					}
				}
				if rawDoneIndex >= 0 {
					item := ResponsesItem{Type: types[rawDoneIndex], ID: ids[rawDoneIndex]}
					if item.Type == "message" {
						value := values[rawDoneIndex]
						item.Role = "assistant"
						item.Content = &ResponsesInput{Items: []ResponsesItem{{Type: "output_text", Text: &value}}}
					} else if item.Type == "function_call" {
						item.CallID = fmt.Sprintf("call_%d", rawDoneIndex)
						item.Name = "lookup"
						item.Arguments = values[rawDoneIndex]
					} else {
						item.Summary = []ResponsesReasoningSummary{{Type: "summary_text", Text: values[rawDoneIndex]}}
						cipher := "native_cipher"
						item.EncryptedContent = &cipher
					}
					doneEvent := map[string]any{"type": "response.output_item.done", "item": item}
					if !tc.omitIndex {
						doneEvent["output_index"] = indices[rawDoneIndex]
					}
					feed(doneEvent)
				}
				tail, err := inbound.FinalizeIncompleteStream(ctx, raw)
				if !errors.Is(err, model.ErrIncompleteUpstreamStream) {
					t.Fatalf("EOF error=%v", err)
				}
				for _, event := range parseSSEEvents(t, tail) {
					if event.SequenceNumber < sequence {
						t.Fatalf("closing event %s sequence=%d before next raw=%d", event.Type, event.SequenceNumber, sequence)
					}
					if event.ItemID != nil {
						for index, id := range ids {
							if *event.ItemID == id && (event.OutputIndex == nil || *event.OutputIndex != indices[index]) {
								t.Fatalf("misindexed closing part: %+v want=%d", event, indices[index])
							}
						}
					}
					if rawDoneIndex >= 0 && event.ItemID != nil && *event.ItemID == ids[rawDoneIndex] {
						t.Fatalf("already-done raw item received closing part: %+v", event)
					}
				}
				events := parseSSEEvents(t, append(append([]byte(nil), raw...), tail...))
				expectedIDs := make([]string, 0, tc.offset+2)
				expectedTypes := make(map[string]string, tc.offset+2)
				expectedIndices := make(map[string]int, tc.offset+2)
				for index := 0; index < tc.offset; index++ {
					id := fmt.Sprintf("rs_empty_%d", index)
					expectedIDs = append(expectedIDs, id)
					expectedTypes[id] = "reasoning"
					expectedIndices[id] = index
				}
				for index, id := range ids {
					expectedIDs = append(expectedIDs, id)
					expectedTypes[id] = types[index]
					expectedIndices[id] = indices[index]
				}
				done := map[string]int{}
				for _, event := range events {
					if event.Type != "response.output_item.done" {
						continue
					}
					if event.Item == nil {
						t.Fatalf("output_item.done missing item: %+v", event)
					}
					done[event.Item.ID]++
					expectedType, knownItem := expectedTypes[event.Item.ID]
					missingRawIndex := tc.omitIndex && event.Item.ID == ids[rawDoneIndex] && event.OutputIndex == nil && event.SequenceNumber < sequence
					if !knownItem || event.Item.Type != expectedType || (!missingRawIndex && (event.OutputIndex == nil || *event.OutputIndex != expectedIndices[event.Item.ID])) || done[event.Item.ID] != 1 {
						t.Fatalf("duplicate or misindexed done: %+v", event)
					}
					index := -1
					for expected, id := range ids {
						if event.Item.ID == id {
							index = expected
						}
					}
					if index < 0 {
						continue
					}
					value := ""
					if event.Item.Type == "message" {
						value = *event.Item.Content.Items[0].Text
					} else if event.Item.Type == "function_call" {
						value = event.Item.Arguments
					} else {
						value = event.Item.Summary[0].Text
					}
					if value != values[index] {
						t.Fatalf("done %s content=%q want=%q", event.Item.ID, value, values[index])
					}
				}
				terminal := findEvent(events, "response.incomplete")
				if terminal == nil || terminal.Response == nil || len(terminal.Response.Output) != len(expectedIDs) || len(done) != len(expectedIDs) {
					t.Fatalf("lost/duplicate terminal output: %+v done=%v", terminal, done)
				}
				terminalIDs := map[string]bool{}
				for outputIndex, id := range expectedIDs {
					item := terminal.Response.Output[outputIndex]
					if terminalIDs[item.ID] || item.ID != id || item.Type != expectedTypes[id] || done[id] != 1 {
						t.Fatalf("terminal item %d=%+v, want id=%q type=%q", outputIndex, item, id, expectedTypes[id])
					}
					terminalIDs[item.ID] = true
					if outputIndex < tc.offset {
						continue
					}
					index := outputIndex - tc.offset
					value := ""
					if item.Type == "message" {
						value = *item.Content.Items[0].Text
					} else if item.Type == "function_call" {
						value = item.Arguments
					} else {
						value = item.Summary[0].Text
					}
					if value != values[index] {
						t.Fatalf("terminal %s content=%q want=%q", item.ID, value, values[index])
					}
					if index == rawDoneIndex && item.Type == "reasoning" && (item.EncryptedContent == nil || *item.EncryptedContent != "native_cipher") {
						t.Fatal("raw done metadata lost")
					}
				}
				again, err := inbound.FinalizeIncompleteStream(ctx, raw)
				if err != nil || len(again) != 0 {
					t.Fatal("EOF finalization repeated")
				}
			})
		}
	}
}
