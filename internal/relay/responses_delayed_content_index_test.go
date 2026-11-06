package relay

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	openaiInbound "github.com/bestruirui/octopus/internal/transformer/inbound/openai"
	openaiOutbound "github.com/bestruirui/octopus/internal/transformer/outbound/openai"
)

func TestResponsesDelayedContentDeltaKeepsToolOutputIndex(t *testing.T) {
	for _, tc := range []struct {
		name, itemType, itemID, eventType, part string
	}{
		{"text", "message", "msg_delayed", "response.output_text.delta", `"content":[{"type":"output_text","text":"answer"}]`},
		{"refusal", "message", "msg_delayed", "response.refusal.delta", `"content":[{"type":"refusal","refusal":"answer"}]`},
		{"reasoning", "reasoning", "rs_delayed", "response.reasoning_summary_text.delta", `"summary":[{"type":"summary_text","text":"answer"}]`},
	} {
		for _, offset := range []int{0, 1} {
			t.Run(fmt.Sprintf("%s/offset_%d", tc.name, offset), func(t *testing.T) {
				ctx := context.Background()
				upstream, downstream := &openaiOutbound.ResponseOutbound{}, &openaiInbound.ResponseInbound{}
				content := fmt.Sprintf(`{"type":%q,"id":%q,%s}`, tc.itemType, tc.itemID, tc.part)
				tool := `{"type":"function_call","id":"fc_delayed","call_id":"call_delayed","name":"lookup","arguments":""}`
				output := content + "," + tool
				if offset > 0 {
					output = `{"type":"reasoning","id":"rs_raw_only","summary":[]},` + output
				}
				frames := []string{
					`{"type":"response.created","response":{"id":"resp_delayed","model":"test","status":"in_progress","output":[]}}`,
					fmt.Sprintf(`{"type":"response.output_item.added","output_index":%d,"item":{"type":%q,"id":%q}}`, offset, tc.itemType, tc.itemID),
					fmt.Sprintf(`{"type":"response.output_item.added","output_index":%d,"item":%s}`, offset+1, tool),
					fmt.Sprintf(`{"type":%q,"output_index":%d,"item_id":%q,"content_index":0,"summary_index":0,"delta":"answer"}`, tc.eventType, offset, tc.itemID),
					`{"type":"response.completed","response":{"id":"resp_delayed","model":"test","status":"completed","output":[` + output + `],"usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}}`,
				}
				var wire []byte
				for _, frame := range frames {
					events, err := upstream.TransformStreamEvent(ctx, []byte(frame))
					if err != nil {
						t.Fatal(err)
					}
					encoded, err := downstream.TransformStreamEvents(ctx, events)
					if err != nil {
						t.Fatal(err)
					}
					wire = append(wire, encoded...)
				}
				indices := map[string]int{tc.itemID: offset, "fc_delayed": offset + 1}
				added, done := map[string]int{}, map[string]int{}
				for _, event := range responseBoundarySSEEvents(t, wire) {
					id := event.ItemID
					if event.Item != nil {
						id = messageIDString(t, event.Item["id"])
					}
					if expected, known := indices[id]; known && (event.OutputIndex == nil || *event.OutputIndex != expected) {
						t.Fatalf("%s item %s index=%v, want %d", event.Type, id, event.OutputIndex, expected)
					}
					switch event.Type {
					case "response.output_item.added":
						added[id]++
					case "response.output_item.done":
						done[id]++
					case "response.completed":
						items := messageIDPayloadItems(t, event.Response, "output")
						for id, index := range indices {
							if len(items) <= index || messageIDString(t, items[index]["id"]) != id {
								t.Fatalf("terminal item %s is missing at index %d", id, index)
							}
						}
					}
				}
				for id := range indices {
					if added[id] != 1 || done[id] != 1 {
						t.Fatalf("item %s lifecycle count: added=%d done=%d", id, added[id], done[id])
					}
				}
				cached, err := downstream.GetInternalResponse(ctx)
				if err != nil || cached == nil {
					t.Fatalf("cached response: %v %v", cached, err)
				}
				var items []map[string]json.RawMessage
				if err := json.Unmarshal(cached.RawResponsesOutputItems, &items); err != nil {
					t.Fatal(err)
				}
				for id, index := range indices {
					if len(items) <= index || messageIDString(t, items[index]["id"]) != id {
						t.Fatalf("cached item %s is missing at index %d", id, index)
					}
				}
			})
		}
	}
}
