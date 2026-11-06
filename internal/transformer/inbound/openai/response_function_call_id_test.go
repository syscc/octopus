package openai

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/bestruirui/octopus/internal/transformer/model"
)

func TestResponseFunctionCallItemID(t *testing.T) {
	for _, tc := range []struct{ name, raw, wantID string }{
		{name: "synthesized"},
		{name: "native", raw: `[{"type":"function_call","id":"fc_native","call_id":"call_a"}]`, wantID: "fc_native"},
		{name: "invalid native", raw: `[{"type":"function_call","id":"call_a","call_id":"call_a"}]`},
		{name: "unrelated native", raw: `[{"type":"function_call","id":"fc_other","call_id":"call_other"}]`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resp := &model.InternalLLMResponse{
				RawResponsesOutputItems: json.RawMessage(tc.raw),
				Choices: []model.Choice{{Message: &model.Message{ToolCalls: []model.ToolCall{{
					ID: "call_a", Type: "function", Function: model.FunctionCall{Name: "first", Arguments: `{}`},
				}}}}},
			}
			item := convertToResponsesAPIResponse(resp).Output[0]
			if !strings.HasPrefix(item.ID, "fc_") || item.CallID != "call_a" || item.ID == item.CallID {
				t.Fatalf("function call must have independent fc_ item ID and unchanged call_id: %+v", item)
			}
			if tc.wantID != "" && item.ID != tc.wantID {
				t.Fatalf("native item ID = %q, want %q", item.ID, tc.wantID)
			}
		})
	}
}

func TestStreamInternalRawFunctionCallIDsMatchTerminal(t *testing.T) {
	for _, interrupted := range []bool{false, true} {
		name := "completed"
		if interrupted {
			name = "interrupted"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			inbound := &ResponseInbound{}
			start := chunkWithDelta("gpt", &model.Message{ToolCalls: []model.ToolCall{{Index: 0, ID: "call_a", Function: model.FunctionCall{Name: "first", Arguments: `{}`}}}})
			addedBytes, err := inbound.TransformStream(ctx, start)
			if err != nil {
				t.Fatal(err)
			}
			added := findEvent(parseSSEEvents(t, addedBytes), "response.output_item.added")
			raw := json.RawMessage(`[{"type":"reasoning","id":"rs_native","encrypted_content":"cipher","unknown_reasoning":{"large":9007199254740993123}},{"type":"function_call","id":"fc_native_a","call_id":"call_a","name":"first","arguments":"{}","unknown_tool":{"large":9007199254740993123}},{"type":"function_call","id":"bad_extra","call_id":"call_extra","name":"extra","arguments":"{}"}]`)
			originalRaw := append(json.RawMessage(nil), raw...)
			finish := chunkWithFinish("gpt", "tool_calls")
			finish.RawResponsesOutputItems = raw
			if interrupted {
				finish.Usage = nil
			}
			terminalBytes, err := inbound.TransformStream(ctx, finish)
			if err != nil {
				t.Fatal(err)
			}
			terminalType := "response.completed"
			if interrupted {
				terminalType = "response.incomplete"
				terminalBytes, err = inbound.FinalizeInterruptedStream(ctx)
				if !errors.Is(err, model.ErrIncompleteUpstreamStream) {
					t.Fatal(err)
				}
			}
			terminal := findEvent(parseSSEEvents(t, terminalBytes), terminalType)
			if terminal == nil || terminal.Response == nil {
				t.Fatalf("missing %s", terminalType)
			}
			if terminal.Response.Output[1].ID != added.Item.ID {
				t.Fatal("terminal item ID differs from added")
			}
			internal, err := inbound.GetInternalResponse(ctx)
			if err != nil || internal == nil {
				t.Fatalf("internal response: %v, %v", internal, err)
			}
			var cached, original []map[string]json.RawMessage
			if err := json.Unmarshal(internal.RawResponsesOutputItems, &cached); err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(originalRaw, &original); err != nil {
				t.Fatal(err)
			}
			if len(cached) != len(original) {
				t.Fatalf("cached raw item count = %d, want %d", len(cached), len(original))
			}
			for index, item := range cached {
				var id string
				if err := json.Unmarshal(item["id"], &id); err != nil {
					t.Fatal(err)
				}
				if id != terminal.Response.Output[index].ID {
					t.Fatalf("cache item %d ID %q != terminal ID %q", index, id, terminal.Response.Output[index].ID)
				}
				if len(item) != len(original[index]) {
					t.Fatalf("raw fields changed at item %d", index)
				}
				for key, value := range original[index] {
					if key != "id" && !bytes.Equal(item[key], value) {
						t.Fatalf("raw field %d.%s changed from %s to %s", index, key, value, item[key])
					}
				}
			}
			if !bytes.Equal(raw, originalRaw) || !bytes.Equal(finish.RawResponsesOutputItems, originalRaw) {
				t.Fatal("input raw buffer was mutated")
			}
			if start.Choices[0].Delta.ToolCalls[0].ID != "call_a" {
				t.Fatal("input tool call ID was mutated")
			}
		})
	}
}

func TestStreamInternalRawIDsUnchangedBeforeTerminal(t *testing.T) {
	ctx := context.Background()
	inbound := &ResponseInbound{}
	start := chunkWithDelta("gpt", &model.Message{ToolCalls: []model.ToolCall{{Index: 0, ID: "call_a", Function: model.FunctionCall{Name: "first", Arguments: `{}`}}}})
	if _, err := inbound.TransformStream(ctx, start); err != nil {
		t.Fatal(err)
	}
	raw := json.RawMessage(`[{"type":"function_call","id":"fc_native_a","call_id":"call_a","name":"first","arguments":"{}"}]`)
	finish := chunkWithFinish("gpt", "tool_calls")
	finish.Usage = nil
	finish.RawResponsesOutputItems = raw
	if _, err := inbound.TransformStream(ctx, finish); err != nil {
		t.Fatal(err)
	}
	response, err := inbound.GetInternalResponse(ctx)
	if err != nil || response == nil {
		t.Fatalf("internal response: %v, %v", response, err)
	}
	if !bytes.Equal(response.RawResponsesOutputItems, raw) {
		t.Fatalf("raw IDs changed before terminal: %s", response.RawResponsesOutputItems)
	}
}

func TestStreamInternalRawIDsPreservedForPassthroughSidecar(t *testing.T) {
	ctx := context.Background()
	inbound := &ResponseInbound{}
	inbound.PreservePassthroughOutputIDs()
	start := chunkWithDelta("gpt", &model.Message{ToolCalls: []model.ToolCall{{Index: 0, ID: "call_a", Function: model.FunctionCall{Name: "first", Arguments: `{}`}}}})
	if _, err := inbound.TransformStream(ctx, start); err != nil {
		t.Fatal(err)
	}
	raw := json.RawMessage(`[{"type":"function_call","id":"fc_native_a","call_id":"call_a","name":"first","arguments":"{}","unknown":9007199254740993123}]`)
	finish := chunkWithFinish("gpt", "tool_calls")
	finish.RawResponsesOutputItems = raw
	if _, err := inbound.TransformStream(ctx, finish); err != nil {
		t.Fatal(err)
	}
	response, err := inbound.GetInternalResponse(ctx)
	if err != nil || response == nil {
		t.Fatalf("internal response: %v, %v", response, err)
	}
	if !bytes.Equal(response.RawResponsesOutputItems, raw) {
		t.Fatalf("sidecar changed native raw IDs: %s", response.RawResponsesOutputItems)
	}
}

func TestStreamNativeFunctionCallItemID(t *testing.T) {
	for _, nativeID := range []string{"fc_native", "call_invalid", ""} {
		t.Run(nativeID, func(t *testing.T) {
			inbound := &ResponseInbound{}
			out, err := inbound.TransformStreamEvents(context.Background(), []model.StreamEvent{
				{Kind: model.StreamEventKindToolCallStart, ToolCall: &model.ToolCall{Index: 7, ID: "call_a", Function: model.FunctionCall{Name: "first", Arguments: `{"a":`}}, ContentBlock: &model.StreamContentBlock{ID: nativeID}},
				{Kind: model.StreamEventKindToolCallDelta, ToolCall: &model.ToolCall{Index: 7, Function: model.FunctionCall{Arguments: `1}`}}, ContentBlock: &model.StreamContentBlock{ID: "fc_do_not_replace"}},
				{Kind: model.StreamEventKindMessageStop, StopReason: model.ParseFinishReason("tool_calls")},
				{Kind: model.StreamEventKindUsageDelta, Usage: &model.Usage{TotalTokens: 1}},
			})
			if err != nil {
				t.Fatal(err)
			}
			events := parseSSEEvents(t, out)
			added := findEvent(events, "response.output_item.added")
			if added == nil || added.Item == nil {
				t.Fatal("missing added tool item")
			}
			itemID := added.Item.ID
			if !strings.HasPrefix(itemID, "fc_") || itemID == "fc_do_not_replace" || added.Item.CallID != "call_a" {
				t.Fatalf("invalid native tool item: %+v", added.Item)
			}
			if nativeID == "fc_native" && itemID != nativeID {
				t.Fatalf("native ID %q became %q", nativeID, itemID)
			}
			for _, event := range events {
				if event.Item != nil && event.Item.Type == "function_call" && event.Item.ID != itemID {
					t.Fatalf("tool item ID changed: %+v", event)
				}
				if event.ItemID != nil && *event.ItemID != itemID {
					t.Fatalf("tool argument ID changed: %+v", event)
				}
			}
			terminal := findEvent(events, "response.completed")
			if terminal == nil || terminal.Response == nil || len(terminal.Response.Output) != 1 || terminal.Response.Output[0].ID != itemID || terminal.Response.Output[0].Arguments != `{"a":1}` || terminal.Response.Output[0].CallID != "call_a" {
				t.Fatalf("invalid terminal output: %+v", terminal)
			}
		})
	}
}

func TestTerminalFunctionCallIDsWithoutEmittedItem(t *testing.T) {
	raw := json.RawMessage(`[{"type":"function_call","id":"fc_native","call_id":"call_native"},{"type":"function_call","id":"call_invalid","call_id":"call_invalid"}]`)
	originalRaw := string(raw)
	inbound := &ResponseInbound{terminalProviderExtensions: &model.ProviderExtensions{OpenAI: &model.OpenAIExtension{RawResponseItems: raw}}}
	items := inbound.terminalOutputItems()
	if len(items) != 2 || items[0].ID != "fc_native" || items[0].CallID != "call_native" || !strings.HasPrefix(items[1].ID, "fc_") || items[1].CallID != "call_invalid" {
		t.Fatalf("terminal native IDs: %+v", items)
	}
	if string(raw) != originalRaw {
		t.Fatal("terminal conversion mutated provider raw output")
	}
}

func TestStreamFunctionCallItemIDsStayConsistent(t *testing.T) {
	for _, interrupted := range []bool{false, true} {
		for _, rawTerminal := range []bool{false, true} {
			name := "completed"
			if interrupted {
				name = "interrupted"
			}
			if rawTerminal {
				name += "_raw"
			}
			t.Run(name, func(t *testing.T) {
				inbound := &ResponseInbound{}
				ctx := context.Background()
				var events []ResponsesStreamEvent
				for _, call := range []model.ToolCall{
					{Index: 7, ID: "call_a", Function: model.FunctionCall{Name: "first", Arguments: `{"a":`}},
					{Index: 3, ID: "call_b", Function: model.FunctionCall{Name: "second", Arguments: `{"b":`}},
					{Index: 7, Function: model.FunctionCall{Arguments: `1}`}},
					{Index: 9, Function: model.FunctionCall{Name: "anonymous", Arguments: `{"c":`}},
					{Index: 3, Function: model.FunctionCall{Arguments: `2}`}},
					{Index: 9, Function: model.FunctionCall{Arguments: `3}`}},
				} {
					out, err := inbound.TransformStream(ctx, chunkWithDelta("gpt", &model.Message{ToolCalls: []model.ToolCall{call}}))
					if err != nil {
						t.Fatal(err)
					}
					events = append(events, parseSSEEvents(t, out)...)
				}
				var raw json.RawMessage
				if rawTerminal {
					raw = json.RawMessage(`[{"type":"reasoning","id":"rs_native","encrypted_content":"cipher","summary":[{"type":"summary_text","text":"summary"}]},{"type":"function_call","id":"fc_native_a","call_id":"call_a","name":"first","arguments":"{\"a\":1}"},{"type":"function_call","id":"call_b","call_id":"call_b","name":"second","arguments":"{\"b\":2}"},{"type":"function_call","id":"bad_anonymous","name":"anonymous","arguments":"{\"c\":3}"},{"type":"function_call","id":"bad_extra","call_id":"call_extra","name":"extra","arguments":"{}"}]`)
				}
				var terminalBytes []byte
				if interrupted {
					if len(raw) > 0 {
						inbound.terminalProviderExtensions = &model.ProviderExtensions{OpenAI: &model.OpenAIExtension{RawResponseItems: raw}}
					}
					var err error
					terminalBytes, err = inbound.FinalizeInterruptedStream(ctx)
					if !errors.Is(err, model.ErrIncompleteUpstreamStream) {
						t.Fatalf("interruption error: %v", err)
					}
				} else {
					finish := chunkWithFinish("gpt", "tool_calls")
					finish.RawResponsesOutputItems = raw
					var err error
					terminalBytes, err = inbound.TransformStream(ctx, finish)
					if err != nil {
						t.Fatal(err)
					}
				}
				events = append(events, parseSSEEvents(t, terminalBytes)...)
				added := make(map[int]ResponsesItem)
				deltas := make(map[int]string)
				done := make(map[int]ResponsesItem)
				for _, event := range events {
					switch event.Type {
					case "response.output_item.added":
						if event.Item.Type != "function_call" {
							continue
						}
						if !strings.HasPrefix(event.Item.ID, "fc_") || event.Item.ID == event.Item.CallID {
							t.Fatalf("invalid added item: %+v", event.Item)
						}
						added[*event.OutputIndex] = *event.Item
					case "response.function_call_arguments.delta", "response.function_call_arguments.done":
						idx := *event.OutputIndex
						if event.ItemID == nil || *event.ItemID != added[idx].ID {
							t.Fatalf("%s ID = %v, added ID = %q", event.Type, event.ItemID, added[idx].ID)
						}
						if event.Type == "response.function_call_arguments.delta" {
							deltas[idx] += event.Delta
						} else if event.Arguments != deltas[idx] {
							t.Fatalf("done arguments %q differ from accumulated delta %q", event.Arguments, deltas[idx])
						}
					case "response.output_item.done":
						if event.Item.Type != "function_call" {
							continue
						}
						idx := *event.OutputIndex
						if event.Item.ID != added[idx].ID || event.Item.CallID != added[idx].CallID || event.Item.Arguments != deltas[idx] {
							t.Fatalf("done item differs from added/deltas: %+v", event.Item)
						}
						done[idx] = *event.Item
					}
				}
				if len(added) != 3 || len(done) != 3 {
					t.Fatalf("added=%d done=%d, want three", len(added), len(done))
				}
				for index, want := range []string{`{"a":1}`, `{"b":2}`, `{"c":3}`} {
					if done[index].Arguments != want || deltas[index] != want {
						t.Fatalf("tool %d arguments: done=%q deltas=%q, want %q", index, done[index].Arguments, deltas[index], want)
					}
				}
				terminalType := "response.completed"
				if interrupted {
					terminalType = "response.incomplete"
				}
				terminal := findEvent(events, terminalType)
				if terminal == nil || terminal.Response == nil {
					t.Fatalf("missing %s", terminalType)
				}
				matched := 0
				for index, item := range terminal.Response.Output {
					if !rawTerminal && item.ID != added[index].ID {
						t.Fatalf("terminal output[%d] ID %q != added ID %q", index, item.ID, added[index].ID)
					}
					if item.Type == "reasoning" {
						if item.ID != "rs_native" || item.EncryptedContent == nil || *item.EncryptedContent != "cipher" || len(item.Summary) != 1 || item.Summary[0].Text != "summary" {
							t.Fatalf("native reasoning changed: %+v", item)
						}
					}
					if item.Type != "function_call" {
						continue
					}
					if !strings.HasPrefix(item.ID, "fc_") {
						t.Fatalf("invalid terminal function_call ID: %+v", item)
					}
					for _, original := range added {
						if item.CallID == original.CallID {
							if item.ID != original.ID {
								t.Fatalf("terminal ID %q != emitted ID %q", item.ID, original.ID)
							}
							matched++
						}
					}
				}
				if matched != 3 {
					t.Fatalf("terminal matched %d emitted function calls, want three", matched)
				}
			})
		}
	}
}
