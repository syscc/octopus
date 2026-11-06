package openai

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/bestruirui/octopus/internal/transformer/model"
	"github.com/samber/lo"
)

func TestResponseMessageReasoningItemIDs(t *testing.T) {
	for _, tc := range []struct{ name, raw, messageID, reasoningID string }{
		{name: "synthesized"},
		{name: "native", raw: `[{"type":"message","id":"msg_native"},{"type":"reasoning","id":"rs_native","encrypted_content":"cipher"}]`, messageID: "msg_native", reasoningID: "rs_native"},
		{name: "legacy", raw: `[{"type":"reasoning","id":"item_old","encrypted_content":"cipher","unknown":9007199254740993123},{"type":"message","id":"item_old_message","unknown":{"keep":true}}]`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw := json.RawMessage(tc.raw)
			original := append(json.RawMessage(nil), raw...)
			response := &model.InternalLLMResponse{RawResponsesOutputItems: raw, Choices: []model.Choice{{Message: &model.Message{
				Content: model.MessageContent{Content: lo.ToPtr("answer")}, ReasoningContent: lo.ToPtr("thought"),
			}}}}
			inbound := &ResponseInbound{}
			body, err := inbound.TransformResponse(context.Background(), response)
			if err != nil {
				t.Fatal(err)
			}
			var result ResponsesResponse
			if err := json.Unmarshal(body, &result); err != nil {
				t.Fatal(err)
			}
			for _, item := range result.Output {
				prefix, want := "msg_", tc.messageID
				if item.Type == "reasoning" {
					prefix, want = "rs_", tc.reasoningID
				}
				if !strings.HasPrefix(item.ID, prefix) || (want != "" && item.ID != want) {
					t.Fatalf("invalid %s ID %q, want prefix %s / %q", item.Type, item.ID, prefix, want)
				}
			}
			stored, err := inbound.GetInternalResponse(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			assertRawItemIDs(t, stored.RawResponsesOutputItems, original, result.Output)
			if !bytes.Equal(raw, original) || !bytes.Equal(response.RawResponsesOutputItems, original) {
				t.Fatal("caller raw modified")
			}
		})
	}
}

func assertRawItemIDs(t *testing.T, cached, original json.RawMessage, output []ResponsesItem) {
	t.Helper()
	if len(original) == 0 {
		return
	}
	var before, after []map[string]json.RawMessage
	if err := json.Unmarshal(original, &before); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(cached, &after); err != nil {
		t.Fatal(err)
	}
	if len(before) != len(after) {
		t.Fatal("raw item count changed")
	}
	byType := map[string][]string{}
	for _, item := range output {
		byType[item.Type] = append(byType[item.Type], item.ID)
	}
	for index, item := range after {
		var typ, id string
		json.Unmarshal(item["type"], &typ)
		json.Unmarshal(item["id"], &id)
		if len(byType[typ]) == 0 || id != byType[typ][0] {
			t.Fatalf("raw %s ID %q differs from sent IDs %v", typ, id, byType[typ])
		}
		byType[typ] = byType[typ][1:]
		if len(item) != len(before[index]) {
			t.Fatal("raw fields changed")
		}
		for key, value := range before[index] {
			if key != "id" && !bytes.Equal(item[key], value) {
				t.Fatalf("raw field %s modified", key)
			}
		}
	}
}

func TestStreamMessageReasoningItemIDLifecycle(t *testing.T) {
	for _, native := range []bool{false, true} {
		t.Run(map[bool]string{false: "synthesized", true: "native"}[native], func(t *testing.T) {
			inbound := &ResponseInbound{}
			ctx := context.Background()
			var events []ResponsesStreamEvent
			rawItems := []ResponsesItem{}
			for _, tc := range []struct{ typ, id, text, refusal, thinking string }{
				{"message", "msg_first", "answer", "", ""},
				{"message", "msg_second", "", "refused", ""},
				{"reasoning", "rs_first", "", "", "thought"},
				{"reasoning", "rs_second", "", "", "next thought"},
			} {
				delta := &model.StreamDelta{Text: tc.text, Refusal: tc.refusal, Thinking: tc.thinking}
				kind := model.StreamEventKindTextDelta
				if tc.typ == "reasoning" {
					kind = model.StreamEventKindThinkingDelta
				}
				event := model.StreamEvent{Kind: kind, Delta: delta}
				if native {
					event.ContentBlock = &model.StreamContentBlock{Type: tc.typ, ID: tc.id}
				}
				out, err := inbound.TransformStreamEvents(ctx, []model.StreamEvent{event})
				if err != nil {
					t.Fatal(err)
				}
				events = append(events, parseSSEEvents(t, out)...)
				if native {
					rawItems = append(rawItems, ResponsesItem{Type: tc.typ, ID: tc.id})
				}
			}
			finish := chunkWithFinish("gpt", "stop")
			if native {
				finish.RawResponsesOutputItems, _ = json.Marshal(rawItems)
			}
			out, err := inbound.TransformStream(ctx, finish)
			if err != nil {
				t.Fatal(err)
			}
			events = append(events, parseSSEEvents(t, out)...)
			added := map[int]ResponsesItem{}
			done := map[int]bool{}
			for _, event := range events {
				if event.Type == "response.output_item.added" {
					added[*event.OutputIndex] = *event.Item
				}
				if event.Item != nil && event.Item.Type != "" {
					prefix := "msg_"
					if event.Item.Type == "reasoning" {
						prefix = "rs_"
					}
					if !strings.HasPrefix(event.Item.ID, prefix) {
						t.Fatalf("invalid %s ID %q", event.Item.Type, event.Item.ID)
					}
					if event.Type == "response.output_item.done" {
						if event.Item.ID != added[*event.OutputIndex].ID {
							t.Fatal("done ID differs")
						}
						done[*event.OutputIndex] = true
					}
				}
				if event.ItemID != nil && *event.ItemID != added[*event.OutputIndex].ID {
					t.Fatalf("%s delta/done ID differs", event.Type)
				}
			}
			terminal := findEvent(events, "response.completed")
			want := 2
			if native {
				want = 4
			}
			if len(added) != want || len(done) != want || terminal == nil || len(terminal.Response.Output) != want {
				t.Fatalf("item boundary lost: added=%d done=%d terminal=%+v", len(added), len(done), terminal)
			}
			for index, item := range terminal.Response.Output {
				if item.ID != added[index].ID || (native && item.ID != rawItems[index].ID) {
					t.Fatalf("terminal/native ID differs at %d", index)
				}
			}
			stored, err := inbound.GetInternalResponse(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if native {
				assertRawItemIDs(t, stored.RawResponsesOutputItems, finish.RawResponsesOutputItems, terminal.Response.Output)
			}
		})
	}
}

func TestStreamRawOnlyMessageReasoningIDs(t *testing.T) {
	inbound := &ResponseInbound{}
	raw := json.RawMessage(`[{"type":"message","id":"item_old","role":"assistant","content":[],"unknown":9007199254740993123},{"type":"reasoning","id":"item_old_r","encrypted_content":"cipher","summary":[]}]`)
	finish := chunkWithFinish("gpt", "stop")
	finish.RawResponsesOutputItems = raw
	out, err := inbound.TransformStream(context.Background(), finish)
	if err != nil {
		t.Fatal(err)
	}
	terminal := findEvent(parseSSEEvents(t, out), "response.completed")
	if terminal == nil || len(terminal.Response.Output) != 2 {
		t.Fatal("missing raw-only terminal")
	}
	for _, item := range terminal.Response.Output {
		prefix := "msg_"
		if item.Type == "reasoning" {
			prefix = "rs_"
		}
		if !strings.HasPrefix(item.ID, prefix) {
			t.Fatalf("raw-only %s invalid ID %q", item.Type, item.ID)
		}
	}
	stored, err := inbound.GetInternalResponse(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	assertRawItemIDs(t, stored.RawResponsesOutputItems, raw, terminal.Response.Output)
}

func TestResponseNativeItemIDsFollowTypeOrder(t *testing.T) {
	raw := json.RawMessage(`[{"type":"message","id":"item_legacy"},{"type":"reasoning","id":"rs_first"},{"type":"message","id":"msg_second"},{"type":"reasoning","id":"rs_second"}]`)
	response := &model.InternalLLMResponse{RawResponsesOutputItems: raw, Choices: []model.Choice{
		{Message: &model.Message{Content: model.MessageContent{Content: lo.ToPtr("one")}, ReasoningContent: lo.ToPtr("first")}},
		{Message: &model.Message{Content: model.MessageContent{Content: lo.ToPtr("two")}, ReasoningContent: lo.ToPtr("second")}},
	}}
	output := convertToResponsesAPIResponse(response).Output
	if len(output) != 4 || output[0].ID != "rs_first" || !strings.HasPrefix(output[1].ID, "msg_") || output[1].ID == "msg_second" || output[2].ID != "rs_second" || output[3].ID != "msg_second" {
		t.Fatalf("native type/order association lost: %+v", output)
	}
}

func TestStreamTerminalPreservesRawOnlyNativeContentIDs(t *testing.T) {
	inbound := &ResponseInbound{}
	raw := json.RawMessage(`[{"type":"message","id":"msg_native","role":"assistant","content":[]},{"type":"reasoning","id":"rs_native","summary":[],"encrypted_content":"cipher"}]`)
	finish := chunkWithFinish("gpt", "stop")
	finish.RawResponsesOutputItems = raw
	out, err := inbound.TransformStream(context.Background(), finish)
	if err != nil {
		t.Fatal(err)
	}
	terminal := findEvent(parseSSEEvents(t, out), "response.completed")
	if terminal == nil || terminal.Response.Output[0].ID != "msg_native" || terminal.Response.Output[1].ID != "rs_native" {
		t.Fatalf("raw-only native IDs replaced: %+v", terminal)
	}
}

func TestStreamLegacyContentIDsMatchCache(t *testing.T) {
	for _, interrupted := range []bool{false, true} {
		t.Run(map[bool]string{false: "completed", true: "interrupted"}[interrupted], func(t *testing.T) {
			inbound := &ResponseInbound{}
			ctx := context.Background()
			var events []ResponsesStreamEvent
			for _, delta := range []*model.Message{{ReasoningContent: lo.ToPtr("thought")}, {Content: model.MessageContent{Content: lo.ToPtr("answer")}}} {
				out, err := inbound.TransformStream(ctx, chunkWithDelta("gpt", delta))
				if err != nil {
					t.Fatal(err)
				}
				events = append(events, parseSSEEvents(t, out)...)
			}
			raw := json.RawMessage(`[{"type":"reasoning","id":"item_old_r","summary":[],"encrypted_content":"cipher","unknown":9007199254740993123},{"type":"message","id":"item_old_m","role":"assistant","content":[{"type":"output_text","text":"answer","annotations":[]}],"unknown":{"keep":true}}]`)
			original := append(json.RawMessage(nil), raw...)
			finish := chunkWithFinish("gpt", "stop")
			finish.RawResponsesOutputItems = raw
			if interrupted {
				finish.Usage = nil
			}
			out, err := inbound.TransformStream(ctx, finish)
			if err != nil {
				t.Fatal(err)
			}
			if interrupted {
				out, err = inbound.FinalizeInterruptedStream(ctx)
				if err != model.ErrIncompleteUpstreamStream {
					t.Fatalf("interrupt error: %v", err)
				}
			}
			events = append(events, parseSSEEvents(t, out)...)
			typ := "response.completed"
			if interrupted {
				typ = "response.incomplete"
			}
			terminal := findEvent(events, typ)
			if terminal == nil {
				t.Fatal("missing terminal")
			}
			added := []*ResponsesItem{}
			for _, event := range events {
				if event.Type == "response.output_item.added" {
					added = append(added, event.Item)
				}
			}
			for index, item := range terminal.Response.Output {
				if item.ID != added[index].ID {
					t.Fatal("terminal replaced an emitted ID")
				}
			}
			if terminal.Response.Output[0].EncryptedContent == nil || *terminal.Response.Output[0].EncryptedContent != "cipher" {
				t.Fatal("reasoning metadata lost")
			}
			stored, err := inbound.GetInternalResponse(ctx)
			if err != nil || stored == nil {
				t.Fatalf("stored: %v %v", stored, err)
			}
			assertRawItemIDs(t, stored.RawResponsesOutputItems, original, terminal.Response.Output)
			if !bytes.Equal(finish.RawResponsesOutputItems, original) {
				t.Fatal("caller raw mutated")
			}
		})
	}
}

func TestMessageReasoningPassthroughCachePreservesIDs(t *testing.T) {
	raw := json.RawMessage(`[{"type":"message","id":"item_native","unknown":9007199254740993123},{"type":"reasoning","id":"item_native_r","encrypted_content":"cipher"}]`)
	for _, stream := range []bool{false, true} {
		inbound := &ResponseInbound{}
		inbound.PreservePassthroughOutputIDs()
		response := &model.InternalLLMResponse{RawResponsesOutputItems: raw, Choices: []model.Choice{{Message: &model.Message{Content: model.MessageContent{Content: lo.ToPtr("answer")}, ReasoningContent: lo.ToPtr("thought")}}}}
		if stream {
			response = chunkWithFinish("gpt", "stop")
			response.RawResponsesOutputItems = raw
			if _, err := inbound.TransformStream(context.Background(), response); err != nil {
				t.Fatal(err)
			}
		} else if _, err := inbound.TransformResponse(context.Background(), response); err != nil {
			t.Fatal(err)
		}
		stored, err := inbound.GetInternalResponse(context.Background())
		if err != nil || stored == nil || !bytes.Equal(stored.RawResponsesOutputItems, raw) {
			t.Fatalf("passthrough native cache changed: %v %v", stored, err)
		}
	}
}

func TestTerminalNativeIdentityBeforeTypeFallback(t *testing.T) {
	inbound := &ResponseInbound{}
	out, err := inbound.TransformStreamEvents(context.Background(), []model.StreamEvent{{Kind: model.StreamEventKindTextDelta, Delta: &model.StreamDelta{Text: "sent"}, ContentBlock: &model.StreamContentBlock{Type: "message", ID: "msg_sent"}}})
	if err != nil {
		t.Fatal(err)
	}
	added := findEvent(parseSSEEvents(t, out), "response.output_item.added")
	raw := json.RawMessage(`[{"type":"message","id":"msg_raw_only","role":"assistant","content":[]},{"type":"message","id":"msg_sent","role":"assistant","content":[]}]`)
	finish := chunkWithFinish("gpt", "stop")
	finish.RawResponsesOutputItems = raw
	out, err = inbound.TransformStream(context.Background(), finish)
	if err != nil {
		t.Fatal(err)
	}
	terminal := findEvent(parseSSEEvents(t, out), "response.completed")
	if terminal == nil || terminal.Response.Output[0].ID != "msg_raw_only" || terminal.Response.Output[1].ID != added.Item.ID {
		t.Fatalf("native identity lost: %+v", terminal)
	}
	stored, err := inbound.GetInternalResponse(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	assertRawItemIDs(t, stored.RawResponsesOutputItems, raw, terminal.Response.Output)
}

func TestStreamContentIDsUnchangedBeforeTerminal(t *testing.T) {
	inbound := &ResponseInbound{}
	ctx := context.Background()
	if _, err := inbound.TransformStream(ctx, chunkWithDelta("gpt", &model.Message{Content: model.MessageContent{Content: lo.ToPtr("answer")}})); err != nil {
		t.Fatal(err)
	}
	raw := json.RawMessage(`[{"type":"message","id":"item_original","content":[]},{"type":"reasoning","id":"item_original_r","summary":[]}]`)
	finish := chunkWithFinish("gpt", "stop")
	finish.Usage = nil
	finish.RawResponsesOutputItems = raw
	if _, err := inbound.TransformStream(ctx, finish); err != nil {
		t.Fatal(err)
	}
	stored, err := inbound.GetInternalResponse(ctx)
	if err != nil || stored == nil || !bytes.Equal(stored.RawResponsesOutputItems, raw) {
		t.Fatalf("preterminal cache changed: %v %v", stored, err)
	}
}

func TestStreamSameNativeItemIDKeepsOneItem(t *testing.T) {
	inbound := &ResponseInbound{}
	ctx := context.Background()
	var events []ResponsesStreamEvent
	for _, event := range []model.StreamEvent{
		{Kind: model.StreamEventKindTextDelta, Delta: &model.StreamDelta{Text: "one"}, ContentBlock: &model.StreamContentBlock{Type: "message", ID: "msg_one"}},
		{Kind: model.StreamEventKindTextDelta, Delta: &model.StreamDelta{Text: "two"}, ContentBlock: &model.StreamContentBlock{Type: "message", ID: "msg_one"}},
		{Kind: model.StreamEventKindThinkingDelta, Delta: &model.StreamDelta{Thinking: "first"}, ContentBlock: &model.StreamContentBlock{Type: "reasoning", ID: "rs_one"}},
		{Kind: model.StreamEventKindThinkingDelta, Delta: &model.StreamDelta{Thinking: "second"}, ContentBlock: &model.StreamContentBlock{Type: "reasoning", ID: "rs_one"}},
		{Kind: model.StreamEventKindMessageStop, StopReason: model.ParseFinishReason("stop")},
		{Kind: model.StreamEventKindUsageDelta, Usage: &model.Usage{TotalTokens: 1}},
	} {
		out, err := inbound.TransformStreamEvents(ctx, []model.StreamEvent{event})
		if err != nil {
			t.Fatal(err)
		}
		events = append(events, parseSSEEvents(t, out)...)
	}
	terminal := findEvent(events, "response.completed")
	if terminal == nil || len(terminal.Response.Output) != 2 || terminal.Response.Output[0].ID != "msg_one" || terminal.Response.Output[1].ID != "rs_one" || *terminal.Response.Output[0].Content.Items[0].Text != "onetwo" || terminal.Response.Output[1].Summary[0].Text != "firstsecond" {
		t.Fatalf("same native item split: %+v", terminal)
	}
}

func TestRawContentIDAlignmentSkipsMalformedToolCallID(t *testing.T) {
	raw := json.RawMessage(`[{"type":"function_call","id":"item_bad","call_id":123},{"type":"message","id":"item_message"}]`)
	aligned := alignRawResponseItemIDs(raw, []ResponsesItem{{Type: "function_call", ID: "fc_empty"}, {Type: "message", ID: "msg_sent"}})
	var items []map[string]json.RawMessage
	if err := json.Unmarshal(aligned, &items); err != nil {
		t.Fatal(err)
	}
	if string(items[0]["id"]) != `"item_bad"` || string(items[0]["call_id"]) != "123" || string(items[1]["id"]) != `"msg_sent"` {
		t.Fatalf("malformed tool was matched: %s", aligned)
	}
}

func TestStreamNativeContentInterleaving(t *testing.T) {
	text := func(id, value string) model.StreamEvent {
		return model.StreamEvent{Kind: model.StreamEventKindTextDelta, Delta: &model.StreamDelta{Text: value}, ContentBlock: &model.StreamContentBlock{Type: "message", ID: id}}
	}
	thinking := func(id, value string) model.StreamEvent {
		return model.StreamEvent{Kind: model.StreamEventKindThinkingDelta, Delta: &model.StreamDelta{Thinking: value}, ContentBlock: &model.StreamContentBlock{Type: "reasoning", ID: id}}
	}
	tool := model.StreamEvent{Kind: model.StreamEventKindToolCallStart, ToolCall: &model.ToolCall{Index: 7, ID: "call_one", Function: model.FunctionCall{Name: "tool", Arguments: "{"}}, ContentBlock: &model.StreamContentBlock{Type: "function_call", ID: "fc_one"}}
	for _, tc := range []struct {
		name        string
		input       []model.StreamEvent
		ids, values []string
	}{
		{"message_tool_message", []model.StreamEvent{text("msg_one", "one"), tool, text("msg_one", "two")}, []string{"msg_one", "fc_one"}, []string{"onetwo", "{}"}},
		{"reasoning_tool_reasoning", []model.StreamEvent{thinking("rs_one", "one"), tool, thinking("rs_one", "two")}, []string{"rs_one", "fc_one"}, []string{"onetwo", "{}"}},
		{"message_message_message", []model.StreamEvent{text("msg_one", "one"), text("msg_two", "other"), text("msg_one", "two")}, []string{"msg_one", "msg_two"}, []string{"onetwo", "other"}},
		{"message_reasoning_message", []model.StreamEvent{text("msg_one", "one"), thinking("rs_one", "thought"), text("msg_one", "two")}, []string{"msg_one", "rs_one"}, []string{"onetwo", "thought"}},
	} {
		for _, interrupted := range []bool{false, true} {
			for _, withRaw := range []bool{false, true} {
				name := tc.name + map[bool]string{false: "/completed", true: "/interrupted"}[interrupted] + map[bool]string{false: "/synthetic_terminal", true: "/native_terminal"}[withRaw]
				t.Run(name, func(t *testing.T) {
					inbound := &ResponseInbound{}
					ctx := context.Background()
					var events []ResponsesStreamEvent
					inputs := append([]model.StreamEvent(nil), tc.input...)
					if tc.ids[1] == "fc_one" {
						inputs = append(inputs, model.StreamEvent{Kind: model.StreamEventKindToolCallDelta, ToolCall: &model.ToolCall{Index: 7, Function: model.FunctionCall{Arguments: "}"}}})
					}
					for _, event := range inputs {
						out, err := inbound.TransformStreamEvents(ctx, []model.StreamEvent{event})
						if err != nil {
							t.Fatal(err)
						}
						events = append(events, parseSSEEvents(t, out)...)
					}
					finish := chunkWithFinish("gpt", "stop")
					if withRaw {
						rawItems := []ResponsesItem{}
						for index, id := range tc.ids {
							item := ResponsesItem{ID: id}
							switch {
							case strings.HasPrefix(id, "msg_"):
								item.Type = "message"
								item.Role = "assistant"
								item.Content = &ResponsesInput{Items: []ResponsesItem{{Type: "output_text", Text: lo.ToPtr(tc.values[index])}}}
							case strings.HasPrefix(id, "rs_"):
								item.Type = "reasoning"
								item.Summary = []ResponsesReasoningSummary{{Type: "summary_text", Text: tc.values[index]}}
							default:
								item.Type = "function_call"
								item.CallID = "call_one"
								item.Name = "tool"
								item.Arguments = tc.values[index]
							}
							rawItems = append(rawItems, item)
						}
						finish.RawResponsesOutputItems, _ = json.Marshal(rawItems)
					}
					if interrupted {
						finish.Usage = nil
					}
					out, err := inbound.TransformStream(ctx, finish)
					if err != nil {
						t.Fatal(err)
					}
					events = append(events, parseSSEEvents(t, out)...)
					if interrupted {
						out, err = inbound.FinalizeInterruptedStream(ctx)
						if err != model.ErrIncompleteUpstreamStream {
							t.Fatalf("interrupt: %v", err)
						}
						events = append(events, parseSSEEvents(t, out)...)
					}
					added, done := map[string]int{}, map[string]int{}
					indices := map[string]int{}
					for _, event := range events {
						if event.Type == "response.output_item.added" {
							added[event.Item.ID]++
							indices[event.Item.ID] = *event.OutputIndex
							if added[event.Item.ID] != 1 {
								t.Fatalf("repeated added for %s", event.Item.ID)
							}
						}
						if event.ItemID != nil && *event.OutputIndex != indices[*event.ItemID] {
							t.Fatalf("%s resumed at wrong output index", event.Type)
						}
						if event.Type == "response.output_item.done" {
							done[event.Item.ID]++
							if *event.OutputIndex != indices[event.Item.ID] {
								t.Fatal("done index changed")
							}
							value := ""
							switch event.Item.Type {
							case "message":
								value = *event.Item.Content.Items[0].Text
							case "reasoning":
								value = event.Item.Summary[0].Text
							case "function_call":
								value = event.Item.Arguments
							}
							if value != tc.values[indices[event.Item.ID]] {
								t.Fatalf("%s done content=%q want %q", event.Item.ID, value, tc.values[indices[event.Item.ID]])
							}
						}
					}
					typ := "response.completed"
					if interrupted {
						typ = "response.incomplete"
					}
					terminal := findEvent(events, typ)
					if terminal == nil || len(terminal.Response.Output) != len(tc.ids) || len(added) != len(tc.ids) {
						t.Fatalf("terminal output has duplicates: %+v", terminal)
					}
					for index, item := range terminal.Response.Output {
						if item.ID != tc.ids[index] || indices[item.ID] != index || added[item.ID] != 1 || done[item.ID] != 1 {
							t.Fatalf("item %d identity/index/done mismatch: %+v", index, item)
						}
					}
					if withRaw {
						stored, err := inbound.GetInternalResponse(ctx)
						if err != nil || stored == nil {
							t.Fatalf("cache: %v %v", stored, err)
						}
						assertRawItemIDs(t, stored.RawResponsesOutputItems, finish.RawResponsesOutputItems, terminal.Response.Output)
					}
				})
			}
		}
	}
}

func TestStreamNativeContentSuspensionPreservesThinkingSignature(t *testing.T) {
	for _, kind := range []model.StreamEventKind{model.StreamEventKindThinkingDelta, model.StreamEventKindSignatureDelta} {
		t.Run(string(kind), func(t *testing.T) {
			inbound := &ResponseInbound{}
			input := []model.StreamEvent{
				{Kind: model.StreamEventKindTextDelta, Delta: &model.StreamDelta{Text: "one"}, ContentBlock: &model.StreamContentBlock{Type: "message", ID: "msg_one"}},
				{Kind: kind, Delta: &model.StreamDelta{Signature: "cipher"}},
				{Kind: model.StreamEventKindTextDelta, Delta: &model.StreamDelta{Text: "two"}, ContentBlock: &model.StreamContentBlock{Type: "message", ID: "msg_one"}},
				{Kind: model.StreamEventKindMessageStop, StopReason: model.ParseFinishReason("stop")},
				{Kind: model.StreamEventKindUsageDelta, Usage: &model.Usage{TotalTokens: 1}},
			}
			out, err := inbound.TransformStreamEvents(context.Background(), input)
			if err != nil {
				t.Fatal(err)
			}
			terminal := findEvent(parseSSEEvents(t, out), "response.completed")
			if terminal == nil || len(terminal.Response.Output) != 2 || terminal.Response.Output[0].ID != "msg_one" || *terminal.Response.Output[0].Content.Items[0].Text != "onetwo" || terminal.Response.Output[1].EncryptedContent == nil || *terminal.Response.Output[1].EncryptedContent != "cipher" {
				t.Fatalf("signature lost on native suspension: %+v", terminal)
			}
		})
	}
}
