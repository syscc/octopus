package openai

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"

	"github.com/bestruirui/octopus/internal/transformer/model"
)

func TestStreamRawOnlyItemBeforeLegacyContentKeepsIdentity(t *testing.T) {
	for _, itemType := range []string{"message", "reasoning"} {
		t.Run(itemType, func(t *testing.T) {
			ctx := context.Background()
			adapter := &ResponseInbound{}
			event := model.StreamEvent{Kind: model.StreamEventKindTextDelta, Delta: &model.StreamDelta{Text: "sent"}, ContentBlock: &model.StreamContentBlock{Type: itemType, ID: "item_legacy"}}
			prefix := "msg_"
			if itemType == "reasoning" {
				event.Kind, event.Delta, prefix = model.StreamEventKindThinkingDelta, &model.StreamDelta{Thinking: "sent"}, "rs_"
			}
			wire, err := adapter.TransformStreamEvents(ctx, []model.StreamEvent{event})
			if err != nil {
				t.Fatal(err)
			}
			added := findEvent(parseSSEEvents(t, wire), "response.output_item.added")
			raw := json.RawMessage(`[{"type":"` + itemType + `","id":"` + prefix + `raw_only","content":[],"summary":[],"unknown":9007199254740993123},{"type":"` + itemType + `","id":"item_legacy","content":[{"type":"output_text","text":"sent"}],"summary":[{"type":"summary_text","text":"sent"}]}]`)
			original := bytes.Clone(raw)
			wire, err = adapter.TransformStreamEvents(ctx, []model.StreamEvent{
				{Kind: model.StreamEventKindMessageStop, StopReason: model.FinishReasonStop, ProviderExtensions: &model.ProviderExtensions{OpenAI: &model.OpenAIExtension{RawResponseItems: raw}}},
				{Kind: model.StreamEventKindUsageDelta, Usage: &model.Usage{TotalTokens: 1}},
			})
			if err != nil {
				t.Fatal(err)
			}
			terminal := findEvent(parseSSEEvents(t, wire), "response.completed")
			if terminal == nil || len(terminal.Response.Output) != 2 || terminal.Response.Output[0].ID != prefix+"raw_only" || terminal.Response.Output[1].ID != added.Item.ID {
				t.Fatalf("raw-only item stole the emitted identity: added=%+v terminal=%+v", added, terminal)
			}
			cached, err := adapter.GetInternalResponse(ctx)
			if err != nil || cached == nil {
				t.Fatalf("cache: %v %v", cached, err)
			}
			var items []map[string]json.RawMessage
			if err := json.Unmarshal(cached.RawResponsesOutputItems, &items); err != nil {
				t.Fatal(err)
			}
			var first, second string
			json.Unmarshal(items[0]["id"], &first)
			json.Unmarshal(items[1]["id"], &second)
			if first != prefix+"raw_only" || second != added.Item.ID || string(items[0]["unknown"]) != "9007199254740993123" || !bytes.Equal(raw, original) {
				t.Fatalf("cache lost original identity or fields: %s", cached.RawResponsesOutputItems)
			}
		})
	}
}

func TestMatchResponseOutputItemsLegacyBeforeNativeFallback(t *testing.T) {
	items := []ResponsesItem{{Type: "message", ID: "msg_raw_only"}, {Type: "message", ID: "item_legacy"}}
	output := []ResponsesItem{{Type: "message", ID: "msg_sent"}}
	matches := matchResponseOutputItems(items, output)
	if matches[0] != -1 || matches[1] != 0 {
		t.Fatalf("legacy source should match the emitted item first, got %v", matches)
	}
}

func TestStreamTerminalMatchesContentBeforeLegacyFallback(t *testing.T) {
	for _, itemType := range []string{"message", "reasoning"} {
		for _, nativeContent := range []bool{true, false} {
			for _, rawOnlyFirst := range []bool{true, false} {
				name := itemType
				if nativeContent {
					name += "/native_content"
				} else {
					name += "/legacy_content"
				}
				if rawOnlyFirst {
					name += "/raw_only_first"
				} else {
					name += "/content_first"
				}
				t.Run(name, func(t *testing.T) {
					ctx := context.Background()
					adapter := &ResponseInbound{}
					event := model.StreamEvent{Kind: model.StreamEventKindTextDelta, Delta: &model.StreamDelta{Text: "sent"}}
					prefix, contentKey, partType := "msg_", "content", "output_text"
					if itemType == "reasoning" {
						event.Kind, event.Delta = model.StreamEventKindThinkingDelta, &model.StreamDelta{Thinking: "sent"}
						prefix, contentKey, partType = "rs_", "summary", "summary_text"
					}
					wire, err := adapter.TransformStreamEvents(ctx, []model.StreamEvent{event})
					if err != nil {
						t.Fatal(err)
					}
					added := findEvent(parseSSEEvents(t, wire), "response.output_item.added")
					if added == nil || added.Item == nil {
						t.Fatal("missing added item")
					}
					contentID, rawOnlyID := "item_content", prefix+"raw_only"
					if nativeContent {
						contentID, rawOnlyID = prefix+"native", "item_raw_only"
					}
					content := `{"type":"` + itemType + `","id":"` + contentID + `","` + contentKey + `":[{"type":"` + partType + `","text":"sent"}],"unknown":9007199254740993123}`
					rawOnly := `{"type":"` + itemType + `","id":"` + rawOnlyID + `","` + contentKey + `":[],"unknown":9007199254740993124}`
					raw := json.RawMessage("[" + content + "," + rawOnly + "]")
					contentIndex, rawOnlyIndex := 0, 1
					if rawOnlyFirst {
						raw = json.RawMessage("[" + rawOnly + "," + content + "]")
						contentIndex, rawOnlyIndex = 1, 0
					}
					original := bytes.Clone(raw)
					wire, err = adapter.TransformStreamEvents(ctx, []model.StreamEvent{
						{Kind: model.StreamEventKindMessageStop, StopReason: model.FinishReasonStop, ProviderExtensions: &model.ProviderExtensions{OpenAI: &model.OpenAIExtension{RawResponseItems: raw}}},
						{Kind: model.StreamEventKindUsageDelta, Usage: &model.Usage{TotalTokens: 1}},
					})
					if err != nil {
						t.Fatal(err)
					}
					terminal := findEvent(parseSSEEvents(t, wire), "response.completed")
					if terminal == nil || terminal.Response == nil || len(terminal.Response.Output) != 2 {
						t.Fatalf("missing terminal output: %+v", terminal)
					}
					output := terminal.Response.Output
					if output[contentIndex].ID != added.Item.ID || output[rawOnlyIndex].ID == added.Item.ID || !validContentItemID(itemType, output[rawOnlyIndex].ID) {
						t.Fatalf("wrong content identity: added=%+v output=%+v", added.Item, output)
					}
					if !nativeContent && output[rawOnlyIndex].ID != rawOnlyID {
						t.Fatalf("native raw-only ID changed: %+v", output)
					}
					cached, err := adapter.GetInternalResponse(ctx)
					if err != nil || cached == nil {
						t.Fatalf("cache: %v %v", cached, err)
					}
					var items []map[string]json.RawMessage
					if err := json.Unmarshal(cached.RawResponsesOutputItems, &items); err != nil || len(items) != 2 {
						t.Fatalf("cached items: %s, %v", cached.RawResponsesOutputItems, err)
					}
					for index, item := range items {
						var id string
						if err := json.Unmarshal(item["id"], &id); err != nil || id != output[index].ID {
							t.Fatalf("cache identity differs at %d: %s, %v", index, cached.RawResponsesOutputItems, err)
						}
					}
					if string(items[contentIndex]["unknown"]) != "9007199254740993123" || string(items[rawOnlyIndex]["unknown"]) != "9007199254740993124" || !bytes.Equal(raw, original) {
						t.Fatalf("raw fields or original changed: %s", cached.RawResponsesOutputItems)
					}
				})
			}
		}
	}
}

func TestResponseCacheMatchesNonEmptyContent(t *testing.T) {
	for _, contentType := range []string{"output_text", "refusal", "summary_text"} {
		for _, nativeContent := range []bool{true, false} {
			for _, rawOnlyFirst := range []bool{true, false} {
				name := contentType
				if nativeContent {
					name += "/native_content"
				} else {
					name += "/legacy_content"
				}
				if rawOnlyFirst {
					name += "/raw_only_first"
				} else {
					name += "/content_first"
				}
				t.Run(name, func(t *testing.T) {
					text := "sent"
					message := &model.Message{Content: model.MessageContent{Content: &text}}
					itemType, contentKey, textKey, nativeID := "message", "content", "text", "msg_native"
					if contentType == "refusal" {
						message.Content.Content, message.Refusal, textKey = nil, text, "refusal"
					} else if contentType == "summary_text" {
						message.Content.Content, message.ReasoningContent = nil, &text
						itemType, contentKey, nativeID = "reasoning", "summary", "rs_native"
					}
					contentID, rawOnlyID := "item_content", "msg_raw_only"
					if itemType == "reasoning" {
						rawOnlyID = "rs_raw_only"
					}
					if nativeContent {
						contentID, rawOnlyID = nativeID, "item_raw_only"
					}
					content := `{"type":"` + itemType + `","id":"` + contentID + `","` + contentKey + `":[{"type":"` + contentType + `","` + textKey + `":"sent"}],"unknown":9007199254740993123}`
					rawOnly := `{"type":"` + itemType + `","id":"` + rawOnlyID + `","` + contentKey + `":[]}`
					raw := json.RawMessage("[" + content + "," + rawOnly + "]")
					contentIndex, rawOnlyIndex := 0, 1
					if rawOnlyFirst {
						raw, contentIndex, rawOnlyIndex = json.RawMessage("["+rawOnly+","+content+"]"), 1, 0
					}
					original := bytes.Clone(raw)
					response := &model.InternalLLMResponse{RawResponsesOutputItems: raw, Choices: []model.Choice{{Message: message}}}
					ctx := context.Background()
					adapter := &ResponseInbound{}
					body, err := adapter.TransformResponse(ctx, response)
					if err != nil {
						t.Fatal(err)
					}
					var result ResponsesResponse
					if err := json.Unmarshal(body, &result); err != nil || len(result.Output) != 1 {
						t.Fatalf("response: %s, %v", body, err)
					}
					if nativeContent && result.Output[0].ID != nativeID {
						t.Fatalf("native content identity changed: %+v", result.Output)
					}
					cached, err := adapter.GetInternalResponse(ctx)
					if err != nil || cached == nil {
						t.Fatalf("cache: %v %v", cached, err)
					}
					var items []map[string]json.RawMessage
					if err := json.Unmarshal(cached.RawResponsesOutputItems, &items); err != nil || len(items) != 2 {
						t.Fatalf("cached items: %s, %v", cached.RawResponsesOutputItems, err)
					}
					var cachedContentID, cachedRawOnlyID string
					json.Unmarshal(items[contentIndex]["id"], &cachedContentID)
					json.Unmarshal(items[rawOnlyIndex]["id"], &cachedRawOnlyID)
					if cachedContentID != result.Output[0].ID || cachedRawOnlyID != rawOnlyID || cachedContentID == cachedRawOnlyID {
						t.Fatalf("wrong cached identity: output=%+v cache=%s", result.Output, cached.RawResponsesOutputItems)
					}
					if string(items[contentIndex]["unknown"]) != "9007199254740993123" || !bytes.Equal(raw, original) || !bytes.Equal(response.RawResponsesOutputItems, original) {
						t.Fatalf("raw fields or caller input changed: %s", cached.RawResponsesOutputItems)
					}
				})
			}
		}
	}
}

func TestMatchResponseOutputItemsContentAmbiguityKeepsLegacyFallback(t *testing.T) {
	text := "same"
	content := &ResponsesInput{Items: []ResponsesItem{{Type: "output_text", Text: &text}}}
	items := []ResponsesItem{{Type: "message", ID: "msg_native", Content: content}, {Type: "message", ID: "item_legacy", Content: content}}
	output := []ResponsesItem{{Type: "message", ID: "msg_sent", Content: content}}
	matches := matchResponseOutputItems(items, output)
	if matches[0] != -1 || matches[1] != 0 {
		t.Fatalf("ambiguous content should retain legacy fallback, got %v", matches)
	}
}

func TestMatchResponseOutputItemsCoalescedTextAndRefusal(t *testing.T) {
	first, second, full, refusal := "se", "nt", "sent", "denied"
	items := []ResponsesItem{
		{Type: "message", ID: "item_empty", Content: &ResponsesInput{}},
		{Type: "message", ID: "msg_native", Content: &ResponsesInput{Items: []ResponsesItem{
			{Type: "output_text", Text: &first}, {Type: "output_text", Text: &second}, {Type: "refusal", Refusal: &refusal},
		}}},
	}
	output := []ResponsesItem{{Type: "message", ID: "msg_sent", Content: &ResponsesInput{Items: []ResponsesItem{
		{Type: "output_text", Text: &full}, {Type: "refusal", Refusal: &refusal},
	}}}}
	matches := matchResponseOutputItems(items, output)
	if matches[0] != -1 || matches[1] != 0 {
		t.Fatalf("coalesced content should match before empty legacy item, got %v", matches)
	}
}

func TestRawContentIDAlignmentMatchesEncryptedReasoning(t *testing.T) {
	raw := json.RawMessage(`[{"type":"reasoning","id":"rs_native","summary":[],"encrypted_content":"cipher","unknown":9007199254740993123},{"type":"reasoning","id":"item_empty","summary":[]}]`)
	original := bytes.Clone(raw)
	cipher := "cipher"
	aligned := alignRawResponseItemIDs(raw, []ResponsesItem{{Type: "reasoning", ID: "rs_sent", EncryptedContent: &cipher}})
	var items []map[string]json.RawMessage
	if err := json.Unmarshal(aligned, &items); err != nil || len(items) != 2 {
		t.Fatalf("aligned items: %s, %v", aligned, err)
	}
	if string(items[0]["id"]) != `"rs_sent"` || string(items[1]["id"]) != `"item_empty"` || string(items[0]["encrypted_content"]) != `"cipher"` || string(items[0]["unknown"]) != "9007199254740993123" || !bytes.Equal(raw, original) {
		t.Fatalf("encrypted content matched the empty legacy item or lost raw fields: %s", aligned)
	}
}

func TestStreamCacheRetainsTerminalMappingForDuplicateContent(t *testing.T) {
	for _, itemType := range []string{"message", "reasoning"} {
		t.Run(itemType, func(t *testing.T) {
			ctx := context.Background()
			adapter := &ResponseInbound{}
			contentEvent := model.StreamEvent{Kind: model.StreamEventKindTextDelta, Delta: &model.StreamDelta{Text: "same"}}
			separatorEvent := model.StreamEvent{Kind: model.StreamEventKindThinkingDelta, Delta: &model.StreamDelta{Thinking: "separator"}, ContentBlock: &model.StreamContentBlock{Type: "reasoning", ID: "rs_separator"}}
			prefix, contentKey, partType := "msg_", "content", "output_text"
			separator := `{"type":"reasoning","id":"rs_separator","summary":[{"type":"summary_text","text":"separator"}]}`
			if itemType == "reasoning" {
				contentEvent.Kind, contentEvent.Delta = model.StreamEventKindThinkingDelta, &model.StreamDelta{Thinking: "same"}
				separatorEvent = model.StreamEvent{Kind: model.StreamEventKindTextDelta, Delta: &model.StreamDelta{Text: "separator"}, ContentBlock: &model.StreamContentBlock{Type: "message", ID: "msg_separator"}}
				prefix, contentKey, partType = "rs_", "summary", "summary_text"
				separator = `{"type":"message","id":"msg_separator","content":[{"type":"output_text","text":"separator"}]}`
			}
			secondEvent := contentEvent
			secondEvent.ContentBlock = &model.StreamContentBlock{Type: itemType, ID: "item_legacy"}
			first := `{"type":"` + itemType + `","id":"` + prefix + `native","` + contentKey + `":[{"type":"` + partType + `","text":"same"}],"opaque":"first","unknown":9007199254740993123}`
			second := `{"type":"` + itemType + `","id":"item_legacy","` + contentKey + `":[{"type":"` + partType + `","text":"same"}],"opaque":"second"}`
			raw := json.RawMessage("[" + first + "," + separator + "," + second + "]")
			original := bytes.Clone(raw)
			wire, err := adapter.TransformStreamEvents(ctx, []model.StreamEvent{
				contentEvent, separatorEvent, secondEvent,
				{Kind: model.StreamEventKindMessageStop, StopReason: model.FinishReasonStop, ProviderExtensions: &model.ProviderExtensions{OpenAI: &model.OpenAIExtension{RawResponseItems: raw}}},
				{Kind: model.StreamEventKindUsageDelta, Usage: &model.Usage{TotalTokens: 1}},
			})
			if err != nil {
				t.Fatal(err)
			}
			terminal := findEvent(parseSSEEvents(t, wire), "response.completed")
			if terminal == nil || terminal.Response == nil || len(terminal.Response.Output) != 3 {
				t.Fatalf("missing terminal output: %+v", terminal)
			}
			cached, err := adapter.GetInternalResponse(ctx)
			if err != nil || cached == nil {
				t.Fatalf("cache: %v %v", cached, err)
			}
			assertRawItemIDMapping(t, cached.RawResponsesOutputItems, original, terminal.Response.Output, []int{0, 1, 2})
			if !bytes.Equal(raw, original) {
				t.Fatal("caller raw changed")
			}
		})
	}
}

func TestResponseCacheRetainsFirstMappingAfterNativeIDAdoption(t *testing.T) {
	for _, contentType := range []string{"output_text", "refusal", "summary_text"} {
		t.Run(contentType, func(t *testing.T) {
			ctx := context.Background()
			adapter := &ResponseInbound{}
			value := "b"
			message := &model.Message{Content: model.MessageContent{Content: &value}}
			itemType, prefix, contentKey, textKey := "message", "msg_", "content", "text"
			if contentType == "refusal" {
				message.Content.Content, message.Refusal, textKey = nil, value, "refusal"
			} else if contentType == "summary_text" {
				message.Content.Content, message.ReasoningContent = nil, &value
				itemType, prefix, contentKey = "reasoning", "rs_", "summary"
			}
			item := func(id, text string) string {
				return `{"type":"` + itemType + `","id":"` + id + `","` + contentKey + `":[{"type":"` + contentType + `","` + textKey + `":"` + text + `"}],"unknown":9007199254740993123}`
			}
			raw := json.RawMessage("[" + item(prefix+"native_a", "a") + "," + item(prefix+"native_b", "b") + "," + item("item_legacy", "a") + "]")
			original := bytes.Clone(raw)
			response := &model.InternalLLMResponse{RawResponsesOutputItems: raw, Choices: []model.Choice{{Message: message}, {Message: message}}}
			body, err := adapter.TransformResponse(ctx, response)
			if err != nil {
				t.Fatal(err)
			}
			var result ResponsesResponse
			if err := json.Unmarshal(body, &result); err != nil || len(result.Output) != 2 {
				t.Fatalf("response: %s, %v", body, err)
			}
			cached, err := adapter.GetInternalResponse(ctx)
			if err != nil || cached == nil {
				t.Fatalf("cache: %v %v", cached, err)
			}
			// Duplicate content leaves the first mapping to the legacy fallback.
			// Native ID adoption must preserve that mapping for cache storage.
			assertRawItemIDMapping(t, cached.RawResponsesOutputItems, original, result.Output, []int{1, -1, 0})
			if !bytes.Equal(raw, original) || !bytes.Equal(response.RawResponsesOutputItems, original) {
				t.Fatal("caller raw changed")
			}
		})
	}
}

func TestRawContentIDAlignmentWithMatchesChecksBounds(t *testing.T) {
	raw := json.RawMessage(`[{"type":"message","id":"item_first"},{"type":"message","id":"item_second"}]`)
	output := []ResponsesItem{{Type: "message", ID: "msg_sent"}}
	for _, matches := range [][]int{nil, {0}, {0, 0, 0}, {-1, 1}, {2, -1}} {
		if aligned := alignRawResponseItemIDsWithMatches(raw, output, matches); !bytes.Equal(aligned, raw) {
			t.Fatalf("invalid mapping %v modified raw: %s", matches, aligned)
		}
	}
}

func TestStreamCacheDoesNotReuseMappingForDifferentRawItems(t *testing.T) {
	ctx := context.Background()
	adapter := &ResponseInbound{}
	raw := json.RawMessage(`[{"type":"message","id":"item_original","content":[{"type":"output_text","text":"sent"}]}]`)
	if _, err := adapter.TransformStreamEvents(ctx, []model.StreamEvent{
		{Kind: model.StreamEventKindTextDelta, Delta: &model.StreamDelta{Text: "sent"}},
		{Kind: model.StreamEventKindMessageStop, StopReason: model.FinishReasonStop, ProviderExtensions: &model.ProviderExtensions{OpenAI: &model.OpenAIExtension{RawResponseItems: raw}}},
		{Kind: model.StreamEventKindUsageDelta, Usage: &model.Usage{TotalTokens: 1}},
	}); err != nil {
		t.Fatal(err)
	}
	otherRaw := json.RawMessage(`[{"type":"message","id":"item_other","content":[{"type":"output_text","text":"sent"}]}]`)
	// Simulate a retained upstream snapshot different from the terminal's raw
	// array. Equal length does not make the recorded mapping applicable.
	adapter.streamAggregator.Add(&model.InternalLLMResponse{RawResponsesOutputItems: otherRaw})
	cached, err := adapter.GetInternalResponse(ctx)
	if err != nil || cached == nil || !bytes.Equal(cached.RawResponsesOutputItems, otherRaw) {
		t.Fatalf("mapping was reused for another raw array: %v %v", cached, err)
	}
}

func assertRawItemIDMapping(t *testing.T, cached, original json.RawMessage, output []ResponsesItem, matches []int) {
	t.Helper()
	var before, after []map[string]json.RawMessage
	if err := json.Unmarshal(original, &before); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(cached, &after); err != nil || len(after) != len(matches) || len(before) != len(after) {
		t.Fatalf("cache length: %s, %v", cached, err)
	}
	seen := make(map[string]bool)
	for index, item := range after {
		var id string
		if err := json.Unmarshal(item["id"], &id); err != nil {
			t.Fatal(err)
		}
		if matched := matches[index]; matched >= 0 {
			if id != output[matched].ID {
				t.Fatalf("cache mapping at %d changed: output=%+v matches=%v cached=%s", index, output, matches, cached)
			}
		} else if !bytes.Equal(item["id"], before[index]["id"]) {
			t.Fatalf("unmatched raw item ID changed: %s", cached)
		}
		if seen[id] {
			t.Fatalf("duplicate cached ID %q: %s", id, cached)
		}
		seen[id] = true
		if len(item) != len(before[index]) {
			t.Fatalf("raw field count changed: %s", cached)
		}
		for key, value := range before[index] {
			if key != "id" && !bytes.Equal(item[key], value) {
				t.Fatalf("raw field %q at %d changed: %s", key, index, cached)
			}
		}
	}
}
