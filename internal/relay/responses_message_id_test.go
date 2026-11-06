package relay

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"strings"
	"testing"

	openaiInbound "github.com/bestruirui/octopus/internal/transformer/inbound/openai"
	transformerModel "github.com/bestruirui/octopus/internal/transformer/model"
	"github.com/bestruirui/octopus/internal/transformer/outbound"
	openaiOutbound "github.com/bestruirui/octopus/internal/transformer/outbound/openai"
)

func TestResponsesMessageReasoningNonstreamClientCacheReplay(t *testing.T) {
	for _, native := range []bool{false, true} {
		t.Run(map[bool]string{false: "legacy", true: "native"}[native], func(t *testing.T) {
			messageID, reasoningID, toolID := "item_message", "item_reasoning", "call_legacy"
			if native {
				messageID, reasoningID, toolID = "msg_native", "rs_native", "fc_native"
			}
			raw := json.RawMessage(`[{"type":"reasoning","id":"` + reasoningID + `","summary":[{"type":"summary_text","text":"thought","native":9007199254740993123}],"encrypted_content":"synthetic_cipher","native":{"large":9007199254740993123}},{"type":"function_call","id":"` + toolID + `","call_id":"call_pair","name":"lookup","arguments":"{}","native":{"large":9007199254740993123}},{"type":"message","id":"` + messageID + `","role":"assistant","status":"completed","content":[{"type":"output_text","text":"answer","native":9007199254740993123}],"native":{"large":9007199254740993123}}]`)
			upstream := &transformerModel.InternalLLMResponse{
				ID: "resp_message_roundtrip", Model: "gpt-test", RawResponsesOutputItems: raw,
				Choices: []transformerModel.Choice{{Message: &transformerModel.Message{
					Role: "assistant", ReasoningContent: stringPtr("thought"),
					Content:   transformerModel.MessageContent{Content: stringPtr("answer")},
					ToolCalls: []transformerModel.ToolCall{{ID: "call_pair", Type: "function", Function: transformerModel.FunctionCall{Name: "lookup", Arguments: "{}"}}},
				}}},
			}
			before, err := json.Marshal(upstream)
			if err != nil {
				t.Fatal(err)
			}
			adapter := &openaiInbound.ResponseInbound{}
			body, err := adapter.TransformResponse(context.Background(), upstream)
			if err != nil {
				t.Fatal(err)
			}
			client := messageIDPayloadItems(t, body, "output")
			if len(client) != 3 {
				t.Fatalf("expected reasoning, tool and message outputs, got %s", body)
			}
			ids := make([]string, len(client))
			for index, item := range client {
				typ, id := messageIDString(t, item["type"]), messageIDString(t, item["id"])
				prefix := map[string]string{"message": "msg_", "reasoning": "rs_", "function_call": "fc_"}[typ]
				if prefix == "" || !strings.HasPrefix(id, prefix) {
					t.Fatalf("invalid client %s ID %q", typ, id)
				}
				if native && id != map[string]string{"message": messageID, "reasoning": reasoningID, "function_call": toolID}[typ] {
					t.Fatalf("native %s ID changed: %q", typ, id)
				}
				if typ == "function_call" && messageIDString(t, item["call_id"]) != "call_pair" {
					t.Fatalf("client call_id changed: %s", body)
				}
				ids[index] = id
			}
			cached, err := adapter.GetInternalResponse(context.Background())
			if err != nil || cached == nil {
				t.Fatalf("get cached response: %v, %v", cached, err)
			}
			cachedItems := messageIDDecodeItems(t, cached.RawResponsesOutputItems)
			if len(cachedItems) != len(ids) {
				t.Fatalf("cache output count differs from client: %d / %d", len(cachedItems), len(ids))
			}
			for index, item := range cachedItems {
				if messageIDString(t, item["id"]) != ids[index] {
					t.Fatalf("cache and client IDs differ: %s / %s", cached.RawResponsesOutputItems, body)
				}
			}
			messageIDAssertFieldsUnchanged(t, messageIDDecodeItems(t, raw), cachedItems)
			messageIDAssertReplay(t, cached, ids)
			after, err := json.Marshal(upstream)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(before, after) {
				t.Fatal("caller response or raw buffer was mutated")
			}
		})
	}
}

func TestResponsesNativeMessageReasoningSSEClientCacheReplay(t *testing.T) {
	ctx := context.Background()
	upstream, downstream := &openaiOutbound.ResponseOutbound{}, &openaiInbound.ResponseInbound{}
	raw := `[{"type":"reasoning","id":"rs_native","summary":[{"type":"summary_text","text":"thought"}],"encrypted_content":"synthetic_cipher"},{"type":"message","id":"msg_first","role":"assistant","status":"completed","content":[{"type":"output_text","text":"answer"}]},{"type":"message","id":"msg_second","role":"assistant","status":"completed","content":[{"type":"refusal","refusal":"refused"}]},{"type":"function_call","id":"fc_native","call_id":"call_pair","name":"lookup","arguments":"{}"}]`
	frames := []string{
		`{"type":"response.created","response":{"id":"resp_native_roundtrip","model":"gpt-test","status":"in_progress","output":[]}}`,
		`{"type":"response.output_item.added","output_index":0,"item":{"type":"reasoning","id":"rs_native","summary":[]}}`,
		`{"type":"response.reasoning_summary_text.delta","output_index":0,"summary_index":0,"item_id":"rs_native","delta":"thought"}`,
		`{"type":"response.output_item.added","output_index":1,"item":{"type":"message","id":"msg_first","role":"assistant","content":[]}}`,
		`{"type":"response.output_text.delta","output_index":1,"content_index":0,"item_id":"msg_first","delta":"answer"}`,
		`{"type":"response.output_item.added","output_index":2,"item":{"type":"message","id":"msg_second","role":"assistant","content":[]}}`,
		`{"type":"response.refusal.delta","output_index":2,"content_index":0,"item_id":"msg_second","delta":"refused"}`,
		`{"type":"response.output_item.added","output_index":3,"item":{"type":"function_call","id":"fc_native","call_id":"call_pair","name":"lookup","arguments":""}}`,
		`{"type":"response.completed","response":{"id":"resp_native_roundtrip","model":"gpt-test","status":"completed","output":` + raw + `,"usage":{"input_tokens":1,"output_tokens":2,"total_tokens":3}}}`,
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
	added := map[string]int{}
	deltas := map[string]string{}
	var terminal []map[string]json.RawMessage
	for _, line := range strings.Split(string(wire), "\n") {
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		var event struct {
			Type     string                     `json:"type"`
			ItemID   string                     `json:"item_id"`
			Delta    string                     `json:"delta"`
			Item     map[string]json.RawMessage `json:"item"`
			Response json.RawMessage            `json:"response"`
		}
		if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &event); err != nil {
			t.Fatal(err)
		}
		switch event.Type {
		case "response.output_item.added":
			added[messageIDString(t, event.Item["id"])]++
		case "response.output_text.delta", "response.refusal.delta", "response.reasoning_summary_text.delta":
			deltas[event.ItemID] += event.Delta
		case "response.completed":
			terminal = messageIDPayloadItems(t, event.Response, "output")
		}
	}
	ids := []string{"rs_native", "msg_first", "msg_second", "fc_native"}
	for index, id := range ids {
		if added[id] != 1 {
			t.Errorf("native item %q added mismatch: added=%v", id, added)
		}
		if index < 3 && deltas[id] != []string{"thought", "answer", "refused"}[index] {
			t.Errorf("native item %q added/delta mismatch: added=%v deltas=%v", id, added, deltas)
		}
	}
	if len(added) != len(ids) || len(terminal) != len(ids) {
		t.Fatalf("native items merged or duplicated: added=%v terminal=%v", added, terminal)
	}
	for index, item := range terminal {
		if messageIDString(t, item["id"]) != ids[index] {
			t.Errorf("terminal item %d ID changed: %s", index, item["id"])
		}
	}
	if string(terminal[1]["content"]) != `[{"type":"output_text","text":"answer"}]` || string(terminal[2]["content"]) != `[{"type":"refusal","refusal":"refused"}]` {
		t.Errorf("consecutive message content merged or changed: %v", terminal)
	}
	if messageIDString(t, terminal[3]["call_id"]) != "call_pair" {
		t.Errorf("terminal call_id changed: %s", terminal[3]["call_id"])
	}
	cached, err := downstream.GetInternalResponse(ctx)
	if err != nil || cached == nil {
		t.Fatalf("get cached response: %v, %v", cached, err)
	}
	messageIDAssertFieldsUnchanged(t, messageIDDecodeItems(t, json.RawMessage(raw)), messageIDDecodeItems(t, cached.RawResponsesOutputItems))
	messageIDAssertReplay(t, cached, ids)
}

func TestResponsesMessageReasoningPassthroughRequestIDs(t *testing.T) {
	raw := []byte(`{"model":"client-model","native":{"large":9007199254740993123},"input":[{"type":"message","id":"item_message","role":"assistant","content":[{"type":"output_text","text":"answer"}],"native":9007199254740993123},{"type":"reasoning","id":"item_reasoning","encrypted_content":"synthetic_cipher","summary":[{"type":"summary_text","text":"thought"}],"native":9007199254740993123},{"type":"message","id":"msg_native","role":"assistant","content":[{"type":"output_text","text":"native answer"}]},{"type":"reasoning","id":"rs_native","summary":[]},{"type":"item_reference","id":"item_message"},{"type":"item_reference","id":"item_reasoning"},{"type":"item_reference","id":"msg_native"},{"type":"item_reference","id":"rs_native"}]}`)
	before := append([]byte(nil), raw...)
	request := &transformerModel.InternalLLMRequest{Model: "upstream-model", RawAPIFormat: transformerModel.APIFormatOpenAIResponse, RawRequest: raw}
	for _, transport := range []string{"ws", "http"} {
		t.Run(transport, func(t *testing.T) {
			var body []byte
			var err error
			if transport == "ws" {
				attempt := &relayAttempt{relayRequest: &relayRequest{internalRequest: request, rawBody: raw}}
				body, err = attempt.buildWSPassthroughRequestPayload()
			} else {
				httpRequest, requestErr := (&openaiOutbound.ResponseOutbound{}).TransformRequestRaw(context.Background(), raw, request.Model, "https://example.invalid/v1", "synthetic-key", nil)
				if requestErr != nil {
					t.Fatal(requestErr)
				}
				defer httpRequest.Body.Close()
				body, err = io.ReadAll(httpRequest.Body)
			}
			if err != nil {
				t.Fatal(err)
			}
			items := messageIDPayloadItems(t, body, "input")
			if len(items) != 8 {
				t.Fatalf("unexpected input count: %s", body)
			}
			for index, prefix := range []string{"msg_", "rs_", "msg_", "rs_"} {
				id := messageIDString(t, items[index]["id"])
				if !strings.HasPrefix(id, prefix) || !bytes.Equal(items[index]["id"], items[index+4]["id"]) {
					t.Errorf("target/reference ID mismatch at %d: %s", index, body)
				}
			}
			if messageIDString(t, items[2]["id"]) != "msg_native" || messageIDString(t, items[3]["id"]) != "rs_native" {
				t.Errorf("native IDs changed: %s", body)
			}
			messageIDAssertFieldsUnchanged(t, messageIDPayloadItems(t, raw, "input"), items)
			var decoded map[string]json.RawMessage
			if err := json.Unmarshal(body, &decoded); err != nil {
				t.Fatal(err)
			}
			if string(decoded["native"]) != `{"large":9007199254740993123}` {
				t.Errorf("top-level native fields changed: %s", body)
			}
			if !bytes.Equal(before, raw) || !bytes.Equal(before, request.RawRequest) {
				t.Fatal("caller raw request was mutated")
			}
		})
	}
}

func TestResponsesNativeInterleavedItemIdentity(t *testing.T) {
	for _, interrupted := range []bool{false, true} {
		t.Run(map[bool]string{false: "completed", true: "interrupted"}[interrupted], func(t *testing.T) {
			ctx := context.Background()
			upstream, downstream := &openaiOutbound.ResponseOutbound{}, &openaiInbound.ResponseInbound{}
			frames := []string{
				`{"type":"response.created","response":{"id":"resp_interleaved","model":"gpt-test","status":"in_progress","output":[]}}`,
				`{"type":"response.output_item.added","output_index":0,"item":{"type":"message","id":"msg_a","role":"assistant","content":[]}}`,
				`{"type":"response.output_text.delta","output_index":0,"content_index":0,"item_id":"msg_a","delta":"one"}`,
				`{"type":"response.output_item.added","output_index":1,"item":{"type":"function_call","id":"fc_native","call_id":"call_pair","name":"lookup","arguments":""}}`,
				`{"type":"response.output_text.delta","output_index":0,"content_index":0,"item_id":"msg_a","delta":"two"}`,
				`{"type":"response.output_item.added","output_index":2,"item":{"type":"reasoning","id":"rs_a","summary":[]}}`,
				`{"type":"response.reasoning_summary_text.delta","output_index":2,"summary_index":0,"item_id":"rs_a","delta":"think"}`,
				`{"type":"response.output_item.added","output_index":3,"item":{"type":"message","id":"msg_b","role":"assistant","content":[]}}`,
				`{"type":"response.output_text.delta","output_index":3,"content_index":0,"item_id":"msg_b","delta":"other"}`,
				`{"type":"response.reasoning_summary_text.delta","output_index":2,"summary_index":0,"item_id":"rs_a","delta":"again"}`,
				`{"type":"response.output_text.delta","output_index":0,"content_index":0,"item_id":"msg_a","delta":"three"}`,
			}
			if !interrupted {
				frames = append(frames, `{"type":"response.completed","response":{"id":"resp_interleaved","model":"gpt-test","status":"completed","output":[{"type":"message","id":"msg_a","role":"assistant","status":"completed","content":[{"type":"output_text","text":"onetwothree"}]},{"type":"function_call","id":"fc_native","call_id":"call_pair","name":"lookup","arguments":""},{"type":"reasoning","id":"rs_a","summary":[{"type":"summary_text","text":"thinkagain"}]},{"type":"message","id":"msg_b","role":"assistant","status":"completed","content":[{"type":"output_text","text":"other"}]}],"usage":{"input_tokens":1,"output_tokens":2,"total_tokens":3}}}`)
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
			if interrupted {
				encoded, err := downstream.FinalizeInterruptedStream(ctx)
				if err != transformerModel.ErrIncompleteUpstreamStream {
					t.Fatalf("interrupted finalization error: %v", err)
				}
				wire = append(wire, encoded...)
			}
			ids := []string{"msg_a", "fc_native", "rs_a", "msg_b"}
			wantText := map[string]string{"msg_a": "onetwothree", "rs_a": "thinkagain", "msg_b": "other"}
			added, done, textDone := map[string]int{}, map[string]int{}, map[string]int{}
			initialIndex, deltas := map[string]int{}, map[string]string{}
			var terminal []map[string]json.RawMessage
			for _, line := range strings.Split(string(wire), "\n") {
				if !strings.HasPrefix(line, "data: ") {
					continue
				}
				var event struct {
					Type        string                     `json:"type"`
					ItemID      string                     `json:"item_id"`
					OutputIndex *int                       `json:"output_index"`
					Delta       string                     `json:"delta"`
					Text        string                     `json:"text"`
					Item        map[string]json.RawMessage `json:"item"`
					Response    json.RawMessage            `json:"response"`
				}
				if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &event); err != nil {
					t.Fatal(err)
				}
				id := event.ItemID
				if event.Item != nil {
					id = messageIDString(t, event.Item["id"])
				}
				if event.Type == "response.output_item.added" {
					if event.OutputIndex == nil {
						t.Fatal("added item has no output_index")
					}
					if added[id] == 0 {
						initialIndex[id] = *event.OutputIndex
					}
					added[id]++
				}
				if id != "" {
					index, exists := initialIndex[id]
					eventIndex := -1
					if event.OutputIndex != nil {
						eventIndex = *event.OutputIndex
					}
					if !exists || eventIndex != index {
						t.Errorf("%s for %s changed initial output_index %d: got %d", event.Type, id, index, eventIndex)
					}
				}
				switch event.Type {
				case "response.output_text.delta", "response.reasoning_summary_text.delta":
					deltas[id] += event.Delta
				case "response.output_text.done", "response.reasoning_summary_text.done":
					textDone[id]++
					if event.Text != wantText[id] {
						t.Errorf("%s for %s closed with partial text %q, want %q", event.Type, id, event.Text, wantText[id])
					}
				case "response.output_item.done":
					done[id]++
					if id != "fc_native" && messageIDItemText(t, event.Item) != wantText[id] {
						t.Errorf("done item %s has partial content: %s", id, messageIDItemText(t, event.Item))
					}
				case "response.completed", "response.incomplete":
					terminal = messageIDPayloadItems(t, event.Response, "output")
				}
			}
			for index, id := range ids {
				if added[id] != 1 || done[id] != 1 || initialIndex[id] != index {
					t.Errorf("item %s lifecycle changed: added=%d done=%d initial_index=%d, want slot %d", id, added[id], done[id], initialIndex[id], index)
				}
				if id != "fc_native" && (deltas[id] != wantText[id] || textDone[id] != 1) {
					t.Errorf("item %s delta/done mismatch: delta=%q text_done=%d", id, deltas[id], textDone[id])
				}
			}
			if len(added) != len(ids) || len(done) != len(ids) || len(textDone) != len(wantText) {
				t.Errorf("unexpected item identities: added=%v done=%v text_done=%v", added, done, textDone)
			}
			cached, err := downstream.GetInternalResponse(ctx)
			if err != nil || cached == nil {
				t.Fatalf("get interleaved cache: %v %v", cached, err)
			}
			if cached.ID != "resp_interleaved" {
				t.Errorf("aggregated source response ID changed: %s", cached.ID)
			}
			outputs := map[string][]map[string]json.RawMessage{"terminal": terminal}
			if !interrupted {
				outputs["cache"] = messageIDDecodeItems(t, cached.RawResponsesOutputItems)
			}
			for name, items := range outputs {
				if len(items) != len(ids) {
					t.Errorf("%s duplicated interleaved items: got %d want %d", name, len(items), len(ids))
					continue
				}
				for index, item := range items {
					id := messageIDString(t, item["id"])
					if id != ids[index] || (id != "fc_native" && messageIDItemText(t, item) != wantText[id]) {
						t.Errorf("%s slot %d lost item identity/content: id=%s text=%s", name, index, id, messageIDItemText(t, item))
					}
				}
			}
			// Incomplete turns do not enter the successful-turn replay store.
			if !interrupted {
				messageIDAssertReplay(t, cached, ids)
			}
		})
	}
}

func messageIDItemText(t *testing.T, item map[string]json.RawMessage) string {
	t.Helper()
	field := "content"
	if messageIDString(t, item["type"]) == "reasoning" {
		field = "summary"
	}
	if len(item[field]) == 0 {
		return ""
	}
	var text strings.Builder
	for _, part := range messageIDDecodeItems(t, item[field]) {
		text.WriteString(messageIDString(t, part["text"]))
	}
	return text.String()
}

func messageIDAssertReplay(t *testing.T, response *transformerModel.InternalLLMResponse, ids []string) {
	t.Helper()
	state := &wsConversationState{LastOutboundType: outbound.OutboundTypeOpenAIResponse, LastOutboundTypeSet: true}
	state.ApplySuccessfulTurn(&transformerModel.InternalLLMRequest{
		Model: "gpt-test", RawAPIFormat: transformerModel.APIFormatOpenAIResponse,
		RawInputItems: json.RawMessage(`[{"type":"message","role":"user","content":[{"type":"input_text","text":"synthetic question"}]}]`),
	}, response)
	refs := make([]map[string]string, len(ids))
	for index, id := range ids {
		refs[index] = map[string]string{"type": "item_reference", "id": id}
	}
	rawRefs, err := json.Marshal(refs)
	if err != nil {
		t.Fatal(err)
	}
	next := &transformerModel.InternalLLMRequest{Model: "gpt-test", PreviousResponseID: stringPtr(response.ID), RawAPIFormat: transformerModel.APIFormatOpenAIResponse, RawInputItems: rawRefs}
	windowBefore := append([]byte(nil), state.ReplayWindowItems...)
	replayed := state.BuildReplayRequest(next)
	if replayed == nil {
		t.Fatal("BuildReplayRequest failed")
	}
	body := messageIDRequestBody(t, replayed)
	var payload map[string]json.RawMessage
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatal(err)
	}
	if _, exists := payload["previous_response_id"]; exists {
		t.Errorf("local replay retained previous_response_id: %s", body)
	}
	items := messageIDPayloadItems(t, body, "input")
	if len(items) != 1+2*len(ids) {
		t.Fatalf("history/reference count changed: %s", body)
	}
	for index, id := range ids {
		if messageIDString(t, items[index+1]["id"]) != id || messageIDString(t, items[index+1+len(ids)]["id"]) != id {
			t.Errorf("client/cache/replay/reference ID mismatch for %q: %s", id, body)
		}
	}
	messageIDAssertFieldsUnchanged(t, messageIDDecodeItems(t, response.RawResponsesOutputItems), items[1:1+len(ids)])
	if !bytes.Equal(state.ReplayWindowItems, windowBefore) || !bytes.Equal(next.RawInputItems, rawRefs) || next.OpenAIPreviousResponseID() != response.ID {
		t.Fatal("replay mutated cached window or caller request")
	}
}

func messageIDRequestBody(t *testing.T, request *transformerModel.InternalLLMRequest) []byte {
	t.Helper()
	httpRequest, err := (&openaiOutbound.ResponseOutbound{}).TransformRequest(context.Background(), request, "https://example.invalid/v1", "synthetic-key")
	if err != nil {
		t.Fatal(err)
	}
	defer httpRequest.Body.Close()
	body, err := io.ReadAll(httpRequest.Body)
	if err != nil {
		t.Fatal(err)
	}
	return body
}

func messageIDPayloadItems(t *testing.T, payload []byte, field string) []map[string]json.RawMessage {
	t.Helper()
	var decoded map[string]json.RawMessage
	if err := json.Unmarshal(payload, &decoded); err != nil {
		t.Fatal(err)
	}
	return messageIDDecodeItems(t, decoded[field])
}

func messageIDDecodeItems(t *testing.T, raw json.RawMessage) []map[string]json.RawMessage {
	t.Helper()
	var items []map[string]json.RawMessage
	if err := json.Unmarshal(raw, &items); err != nil {
		t.Fatal(err)
	}
	return items
}

func messageIDString(t *testing.T, raw json.RawMessage) string {
	t.Helper()
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		t.Fatal(err)
	}
	return value
}

func messageIDAssertFieldsUnchanged(t *testing.T, before, after []map[string]json.RawMessage) {
	t.Helper()
	if len(before) != len(after) {
		t.Fatalf("raw item count changed: %d / %d", len(before), len(after))
	}
	for index, item := range before {
		if len(item) != len(after[index]) {
			t.Errorf("item %d field count changed: %d / %d", index, len(item), len(after[index]))
		}
		for field, value := range item {
			if field != "id" && !bytes.Equal(value, after[index][field]) {
				t.Errorf("item %d field %s changed: %s / %s", index, field, value, after[index][field])
			}
		}
	}
}
