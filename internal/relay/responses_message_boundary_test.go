package relay

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	openaiInbound "github.com/bestruirui/octopus/internal/transformer/inbound/openai"
	transformerModel "github.com/bestruirui/octopus/internal/transformer/model"
	openaiOutbound "github.com/bestruirui/octopus/internal/transformer/outbound/openai"
)

func TestResponsesRawOnlyNativeItemBeforeLegacyStreamItem(t *testing.T) {
	for _, tc := range []struct {
		itemType, nativeID, eventType, raw string
		anonymous, sentFirst               bool
	}{
		{"message", "msg_raw", "response.output_text.delta", `[{"type":"message","id":"msg_raw","role":"assistant","content":[]},{"type":"message","id":"item_legacy","role":"assistant","content":[{"type":"output_text","text":"sent"}]}]`, false, false},
		{"reasoning", "rs_raw", "response.reasoning_summary_text.delta", `[{"type":"reasoning","id":"rs_raw","summary":[],"encrypted_content":"synthetic_cipher_raw"},{"type":"reasoning","id":"item_legacy","summary":[{"type":"summary_text","text":"sent"}]}]`, false, false},
		{"message", "", "response.output_text.delta", `[{"type":"message","id":"msg_native","role":"assistant","content":[{"type":"output_text","text":"sent"}]},{"type":"message","id":"item_raw_only","role":"assistant","content":[]}]`, true, true},
		{"message", "", "response.output_text.delta", `[{"type":"message","id":"item_raw_only","role":"assistant","content":[]},{"type":"message","id":"msg_native","role":"assistant","content":[{"type":"output_text","text":"sent"}]}]`, true, false},
		{"reasoning", "", "response.reasoning_summary_text.delta", `[{"type":"reasoning","id":"rs_native","summary":[{"type":"summary_text","text":"sent"}]},{"type":"reasoning","id":"item_raw_only","summary":[]}]`, true, true},
		{"reasoning", "", "response.reasoning_summary_text.delta", `[{"type":"reasoning","id":"item_raw_only","summary":[]},{"type":"reasoning","id":"rs_native","summary":[{"type":"summary_text","text":"sent"}]}]`, true, false},
	} {
		t.Run(fmt.Sprintf("%s/anonymous_%t/sent_first_%t", tc.itemType, tc.anonymous, tc.sentFirst), func(t *testing.T) {
			ctx := context.Background()
			upstream, downstream := &openaiOutbound.ResponseOutbound{}, &openaiInbound.ResponseInbound{}
			itemID := "item_legacy"
			if tc.anonymous {
				itemID = ""
			}
			upstreamOutputIndex := 1
			if tc.sentFirst {
				upstreamOutputIndex = 0
			}
			frames := []string{
				`{"type":"response.created","response":{"id":"resp_raw_before_legacy","model":"gpt-test","status":"in_progress","output":[]}}`,
				fmt.Sprintf(`{"type":"%s","output_index":%d,"content_index":0,"summary_index":0,"item_id":"%s","delta":"sent"}`, tc.eventType, upstreamOutputIndex, itemID),
				`{"type":"response.completed","response":{"id":"resp_raw_before_legacy","model":"gpt-test","status":"completed","output":` + tc.raw + `,"usage":{"input_tokens":1,"output_tokens":2,"total_tokens":3}}}`,
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
			var clientID string
			var addedIndex, deltaIndex, doneIndex int
			var sawAddedIndex, sawDeltaIndex, sawDoneIndex bool
			var terminal []map[string]json.RawMessage
			for _, event := range responseBoundarySSEEvents(t, wire) {
				if event.Type == "response.output_item.added" {
					if clientID != "" {
						t.Fatal("legacy stream item was added more than once")
					}
					clientID = messageIDString(t, event.Item["id"])
					if event.OutputIndex != nil {
						addedIndex, sawAddedIndex = *event.OutputIndex, true
					}
				}
				if event.Type == tc.eventType && (event.ItemID != clientID || event.Delta != "sent") {
					t.Errorf("legacy delta differs from added item: id=%s delta=%s added=%s", event.ItemID, event.Delta, clientID)
				}
				if event.Type == tc.eventType && event.Delta == "sent" && event.OutputIndex != nil {
					deltaIndex, sawDeltaIndex = *event.OutputIndex, true
				}
				if event.Type == "response.output_item.done" && event.Item != nil && messageIDString(t, event.Item["id"]) == clientID && event.OutputIndex != nil {
					doneIndex, sawDoneIndex = *event.OutputIndex, true
				}
				if event.Type == "response.completed" {
					terminal = messageIDPayloadItems(t, event.Response, "output")
				}
			}
			prefix := map[string]string{"message": "msg_", "reasoning": "rs_"}[tc.itemType]
			if !strings.HasPrefix(clientID, prefix) || clientID == tc.nativeID {
				t.Fatalf("invalid client legacy item ID: %q", clientID)
			}
			cached, err := downstream.GetInternalResponse(ctx)
			if err != nil || cached == nil {
				t.Fatalf("get response cache: %v %v", cached, err)
			}
			sentIndex := 1
			if tc.sentFirst {
				sentIndex = 0
			}
			rawOnlyIndex := 1 - sentIndex
			if !sawAddedIndex || !sawDeltaIndex || !sawDoneIndex || addedIndex != sentIndex || deltaIndex != sentIndex || doneIndex != sentIndex {
				t.Fatalf("stream item output_index mismatch: added=%d delta=%d done=%d want=%d", addedIndex, deltaIndex, doneIndex, sentIndex)
			}
			for name, items := range map[string][]map[string]json.RawMessage{"terminal": terminal, "cache": messageIDDecodeItems(t, cached.RawResponsesOutputItems)} {
				if len(items) != 2 {
					t.Fatalf("%s lost native or legacy item: got %d", name, len(items))
				}
				rawOnlyID := messageIDString(t, items[rawOnlyIndex]["id"])
				if !strings.HasPrefix(rawOnlyID, prefix) || rawOnlyID == clientID || (tc.nativeID != "" && rawOnlyID != tc.nativeID) || messageIDItemText(t, items[rawOnlyIndex]) != "" {
					t.Errorf("%s changed raw-only identity/content: id=%s text=%s", name, rawOnlyID, messageIDItemText(t, items[rawOnlyIndex]))
				}
				if messageIDString(t, items[sentIndex]["id"]) != clientID || messageIDItemText(t, items[sentIndex]) != "sent" {
					t.Errorf("%s attached sent ID to wrong item: id=%s text=%s want id=%s", name, items[sentIndex]["id"], messageIDItemText(t, items[sentIndex]), clientID)
				}
			}
			messageIDAssertReplay(t, cached, []string{messageIDString(t, terminal[0]["id"]), messageIDString(t, terminal[1]["id"])})
		})
	}
}

func TestResponsesPassthroughEOFInterleavedNativeMessageSlots(t *testing.T) {
	ctx := context.Background()
	upstream, sidecar := &openaiOutbound.ResponseOutbound{}, &openaiInbound.ResponseInbound{}
	sidecar.PreservePassthroughOutputIDs()
	frames := []string{
		`{"type":"response.created","sequence_number":0,"response":{"id":"resp_passthrough_eof","model":"gpt-test","status":"in_progress","output":[]}}`,
		`{"type":"response.output_item.added","sequence_number":1,"output_index":0,"item":{"type":"message","id":"msg_one","role":"assistant","content":[]}}`,
		`{"type":"response.output_item.added","sequence_number":2,"output_index":1,"item":{"type":"message","id":"msg_two","role":"assistant","content":[]}}`,
		`{"type":"response.output_text.delta","sequence_number":3,"output_index":0,"content_index":0,"item_id":"msg_one","delta":"one"}`,
		`{"type":"response.output_text.delta","sequence_number":4,"output_index":1,"content_index":0,"item_id":"msg_two","delta":"two"}`,
		`{"type":"response.output_text.delta","sequence_number":5,"output_index":0,"content_index":0,"item_id":"msg_one","delta":"again"}`,
	}
	var rawStream []byte
	for _, frame := range frames {
		rawStream = append(rawStream, []byte("data: "+frame+"\n\n")...)
		events, err := upstream.TransformStreamEvent(ctx, []byte(frame))
		if err != nil {
			t.Fatal(err)
		}
		// Metrics-sidecar encoded events are discarded on the raw passthrough path.
		if _, err := sidecar.TransformStreamEvents(ctx, events); err != nil {
			t.Fatal(err)
		}
	}
	final, err := sidecar.FinalizeIncompleteStream(ctx, rawStream)
	if err != transformerModel.ErrIncompleteUpstreamStream {
		t.Fatalf("EOF finalization error: %v", err)
	}
	indices := map[string]int{"msg_one": 0, "msg_two": 1}
	wantText := map[string]string{"msg_one": "oneagain", "msg_two": "two"}
	done := map[string]int{}
	textDone := map[string]int{}
	var terminal []map[string]json.RawMessage
	terminalCount := 0
	for _, event := range responseBoundarySSEEvents(t, final) {
		id := event.ItemID
		if event.Item != nil {
			id = messageIDString(t, event.Item["id"])
		}
		if id != "" {
			index, exists := indices[id]
			if !exists || event.OutputIndex == nil || *event.OutputIndex != index {
				t.Errorf("EOF %s changed original slot for %s", event.Type, id)
			}
		}
		switch event.Type {
		case "response.output_text.done":
			textDone[id]++
			if event.Text != wantText[id] {
				t.Errorf("EOF text for %s incomplete: %q want %q", id, event.Text, wantText[id])
			}
		case "response.output_item.done":
			done[id]++
			if messageIDItemText(t, event.Item) != wantText[id] {
				t.Errorf("EOF done content for %s changed: %q", id, messageIDItemText(t, event.Item))
			}
		case "response.incomplete":
			terminalCount++
			terminal = messageIDPayloadItems(t, event.Response, "output")
		}
	}
	for id := range indices {
		if done[id] != 1 || textDone[id] != 1 {
			t.Errorf("EOF item %s closed repeatedly or missing: done=%d text_done=%d", id, done[id], textDone[id])
		}
	}
	if terminalCount != 1 || len(terminal) != 2 || len(done) != 2 || len(textDone) != 2 {
		t.Fatalf("EOF terminal or item count changed: terminals=%d output=%d done=%v text_done=%v", terminalCount, len(terminal), done, textDone)
	}
	for index, id := range []string{"msg_one", "msg_two"} {
		if messageIDString(t, terminal[index]["id"]) != id || messageIDItemText(t, terminal[index]) != wantText[id] {
			t.Errorf("EOF terminal slot %d identity/content changed: id=%s text=%s", index, terminal[index]["id"], messageIDItemText(t, terminal[index]))
		}
	}
}

type responseBoundarySSEEvent struct {
	Type        string                     `json:"type"`
	ItemID      string                     `json:"item_id"`
	OutputIndex *int                       `json:"output_index"`
	Delta       string                     `json:"delta"`
	Text        string                     `json:"text"`
	Item        map[string]json.RawMessage `json:"item"`
	Response    json.RawMessage            `json:"response"`
}

func responseBoundarySSEEvents(t *testing.T, wire []byte) []responseBoundarySSEEvent {
	t.Helper()
	var events []responseBoundarySSEEvent
	for _, line := range strings.Split(string(wire), "\n") {
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		var event responseBoundarySSEEvent
		if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &event); err != nil {
			t.Fatal(err)
		}
		events = append(events, event)
	}
	return events
}
