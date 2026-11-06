package openai

import (
	"context"
	"encoding/json"
	"io"
	"strings"
	"testing"

	inbound "github.com/bestruirui/octopus/internal/transformer/inbound/openai"
	"github.com/bestruirui/octopus/internal/transformer/model"
)

func TestResponsesFunctionCallIDsMatchUpstreamFormat(t *testing.T) {
	req := &model.InternalLLMRequest{Model: "test", Messages: []model.Message{{
		Role: "assistant", ToolCalls: []model.ToolCall{{ID: "call_123", Type: "function", Function: model.FunctionCall{Name: "lookup", Arguments: "{}"}}},
	}}}
	input := ConvertToResponsesRequest(req).Input.Items
	if len(input) != 1 || !strings.HasPrefix(input[0].ID, "fc_") || input[0].CallID != "call_123" {
		t.Fatalf("expected independent fc_ item ID and unchanged call_id, got %#v", input)
	}
}

func TestResponsesRawFunctionCallIDsRepairLegacyHistory(t *testing.T) {
	for _, id := range []string{"call_123", "item_123", ""} {
		t.Run(id, func(t *testing.T) {
			raw := json.RawMessage(`[{"type":"function_call","id":"` + id + `","call_id":"call_123","name":"lookup","arguments":"{}"},{"type":"function_call_output","call_id":"call_123","item_reference":"` + id + `","output":"ok"}]`)
			var items []map[string]json.RawMessage
			if err := json.Unmarshal(sanitizeResponsesRawItems(raw), &items); err != nil {
				t.Fatal(err)
			}
			itemID := decodeRawString(items[0]["id"])
			if !strings.HasPrefix(itemID, "fc_") || decodeRawString(items[0]["call_id"]) != "call_123" {
				t.Fatalf("invalid repaired call: %s", items[0])
			}
			if decodeRawString(items[1]["item_reference"]) != itemID || decodeRawString(items[1]["call_id"]) != "call_123" {
				t.Fatalf("tool output reference not repaired: %s", items[1])
			}
		})
	}
}

func TestResponsesRawRequestRepairsOnlyInvalidFunctionCallIDs(t *testing.T) {
	raw := []byte(`{"model":"old","input":[{"type":"function_call","id":"call_legacy","call_id":"call_legacy","name":"lookup","arguments":"{}","native":{"number":900719925474099312345}},{"type":"function_call_output","call_id":"call_legacy","item_reference":"call_legacy","output":"ok"},{"type":"item_reference","id":"call_legacy"},{"type":"function_call","id":"fc_native","call_id":"call_native","name":"lookup","arguments":"{}"},{"type":"function_call","call_id":"call_missing","name":"lookup","arguments":"{}"}],"future":{"number":900719925474099312345},"nested":{"id":"call_legacy"}}`)
	req, err := (&ResponseOutbound{}).TransformRequestRaw(context.Background(), raw, "new", "https://example.invalid/v1", "dummy", nil)
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(req.Body)
	req.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	var payload struct {
		Input  []map[string]json.RawMessage `json:"input"`
		Future json.RawMessage              `json:"future"`
		Nested json.RawMessage              `json:"nested"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatal(err)
	}
	id := decodeRawString(payload.Input[0]["id"])
	if !strings.HasPrefix(id, "fc_") || decodeRawString(payload.Input[0]["call_id"]) != "call_legacy" {
		t.Fatalf("bad ID survived raw passthrough: %s", body)
	}
	if decodeRawString(payload.Input[1]["item_reference"]) != id || decodeRawString(payload.Input[2]["id"]) != id {
		t.Fatalf("legacy references did not follow repaired ID: %s", body)
	}
	if decodeRawString(payload.Input[3]["id"]) != "fc_native" || payload.Input[4]["id"] != nil {
		t.Fatalf("valid or optional ID was changed: %s", body)
	}
	if string(payload.Input[0]["native"]) != `{"number":900719925474099312345}` || string(payload.Future) != `{"number":900719925474099312345}` || string(payload.Nested) != `{"id":"call_legacy"}` {
		t.Fatalf("native fields changed: %s", body)
	}
	if string(raw) == string(body) || !strings.Contains(string(raw), `"id":"call_legacy"`) {
		t.Fatal("input buffer unexpectedly modified")
	}
}

func TestResponsesRawRequestKeepsValidInputBytes(t *testing.T) {
	raw := []byte("{\n  \"model\":\"test\", \"input\" : [ { \"type\":\"function_call\", \"id\":\"fc_native\", \"call_id\":\"call_native\" }, { \"type\":\"function_call\", \"call_id\":\"call_missing\" } ]\n}")
	req, err := (&ResponseOutbound{}).TransformRequestRaw(context.Background(), raw, "", "https://example.invalid/v1", "dummy", nil)
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(req.Body)
	req.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != string(raw) {
		t.Fatalf("valid input bytes changed: %q", body)
	}
}

func TestResponsesStreamPreservesNativeFunctionCallItemID(t *testing.T) {
	for _, raw := range []string{
		`{"type":"response.output_item.added","output_index":2,"item":{"type":"function_call","id":"fc_native","call_id":"call_native","name":"lookup"}}`,
		`{"type":"response.function_call_arguments.delta","output_index":2,"item_id":"fc_native","call_id":"call_native","name":"lookup","delta":"{}"}`,
	} {
		events, err := (&ResponseOutbound{}).TransformStreamEvent(context.Background(), []byte(raw))
		if err != nil {
			t.Fatal(err)
		}
		body, err := (&inbound.ResponseInbound{}).TransformStreamEvents(context.Background(), events)
		if err != nil {
			t.Fatal(err)
		}
		found := false
		for _, frame := range strings.Split(string(body), "\n\n") {
			if !strings.HasPrefix(frame, "data: ") {
				continue
			}
			var event struct {
				Type string         `json:"type"`
				Item *ResponsesItem `json:"item"`
			}
			if err := json.Unmarshal([]byte(strings.TrimPrefix(frame, "data: ")), &event); err != nil {
				t.Fatal(err)
			}
			if event.Type == "response.output_item.added" {
				found = true
				if event.Item.ID != "fc_native" || event.Item.CallID != "call_native" {
					t.Fatalf("native item identity changed: %s", body)
				}
			}
		}
		if !found {
			t.Fatal("no native function-call item emitted")
		}
	}
}
