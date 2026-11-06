package openai

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/bestruirui/octopus/internal/transformer/model"
)

func TestResponsesRequestNormalizesMessageAndReasoningIDs(t *testing.T) {
	for _, test := range []struct {
		name   string
		item   string
		prefix string
	}{
		{"message", `{"type":"message","role":"assistant","id":"item_legacy","content":[]}`, "msg_"},
		{"implicit_message", `{"role":"assistant","id":"item_legacy","content":"hello"}`, "msg_"},
		{"reasoning", `{"type":"reasoning","id":"item_legacy","encrypted_content":"opaque","summary":[{"type":"summary_text","text":"native"}]}`, "rs_"},
	} {
		t.Run(test.name, func(t *testing.T) {
			raw := []byte(`{"input":[{"type":"item_reference","id":"item_legacy"},` + test.item + `],"native":{"number":900719925474099312345}}`)
			original := bytes.Clone(raw)
			normalized, err := NormalizeResponsesRequestFunctionCallIDs(raw)
			if err != nil {
				t.Fatal(err)
			}
			var payload struct {
				Input []map[string]json.RawMessage `json:"input"`
			}
			if err := json.Unmarshal(normalized, &payload); err != nil {
				t.Fatal(err)
			}
			id := decodeRawString(payload.Input[1]["id"])
			if !strings.HasPrefix(id, test.prefix) || decodeRawString(payload.Input[0]["id"]) != id {
				t.Fatalf("item ID and preceding reference not repaired: %s", normalized)
			}
			var expected map[string]json.RawMessage
			if err := json.Unmarshal([]byte(test.item), &expected); err != nil {
				t.Fatal(err)
			}
			for field, value := range expected {
				if field != "id" && !bytes.Equal(payload.Input[1][field], value) {
					t.Fatalf("native %s changed: %s", field, normalized)
				}
			}
			if !bytes.Equal(raw, original) || !bytes.Contains(normalized, []byte(`"native":{"number":900719925474099312345}`)) {
				t.Fatalf("source or native root fields changed: %s", normalized)
			}
			second, err := NormalizeResponsesRequestFunctionCallIDs(normalized)
			if err != nil || !bytes.Equal(second, normalized) {
				t.Fatalf("normalization is not idempotent: %s, %v", second, err)
			}
		})
	}
}

func TestResponsesRequestPreservesValidOptionalAndUnknownItemIDs(t *testing.T) {
	raw := []byte(`{
  "input": [
    { "type":"message", "id":"msg_native", "role":"assistant", "content":[] },
    { "role":"user", "id":"msg_implicit", "content":"hello" },
    { "type":"reasoning", "id":"rs_native", "summary":[], "encrypted_content":"opaque" },
    { "type":"message", "role":"user", "content":"missing optional ID" },
    { "role":"assistant", "content":[] },
    { "type":"reasoning", "summary":[], "encrypted_content":"opaque" },
    { "type":"function_call", "call_id":"call_missing", "name":"lookup", "arguments":"{}" },
    { "type":"custom_tool_call", "id":"item_custom", "role":"assistant", "content":[] },
    { "type":"future_item", "id":"item_future", "role":"assistant", "content":[] },
    { "id":"item_without_content", "role":"assistant" },
    { "id":"item_without_role", "content":[] }
  ]
}`)
	normalized, err := NormalizeResponsesRequestFunctionCallIDs(raw)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(normalized, raw) {
		t.Fatalf("valid, optional, or unrelated IDs changed: %s", normalized)
	}
}

func TestResponsesRequestNormalizesMixedItemReferences(t *testing.T) {
	raw := []byte(`{"input":[{"type":"message","id":"item_message","role":"assistant","content":[{"type":"output_text","text":"hello","native":{"number":900719925474099312345}}]},{"type":"reasoning","id":"item_reasoning","encrypted_content":"opaque","summary":[{"type":"summary_text","text":"native"}]},{"type":"function_call","id":"item_function","call_id":"call_native","name":"lookup","arguments":"{}"},{"type":"item_reference","id":"item_message"},{"type":"item_reference","id":"item_reasoning"},{"type":"item_reference","id":"item_function"},{"type":"function_call_output","call_id":"call_native","item_reference":"item_function","output":{"number":900719925474099312345}},{"type":"item_reference","id":"item_unrelated"}]}`)
	normalized, err := NormalizeResponsesRequestFunctionCallIDs(raw)
	if err != nil {
		t.Fatal(err)
	}
	var payload struct {
		Input []map[string]json.RawMessage `json:"input"`
	}
	if err := json.Unmarshal(normalized, &payload); err != nil {
		t.Fatal(err)
	}
	for index, prefix := range []string{"msg_", "rs_", "fc_"} {
		id := decodeRawString(payload.Input[index]["id"])
		if !strings.HasPrefix(id, prefix) || decodeRawString(payload.Input[index+3]["id"]) != id {
			t.Fatalf("mixed item reference not repaired: %s", normalized)
		}
	}
	if decodeRawString(payload.Input[6]["item_reference"]) != decodeRawString(payload.Input[2]["id"]) ||
		decodeRawString(payload.Input[2]["call_id"]) != "call_native" || decodeRawString(payload.Input[6]["call_id"]) != "call_native" ||
		decodeRawString(payload.Input[7]["id"]) != "item_unrelated" {
		t.Fatalf("call identity or unrelated reference changed: %s", normalized)
	}
	if string(payload.Input[6]["output"]) != `{"number":900719925474099312345}` ||
		string(payload.Input[1]["encrypted_content"]) != `"opaque"` ||
		string(payload.Input[1]["summary"]) != `[{"type":"summary_text","text":"native"}]` ||
		!bytes.Contains(payload.Input[0]["content"], []byte(`900719925474099312345`)) {
		t.Fatalf("native fields changed: %s", normalized)
	}
}

func TestResponsesMessageIDsNormalizeThroughRawAndReplayRequests(t *testing.T) {
	rawItems := json.RawMessage(`[{"type":"message","id":"item_message","role":"assistant","content":[]},{"type":"reasoning","id":"item_reasoning","encrypted_content":"opaque","summary":[{"type":"summary_text","text":"native"}]},{"type":"item_reference","id":"item_message"},{"type":"message","role":"user","content":"no ID"},{"type":"reasoning","summary":[{"type":"summary_text","text":"native"}]}]`)
	for _, source := range []string{"raw_passthrough", "raw_input", "replay", "legacy_extension"} {
		t.Run(source, func(t *testing.T) {
			var body []byte
			if source == "raw_passthrough" {
				req, err := (&ResponseOutbound{}).TransformRequestRaw(context.Background(), append([]byte(`{"input":`), append(bytes.Clone(rawItems), '}')...), "test", "https://example.invalid/v1", "dummy", nil)
				if err != nil {
					t.Fatal(err)
				}
				defer req.Body.Close()
				body, err = io.ReadAll(req.Body)
				if err != nil {
					t.Fatal(err)
				}
			} else {
				req := &model.InternalLLMRequest{Model: "test", RawAPIFormat: model.APIFormatOpenAIResponse}
				if source == "legacy_extension" {
					req.SetOpenAIExtensions(model.OpenAIExtension{RawResponseItems: bytes.Clone(rawItems)})
				} else {
					req.SetOpenAIRawInputItems(rawItems)
					if source == "replay" {
						req.MarkOpenAIExactReplayRequest()
					}
				}
				var err error
				body, err = json.Marshal(ConvertToResponsesRequest(req))
				if err != nil {
					t.Fatal(err)
				}
			}
			var payload struct {
				Input []map[string]json.RawMessage `json:"input"`
			}
			if err := json.Unmarshal(body, &payload); err != nil {
				t.Fatal(err)
			}
			id := decodeRawString(payload.Input[0]["id"])
			if !strings.HasPrefix(id, "msg_") || decodeRawString(payload.Input[2]["id"]) != id || !strings.HasPrefix(decodeRawString(payload.Input[1]["id"]), "rs_") {
				t.Fatalf("request path did not normalize IDs: %s", body)
			}
			if payload.Input[3]["id"] != nil || payload.Input[4]["id"] != nil || string(payload.Input[1]["encrypted_content"]) != `"opaque"` || string(payload.Input[1]["summary"]) != `[{"type":"summary_text","text":"native"}]` {
				t.Fatalf("optional ID or native reasoning changed: %s", body)
			}
		})
	}
}

func TestResponsesRequestRepairsSyntheticLongMessageHistory(t *testing.T) {
	items := make([]json.RawMessage, 982)
	for index := range items {
		items[index] = json.RawMessage(fmt.Sprintf(`{"type":"message","role":"assistant","id":"msg_%024x","content":[{"type":"output_text","text":"synthetic history"}]}`, index))
	}
	for _, index := range []int{958, 969, 974} {
		items[index] = bytes.Replace(items[index], []byte(`"msg_`), []byte(`"item_`), 1)
	}
	rawItems, err := json.Marshal(items)
	if err != nil {
		t.Fatal(err)
	}
	raw := append([]byte(`{"input":`), append(rawItems, '}')...)
	normalized, err := NormalizeResponsesRequestFunctionCallIDs(raw)
	if err != nil {
		t.Fatal(err)
	}
	var payload struct {
		Input []map[string]json.RawMessage `json:"input"`
	}
	if err := json.Unmarshal(normalized, &payload); err != nil {
		t.Fatal(err)
	}
	if len(payload.Input) != len(items) {
		t.Fatalf("history length changed: %d", len(payload.Input))
	}
	for index, item := range payload.Input {
		id := decodeRawString(item["id"])
		if !strings.HasPrefix(id, "msg_") {
			t.Fatalf("history item %d has invalid ID %q", index, id)
		}
		if index != 958 && index != 969 && index != 974 && id != fmt.Sprintf("msg_%024x", index) {
			t.Fatalf("valid history item %d changed: %q", index, id)
		}
	}
}
