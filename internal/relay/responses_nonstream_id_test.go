package relay

import (
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

func TestNonstreamResponsesClientCacheReplayFunctionCallIDs(t *testing.T) {
	for _, id := range []string{"call_x", "item_legacy", "", "fc_native"} {
		t.Run("id="+id, func(t *testing.T) {
			idField := ""
			if id != "" {
				idField = `"id":"` + id + `",`
			}
			raw := json.RawMessage(`[{"type":"reasoning","id":"rs_native","encrypted_content":"cipher","unknown_reasoning":{"large":9007199254740993123}},{"type":"function_call",` + idField + `"call_id":"call_x","name":"lookup","arguments":"{}","unknown_tool":{"large":9007199254740993123}}]`)
			originalRaw := string(raw)
			upstream := &transformerModel.InternalLLMResponse{
				ID: "resp_nonstream", Model: "gpt-test",
				RawResponsesOutputItems: raw,
				Choices: []transformerModel.Choice{{Message: &transformerModel.Message{
					Role:      "assistant",
					ToolCalls: []transformerModel.ToolCall{{ID: "call_x", Type: "function", Function: transformerModel.FunctionCall{Name: "lookup", Arguments: "{}"}}},
				}}},
			}
			originalUpstream, err := json.Marshal(upstream)
			if err != nil {
				t.Fatal(err)
			}
			adapter := &openaiInbound.ResponseInbound{}
			clientBody, err := adapter.TransformResponse(context.Background(), upstream)
			if err != nil {
				t.Fatal(err)
			}
			var client struct {
				Output []struct {
					Type   string `json:"type"`
					ID     string `json:"id"`
					CallID string `json:"call_id"`
				} `json:"output"`
			}
			if err := json.Unmarshal(clientBody, &client); err != nil {
				t.Fatal(err)
			}
			clientID := ""
			for _, item := range client.Output {
				if item.Type == "function_call" {
					clientID = item.ID
					if item.CallID != "call_x" {
						t.Fatalf("client call_id changed: %s", clientBody)
					}
				}
			}
			if !strings.HasPrefix(clientID, "fc_") || (id == "fc_native" && clientID != id) {
				t.Fatalf("invalid client ID: %q", clientID)
			}
			cached, err := adapter.GetInternalResponse(context.Background())
			if err != nil || cached == nil {
				t.Fatalf("get cached response: response=%v err=%v", cached, err)
			}
			var cachedItems []map[string]json.RawMessage
			if err := json.Unmarshal(cached.RawResponsesOutputItems, &cachedItems); err != nil {
				t.Fatal(err)
			}
			cachedID := ""
			for _, item := range cachedItems {
				if string(item["type"]) == `"function_call"` && len(item["id"]) > 0 {
					if err := json.Unmarshal(item["id"], &cachedID); err != nil {
						t.Fatal(err)
					}
				}
			}
			if cachedID != clientID {
				t.Fatalf("client/cache ID mismatch: client=%q cache=%q", clientID, cachedID)
			}
			cachedAgain, err := adapter.GetInternalResponse(context.Background())
			if err != nil || cachedAgain != cached {
				t.Fatalf("repeated retrieval changed cached response: first=%p next=%p err=%v", cached, cachedAgain, err)
			}
			state := &wsConversationState{LastOutboundTypeSet: true, LastOutboundType: outbound.OutboundTypeOpenAIResponse}
			state.ApplySuccessfulTurn(&transformerModel.InternalLLMRequest{
				Model: "gpt-test", RawAPIFormat: transformerModel.APIFormatOpenAIResponse,
				RawInputItems: json.RawMessage(`[{"type":"message","role":"user","content":[{"type":"input_text","text":"lookup"}]}]`),
			}, cached)
			replayed := state.BuildReplayRequest(&transformerModel.InternalLLMRequest{
				Model: "gpt-test", PreviousResponseID: stringPtr("resp_nonstream"),
				RawAPIFormat:  transformerModel.APIFormatOpenAIResponse,
				RawInputItems: json.RawMessage(`[{"type":"item_reference","id":"` + clientID + `"}]`),
			})
			if replayed == nil {
				t.Fatal("BuildReplayRequest failed")
			}
			request, err := (&openaiOutbound.ResponseOutbound{}).TransformRequest(context.Background(), replayed, "https://example.invalid/v1", "test-key")
			if err != nil {
				t.Fatal(err)
			}
			defer request.Body.Close()
			body, err := io.ReadAll(request.Body)
			if err != nil {
				t.Fatal(err)
			}
			var payload struct {
				Input []map[string]json.RawMessage `json:"input"`
			}
			if err := json.Unmarshal(body, &payload); err != nil {
				t.Fatal(err)
			}
			var targetID, referenceID string
			reasoningFound := false
			for _, item := range payload.Input {
				switch string(item["type"]) {
				case `"function_call"`:
					if err := json.Unmarshal(item["id"], &targetID); err != nil {
						t.Fatal(err)
					}
					if string(item["call_id"]) != `"call_x"` || string(item["unknown_tool"]) != `{"large":9007199254740993123}` {
						t.Fatalf("tool native fields changed: %s", body)
					}
				case `"item_reference"`:
					if err := json.Unmarshal(item["id"], &referenceID); err != nil {
						t.Fatal(err)
					}
				case `"reasoning"`:
					reasoningFound = true
					if string(item["encrypted_content"]) != `"cipher"` || string(item["unknown_reasoning"]) != `{"large":9007199254740993123}` {
						t.Fatalf("reasoning native fields changed: %s", body)
					}
				}
			}
			if !reasoningFound || targetID != clientID || referenceID != targetID {
				t.Fatalf("client/cache replay IDs or reasoning mismatch: client=%q target=%q reference=%q reasoning=%v", clientID, targetID, referenceID, reasoningFound)
			}
			currentUpstream, err := json.Marshal(upstream)
			if err != nil {
				t.Fatal(err)
			}
			if string(currentUpstream) != string(originalUpstream) || string(raw) != originalRaw {
				t.Fatal("caller upstream response or raw buffer was mutated")
			}
		})
	}
}

func TestNonstreamResponsesPassthroughSidecarPreservesRawIDs(t *testing.T) {
	for _, id := range []string{"call_x", "item_legacy", "", "fc_native"} {
		t.Run("id="+id, func(t *testing.T) {
			idField := ""
			if id != "" {
				idField = `"id":"` + id + `",`
			}
			raw := json.RawMessage(`[{"type":"function_call",` + idField + `"call_id":"call_x","name":"lookup","arguments":"{}","native":9007199254740993123}]`)
			originalRaw := string(raw)
			upstream := &transformerModel.InternalLLMResponse{
				ID: "resp_sidecar", RawResponsesOutputItems: raw,
				Choices: []transformerModel.Choice{{Message: &transformerModel.Message{
					ToolCalls: []transformerModel.ToolCall{{ID: "call_x", Type: "function", Function: transformerModel.FunctionCall{Name: "lookup", Arguments: "{}"}}},
				}}},
			}
			adapter := &openaiInbound.ResponseInbound{}
			adapter.PreservePassthroughOutputIDs()
			if _, err := adapter.TransformResponse(context.Background(), upstream); err != nil {
				t.Fatal(err)
			}
			cached, err := adapter.GetInternalResponse(context.Background())
			if err != nil || cached == nil {
				t.Fatalf("get sidecar cache: response=%v err=%v", cached, err)
			}
			if string(cached.RawResponsesOutputItems) != originalRaw || string(upstream.RawResponsesOutputItems) != originalRaw || string(raw) != originalRaw {
				t.Fatal("nonstream sidecar changed upstream raw IDs or fields")
			}
		})
	}
}
