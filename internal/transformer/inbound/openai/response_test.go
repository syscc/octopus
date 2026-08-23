package openai

import (
	"context"
	"encoding/json"
	"testing"
)

func TestResponseInboundMarksUnknownFieldsAsNativeOnly(t *testing.T) {
	inbound := &ResponseInbound{}
	req, err := inbound.TransformRequest(context.Background(), []byte(`{
		"model":"gpt-4o",
		"input":"hello",
		"future_responses_option":{"enabled":true}
	}`))
	if err != nil {
		t.Fatalf("TransformRequest failed: %v", err)
	}
	if !req.HasOpenAIResponsesPassthrough() || req.OpenAIResponsesPassthroughReasonTextValue() != "field:future_responses_option" {
		t.Fatalf("expected unknown field to require native Responses, got required=%t reason=%q", req.HasOpenAIResponsesPassthrough(), req.OpenAIResponsesPassthroughReasonTextValue())
	}
}

func TestResponseInboundMapsVerbosityToChatFallback(t *testing.T) {
	inbound := &ResponseInbound{}
	req, err := inbound.TransformRequest(context.Background(), []byte(`{
		"model":"gpt-5",
		"input":"hello",
		"text":{"verbosity":"high"}
	}`))
	if err != nil {
		t.Fatalf("TransformRequest failed: %v", err)
	}
	if req.HasOpenAIResponsesPassthrough() {
		t.Fatalf("expected supported verbosity to remain convertible, got reason %q", req.OpenAIResponsesPassthroughReasonTextValue())
	}
	if req.Verbosity == nil || *req.Verbosity != "high" {
		t.Fatalf("expected verbosity high in internal request, got %#v", req.Verbosity)
	}
}

func TestResponseInboundMarksReasoningInputNativeOnly(t *testing.T) {
	inbound := &ResponseInbound{}
	req, err := inbound.TransformRequest(context.Background(), []byte(`{
		"model":"gpt-5",
		"input":[{"type":"reasoning","encrypted_content":"enc"}]
	}`))
	if err != nil {
		t.Fatalf("TransformRequest failed: %v", err)
	}
	if !req.HasOpenAIResponsesPassthrough() {
		t.Fatal("expected reasoning input to require native Responses")
	}
}

func TestConvertToInternalRequestPreservesRawInputItems(t *testing.T) {
	req := &ResponsesRequest{
		Model: "gpt-4o",
		Input: ResponsesInput{Items: []ResponsesItem{
			{Type: "input_text", Text: stringPtr("hello")},
		}},
	}

	internalReq, err := convertToInternalRequest(req)
	if err != nil {
		t.Fatalf("convertToInternalRequest failed: %v", err)
	}
	if len(internalReq.RawInputItems) == 0 {
		t.Fatalf("expected raw input items to be preserved")
	}

	var items []map[string]any
	if err := json.Unmarshal(internalReq.RawInputItems, &items); err != nil {
		t.Fatalf("unmarshal raw input items failed: %v", err)
	}
	if len(items) != 1 || items[0]["type"] != "input_text" {
		t.Fatalf("expected original raw input items to be kept, got %#v", items)
	}
	if internalReq.TransformOptions.ArrayInputs == nil || !*internalReq.TransformOptions.ArrayInputs {
		t.Fatalf("expected array input flag to stay true")
	}
}

func TestConvertToInternalRequestMarksPassthroughForUnsupportedToolType(t *testing.T) {
	req := &ResponsesRequest{
		Model: "gpt-4o",
		Input: ResponsesInput{Text: stringPtr("hello")},
		Tools: []ResponsesTool{{
			Type: "apply_patch",
		}},
	}

	internalReq, err := convertToInternalRequest(req)
	if err != nil {
		t.Fatalf("convertToInternalRequest failed: %v", err)
	}
	if !internalReq.HasOpenAIResponsesPassthrough() {
		t.Fatalf("expected unsupported responses tool to require passthrough")
	}
	if ext := internalReq.GetOpenAIExtensions(); !ext.ResponsesPassthroughRequired || ext.ResponsesPassthroughReason != "tool:apply_patch" {
		t.Fatalf("expected OpenAI extension passthrough view, got %#v", ext)
	}
}

func TestConvertToInternalRequestMarksPassthroughForUnsupportedInputItem(t *testing.T) {
	req := &ResponsesRequest{
		Model: "gpt-4o",
		Input: ResponsesInput{Items: []ResponsesItem{{
			Type:   "apply_patch_call_output",
			CallID: "apc_123",
		}}},
	}

	internalReq, err := convertToInternalRequest(req)
	if err != nil {
		t.Fatalf("convertToInternalRequest failed: %v", err)
	}
	if !internalReq.HasOpenAIResponsesPassthrough() {
		t.Fatalf("expected unsupported responses input item to require passthrough")
	}
	if ext := internalReq.GetOpenAIExtensions(); !ext.ResponsesPassthroughRequired || ext.ResponsesPassthroughReason != "input:apply_patch_call_output" {
		t.Fatalf("expected OpenAI extension passthrough view, got %#v", ext)
	}
}

func TestConvertToInternalRequestDoesNotMarkPassthroughForSupportedFileAndAudioInputs(t *testing.T) {
	req := &ResponsesRequest{
		Model: "gpt-4o",
		Input: ResponsesInput{Items: []ResponsesItem{
			{
				Type: "message",
				Role: "user",
				Content: &ResponsesInput{Items: []ResponsesItem{
					{Type: "input_file", FileID: stringPtr("file_123")},
					{Type: "input_audio", InputAudio: &ResponsesInputAudio{Format: "wav", Data: "AAA="}},
				}},
			},
		}},
	}

	internalReq, err := convertToInternalRequest(req)
	if err != nil {
		t.Fatalf("convertToInternalRequest failed: %v", err)
	}
	if internalReq.HasOpenAIResponsesPassthrough() {
		t.Fatalf("expected supported file/audio inputs to stay normalized without passthrough")
	}
	if len(internalReq.Messages) != 1 || len(internalReq.Messages[0].Content.MultipleContent) != 2 {
		t.Fatalf("expected supported file/audio inputs to normalize into message content, got %#v", internalReq.Messages)
	}
	if internalReq.Messages[0].Content.MultipleContent[0].Type != "file" {
		t.Fatalf("expected file content part, got %#v", internalReq.Messages[0].Content.MultipleContent[0])
	}
	if internalReq.Messages[0].Content.MultipleContent[1].Type != "input_audio" {
		t.Fatalf("expected input_audio content part, got %#v", internalReq.Messages[0].Content.MultipleContent[1])
	}
}

func TestConvertToInternalRequestNormalizesTopLevelInputFile(t *testing.T) {
	req := &ResponsesRequest{
		Model: "gpt-4o",
		Input: ResponsesInput{Items: []ResponsesItem{{
			Type:     "input_file",
			FileID:   stringPtr("file_456"),
			Filename: stringPtr("notes.txt"),
		}}},
	}

	internalReq, err := convertToInternalRequest(req)
	if err != nil {
		t.Fatalf("convertToInternalRequest failed: %v", err)
	}
	if internalReq.HasOpenAIResponsesPassthrough() {
		t.Fatalf("expected top-level input_file to stay normalized without passthrough")
	}
	if len(internalReq.Messages) != 1 {
		t.Fatalf("expected one normalized message, got %#v", internalReq.Messages)
	}
	if internalReq.Messages[0].Role != "user" {
		t.Fatalf("expected top-level input_file to default to user role, got %#v", internalReq.Messages[0].Role)
	}
	if len(internalReq.Messages[0].Content.MultipleContent) != 1 || internalReq.Messages[0].Content.MultipleContent[0].Type != "file" {
		t.Fatalf("expected top-level input_file to become file content, got %#v", internalReq.Messages[0].Content)
	}
	if internalReq.Messages[0].Content.MultipleContent[0].File == nil || internalReq.Messages[0].Content.MultipleContent[0].File.FileID != "file_456" {
		t.Fatalf("expected normalized file reference to preserve file_id, got %#v", internalReq.Messages[0].Content.MultipleContent[0].File)
	}
}

func stringPtr(value string) *string {
	return &value
}

func TestConvertToInternalRequestMarksPassthroughForImageGenerationTool(t *testing.T) {
	req := &ResponsesRequest{
		Model: "gpt-4o",
		Input: ResponsesInput{Text: stringPtr("draw a cat")},
		Tools: []ResponsesTool{{
			Type:         "image_generation",
			Name:         "img_gen",
			Size:         "1024x1024",
			OutputFormat: "png",
		}},
	}

	internalReq, err := convertToInternalRequest(req)
	if err != nil {
		t.Fatalf("convertToInternalRequest failed: %v", err)
	}
	// image_generation 无法用 Chat Completions 表达，必须要求原生 Responses 通道，
	// 避免工具被 Chat 出站静默丢弃。
	if !internalReq.HasOpenAIResponsesPassthrough() {
		t.Fatalf("expected image_generation tool to require OpenAI Responses passthrough")
	}
	if ext := internalReq.GetOpenAIExtensions(); !ext.ResponsesPassthroughRequired || ext.ResponsesPassthroughReason != "tool:image_generation" {
		t.Fatalf("expected passthrough reason tool:image_generation, got %#v", ext)
	}
}
