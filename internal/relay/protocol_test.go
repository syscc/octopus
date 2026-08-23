package relay

import (
	"fmt"
	"net/http"
	"testing"

	transformerModel "github.com/bestruirui/octopus/internal/transformer/model"
	"github.com/bestruirui/octopus/internal/transformer/outbound"
)

func TestExactReplayRequiresNativeResponsesUpstream(t *testing.T) {
	req := &transformerModel.InternalLLMRequest{
		RawAPIFormat:  transformerModel.APIFormatOpenAIResponse,
		RawInputItems: []byte(`[{"type":"message","role":"user","content":[{"type":"input_text","text":"history"}]}]`),
	}
	req.MarkOpenAIExactReplayRequest()

	if !requiresNativeResponsesUpstream(req) {
		t.Fatalf("expected exact replay to require a native Responses upstream")
	}
	if canFallbackToOutbound(req, outbound.OutboundTypeOpenAIChat) {
		t.Fatalf("expected exact replay to reject Chat fallback")
	}
}

func TestChatFallbackRejectsUnrepresentableFields(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*transformerModel.InternalLLMRequest)
	}{
		{
			name: "top_k",
			mutate: func(req *transformerModel.InternalLLMRequest) {
				value := int64(20)
				req.TopK = &value
			},
		},
		{
			name: "stream_options",
			mutate: func(req *transformerModel.InternalLLMRequest) {
				req.StreamOptions = &transformerModel.StreamOptions{IncludeUsage: true}
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := &transformerModel.InternalLLMRequest{
				RawAPIFormat: transformerModel.APIFormatOpenAIChatCompletion,
			}
			tc.mutate(req)
			if canFallbackToOutbound(req, outbound.OutboundTypeOpenAIResponse) {
				t.Fatalf("expected %s to reject Chat to Responses fallback", tc.name)
			}
		})
	}
}

func TestResponsesNativeRequirementIncludesReasoningBudget(t *testing.T) {
	budget := int64(1024)
	req := &transformerModel.InternalLLMRequest{
		RawAPIFormat:    transformerModel.APIFormatOpenAIResponse,
		ReasoningBudget: &budget,
	}
	if !requiresNativeResponsesUpstream(req) {
		t.Fatal("expected Responses reasoning budget to require a native Responses upstream")
	}
}
func TestProtocolFallbackAllowsBareStatusOnlyForAlternateProbe(t *testing.T) {
	req := &transformerModel.InternalLLMRequest{RawAPIFormat: transformerModel.APIFormatOpenAIResponse}
	if !shouldTryProtocolFallbackForAttempt(req, outbound.OutboundTypeOpenAIChat, outbound.OutboundTypeOpenAIResponse, http.StatusNotFound, fmt.Errorf("upstream error: 404")) {
		t.Fatal("expected bare 404 to allow fallback when probing the non-declared protocol")
	}
	if shouldTryProtocolFallbackForAttempt(req, outbound.OutboundTypeOpenAIResponse, outbound.OutboundTypeOpenAIResponse, http.StatusNotFound, fmt.Errorf("upstream error: 404")) {
		t.Fatal("expected bare 404 on the declared protocol to preserve the upstream error")
	}
}

func TestProtocolFallbackOnlyForEndpointCapabilityErrors(t *testing.T) {
	for _, message := range []string{
		"upstream error: endpoint not found",
		"upstream error: method not allowed",
		"upstream error: route not found",
	} {
		if !shouldTryProtocolFallback(http.StatusNotFound, fmt.Errorf("%s", message)) {
			t.Fatalf("expected %q to allow protocol fallback", message)
		}
	}
	for _, tc := range []struct {
		status  int
		message string
	}{
		{http.StatusNotFound, "model_not_found"},
		{http.StatusNotFound, "previous_response_id not found"},
		{http.StatusMethodNotAllowed, "request blocked by WAF"},
		{http.StatusUnauthorized, "endpoint not found"},
		{http.StatusTooManyRequests, "rate_limit"},
		{http.StatusServiceUnavailable, "temporarily unavailable"},
	} {
		if shouldTryProtocolFallback(tc.status, fmt.Errorf("%s", tc.message)) {
			t.Fatalf("expected status=%d message=%q to preserve the original upstream error", tc.status, tc.message)
		}
	}
}
