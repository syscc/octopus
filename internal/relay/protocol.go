package relay

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/bestruirui/octopus/internal/transformer/inbound"
	transformerModel "github.com/bestruirui/octopus/internal/transformer/model"
	"github.com/bestruirui/octopus/internal/transformer/outbound"
	openaiOutbound "github.com/bestruirui/octopus/internal/transformer/outbound/openai"
)

// isOpenAIProtocolChannel reports whether a channel uses one of the two
// interchangeable OpenAI text protocols. The channel type remains a useful
// capability hint, but the downstream request chooses the first protocol to
// try for this request.
func isOpenAIProtocolChannel(channelType outbound.OutboundType) bool {
	return channelType == outbound.OutboundTypeOpenAIChat || channelType == outbound.OutboundTypeOpenAIResponse
}

// preferredOutboundTypes returns the downstream protocol first and the other
// OpenAI protocol second. A fallback is only available for OpenAI protocol
// channels; other providers retain their existing channel-specific behavior.
func preferredOutboundTypes(req *transformerModel.InternalLLMRequest, channelType outbound.OutboundType) (outbound.OutboundType, outbound.OutboundType, bool) {
	if req == nil || !isOpenAIProtocolChannel(channelType) {
		return channelType, 0, false
	}

	switch req.RawAPIFormat {
	case transformerModel.APIFormatOpenAIChatCompletion:
		return outbound.OutboundTypeOpenAIChat, outbound.OutboundTypeOpenAIResponse, true
	case transformerModel.APIFormatOpenAIResponse:
		return outbound.OutboundTypeOpenAIResponse, outbound.OutboundTypeOpenAIChat, true
	default:
		return channelType, 0, false
	}
}

func outboundTypeForRequest(req *transformerModel.InternalLLMRequest, channelType outbound.OutboundType) outbound.OutboundType {
	preferred, _, _ := preferredOutboundTypes(req, channelType)
	return preferred
}

func alternateOutboundType(req *transformerModel.InternalLLMRequest, current outbound.OutboundType) (outbound.OutboundType, bool) {
	if req == nil || !isOpenAIProtocolChannel(current) {
		return 0, false
	}
	switch req.RawAPIFormat {
	case transformerModel.APIFormatOpenAIChatCompletion:
		if current == outbound.OutboundTypeOpenAIChat {
			return outbound.OutboundTypeOpenAIResponse, true
		}
	case transformerModel.APIFormatOpenAIResponse:
		if current == outbound.OutboundTypeOpenAIResponse {
			return outbound.OutboundTypeOpenAIChat, true
		}
	}
	return 0, false
}

func (ra *relayAttempt) currentOutboundType() outbound.OutboundType {
	if ra == nil {
		return outbound.OutboundTypeOpenAIChat
	}
	if ra.activeOutboundTypeSet {
		return ra.activeOutboundType
	}
	if _, ok := ra.outAdapter.(*openaiOutbound.ResponseOutbound); ok {
		return outbound.OutboundTypeOpenAIResponse
	}
	if _, ok := ra.outAdapter.(*openaiOutbound.ChatOutbound); ok {
		return outbound.OutboundTypeOpenAIChat
	}
	if ra.channel != nil {
		return ra.channel.Type
	}
	return outbound.OutboundTypeOpenAIChat
}

func (ra *relayAttempt) setOutboundType(outboundType outbound.OutboundType) {
	if ra == nil {
		return
	}
	ra.activeOutboundType = outboundType
	ra.activeOutboundTypeSet = true
	ra.outAdapter = outbound.Get(outboundType)
}

func isOpenAIInbound(inboundType inbound.InboundType) bool {
	return inboundType == inbound.InboundTypeOpenAIChat || inboundType == inbound.InboundTypeOpenAIResponse
}

// requiresNativeResponsesUpstream protects Responses fields that have no
// equivalent in Chat Completions. Ordinary text, multimodal input, and
// function tools remain eligible for cross-protocol conversion.
func requiresNativeResponsesUpstream(req *transformerModel.InternalLLMRequest) bool {
	if req == nil || req.RawAPIFormat != transformerModel.APIFormatOpenAIResponse {
		return false
	}
	if req.IsOpenAIExactReplayRequest() {
		return true
	}
	if req.HasOpenAIResponsesPassthrough() {
		return true
	}

	opts := req.GetOpenAIResponsesOptions()
	if opts.PreviousResponseID != nil && strings.TrimSpace(*opts.PreviousResponseID) != "" {
		return true
	}
	if req.ReasoningBudget != nil {
		return true
	}
	if opts.Background != nil || len(opts.Prompt) > 0 || opts.PromptCacheRetention != nil {
		return true
	}
	if opts.MaxToolCalls != nil || len(opts.Conversation) > 0 || len(opts.ContextManagement) > 0 {
		return true
	}
	if len(opts.StreamOptions) > 0 || opts.ReasoningSummary != nil || opts.ReasoningGenerateSummary != nil {
		return true
	}
	if len(req.Include) > 0 || req.Truncation != nil {
		return true
	}
	return false
}

func canFallbackToOutbound(req *transformerModel.InternalLLMRequest, target outbound.OutboundType) bool {
	if req == nil {
		return false
	}
	if target == outbound.OutboundTypeOpenAIChat {
		return !requiresNativeResponsesUpstream(req)
	}
	if target != outbound.OutboundTypeOpenAIResponse || req.RawAPIFormat != transformerModel.APIFormatOpenAIChatCompletion {
		return true
	}

	// These Chat Completions controls have no equivalent in the current
	// Responses transformer. Refuse the fallback instead of silently dropping
	// caller intent.
	if req.FrequencyPenalty != nil || req.Logprobs != nil || req.PresencePenalty != nil || req.Seed != nil {
		return false
	}
	if req.LogitBias != nil || req.Stop != nil || req.TopK != nil || len(req.Modalities) > 0 || req.Audio != nil {
		return false
	}
	if len(req.Prediction) > 0 || len(req.WebSearchOptions) > 0 || req.Thinking != nil || req.EnableThinking != nil || req.User != nil {
		return false
	}
	// Chat stream_options has no equivalent in the current Responses
	// transformer. Reject it rather than silently changing usage semantics.
	if req.StreamOptions != nil {
		return false
	}
	for _, tool := range req.Tools {
		if tool.Type != "" && tool.Type != "function" {
			return false
		}
	}
	return true
}

// shouldTryProtocolFallback accepts only errors that indicate the selected
// endpoint is unavailable. A malformed request or an upstream model error must
// not be retried against another protocol and accidentally hide the real error.
func shouldTryProtocolFallback(statusCode int, err error) bool {
	message := ""
	if err != nil {
		message = strings.ToLower(err.Error())
	}
	if hasNonEndpointErrorMarker(message) || !hasEndpointCapabilityMarker(message) {
		return false
	}
	switch statusCode {
	case 0, http.StatusBadRequest, http.StatusNotFound, http.StatusMethodNotAllowed, http.StatusNotImplemented:
		return true
	default:
		return false
	}
}

func shouldTryProtocolFallbackForAttempt(req *transformerModel.InternalLLMRequest, channelType, currentType outbound.OutboundType, statusCode int, err error) bool {
	if req == nil || !isOpenAIProtocolChannel(channelType) || !isOpenAIProtocolChannel(currentType) {
		return false
	}
	if shouldTryProtocolFallback(statusCode, err) {
		return true
	}
	if channelType == currentType {
		return false
	}
	if statusCode != http.StatusNotFound && statusCode != http.StatusMethodNotAllowed && statusCode != http.StatusNotImplemented {
		return false
	}
	message := ""
	if err != nil {
		message = strings.ToLower(err.Error())
	}
	return !hasNonEndpointErrorMarker(message)
}

func hasEndpointCapabilityMarker(message string) bool {
	for _, marker := range []string{
		"unsupported endpoint",
		"unknown endpoint",
		"endpoint not found",
		"route not found",
		"path not found",
		"method not allowed",
		"unsupported method",
		"not implemented",
	} {
		if strings.Contains(message, marker) {
			return true
		}
	}
	return false
}

func hasNonEndpointErrorMarker(message string) bool {
	for _, marker := range []string{
		"model_not_found",
		"model not found",
		"response_not_found",
		"response not found",
		"previous_response_id",
		"previous response",
		"conversation",
		"unauthorized",
		"forbidden",
		"permission",
		"invalid parameter",
		"invalid_parameter",
		"invalid_request",
		"invalid request",
		"authentication",
		"insufficient_quota",
		"quota",
		"rate_limit",
		"rate limit",
		"request blocked",
		"blocked by",
		"waf",
	} {
		if strings.Contains(message, marker) {
			return true
		}
	}
	return false
}

type nativeResponsesAvailability struct {
	candidateFound         bool
	attempted              bool
	capable                bool
	endpointUnsupported    bool
	operationalUnavailable bool
}

func (s *nativeResponsesAvailability) markCandidate(channelType outbound.OutboundType) {
	if s != nil && isOpenAIProtocolChannel(channelType) {
		s.candidateFound = true
	}
}

func (s *nativeResponsesAvailability) markUnavailable() {
	if s != nil {
		s.operationalUnavailable = true
	}
}

func (s *nativeResponsesAvailability) markAttempt(result attemptResult, endpointUnsupported bool) {
	if s == nil {
		return
	}
	s.attempted = true
	if endpointUnsupported {
		s.endpointUnsupported = true
		return
	}
	if result.Success || result.StatusCode >= http.StatusBadRequest {
		s.capable = true
		return
	}
	s.operationalUnavailable = true
}

func (s nativeResponsesAvailability) shouldReportCapabilityError() bool {
	return !s.capable && !s.operationalUnavailable && (!s.candidateFound || s.endpointUnsupported)
}

func (s nativeResponsesAvailability) unavailableBeforeAttempt() bool {
	return s.candidateFound && s.operationalUnavailable && !s.attempted
}

func protocolFallbackError(req *transformerModel.InternalLLMRequest) error {
	if req != nil && req.RawAPIFormat == transformerModel.APIFormatOpenAIResponse {
		return fmt.Errorf("openai responses request requires a native responses upstream")
	}
	return fmt.Errorf("openai chat request could not find a compatible upstream protocol")
}
