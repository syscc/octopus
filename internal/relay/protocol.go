package relay

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	dbmodel "github.com/bestruirui/octopus/internal/model"
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
	return dbmodel.IsOpenAITextChannelType(channelType)
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

func effectiveOpenAIProtocolCapability(channel *dbmodel.Channel, protocol outbound.OutboundType) dbmodel.OpenAIProtocolCapability {
	if channel == nil || !isOpenAIProtocolChannel(channel.Type) {
		return dbmodel.OpenAIProtocolCapabilityUnsupported
	}
	if isCloudflareWorkersAIChannel(channel) {
		if protocol == outbound.OutboundTypeOpenAIChat {
			return dbmodel.OpenAIProtocolCapabilitySupported
		}
		return dbmodel.OpenAIProtocolCapabilityUnsupported
	}
	return channel.EffectiveOpenAIProtocolCapability(protocol)
}

// outboundTypeForChannel selects the first network protocol for this channel.
// Unknown downstream-native support is probed; only a persisted unsupported
// result or a manual mode can bypass that probe.
func outboundTypeForChannel(req *transformerModel.InternalLLMRequest, channel *dbmodel.Channel) (outbound.OutboundType, bool) {
	if channel == nil {
		return 0, false
	}
	preferred, alternate, hasAlternate := preferredOutboundTypes(req, channel.Type)
	if !hasAlternate {
		return preferred, true
	}
	if effectiveOpenAIProtocolCapability(channel, preferred) != dbmodel.OpenAIProtocolCapabilityUnsupported {
		return preferred, true
	}
	if !canFallbackToOutbound(req, alternate) ||
		effectiveOpenAIProtocolCapability(channel, alternate) == dbmodel.OpenAIProtocolCapabilityUnsupported {
		return 0, false
	}
	return alternate, true
}

func channelAllowsOutboundProtocol(channel *dbmodel.Channel, protocol outbound.OutboundType) bool {
	if channel == nil || !isOpenAIProtocolChannel(channel.Type) || !isOpenAIProtocolChannel(protocol) {
		return false
	}
	return effectiveOpenAIProtocolCapability(channel, protocol) != dbmodel.OpenAIProtocolCapabilityUnsupported
}

func protocolCandidateRank(req *transformerModel.InternalLLMRequest, channel *dbmodel.Channel) int {
	if req == nil || channel == nil || !isOpenAIProtocolChannel(channel.Type) {
		return 5
	}
	preferred, alternate, ok := preferredOutboundTypes(req, channel.Type)
	if !ok {
		return 5
	}
	preferredCapability := effectiveOpenAIProtocolCapability(channel, preferred)
	switch preferredCapability {
	case dbmodel.OpenAIProtocolCapabilitySupported:
		return 0
	case dbmodel.OpenAIProtocolCapabilityUnknown:
		if channel.Type == preferred {
			return 1
		}
		return 2
	}
	if !canFallbackToOutbound(req, alternate) {
		return 6
	}
	switch effectiveOpenAIProtocolCapability(channel, alternate) {
	case dbmodel.OpenAIProtocolCapabilitySupported:
		return 3
	case dbmodel.OpenAIProtocolCapabilityUnknown:
		return 4
	default:
		return 6
	}
}

// isCloudflareWorkersAIChannel recognizes the documented Workers AI
// OpenAI-compatible base. It is retained for provider-specific error parsing;
// effectiveOpenAIProtocolCapability enforces the provider's Chat-only contract.
func isCloudflareWorkersAIChannel(channel *dbmodel.Channel) bool {
	if channel == nil {
		return false
	}
	for _, baseURL := range channel.BaseUrls {
		parsed, err := url.Parse(strings.TrimSpace(baseURL.URL))
		if err == nil && dbmodel.ValidateCloudflareWorkersAIBaseURL(parsed) == nil {
			return true
		}
	}
	return false
}

func alternateOutboundType(req *transformerModel.InternalLLMRequest, current outbound.OutboundType) (outbound.OutboundType, bool) {
	if req == nil || !isOpenAIProtocolChannel(current) {
		return 0, false
	}
	switch req.RawAPIFormat {
	case transformerModel.APIFormatOpenAIChatCompletion, transformerModel.APIFormatOpenAIResponse:
	default:
		return 0, false
	}
	if current == outbound.OutboundTypeOpenAIChat {
		return outbound.OutboundTypeOpenAIResponse, true
	}
	return outbound.OutboundTypeOpenAIChat, true
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
	// A standalone tool output depends on upstream conversation state when its
	// matching assistant tool call is absent from this request. Chat conversion
	// cannot reconstruct that state safely.
	if requiresUpstreamWSContinuation(req) {
		return true
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
// OpenAI-style JSON errors typed invalid_request_error are the exception when
// their own message reports a missing URL/route/endpoint/path: providers emit
// those bodies (e.g. "Invalid URL (POST /v1/responses)") when an endpoint is
// not deployed, so they act as capability signals rather than ordinary
// invalid-parameter rejections.
func shouldTryProtocolFallback(statusCode int, err error) bool {
	if isOpenAIEndpointRoutingError(statusCode, err) {
		return true
	}
	message := upstreamClassificationMessage(err)
	if hasNonEndpointErrorMarker(message) || !hasEndpointCapabilityMarker(message) {
		return false
	}
	switch statusCode {
	case 0, http.StatusNotFound, http.StatusMethodNotAllowed, http.StatusNotImplemented:
		return true
	default:
		return false
	}
}

func shouldTryProtocolFallbackForAttempt(req *transformerModel.InternalLLMRequest, channel *dbmodel.Channel, currentType outbound.OutboundType, statusCode int, err error) bool {
	if isDownstreamWriteError(err) {
		return false
	}
	if req == nil || channel == nil || !isOpenAIProtocolChannel(channel.Type) || !isOpenAIProtocolChannel(currentType) {
		return false
	}
	if isResponsesModelUnsupportedError(currentType, statusCode, err) ||
		isCloudflareWorkersAIResponsesSchemaError(req, channel, currentType, statusCode, err) {
		return true
	}
	if shouldTryProtocolFallback(statusCode, err) {
		return true
	}
	if channel.Type == currentType {
		return false
	}
	if statusCode != http.StatusNotFound && statusCode != http.StatusMethodNotAllowed && statusCode != http.StatusNotImplemented {
		return false
	}
	message := upstreamClassificationMessage(err)
	// A structured JSON error without endpoint evidence is ambiguous. Keep the
	// intentional model-scoped fallback (for providers that expose a model as
	// unavailable on one protocol), but do not treat arbitrary resource-not-found
	// responses as a protocol probe failure on every request.
	if _, structured := openAIErrorDetailFromUpstream(err); structured {
		return isModelScopedProtocolFallbackError(err)
	}
	return !hasNonEndpointErrorMarker(message)
}

// isModelScopedProtocolFallbackError recognizes a model availability error that
// may legitimately differ between Chat and Responses. It is deliberately
// narrower than a generic 404 so unrelated missing-resource responses do not
// cause repeated protocol probes.
func isModelScopedProtocolFallbackError(err error) bool {
	detail, ok := openAIErrorDetailFromUpstream(err)
	if !ok {
		return false
	}
	message := strings.ToLower(strings.TrimSpace(detail.Message))
	typ := strings.ToLower(strings.TrimSpace(detail.Type))
	return strings.Contains(message, "model") &&
		(strings.Contains(message, "not found") || strings.Contains(message, "not available") || strings.Contains(typ, "not_found"))
}

func shouldLearnProtocolUnsupportedForAttempt(req *transformerModel.InternalLLMRequest, channel *dbmodel.Channel, currentType outbound.OutboundType, statusCode int, err error) bool {
	// This code is explicitly model-scoped. Persisting it at channel scope would
	// incorrectly disable Responses for every other model on the same provider.
	if isResponsesModelUnsupportedError(currentType, statusCode, err) {
		return false
	}
	// A structured JSON error object without an explicit endpoint/routing marker
	// describes an ordinary request failure on the alternate probe, not channel
	// capability. It may still fall back for this request, but must never persist
	// a channel-wide unsupported capability. Bare or non-JSON endpoint statuses
	// keep the existing learning policy.
	if jsonErrorLacksEndpointRoutingMarker(statusCode, err) {
		return false
	}
	return shouldTryProtocolFallbackForAttempt(req, channel, currentType, statusCode, err)
}

// jsonErrorLacksEndpointRoutingMarker reports whether the upstream failure
// carries a structured OpenAI-style error object that itself gives no explicit
// endpoint/routing evidence (neither a routing message nor an endpoint marker).
func jsonErrorLacksEndpointRoutingMarker(statusCode int, err error) bool {
	detail, ok := openAIErrorDetailFromUpstream(err)
	if !ok {
		return false
	}
	if isOpenAIEndpointRoutingError(statusCode, err) {
		return false
	}
	message := strings.ToLower(strings.TrimSpace(detail.Message))
	if hasEndpointCapabilityMarker(message) || hasOpenAIInvalidEndpointURL(message) {
		return false
	}
	return true
}

// isResponsesModelUnsupportedError recognizes an explicit provider capability
// code. It is intentionally limited to a failed Responses attempt; ordinary
// model, authentication, parameter, quota, and rate-limit errors remain final.
func isResponsesModelUnsupportedError(currentType outbound.OutboundType, statusCode int, err error) bool {
	if currentType != outbound.OutboundTypeOpenAIResponse {
		return false
	}
	detail, ok := openAIErrorDetailFromUpstream(err)
	if !ok || !openAIErrorCodeIs(detail, "RESPONSES_MODEL_NOT_SUPPORTED") {
		return false
	}
	var wsErr *wsUpstreamEventError
	return statusCode == http.StatusBadRequest || errors.As(err, &wsErr)
}

// isCloudflareWorkersAIResponsesSchemaError recognizes the exact Workers AI
// Chat-schema rejection observed when a Responses payload is sent to /responses.
// The provider URL, error code, and schema markers must all match so a generic
// invalid_prompt or parameter error never becomes a protocol fallback.
func isCloudflareWorkersAIResponsesSchemaError(req *transformerModel.InternalLLMRequest, channel *dbmodel.Channel, currentType outbound.OutboundType, statusCode int, err error) bool {
	if req == nil || req.RawAPIFormat != transformerModel.APIFormatOpenAIResponse || requiresNativeResponsesUpstream(req) ||
		currentType != outbound.OutboundTypeOpenAIResponse || statusCode != http.StatusBadRequest ||
		!isCloudflareWorkersAIChannel(channel) {
		return false
	}
	detail, ok := openAIErrorDetailFromUpstream(err)
	if !ok || !openAIErrorCodeIs(detail, "invalid_prompt") {
		return false
	}
	message := strings.ToLower(detail.Message)
	return strings.Contains(message, "oneof at '/' not met") &&
		strings.Contains(message, "0 matches") &&
		strings.Contains(message, "required properties at '/' are 'messages'")
}

// isOpenAIEndpointRoutingError reports whether the upstream answered a route
// status with an OpenAI-style JSON error whose type is invalid_request_error
// while the message itself reports a missing URL/route/endpoint/path (for
// example "Invalid URL (POST /v1/responses)"). Such bodies describe endpoint
// availability rather than request validity, so they must not block protocol
// fallback through the generic "invalid_request" marker.
func isOpenAIEndpointRoutingError(statusCode int, err error) bool {
	detail, ok := openAIErrorDetailFromUpstream(err)
	if !ok || strings.ToLower(strings.TrimSpace(detail.Type)) != "invalid_request_error" {
		return false
	}
	message := strings.ToLower(strings.TrimSpace(detail.Message))
	if message == "" || hasNonEndpointErrorMarker(message) {
		return false
	}
	var wsErr *wsUpstreamEventError
	if errors.As(err, &wsErr) {
		status := wsErr.Status
		if status == 0 {
			status = statusCode
		}
		// Responses WS providers often omit an HTTP-equivalent status or wrap
		// protocol errors as 502. Preserve that compatibility, but never let an
		// explicit auth, rate-limit, request-state, or upstream 5xx status poison
		// channel-wide endpoint capability.
		switch status {
		case 0, http.StatusBadRequest, http.StatusNotFound, http.StatusMethodNotAllowed,
			http.StatusNotImplemented, http.StatusBadGateway:
			return hasEndpointCapabilityMarker(message) || hasOpenAIInvalidEndpointURL(message)
		default:
			return false
		}
	}
	switch statusCode {
	case http.StatusBadRequest:
		return hasOpenAIInvalidEndpointURL(message)
	case http.StatusNotFound, http.StatusMethodNotAllowed, http.StatusNotImplemented:
		return hasEndpointCapabilityMarker(message) || hasOpenAIInvalidEndpointURL(message)
	default:
		return false
	}
}

// hasOpenAIInvalidEndpointURL recognizes the endpoint-shaped error emitted by
// OpenAI-compatible gateways, while rejecting invalid URLs supplied in request
// parameters. Both interchangeable endpoints are POST-only.
func hasOpenAIInvalidEndpointURL(message string) bool {
	message = strings.ToLower(strings.TrimSpace(message))
	if !strings.HasPrefix(message, "invalid url") {
		return false
	}
	open := strings.IndexByte(message, '(')
	close := strings.LastIndexByte(message, ')')
	if open < 0 || close <= open+1 {
		return false
	}
	requestLine := strings.Fields(strings.TrimSpace(message[open+1 : close]))
	if len(requestLine) != 2 || !strings.EqualFold(requestLine[0], http.MethodPost) {
		return false
	}
	path := requestLine[1]
	if !strings.HasPrefix(path, "/") {
		return false
	}
	if delimiter := strings.IndexAny(path, "?#"); delimiter >= 0 {
		path = path[:delimiter]
	}
	path = strings.TrimSuffix(path, "/")
	return strings.HasSuffix(path, "/responses") || strings.HasSuffix(path, "/chat/completions")
}

// upstreamHTTPError preserves only bounded, private classification data from an
// upstream HTTP error. Its Error string is deliberately body-free so logs,
// relay logs, and public error paths cannot echo provider response content.
type upstreamHTTPError struct {
	statusCode           int
	classificationText   string
	detail               openAIErrorDetail
	hasOpenAIErrorDetail bool
}

func (e *upstreamHTTPError) Error() string {
	if e == nil || e.statusCode <= 0 {
		return "upstream error"
	}
	return fmt.Sprintf("upstream error: %d", e.statusCode)
}

func newUpstreamHTTPError(statusCode int, body []byte) error {
	text := strings.TrimSpace(string(body))
	detail, ok := parseOpenAIErrorDetail(text)
	if len(text) > 64*1024 {
		text = text[:64*1024]
	}
	return &upstreamHTTPError{
		statusCode:           statusCode,
		classificationText:   text,
		detail:               detail,
		hasOpenAIErrorDetail: ok,
	}
}

func upstreamClassificationMessage(err error) string {
	if err == nil {
		return ""
	}
	parts := []string{strings.ToLower(err.Error())}
	var httpErr *upstreamHTTPError
	if errors.As(err, &httpErr) && httpErr != nil {
		parts = append(parts, httpErr.classificationText)
	}
	var responseErr *transformerModel.ResponseError
	if errors.As(err, &responseErr) && responseErr != nil {
		parts = append(parts, responseErr.Detail.Message, responseErr.Detail.Code, responseErr.Detail.Type)
	}
	var wsErr *wsUpstreamEventError
	if errors.As(err, &wsErr) && wsErr != nil {
		parts = append(parts, wsErr.Message, wsErr.Code, wsErr.Type)
	}
	// Sanitized WS transport errors expose only fixed classification markers;
	// the provider-controlled close reason is intentionally unavailable here.
	var wsTransportErr *wsTransportError
	if errors.As(err, &wsTransportErr) && wsTransportErr != nil {
		parts = append(parts, wsTransportErr.markers...)
	}
	return strings.ToLower(strings.Join(parts, " "))
}

// upstreamErrorJSONBody extracts the JSON object embedded in an upstream error
// message ("upstream error: <status>: <body>"); ok reports whether a JSON
// object candidate was found. Legacy synthetic errors still use this path;
// typed HTTP errors are handled without formatting their body into Error().
func upstreamErrorJSONBody(err error) (string, bool) {
	if err == nil {
		return "", false
	}
	text := err.Error()
	start := strings.Index(text, "{")
	end := strings.LastIndex(text, "}")
	if start < 0 || end <= start {
		return "", false
	}
	return text[start : end+1], true
}

// openAIErrorPayload mirrors the standard OpenAI error envelope.
type openAIErrorPayload struct {
	Error openAIErrorDetail `json:"error"`
}

type openAIErrorDetail struct {
	Message string          `json:"message"`
	Type    string          `json:"type"`
	Code    json.RawMessage `json:"code"`
}

func openAIErrorDetailFromUpstream(err error) (openAIErrorDetail, bool) {
	var responseErr *transformerModel.ResponseError
	if errors.As(err, &responseErr) && responseErr != nil {
		var code json.RawMessage
		if strings.TrimSpace(responseErr.Detail.Code) != "" {
			if encoded, marshalErr := json.Marshal(responseErr.Detail.Code); marshalErr == nil {
				code = encoded
			}
		}
		detail := openAIErrorDetail{
			Message: responseErr.Detail.Message,
			Type:    responseErr.Detail.Type,
			Code:    code,
		}
		return detail, detail.Message != "" || detail.Type != "" || len(detail.Code) > 0
	}

	var httpErr *upstreamHTTPError
	if errors.As(err, &httpErr) && httpErr != nil {
		return httpErr.detail, httpErr.hasOpenAIErrorDetail
	}

	var wsErr *wsUpstreamEventError
	if errors.As(err, &wsErr) && wsErr != nil {
		var code json.RawMessage
		if strings.TrimSpace(wsErr.Code) != "" {
			encoded, marshalErr := json.Marshal(wsErr.Code)
			if marshalErr == nil {
				code = encoded
			}
		}
		detail := openAIErrorDetail{Message: wsErr.Message, Type: wsErr.Type, Code: code}
		return detail, detail.Type != "" || detail.Message != "" || len(detail.Code) > 0
	}

	body, ok := upstreamErrorJSONBody(err)
	if !ok {
		return openAIErrorDetail{}, false
	}
	return parseOpenAIErrorDetail(body)
}

func parseOpenAIErrorDetail(text string) (openAIErrorDetail, bool) {
	text = strings.TrimSpace(text)
	start := strings.Index(text, "{")
	end := strings.LastIndex(text, "}")
	if start >= 0 && end > start {
		text = text[start : end+1]
	}
	var payload openAIErrorPayload
	if jsonErr := json.Unmarshal([]byte(text), &payload); jsonErr != nil {
		return openAIErrorDetail{}, false
	}
	detail := payload.Error
	if detail.Type == "" && detail.Message == "" && len(detail.Code) == 0 {
		// Some gateways omit the "error" wrapper and return the detail directly.
		if jsonErr := json.Unmarshal([]byte(text), &detail); jsonErr != nil {
			return openAIErrorDetail{}, false
		}
	}
	return detail, detail.Type != "" || detail.Message != "" || len(detail.Code) > 0
}

func openAIErrorCodeIs(detail openAIErrorDetail, expected string) bool {
	if len(detail.Code) == 0 {
		return false
	}
	var code string
	if err := json.Unmarshal(detail.Code, &code); err != nil {
		return false
	}
	return strings.EqualFold(strings.TrimSpace(code), expected)
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

func (s *nativeResponsesAvailability) markEndpointUnsupported() {
	if s == nil {
		return
	}
	s.candidateFound = true
	s.endpointUnsupported = true
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
	if s.capable {
		return false
	}
	if s.endpointUnsupported {
		return true
	}
	return !s.operationalUnavailable && !s.candidateFound
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
