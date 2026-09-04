package relay

import (
	"fmt"
	"net/http"
	"strings"
	"testing"

	dbmodel "github.com/bestruirui/octopus/internal/model"

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

func TestCloudflareWorkersAIResponsesSchemaFallback(t *testing.T) {
	channel := &dbmodel.Channel{
		Type: outbound.OutboundTypeOpenAIChat,
		BaseUrls: []dbmodel.BaseUrl{{
			URL: "https://api.cloudflare.com/client/v4/accounts/test-account/ai/v1",
		}},
	}
	req := &transformerModel.InternalLLMRequest{RawAPIFormat: transformerModel.APIFormatOpenAIResponse}
	if got, ok := outboundTypeForChannel(req, channel); !ok || got != outbound.OutboundTypeOpenAIChat {
		t.Fatalf("expected Cloudflare capability constraint to select Chat directly, got %v ok=%t", got, ok)
	}

	observed := fmt.Errorf(`upstream error: 400: {"error":{"code":"invalid_prompt","message":"AiError: Bad input: Error: oneOf at '/' not met, 0 matches: required properties at '/audio' are 'voice,format', required properties at '/' are 'prompt', required properties at '/' are 'messages'"}}`)
	if !shouldTryProtocolFallbackForAttempt(req, channel, outbound.OutboundTypeOpenAIResponse, http.StatusBadRequest, observed) {
		t.Fatal("expected the observed Cloudflare Chat-schema rejection to allow fallback")
	}

	budget := int64(128)
	nativeOnly := &transformerModel.InternalLLMRequest{
		RawAPIFormat:    transformerModel.APIFormatOpenAIResponse,
		ReasoningBudget: &budget,
	}
	if shouldTryProtocolFallbackForAttempt(nativeOnly, channel, outbound.OutboundTypeOpenAIResponse, http.StatusBadRequest, observed) {
		t.Fatal("expected native-only Responses fields to reject Chat fallback")
	}

	ordinaryInvalidPrompt := fmt.Errorf(`upstream error: 400: {"error":{"code":"invalid_prompt","message":"max_tokens must be greater than zero"}}`)
	if shouldTryProtocolFallbackForAttempt(req, channel, outbound.OutboundTypeOpenAIResponse, http.StatusBadRequest, ordinaryInvalidPrompt) {
		t.Fatal("expected an ordinary Cloudflare invalid_prompt error to remain visible")
	}

	misconfigured := &dbmodel.Channel{
		Type: outbound.OutboundTypeOpenAIResponse,
		BaseUrls: []dbmodel.BaseUrl{{
			URL: "https://api.cloudflare.com/client/v4/accounts/test-account/ai/v1",
		}},
	}
	if !shouldTryProtocolFallbackForAttempt(req, misconfigured, outbound.OutboundTypeOpenAIResponse, http.StatusBadRequest, observed) {
		t.Fatal("expected strict Workers AI URL detection to survive a misconfigured channel type")
	}

	lookalike := &dbmodel.Channel{
		Type:     outbound.OutboundTypeOpenAIChat,
		BaseUrls: []dbmodel.BaseUrl{{URL: "https://api.cloudflare.com.evil.example/client/v4/accounts/test-account/ai/v1"}},
	}
	if isCloudflareWorkersAIChannel(lookalike) {
		t.Fatal("expected Cloudflare lookalike host not to match Workers AI")
	}
	if shouldTryProtocolFallbackForAttempt(req, lookalike, outbound.OutboundTypeOpenAIResponse, http.StatusBadRequest, observed) {
		t.Fatal("expected a lookalike host not to activate Cloudflare-specific fallback")
	}
}

func TestResponsesModelNotSupportedCapabilityCode(t *testing.T) {
	req := &transformerModel.InternalLLMRequest{RawAPIFormat: transformerModel.APIFormatOpenAIResponse}
	unsupported := fmt.Errorf(`upstream error: 400: {"error":{"message":"current model does not support Responses API","type":"invalid_request_error","code":"RESPONSES_MODEL_NOT_SUPPORTED"}}`)
	for _, channelType := range []outbound.OutboundType{
		outbound.OutboundTypeOpenAIChat,
		outbound.OutboundTypeOpenAIResponse,
	} {
		channel := &dbmodel.Channel{Type: channelType}
		if !shouldTryProtocolFallbackForAttempt(req, channel, outbound.OutboundTypeOpenAIResponse, http.StatusBadRequest, unsupported) {
			t.Fatalf("expected explicit capability code to allow fallback for channel type %v", channelType)
		}
	}

	ordinaryModelError := fmt.Errorf(`upstream error: 400: {"error":{"message":"model not found","type":"invalid_request_error","code":"MODEL_NOT_FOUND"}}`)
	channel := &dbmodel.Channel{Type: outbound.OutboundTypeOpenAIChat}
	if shouldTryProtocolFallbackForAttempt(req, channel, outbound.OutboundTypeOpenAIResponse, http.StatusBadRequest, ordinaryModelError) {
		t.Fatal("expected an ordinary model error not to allow protocol fallback")
	}
	if shouldTryProtocolFallbackForAttempt(req, channel, outbound.OutboundTypeOpenAIResponse, http.StatusUnauthorized, unsupported) {
		t.Fatal("expected an authentication status not to allow protocol fallback")
	}
	if shouldTryProtocolFallbackForAttempt(req, channel, outbound.OutboundTypeOpenAIChat, http.StatusBadRequest, unsupported) {
		t.Fatal("expected a Responses-only capability code not to apply to a Chat attempt")
	}
	if shouldLearnProtocolUnsupportedForAttempt(req, channel, outbound.OutboundTypeOpenAIResponse, http.StatusBadRequest, unsupported) {
		t.Fatal("expected a model-scoped Responses error not to poison channel-wide capability")
	}
	endpointMissing := fmt.Errorf(`upstream error: 404: {"error":{"message":"Invalid URL (POST /v1/responses)","type":"invalid_request_error"}}`)
	if !shouldLearnProtocolUnsupportedForAttempt(req, channel, outbound.OutboundTypeOpenAIResponse, http.StatusNotFound, endpointMissing) {
		t.Fatal("expected an endpoint routing error to be persisted as unsupported")
	}
}

func TestOutboundTypeForChannelUsesPersistedAndManualCapabilities(t *testing.T) {
	responsesRequest := &transformerModel.InternalLLMRequest{RawAPIFormat: transformerModel.APIFormatOpenAIResponse}
	nativeResponsesRequest := &transformerModel.InternalLLMRequest{
		RawAPIFormat: transformerModel.APIFormatOpenAIResponse,
		ReasoningBudget: func() *int64 {
			value := int64(64)
			return &value
		}(),
	}
	chatRequest := &transformerModel.InternalLLMRequest{RawAPIFormat: transformerModel.APIFormatOpenAIChatCompletion}

	tests := []struct {
		name    string
		channel dbmodel.Channel
		req     *transformerModel.InternalLLMRequest
		want    outbound.OutboundType
		ok      bool
	}{
		{
			name:    "unknown probes downstream responses",
			channel: dbmodel.Channel{Type: outbound.OutboundTypeOpenAIChat},
			req:     responsesRequest,
			want:    outbound.OutboundTypeOpenAIResponse,
			ok:      true,
		},
		{
			name: "learned responses unsupported starts chat",
			channel: dbmodel.Channel{
				Type:                      outbound.OutboundTypeOpenAIResponse,
				OpenAIProtocolMode:        dbmodel.OpenAIProtocolModeAuto,
				OpenAIChatCapability:      dbmodel.OpenAIProtocolCapabilitySupported,
				OpenAIResponsesCapability: dbmodel.OpenAIProtocolCapabilityUnsupported,
			},
			req:  responsesRequest,
			want: outbound.OutboundTypeOpenAIChat,
			ok:   true,
		},
		{
			name: "native responses cannot use learned chat only",
			channel: dbmodel.Channel{
				Type:                      outbound.OutboundTypeOpenAIChat,
				OpenAIChatCapability:      dbmodel.OpenAIProtocolCapabilitySupported,
				OpenAIResponsesCapability: dbmodel.OpenAIProtocolCapabilityUnsupported,
			},
			req: nativeResponsesRequest,
			ok:  false,
		},
		{
			name: "manual responses only converts chat",
			channel: dbmodel.Channel{
				Type:               outbound.OutboundTypeOpenAIChat,
				OpenAIProtocolMode: dbmodel.OpenAIProtocolModeResponsesOnly,
			},
			req:  chatRequest,
			want: outbound.OutboundTypeOpenAIResponse,
			ok:   true,
		},
		{
			name: "manual chat only rejects native responses",
			channel: dbmodel.Channel{
				Type:               outbound.OutboundTypeOpenAIChat,
				OpenAIProtocolMode: dbmodel.OpenAIProtocolModeChatOnly,
			},
			req: nativeResponsesRequest,
			ok:  false,
		},
		{
			name: "both unsupported skips channel",
			channel: dbmodel.Channel{
				Type:                      outbound.OutboundTypeOpenAIChat,
				OpenAIChatCapability:      dbmodel.OpenAIProtocolCapabilityUnsupported,
				OpenAIResponsesCapability: dbmodel.OpenAIProtocolCapabilityUnsupported,
			},
			req: responsesRequest,
			ok:  false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := outboundTypeForChannel(tc.req, &tc.channel)
			if ok != tc.ok || ok && got != tc.want {
				t.Fatalf("outboundTypeForChannel() = (%v, %t), want (%v, %t)", got, ok, tc.want, tc.ok)
			}
		})
	}
}

func TestProtocolCandidateRankPrefersDirectCapability(t *testing.T) {
	req := &transformerModel.InternalLLMRequest{RawAPIFormat: transformerModel.APIFormatOpenAIResponse}
	direct := &dbmodel.Channel{
		Type:                      outbound.OutboundTypeOpenAIResponse,
		OpenAIResponsesCapability: dbmodel.OpenAIProtocolCapabilitySupported,
	}
	unknown := &dbmodel.Channel{Type: outbound.OutboundTypeOpenAIResponse}
	converted := &dbmodel.Channel{
		Type:                      outbound.OutboundTypeOpenAIChat,
		OpenAIChatCapability:      dbmodel.OpenAIProtocolCapabilitySupported,
		OpenAIResponsesCapability: dbmodel.OpenAIProtocolCapabilityUnsupported,
	}
	incompatible := &dbmodel.Channel{
		Type:                      outbound.OutboundTypeOpenAIChat,
		OpenAIChatCapability:      dbmodel.OpenAIProtocolCapabilityUnsupported,
		OpenAIResponsesCapability: dbmodel.OpenAIProtocolCapabilityUnsupported,
	}
	if !(protocolCandidateRank(req, direct) < protocolCandidateRank(req, unknown) &&
		protocolCandidateRank(req, unknown) < protocolCandidateRank(req, converted) &&
		protocolCandidateRank(req, converted) < protocolCandidateRank(req, incompatible)) {
		t.Fatalf("unexpected ranks: direct=%d unknown=%d converted=%d incompatible=%d",
			protocolCandidateRank(req, direct), protocolCandidateRank(req, unknown),
			protocolCandidateRank(req, converted), protocolCandidateRank(req, incompatible))
	}
}
func TestWSProtocolCapabilityErrorsRemainStructured(t *testing.T) {
	req := &transformerModel.InternalLLMRequest{RawAPIFormat: transformerModel.APIFormatOpenAIResponse}
	channel := &dbmodel.Channel{Type: outbound.OutboundTypeOpenAIResponse}

	endpointMissing := &wsUpstreamEventError{
		Status:  http.StatusBadGateway,
		Type:    "invalid_request_error",
		Message: "Invalid URL (POST /v1/responses)",
	}
	if !shouldTryProtocolFallbackForAttempt(req, channel, outbound.OutboundTypeOpenAIResponse, endpointMissing.Status, endpointMissing) {
		t.Fatal("expected a structured WS endpoint error to allow fallback")
	}
	if !shouldLearnProtocolUnsupportedForAttempt(req, channel, outbound.OutboundTypeOpenAIResponse, endpointMissing.Status, endpointMissing) {
		t.Fatal("expected a structured WS endpoint error to update channel capability")
	}

	modelUnsupported := &wsUpstreamEventError{
		Status:  http.StatusBadGateway,
		Code:    "RESPONSES_MODEL_NOT_SUPPORTED",
		Type:    "invalid_request_error",
		Message: "current model does not support Responses API",
	}
	if !shouldTryProtocolFallbackForAttempt(req, channel, outbound.OutboundTypeOpenAIResponse, modelUnsupported.Status, modelUnsupported) {
		t.Fatal("expected the model-scoped WS capability code to allow this request to fall back")
	}
	if shouldLearnProtocolUnsupportedForAttempt(req, channel, outbound.OutboundTypeOpenAIResponse, modelUnsupported.Status, modelUnsupported) {
		t.Fatal("expected the model-scoped WS capability code not to poison the channel")
	}

	authFailure := &wsUpstreamEventError{
		Status:  http.StatusUnauthorized,
		Type:    "authentication_error",
		Message: "invalid API key",
	}
	if shouldTryProtocolFallbackForAttempt(req, channel, outbound.OutboundTypeOpenAIResponse, authFailure.Status, authFailure) {
		t.Fatal("authentication failures must not trigger protocol fallback")
	}
}

func TestWSProtocolEndpointErrorsRespectHTTPStatusGate(t *testing.T) {
	req := &transformerModel.InternalLLMRequest{RawAPIFormat: transformerModel.APIFormatOpenAIResponse}
	channel := &dbmodel.Channel{Type: outbound.OutboundTypeOpenAIResponse}
	for _, tt := range []struct {
		name   string
		status int
		want   bool
	}{
		{name: "missing status allows endpoint probe", status: 0, want: true},
		{name: "bad request allows endpoint probe", status: http.StatusBadRequest, want: true},
		{name: "not found allows endpoint probe", status: http.StatusNotFound, want: true},
		{name: "gateway wrapper allows endpoint probe", status: http.StatusBadGateway, want: true},
		{name: "unauthorized is ordinary auth failure", status: http.StatusUnauthorized, want: false},
		{name: "forbidden is ordinary auth failure", status: http.StatusForbidden, want: false},
		{name: "rate limit is ordinary provider failure", status: http.StatusTooManyRequests, want: false},
		{name: "upstream failure is ordinary provider failure", status: http.StatusServiceUnavailable, want: false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			err := &wsUpstreamEventError{
				Status:  tt.status,
				Type:    "invalid_request_error",
				Message: "Invalid URL (POST /v1/responses)",
			}
			got := shouldTryProtocolFallbackForAttempt(req, channel, outbound.OutboundTypeOpenAIResponse, tt.status, err)
			if got != tt.want {
				t.Fatalf("status %d: expected fallback=%t, got %t", tt.status, tt.want, got)
			}
		})
	}
}

func TestWSProtocolEndpointErrorUsesStructuredStatusOverOuterStatus(t *testing.T) {
	req := &transformerModel.InternalLLMRequest{RawAPIFormat: transformerModel.APIFormatOpenAIResponse}
	channel := &dbmodel.Channel{Type: outbound.OutboundTypeOpenAIResponse}
	err := &wsUpstreamEventError{
		Status:  http.StatusUnauthorized,
		Type:    "invalid_request_error",
		Message: "Invalid URL (POST /v1/responses)",
	}
	if shouldTryProtocolFallbackForAttempt(req, channel, outbound.OutboundTypeOpenAIResponse, http.StatusNotFound, err) {
		t.Fatal("an explicit structured auth status must block fallback even when the outer status is 404")
	}
}

func TestProtocolFallbackAllowsBareStatusOnlyForAlternateProbe(t *testing.T) {
	req := &transformerModel.InternalLLMRequest{RawAPIFormat: transformerModel.APIFormatOpenAIResponse}
	chatChannel := &dbmodel.Channel{Type: outbound.OutboundTypeOpenAIChat}
	if !shouldTryProtocolFallbackForAttempt(req, chatChannel, outbound.OutboundTypeOpenAIResponse, http.StatusNotFound, fmt.Errorf("upstream error: 404")) {
		t.Fatal("expected bare 404 to allow fallback when probing the non-declared protocol")
	}
	responsesChannel := &dbmodel.Channel{Type: outbound.OutboundTypeOpenAIResponse}
	if shouldTryProtocolFallbackForAttempt(req, responsesChannel, outbound.OutboundTypeOpenAIResponse, http.StatusNotFound, fmt.Errorf("upstream error: 404")) {
		t.Fatal("expected bare 404 on the declared protocol to preserve the upstream error")
	}
}

func TestProtocolFallbackDoesNotLearnStructuredModelNotFound(t *testing.T) {
	req := &transformerModel.InternalLLMRequest{RawAPIFormat: transformerModel.APIFormatOpenAIResponse}
	channel := &dbmodel.Channel{Type: outbound.OutboundTypeOpenAIChat}
	errors := []error{
		fmt.Errorf(`upstream error: 404: {"error":{"message":"model not available","type":"not_found_error"}}`),
		&wsUpstreamEventError{Status: http.StatusNotFound, Type: "not_found_error", Message: "model not available"},
	}
	for _, upstreamErr := range errors {
		if !shouldTryProtocolFallbackForAttempt(req, channel, outbound.OutboundTypeOpenAIResponse, http.StatusNotFound, upstreamErr) {
			t.Fatalf("alternate protocol probe should remain eligible for this request: %v", upstreamErr)
		}
		if shouldLearnProtocolUnsupportedForAttempt(req, channel, outbound.OutboundTypeOpenAIResponse, http.StatusNotFound, upstreamErr) {
			t.Fatalf("model-scoped JSON 404 must not persist channel capability: %v", upstreamErr)
		}
	}
}

func TestProtocolFallbackRejectsAmbiguousStructuredNotFound(t *testing.T) {
	req := &transformerModel.InternalLLMRequest{RawAPIFormat: transformerModel.APIFormatOpenAIResponse}
	channel := &dbmodel.Channel{Type: outbound.OutboundTypeOpenAIChat}
	err := fmt.Errorf(`upstream error: 404: {"error":{"message":"resource missing","type":"not_found"}}`)
	if shouldTryProtocolFallbackForAttempt(req, channel, outbound.OutboundTypeOpenAIResponse, http.StatusNotFound, err) {
		t.Fatal("an unrelated structured resource-not-found error must not trigger a protocol probe")
	}
	if shouldLearnProtocolUnsupportedForAttempt(req, channel, outbound.OutboundTypeOpenAIResponse, http.StatusNotFound, err) {
		t.Fatal("an unrelated structured resource-not-found error must not be learned as channel capability")
	}
}

func TestAlternateOutboundTypeSupportsReplayPreferredProtocol(t *testing.T) {
	for _, format := range []transformerModel.APIFormat{
		transformerModel.APIFormatOpenAIChatCompletion,
		transformerModel.APIFormatOpenAIResponse,
	} {
		req := &transformerModel.InternalLLMRequest{RawAPIFormat: format}
		if got, ok := alternateOutboundType(req, outbound.OutboundTypeOpenAIChat); !ok || got != outbound.OutboundTypeOpenAIResponse {
			t.Fatalf("format=%q expected Chat to alternate to Responses, got type=%v ok=%t", format, got, ok)
		}
		if got, ok := alternateOutboundType(req, outbound.OutboundTypeOpenAIResponse); !ok || got != outbound.OutboundTypeOpenAIChat {
			t.Fatalf("format=%q expected Responses to alternate to Chat, got type=%v ok=%t", format, got, ok)
		}
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
		{http.StatusBadRequest, "method not allowed for this deployment"},
		{http.StatusBadRequest, "tool choice not implemented"},
	} {
		if shouldTryProtocolFallback(tc.status, fmt.Errorf("%s", tc.message)) {
			t.Fatalf("expected status=%d message=%q to preserve the original upstream error", tc.status, tc.message)
		}
	}
}

func TestProtocolProbeDoesNotFallbackOnOperationalErrors(t *testing.T) {
	req := &transformerModel.InternalLLMRequest{RawAPIFormat: transformerModel.APIFormatOpenAIResponse}
	channel := &dbmodel.Channel{Type: outbound.OutboundTypeOpenAIChat}
	for _, tc := range []struct {
		status  int
		message string
	}{
		{http.StatusUnauthorized, "endpoint not found"},
		{http.StatusForbidden, "request forbidden"},
		{http.StatusTooManyRequests, "rate limit for POST /v1/responses"},
		{http.StatusInternalServerError, "POST /v1/responses failed"},
	} {
		if shouldTryProtocolFallbackForAttempt(req, channel, outbound.OutboundTypeOpenAIResponse, tc.status, fmt.Errorf("upstream error: %s", tc.message)) {
			t.Fatalf("expected probe status=%d message=%q not to trigger fallback", tc.status, tc.message)
		}
	}
}

func TestProtocolFallbackClassificationIgnoresChannelName(t *testing.T) {
	upstreamErr := fmt.Errorf("upstream error: 404: endpoint not found")
	result := attemptResult{
		Err:         fmt.Errorf("channel quota-waf-conversation failed: %v", upstreamErr),
		UpstreamErr: upstreamErr,
		StatusCode:  http.StatusNotFound,
	}
	if shouldTryProtocolFallback(result.StatusCode, result.Err) {
		t.Fatal("test setup expected wrapped channel metadata to pollute the text classifier")
	}
	if !shouldTryProtocolFallback(result.StatusCode, result.protocolError()) {
		t.Fatal("expected protocol classification to use the unwrapped upstream error")
	}
}

func TestProtocolFallbackAcceptsStructuredOpenAIEndpointErrors(t *testing.T) {
	tests := []struct {
		name   string
		status int
		body   string
	}{
		{
			name:   "wrapped invalid responses URL",
			status: http.StatusNotFound,
			body:   `{"error":{"message":"Invalid URL (POST /v1/responses)","type":"invalid_request_error","param":null,"code":null}}`,
		},
		{
			name:   "direct invalid chat URL",
			status: http.StatusBadRequest,
			body:   `{"message":"Invalid URL (POST /v1/chat/completions)","type":"invalid_request_error"}`,
		},
		{
			name:   "route missing",
			status: http.StatusMethodNotAllowed,
			body:   `{"error":{"message":"Route not found for this deployment","type":"invalid_request_error"}}`,
		},
		{
			name:   "endpoint not implemented",
			status: http.StatusNotImplemented,
			body:   `{"error":{"message":"Endpoint not found","type":"invalid_request_error"}}`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := fmt.Errorf("upstream error: %d: %s", tc.status, tc.body)
			if !isOpenAIEndpointRoutingError(tc.status, err) {
				t.Fatalf("expected structured endpoint error to be classified: %v", err)
			}
			if !shouldTryProtocolFallback(tc.status, err) {
				t.Fatalf("expected structured endpoint error to allow fallback: %v", err)
			}
		})
	}
}

func TestProtocolFallbackRejectsStructuredOpenAINonEndpointErrors(t *testing.T) {
	tests := []struct {
		name   string
		status int
		body   string
	}{
		{
			name:   "invalid request URL parameter",
			status: http.StatusBadRequest,
			body:   `{"error":{"message":"Invalid URL supplied in image_url","type":"invalid_request_error"}}`,
		},
		{
			name:   "model missing",
			status: http.StatusNotFound,
			body:   `{"error":{"message":"The requested model does not exist","type":"invalid_request_error"}}`,
		},
		{
			name:   "previous response missing",
			status: http.StatusNotFound,
			body:   `{"error":{"message":"previous_response_id not found","type":"invalid_request_error"}}`,
		},
		{
			name:   "wrong error type",
			status: http.StatusNotFound,
			body:   `{"error":{"message":"Invalid URL (POST /v1/responses)","type":"authentication_error"}}`,
		},
		{
			name:   "wrong status",
			status: http.StatusUnauthorized,
			body:   `{"error":{"message":"Invalid URL (POST /v1/responses)","type":"invalid_request_error"}}`,
		},
		{
			name:   "non API endpoint path",
			status: http.StatusNotFound,
			body:   `{"error":{"message":"Invalid URL (POST /v1/files)","type":"invalid_request_error"}}`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := fmt.Errorf("upstream error: %d: %s", tc.status, tc.body)
			if isOpenAIEndpointRoutingError(tc.status, err) {
				t.Fatalf("expected non-endpoint error to be preserved: %v", err)
			}
			if shouldTryProtocolFallback(tc.status, err) {
				t.Fatalf("expected non-endpoint error not to allow fallback: %v", err)
			}
		})
	}
}

func TestProtocolFallbackRejectsStructured400ParameterCapabilityMessage(t *testing.T) {
	err := fmt.Errorf(`upstream error: 400: {"error":{"message":"Parameter 'tool_choice' is not implemented for this model","type":"invalid_request_error"}}`)
	if isOpenAIEndpointRoutingError(http.StatusBadRequest, err) || shouldTryProtocolFallback(http.StatusBadRequest, err) {
		t.Fatal("expected parameter-level 400 error not to trigger protocol fallback")
	}
}

func TestNativeResponsesAvailabilityPrioritizesCapabilityEvidence(t *testing.T) {
	state := nativeResponsesAvailability{
		candidateFound:         true,
		attempted:              true,
		endpointUnsupported:    true,
		operationalUnavailable: true,
	}
	if !state.shouldReportCapabilityError() {
		t.Fatal("expected an attempted unsupported endpoint to remain a capability error")
	}
}

func TestRouteLearningUsesOnlyDeclaredProtocol(t *testing.T) {
	channel := &dbmodel.Channel{Type: outbound.OutboundTypeOpenAIChat}
	if usedDeclaredChannelProtocol(channel, attemptResult{OutboundType: outbound.OutboundTypeOpenAIResponse}) {
		t.Fatal("expected a Responses probe on a Chat channel not to be route-learning evidence")
	}
	if !usedDeclaredChannelProtocol(channel, attemptResult{OutboundType: outbound.OutboundTypeOpenAIChat}) {
		t.Fatal("expected a failure on the declared Chat protocol to remain learnable")
	}
}

func TestUpstreamHTTPErrorHidesBodyButPreservesEndpointClassification(t *testing.T) {
	body := []byte(`{"error":{"type":"invalid_request_error","code":"private-provider-code","message":"Invalid URL (POST /v1/responses)"}}`)
	err := newUpstreamHTTPError(http.StatusNotFound, body)
	if strings.Contains(err.Error(), string(body)) || strings.Contains(err.Error(), "private-provider-code") {
		t.Fatalf("upstream body leaked through error string: %q", err.Error())
	}
	if !isOpenAIEndpointRoutingError(http.StatusNotFound, err) {
		t.Fatal("expected typed upstream error to preserve endpoint classification")
	}
	if !shouldTryProtocolFallback(http.StatusNotFound, err) {
		t.Fatal("expected typed upstream error to allow protocol fallback")
	}
	detail, ok := openAIErrorDetailFromUpstream(err)
	if !ok || detail.Code == nil || string(detail.Code) != `"private-provider-code"` {
		t.Fatalf("expected private detail to remain available only for classification, got %+v", detail)
	}
}
