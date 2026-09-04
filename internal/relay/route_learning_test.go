package relay

import (
	"errors"
	"net/http"
	"testing"

	"github.com/bestruirui/octopus/internal/model"
	"github.com/bestruirui/octopus/internal/transformer/inbound"
)

func TestDetectRouteMismatchTargetRequiresRoutingEvidence(t *testing.T) {
	tests := []struct {
		name       string
		inbound    inbound.InboundType
		statusCode int
		err        error
		want       model.SiteModelRouteType
		ok         bool
	}{
		{name: "responses endpoint", inbound: inbound.InboundTypeOpenAIResponse, statusCode: http.StatusNotFound, err: errors.New("Invalid URL (POST /v1/responses)"), want: model.SiteModelRouteTypeOpenAIResponse, ok: true},
		{name: "anthropic endpoint", inbound: inbound.InboundTypeOpenAIChat, statusCode: http.StatusBadRequest, err: errors.New("use /v1/messages with anthropic-version"), want: model.SiteModelRouteTypeAnthropic, ok: true},
		{name: "stream content type", inbound: inbound.InboundTypeOpenAIChat, statusCode: http.StatusOK, err: errors.New("unexpected text/event-stream response"), want: model.SiteModelRouteTypeOpenAIResponse, ok: true},
		{name: "authentication path echo", inbound: inbound.InboundTypeOpenAIResponse, statusCode: http.StatusUnauthorized, err: errors.New("invalid API key for /v1/responses")},
		{name: "permission path echo", inbound: inbound.InboundTypeOpenAIChat, statusCode: http.StatusForbidden, err: errors.New("permission denied for /v1/messages")},
		{name: "quota path echo", inbound: inbound.InboundTypeOpenAIResponse, statusCode: http.StatusBadRequest, err: errors.New("quota exceeded for /v1/responses")},
		{name: "rate limit path echo", inbound: inbound.InboundTypeOpenAIResponse, statusCode: http.StatusTooManyRequests, err: errors.New("rate limit for /v1/responses")},
		{name: "provider outage path echo", inbound: inbound.InboundTypeOpenAIResponse, statusCode: http.StatusServiceUnavailable, err: errors.New("provider unavailable at /v1/responses")},
		{name: "transport error path echo", inbound: inbound.InboundTypeOpenAIResponse, statusCode: 0, err: errors.New(`failed to send request: Post "https://host/v1/responses": dial tcp: connection refused`)},
		{name: "model error path echo", inbound: inbound.InboundTypeOpenAIResponse, statusCode: http.StatusNotFound, err: errors.New("model not found on /v1/responses")},
		{name: "structured responses endpoint", inbound: inbound.InboundTypeOpenAIResponse, statusCode: http.StatusBadRequest, err: newUpstreamHTTPError(http.StatusBadRequest, []byte(`{"error":{"message":"Invalid URL (POST /v1/responses)","type":"invalid_request_error"}}`)), want: model.SiteModelRouteTypeOpenAIResponse, ok: true},
		{name: "structured anthropic instruction", inbound: inbound.InboundTypeOpenAIChat, statusCode: http.StatusBadRequest, err: newUpstreamHTTPError(http.StatusBadRequest, []byte(`{"error":{"message":"Use /v1/messages with anthropic-version","type":"invalid_request_error"}}`)), want: model.SiteModelRouteTypeAnthropic, ok: true},
		{name: "responses documentation mention", inbound: inbound.InboundTypeOpenAIResponse, statusCode: http.StatusBadRequest, err: newUpstreamHTTPError(http.StatusBadRequest, []byte(`{"error":{"message":"Invalid image_url; see /v1/responses documentation","type":"invalid_request_error"}}`))},
		{name: "messages documentation mention", inbound: inbound.InboundTypeOpenAIChat, statusCode: http.StatusBadRequest, err: newUpstreamHTTPError(http.StatusBadRequest, []byte(`{"error":{"message":"Invalid tool payload; see /v1/messages documentation","type":"invalid_request_error"}}`))},
		{name: "responses api documentation mention", inbound: inbound.InboundTypeOpenAIResponse, statusCode: http.StatusBadRequest, err: newUpstreamHTTPError(http.StatusBadRequest, []byte(`{"error":{"message":"Invalid image input; see the Responses API documentation","type":"invalid_request_error"}}`))},
		{name: "anthropic version documentation mention", inbound: inbound.InboundTypeOpenAIChat, statusCode: http.StatusBadRequest, err: newUpstreamHTTPError(http.StatusBadRequest, []byte(`{"error":{"message":"Invalid header; see anthropic-version documentation","type":"invalid_request_error"}}`))},
		{name: "stream mime value mention", inbound: inbound.InboundTypeOpenAIChat, statusCode: http.StatusOK, err: errors.New("invalid field value text/event-stream")},
		{name: "responses instruction", inbound: inbound.InboundTypeOpenAIChat, statusCode: http.StatusBadRequest, err: errors.New("use the Responses API for this request"), want: model.SiteModelRouteTypeOpenAIResponse, ok: true},
		{name: "missing anthropic version", inbound: inbound.InboundTypeOpenAIChat, statusCode: http.StatusBadRequest, err: errors.New("required anthropic-version header is missing"), want: model.SiteModelRouteTypeAnthropic, ok: true},
		{name: "plain responses endpoint not found", inbound: inbound.InboundTypeOpenAIResponse, statusCode: http.StatusNotFound, err: errors.New("endpoint not found: /v1/responses"), want: model.SiteModelRouteTypeOpenAIResponse, ok: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := detectRouteMismatchTarget(tt.inbound, tt.statusCode, tt.err)
			if ok != tt.ok || got != tt.want {
				t.Fatalf("expected route=%q ok=%t, got route=%q ok=%t", tt.want, tt.ok, got, ok)
			}
		})
	}
}
