package relay

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"

	model "github.com/bestruirui/octopus/internal/transformer/model"
)

func TestObserveWSPassthroughEventParsesErrorEnvelope(t *testing.T) {
	stats := &wsPassthroughStats{}
	observeWSPassthroughEvent(stats, []byte(`{"type":"error","status":429,"error":{"code":"rate_limit_exceeded","type":"requests","message":"Too many requests"}}`))
	if stats.Error == nil {
		t.Fatalf("expected top-level error to be parsed")
	}
	if stats.Error.Status != http.StatusTooManyRequests || stats.Error.Code != "rate_limit_exceeded" || stats.Error.Type != "requests" || stats.Error.Message != "Too many requests" {
		t.Fatalf("unexpected parsed error: %#v", stats.Error)
	}
	publicErr, ok := classifyWSPublicError(stats.Error, stats.Error.Status)
	if !ok || publicErr.Status != http.StatusTooManyRequests || publicErr.Code != "upstream_rate_limited" {
		t.Fatalf("expected rate limit public error, got %#v ok=%t", publicErr, ok)
	}
}

func TestObserveWSPassthroughEventParsesResponseErrorEnvelope(t *testing.T) {
	stats := &wsPassthroughStats{}
	observeWSPassthroughEvent(stats, []byte(`{"type":"response.failed","response":{"id":"resp_failed","model":"gpt-4o","status":"failed","error":{"code":"context_length_exceeded","type":"invalid_request_error","message":"maximum context length exceeded"}}}`))
	if stats.ResponseID != "resp_failed" {
		t.Fatalf("expected response id to be captured, got %q", stats.ResponseID)
	}
	if stats.Error == nil || stats.Error.Code != "context_length_exceeded" || stats.Error.Status != http.StatusBadGateway {
		t.Fatalf("unexpected response error: %#v", stats.Error)
	}
	publicErr, ok := classifyWSPublicError(stats.Error, 0)
	if !ok || publicErr.Status != http.StatusBadRequest || publicErr.Code != "context_length_exceeded" {
		t.Fatalf("expected context limit public error, got %#v ok=%t", publicErr, ok)
	}
}

func TestNormalizeWSUpstreamErrorCode(t *testing.T) {
	var payload struct {
		Code any `json:"code"`
	}
	if err := json.Unmarshal([]byte(`{"code":429}`), &payload); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if got := normalizeWSUpstreamErrorCode(payload.Code); got != "429" {
		t.Fatalf("expected numeric code to normalize to 429, got %q", got)
	}
}

func TestNormalizeWSUpstreamErrorCodeRejectsStructuredValues(t *testing.T) {
	if got := normalizeWSUpstreamErrorCode(map[string]any{"secret": "value"}); got != "" {
		t.Fatalf("object code must be rejected, got %q", got)
	}
	if got := normalizeWSUpstreamErrorCode([]any{"secret"}); got != "" {
		t.Fatalf("array code must be rejected, got %q", got)
	}
}

func TestWrappedWSUpstreamErrorPreservesClientStatus(t *testing.T) {
	upstreamErr := &wsUpstreamEventError{
		Status:  http.StatusForbidden,
		Code:    "model_not_allowed",
		Type:    "permission_error",
		Message: "model access forbidden",
	}
	wrapped := fmt.Errorf("channel failed: %w", upstreamErr)

	var recovered *wsUpstreamEventError
	if !errors.As(wrapped, &recovered) || recovered != upstreamErr {
		t.Fatalf("wrapped error lost its structured cause: %v", wrapped)
	}
	if got := wsUpstreamErrorStatus(wrapped); got != http.StatusForbidden {
		t.Fatalf("structured 403 was rewritten to %d", got)
	}
	publicErr, ok := classifyWSPublicError(wrapped, http.StatusBadGateway)
	if !ok || publicErr.Status != http.StatusForbidden {
		t.Fatalf("expected public 403 classification, got %#v ok=%t", publicErr, ok)
	}
}

func TestBuildWSPassthroughFailureTerminalSanitizesProviderFields(t *testing.T) {
	ra := &relayAttempt{relayRequest: &relayRequest{requestModel: "model-test", internalRequest: &model.InternalLLMRequest{Model: "model-test"}}}
	stats := &wsPassthroughStats{
		ResponseID: "response-test",
		Model:      "model-test",
		Error: &wsUpstreamEventError{
			Code:    "provider-secret-code",
			Type:    "provider-secret-type",
			Message: "provider-secret-message",
		},
	}
	body, err := ra.buildWSPassthroughFailureTerminal(stats)
	if err != nil {
		t.Fatalf("build synthetic terminal: %v", err)
	}
	text := string(body)
	for _, forbidden := range []string{"provider-secret-code", "provider-secret-type", "provider-secret-message"} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("synthetic terminal leaked provider field %q: %s", forbidden, text)
		}
	}
	if !strings.Contains(text, `"code":"upstream_error"`) || !strings.Contains(text, "The upstream request failed.") {
		t.Fatalf("synthetic terminal did not use fixed public wording: %s", text)
	}
}
