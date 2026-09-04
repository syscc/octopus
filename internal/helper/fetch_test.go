package helper

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/bestruirui/octopus/internal/model"
	"github.com/bestruirui/octopus/internal/transformer/outbound"
)

func TestFetchModelsUsesBrowserHeadersAndSummarizesHTMLError(t *testing.T) {
	observedUserAgent := ""
	observedAccept := ""
	observedAcceptLanguage := ""
	observedCookie := ""
	observedAuthorization := ""
	observedSetCookie := ""

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		observedUserAgent = r.Header.Get("User-Agent")
		observedAccept = r.Header.Get("Accept")
		observedAcceptLanguage = r.Header.Get("Accept-Language")
		observedCookie = r.Header.Get("Cookie")
		observedAuthorization = r.Header.Get("Authorization")
		observedSetCookie = r.Header.Get("Set-Cookie")
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`<!DOCTYPE html><html lang="en-US"><head><title>Just a moment...</title></head><body>blocked</body></html>`))
	}))
	defer server.Close()

	_, err := FetchModels(context.Background(), model.Channel{
		Type:     outbound.OutboundTypeOpenAIChat,
		BaseUrls: []model.BaseUrl{{URL: server.URL, Delay: 0}},
		Keys:     []model.ChannelKey{{Enabled: true, ChannelKey: "managed-key"}},
		CustomHeader: []model.CustomHeader{
			{HeaderKey: "Authorization", HeaderValue: "Bearer custom-key"},
			{HeaderKey: "Cookie", HeaderValue: "session=channel-cookie"},
			{HeaderKey: "Set-Cookie", HeaderValue: "session=must-not-send"},
		},
	})
	if err == nil {
		t.Fatalf("expected FetchModels to fail")
	}
	if !strings.Contains(err.Error(), "http 403: Just a moment...") {
		t.Fatalf("expected summarized HTML error, got %v", err)
	}
	if !strings.Contains(observedUserAgent, "Mozilla/5.0") {
		t.Fatalf("expected browser user-agent, got %q", observedUserAgent)
	}
	if observedAccept == "" {
		t.Fatalf("expected Accept header to be set")
	}
	if observedAcceptLanguage == "" {
		t.Fatalf("expected Accept-Language header to be set")
	}
	if observedCookie != "session=channel-cookie" {
		t.Fatalf("configured channel Cookie = %q, want channel cookie", observedCookie)
	}
	if observedAuthorization != "Bearer managed-key" {
		t.Fatalf("selected model-fetch credential = %q, want managed key", observedAuthorization)
	}
	if observedSetCookie != "" {
		t.Fatalf("Set-Cookie must not be sent upstream: %q", observedSetCookie)
	}
}

func TestIsProtectedModelRequestHeaderBlocksBoundaryHeaders(t *testing.T) {
	blocked := []string{
		"Authorization",
		"X-Api-Key",
		"X-Forwarded-Host ",
		"Set-Cookie",
		"Connection",
		"Transfer-Encoding",
		"X-Forwarded-For",
		"Sec-WebSocket-Key",
		"Host",
		" Content-Length ",
		"",
		"   ",
	}
	for _, name := range blocked {
		if !isProtectedModelRequestHeader(name) {
			t.Errorf("header %q should be blocked", name)
		}
	}

	allowed := []string{"Cookie", "Accept", "Accept-Language", "User-Agent", "OpenAI-Organization"}
	for _, name := range allowed {
		if isProtectedModelRequestHeader(name) {
			t.Errorf("header %q should remain allowed", name)
		}
	}
}
