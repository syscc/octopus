package grouphealth

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/bestruirui/octopus/internal/model"
	"github.com/bestruirui/octopus/internal/transformer/outbound"
)

func TestBuildProbeRequestForResponses(t *testing.T) {
	channel := &model.Channel{
		Type:     outbound.OutboundTypeOpenAIResponse,
		BaseUrls: []model.BaseUrl{{URL: "https://example.com/v1"}},
	}
	usedKey := &model.ChannelKey{ID: 1, ChannelKey: "sk-test"}

	req, err := buildProbeRequest(context.Background(), channel, usedKey, "gpt-5.4")
	if err != nil {
		t.Fatalf("buildProbeRequest returned error: %v", err)
	}
	if req.URL.Path != "/v1/responses" {
		t.Fatalf("expected /v1/responses, got %s", req.URL.Path)
	}
}

func TestBuildProbeRequestForEmbeddings(t *testing.T) {
	channel := &model.Channel{
		Type:     outbound.OutboundTypeOpenAIEmbedding,
		BaseUrls: []model.BaseUrl{{URL: "https://example.com/v1"}},
	}
	usedKey := &model.ChannelKey{ID: 1, ChannelKey: "sk-test"}

	req, err := buildProbeRequest(context.Background(), channel, usedKey, "text-embedding-3-large")
	if err != nil {
		t.Fatalf("buildProbeRequest returned error: %v", err)
	}
	if req.URL.Path != "/v1/embeddings" {
		t.Fatalf("expected /v1/embeddings, got %s", req.URL.Path)
	}
}

func TestRunCandidateRedactsUpstreamBodyAndKeepsCloudflareClassification(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.Header().Set("Server", "cloudflare")
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte("private provider challenge body"))
	}))
	defer server.Close()

	prober := &Prober{CandidateTimeout: time.Second}
	result := prober.RunCandidate(context.Background(), model.Channel{
		Type:     outbound.OutboundTypeOpenAIChat,
		BaseUrls: []model.BaseUrl{{URL: server.URL + "/v1"}},
	}, model.ChannelKey{ID: 1, ChannelKey: "test-key"}, "test-model")

	if result.Success {
		t.Fatal("expected forbidden probe to fail")
	}
	if result.ErrorMessage != "upstream error: 403" {
		t.Fatalf("expected body-free probe error, got %q", result.ErrorMessage)
	}
	if strings.Contains(result.ErrorMessage, "private provider challenge body") {
		t.Fatal("probe error must not contain upstream response body")
	}
	if !result.CloudflareBlocked {
		t.Fatal("expected private Cloudflare classification to be retained")
	}
}

func TestProbeCustomHeadersKeepConfiguredCookieAndSelectedCredential(t *testing.T) {
	request, err := http.NewRequest(http.MethodPost, "https://example.invalid", nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer old-key")
	applyCustomHeaders(request, []model.CustomHeader{
		{HeaderKey: "Authorization", HeaderValue: "Bearer custom-key"},
		{HeaderKey: "Cookie", HeaderValue: "session=channel-cookie"},
		{HeaderKey: "Set-Cookie", HeaderValue: "session=must-not-send"},
		{HeaderKey: "X-Probe-Metadata", HeaderValue: "kept"},
	})
	applySelectedProbeCredentialHeader(request.Header, outbound.OutboundTypeOpenAIChat, "selected-key")

	if got := request.Header.Get("Authorization"); got != "Bearer selected-key" {
		t.Fatalf("selected probe credential = %q, want selected key", got)
	}
	if got := request.Header.Get("Cookie"); got != "session=channel-cookie" {
		t.Fatalf("configured probe Cookie = %q, want channel cookie", got)
	}
	if got := request.Header.Get("Set-Cookie"); got != "" {
		t.Fatalf("Set-Cookie must not be sent upstream: %q", got)
	}
	if got := request.Header.Get("X-Probe-Metadata"); got != "kept" {
		t.Fatalf("safe probe metadata = %q, want kept", got)
	}
}
