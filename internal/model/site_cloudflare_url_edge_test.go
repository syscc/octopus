package model

import (
	"net/url"
	"strings"
	"testing"
)

// validateCloudflareBaseURL mirrors how Site.Validate reaches the Cloudflare
// check so the edge cases below exercise the production contract.
func validateCloudflareBaseURL(t *testing.T, rawURL string) error {
	t.Helper()
	site := &Site{Name: "cloudflare", Platform: SitePlatformCloudflare, BaseURL: rawURL}
	return site.Validate()
}

func TestValidateCloudflareWorkersAIBaseURLAcceptsOnlyDocumentedBases(t *testing.T) {
	accepted := []string{
		"https://api.cloudflare.com/client/v4/accounts/account-id/ai",
		"https://api.cloudflare.com/client/v4/accounts/account-id/ai/",
		"https://api.cloudflare.com/client/v4/accounts/account-id/ai/v1",
		"https://api.cloudflare.com/client/v4/accounts/account-id/ai/v1/",
		"https://API.CloudFlare.com/client/v4/accounts/account-id/ai/v1",
		"https://api.cloudflare.com/client/v4/accounts/023e105f4ecef8ad9ca31a8372d0c353/ai/v1",
		// Explicit scheme-default ports are equivalent, not a bypass.
		"https://api.cloudflare.com:443/client/v4/accounts/account-id/ai",
	}

	for _, rawURL := range accepted {
		t.Run(rawURL, func(t *testing.T) {
			if err := validateCloudflareBaseURL(t, rawURL); err != nil {
				t.Fatalf("Validate(%q) returned error: %v", rawURL, err)
			}
		})
	}
}

func TestValidateCloudflareWorkersAIBaseURLRejectsEdgeCases(t *testing.T) {
	rejected := map[string][]string{
		"extra path prefix": {
			"https://api.cloudflare.com/proxy/client/v4/accounts/account-id/ai",
			"https://api.cloudflare.com/v1/client/v4/accounts/account-id/ai/v1",
			"https://api.cloudflare.com/client/client/v4/accounts/account-id/ai",
		},
		"missing ai segment": {
			"https://api.cloudflare.com",
			"https://api.cloudflare.com/",
			"https://api.cloudflare.com/client/v4",
			"https://api.cloudflare.com/client/v4/accounts",
			"https://api.cloudflare.com/client/v4/accounts/account-id",
			"https://api.cloudflare.com/client/v4/accounts/account-id/",
			"https://api.cloudflare.com/client/v4/accounts/account-id/v1",
			"https://api.cloudflare.com/client/v4/accounts/account-id/aiv1",
			"https://api.cloudflare.com/client/v4/accounts/account-id/ai-gateway",
		},
		"empty or unusable account id": {
			"https://api.cloudflare.com/client/v4/accounts//ai",
			"https://api.cloudflare.com/client/v4/accounts//ai/v1",
			"https://api.cloudflare.com/client/v4/accounts/%20/ai",
			"https://api.cloudflare.com/client/v4/accounts/./ai",
			"https://api.cloudflare.com/client/v4/accounts/../ai",
			"https://api.cloudflare.com/client/v4/accounts/../ai/v1",
			"https://api.cloudflare.com/client//v4/accounts/account-id/ai",
			"https://api.cloudflare.com/client/v4//accounts/account-id/ai",
		},
		"unknown api version": {
			"https://api.cloudflare.com/client/v4/accounts/account-id/ai/v0",
			"https://api.cloudflare.com/client/v4/accounts/account-id/ai/v2",
			"https://api.cloudflare.com/client/v4/accounts/account-id/ai/v3",
			"https://api.cloudflare.com/client/v4/accounts/account-id/ai/v10",
			"https://api.cloudflare.com/client/v4/accounts/account-id/ai/V2",
			"https://api.cloudflare.com/client/v4/accounts/account-id/ai//v1",
		},
		"deep endpoint is not a base": {
			"https://api.cloudflare.com/client/v4/accounts/account-id/ai/v1/chat/completions",
			"https://api.cloudflare.com/client/v4/accounts/account-id/ai/v1/embeddings",
			"https://api.cloudflare.com/client/v4/accounts/account-id/ai/models/search",
			"https://api.cloudflare.com/client/v4/accounts/account-id/ai/run/@cf/meta/llama-3-8b-instruct",
		},
		"userinfo": {
			"https://user:pass@api.cloudflare.com/client/v4/accounts/account-id/ai",
			"https://user@api.cloudflare.com/client/v4/accounts/account-id/ai/v1",
			"https://api.cloudflare.com@evil.example/client/v4/accounts/account-id/ai/v1",
		},
		"lookalike host": {
			"https://api.cloudflare.com.evil.example/client/v4/accounts/account-id/ai",
			"https://evil-api.cloudflare.com/client/v4/accounts/account-id/ai/v1",
			"https://apixcloudflare.com/client/v4/accounts/account-id/ai",
			"https://api.cloudflare.com./client/v4/accounts/account-id/ai",
			"https://cloudflare.com/client/v4/accounts/account-id/ai",
			"https://proxy.example.com/client/v4/accounts/account-id/ai/v1",
		},
		"port bypass": {
			"https://api.cloudflare.com:8443/client/v4/accounts/account-id/ai",
			"https://api.cloudflare.com:8080/client/v4/accounts/account-id/ai/v1",
			"https://api.cloudflare.com:80/client/v4/accounts/account-id/ai",
			"http://api.cloudflare.com:8080/client/v4/accounts/account-id/ai/v1",
			"http://api.cloudflare.com:443/client/v4/accounts/account-id/ai",
		},
		"insecure scheme": {
			"http://api.cloudflare.com/client/v4/accounts/account-id/ai",
			"http://api.cloudflare.com/client/v4/accounts/account-id/ai/v1",
			"http://api.cloudflare.com:80/client/v4/accounts/account-id/ai/v1",
			"http://api.cloudflare.com:443/client/v4/accounts/account-id/ai",
		},
		"encoded path bypass": {
			"https://api.cloudflare.com/client/v4/accounts/account-id/ai%2Fv1",
			"https://api.cloudflare.com/client/v4/accounts/account-id%2Fai",
			"https://api.cloudflare.com/%63lient/v4/accounts/account-id/ai",
			"https://api.cloudflare.com/client%2Fv4/accounts/account-id/ai/v1",
			"https://api.cloudflare.com/client/v4/accounts/account%2Did/ai",
			"https://api.cloudflare.com/client/v4/accounts/%2e%2e/ai",
		},
		"query or fragment": {
			"https://api.cloudflare.com/client/v4/accounts/account-id/ai?foo=bar",
			"https://api.cloudflare.com/client/v4/accounts/account-id/ai/v1?",
			"https://api.cloudflare.com/client/v4/accounts/account-id/ai/v1#frag",
		},
	}

	for group, cases := range rejected {
		for _, rawURL := range cases {
			t.Run(group+"/"+rawURL, func(t *testing.T) {
				if err := validateCloudflareBaseURL(t, rawURL); err == nil {
					t.Fatalf("expected Validate(%q) to fail", rawURL)
				}
			})
		}
	}
}

func TestNormalizeCloudflareWorkersAIBaseURLCollapsesDocumentedEndpoints(t *testing.T) {
	const managementBase = "https://api.cloudflare.com/client/v4/accounts/account-id/ai"
	const openAIBase = managementBase + "/v1"

	cases := []struct {
		input    string
		expected string
	}{
		{input: managementBase, expected: managementBase},
		{input: managementBase + "/", expected: managementBase},
		{input: openAIBase, expected: openAIBase},
		{input: openAIBase + "/", expected: openAIBase},
		{input: openAIBase + "/chat/completions", expected: openAIBase},
		{input: openAIBase + "/chat/completions/", expected: openAIBase},
		{input: openAIBase + "/chat/completions?stream=true#done", expected: openAIBase},
		{input: openAIBase + "/embeddings", expected: openAIBase},
		{input: openAIBase + "/responses", expected: openAIBase},
		{input: managementBase + "/models/search", expected: managementBase},
		{input: managementBase + "/models/search?per_page=100", expected: managementBase},
		{input: managementBase + "/run/@cf/meta/llama-3-8b-instruct", expected: managementBase},
		{input: "  " + managementBase + "/models/search  ", expected: managementBase},
		// Scheme-default ports are dropped from the canonical base.
		{input: "https://api.cloudflare.com:443/client/v4/accounts/account-id/ai/v1/chat/completions", expected: openAIBase},
	}

	for _, tt := range cases {
		t.Run(tt.input, func(t *testing.T) {
			actual, ok := NormalizeCloudflareWorkersAIBaseURL(tt.input)
			if !ok {
				t.Fatalf("NormalizeCloudflareWorkersAIBaseURL(%q) ok = false, want true", tt.input)
			}
			if actual != tt.expected {
				t.Fatalf("NormalizeCloudflareWorkersAIBaseURL(%q) = %q, want %q", tt.input, actual, tt.expected)
			}
		})
	}
}

func TestNormalizeCloudflareWorkersAIBaseURLRejectsEdgeCases(t *testing.T) {
	rejected := []string{
		"",
		"   ",
		"api.cloudflare.com/client/v4/accounts/account-id/ai",
		"ftp://api.cloudflare.com/client/v4/accounts/account-id/ai",
		"//api.cloudflare.com/client/v4/accounts/account-id/ai",
		"https://api.cloudflare.com",
		"https://api.cloudflare.com/client/v4",
		"https://api.cloudflare.com/client/v4/accounts/account-id",
		"https://api.cloudflare.com/client/v4/accounts//ai",
		"https://api.cloudflare.com/client/v4/accounts/%20/ai",
		"https://api.cloudflare.com/client/v4/accounts/../ai",
		"https://api.cloudflare.com/client/v4/accounts/./ai/v1",
		"https://api.cloudflare.com/proxy/client/v4/accounts/account-id/ai",
		"https://api.cloudflare.com/client/v4/accounts/account-id/ai-gateway",
		// Undocumented API versions must not collapse onto the management base.
		"https://api.cloudflare.com/client/v4/accounts/account-id/ai/v2",
		"https://api.cloudflare.com/client/v4/accounts/account-id/ai/V2",
		"https://api.cloudflare.com/client/v4/accounts/account-id/ai/v2/chat/completions",
		"https://api.cloudflare.com/client/v4/accounts/account-id/ai/v3/models/search",
		"https://api.cloudflare.com/client/v4/accounts/account-id/ai/v10",
		"https://api.cloudflare.com/client/v4/accounts/account-id/ai/v0/chat/completions",
		"https://api.cloudflare.com/client/v4/accounts/account-id/ai//v1/chat/completions",
		"https://user@api.cloudflare.com/client/v4/accounts/account-id/ai",
		"https://user:pass@api.cloudflare.com/client/v4/accounts/account-id/ai/v1/chat/completions",
		"https://api.cloudflare.com@evil.example/client/v4/accounts/account-id/ai/v1",
		"https://api.cloudflare.com.evil.example/client/v4/accounts/account-id/ai/v1",
		"https://evil-api.cloudflare.com/client/v4/accounts/account-id/ai",
		"https://api.cloudflare.com./client/v4/accounts/account-id/ai",
		"https://api.cloudflare.com:8443/client/v4/accounts/account-id/ai/v1",
		"http://api.cloudflare.com:8080/client/v4/accounts/account-id/ai",
		"https://api.cloudflare.com:80/client/v4/accounts/account-id/ai",
		// Bearer credentials must never travel over plaintext http.
		"http://api.cloudflare.com/client/v4/accounts/account-id/ai",
		"http://api.cloudflare.com/client/v4/accounts/account-id/ai/v1/chat/completions",
		"https://api.cloudflare.com/client/v4/accounts/account-id/ai%2Fv1",
		"https://api.cloudflare.com/%63lient/v4/accounts/account-id/ai",
		"https://api.cloudflare.com/client/v4/accounts/%2e%2e/ai/v1",
	}

	for _, rawURL := range rejected {
		t.Run(rawURL, func(t *testing.T) {
			normalized, ok := NormalizeCloudflareWorkersAIBaseURL(rawURL)
			if ok {
				t.Fatalf("expected NormalizeCloudflareWorkersAIBaseURL(%q) to fail, got %q", rawURL, normalized)
			}
			if normalized != "" {
				t.Fatalf("expected empty result for %q, got %q", rawURL, normalized)
			}
		})
	}
}

// TestNormalizeCloudflareWorkersAIBaseURLProducesValidatableBase ties the import
// path to the production contract: whatever Normalize accepts must be storable
// on a Cloudflare site, and normalizing again must be a no-op.
func TestNormalizeCloudflareWorkersAIBaseURLProducesValidatableBase(t *testing.T) {
	inputs := []string{
		"https://api.cloudflare.com/client/v4/accounts/account-id/ai",
		"https://api.cloudflare.com/client/v4/accounts/account-id/ai/v1/chat/completions?x=1#y",
		"https://api.cloudflare.com/client/v4/accounts/account-id/ai/models/search",
		"https://api.cloudflare.com:443/client/v4/accounts/account-id/ai/v1/",
		"https://api.cloudflare.com/client/v4/accounts/account-id/ai/run/@cf/meta/llama-3-8b-instruct",
	}

	for _, rawURL := range inputs {
		t.Run(rawURL, func(t *testing.T) {
			normalized, ok := NormalizeCloudflareWorkersAIBaseURL(rawURL)
			if !ok {
				t.Fatalf("NormalizeCloudflareWorkersAIBaseURL(%q) ok = false, want true", rawURL)
			}
			if err := validateCloudflareBaseURL(t, normalized); err != nil {
				t.Fatalf("normalized base %q failed Site.Validate: %v", normalized, err)
			}
			again, ok := NormalizeCloudflareWorkersAIBaseURL(normalized)
			if !ok || again != normalized {
				t.Fatalf("normalize is not idempotent for %q: got %q (ok=%v)", normalized, again, ok)
			}
			if strings.Contains(normalized, "?") || strings.Contains(normalized, "#") {
				t.Fatalf("normalized base %q must drop query and fragment", normalized)
			}
			parsed, err := url.Parse(normalized)
			if err != nil {
				t.Fatalf("normalized base %q is not parseable: %v", normalized, err)
			}
			if parsed.Port() != "" {
				t.Fatalf("normalized base %q must not carry an explicit port", normalized)
			}
		})
	}
}

func TestValidateCloudflareWorkersAIBaseURLRejectsUnsupportedSchemeDirectly(t *testing.T) {
	parsed, err := url.Parse("ftp://api.cloudflare.com/client/v4/accounts/account-id/ai")
	if err != nil {
		t.Fatalf("url.Parse returned error: %v", err)
	}
	if err := ValidateCloudflareWorkersAIBaseURL(parsed); err == nil || !strings.Contains(err.Error(), "https") {
		t.Fatalf("expected direct Cloudflare validator to reject ftp scheme, got %v", err)
	}
}
