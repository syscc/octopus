package op

import (
	"strings"
	"testing"

	"github.com/bestruirui/octopus/internal/model"
)

func TestCloudflareImportPreservesAccountScopedBaseURL(t *testing.T) {
	const rawURL = "https://api.cloudflare.com/client/v4/accounts/account-id/ai/v1/?ignored=true#fragment"
	const expectedURL = "https://api.cloudflare.com/client/v4/accounts/account-id/ai/v1"

	if actual := normalizeImportBaseURL(rawURL); actual != expectedURL {
		t.Fatalf("expected Cloudflare account path %q, got %q", expectedURL, actual)
	}
	deepURL := expectedURL + "/chat/completions"
	if actual := normalizeImportBaseURL(deepURL); actual != expectedURL {
		t.Fatalf("expected deep Cloudflare endpoint to normalize to %q, got %q", expectedURL, actual)
	}
	const managementURL = "https://api.cloudflare.com/client/v4/accounts/account-id/ai"
	if actual := normalizeImportBaseURL(managementURL + "/models/search"); actual != managementURL {
		t.Fatalf("expected deep management endpoint to normalize to %q, got %q", managementURL, actual)
	}

	platform, unsupported := detectSupportedPlatform("workers-ai", rawURL)
	if unsupported {
		t.Fatal("expected Cloudflare Workers AI import to be supported")
	}
	if platform != model.SitePlatformCloudflare {
		t.Fatalf("expected Cloudflare platform, got %q", platform)
	}
	if !isDirectImportPlatform(platform) {
		t.Fatal("expected Cloudflare to use direct API token credentials")
	}
	if platformSupportsCheckin(platform) {
		t.Fatal("expected Cloudflare to skip check-in")
	}
}

func TestCloudflareImportRejectsUndocumentedURLs(t *testing.T) {
	// Only the documented base URLs count as Cloudflare Workers AI URLs; anything
	// else falls back to origin-only normalization and stays undetected.
	rejected := []string{
		"https://api.cloudflare.com/client/v4/accounts/account-id",
		"https://api.cloudflare.com/client/v4/accounts//ai/v1",
		"https://api.cloudflare.com/proxy/client/v4/accounts/account-id/ai/v1",
		"https://user:pass@api.cloudflare.com/client/v4/accounts/account-id/ai/v1",
		"https://api.cloudflare.com.evil.example/client/v4/accounts/account-id/ai/v1",
	}

	for _, rawURL := range rejected {
		t.Run(rawURL, func(t *testing.T) {
			if hasCloudflareWorkersAIURL(rawURL) {
				t.Fatalf("expected %q not to be treated as a Cloudflare Workers AI url", rawURL)
			}
			normalized := normalizeImportBaseURL(rawURL)
			if strings.Contains(normalized, "/client/v4/") {
				t.Fatalf("expected %q to lose its path, got %q", rawURL, normalized)
			}
			platform, unsupported := detectSupportedPlatform("", rawURL)
			if unsupported || platform == model.SitePlatformCloudflare {
				t.Fatalf("expected %q to stay undetected, got platform=%q unsupported=%v", rawURL, platform, unsupported)
			}
		})
	}
}

func TestCloudflarePlatformTextWithInvalidURLIsRejectedBeforePersistence(t *testing.T) {
	const bareAccountURL = "https://api.cloudflare.com/client/v4/accounts/account-id"

	platform, unsupported := detectSupportedPlatform("workers-ai", bareAccountURL)
	if !unsupported || platform != "" {
		t.Fatalf("expected invalid explicit workers-ai input to be rejected, got platform=%q unsupported=%v", platform, unsupported)
	}
	if resolved, ok := resolveImportedPlatform("workers-ai", normalizeImportBaseURL(bareAccountURL)); ok || resolved != "" {
		t.Fatalf("expected invalid workers-ai import to be skipped, got platform=%q ok=%v", resolved, ok)
	}
}

func TestDetectSupportedPlatformRejectsLookalikeCloudflareHost(t *testing.T) {
	platform, unsupported := detectSupportedPlatform("", "https://api.cloudflare.com.evil.example/client/v4/accounts/account-id/ai/v1")
	if unsupported || platform != "" {
		t.Fatalf("expected lookalike Cloudflare host to remain undetected, got platform=%q unsupported=%v", platform, unsupported)
	}
}

func TestDetectSupportedPlatformDoesNotRejectIncidentalCloudflareText(t *testing.T) {
	tests := []string{
		"new-api (cloudflare tunnel)",
		"cloudflare-proxy.example.com",
	}
	for _, rawPlatform := range tests {
		platform, unsupported := detectSupportedPlatform(rawPlatform, "https://ordinary.example.com")
		if unsupported || platform != "" {
			t.Fatalf("expected incidental text %q to stay undetected, got platform=%q unsupported=%v", rawPlatform, platform, unsupported)
		}
	}
}

func TestCloudflareImportURLInvalidClassification(t *testing.T) {
	// Records that clearly target Cloudflare Workers AI (exact api.cloudflare.com
	// host without any platform hint, or an explicit cloudflare/workers-ai hint)
	// must be skipped when the strict HTTPS/host/path/base normalization fails.
	invalidWithoutHint := []string{
		"http://api.cloudflare.com/client/v4/accounts/account-id/ai",
		"https://api.cloudflare.com/client/v4/accounts/account-id",
		"https://api.cloudflare.com/client/v4/accounts/account-id/ai/v2",
		"https://user:pass@api.cloudflare.com/client/v4/accounts/account-id/ai",
		"https://api.cloudflare.com:8443/client/v4/accounts/account-id/ai",
		"api.cloudflare.com/client/v4/accounts/account-id/ai",
	}
	for _, rawURL := range invalidWithoutHint {
		t.Run(rawURL, func(t *testing.T) {
			if !cloudflareImportURLInvalid(rawURL, "") {
				t.Fatalf("expected %q without a platform hint to be rejected as an invalid Cloudflare import", rawURL)
			}
		})
	}

	invalidWithHint := []string{
		"https://gateway.ai.cloudflare.com/v1/example/gateway",
		"https://proxy.example.com",
	}
	for _, rawURL := range invalidWithHint {
		t.Run(rawURL, func(t *testing.T) {
			if !cloudflareImportURLInvalid(rawURL, "workers-ai") {
				t.Fatalf("expected %q with an explicit workers-ai hint to be rejected", rawURL)
			}
		})
	}

	notCloudflare := []string{
		"",
		"https://gateway.ai.cloudflare.com/v1/example/gateway",
		"https://api.cloudflare.com.evil.example/client/v4/accounts/account-id/ai",
		"https://ordinary.example.com",
	}
	for _, rawURL := range notCloudflare {
		t.Run(rawURL, func(t *testing.T) {
			if cloudflareImportURLInvalid(rawURL, "") {
				t.Fatalf("expected %q without a hint to keep ordinary import behavior", rawURL)
			}
			if cloudflareImportURLInvalid(rawURL, "new-api") {
				t.Fatalf("expected %q with an unrelated hint to keep ordinary import behavior", rawURL)
			}
		})
	}

	valid := []string{
		"https://api.cloudflare.com/client/v4/accounts/account-id/ai",
		"https://api.cloudflare.com/client/v4/accounts/account-id/ai/v1",
		"https://api.cloudflare.com/client/v4/accounts/account-id/ai/v1/chat/completions?x=1#f",
		"https://api.cloudflare.com/client/v4/accounts/account-id/ai/models/search",
	}
	for _, rawURL := range valid {
		t.Run(rawURL, func(t *testing.T) {
			if cloudflareImportURLInvalid(rawURL, "") {
				t.Fatalf("expected %q to remain importable as Cloudflare", rawURL)
			}
			if cloudflareImportURLInvalid(rawURL, "workers-ai") {
				t.Fatalf("expected %q with a hint to remain importable as Cloudflare", rawURL)
			}
		})
	}
}

func TestParseAllAPIHubAccountRowSkipsInvalidCloudflareWithoutHint(t *testing.T) {
	row := rawImportObject{
		"id":        "http-cloudflare-account",
		"site_name": "Workers AI Without Hint",
		"site_url":  "http://api.cloudflare.com/client/v4/accounts/account-id/ai",
		"authType":  "api_key",
		"account_info": map[string]any{
			"username":     "cf-user",
			"access_token": "cf-api-token",
		},
	}

	input, warning, ok := parseAllAPIHubAccountRow(row)
	if ok || input.Site.Platform == model.SitePlatformCloudflare {
		t.Fatalf("expected invalid Cloudflare row without a hint to be skipped, got input=%+v ok=%v", input, ok)
	}
	if !strings.Contains(warning, "Cloudflare Workers AI 站点地址无效") {
		t.Fatalf("expected Cloudflare-specific skip warning, got %q", warning)
	}
}

func TestParseAllAPIHubAccountRowImportsValidCloudflareWithoutHint(t *testing.T) {
	row := rawImportObject{
		"id":        "valid-cloudflare-account",
		"site_name": "Workers AI Without Hint",
		"site_url":  "https://api.cloudflare.com/client/v4/accounts/account-id/ai/v1/chat/completions",
		"authType":  "api_key",
		"account_info": map[string]any{
			"username":     "cf-user",
			"access_token": "cf-api-token",
		},
	}

	input, _, ok := parseAllAPIHubAccountRow(row)
	if !ok {
		t.Fatal("expected valid Cloudflare row without a hint to be importable")
	}
	if input.Site.Platform != model.SitePlatformCloudflare {
		t.Fatalf("expected Cloudflare platform, got %q", input.Site.Platform)
	}
	const expectedURL = "https://api.cloudflare.com/client/v4/accounts/account-id/ai/v1"
	if input.Site.BaseURL != expectedURL {
		t.Fatalf("expected canonical base url %q, got %q", expectedURL, input.Site.BaseURL)
	}
	if input.CredentialType != model.SiteCredentialTypeAPIKey || input.APIKey != "cf-api-token" {
		t.Fatalf("expected direct API key credentials, got %+v", input)
	}
	if input.AutoCheckin {
		t.Fatal("expected Cloudflare import to disable check-in")
	}
}

func TestParseAllAPIHubProfileSkipsInvalidCloudflareWithoutHint(t *testing.T) {
	profile := rawImportObject{
		"id":      "cf-profile",
		"name":    "Workers AI Profile",
		"baseUrl": "https://api.cloudflare.com/client/v4/accounts/account-id",
		"apiKey":  "cf-api-token",
	}

	input, warning, ok := parseAllAPIHubProfile(profile)
	if ok || input.Site.Platform == model.SitePlatformCloudflare {
		t.Fatalf("expected invalid Cloudflare profile without a hint to be skipped, got input=%+v ok=%v", input, ok)
	}
	if !strings.Contains(warning, "Cloudflare Workers AI 站点地址无效") {
		t.Fatalf("expected Cloudflare-specific skip warning, got %q", warning)
	}
}

func TestPrepareMetAPIImportedModelsForCloudflareForcesOpenAIChat(t *testing.T) {
	items := prepareMetAPIImportedModels(42, model.SitePlatformCloudflare, []model.SiteModel{
		{
			GroupKey:        model.SiteDefaultGroupKey,
			ModelName:       "claude-3-5-sonnet",
			RouteType:       model.SiteModelRouteTypeAnthropic,
			RouteSource:     model.SiteModelRouteSourceManualOverride,
			ManualOverride:  true,
			RouteRawPayload: "legacy-route-metadata",
		},
		{
			GroupKey:  model.SiteDefaultGroupKey,
			ModelName: "gemini-2.0-flash",
			RouteType: model.SiteModelRouteTypeGemini,
		},
	})
	if len(items) != 2 {
		t.Fatalf("expected 2 imported models, got %+v", items)
	}
	for _, item := range items {
		if item.SiteAccountID != 42 {
			t.Fatalf("expected account id 42, got %d", item.SiteAccountID)
		}
		if item.RouteType != model.SiteModelRouteTypeOpenAIChat {
			t.Fatalf("expected Cloudflare route openai_chat for %q, got %q", item.ModelName, item.RouteType)
		}
		if item.RouteSource != model.SiteModelRouteSourceSyncInferred || item.ManualOverride || item.RouteRawPayload != "" {
			t.Fatalf("expected imported Cloudflare route state to be canonical, got %+v", item)
		}
	}

	nonCloudflare := prepareMetAPIImportedModels(42, model.SitePlatformAPI, []model.SiteModel{{
		GroupKey:  model.SiteDefaultGroupKey,
		ModelName: "claude-3-5-sonnet",
		RouteType: model.SiteModelRouteTypeAnthropic,
	}})
	if len(nonCloudflare) != 1 || nonCloudflare[0].RouteType != model.SiteModelRouteTypeAnthropic {
		t.Fatalf("expected non-Cloudflare route behavior to remain unchanged, got %+v", nonCloudflare)
	}
}
