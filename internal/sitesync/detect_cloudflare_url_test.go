package sitesync

import (
	"context"
	"testing"

	"github.com/bestruirui/octopus/internal/model"
)

func TestDetectPlatformAcceptsDocumentedCloudflareBaseURLs(t *testing.T) {
	documented := []string{
		"https://api.cloudflare.com/client/v4/accounts/account-id/ai",
		"https://api.cloudflare.com/client/v4/accounts/account-id/ai/",
		"https://api.cloudflare.com/client/v4/accounts/account-id/ai/v1",
		"https://API.CloudFlare.com/client/v4/accounts/account-id/ai/v1",
	}

	for _, rawURL := range documented {
		t.Run(rawURL, func(t *testing.T) {
			platform, routeType, err := DetectPlatform(context.Background(), rawURL)
			if err != nil {
				t.Fatalf("DetectPlatform(%q) returned error: %v", rawURL, err)
			}
			if platform != model.SitePlatformCloudflare {
				t.Fatalf("expected Cloudflare platform, got %q", platform)
			}
			if routeType != model.SiteModelRouteTypeOpenAIChat {
				t.Fatalf("expected OpenAI Chat route, got %q", routeType)
			}
		})
	}
}

func TestDetectPlatformRejectsUndocumentedCloudflarePaths(t *testing.T) {
	// api.cloudflare.com only serves Workers AI in this project, so anything
	// outside the documented base URLs must fail fast without remote probing.
	rejected := []string{
		"ftp://api.cloudflare.com/client/v4/accounts/account-id/ai",
		"https://api.cloudflare.com",
		"https://api.cloudflare.com/client/v4",
		"https://api.cloudflare.com/client/v4/accounts/account-id",
		"https://api.cloudflare.com/client/v4/accounts//ai",
		"https://api.cloudflare.com/proxy/client/v4/accounts/account-id/ai",
		"https://api.cloudflare.com/client/v4/accounts/account-id/ai/v2",
		"https://api.cloudflare.com/client/v4/accounts/account-id/ai/v1/chat/completions",
		"https://api.cloudflare.com/client/v4/accounts/account-id/ai/models/search",
		"https://api.cloudflare.com/client/v4/accounts/account-id/ai/v1?foo=bar",
		"https://api.cloudflare.com/client/v4/accounts/account-id/ai/v1#frag",
		"https://user:pass@api.cloudflare.com/client/v4/accounts/account-id/ai/v1",
	}

	for _, rawURL := range rejected {
		t.Run(rawURL, func(t *testing.T) {
			platform, routeType, err := DetectPlatform(context.Background(), rawURL)
			if err == nil {
				t.Fatalf("expected DetectPlatform(%q) to fail, got platform=%q route=%q", rawURL, platform, routeType)
			}
			if platform != "" || routeType != "" {
				t.Fatalf("expected empty detection result, got platform=%q route=%q", platform, routeType)
			}
		})
	}
}
