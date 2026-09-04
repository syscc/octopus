package op

import (
	"encoding/json"
	"strings"
	"testing"

	dbpkg "github.com/bestruirui/octopus/internal/db"
	"github.com/bestruirui/octopus/internal/model"
)

func TestAllAPIHubImportSkipsInvalidCloudflareRecordWithoutRollback(t *testing.T) {
	ctx := setupSiteOpTestDB(t)
	payload := map[string]any{
		"accounts": map[string]any{
			"accounts": []any{
				map[string]any{
					"id":        "valid-account",
					"site_name": "Valid New API",
					"site_url":  "https://valid-import.example.com",
					"site_type": "new-api",
					"authType":  "access_token",
					"account_info": map[string]any{
						"username":     "valid-user",
						"access_token": "valid-session-token",
					},
				},
				map[string]any{
					"id":        "invalid-cloudflare-account",
					"site_name": "Invalid Workers AI Gateway",
					"site_url":  "https://gateway.ai.cloudflare.com/v1/example/gateway",
					"site_type": "workers-ai",
					"authType":  "api_key",
					"account_info": map[string]any{
						"username":     "invalid-user",
						"access_token": "invalid-token",
					},
				},
			},
		},
	}
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal import payload failed: %v", err)
	}

	result, accountIDs, err := SiteImportAllAPIHub(ctx, body)
	if err != nil {
		t.Fatalf("SiteImportAllAPIHub failed: %v", err)
	}
	if result.CreatedSites != 1 || result.CreatedAccounts != 1 || result.SkippedAccounts != 1 {
		t.Fatalf("unexpected import result: %+v", result)
	}
	if len(accountIDs) != 1 {
		t.Fatalf("expected only the valid account to be scheduled, got %v", accountIDs)
	}
	if !containsImportWarning(result.Warnings, "Cloudflare Workers AI 站点地址无效") {
		t.Fatalf("expected invalid Cloudflare warning, got %v", result.Warnings)
	}
	assertImportedSiteCounts(t, 1, 1)
}

func TestMetAPIImportSkipsInvalidCloudflareRecordWithoutRollback(t *testing.T) {
	ctx := setupSiteOpTestDB(t)
	payload := map[string]any{
		"accounts": map[string]any{
			"sites": []any{
				map[string]any{"id": 1, "name": "Valid New API", "url": "https://valid-metapi.example.com", "platform": "new-api"},
				map[string]any{"id": 2, "name": "Invalid Workers AI Gateway", "url": "https://gateway.ai.cloudflare.com/v1/example/gateway", "platform": "workers-ai"},
			},
			"accounts": []any{
				map[string]any{"id": 101, "siteId": 1, "username": "valid-user", "accessToken": "valid-session-token", "apiToken": "valid-api-token", "status": "active", "checkinEnabled": false},
				map[string]any{"id": 202, "siteId": 2, "username": "invalid-user", "apiToken": "invalid-token", "status": "active", "checkinEnabled": false},
			},
			"accountTokens": []any{},
			"tokenRoutes":   []any{},
			"routeChannels": []any{},
		},
	}
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal import payload failed: %v", err)
	}

	result, err := SiteImportMetAPI(ctx, body)
	if err != nil {
		t.Fatalf("SiteImportMetAPI failed: %v", err)
	}
	if result.CreatedSites != 1 || result.CreatedAccounts != 1 || result.SkippedAccounts != 1 {
		t.Fatalf("unexpected import result: %+v", result)
	}
	if !containsImportWarning(result.Warnings, "Cloudflare Workers AI 站点地址无效") {
		t.Fatalf("expected invalid Cloudflare warning, got %v", result.Warnings)
	}
	assertImportedSiteCounts(t, 1, 1)
}

func TestAllAPIHubImportSkipsHTTPCloudflareRecordWithoutPlatformHint(t *testing.T) {
	ctx := setupSiteOpTestDB(t)
	payload := map[string]any{
		"accounts": map[string]any{
			"accounts": []any{
				map[string]any{
					"id":        "valid-account",
					"site_name": "Valid New API",
					"site_url":  "https://valid-import.example.com",
					"site_type": "new-api",
					"authType":  "access_token",
					"account_info": map[string]any{
						"username":     "valid-user",
						"access_token": "valid-session-token",
					},
				},
				// No platform hint at all: the exact api.cloudflare.com host with a
				// non-https scheme must be skipped instead of degrading into a
				// generic NewAPI site that would persist the Cloudflare token.
				map[string]any{
					"id":        "http-cloudflare-account",
					"site_name": "Workers AI Without Hint",
					"site_url":  "http://api.cloudflare.com/client/v4/accounts/account-id/ai",
					"authType":  "api_key",
					"account_info": map[string]any{
						"username":     "cf-user",
						"access_token": "cf-api-token",
					},
				},
			},
		},
	}
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal import payload failed: %v", err)
	}

	result, accountIDs, err := SiteImportAllAPIHub(ctx, body)
	if err != nil {
		t.Fatalf("SiteImportAllAPIHub failed: %v", err)
	}
	if result.CreatedSites != 1 || result.CreatedAccounts != 1 || result.SkippedAccounts != 1 {
		t.Fatalf("unexpected import result: %+v", result)
	}
	if len(accountIDs) != 1 {
		t.Fatalf("expected only the valid account to be scheduled, got %v", accountIDs)
	}
	if !containsImportWarning(result.Warnings, "Cloudflare Workers AI 站点地址无效") {
		t.Fatalf("expected invalid Cloudflare warning, got %v", result.Warnings)
	}
	assertImportedSiteCounts(t, 1, 1)
	var degraded int64
	if err := dbpkg.GetDB().Model(&model.Site{}).Where("base_url LIKE ?", "%api.cloudflare.com%").Count(&degraded).Error; err != nil {
		t.Fatalf("count cloudflare sites failed: %v", err)
	}
	if degraded != 0 {
		t.Fatalf("expected no degraded api.cloudflare.com site, got %d", degraded)
	}
}

func TestMetAPIImportSkipsHTTPCloudflareSiteWithoutPlatformHint(t *testing.T) {
	ctx := setupSiteOpTestDB(t)
	payload := map[string]any{
		"accounts": map[string]any{
			"sites": []any{
				map[string]any{"id": 1, "name": "Valid New API", "url": "https://valid-metapi.example.com", "platform": "new-api"},
				// No platform hint: the exact host with an unknown /ai/v2 path
				// must be skipped instead of degrading into a generic site.
				map[string]any{"id": 2, "name": "Workers AI v2", "url": "https://api.cloudflare.com/client/v4/accounts/account-id/ai/v2"},
			},
			"accounts": []any{
				map[string]any{"id": 101, "siteId": 1, "username": "valid-user", "accessToken": "valid-session-token", "apiToken": "valid-api-token", "status": "active", "checkinEnabled": false},
				map[string]any{"id": 202, "siteId": 2, "username": "cf-user", "apiToken": "cf-api-token", "status": "active", "checkinEnabled": false},
			},
			"accountTokens": []any{},
			"tokenRoutes":   []any{},
			"routeChannels": []any{},
		},
	}
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal import payload failed: %v", err)
	}

	result, err := SiteImportMetAPI(ctx, body)
	if err != nil {
		t.Fatalf("SiteImportMetAPI failed: %v", err)
	}
	if result.CreatedSites != 1 || result.CreatedAccounts != 1 || result.SkippedAccounts != 1 {
		t.Fatalf("unexpected import result: %+v", result)
	}
	if !containsImportWarning(result.Warnings, "Cloudflare Workers AI 站点地址无效") {
		t.Fatalf("expected invalid Cloudflare warning, got %v", result.Warnings)
	}
	assertImportedSiteCounts(t, 1, 1)
	var degraded int64
	if err := dbpkg.GetDB().Model(&model.Site{}).Where("base_url LIKE ?", "%api.cloudflare.com%").Count(&degraded).Error; err != nil {
		t.Fatalf("count cloudflare sites failed: %v", err)
	}
	if degraded != 0 {
		t.Fatalf("expected no degraded api.cloudflare.com site, got %d", degraded)
	}
}

func containsImportWarning(warnings []string, marker string) bool {
	for _, warning := range warnings {
		if strings.Contains(warning, marker) {
			return true
		}
	}
	return false
}

func assertImportedSiteCounts(t *testing.T, expectedSites int64, expectedAccounts int64) {
	t.Helper()
	var siteCount int64
	if err := dbpkg.GetDB().Model(&model.Site{}).Count(&siteCount).Error; err != nil {
		t.Fatalf("count imported sites failed: %v", err)
	}
	var accountCount int64
	if err := dbpkg.GetDB().Model(&model.SiteAccount{}).Count(&accountCount).Error; err != nil {
		t.Fatalf("count imported accounts failed: %v", err)
	}
	if siteCount != expectedSites || accountCount != expectedAccounts {
		t.Fatalf("unexpected persisted counts: sites=%d accounts=%d", siteCount, accountCount)
	}
}
