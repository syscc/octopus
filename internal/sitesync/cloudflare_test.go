package sitesync

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/bestruirui/octopus/internal/db"

	"github.com/bestruirui/octopus/internal/model"
	"github.com/bestruirui/octopus/internal/op"
	transformerModel "github.com/bestruirui/octopus/internal/transformer/model"
	"github.com/bestruirui/octopus/internal/transformer/outbound"
)

func TestCloudflareWorkersAIBaseURL(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{name: "account path", input: "https://api.cloudflare.com/client/v4/accounts/account-id", expected: "https://api.cloudflare.com/client/v4/accounts/account-id/ai"},
		{name: "management base", input: "https://api.cloudflare.com/client/v4/accounts/account-id/ai", expected: "https://api.cloudflare.com/client/v4/accounts/account-id/ai"},
		{name: "openai base", input: "https://api.cloudflare.com/client/v4/accounts/account-id/ai/v1/", expected: "https://api.cloudflare.com/client/v4/accounts/account-id/ai"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if actual := cloudflareWorkersAIBaseURL(tt.input); actual != tt.expected {
				t.Fatalf("expected %q, got %q", tt.expected, actual)
			}
		})
	}
}

func TestDetectPlatformRecognizesCloudflareWorkersAI(t *testing.T) {
	platform, routeType, err := DetectPlatform(context.Background(), "https://api.cloudflare.com/client/v4/accounts/account-id/ai/v1")
	if err != nil {
		t.Fatalf("DetectPlatform returned error: %v", err)
	}
	if platform != model.SitePlatformCloudflare {
		t.Fatalf("expected Cloudflare platform, got %q", platform)
	}
	if routeType != model.SiteModelRouteTypeOpenAIChat {
		t.Fatalf("expected OpenAI Chat route, got %q", routeType)
	}
}

func TestDetectPlatformRejectsLookalikeCloudflareHost(t *testing.T) {
	platform, _, err := DetectPlatform(context.Background(), "https://api.cloudflare.com.evil.example/client/v4/accounts/account-id/ai/v1")
	if err == nil || platform != "" {
		t.Fatalf("expected lookalike host detection to fail, got platform=%q err=%v", platform, err)
	}
}

func TestCloudflareSyncProjectsOpenAIChatRoute(t *testing.T) {
	const (
		apiToken  = "cf-api-token"
		modelName = "@cf/meta/llama-3.1-8b-instruct"
	)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/client/v4/accounts/account-id/ai/models/search" {
			t.Errorf("unexpected Cloudflare catalog path: %s", r.URL.Path)
		}
		if r.Header.Get("Authorization") != "Bearer "+apiToken {
			t.Errorf("unexpected Authorization header: %q", r.Header.Get("Authorization"))
		}
		if r.URL.Query().Get("task") != "Text Generation" {
			t.Errorf("expected Text Generation task filter, got %q", r.URL.Query().Get("task"))
		}
		if r.URL.Query().Get("per_page") != "50" {
			t.Errorf("unexpected pagination query: %s", r.URL.RawQuery)
		}
		result := []any{}
		if r.URL.Query().Get("page") == "1" {
			result = append(result, map[string]any{
				"name": modelName,
				"task": map[string]any{"name": "Text Generation"},
			})
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"success":  true,
			"errors":   []any{},
			"messages": []any{},
			"result":   result,
		})
	}))
	defer server.Close()

	ctx := setupProjectTestDB(t)
	site := &model.Site{
		Name:             "Cloudflare Workers AI",
		Platform:         model.SitePlatformCloudflare,
		BaseURL:          server.URL + "/client/v4/accounts/account-id/ai/v1",
		DefaultRouteType: model.SiteModelRouteTypeOpenAIChat,
		Enabled:          true,
	}
	// Persist directly so the integration test can point at httptest while
	// production validation remains locked to api.cloudflare.com.
	if err := db.GetDB().WithContext(ctx).Create(site).Error; err != nil {
		t.Fatalf("create test site failed: %v", err)
	}
	account := &model.SiteAccount{
		SiteID:         site.ID,
		Name:           "Cloudflare account",
		CredentialType: model.SiteCredentialTypeAccessToken,
		AccessToken:    "Bearer " + apiToken,
		Enabled:        true,
		AutoSync:       false,
		AutoCheckin:    false,
	}
	if err := op.SiteAccountCreate(account, ctx); err != nil {
		t.Fatalf("SiteAccountCreate failed: %v", err)
	}

	result, err := SyncAccount(ctx, account.ID)
	if err != nil {
		t.Fatalf("SyncAccount failed: %v", err)
	}
	if result.Status != model.SiteExecutionStatusSuccess || result.ModelCount != 1 || result.ChannelCount != 1 {
		t.Fatalf("unexpected sync result: %+v", result)
	}
	if len(result.Models) != 1 || result.Models[0] != modelName {
		t.Fatalf("unexpected synced models: %+v", result.Models)
	}

	channels := loadProjectedChannelsByGroupKey(t, ctx, account.ID)
	channel, ok := channels[model.SiteDefaultGroupKey]
	if !ok {
		t.Fatalf("expected default projected channel, got %+v", channels)
	}
	if channel.Type != outbound.OutboundTypeOpenAIChat {
		t.Fatalf("expected OpenAI Chat channel, got %q", channel.Type)
	}
	if len(channel.BaseUrls) != 1 || channel.BaseUrls[0].URL != site.BaseURL {
		t.Fatalf("unexpected projected base URL: %+v", channel.BaseUrls)
	}
	if len(channel.Keys) != 1 || channel.Keys[0].ChannelKey != apiToken {
		t.Fatalf("expected verbatim Cloudflare API token, got %+v", channel.Keys)
	}

	adapter := outbound.Get(channel.Type)
	request, err := adapter.TransformRequest(
		ctx,
		&transformerModel.InternalLLMRequest{Model: modelName},
		channel.BaseUrls[0].URL,
		channel.Keys[0].ChannelKey,
	)
	if err != nil {
		t.Fatalf("building OpenAI request failed: %v", err)
	}
	expectedURL := site.BaseURL + "/chat/completions"
	if request.URL.String() != expectedURL {
		t.Fatalf("expected OpenAI route %q, got %q", expectedURL, request.URL.String())
	}
	if request.Header.Get("Authorization") != "Bearer "+apiToken {
		t.Fatalf("unexpected relay Authorization header: %q", request.Header.Get("Authorization"))
	}
	var body map[string]any
	if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
		t.Fatalf("decode relay request failed: %v", err)
	}
	if body["model"] != modelName {
		t.Fatalf("expected model %q in relay body, got %#v", modelName, body["model"])
	}
}

func TestFetchCloudflareModelsPaginatesAndDeduplicates(t *testing.T) {
	const apiToken = "cf-api-token"
	calls := 0

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		page := r.URL.Query().Get("page")
		result := make([]map[string]any, 0, cloudflareModelCatalogPageSize)
		switch page {
		case "1":
			for i := 0; i < cloudflareModelCatalogPageSize; i++ {
				result = append(result, map[string]any{"name": fmt.Sprintf("@cf/test/model-%02d", i)})
			}
		case "2":
			result = append(result,
				map[string]any{"name": "@cf/test/model-00"},
				map[string]any{"name": "@cf/test/model-final"},
			)
		default:
			t.Fatalf("unexpected page %q", page)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"success": true,
			"result":  result,
			"result_info": map[string]any{
				"page":        calls,
				"total_pages": 2,
			},
		})
	}))
	defer server.Close()

	models, err := fetchCloudflareModelsForSiteToken(
		context.Background(),
		&model.Site{Platform: model.SitePlatformCloudflare, BaseURL: server.URL + "/client/v4/accounts/account-id/ai"},
		nil,
		model.SiteToken{Token: apiToken},
	)
	if err != nil {
		t.Fatalf("fetchCloudflareModelsForSiteToken failed: %v", err)
	}
	if calls != 2 {
		t.Fatalf("expected two catalog pages, got %d", calls)
	}
	if len(models) != cloudflareModelCatalogPageSize+1 {
		t.Fatalf("expected %d deduplicated models, got %d", cloudflareModelCatalogPageSize+1, len(models))
	}
	if models[len(models)-1] != "@cf/test/model-final" {
		t.Fatalf("expected final model to be preserved, got %q", models[len(models)-1])
	}
}

func TestFetchCloudflareModelsReturnsEnvelopeError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"success": false,
			"errors":  []map[string]any{{"message": "Workers AI permission denied"}},
		})
	}))
	defer server.Close()

	_, err := fetchCloudflareModelsForSiteToken(
		context.Background(),
		&model.Site{Platform: model.SitePlatformCloudflare, BaseURL: server.URL + "/client/v4/accounts/account-id/ai/v1"},
		nil,
		model.SiteToken{Token: "cf-api-token"},
	)
	if err == nil || !strings.Contains(err.Error(), "Workers AI permission denied") {
		t.Fatalf("expected Cloudflare envelope error, got %v", err)
	}
}

func TestFetchCloudflareModelsKeepsPagingAfterShortPage(t *testing.T) {
	const apiToken = "cf-api-token"
	pages := 0

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		pages++
		if got := r.URL.Query().Get("per_page"); got != fmt.Sprint(cloudflareModelCatalogPageSize) {
			t.Errorf("unexpected per_page %q", got)
		}
		result := []map[string]any{}
		// Every page stays well below the requested per_page, emulating a server
		// that caps the page size while still holding more pages.
		switch r.URL.Query().Get("page") {
		case "1":
			result = append(result,
				map[string]any{"name": "@cf/test/model-a"},
				map[string]any{"name": "@cf/test/model-b"},
			)
		case "2":
			result = append(result, map[string]any{"name": "@cf/test/model-c"})
		case "3":
		default:
			t.Errorf("unexpected page %q", r.URL.Query().Get("page"))
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"success": true,
			"result":  result,
		})
	}))
	defer server.Close()

	models, err := fetchCloudflareModelsForSiteToken(
		context.Background(),
		&model.Site{Platform: model.SitePlatformCloudflare, BaseURL: server.URL + "/client/v4/accounts/account-id/ai"},
		nil,
		model.SiteToken{Token: apiToken},
	)
	if err != nil {
		t.Fatalf("fetchCloudflareModelsForSiteToken failed: %v", err)
	}
	if pages != 3 {
		t.Fatalf("expected pagination to continue past the short page, got %d requests", pages)
	}
	expected := []string{"@cf/test/model-a", "@cf/test/model-b", "@cf/test/model-c"}
	if len(models) != len(expected) {
		t.Fatalf("expected %v, got %v", expected, models)
	}
	for i, name := range expected {
		if models[i] != name {
			t.Fatalf("expected %v, got %v", expected, models)
		}
	}
}

func TestCollectCloudflareCatalogModelsStopsOnEmptyPageWithoutTotalPages(t *testing.T) {
	requested := make([]int, 0, 3)
	models, err := collectCloudflareCatalogModels(cloudflareModelCatalogMaxPages, func(page int) (map[string]any, error) {
		requested = append(requested, page)
		result := []any{}
		if page < 3 {
			result = append(result, map[string]any{"name": fmt.Sprintf("@cf/test/model-%d", page)})
		}
		return map[string]any{"success": true, "result": result}, nil
	})
	if err != nil {
		t.Fatalf("collectCloudflareCatalogModels failed: %v", err)
	}
	if len(requested) != 3 || requested[2] != 3 {
		t.Fatalf("expected pages 1..3 to be requested, got %v", requested)
	}
	if len(models) != 2 {
		t.Fatalf("expected two models, got %v", models)
	}
}

func TestCollectCloudflareCatalogModelsFailsWhenPageLimitReached(t *testing.T) {
	calls := 0
	models, err := collectCloudflareCatalogModels(3, func(page int) (map[string]any, error) {
		calls++
		return map[string]any{
			"success": true,
			"result":  []any{map[string]any{"name": fmt.Sprintf("@cf/test/model-%d", page)}},
		}, nil
	})
	if err == nil {
		t.Fatalf("expected an error when the page guard is exhausted, got models %v", models)
	}
	if models != nil {
		t.Fatalf("expected no partial catalog on error, got %v", models)
	}
	if calls != 3 {
		t.Fatalf("expected the page guard to cap requests at 3, got %d", calls)
	}
	if !strings.Contains(err.Error(), "exceeded 3 pages") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func decodeCloudflareCatalogPayload(t *testing.T, raw string) map[string]any {
	t.Helper()
	var payload map[string]any
	if err := json.Unmarshal([]byte(raw), &payload); err != nil {
		t.Fatalf("invalid catalog payload %q: %v", raw, err)
	}
	return payload
}

// TestCollectCloudflareCatalogModelsTotalPagesJSONForms feeds total_pages through
// real JSON decoding so every numeric shape the API could emit is exercised.
// Usable values must terminate pagination; unusable ones must fall back to the
// empty-page probe instead of truncating the catalog.
func TestCollectCloudflareCatalogModelsTotalPagesJSONForms(t *testing.T) {
	tests := []struct {
		name          string
		resultInfo    string
		expectedPages int
	}{
		{name: "integer", resultInfo: `"result_info":{"total_pages":2},`, expectedPages: 2},
		{name: "float", resultInfo: `"result_info":{"total_pages":2.0},`, expectedPages: 2},
		{name: "string", resultInfo: `"result_info":{"total_pages":"2"},`, expectedPages: 2},
		{name: "null total_pages", resultInfo: `"result_info":{"total_pages":null},`, expectedPages: 4},
		{name: "zero total_pages", resultInfo: `"result_info":{"total_pages":0},`, expectedPages: 4},
		{name: "negative total_pages", resultInfo: `"result_info":{"total_pages":-1},`, expectedPages: 4},
		{name: "unparsable total_pages", resultInfo: `"result_info":{"total_pages":"many"},`, expectedPages: 4},
		{name: "null result_info", resultInfo: `"result_info":null,`, expectedPages: 4},
		{name: "array result_info", resultInfo: `"result_info":[],`, expectedPages: 4},
		{name: "absent result_info", resultInfo: ``, expectedPages: 4},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			requested := 0
			models, err := collectCloudflareCatalogModels(cloudflareModelCatalogMaxPages, func(page int) (map[string]any, error) {
				requested = page
				items := fmt.Sprintf(`[{"name":"@cf/test/model-%d"}]`, page)
				if page > 3 {
					items = `[]`
				}
				return decodeCloudflareCatalogPayload(t, fmt.Sprintf(`{"success":true,%s"result":%s}`, tt.resultInfo, items)), nil
			})
			if err != nil {
				t.Fatalf("collectCloudflareCatalogModels failed: %v", err)
			}
			if requested != tt.expectedPages {
				t.Fatalf("expected %d pages to be requested, got %d", tt.expectedPages, requested)
			}
			expectedModels := min(tt.expectedPages, 3)
			if len(models) != expectedModels {
				t.Fatalf("expected %d models, got %v", expectedModels, models)
			}
		})
	}
}

func TestCollectCloudflareCatalogModelsRejectsNonArrayResult(t *testing.T) {
	tests := []struct {
		name    string
		payload string
	}{
		{name: "missing result", payload: `{"success":true}`},
		{name: "null result", payload: `{"success":true,"result":null}`},
		{name: "object result", payload: `{"success":true,"result":{}}`},
		{name: "string result", payload: `{"success":true,"result":"none"}`},
		{name: "number result", payload: `{"success":true,"result":0}`},
		{name: "empty envelope", payload: `{}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			models, err := collectCloudflareCatalogModels(cloudflareModelCatalogMaxPages, func(page int) (map[string]any, error) {
				return decodeCloudflareCatalogPayload(t, tt.payload), nil
			})
			if err == nil || !strings.Contains(err.Error(), "missing a result array") {
				t.Fatalf("expected a missing result array error, got models %v err %v", models, err)
			}
			if models != nil {
				t.Fatalf("expected no catalog on error, got %v", models)
			}
		})
	}
}

func TestCollectCloudflareCatalogModelsEmptyResultOnFirstPage(t *testing.T) {
	calls := 0
	models, err := collectCloudflareCatalogModels(cloudflareModelCatalogMaxPages, func(page int) (map[string]any, error) {
		calls++
		return decodeCloudflareCatalogPayload(t, `{"success":true,"result":[],"result_info":{"total_pages":7}}`), nil
	})
	if err != nil {
		t.Fatalf("collectCloudflareCatalogModels failed: %v", err)
	}
	if calls != 1 {
		t.Fatalf("expected an empty first page to end pagination, got %d requests", calls)
	}
	if models == nil || len(models) != 0 {
		t.Fatalf("expected an empty non-nil catalog, got %#v", models)
	}
}

func TestCollectCloudflareCatalogModelsRejectsEnvelopeFailureMidPagination(t *testing.T) {
	calls := 0
	models, err := collectCloudflareCatalogModels(cloudflareModelCatalogMaxPages, func(page int) (map[string]any, error) {
		calls++
		if page == 1 {
			return decodeCloudflareCatalogPayload(t, `{"success":true,"result":[{"name":"@cf/test/model-1"}],"result_info":{"total_pages":3}}`), nil
		}
		return decodeCloudflareCatalogPayload(t, `{"success":false,"result":null,"errors":[{"message":"Authentication error"}]}`), nil
	})
	if err == nil || !strings.Contains(err.Error(), "Authentication error") {
		t.Fatalf("expected the envelope error to surface, got models %v err %v", models, err)
	}
	if models != nil {
		t.Fatalf("expected no partial catalog on envelope failure, got %v", models)
	}
	if calls != 2 {
		t.Fatalf("expected pagination to stop at the failing page, got %d requests", calls)
	}
}

func TestCollectCloudflareCatalogModelsPropagatesFetchError(t *testing.T) {
	sentinel := errors.New("transport exploded")
	calls := 0
	models, err := collectCloudflareCatalogModels(cloudflareModelCatalogMaxPages, func(page int) (map[string]any, error) {
		calls++
		if page == 1 {
			return decodeCloudflareCatalogPayload(t, `{"success":true,"result":[{"name":"@cf/test/model-1"}],"result_info":{"total_pages":3}}`), nil
		}
		return nil, sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("expected the fetch error to propagate unchanged, got %v", err)
	}
	if models != nil {
		t.Fatalf("expected no partial catalog on fetch failure, got %v", models)
	}
	if calls != 2 {
		t.Fatalf("expected pagination to stop at the failing page, got %d requests", calls)
	}
}

func TestCollectCloudflareCatalogModelsDeduplicatesAcrossPages(t *testing.T) {
	models, err := collectCloudflareCatalogModels(cloudflareModelCatalogMaxPages, func(page int) (map[string]any, error) {
		return decodeCloudflareCatalogPayload(t, fmt.Sprintf(`{
			"success": true,
			"result": [
				{"name": "@cf/test/model-shared"},
				{"name": "  @cf/test/model-shared  "},
				{"name": "@cf/test/model-page-%d"},
				{"name": "@cf/test/model-image", "task": {"name": "Text-to-Image"}}
			],
			"result_info": {"total_pages": 2}
		}`, page)), nil
	})
	if err != nil {
		t.Fatalf("collectCloudflareCatalogModels failed: %v", err)
	}
	expected := []string{"@cf/test/model-page-1", "@cf/test/model-page-2", "@cf/test/model-shared"}
	if len(models) != len(expected) {
		t.Fatalf("expected %v, got %v", expected, models)
	}
	for i, name := range expected {
		if models[i] != name {
			t.Fatalf("expected %v, got %v", expected, models)
		}
	}
}

func TestFetchCloudflareModelsPropagatesCanceledContext(t *testing.T) {
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"success": true, "result": []any{}})
	}))
	defer server.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	models, err := fetchCloudflareModelsForSiteToken(
		ctx,
		&model.Site{Platform: model.SitePlatformCloudflare, BaseURL: server.URL + "/client/v4/accounts/account-id/ai"},
		nil,
		model.SiteToken{Token: "cf-api-token"},
	)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context cancellation to propagate, got %v", err)
	}
	if models != nil {
		t.Fatalf("expected no catalog after cancellation, got %v", models)
	}
	if requests != 0 {
		t.Fatalf("expected no catalog request after cancellation, got %d", requests)
	}
}

func TestFetchCloudflareModelsPropagatesHTTPErrorWithoutPartialCatalog(t *testing.T) {
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Query().Get("page") == "1" {
			_ = json.NewEncoder(w).Encode(map[string]any{
				"success":     true,
				"result":      []any{map[string]any{"name": "@cf/test/model-1"}},
				"result_info": map[string]any{"total_pages": 3},
			})
			return
		}
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"success": false,
			"errors":  []map[string]any{{"message": "catalog unavailable"}},
		})
	}))
	defer server.Close()

	models, err := fetchCloudflareModelsForSiteToken(
		context.Background(),
		&model.Site{Platform: model.SitePlatformCloudflare, BaseURL: server.URL + "/client/v4/accounts/account-id/ai"},
		nil,
		model.SiteToken{Token: "cf-api-token"},
	)
	if err == nil || !strings.Contains(err.Error(), "catalog unavailable") {
		t.Fatalf("expected the upstream HTTP error to surface, got models %v err %v", models, err)
	}
	if models != nil {
		t.Fatalf("expected no partial catalog on HTTP failure, got %v", models)
	}
	if requests != 2 {
		t.Fatalf("expected pagination to stop at the failing page, got %d requests", requests)
	}
}
