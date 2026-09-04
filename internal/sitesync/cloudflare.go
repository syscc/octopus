package sitesync

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/bestruirui/octopus/internal/model"
)

const (
	cloudflareModelCatalogPageSize = 50
	cloudflareModelCatalogMaxPages = 20
)

// cloudflareWorkersAIBaseURL normalizes either documented Workers AI base URL
// form (ending in /ai or /ai/v1) to the management API path used by the
// catalog endpoint.
func cloudflareWorkersAIBaseURL(baseURL string) string {
	baseURL = strings.TrimRight(strings.TrimSpace(baseURL), "/")
	if baseURL == "" {
		return ""
	}

	lowered := strings.ToLower(baseURL)
	if strings.HasSuffix(lowered, "/ai/v1") {
		return baseURL[:len(baseURL)-len("/v1")]
	}
	if strings.HasSuffix(lowered, "/ai") {
		return baseURL
	}
	return baseURL + "/ai"
}

func fetchCloudflareModelsForSiteToken(ctx context.Context, siteRecord *model.Site, account *model.SiteAccount, token model.SiteToken) ([]string, error) {
	if siteRecord == nil {
		return nil, fmt.Errorf("cloudflare site is nil")
	}

	apiToken := model.NormalizeSiteSyncTokenValueForPlatform(siteRecord.Platform, token.Token)
	apiToken = stripBearerPrefix(apiToken)
	if apiToken == "" {
		return nil, newDirectTokenRequiredError()
	}

	catalogBaseURL := cloudflareWorkersAIBaseURL(siteRecord.BaseURL)
	if catalogBaseURL == "" {
		return nil, fmt.Errorf("cloudflare workers ai base url is empty")
	}
	catalogURL, err := url.Parse(buildSiteURL(catalogBaseURL, "/models/search"))
	if err != nil {
		return nil, fmt.Errorf("invalid cloudflare model catalog url: %w", err)
	}

	return collectCloudflareCatalogModels(cloudflareModelCatalogMaxPages, func(page int) (map[string]any, error) {
		query := catalogURL.Query()
		query.Set("task", "Text Generation")
		query.Set("page", fmt.Sprint(page))
		query.Set("per_page", fmt.Sprint(cloudflareModelCatalogPageSize))
		catalogURL.RawQuery = query.Encode()

		return requestJSON(
			ctx,
			siteRecord,
			http.MethodGet,
			catalogURL.String(),
			nil,
			map[string]string{"Authorization": ensureBearer(apiToken)},
			account,
		)
	})
}

// collectCloudflareCatalogModels walks catalog pages until the response proves
// the catalog is exhausted: either result_info advertises an explicit
// total_pages, or a page comes back with no results at all. A short page is not
// a terminal signal because Cloudflare may cap per_page below the requested
// size, which would silently truncate the catalog. Hitting maxPages without a
// terminal page is reported as an error so a partial catalog is never treated
// as authoritative.
func collectCloudflareCatalogModels(maxPages int, fetchPage func(page int) (map[string]any, error)) ([]string, error) {
	if maxPages <= 0 {
		maxPages = cloudflareModelCatalogMaxPages
	}

	models := make([]string, 0)
	for page := 1; page <= maxPages; page++ {
		payload, requestErr := fetchPage(page)
		if requestErr != nil {
			return nil, requestErr
		}
		if success, exists := payload["success"]; exists && !jsonBool(success) {
			return nil, fmt.Errorf("cloudflare model catalog request failed: %s", cloudflareModelCatalogErrorMessage(payload))
		}

		rawItems, exists := payload["result"]
		if !exists {
			return nil, fmt.Errorf("cloudflare model catalog response is missing a result array")
		}
		items, parseErr := parseCloudflareCatalogItems(rawItems)
		if parseErr != nil {
			return nil, parseErr
		}
		for index, item := range items {
			if !cloudflareCatalogItemIsTextGeneration(item) {
				continue
			}
			rawName, ok := item["name"].(string)
			name := strings.TrimSpace(rawName)
			if !ok || name == "" {
				return nil, fmt.Errorf("cloudflare model catalog result item %d on page %d is missing a non-empty string name", index, page)
			}
			models = append(models, name)
		}

		if len(items) == 0 {
			return normalizeModelNames(models), nil
		}
		if totalPages := cloudflareCatalogTotalPages(payload); totalPages > 0 && page >= totalPages {
			return normalizeModelNames(models), nil
		}
	}

	return nil, fmt.Errorf("cloudflare model catalog pagination exceeded %d pages without reaching a terminal page", maxPages)
}

func parseCloudflareCatalogItems(value any) ([]map[string]any, error) {
	switch rawItems := value.(type) {
	case []map[string]any:
		for index, item := range rawItems {
			if item == nil {
				return nil, fmt.Errorf("cloudflare model catalog result item %d is not an object", index)
			}
		}
		return rawItems, nil
	case []any:
		items := make([]map[string]any, 0, len(rawItems))
		for index, raw := range rawItems {
			item, ok := raw.(map[string]any)
			if !ok || item == nil {
				return nil, fmt.Errorf("cloudflare model catalog result item %d is not an object", index)
			}
			items = append(items, item)
		}
		return items, nil
	default:
		return nil, fmt.Errorf("cloudflare model catalog response is missing a result array")
	}
}

func cloudflareCatalogTotalPages(payload map[string]any) int {
	resultInfo, ok := payload["result_info"].(map[string]any)
	if !ok {
		return 0
	}
	return int(anyToInt64(resultInfo["total_pages"]))
}

func cloudflareCatalogItemIsTextGeneration(item map[string]any) bool {
	if item == nil {
		return false
	}
	taskName := firstNonEmptyString(
		jsonString(nestedValue(item, "task", "name")),
		jsonString(item["task"]),
		jsonString(item["task_name"]),
		jsonString(item["taskName"]),
	)
	return taskName == "" || strings.EqualFold(strings.TrimSpace(taskName), "Text Generation")
}

func cloudflareModelCatalogErrorMessage(payload map[string]any) string {
	if message := extractSiteResponseMessage(payload); message != "" {
		return message
	}
	for _, item := range normalizeItemSlice(payload["errors"]) {
		if message := firstNonEmptyString(jsonString(item["message"]), jsonString(item["error"])); message != "" {
			return message
		}
	}
	return "unknown error"
}
