package sitesync

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/bestruirui/octopus/internal/apperror"
	"github.com/bestruirui/octopus/internal/helper"
	"github.com/bestruirui/octopus/internal/model"
)

const (
	siteModelSourceSync         = "sync"
	siteModelSourceSyncFallback = "sync_fallback"

	sub2APIUserUIRequestHeader = "X-User-UI-Request"
	sub2APIPageSize            = 100
	sub2APIMaxPages            = 10
	sub2APIReadMaxAttempts     = 3
	sub2APIReadRetryDelay      = 150 * time.Millisecond
)

type sub2APITokenFetchResult struct {
	tokens   []model.SiteToken
	complete bool
}

type sub2APIGroupDiscoveryState struct {
	authoritative bool
	complete      bool
}

func sub2APIUserHeaders(accessToken string) map[string]string {
	return map[string]string{
		"Authorization":            ensureBearer(accessToken),
		sub2APIUserUIRequestHeader: "1",
	}
}

type siteModelFetchResult struct {
	names         []string
	source        string
	detections    map[string]siteModelRouteDetection
	authoritative bool
	message       string
}

func fetchManagementTokens(ctx context.Context, siteRecord *model.Site, account *model.SiteAccount, accessToken string) ([]model.SiteToken, error) {
	payload, err := requestJSONWithManagedAccessToken(ctx, siteRecord, http.MethodGet, buildSiteURL(siteRecord.BaseURL, "/api/token/?p=0&size=100"), nil, accessToken, account)
	if err != nil {
		return nil, err
	}
	items := parseTokenItems(payload)
	resolvedKeys := fetchMaskedManagedTokenKeys(ctx, siteRecord, account, accessToken, items, nil)
	tokens := make([]model.SiteToken, 0, len(items))
	for index, item := range items {
		tokenValue := strings.TrimSpace(jsonString(item["key"]))
		if model.IsMaskedSiteTokenValue(tokenValue) {
			if remoteID, ok := siteTokenRemoteID(item); ok {
				tokenValue = firstNonEmptyString(resolvedKeys[strconv.Itoa(remoteID)], tokenValue)
			}
		}
		if tokenValue == "" {
			continue
		}
		groupKey := model.NormalizeSiteGroupKey(firstNonEmptyString(jsonString(item["group"]), jsonString(item["token_group"]), jsonString(item["group_name"])))
		groupName := model.NormalizeSiteGroupName(groupKey, firstNonEmptyString(jsonString(item["group_name"]), jsonString(item["group"]), jsonString(item["token_group"])))
		tokens = append(tokens, model.SiteToken{Name: firstNonEmptyString(strings.TrimSpace(jsonString(item["name"])), fmt.Sprintf("token-%d", index+1)), Token: tokenValue, GroupKey: groupKey, GroupName: groupName, Enabled: parseEnabledFlag(item["status"]), Source: "sync", IsDefault: index == 0})
	}
	return tokens, nil
}

func fetchMaskedManagedTokenKeys(ctx context.Context, siteRecord *model.Site, account *model.SiteAccount, accessToken string, items []map[string]any, extraHeaders map[string]string) map[string]string {
	ids := make([]int, 0, len(items))
	seen := make(map[int]struct{}, len(items))
	for _, item := range items {
		if !model.IsMaskedSiteTokenValue(jsonString(item["key"])) {
			continue
		}
		remoteID, ok := siteTokenRemoteID(item)
		if !ok {
			continue
		}
		if _, ok := seen[remoteID]; ok {
			continue
		}
		seen[remoteID] = struct{}{}
		ids = append(ids, remoteID)
	}
	if len(ids) == 0 {
		return nil
	}

	request := func(method string, path string, body any) (map[string]any, error) {
		requestURL := buildSiteURL(siteRecord.BaseURL, path)
		if len(extraHeaders) > 0 {
			return requestJSONWithManagedHeaders(ctx, siteRecord, method, requestURL, body, accessToken, extraHeaders, account)
		}
		return requestJSONWithManagedAccessToken(ctx, siteRecord, method, requestURL, body, accessToken, account)
	}

	resolved := make(map[string]string, len(ids))
	if payload, err := request(http.MethodPost, "/api/token/batch/keys", map[string]any{"ids": ids}); err == nil {
		for key, value := range parseManagedTokenKeys(payload) {
			resolved[key] = value
		}
	}

	for _, remoteID := range ids {
		key := strconv.Itoa(remoteID)
		if value := strings.TrimSpace(resolved[key]); value != "" && !model.IsMaskedSiteTokenValue(value) {
			continue
		}
		for _, detail := range []struct {
			method string
			path   string
		}{
			{method: http.MethodPost, path: "/api/token/" + key + "/key"},
			{method: http.MethodGet, path: "/api/token/" + key},
		} {
			payload, err := request(detail.method, detail.path, nil)
			if err != nil {
				continue
			}
			value := extractSiteTokenValueFromPayload(payload)
			if value != "" && !model.IsMaskedSiteTokenValue(value) {
				resolved[key] = value
				break
			}
		}
	}
	return resolved
}

func parseManagedTokenKeys(payload map[string]any) map[string]string {
	resolved := make(map[string]string)
	candidates := []any{
		nestedValue(payload, "data", "keys"),
		payload["keys"],
	}
	for _, candidate := range candidates {
		values, ok := candidate.(map[string]any)
		if !ok {
			continue
		}
		for remoteID, rawValue := range values {
			value := siteTokenStringValue(rawValue)
			if value == "" {
				value = extractSiteTokenValueFromPayload(rawValue)
			}
			if value == "" || model.IsMaskedSiteTokenValue(value) {
				continue
			}
			resolved[remoteID] = value
		}
	}
	return resolved
}

func siteTokenRemoteID(item map[string]any) (int, bool) {
	for _, key := range []string{"id", "token_id", "tokenId", "key_id", "keyId"} {
		value := strings.TrimSpace(jsonString(item[key]))
		if value == "" {
			continue
		}
		remoteID, err := strconv.Atoi(value)
		if err == nil && remoteID > 0 {
			return remoteID, true
		}
	}
	return 0, false
}

func fetchManagementGroups(ctx context.Context, siteRecord *model.Site, account *model.SiteAccount, accessToken string) ([]model.SiteUserGroup, error) {
	endpoints := []string{"/api/user/self/groups", "/api/user_group_map"}
	seen := make(map[string]model.SiteUserGroup)
	for _, endpoint := range endpoints {
		payload, err := requestJSONWithManagedAccessToken(ctx, siteRecord, "GET", buildSiteURL(siteRecord.BaseURL, endpoint), nil, accessToken, account)
		if err != nil {
			continue
		}
		for _, group := range parseGroupItems(payload) {
			key := model.NormalizeSiteGroupKey(group.GroupKey)
			group.GroupKey = key
			group.Name = model.NormalizeSiteGroupName(key, group.Name)
			group.RawPayload = marshalRawPayload(payload)
			seen[key] = group
		}
	}
	if len(seen) == 0 {
		return []model.SiteUserGroup{{GroupKey: model.SiteDefaultGroupKey, Name: model.SiteDefaultGroupName}}, nil
	}
	groups := make([]model.SiteUserGroup, 0, len(seen))
	for _, group := range seen {
		groups = append(groups, group)
	}
	return groups, nil
}

func fetchSub2APITokens(ctx context.Context, siteRecord *model.Site, account *model.SiteAccount, accessToken string) (sub2APITokenFetchResult, error) {
	endpoints := []string{"/api/v1/keys", "/api/v1/api-keys"}
	var firstErr error
	var partialResult sub2APITokenFetchResult
	var successfulEndpoint bool
	var incompleteEndpoint bool
	for _, endpoint := range endpoints {
		items, complete, err := fetchSub2APIPagedItems(ctx, siteRecord, account, accessToken, endpoint, parseTokenItemsFromAny)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		tokens := buildSub2APITokensFromItems(items)
		successfulEndpoint = true
		if !complete {
			incompleteEndpoint = true
		}
		if len(tokens) > 0 {
			if complete {
				return sub2APITokenFetchResult{tokens: tokens, complete: true}, nil
			}
			if len(partialResult.tokens) == 0 {
				partialResult = sub2APITokenFetchResult{tokens: tokens, complete: false}
			}
			continue
		}
		if !complete && firstErr == nil {
			firstErr = fmt.Errorf("sub2api %s pagination did not complete", endpoint)
		}
	}
	if len(partialResult.tokens) > 0 {
		return partialResult, nil
	}
	if firstErr != nil && (!successfulEndpoint || incompleteEndpoint) {
		return sub2APITokenFetchResult{}, firstErr
	}
	return sub2APITokenFetchResult{complete: true}, nil
}

func fetchSub2APIGroups(ctx context.Context, siteRecord *model.Site, account *model.SiteAccount, accessToken string, tokens []model.SiteToken) ([]model.SiteUserGroup, sub2APIGroupDiscoveryState, error) {
	seen := make(map[string]model.SiteUserGroup)
	for _, group := range inferSub2APIGroupsFromTokens(tokens) {
		mergeSub2APIGroup(seen, group)
	}

	// Standard Sub2API exposes /groups/available. Fengwind's current deployment
	// uses /channels/products instead and binds keys through channel_id.
	endpoints := []string{
		"/api/v1/groups/available",
		"/api/v1/channels/products",
	}
	var firstErr error
	for _, endpoint := range endpoints {
		groups, err := fetchSub2APISingleGroupEndpoint(ctx, siteRecord, account, accessToken, endpoint)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		for _, group := range groups {
			mergeSub2APIGroup(seen, group)
		}
		return sortedSub2APIGroups(seen), sub2APIGroupDiscoveryState{authoritative: true, complete: true}, nil
	}

	fallbackSucceeded := false
	fallbackComplete := true
	for _, endpoint := range []string{"/api/v1/groups", "/api/v1/group"} {
		groups, complete, fallbackErr := fetchSub2APIPagedGroups(ctx, siteRecord, account, accessToken, endpoint)
		if fallbackErr != nil {
			if firstErr == nil {
				firstErr = fallbackErr
			}
			continue
		}
		fallbackSucceeded = true
		fallbackComplete = fallbackComplete && complete
		for _, group := range groups {
			mergeSub2APIGroup(seen, group)
		}
	}
	if len(seen) > 0 {
		return sortedSub2APIGroups(seen), sub2APIGroupDiscoveryState{complete: fallbackSucceeded && fallbackComplete}, nil
	}
	if firstErr != nil {
		return nil, sub2APIGroupDiscoveryState{}, firstErr
	}
	return []model.SiteUserGroup{{GroupKey: model.SiteDefaultGroupKey, Name: model.SiteDefaultGroupName}}, sub2APIGroupDiscoveryState{complete: fallbackSucceeded && fallbackComplete}, nil
}

func requestSub2APIUserJSON(ctx context.Context, siteRecord *model.Site, account *model.SiteAccount, accessToken string, endpoint string) (map[string]any, error) {
	requestURL := endpoint
	if !strings.HasPrefix(endpoint, "http://") && !strings.HasPrefix(endpoint, "https://") {
		requestURL = buildSiteURL(siteRecord.BaseURL, endpoint)
	}
	var lastErr error
	for attempt := 1; attempt <= sub2APIReadMaxAttempts; attempt++ {
		payload, err := requestJSON(ctx, siteRecord, http.MethodGet, requestURL, nil, sub2APIUserHeaders(accessToken), account)
		if err == nil {
			return payload, nil
		}
		lastErr = err
		if attempt >= sub2APIReadMaxAttempts || !isRetryableSub2APIReadError(err) {
			break
		}
		timer := time.NewTimer(sub2APIReadRetryDelay * time.Duration(attempt))
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
	return nil, lastErr
}

func isRetryableSub2APIReadError(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "eof") ||
		strings.Contains(message, "connection reset") ||
		strings.Contains(message, "connection refused") ||
		strings.Contains(message, "http 502") ||
		strings.Contains(message, "http 503") ||
		strings.Contains(message, "http 504")
}

func fetchSub2APISingleGroupEndpoint(ctx context.Context, siteRecord *model.Site, account *model.SiteAccount, accessToken string, endpoint string) ([]model.SiteUserGroup, error) {
	payload, err := requestSub2APIUserJSON(ctx, siteRecord, account, accessToken, endpoint)
	if err != nil {
		return nil, err
	}
	data, err := unwrapSub2APIData(payload, endpoint)
	if err != nil {
		return nil, err
	}
	if isExplicitEmptyCollection(data) {
		return []model.SiteUserGroup{}, nil
	}
	if endpoint == "/api/v1/channels/products" {
		groups := parseSub2APIProductGroups(data)
		if len(groups) == 0 {
			return nil, fmt.Errorf("sub2api %s returned an unrecognized group response", endpoint)
		}
		return groups, nil
	}
	groups := parseGroupItemsFromAny(data)
	if len(groups) == 0 {
		return nil, fmt.Errorf("sub2api %s returned an unrecognized group response", endpoint)
	}
	return groups, nil
}

func fetchSub2APIPagedGroups(ctx context.Context, siteRecord *model.Site, account *model.SiteAccount, accessToken string, endpoint string) ([]model.SiteUserGroup, bool, error) {
	seen := make(map[string]model.SiteUserGroup)
	previousSignature := ""
	for page := 1; page <= sub2APIMaxPages; page++ {
		payload, data, err := fetchSub2APIPage(ctx, siteRecord, account, accessToken, endpoint, page)
		if err != nil {
			if page == 1 {
				return nil, false, err
			}
			return sortedSub2APIGroups(seen), false, nil
		}
		if !isExplicitEmptyCollection(data) && len(parseGroupItemsFromAny(data)) == 0 {
			return sortedSub2APIGroups(seen), false, fmt.Errorf("sub2api %s returned an unrecognized group response", endpoint)
		}
		groups := parseGroupItemsFromAny(data)
		signature := sub2APIValueSignature(groups)
		if page > 1 && signature != "" && signature == previousSignature {
			return sortedSub2APIGroups(seen), false, nil
		}
		previousSignature = signature
		for _, group := range groups {
			mergeSub2APIGroup(seen, group)
		}
		if !sub2APIHasNextPage(payload, page, len(groups)) {
			return sortedSub2APIGroups(seen), true, nil
		}
	}
	return sortedSub2APIGroups(seen), false, nil
}

func fetchSub2APIPagedItems(ctx context.Context, siteRecord *model.Site, account *model.SiteAccount, accessToken string, endpoint string, parse func(any) []map[string]any) ([]map[string]any, bool, error) {
	items := make([]map[string]any, 0)
	seen := make(map[string]struct{})
	previousSignature := ""
	for page := 1; page <= sub2APIMaxPages; page++ {
		payload, data, err := fetchSub2APIPage(ctx, siteRecord, account, accessToken, endpoint, page)
		if err != nil {
			if page == 1 {
				return nil, false, err
			}
			return items, false, nil
		}
		if !isRecognizedSub2APIItemCollection(data) {
			return items, false, fmt.Errorf("sub2api %s returned an unrecognized item response", endpoint)
		}
		pageItems := parse(data)
		signature := sub2APIValueSignature(pageItems)
		if page > 1 && signature != "" && signature == previousSignature {
			return items, false, nil
		}
		previousSignature = signature
		for _, item := range pageItems {
			itemSignature := sub2APIValueSignature(item)
			if _, ok := seen[itemSignature]; ok {
				continue
			}
			seen[itemSignature] = struct{}{}
			items = append(items, item)
		}
		if !sub2APIHasNextPage(payload, page, len(pageItems)) {
			return items, true, nil
		}
	}
	return items, false, nil
}

func fetchSub2APIPage(ctx context.Context, siteRecord *model.Site, account *model.SiteAccount, accessToken string, endpoint string, page int) (map[string]any, any, error) {
	requestURL, err := url.Parse(buildSiteURL(siteRecord.BaseURL, endpoint))
	if err != nil {
		return nil, nil, err
	}
	query := requestURL.Query()
	query.Set("page", strconv.Itoa(page))
	query.Set("page_size", strconv.Itoa(sub2APIPageSize))
	requestURL.RawQuery = query.Encode()
	payload, err := requestSub2APIUserJSON(ctx, siteRecord, account, accessToken, requestURL.String())
	if err != nil {
		return nil, nil, err
	}
	data, err := unwrapSub2APIData(payload, endpoint)
	if err != nil {
		return nil, nil, err
	}
	return payload, data, nil
}

func sub2APIHasNextPage(payload map[string]any, page int, itemCount int) bool {
	metadata := sub2APIPaginationMetadata(payload)
	if pages := firstPositiveInt(metadata, "pages", "total_pages", "totalPages", "last_page", "lastPage"); pages > 0 {
		return page < pages
	}
	for _, key := range []string{"has_more", "hasMore"} {
		if raw, ok := metadata[key]; ok {
			return parseEnabledFlag(raw)
		}
	}
	if total := firstPositiveInt(metadata, "total", "total_count", "totalCount"); total > 0 {
		return page*sub2APIPageSize < total
	}
	return itemCount >= sub2APIPageSize
}

func sub2APIPaginationMetadata(payload map[string]any) map[string]any {
	merged := make(map[string]any)
	copyFields := func(value any) {
		fields, ok := value.(map[string]any)
		if !ok {
			return
		}
		for key, raw := range fields {
			merged[key] = raw
		}
	}
	copyFields(payload)
	copyFields(payload["data"])
	copyFields(payload["pagination"])
	copyFields(payload["meta"])
	if data, ok := payload["data"].(map[string]any); ok {
		copyFields(data["pagination"])
		copyFields(data["meta"])
	}
	return merged
}

func firstPositiveInt(values map[string]any, keys ...string) int {
	for _, key := range keys {
		if value := int(anyToInt64(values[key])); value > 0 {
			return value
		}
	}
	return 0
}

func sub2APIValueSignature(value any) string {
	encoded, err := json.Marshal(value)
	if err != nil {
		return fmt.Sprintf("%#v", value)
	}
	return string(encoded)
}

func mergeSub2APIGroup(groups map[string]model.SiteUserGroup, group model.SiteUserGroup) {
	key := model.NormalizeSiteGroupKey(group.GroupKey)
	group.GroupKey = key
	group.Name = model.NormalizeSiteGroupName(key, group.Name)
	if existing, ok := groups[key]; ok && strings.TrimSpace(existing.Name) != "" && strings.TrimSpace(existing.Name) != existing.GroupKey && (strings.TrimSpace(group.Name) == "" || group.Name == group.GroupKey) {
		return
	}
	groups[key] = group
}

func sortedSub2APIGroups(groups map[string]model.SiteUserGroup) []model.SiteUserGroup {
	keys := make([]string, 0, len(groups))
	for key := range groups {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	result := make([]model.SiteUserGroup, 0, len(keys))
	for _, key := range keys {
		result = append(result, groups[key])
	}
	return result
}

func parseSub2APIProductGroups(value any) []model.SiteUserGroup {
	items := make([]map[string]any, 0)
	for _, candidate := range itemSliceCandidates(value) {
		items = append(items, normalizeItemSlice(candidate)...)
	}
	seen := make(map[string]model.SiteUserGroup)
	for _, item := range items {
		key := firstNonEmptyString(
			jsonString(item["channel_id"]),
			jsonString(item["channelId"]),
			jsonString(item["id"]),
			jsonString(item["group_id"]),
			jsonString(item["groupId"]),
			jsonString(item["key"]),
		)
		if key == "" {
			continue
		}
		name := firstNonEmptyString(
			jsonString(item["name"]),
			jsonString(item["channel_name"]),
			jsonString(item["channelName"]),
			jsonString(item["group_name"]),
			jsonString(item["groupName"]),
			key,
		)
		mergeSub2APIGroup(seen, model.SiteUserGroup{GroupKey: key, Name: name, RawPayload: marshalRawPayload(item)})
	}
	return sortedSub2APIGroups(seen)
}

func isExplicitEmptyCollection(value any) bool {
	switch typed := value.(type) {
	case []any:
		return len(typed) == 0
	case []map[string]any:
		return len(typed) == 0
	case []string:
		return len(typed) == 0
	case map[string]any:
		found := false
		for _, key := range []string{"groups", "products", "channels", "items", "list", "records", "rows", "data"} {
			child, ok := typed[key]
			if !ok {
				continue
			}
			found = true
			if !isExplicitEmptyCollection(child) {
				return false
			}
		}
		return found
	}
	return false
}

func isRecognizedSub2APIItemCollection(value any) bool {
	switch typed := value.(type) {
	case []any, []map[string]any, []string:
		return true
	case map[string]any:
		for _, key := range []string{"items", "models", "data", "list", "records", "rows"} {
			if child, ok := typed[key]; ok {
				return isRecognizedSub2APIItemCollection(child)
			}
		}
	}
	return false
}

func unwrapSub2APIData(payload map[string]any, endpoint string) (any, error) {
	if payload == nil {
		return nil, apperror.Newf(apperror.CodeSiteSub2APIMissingData, "sub2api %s returned empty response", endpoint)
	}
	if rawCode, ok := payload["code"]; ok {
		code := anyToInt64(rawCode)
		if code != 0 {
			message := firstNonEmptyString(extractSiteResponseMessage(payload), fmt.Sprintf("sub2api %s returned code %d", endpoint, code))
			return nil, apperror.Newf(apperror.CodeSiteSub2APIEnvelopeFailed, "sub2api %s failed: %s", endpoint, message)
		}
		if data, ok := payload["data"]; ok {
			return data, nil
		}
		return nil, apperror.Newf(apperror.CodeSiteSub2APIMissingData, "sub2api %s response missing data", endpoint)
	}
	return payload, nil
}

func buildSub2APITokensFromItems(items []map[string]any) []model.SiteToken {
	tokens := make([]model.SiteToken, 0, len(items))
	for index, item := range items {
		tokenValue := firstNonEmptyString(
			jsonString(item["key"]),
			jsonString(item["token"]),
			jsonString(item["api_key"]),
			jsonString(item["apiKey"]),
			jsonString(item["access_token"]),
			jsonString(item["accessToken"]),
		)
		if tokenValue == "" {
			continue
		}
		groupKey := model.NormalizeSiteGroupKey(firstNonEmptyString(
			jsonString(item["group_id"]),
			jsonString(item["groupId"]),
			jsonString(nestedValue(item, "group", "id")),
			jsonString(item["channel_id"]),
			jsonString(item["channelId"]),
			jsonString(item["token_group"]),
			jsonString(item["tokenGroup"]),
			jsonString(item["group_name"]),
			jsonString(item["groupName"]),
			jsonString(nestedValue(item, "group", "name")),
			jsonString(item["channel_name"]),
			jsonString(item["channelName"]),
			jsonString(item["group"]),
		))
		groupName := model.NormalizeSiteGroupName(groupKey, firstNonEmptyString(
			jsonString(item["group_name"]),
			jsonString(item["groupName"]),
			jsonString(nestedValue(item, "group", "name")),
			jsonString(item["channel_name"]),
			jsonString(item["channelName"]),
			jsonString(item["group"]),
			jsonString(item["token_group"]),
			jsonString(item["tokenGroup"]),
		))
		tokens = append(tokens, model.SiteToken{
			Name:      firstNonEmptyString(strings.TrimSpace(jsonString(item["name"])), fmt.Sprintf("token-%d", index+1)),
			Token:     tokenValue,
			GroupKey:  groupKey,
			GroupName: groupName,
			Enabled:   parseSub2APITokenEnabled(item),
			Source:    "sync",
			IsDefault: index == 0,
		})
	}
	return tokens
}

func parseSub2APITokenEnabled(item map[string]any) bool {
	for _, key := range []string{"enabled", "is_enabled", "isEnabled", "active", "is_active", "isActive", "status"} {
		if raw, ok := item[key]; ok {
			return parseEnabledFlag(raw)
		}
	}
	if raw, ok := item["disabled"]; ok {
		return !parseEnabledFlag(raw)
	}
	return true
}

func inferSub2APIGroupsFromTokens(tokens []model.SiteToken) []model.SiteUserGroup {
	inferredGroups := make([]model.SiteUserGroup, 0)
	seen := make(map[string]struct{})
	for _, token := range tokens {
		key := model.NormalizeSiteGroupKey(token.GroupKey)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		inferredGroups = append(inferredGroups, model.SiteUserGroup{GroupKey: key, Name: model.NormalizeSiteGroupName(key, token.GroupName)})
	}
	return inferredGroups
}

func stripBearerPrefix(token string) string {
	trimmed := strings.TrimSpace(token)
	if strings.HasPrefix(strings.ToLower(trimmed), "bearer ") {
		return strings.TrimSpace(trimmed[7:])
	}
	return trimmed
}

func fetchModelsForSiteToken(ctx context.Context, siteRecord *model.Site, account *model.SiteAccount, token model.SiteToken) ([]string, error) {
	if siteRecord != nil && siteRecord.Platform == model.SitePlatformSub2API {
		return fetchSub2APIModelsForSiteToken(ctx, siteRecord, account, token)
	}
	if siteRecord != nil && siteRecord.Platform == model.SitePlatformCloudflare {
		return fetchCloudflareModelsForSiteToken(ctx, siteRecord, account, token)
	}

	// Normalize the token the same way projection does, so the model-fetch
	// request authenticates with the value the upstream actually expects
	// (new-api family needs the "sk-" prefix; direct providers stay verbatim).
	tokenValue := model.NormalizeSiteSyncTokenValueForPlatform(siteRecord.Platform, token.Token)

	proxyMode, proxyConfigID := resolveSiteAccountProxy(siteRecord, account)
	var (
		firstErr error
		models   []string
	)

	for _, baseURL := range buildModelFetchBaseURLs(siteRecord) {
		channel := model.Channel{Type: platformOutboundType(siteRecord), BaseUrls: []model.BaseUrl{{URL: baseURL, Delay: 0}}, Keys: []model.ChannelKey{{Enabled: true, ChannelKey: tokenValue}}, ProxyMode: proxyMode, ProxyConfigID: proxyConfigID, CustomHeader: siteRecord.CustomHeader}
		fetched, err := helper.FetchModels(ctx, channel)
		if err == nil && len(fetched) > 0 {
			return normalizeModelNames(fetched), nil
		}
		if err != nil && firstErr == nil {
			firstErr = err
		}
		if len(fetched) > 0 {
			models = fetched
		}
	}
	if siteRecord.Platform != model.SitePlatformOneHub && siteRecord.Platform != model.SitePlatformDoneHub {
		if firstErr != nil {
			return nil, firstErr
		}
		return normalizeModelNames(models), nil
	}

	payload, fallbackErr := requestJSON(ctx, siteRecord, "GET", buildSiteURL(siteRecord.BaseURL, "/api/available_model"), nil, map[string]string{"Authorization": "Bearer " + tokenValue}, account)
	if fallbackErr != nil {
		if firstErr != nil {
			return nil, firstErr
		}
		return nil, fallbackErr
	}

	modelSet := make(map[string]struct{})
	if dataMap, ok := nestedValue(payload, "data").(map[string]any); ok {
		for key := range dataMap {
			trimmed := strings.TrimSpace(key)
			if trimmed != "" {
				modelSet[trimmed] = struct{}{}
			}
		}
	}
	if len(modelSet) == 0 {
		if firstErr != nil {
			return nil, firstErr
		}
		return normalizeModelNames(models), nil
	}
	names := make([]string, 0, len(modelSet))
	for name := range modelSet {
		names = append(names, name)
	}
	return normalizeModelNames(names), nil
}

func fetchManagementModels(
	ctx context.Context,
	siteRecord *model.Site,
	account *model.SiteAccount,
	accessToken string,
	token model.SiteToken,
	sessionFallbackFetcher func(token model.SiteToken) (siteModelFetchResult, error),
) (siteModelFetchResult, error) {
	models, err := fetchModelsForSiteToken(ctx, siteRecord, account, token)
	if len(models) > 0 {
		return siteModelFetchResult{names: models, source: siteModelSourceSync, authoritative: true, message: fmt.Sprintf("同步到 %d 个模型", len(models))}, nil
	}
	if siteRecord.Platform != model.SitePlatformNewAPI {
		return siteModelFetchResult{source: siteModelSourceSync, authoritative: err == nil, message: "上游当前没有返回可用模型"}, err
	}

	if sessionFallbackFetcher == nil {
		return siteModelFetchResult{message: "本次未能确认该分组模型，已保留历史模型"}, err
	}

	fallbackResult, fallbackErr := sessionFallbackFetcher(token)
	if len(fallbackResult.names) > 0 || fallbackResult.authoritative {
		if strings.TrimSpace(fallbackResult.source) == "" {
			fallbackResult.source = siteModelSourceSyncFallback
		}
		return fallbackResult, nil
	}
	if err != nil {
		if strings.TrimSpace(fallbackResult.message) == "" {
			fallbackResult.message = "本次未能确认该分组模型，已保留历史模型"
		}
		return fallbackResult, err
	}
	if fallbackErr != nil {
		return fallbackResult, fallbackErr
	}
	if strings.TrimSpace(fallbackResult.message) == "" {
		fallbackResult.message = "本次未能确认该分组模型，已保留历史模型"
	}
	return fallbackResult, nil
}

func fetchManagedSessionModels(ctx context.Context, siteRecord *model.Site, account *model.SiteAccount, accessToken string) ([]string, error) {
	if strings.TrimSpace(accessToken) == "" {
		return nil, nil
	}
	payload, err := requestJSONWithManagedAccessToken(ctx, siteRecord, "GET", buildSiteURL(siteRecord.BaseURL, "/api/user/models"), nil, accessToken, account)
	if err != nil {
		return nil, err
	}
	return anyRouterParseModelNames(payload), nil
}

func fetchSub2APIModelsForSiteToken(ctx context.Context, siteRecord *model.Site, account *model.SiteAccount, token model.SiteToken) ([]string, error) {
	tokenValue := strings.TrimSpace(token.Token)
	if tokenValue == "" {
		return nil, apperror.New(apperror.CodeSiteSub2APIModelAPIKeyRequired, "sub2api api key is required for model discovery")
	}

	var firstErr error
	for _, endpoint := range buildSub2APIModelEndpointURLs(siteRecord) {
		payload, err := requestJSON(ctx, siteRecord, "GET", endpoint, nil, map[string]string{"Authorization": ensureBearer(tokenValue)}, account)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		models, err := parseSub2APIModelNames(payload)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if len(models) > 0 {
			return models, nil
		}
	}
	if firstErr != nil {
		return nil, firstErr
	}
	return nil, nil
}

func buildSub2APIModelEndpointURLs(siteRecord *model.Site) []string {
	if siteRecord == nil {
		return nil
	}
	baseURL := strings.TrimRight(strings.TrimSpace(siteRecord.BaseURL), "/")
	if baseURL == "" {
		return nil
	}
	lower := strings.ToLower(baseURL)
	if strings.HasSuffix(lower, "/models") {
		return []string{baseURL}
	}

	candidates := make([]string, 0, 5)
	appendCandidate := func(url string) {
		url = strings.TrimRight(strings.TrimSpace(url), "/")
		if url == "" {
			return
		}
		for _, existing := range candidates {
			if strings.EqualFold(existing, url) {
				return
			}
		}
		candidates = append(candidates, url)
	}

	if strings.HasSuffix(lower, "/v1") || strings.HasSuffix(lower, "/v1beta") || strings.HasSuffix(lower, "/api/v1") || strings.HasSuffix(lower, "/antigravity/v1") || strings.HasSuffix(lower, "/antigravity/v1beta") {
		appendCandidate(baseURL + "/models")
		return candidates
	}
	if strings.HasSuffix(lower, "/antigravity") {
		appendCandidate(baseURL + "/v1/models")
		appendCandidate(baseURL + "/v1beta/models")
		return candidates
	}

	appendCandidate(baseURL + "/v1/models")
	appendCandidate(baseURL + "/api/v1/models")
	appendCandidate(baseURL + "/v1beta/models")
	appendCandidate(baseURL + "/antigravity/v1/models")
	appendCandidate(baseURL + "/antigravity/v1beta/models")
	appendCandidate(baseURL + "/models")
	return candidates
}

func parseSub2APIModelNames(payload map[string]any) ([]string, error) {
	data, err := unwrapSub2APIData(payload, "/models")
	if err != nil {
		return nil, err
	}
	return normalizeModelNames(collectSub2APIModelNames(data)), nil
}

func collectSub2APIModelNames(value any) []string {
	switch typed := value.(type) {
	case []any:
		names := make([]string, 0, len(typed))
		for _, item := range typed {
			names = append(names, collectSub2APIModelNames(item)...)
		}
		return names
	case []string:
		return typed
	case map[string]any:
		for _, key := range []string{"items", "models", "data", "list", "records", "rows"} {
			if child, ok := typed[key]; ok {
				if names := collectSub2APIModelNames(child); len(names) > 0 {
					return names
				}
			}
		}
		name := firstNonEmptyString(jsonString(typed["id"]), jsonString(typed["name"]), jsonString(typed["model"]), jsonString(typed["model_name"]), jsonString(typed["modelName"]))
		if name != "" {
			return []string{strings.TrimPrefix(name, "models/")}
		}
	case string:
		if trimmed := strings.TrimSpace(typed); trimmed != "" {
			return []string{strings.TrimPrefix(trimmed, "models/")}
		}
	}
	return nil
}

func buildModelFetchBaseURLs(siteRecord *model.Site) []string {
	if siteRecord == nil {
		return nil
	}
	// 和 helper.ProbeModelProtocol 共用同一套候选生成。这两处一旦不一致，就会出现
	// 只在第二次同步才暴露的 bug：探测在某个变体上认出协议并写库，下次同步因为协议
	// 已定不再探测，却去打另一个变体 —— 404，模型全丢。
	return helper.BuildVersionedBaseURLs(siteRecord.BaseURL, siteModelFetchVersionSuffix(siteRecord))
}

func filterSessionFallbackModelsByGroup(
	names []string,
	groupKey string,
	detections map[string]siteModelRouteDetection,
) siteModelFetchResult {
	normalizedGroupKey := model.NormalizeSiteGroupKey(groupKey)
	if normalizedGroupKey == "" {
		normalizedGroupKey = model.SiteDefaultGroupKey
	}
	if len(detections) == 0 {
		return siteModelFetchResult{message: fmt.Sprintf("无法从显式分组元数据确认分组 %q 的模型", normalizedGroupKey)}
	}

	filteredNames := make([]string, 0, len(names))
	filteredDetections := make(map[string]siteModelRouteDetection)
	hasExplicitGroupMetadata := false
	allModelsHaveExplicitGroupMetadata := true
	for _, name := range normalizeModelNames(names) {
		lookupKey := strings.ToLower(strings.TrimSpace(name))
		detection, ok := detections[lookupKey]
		if !ok {
			allModelsHaveExplicitGroupMetadata = false
			continue
		}
		metadata, ok := model.ParseSiteModelRouteMetadata(detection.RouteRawPayload)
		if !ok || len(metadata.EnableGroups) == 0 {
			allModelsHaveExplicitGroupMetadata = false
			continue
		}
		hasExplicitGroupMetadata = true
		if !stringSliceContainsFold(metadata.EnableGroups, normalizedGroupKey) {
			continue
		}
		filteredNames = append(filteredNames, name)
		filteredDetections[lookupKey] = detection
	}
	if len(filteredNames) > 0 {
		return siteModelFetchResult{
			names:         filteredNames,
			source:        siteModelSourceSyncFallback,
			detections:    filteredDetections,
			authoritative: true,
			message:       fmt.Sprintf("同步到 %d 个模型", len(filteredNames)),
		}
	}
	if !hasExplicitGroupMetadata {
		return siteModelFetchResult{message: fmt.Sprintf("显式分组元数据缺失，无法确认分组 %q 的模型", normalizedGroupKey)}
	}
	if !allModelsHaveExplicitGroupMetadata {
		return siteModelFetchResult{message: fmt.Sprintf("部分模型缺少显式分组元数据，无法确认分组 %q 的模型", normalizedGroupKey)}
	}
	if len(filteredNames) == 0 {
		return siteModelFetchResult{source: siteModelSourceSyncFallback, authoritative: true, message: fmt.Sprintf("分组 %q 当前没有可用模型", normalizedGroupKey)}
	}

	return siteModelFetchResult{message: fmt.Sprintf("无法确认分组 %q 的模型", normalizedGroupKey)}
}

func stringSliceContainsFold(values []string, target string) bool {
	normalizedTarget := strings.ToLower(strings.TrimSpace(target))
	if normalizedTarget == "" {
		return false
	}
	for _, value := range values {
		if strings.EqualFold(strings.TrimSpace(value), normalizedTarget) {
			return true
		}
	}
	return false
}

// siteModelFetchVersionSuffix 返回该站点还应该尝试的 base URL 版本段（"" = 只试
// base 本身）。
//
// 这里必须和 helper.ProbeModelProtocol 的候选保持一致。否则会出现一个只在第二次
// 同步才暴露的 bug：探测在 {base}/v1 上认出 anthropic 并写入协议，下次同步因为协议
// 已定不再探测，却只试 {base}/models —— 404，模型全丢。
func siteModelFetchVersionSuffix(site *model.Site) string {
	if site == nil || site.Platform != model.SitePlatformAPI {
		return "/v1"
	}
	if site.ResolveDefaultRouteType() == model.SiteModelRouteTypeGemini {
		return "/v1beta"
	}
	return "/v1"
}

func buildSiteModels(names []string, groupKey string, source string) []model.SiteModel {
	names = normalizeModelNames(names)
	models := make([]model.SiteModel, 0, len(names))
	groupKey = model.NormalizeSiteGroupKey(groupKey)
	for _, name := range names {
		models = append(models, model.SiteModel{GroupKey: groupKey, ModelName: name, Source: source})
	}
	return models
}

func buildGlobalSiteModels(names []string, groups []model.SiteUserGroup, source string) []model.SiteModel {
	if len(groups) == 0 {
		return buildSiteModels(names, model.SiteDefaultGroupKey, source)
	}
	seen := make(map[string]struct{})
	models := make([]model.SiteModel, 0, len(names)*len(groups))
	for _, group := range groups {
		groupKey := model.NormalizeSiteGroupKey(group.GroupKey)
		for _, item := range buildSiteModels(names, groupKey, source) {
			key := groupKey + "\x00" + item.ModelName
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			models = append(models, item)
		}
	}
	return models
}

func pickModelTokensByGroup(tokens []model.SiteToken) []model.SiteToken {
	if len(tokens) == 0 {
		return nil
	}

	order := make([]string, 0, len(tokens))
	selected := make(map[string]model.SiteToken, len(tokens))
	for _, token := range tokens {
		groupKey := model.NormalizeSiteGroupKey(token.GroupKey)
		token.GroupKey = groupKey
		token.GroupName = model.NormalizeSiteGroupName(groupKey, token.GroupName)
		if _, ok := selected[groupKey]; !ok {
			order = append(order, groupKey)
			selected[groupKey] = token
			continue
		}
		if shouldPreferGroupModelToken(token, selected[groupKey]) {
			selected[groupKey] = token
		}
	}

	result := make([]model.SiteToken, 0, len(order))
	for _, groupKey := range order {
		token := selected[groupKey]
		if strings.TrimSpace(token.Token) == "" {
			continue
		}
		result = append(result, token)
	}
	return result
}

func shouldPreferGroupModelToken(candidate model.SiteToken, current model.SiteToken) bool {
	candidateToken := strings.TrimSpace(candidate.Token)
	currentToken := strings.TrimSpace(current.Token)
	if candidateToken == "" {
		return false
	}
	if currentToken == "" {
		return true
	}
	if candidate.Enabled != current.Enabled {
		return candidate.Enabled
	}
	return candidate.IsDefault && !current.IsDefault
}

func syncSiteModelsByGroup(
	ctx context.Context,
	siteRecord *model.Site,
	account *model.SiteAccount,
	accessToken string,
	groupTokens []model.SiteToken,
	platformUserID string,
	source string,
	fetcher func(token model.SiteToken, allowGlobalFallback bool) (siteModelFetchResult, error),
) ([]model.SiteModel, []siteGroupSyncResult) {
	if len(groupTokens) == 0 {
		return nil, nil
	}

	allowGlobalFallback := len(groupTokens) == 1
	models := make([]model.SiteModel, 0)
	results := make([]siteGroupSyncResult, 0, len(groupTokens))
	seen := make(map[string]struct{})

	for _, token := range groupTokens {
		result, err := fetcher(token, allowGlobalFallback)
		groupResult := siteGroupSyncResult{
			GroupKey:  model.NormalizeSiteGroupKey(token.GroupKey),
			GroupName: model.NormalizeSiteGroupName(token.GroupKey, token.GroupName),
			HasKey:    true,
		}
		if result.authoritative && len(result.names) == 0 {
			groupResult.Status = siteGroupSyncStatusEmpty
			groupResult.Authoritative = true
			groupResult.Message = firstNonEmptyString(strings.TrimSpace(result.message), "上游当前没有可用模型，已清空该分组历史模型")
			results = append(results, groupResult)
			continue
		}
		if len(result.names) == 0 {
			if err != nil {
				groupResult.Status = siteGroupSyncStatusFailed
				groupResult.Message = firstNonEmptyString(strings.TrimSpace(result.message), err.Error())
			} else {
				groupResult.Status = siteGroupSyncStatusUnresolved
				groupResult.Message = firstNonEmptyString(strings.TrimSpace(result.message), "本次未能确认该分组模型，已沿用历史投影")
			}
			results = append(results, groupResult)
			continue
		}

		groupSource := strings.TrimSpace(result.source)
		if groupSource == "" {
			groupSource = source
		}
		groupModels := buildSiteModels(result.names, token.GroupKey, groupSource)
		if len(result.detections) > 0 {
			groupModels = applyKnownRouteDetectionsToSiteModels(groupModels, result.detections)
		} else {
			groupModels = applyDetectedRoutesToSiteModels(ctx, siteRecord, account, accessToken, token, platformUserID, groupModels)
		}
		for _, item := range groupModels {
			key := model.NormalizeSiteGroupKey(item.GroupKey) + "\x00" + strings.TrimSpace(item.ModelName)
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			models = append(models, item)
		}
		groupResult.Status = siteGroupSyncStatusSynced
		groupResult.Authoritative = result.authoritative || len(groupModels) > 0
		groupResult.ModelCount = len(groupModels)
		groupResult.Message = firstNonEmptyString(strings.TrimSpace(result.message), fmt.Sprintf("同步到 %d 个模型", len(groupModels)))
		results = append(results, groupResult)
	}

	sort.Slice(models, func(i, j int) bool {
		leftGroup := model.NormalizeSiteGroupKey(models[i].GroupKey)
		rightGroup := model.NormalizeSiteGroupKey(models[j].GroupKey)
		if leftGroup == rightGroup {
			return models[i].ModelName < models[j].ModelName
		}
		return leftGroup < rightGroup
	})
	return models, results
}

func expandExplicitGroupModelsToGroups(
	items []model.SiteModel,
	groups []model.SiteUserGroup,
	tokens []model.SiteToken,
) []model.SiteModel {
	if len(items) == 0 || len(groups) == 0 {
		return items
	}

	groupKeys := make(map[string]struct{}, len(groups))
	for _, group := range groups {
		groupKey := model.NormalizeSiteGroupKey(group.GroupKey)
		groupKeys[groupKey] = struct{}{}
	}

	groupKeysWithTokens := make(map[string]struct{}, len(tokens))
	for _, token := range tokens {
		groupKey := model.NormalizeSiteGroupKey(token.GroupKey)
		groupKeysWithTokens[groupKey] = struct{}{}
	}

	groupsWithoutTokens := make(map[string]struct{})
	for groupKey := range groupKeys {
		if _, ok := groupKeysWithTokens[groupKey]; ok {
			continue
		}
		groupsWithoutTokens[groupKey] = struct{}{}
	}
	if len(groupsWithoutTokens) == 0 {
		return items
	}

	expanded := make([]model.SiteModel, 0, len(items))
	seen := make(map[string]struct{}, len(items))
	for _, item := range items {
		groupKey := model.NormalizeSiteGroupKey(item.GroupKey)
		modelName := strings.TrimSpace(item.ModelName)
		key := groupKey + "\x00" + modelName
		if _, ok := seen[key]; !ok {
			seen[key] = struct{}{}
			expanded = append(expanded, item)
		}

		metadata, ok := model.ParseSiteModelRouteMetadata(item.RouteRawPayload)
		if !ok || len(metadata.EnableGroups) == 0 {
			continue
		}
		for _, explicitGroupKey := range metadata.EnableGroups {
			targetGroupKey := model.NormalizeSiteGroupKey(explicitGroupKey)
			if _, ok := groupsWithoutTokens[targetGroupKey]; !ok {
				continue
			}
			targetKey := targetGroupKey + "\x00" + modelName
			if _, ok := seen[targetKey]; ok {
				continue
			}
			copy := item
			copy.ID = 0
			copy.SiteAccountID = 0
			copy.GroupKey = targetGroupKey
			expanded = append(expanded, copy)
			seen[targetKey] = struct{}{}
		}
	}
	return expanded
}

func mergeSiteGroups(groups []model.SiteUserGroup, tokens []model.SiteToken) []model.SiteUserGroup {
	merged := make(map[string]model.SiteUserGroup)
	for _, item := range groups {
		key := model.NormalizeSiteGroupKey(item.GroupKey)
		item.GroupKey = key
		item.Name = model.NormalizeSiteGroupName(key, item.Name)
		merged[key] = item
	}
	for _, token := range tokens {
		key := model.NormalizeSiteGroupKey(token.GroupKey)
		if _, ok := merged[key]; ok {
			continue
		}
		merged[key] = model.SiteUserGroup{GroupKey: key, Name: model.NormalizeSiteGroupName(key, token.GroupName)}
	}
	if len(merged) == 0 {
		merged[model.SiteDefaultGroupKey] = model.SiteUserGroup{GroupKey: model.SiteDefaultGroupKey, Name: model.SiteDefaultGroupName}
	}
	return sortedSub2APIGroups(merged)
}

func jsonFloat(value any) float64 {
	switch typed := value.(type) {
	case float64:
		return typed
	case int:
		return float64(typed)
	case int64:
		return float64(typed)
	case string:
		trimmed := strings.TrimSpace(typed)
		if trimmed == "" {
			return 0
		}
		var f float64
		if _, err := fmt.Sscanf(trimmed, "%f", &f); err == nil {
			return f
		}
		return 0
	default:
		return 0
	}
}
func pickModelToken(tokens []model.SiteToken) model.SiteToken {
	for _, token := range tokens {
		if token.Enabled && strings.TrimSpace(token.Token) != "" {
			return token
		}
	}
	for _, token := range tokens {
		if strings.TrimSpace(token.Token) != "" {
			return token
		}
	}
	return model.SiteToken{}
}
