package sitesync

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/bestruirui/octopus/internal/client"
	"github.com/bestruirui/octopus/internal/model"
	"github.com/bestruirui/octopus/internal/op"
	"github.com/bestruirui/octopus/internal/utils/httpbody"
)

func siteHTTPClient(ctx context.Context, siteRecord *model.Site, accounts ...*model.SiteAccount) (*http.Client, error) {
	if siteRecord == nil {
		return nil, fmt.Errorf("site is nil")
	}
	proxyMode, proxyConfigID := resolveSiteAccountProxy(siteRecord, accounts...)
	switch proxyMode {
	case "", model.ProxyUsageModeDirect:
		return client.GetHTTPClientSystemProxy(false)
	case model.ProxyUsageModeSystem:
		return client.GetHTTPClientSystemProxy(true)
	case model.ProxyUsageModePool:
		if proxyConfigID == nil || *proxyConfigID <= 0 {
			return nil, fmt.Errorf("proxy config id is required when proxy mode is pool")
		}
		proxyURL, err := op.ProxyURLForConfig(*proxyConfigID, ctx)
		if err != nil {
			return nil, err
		}
		return client.GetHTTPClientCustomProxy(proxyURL)
	default:
		return nil, fmt.Errorf("unsupported proxy mode: %s", proxyMode)
	}
}

func resolveSiteAccountProxy(siteRecord *model.Site, accounts ...*model.SiteAccount) (model.ProxyUsageMode, *int) {
	if len(accounts) > 0 && accounts[0] != nil && accounts[0].ProxyMode != "" && accounts[0].ProxyMode != model.ProxyUsageModeInherit {
		return accounts[0].ProxyMode, accounts[0].ProxyConfigID
	}
	if siteRecord == nil {
		return model.ProxyUsageModeDirect, nil
	}
	if siteRecord.ProxyMode == "" {
		return model.ProxyUsageModeDirect, nil
	}
	return siteRecord.ProxyMode, siteRecord.ProxyConfigID
}

func requestJSON(ctx context.Context, siteRecord *model.Site, method string, requestURL string, body any, headers map[string]string, accounts ...*model.SiteAccount) (map[string]any, error) {
	httpClient, err := siteHTTPClient(ctx, siteRecord, accounts...)
	if err != nil {
		return nil, err
	}

	var bodyReader io.Reader
	if body != nil {
		payload, marshalErr := json.Marshal(body)
		if marshalErr != nil {
			return nil, marshalErr
		}
		bodyReader = bytes.NewReader(payload)
	}

	req, err := http.NewRequestWithContext(ctx, method, requestURL, bodyReader)
	if err != nil {
		return nil, err
	}
	applyDefaultSiteRequestHeaders(req, body != nil)
	for _, item := range siteRecord.CustomHeader {
		if strings.TrimSpace(item.HeaderKey) != "" {
			req.Header.Set(strings.TrimSpace(item.HeaderKey), item.HeaderValue)
		}
	}
	for key, value := range headers {
		if strings.TrimSpace(key) != "" && strings.TrimSpace(value) != "" {
			req.Header.Set(key, value)
		}
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	bodyBytes, err := httpbody.ReadResponse(resp)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, formatSiteHTTPError(resp.StatusCode, resp.Header, bodyBytes)
	}
	if len(bodyBytes) == 0 {
		return map[string]any{}, nil
	}

	var payload map[string]any
	if err := json.Unmarshal(bodyBytes, &payload); err != nil {
		return nil, formatSiteDecodeError(resp.Header.Get("Content-Type"), bodyBytes, err)
	}
	return payload, nil
}

func applyDefaultSiteRequestHeaders(req *http.Request, hasJSONBody bool) {
	if req == nil {
		return
	}
	if hasJSONBody {
		req.Header.Set("Content-Type", "application/json")
	}
	if req.Header.Get("User-Agent") == "" {
		req.Header.Set("User-Agent", anyRouterUserAgent)
	}
	if req.Header.Get("Accept") == "" {
		req.Header.Set("Accept", "application/json, text/plain, */*")
	}
	if req.Header.Get("Accept-Language") == "" {
		req.Header.Set("Accept-Language", "zh-CN,zh;q=0.9,en;q=0.8")
	}
}

func formatSiteHTTPError(statusCode int, header http.Header, bodyBytes []byte) error {
	// Detect challenge pages before extracting a JSON message. A challenge may
	// be served with an application/json content type or a JSON-looking wrapper,
	// but must never be persisted as an ordinary provider error.
	if IsCloudflareProtectionResponse(statusCode, header, bodyBytes) {
		return wrapCloudflareProtectionError(newCloudflareProtectionError(statusCode, header))
	}
	if payload, ok := parseSiteJSONMap(bodyBytes); ok {
		if message := extractSiteResponseMessage(payload); message != "" {
			return newSiteHTTPError(statusCode, message)
		}
	}
	if summary := extractSiteHTMLResponseSummary(header.Get("Content-Type"), bodyBytes); summary != "" {
		return newSiteHTTPError(statusCode, summary)
	}
	return newSiteHTTPError(statusCode, "上游返回非 JSON 响应，无法解析为接口响应")
}

// IsCloudflareProtectionResponse 判断一次上游响应是否为 Cloudflare 防护拦截（403 + CF 指纹）。
// 供 sitesync 内部与被动离群退役（POR）门3 复用。
//
// 分类顺序（自上而下短路）：
//  1. 命中挑战页/防火墙特征标记 -> 挑战页；
//  2. 响应体承载结构化 JSON -> 普通上游接口错误（Cloudflare 挑战页不会返回 JSON）；
//  3. 非 JSON 响应体里出现 cloudflare 字样 -> 挑战页；
//  4. 回落到 CF-Ray / Server 响应头指纹。
//
// 因此 api.cloudflare.com 之类以 JSON 返回的 403（鉴权失败、路由不存在等）不再被误判为挑战页，
// 而把 Content-Type 伪装成 application/json 的挑战页仍会在第 1/3 步按响应体内容识别出来。
func IsCloudflareProtectionResponse(statusCode int, header http.Header, bodyBytes []byte) bool {
	if statusCode != http.StatusForbidden {
		return false
	}
	body := strings.ToLower(string(bodyBytes))
	if containsCloudflareChallengeMarker(body) {
		return true
	}
	if looksLikeSiteJSONBody(bodyBytes) {
		return false
	}
	if strings.Contains(body, "cloudflare") {
		return true
	}
	server := strings.ToLower(header.Get("Server"))
	return header.Get("CF-Ray") != "" || strings.Contains(server, "cloudflare")
}

// cloudflareChallengeBodyMarkers 是挑战页与防火墙拦截页（含 1020 之类纯文本错误码）的强特征，
// 命中即判定为防护拦截，优先级高于 JSON 判定。
var cloudflareChallengeBodyMarkers = []string{
	"attention required",
	"just a moment",
	"cf-error-code",
	"cf-error-details",
	"cloudflare ray id",
	"cf_chl_opt",
	"challenge-platform",
	"enable javascript and cookies to continue",
	"error code: 1020",
	"error code 1020",
}

func containsCloudflareChallengeMarker(loweredBody string) bool {
	return slices.ContainsFunc(cloudflareChallengeBodyMarkers, func(marker string) bool {
		return strings.Contains(loweredBody, marker)
	})
}

// looksLikeSiteJSONBody 判断响应体是否承载 JSON 对象/数组，允许上游在外层包一段文本前缀
// （例如 POR 门3 记录的 `upstream error: 403: {...}`）。
// 明显是 HTML 的响应体直接排除，避免挑战页内嵌 JSON 片段绕过检测。
func looksLikeSiteJSONBody(bodyBytes []byte) bool {
	body := strings.TrimSpace(string(bodyBytes))
	if body == "" {
		return false
	}
	lowered := strings.ToLower(body)
	if strings.HasPrefix(body, "<") || strings.Contains(lowered, "<html") || strings.Contains(lowered, "<!doctype html") {
		return false
	}
	start := strings.IndexAny(body, "{[")
	end := strings.LastIndexAny(body, "}]")
	if start < 0 || end <= start {
		return false
	}
	var payload any
	if err := json.Unmarshal([]byte(body[start:end+1]), &payload); err != nil {
		return false
	}
	switch payload.(type) {
	case map[string]any, []any:
		return true
	default:
		return false
	}
}

func formatSiteDecodeError(contentType string, bodyBytes []byte, err error) error {
	if summary := extractSiteHTMLResponseSummary(contentType, bodyBytes); summary != "" {
		return wrapSiteDecodeError(fmt.Sprintf("decode response failed: %s", summary), err)
	}
	return wrapSiteDecodeError(fmt.Sprintf("decode response failed: %v", err), err)
}

func parseSiteJSONMap(bodyBytes []byte) (map[string]any, bool) {
	if len(bodyBytes) == 0 {
		return map[string]any{}, true
	}
	var payload map[string]any
	if err := json.Unmarshal(bodyBytes, &payload); err != nil {
		return nil, false
	}
	return payload, true
}

func extractSiteResponseMessage(payload map[string]any) string {
	if payload == nil {
		return ""
	}
	if message := firstNonEmptyString(
		jsonString(payload["message"]),
		jsonString(nestedValue(payload, "error", "message")),
		jsonString(payload["msg"]),
	); message != "" {
		return message
	}
	for _, item := range normalizeItemSlice(payload["errors"]) {
		if message := firstNonEmptyString(jsonString(item["message"]), jsonString(item["error"])); message != "" {
			return message
		}
		// Cloudflare V4 的部分错误只在 errors[].error_chain[].message 里带描述。
		for _, chained := range normalizeItemSlice(item["error_chain"]) {
			if message := firstNonEmptyString(jsonString(chained["message"]), jsonString(chained["error"])); message != "" {
				return message
			}
		}
	}
	return ""
}

func extractSiteHTMLResponseSummary(contentType string, bodyBytes []byte) string {
	body := strings.TrimSpace(string(bodyBytes))
	if body == "" {
		return ""
	}
	if summary := anyRouterExtractHTMLErrorSummary(body); summary != "" {
		return summary
	}
	lowered := strings.ToLower(contentType + "\n" + body)
	if strings.Contains(lowered, "just a moment") {
		return "Just a moment..."
	}
	if strings.Contains(lowered, "cloudflare") {
		return "Cloudflare challenge"
	}
	return ""
}
func buildSiteURL(baseURL string, path string) string {
	baseURL = strings.TrimRight(strings.TrimSpace(baseURL), "/")
	if strings.HasPrefix(path, "http://") || strings.HasPrefix(path, "https://") {
		return path
	}
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	return baseURL + path
}

func parseTokenItems(payload map[string]any) []map[string]any {
	return parseTokenItemsFromAny(payload)
}

func parseTokenItemsFromAny(value any) []map[string]any {
	for _, candidate := range itemSliceCandidates(value) {
		if items := normalizeItemSlice(candidate); len(items) > 0 {
			return items
		}
	}
	return nil
}

func itemSliceCandidates(value any) []any {
	payload, ok := value.(map[string]any)
	if !ok {
		return []any{value}
	}
	return []any{
		nestedValue(payload, "data", "items"),
		nestedValue(payload, "data", "list"),
		nestedValue(payload, "data", "records"),
		nestedValue(payload, "data", "rows"),
		nestedValue(payload, "data", "data"),
		nestedValue(payload, "data", "products"),
		nestedValue(payload, "data", "channels"),
		payload["items"],
		payload["list"],
		payload["records"],
		payload["rows"],
		payload["products"],
		payload["channels"],
		payload["data"],
	}
}

func parseGroupItems(payload map[string]any) []model.SiteUserGroup {
	return parseGroupItemsFromAny(payload)
}

func parseGroupItemsFromAny(value any) []model.SiteUserGroup {
	items := make([]model.SiteUserGroup, 0)
	for _, candidate := range groupItemCandidates(value) {
		items = parseGroupCandidate(candidate)
		if len(items) > 0 {
			break
		}
	}
	deduped := make(map[string]model.SiteUserGroup)
	for _, item := range items {
		key := model.NormalizeSiteGroupKey(item.GroupKey)
		item.GroupKey = key
		item.Name = model.NormalizeSiteGroupName(key, item.Name)
		deduped[key] = item
	}
	keys := make([]string, 0, len(deduped))
	for key := range deduped {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	result := make([]model.SiteUserGroup, 0, len(keys))
	for _, key := range keys {
		result = append(result, deduped[key])
	}
	return result
}

func groupItemCandidates(value any) []any {
	payload, ok := value.(map[string]any)
	if !ok {
		return []any{value}
	}
	return []any{
		nestedValue(payload, "data", "groups"),
		nestedValue(payload, "data", "items"),
		nestedValue(payload, "data", "list"),
		nestedValue(payload, "data", "records"),
		nestedValue(payload, "data", "rows"),
		nestedValue(payload, "data", "data"),
		payload["groups"],
		payload["items"],
		payload["list"],
		payload["records"],
		payload["rows"],
		payload["data"],
		payload,
	}
}

func parseGroupCandidate(candidate any) []model.SiteUserGroup {
	items := make([]model.SiteUserGroup, 0)
	switch value := candidate.(type) {
	case []any:
		for _, raw := range value {
			switch item := raw.(type) {
			case string:
				if trimmed := strings.TrimSpace(item); trimmed != "" {
					items = append(items, model.SiteUserGroup{GroupKey: trimmed, Name: trimmed})
				}
			case float64, int, int64:
				if groupKey := jsonString(item); groupKey != "" {
					items = append(items, model.SiteUserGroup{GroupKey: groupKey, Name: groupKey})
				}
			case map[string]any:
				if group, ok := parseGroupObject(item); ok {
					items = append(items, group)
				}
			}
		}
	case []string:
		for _, item := range value {
			if trimmed := strings.TrimSpace(item); trimmed != "" {
				items = append(items, model.SiteUserGroup{GroupKey: trimmed, Name: trimmed})
			}
		}
	case map[string]any:
		if group, ok := parseGroupObject(value); ok {
			return []model.SiteUserGroup{group}
		}
		for key, raw := range value {
			if isIgnorableGroupMapKey(key) {
				continue
			}
			name := key
			switch item := raw.(type) {
			case string:
				name = firstNonEmptyString(item, key)
			case map[string]any:
				name = firstNonEmptyString(jsonString(item["name"]), jsonString(item["group_name"]), jsonString(item["groupName"]), jsonString(item["title"]), jsonString(item["label"]), key)
			default:
				continue
			}
			items = append(items, model.SiteUserGroup{GroupKey: key, Name: name})
		}
	}
	return items
}

func parseGroupObject(item map[string]any) (model.SiteUserGroup, bool) {
	groupKey := firstNonEmptyString(
		jsonString(item["group_id"]),
		jsonString(item["groupId"]),
		jsonString(item["id"]),
		jsonString(item["value"]),
		jsonString(item["code"]),
		jsonString(item["name"]),
		jsonString(item["group_name"]),
		jsonString(item["groupName"]),
		jsonString(item["title"]),
		jsonString(item["label"]),
	)
	groupName := firstNonEmptyString(
		jsonString(item["name"]),
		jsonString(item["group_name"]),
		jsonString(item["groupName"]),
		jsonString(item["title"]),
		jsonString(item["label"]),
		groupKey,
	)
	if strings.TrimSpace(groupKey) == "" {
		return model.SiteUserGroup{}, false
	}
	return model.SiteUserGroup{GroupKey: strings.TrimSpace(groupKey), Name: strings.TrimSpace(groupName)}, true
}

func isIgnorableGroupMapKey(key string) bool {
	switch strings.ToLower(strings.TrimSpace(key)) {
	case "", "success", "message", "msg", "data", "code", "error", "errors", "groups", "items", "list", "records", "rows", "total", "total_count", "totalcount", "page", "current_page", "currentpage", "page_size", "pagesize", "pages", "total_pages", "totalpages", "last_page", "lastpage", "has_more", "hasmore", "pagination", "meta", "timestamp":
		return true
	default:
		return false
	}
}

func normalizeItemSlice(value any) []map[string]any {
	switch rawItems := value.(type) {
	case []map[string]any:
		return rawItems
	case []any:
		items := make([]map[string]any, 0, len(rawItems))
		for _, raw := range rawItems {
			if item, ok := raw.(map[string]any); ok {
				items = append(items, item)
			}
		}
		return items
	default:
		return nil
	}
}

// maxSiteModelNameLength 对齐 SiteModel.ModelName 的列宽（varchar(191)）。
//
// 上游偶尔会把一整串模型名塞进一个字段：实测 hub.linux.do 返回过一个 3200 字符的
// "模型名"，内容是几十个真模型名用空格拼起来的。SQLite 不限列宽，这种值在本地能
// 存下去；PostgreSQL 会直接抛 22001 (value too long for varchar(191))，让整次同步
// 失败并返回 500 —— 于是同一份配置本地跑得通、容器里报"服务内部错误"。
//
// 只按长度判，不按空格判：有站点真的用带空格的模型名（hub 上就有 "claude 4.5"）。
const maxSiteModelNameLength = 191

func normalizeModelNames(names []string) []string {
	seen := make(map[string]struct{}, len(names))
	result := make([]string, 0, len(names))
	for _, name := range names {
		trimmed := strings.TrimSpace(name)
		if trimmed == "" {
			continue
		}
		if utf8.RuneCountInString(trimmed) > maxSiteModelNameLength {
			continue
		}
		if _, ok := seen[trimmed]; ok {
			continue
		}
		seen[trimmed] = struct{}{}
		result = append(result, trimmed)
	}
	slices.Sort(result)
	return result
}

func parseEnabledFlag(value any) bool {
	switch typed := value.(type) {
	case bool:
		return typed
	case float64:
		return int(typed) != 0
	case int:
		return typed != 0
	case string:
		switch strings.ToLower(strings.TrimSpace(typed)) {
		case "", "enabled", "active", "1", "true", "on":
			return true
		case "disabled", "inactive", "0", "false", "off":
			return false
		default:
			return true
		}
	default:
		return true
	}
}

func ensureBearer(token string) string {
	token = strings.TrimSpace(token)
	if strings.HasPrefix(strings.ToLower(token), "bearer ") {
		return token
	}
	return "Bearer " + token
}

func requestJSONWithManagedAccessToken(ctx context.Context, siteRecord *model.Site, method string, requestURL string, body any, accessToken string, accounts ...*model.SiteAccount) (map[string]any, error) {
	initialHeaders := managedUserIDHeaders(firstManagedPlatformUserID(accounts...))
	payload, err := requestJSONWithManagedHeaders(ctx, siteRecord, method, requestURL, body, accessToken, initialHeaders, accounts...)
	if err == nil || !siteRequiresManagedUserIDHeader(siteRecord) || !shouldRetryManagedRequestWithUserID(err) {
		return payload, err
	}

	userID, discoverErr := discoverManagedUserID(ctx, siteRecord, accessToken, accounts...)
	if discoverErr != nil {
		return nil, discoverErr
	}
	if userID <= 0 {
		return nil, err
	}
	rememberManagedPlatformUserID(userID, accounts...)

	userHeaders := managedUserIDHeaders(userID)
	return requestJSONWithManagedHeaders(ctx, siteRecord, method, requestURL, body, accessToken, userHeaders, accounts...)
}

func requestJSONWithManagedHeaders(ctx context.Context, siteRecord *model.Site, method string, requestURL string, body any, accessToken string, extraHeaders map[string]string, accounts ...*model.SiteAccount) (map[string]any, error) {
	var firstErr error
	for _, headers := range buildManagedAuthHeaders(accessToken) {
		payload, err := requestJSON(ctx, siteRecord, method, requestURL, body, mergeHeaders(headers, extraHeaders), accounts...)
		if err == nil {
			return payload, nil
		}
		if firstErr == nil {
			firstErr = err
		}
		if !shouldTryAlternativeManagedAuth(err) {
			return nil, err
		}
	}
	return nil, firstErr
}

func buildManagedAuthHeaders(accessToken string) []map[string]string {
	token := strings.TrimSpace(accessToken)
	if token == "" {
		return []map[string]string{{}}
	}

	candidates := make([]map[string]string, 0, 2)
	if looksLikeCookieToken(token) {
		candidates = append(candidates, map[string]string{"Cookie": token})
	}
	candidates = append(candidates, map[string]string{"Authorization": ensureBearer(token)})
	return candidates
}

func looksLikeCookieToken(token string) bool {
	trimmed := strings.TrimSpace(token)
	lowered := strings.ToLower(trimmed)
	if trimmed == "" || strings.HasPrefix(lowered, "bearer ") {
		return false
	}
	if strings.Contains(trimmed, ";") {
		return true
	}
	return strings.Contains(trimmed, "=") && !strings.Contains(trimmed, " ")
}

func shouldTryAlternativeManagedAuth(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "http 400") || strings.Contains(message, "http 401") || strings.Contains(message, "http 403")
}

func siteRequiresManagedUserIDHeader(siteRecord *model.Site) bool {
	return siteRecord != nil && siteRecord.Platform == model.SitePlatformNewAPI
}

func shouldRetryManagedRequestWithUserID(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "new-api-user") ||
		strings.Contains(message, "missing user id") ||
		strings.Contains(message, "requires user id") ||
		strings.Contains(message, "invalid user id") ||
		strings.Contains(message, "wrong user id") ||
		strings.Contains(message, "未提供")
}

func discoverManagedUserID(ctx context.Context, siteRecord *model.Site, accessToken string, accounts ...*model.SiteAccount) (int, error) {
	requestURL := buildSiteURL(siteRecord.BaseURL, "/api/user/self")

	payload, err := requestJSONWithManagedHeaders(ctx, siteRecord, http.MethodGet, requestURL, nil, accessToken, nil, accounts...)
	if err == nil {
		if userID := anyRouterExtractUserID(payload); userID > 0 {
			return userID, nil
		}
	}

	var firstErr error
	if err != nil {
		firstErr = err
	}

	for _, userID := range anyRouterBuildUserIDProbeCandidates(accessToken) {
		userHeaders := map[string]string{}
		anyRouterAddUserIDHeaders(userHeaders, userID)
		payload, probeErr := requestJSONWithManagedHeaders(ctx, siteRecord, http.MethodGet, requestURL, nil, accessToken, userHeaders, accounts...)
		if probeErr != nil {
			if firstErr == nil {
				firstErr = probeErr
			}
			continue
		}
		if anyRouterExtractUserID(payload) > 0 {
			return userID, nil
		}
	}

	return 0, firstErr
}

func firstManagedPlatformUserID(accounts ...*model.SiteAccount) int {
	for _, account := range accounts {
		if account != nil && account.PlatformUserID != nil && *account.PlatformUserID > 0 {
			return *account.PlatformUserID
		}
	}
	return 0
}

func rememberManagedPlatformUserID(userID int, accounts ...*model.SiteAccount) {
	if userID <= 0 {
		return
	}
	for _, account := range accounts {
		if account == nil {
			continue
		}
		if account.PlatformUserID == nil || *account.PlatformUserID != userID {
			resolvedUserID := userID
			account.PlatformUserID = &resolvedUserID
		}
	}
}

func managedUserIDHeaders(userID int) map[string]string {
	if userID <= 0 {
		return nil
	}
	headers := map[string]string{}
	anyRouterAddUserIDHeaders(headers, userID)
	return headers
}

func mergeHeaders(base map[string]string, extra map[string]string) map[string]string {
	if len(base) == 0 && len(extra) == 0 {
		return nil
	}
	merged := make(map[string]string, len(base)+len(extra))
	for key, value := range base {
		merged[key] = value
	}
	for key, value := range extra {
		merged[key] = value
	}
	return merged
}

func jsonString(value any) string {
	switch typed := value.(type) {
	case string:
		return strings.TrimSpace(typed)
	case float64:
		return strings.TrimSpace(fmt.Sprintf("%.0f", typed))
	case int:
		return strings.TrimSpace(fmt.Sprintf("%d", typed))
	case int64:
		return strings.TrimSpace(fmt.Sprintf("%d", typed))
	default:
		return ""
	}
}

func jsonBool(value any) bool {
	typed, ok := value.(bool)
	if ok {
		return typed
	}
	return false
}

func nestedValue(payload map[string]any, keys ...string) any {
	var current any = payload
	for _, key := range keys {
		obj, ok := current.(map[string]any)
		if !ok {
			return nil
		}
		current = obj[key]
	}
	return current
}

func firstNonEmptyString(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func marshalRawPayload(value any) string {
	payload, err := json.Marshal(value)
	if err != nil {
		return ""
	}
	return string(payload)
}
