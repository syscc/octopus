package sitesync

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/bestruirui/octopus/internal/model"
)

const siteTokenSourceCreated = "created"

func CreateAccountToken(ctx context.Context, accountID int, req model.SiteChannelKeyCreateRequest) (*model.SiteSyncResult, error) {
	siteRecord, account, err := loadSiteAccount(ctx, accountID)
	if err != nil {
		return nil, err
	}
	if siteRecord == nil || account == nil {
		return nil, fmt.Errorf("site account not found")
	}

	groupKey := model.NormalizeSiteGroupKey(req.GroupKey)
	name := strings.TrimSpace(req.Name)
	var createdToken *model.SiteToken

	switch siteRecord.Platform {
	case model.SitePlatformAnyRouter:
		createdToken, err = createAnyRouterToken(ctx, siteRecord, account, groupKey, name)
	case model.SitePlatformNewAPI, model.SitePlatformOneAPI, model.SitePlatformOneHub, model.SitePlatformDoneHub:
		createdToken, err = createManagementPlatformToken(ctx, siteRecord, account, groupKey, name)
	case model.SitePlatformSub2API:
		createdToken, err = createSub2APIToken(ctx, siteRecord, account, groupKey, name)
	default:
		return nil, fmt.Errorf("site platform %s does not support quick key creation", siteRecord.Platform)
	}
	if err != nil {
		return nil, err
	}

	return syncAccountWithCreatedToken(ctx, accountID, createdToken)
}

func createManagementPlatformToken(ctx context.Context, siteRecord *model.Site, account *model.SiteAccount, groupKey string, name string) (*model.SiteToken, error) {
	if account == nil {
		return nil, fmt.Errorf("site account is nil")
	}
	if account.CredentialType == model.SiteCredentialTypeAPIKey {
		return nil, fmt.Errorf("API key credential account does not support quick site key creation")
	}

	accessToken, err := resolveManagedAccessToken(ctx, siteRecord, account)
	if err != nil {
		return nil, err
	}

	requestBody := buildManagedTokenCreatePayload(account, groupKey, name)
	payload, err := requestJSONWithManagedAccessToken(
		ctx,
		siteRecord,
		http.MethodPost,
		buildSiteURL(siteRecord.BaseURL, "/api/token/"),
		requestBody,
		accessToken,
		account,
	)
	if err != nil {
		return nil, err
	}
	if !siteTokenCreateSucceeded(payload) {
		return nil, fmt.Errorf("%s", firstNonEmptyString(extractSiteResponseMessage(payload), "site token creation failed"))
	}
	return createdSiteTokenFromPayload(payload, groupKey, jsonString(requestBody["name"])), nil
}

func createAnyRouterToken(ctx context.Context, siteRecord *model.Site, account *model.SiteAccount, groupKey string, name string) (*model.SiteToken, error) {
	if account == nil {
		return nil, fmt.Errorf("site account is nil")
	}
	if account.CredentialType == model.SiteCredentialTypeAPIKey {
		return nil, fmt.Errorf("API key credential account does not support quick site key creation")
	}

	accessToken, err := resolveAnyRouterManagedAccessToken(ctx, siteRecord, account)
	if err != nil {
		return nil, err
	}

	payloadBody := buildManagedTokenCreatePayload(account, groupKey, name)
	requestURL := buildSiteURL(siteRecord.BaseURL, "/api/token/")

	userID, _ := anyRouterDiscoverUserID(ctx, siteRecord, account, accessToken)
	payload, _, err := anyRouterRequestJSONWithCookies(
		ctx,
		siteRecord,
		http.MethodPost,
		requestURL,
		payloadBody,
		anyRouterAuthHeaders(accessToken, userID),
		account,
	)
	if err == nil && siteTokenCreateSucceeded(payload) {
		return createdSiteTokenFromPayload(payload, groupKey, jsonString(payloadBody["name"])), nil
	}

	tryUserIDs := []int{userID}
	if alternateUserID, probeErr := anyRouterProbeAlternateUserIDByCookie(ctx, siteRecord, account, accessToken, userID); probeErr == nil && alternateUserID > 0 {
		tryUserIDs = append(tryUserIDs, alternateUserID)
	}
	if userID <= 0 {
		if probedUserID, probeErr := anyRouterProbeUserIDByCookie(ctx, siteRecord, account, accessToken); probeErr == nil && probedUserID > 0 {
			tryUserIDs = append(tryUserIDs, probedUserID)
		}
	}
	tryUserIDs = slicesCompactInts(tryUserIDs)

	for _, candidateUserID := range tryUserIDs {
		for _, cookie := range anyRouterBuildCookieCandidates(accessToken) {
			headers := map[string]string{"Cookie": cookie}
			anyRouterAddUserIDHeaders(headers, candidateUserID)
			payload, _, requestErr := anyRouterRequestJSONWithCookies(
				ctx,
				siteRecord,
				http.MethodPost,
				requestURL,
				payloadBody,
				headers,
				account,
			)
			if requestErr != nil {
				if err == nil {
					err = requestErr
				}
				continue
			}
			if siteTokenCreateSucceeded(payload) {
				return createdSiteTokenFromPayload(payload, groupKey, jsonString(payloadBody["name"])), nil
			}
			if message := strings.TrimSpace(extractSiteResponseMessage(payload)); message != "" {
				err = fmt.Errorf("%s", message)
			}
		}
	}

	if err != nil {
		return nil, err
	}
	return nil, fmt.Errorf("site token creation failed")
}

func createSub2APIToken(ctx context.Context, siteRecord *model.Site, account *model.SiteAccount, groupKey string, name string) (*model.SiteToken, error) {
	if account == nil {
		return nil, fmt.Errorf("site account is nil")
	}
	if account.CredentialType == model.SiteCredentialTypeAPIKey {
		return nil, fmt.Errorf("API key credential account does not support quick site key creation")
	}

	accessToken := strings.TrimSpace(account.AccessToken)
	accessToken, err := ensureFreshSub2APIAccessToken(ctx, siteRecord, account, false)
	if err != nil {
		return nil, err
	}

	requestBody := buildSub2APITokenCreatePayload(account, groupKey, name)
	headers := sub2APIUserHeaders(accessToken)
	endpoints := []string{"/api/v1/keys", "/api/v1/api-keys"}
	var firstErr error

	for _, endpoint := range endpoints {
		payload, err := requestJSON(
			ctx,
			siteRecord,
			http.MethodPost,
			buildSiteURL(siteRecord.BaseURL, endpoint),
			requestBody,
			headers,
			account,
		)
		if err != nil {
			if shouldRetrySub2APIAfterRefresh(err, account) {
				refreshedToken, refreshErr := ensureFreshSub2APIAccessToken(ctx, siteRecord, account, true)
				if refreshErr == nil && stripBearerPrefix(refreshedToken) != stripBearerPrefix(accessToken) {
					headers = sub2APIUserHeaders(refreshedToken)
					payload, err = requestJSON(
						ctx,
						siteRecord,
						http.MethodPost,
						buildSiteURL(siteRecord.BaseURL, endpoint),
						requestBody,
						headers,
						account,
					)
					if err == nil {
						data, envelopeErr := unwrapSub2APIData(payload, endpoint)
						if envelopeErr == nil && siteTokenCreateSucceededFromAny(data) {
							return createdSiteTokenFromPayload(payload, groupKey, jsonString(requestBody["name"])), nil
						}
						if envelopeErr != nil && firstErr == nil {
							firstErr = envelopeErr
						}
					}
				}
			}
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if data, envelopeErr := unwrapSub2APIData(payload, endpoint); envelopeErr == nil {
			if siteTokenCreateSucceededFromAny(data) {
				return createdSiteTokenFromPayload(payload, groupKey, jsonString(requestBody["name"])), nil
			}
		} else {
			return nil, envelopeErr
		}
		if siteTokenCreateSucceeded(payload) {
			return createdSiteTokenFromPayload(payload, groupKey, jsonString(requestBody["name"])), nil
		}
		return nil, fmt.Errorf("%s", firstNonEmptyString(extractSiteResponseMessage(payload), "site token creation failed"))
	}

	if firstErr != nil {
		return nil, firstErr
	}
	return nil, fmt.Errorf("site token creation failed")
}

func buildManagedTokenCreatePayload(account *model.SiteAccount, groupKey string, name string) map[string]any {
	return map[string]any{
		"name":                 defaultSiteTokenCreateName(account, groupKey, name),
		"unlimited_quota":      true,
		"expired_time":         -1,
		"remain_quota":         0,
		"allow_ips":            "",
		"model_limits_enabled": false,
		"model_limits":         "",
		"group":                model.NormalizeSiteGroupKey(groupKey),
	}
}

func buildSub2APITokenCreatePayload(account *model.SiteAccount, groupKey string, name string) map[string]any {
	payload := map[string]any{
		"name": defaultSiteTokenCreateName(account, groupKey, name),
	}
	groupKey = model.NormalizeSiteGroupKey(groupKey)
	if groupID, err := strconv.Atoi(groupKey); err == nil && groupID > 0 {
		// Standard Sub2API uses group_id; Fengwind's current product-channel
		// variant uses channel_id. Both implementations ignore unknown fields.
		payload["group_id"] = groupID
		payload["channel_id"] = groupID
	}
	return payload
}

func defaultSiteTokenCreateName(account *model.SiteAccount, groupKey string, name string) string {
	if trimmed := strings.TrimSpace(name); trimmed != "" {
		return trimmed
	}

	groupPart := strings.TrimSpace(groupKey)
	groupPart = strings.NewReplacer("/", "-", "\\", "-", " ", "-", "\t", "-", "\n", "-").Replace(groupPart)
	groupPart = strings.Trim(groupPart, "-")
	if groupPart == "" {
		groupPart = model.SiteDefaultGroupKey
	}
	return fmt.Sprintf("octopus-%s-%d", groupPart, time.Now().Unix())
}

func siteTokenCreateSucceeded(payload map[string]any) bool {
	if payload == nil {
		return false
	}
	return siteTokenCreateSucceededFromAny(payload)
}

func siteTokenCreateSucceededFromAny(value any) bool {
	switch typed := value.(type) {
	case bool:
		return typed
	case map[string]any:
		if raw, ok := typed["success"]; ok {
			succeeded, recognized := parseSiteTokenSuccessFlag(raw)
			return recognized && succeeded && !siteTokenExplicitFailure(typed)
		}
		return !siteTokenResponseHasFailure(typed)
	default:
		return false
	}
}

func parseSiteTokenSuccessFlag(value any) (bool, bool) {
	switch typed := value.(type) {
	case bool:
		return typed, true
	case float64:
		return typed != 0, true
	case int:
		return typed != 0, true
	case int64:
		return typed != 0, true
	case string:
		switch strings.ToLower(strings.TrimSpace(typed)) {
		case "1", "true", "ok", "success":
			return true, true
		case "0", "false", "fail", "failed", "failure", "error":
			return false, true
		default:
			return false, false
		}
	default:
		return false, false
	}
}

func siteTokenResponseHasFailure(value any) bool {
	switch typed := value.(type) {
	case string:
		return siteTokenFailureText(typed)
	case []any:
		for _, item := range typed {
			if siteTokenResponseHasFailure(item) {
				return true
			}
		}
	case []map[string]any:
		for _, item := range typed {
			if siteTokenResponseHasFailure(item) {
				return true
			}
		}
	case map[string]any:
		if raw, ok := typed["success"]; ok {
			if succeeded, recognized := parseSiteTokenSuccessFlag(raw); !recognized || !succeeded {
				return true
			}
		}
		if siteTokenExplicitFailure(typed) {
			return true
		}
		for _, key := range []string{"message", "msg"} {
			if raw, ok := typed[key]; ok {
				if text, ok := raw.(string); ok && siteTokenFailureStatusText(text) {
					return true
				}
			}
		}
		for _, key := range []string{"data", "result", "item", "items", "list", "records", "rows", "payload"} {
			if nested, ok := typed[key]; ok && siteTokenResponseHasFailure(nested) {
				return true
			}
		}
	}
	return false
}

func siteTokenExplicitFailure(payload map[string]any) bool {
	for _, key := range []string{"code", "error_code", "errorCode"} {
		if raw, ok := payload[key]; ok {
			code, recognized := parseSiteTokenResponseCode(raw)
			if !recognized || code != 0 {
				return true
			}
		}
	}
	if raw, ok := payload["errors"]; ok && siteTokenErrorsValue(raw) {
		return true
	}
	if raw, ok := payload["status"]; ok && siteTokenStatusFailure(raw) {
		return true
	}
	if raw, ok := payload["error"]; ok && siteTokenErrorValue(raw) {
		return true
	}
	return false
}

func parseSiteTokenResponseCode(value any) (int64, bool) {
	switch typed := value.(type) {
	case int:
		return int64(typed), true
	case int64:
		return typed, true
	case float64:
		code := int64(typed)
		if float64(code) != typed {
			return 0, false
		}
		return code, true
	case string:
		trimmed := strings.TrimSpace(typed)
		if trimmed == "" {
			return 0, false
		}
		code, err := strconv.ParseInt(trimmed, 10, 64)
		return code, err == nil
	default:
		return 0, false
	}
}

func siteTokenErrorsValue(value any) bool {
	switch typed := value.(type) {
	case nil:
		return false
	case bool:
		return typed
	case string:
		return strings.TrimSpace(typed) != ""
	case []any:
		return len(typed) > 0
	case []map[string]any:
		return len(typed) > 0
	case map[string]any:
		return len(typed) > 0
	default:
		return true
	}
}

func siteTokenStatusFailure(value any) bool {
	switch typed := value.(type) {
	case bool:
		return !typed
	case float64:
		return typed == 0 || typed >= 400
	case int:
		return typed == 0 || typed >= 400
	case int64:
		return typed == 0 || typed >= 400
	case string:
		return siteTokenFailureStatusText(typed)
	default:
		return false
	}
}

func siteTokenErrorValue(value any) bool {
	switch typed := value.(type) {
	case nil:
		return false
	case bool:
		return typed
	case string:
		trimmed := strings.TrimSpace(strings.ToLower(typed))
		return trimmed != "" && trimmed != "none" && trimmed != "null" && trimmed != "nil"
	default:
		return true
	}
}

func siteTokenFailureText(value string) bool {
	lowered := strings.ToLower(strings.TrimSpace(value))
	if lowered == "" {
		return false
	}
	switch lowered {
	case "0", "false", "error", "fail", "failed", "failure", "unauthorized", "forbidden", "not found", "not_found", "not-found", "denied", "invalid", "expired", "disabled", "inactive", "success", "ok", "true", "message", "msg", "status", "result", "data", "payload", "item", "items", "list", "records", "rows":
		return true
	}
	for _, prefix := range []string{"error:", "fail:", "failed:", "failure:", "unauthorized:", "forbidden:"} {
		if strings.HasPrefix(lowered, prefix) {
			return true
		}
	}
	return strings.ContainsAny(lowered, " \t\r\n") && siteTokenFailureStatusText(lowered)
}

func siteTokenFailureStatusText(value string) bool {
	lowered := strings.ToLower(strings.TrimSpace(value))
	if lowered == "" {
		return false
	}
	switch lowered {
	case "0", "false", "error", "fail", "failed", "failure", "unauthorized", "forbidden", "not found", "not_found", "not-found", "denied", "invalid", "expired", "disabled", "inactive":
		return true
	}
	normalized := strings.NewReplacer("_", " ", "-", " ").Replace(lowered)
	for _, marker := range []string{"unauthorized", "forbidden", "not found", "failed", "failure", "error", "denied", "invalid token", "permission denied", "access denied", "expired"} {
		if strings.Contains(normalized, marker) {
			return true
		}
	}
	return false
}

func slicesCompactInts(values []int) []int {
	if len(values) == 0 {
		return nil
	}
	seen := make(map[int]struct{}, len(values))
	result := make([]int, 0, len(values))
	for _, value := range values {
		if value < 0 {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	return result
}

func createdSiteTokenFromPayload(payload any, groupKey string, name string) *model.SiteToken {
	if payloadMap, ok := payload.(map[string]any); ok && !siteTokenCreateSucceededFromAny(payloadMap) {
		return nil
	}
	tokenValue := extractSiteTokenValueFromPayload(payload)
	if tokenValue == "" || model.IsMaskedSiteTokenValue(tokenValue) {
		return nil
	}

	groupKey = model.NormalizeSiteGroupKey(groupKey)
	name = strings.TrimSpace(name)
	if name == "" {
		name = "created"
	}
	return &model.SiteToken{
		Name:        name,
		Token:       tokenValue,
		ValueStatus: model.SiteTokenValueStatusReady,
		GroupKey:    groupKey,
		GroupName:   model.NormalizeSiteGroupName(groupKey, groupKey),
		Enabled:     true,
		Source:      siteTokenSourceCreated,
	}
}

func extractSiteTokenValueFromPayload(value any) string {
	return extractSiteTokenValue(value, false)
}

func extractSiteTokenValue(value any, allowDirectString bool) string {
	switch typed := value.(type) {
	case string:
		if !allowDirectString {
			return ""
		}
		return validSiteTokenCandidate(typed)
	case []any:
		for _, item := range typed {
			if candidate := extractSiteTokenValue(item, false); candidate != "" {
				return candidate
			}
		}
		return ""
	case []map[string]any:
		for _, item := range typed {
			if candidate := extractSiteTokenValue(item, false); candidate != "" {
				return candidate
			}
		}
		return ""
	case map[string]any:
		for _, key := range []string{"key", "token", "api_key", "apiKey", "channel_key", "channelKey"} {
			if candidate := siteTokenStringValue(typed[key]); candidate != "" {
				return candidate
			}
		}
		for _, key := range []string{"data", "result", "item", "items", "list", "records", "rows", "payload"} {
			if nested, ok := typed[key]; ok {
				allowScalar := key == "data" || key == "result" || key == "payload"
				if candidate := extractSiteTokenValue(nested, allowScalar); candidate != "" {
					return candidate
				}
			}
		}
	}
	return ""
}

func siteTokenStringValue(value any) string {
	candidate, ok := value.(string)
	if !ok {
		return ""
	}
	return validSiteTokenCandidate(candidate)
}

func validSiteTokenCandidate(value string) string {
	candidate := strings.TrimSpace(value)
	if candidate == "" || model.IsMaskedSiteTokenValue(candidate) || siteTokenFailureText(candidate) {
		return ""
	}
	return candidate
}
