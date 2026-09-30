package sitesync

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/bestruirui/octopus/internal/db"
	"github.com/bestruirui/octopus/internal/model"
)

const sub2APIAccessTokenRefreshLead = 5 * time.Minute

var sub2APIRefreshLocks sync.Map

type sub2APIRefreshLock struct {
	permit chan struct{}
}

func newSub2APIRefreshLock() *sub2APIRefreshLock {
	lock := &sub2APIRefreshLock{permit: make(chan struct{}, 1)}
	lock.permit <- struct{}{}
	return lock
}

func (lock *sub2APIRefreshLock) acquire(ctx context.Context) error {
	select {
	case <-lock.permit:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (lock *sub2APIRefreshLock) release() {
	lock.permit <- struct{}{}
}

type sub2APIRefreshedCredentials struct {
	AccessToken    string
	RefreshToken   string
	TokenExpiresAt int64
}

func ensureFreshSub2APIAccessToken(ctx context.Context, siteRecord *model.Site, account *model.SiteAccount, forceRefresh bool) (string, error) {
	if account == nil {
		return "", fmt.Errorf("site account is nil")
	}

	accessToken := stripBearerPrefix(account.AccessToken)
	refreshToken := strings.TrimSpace(account.RefreshToken)
	if accessToken == "" && refreshToken == "" {
		return "", newAccessTokenRequiredError()
	}
	if refreshToken == "" {
		return accessToken, nil
	}
	if !forceRefresh && accessToken != "" && !shouldProactivelyRefreshSub2API(account) {
		return accessToken, nil
	}

	if account.ID > 0 {
		lock, _ := sub2APIRefreshLocks.LoadOrStore(account.ID, newSub2APIRefreshLock())
		refreshLock := lock.(*sub2APIRefreshLock)
		if err := refreshLock.acquire(ctx); err != nil {
			return "", err
		}
		defer refreshLock.release()

		var persisted model.SiteAccount
		if err := db.GetDB().WithContext(ctx).Select("id", "access_token", "refresh_token", "token_expires_at").First(&persisted, account.ID).Error; err != nil {
			return "", fmt.Errorf("failed to load sub2api session: %w", err)
		}
		if persisted.AccessToken != account.AccessToken || persisted.RefreshToken != refreshToken || persisted.TokenExpiresAt != account.TokenExpiresAt {
			account.AccessToken = persisted.AccessToken
			account.RefreshToken = persisted.RefreshToken
			account.TokenExpiresAt = persisted.TokenExpiresAt
			if persistedAccessToken := stripBearerPrefix(persisted.AccessToken); persistedAccessToken != "" {
				return persistedAccessToken, nil
			}
			return "", newAccessTokenRequiredError()
		}
	}

	refreshed, err := refreshSub2APIManagedSession(ctx, siteRecord, account)
	if err != nil {
		if forceRefresh || accessToken == "" || (account.TokenExpiresAt > 0 && account.TokenExpiresAt <= time.Now().UnixMilli()) {
			return "", err
		}
		return accessToken, nil
	}
	return refreshed, nil
}

func shouldProactivelyRefreshSub2API(account *model.SiteAccount) bool {
	if account == nil {
		return false
	}
	if strings.TrimSpace(account.RefreshToken) == "" {
		return false
	}
	if account.TokenExpiresAt <= 0 {
		return false
	}
	return time.Until(time.UnixMilli(account.TokenExpiresAt)) <= sub2APIAccessTokenRefreshLead
}

func shouldRetrySub2APIAfterRefresh(err error, account *model.SiteAccount) bool {
	if err == nil || account == nil || strings.TrimSpace(account.RefreshToken) == "" {
		return false
	}
	text := strings.ToLower(strings.TrimSpace(err.Error()))
	if text == "" {
		return false
	}
	return strings.Contains(text, "http 401") ||
		strings.Contains(text, "http 403") ||
		strings.Contains(text, "unauthorized") ||
		strings.Contains(text, "forbidden") ||
		strings.Contains(text, "expired") ||
		strings.Contains(text, "invalid token") ||
		strings.Contains(text, "access token")
}

func refreshSub2APIManagedSession(ctx context.Context, siteRecord *model.Site, account *model.SiteAccount) (string, error) {
	if siteRecord == nil || account == nil {
		return "", fmt.Errorf("site or account is nil")
	}
	refreshToken := strings.TrimSpace(account.RefreshToken)
	if refreshToken == "" {
		return "", fmt.Errorf("sub2api managed refresh token missing")
	}

	headers := map[string]string{
		"Content-Type":             "application/json",
		sub2APIUserUIRequestHeader: "1",
	}
	// The public refresh endpoint authenticates using the refresh_token body only.
	refreshSite := *siteRecord
	refreshSite.CustomHeader = nil
	for _, header := range siteRecord.CustomHeader {
		if !strings.EqualFold(strings.TrimSpace(header.HeaderKey), "Authorization") {
			refreshSite.CustomHeader = append(refreshSite.CustomHeader, header)
		}
	}

	payload, err := requestJSON(
		ctx,
		&refreshSite,
		"POST",
		buildSiteURL(siteRecord.BaseURL, "/api/v1/auth/refresh"),
		map[string]any{"refresh_token": refreshToken},
		headers,
		account,
	)
	if err != nil {
		return "", fmt.Errorf("sub2api token refresh request failed: %w", err)
	}
	if _, err := unwrapSub2APIData(payload, "/api/v1/auth/refresh"); err != nil {
		return "", fmt.Errorf("sub2api token refresh failed: %w", err)
	}

	refreshed, ok := parseSub2APIRefreshPayload(payload)
	if !ok {
		return "", fmt.Errorf("sub2api token refresh failed")
	}

	if account.ID > 0 {
		result := db.GetDB().WithContext(ctx).
			Model(&model.SiteAccount{}).
			Where("id = ? AND access_token = ? AND refresh_token = ? AND token_expires_at = ?", account.ID, account.AccessToken, refreshToken, account.TokenExpiresAt).
			Updates(map[string]any{
				"access_token":     refreshed.AccessToken,
				"refresh_token":    refreshed.RefreshToken,
				"token_expires_at": refreshed.TokenExpiresAt,
			})
		if result.Error != nil {
			return "", fmt.Errorf("failed to persist sub2api refreshed session: %w", result.Error)
		}
		if result.RowsAffected != 1 {
			// Preserve concurrent access/expiry edits. The consumed refresh token
			// must still be rotated unless another writer already replaced it.
			if err := db.GetDB().WithContext(ctx).
				Model(&model.SiteAccount{}).
				Where("id = ? AND refresh_token = ?", account.ID, refreshToken).
				Update("refresh_token", refreshed.RefreshToken).Error; err != nil {
				return "", fmt.Errorf("failed to persist sub2api refreshed session: %w", err)
			}
			var current model.SiteAccount
			if err := db.GetDB().WithContext(ctx).Select("id", "access_token", "refresh_token", "token_expires_at").First(&current, account.ID).Error; err != nil {
				return "", fmt.Errorf("failed to load sub2api refreshed session: %w", err)
			}
			account.AccessToken = current.AccessToken
			account.RefreshToken = current.RefreshToken
			account.TokenExpiresAt = current.TokenExpiresAt
			if accessToken := stripBearerPrefix(current.AccessToken); accessToken != "" {
				return accessToken, nil
			}
			return "", newAccessTokenRequiredError()
		}
	}
	account.AccessToken = refreshed.AccessToken
	account.RefreshToken = refreshed.RefreshToken
	account.TokenExpiresAt = refreshed.TokenExpiresAt

	return refreshed.AccessToken, nil
}

func parseSub2APIRefreshPayload(payload map[string]any) (sub2APIRefreshedCredentials, bool) {
	if payload == nil {
		return sub2APIRefreshedCredentials{}, false
	}

	if rawCode, ok := payload["code"]; ok {
		code := anyToInt64(rawCode)
		if code != 0 {
			return sub2APIRefreshedCredentials{}, false
		}
	}

	data, ok := payload["data"].(map[string]any)
	if !ok {
		return sub2APIRefreshedCredentials{}, false
	}

	accessToken := stripBearerPrefix(jsonString(data["access_token"]))
	refreshToken := strings.TrimSpace(jsonString(data["refresh_token"]))
	expiresInSeconds := anyToInt64(data["expires_in"])
	if accessToken == "" || refreshToken == "" || expiresInSeconds <= 0 {
		return sub2APIRefreshedCredentials{}, false
	}

	return sub2APIRefreshedCredentials{
		AccessToken:    accessToken,
		RefreshToken:   refreshToken,
		TokenExpiresAt: time.Now().Add(time.Duration(expiresInSeconds) * time.Second).UnixMilli(),
	}, true
}

func anyToInt64(value any) int64 {
	switch typed := value.(type) {
	case int:
		return int64(typed)
	case int64:
		return typed
	case float64:
		return int64(typed)
	case string:
		trimmed := strings.TrimSpace(typed)
		if trimmed == "" {
			return 0
		}
		var parsed int64
		if _, err := fmt.Sscanf(trimmed, "%d", &parsed); err == nil {
			return parsed
		}
	}
	return 0
}
