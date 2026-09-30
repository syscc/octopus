package sitesync

import (
	"context"
	"crypto/sha256"
	"strings"
	"sync"
	"time"

	"github.com/bestruirui/octopus/internal/db"
	"github.com/bestruirui/octopus/internal/model"
	"github.com/bestruirui/octopus/internal/utils/log"
)

type sub2APIRefreshRetry struct {
	credentials [32]byte
	until       time.Time
}

var sub2APIRefreshRetryAfter sync.Map

const sub2APIRefreshRetryDelay = 5 * time.Minute
const sub2APIRefreshAccountTimeout = 30 * time.Second

// RefreshDueSub2APISessions keeps managed sessions alive independently of model sync.
func RefreshDueSub2APISessions(ctx context.Context) (int, error) {
	return refreshDueSub2APISessions(ctx, sub2APIRefreshAccountTimeout)
}

func refreshDueSub2APISessions(ctx context.Context, accountTimeout time.Duration) (int, error) {
	sub2APIRefreshRetryAfter.Range(func(key, value any) bool {
		if !time.Now().Before(value.(sub2APIRefreshRetry).until) {
			sub2APIRefreshRetryAfter.Delete(key)
		}
		return true
	})

	var sites []model.Site
	if err := db.GetDB().WithContext(ctx).
		Where("platform = ? AND enabled = ?", model.SitePlatformSub2API, true).
		Preload("Accounts", "enabled = ? AND credential_type = ?", true, model.SiteCredentialTypeAccessToken).
		Find(&sites).Error; err != nil {
		return 0, err
	}

	refreshed := 0
	for i := range sites {
		for j := range sites[i].Accounts {
			if err := ctx.Err(); err != nil {
				return refreshed, err
			}
			account := &sites[i].Accounts[j]
			refreshToken := strings.TrimSpace(account.RefreshToken)
			if refreshToken == "" {
				continue
			}
			if account.TokenExpiresAt > 0 && !shouldProactivelyRefreshSub2API(account) {
				continue
			}
			credentials := sha256.Sum256([]byte(account.AccessToken + "\x00" + refreshToken))
			if retry, ok := sub2APIRefreshRetryAfter.Load(account.ID); ok {
				state := retry.(sub2APIRefreshRetry)
				if state.credentials == credentials && time.Now().Before(state.until) {
					continue
				}
				sub2APIRefreshRetryAfter.Delete(account.ID)
			}
			accountCtx, cancel := context.WithTimeout(ctx, accountTimeout)
			_, err := ensureFreshSub2APIAccessToken(accountCtx, &sites[i], account, true)
			cancel()
			if err != nil {
				if ctx.Err() != nil {
					return refreshed, ctx.Err()
				}
				if account.TokenExpiresAt <= time.Now().UnixMilli() {
					sub2APIRefreshRetryAfter.Store(account.ID, sub2APIRefreshRetry{
						credentials: credentials,
						until:       time.Now().Add(sub2APIRefreshRetryDelay),
					})
				}
				log.Warnf("sub2api session refresh failed (account=%d): %s", account.ID, sanitizeSiteStatusMessage(err))
				continue
			}
			sub2APIRefreshRetryAfter.Delete(account.ID)
			refreshed++
		}
	}
	return refreshed, nil
}
