package op

import (
	"context"
	"testing"

	dbpkg "github.com/bestruirui/octopus/internal/db"
	"github.com/bestruirui/octopus/internal/model"
)

// countRowsForSiteAccount counts persisted child rows for an account.
func countRowsForSiteAccount(t *testing.T, ctx context.Context, accountID int) (tokens, groups, models, bindings int64) {
	t.Helper()
	db := dbpkg.GetDB().WithContext(ctx)
	if err := db.Model(&model.SiteToken{}).Where("site_account_id = ?", accountID).Count(&tokens).Error; err != nil {
		t.Fatalf("count site tokens failed: %v", err)
	}
	if err := db.Model(&model.SiteUserGroup{}).Where("site_account_id = ?", accountID).Count(&groups).Error; err != nil {
		t.Fatalf("count site user groups failed: %v", err)
	}
	if err := db.Model(&model.SiteModel{}).Where("site_account_id = ?", accountID).Count(&models).Error; err != nil {
		t.Fatalf("count site models failed: %v", err)
	}
	if err := db.Model(&model.SiteChannelBinding{}).Where("site_account_id = ?", accountID).Count(&bindings).Error; err != nil {
		t.Fatalf("count site channel bindings failed: %v", err)
	}
	return tokens, groups, models, bindings
}

// TestSiteCreateDoesNotPersistNestedAccounts verifies that accounts nested in
// a client site payload are never cascade-inserted. Without the guard a
// Cloudflare site could be created together with username/password accounts,
// enabled check-in and Anthropic-routed models, bypassing the platform,
// credential and route invariants enforced by SiteAccountCreate.
func TestSiteCreateDoesNotPersistNestedAccounts(t *testing.T) {
	ctx := setupSiteOpTestDB(t)

	site := &model.Site{
		Name:     "cloudflare-nested-accounts",
		Platform: model.SitePlatformCloudflare,
		BaseURL:  "https://api.cloudflare.com/client/v4/accounts/nested-accounts-site/ai",
		Accounts: []model.SiteAccount{
			{
				Name:           "bypass",
				CredentialType: model.SiteCredentialTypeUsernamePassword,
				Username:       "user",
				Password:       "pass",
				AutoCheckin:    true,
				RandomCheckin:  true,
				Models: []model.SiteModel{
					{
						GroupKey:        "default",
						ModelName:       "@cf/meta/llama-3.2-3b-instruct",
						RouteType:       model.SiteModelRouteTypeAnthropic,
						RouteRawPayload: `{"enable_groups":["hidden"]}`,
					},
				},
				ChannelBindings: []model.SiteChannelBinding{
					{GroupKey: "default", ChannelID: 12345},
				},
			},
		},
	}
	if err := SiteCreate(site, ctx); err != nil {
		t.Fatalf("SiteCreate failed: %v", err)
	}
	if len(site.Accounts) != 0 {
		t.Fatalf("returned site claims %d persisted accounts, want 0", len(site.Accounts))
	}

	var accountCount int64
	if err := dbpkg.GetDB().WithContext(ctx).Model(&model.SiteAccount{}).
		Where("site_id = ?", site.ID).Count(&accountCount).Error; err != nil {
		t.Fatalf("count site accounts failed: %v", err)
	}
	if accountCount != 0 {
		t.Fatalf("expected 0 persisted accounts for the new site, got %d", accountCount)
	}

	var modelCount, bindingCount int64
	if err := dbpkg.GetDB().WithContext(ctx).Model(&model.SiteModel{}).Count(&modelCount).Error; err != nil {
		t.Fatalf("count site models failed: %v", err)
	}
	if err := dbpkg.GetDB().WithContext(ctx).Model(&model.SiteChannelBinding{}).Count(&bindingCount).Error; err != nil {
		t.Fatalf("count site channel bindings failed: %v", err)
	}
	if modelCount != 0 || bindingCount != 0 {
		t.Fatalf("nested account children leaked into db: models=%d bindings=%d", modelCount, bindingCount)
	}
}

// TestSiteCreateDisabledDoesNotPersistNestedAccounts covers the disabled-site
// transaction branch of SiteCreate with the same nested-account guarantee.
func TestSiteCreateDisabledDoesNotPersistNestedAccounts(t *testing.T) {
	ctx := setupSiteOpTestDB(t)

	site := &model.Site{
		Name:       "cloudflare-nested-disabled",
		Platform:   model.SitePlatformCloudflare,
		BaseURL:    "https://api.cloudflare.com/client/v4/accounts/nested-disabled-site/ai",
		Enabled:    false,
		EnabledSet: true,
	}
	site.Accounts = []model.SiteAccount{
		{
			Name:           "bypass-disabled",
			CredentialType: model.SiteCredentialTypeUsernamePassword,
			Username:       "user",
			Password:       "pass",
		},
	}
	if err := SiteCreate(site, ctx); err != nil {
		t.Fatalf("SiteCreate failed: %v", err)
	}

	var accountCount int64
	if err := dbpkg.GetDB().WithContext(ctx).Model(&model.SiteAccount{}).
		Where("site_id = ?", site.ID).Count(&accountCount).Error; err != nil {
		t.Fatalf("count site accounts failed: %v", err)
	}
	if accountCount != 0 {
		t.Fatalf("expected 0 persisted accounts for the disabled site, got %d", accountCount)
	}
}

// TestSiteAccountCreateDoesNotPersistNestedChildren verifies that tokens,
// user groups, models and channel bindings nested in a client account payload
// are never cascade-inserted. Without the guard a valid Cloudflare account
// could smuggle Anthropic-routed models with raw route metadata and bindings
// that bypass ownership, platform and route validation.
func TestSiteAccountCreateDoesNotPersistNestedChildren(t *testing.T) {
	ctx := setupSiteOpTestDB(t)

	site := &model.Site{
		Name:     "cloudflare-nested-children",
		Platform: model.SitePlatformCloudflare,
		BaseURL:  "https://api.cloudflare.com/client/v4/accounts/nested-children-site/ai",
	}
	if err := SiteCreate(site, ctx); err != nil {
		t.Fatalf("SiteCreate failed: %v", err)
	}

	account := &model.SiteAccount{
		SiteID:         site.ID,
		Name:           "direct",
		CredentialType: model.SiteCredentialTypeAccessToken,
		AccessToken:    "cf-token",
		Tokens: []model.SiteToken{
			{Name: "smuggled", Token: "sk-smuggled", GroupKey: "default"},
		},
		UserGroups: []model.SiteUserGroup{
			{GroupKey: "vip", Name: "VIP"},
		},
		Models: []model.SiteModel{
			{
				GroupKey:        "default",
				ModelName:       "@cf/openai/gpt-oss-20b",
				RouteType:       model.SiteModelRouteTypeAnthropic,
				RouteRawPayload: `{"enable_groups":["hidden"]}`,
				ManualOverride:  true,
			},
		},
		ChannelBindings: []model.SiteChannelBinding{
			{GroupKey: "default", ChannelID: 99999},
		},
	}
	if err := SiteAccountCreate(account, ctx); err != nil {
		t.Fatalf("SiteAccountCreate failed: %v", err)
	}

	tokens, groups, models, bindings := countRowsForSiteAccount(t, ctx, account.ID)
	if tokens != 0 || groups != 0 || models != 0 || bindings != 0 {
		t.Fatalf("nested children leaked into db: tokens=%d groups=%d models=%d bindings=%d", tokens, groups, models, bindings)
	}

	// The account itself must still be persisted normally.
	var accountCount int64
	if err := dbpkg.GetDB().WithContext(ctx).Model(&model.SiteAccount{}).
		Where("site_id = ?", site.ID).Count(&accountCount).Error; err != nil {
		t.Fatalf("count site accounts failed: %v", err)
	}
	if accountCount != 1 {
		t.Fatalf("expected the account itself to be persisted, got %d rows", accountCount)
	}
}
