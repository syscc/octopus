package op

import (
	"strings"
	"testing"
	"time"

	dbpkg "github.com/bestruirui/octopus/internal/db"
	"github.com/bestruirui/octopus/internal/model"
)

func TestCloudflareManualModelsOnlyAllowOpenAIChat(t *testing.T) {
	ctx := setupSiteOpTestDB(t)
	site := &model.Site{
		Name:     "cloudflare-route-site",
		Platform: model.SitePlatformCloudflare,
		BaseURL:  "https://api.cloudflare.com/client/v4/accounts/account-id/ai",
		Enabled:  true,
	}
	if err := SiteCreate(site, ctx); err != nil {
		t.Fatalf("SiteCreate failed: %v", err)
	}
	account := &model.SiteAccount{
		SiteID:         site.ID,
		Name:           "cloudflare-account",
		CredentialType: model.SiteCredentialTypeAPIKey,
		APIKey:         "cf-api-token",
		Enabled:        true,
		AutoSync:       false,
		AutoCheckin:    false,
	}
	if err := SiteAccountCreate(account, ctx); err != nil {
		t.Fatalf("SiteAccountCreate failed: %v", err)
	}

	anthropicRequest := &model.SiteManualModelAddRequest{
		GroupKey: model.SiteDefaultGroupKey,
		Models: []model.SiteManualModelAddEntry{{
			ModelName: "@cf/test/not-chat",
			RouteType: model.SiteModelRouteTypeAnthropic,
		}},
	}
	if err := SiteManualModelsAdd(site.ID, account.ID, anthropicRequest, ctx); err == nil || !strings.Contains(err.Error(), "unsupported route type") || !strings.Contains(err.Error(), "only supports the openai chat route") {
		t.Fatalf("expected non-OpenAI route rejection, got %v", err)
	}

	chatRequest := &model.SiteManualModelAddRequest{
		GroupKey: model.SiteDefaultGroupKey,
		Models: []model.SiteManualModelAddEntry{{
			ModelName: "@cf/meta/llama-3.1-8b-instruct",
			RouteType: model.SiteModelRouteTypeOpenAIChat,
		}},
	}
	if err := SiteManualModelsAdd(site.ID, account.ID, chatRequest, ctx); err != nil {
		t.Fatalf("expected OpenAI Chat route to be accepted: %v", err)
	}
	if err := SiteModelRouteUpdate(
		account.ID,
		model.SiteDefaultGroupKey,
		"@cf/meta/llama-3.1-8b-instruct",
		model.SiteModelRouteTypeOpenAIResponse,
		model.SiteModelRouteSourceManualOverride,
		true,
		"",
		ctx,
	); err == nil || !strings.Contains(err.Error(), "unsupported route type") || !strings.Contains(err.Error(), "only supports the openai chat route") {
		t.Fatalf("expected OpenAI Responses route rejection, got %v", err)
	}
}

func TestCloudflareBatchRouteUpdateValidatesBeforeWriting(t *testing.T) {
	ctx := setupSiteOpTestDB(t)
	site := &model.Site{
		Name:     "cloudflare-batch-route-site",
		Platform: model.SitePlatformCloudflare,
		BaseURL:  "https://api.cloudflare.com/client/v4/accounts/account-id/ai",
		Enabled:  true,
	}
	if err := SiteCreate(site, ctx); err != nil {
		t.Fatalf("SiteCreate failed: %v", err)
	}
	account := &model.SiteAccount{
		SiteID:         site.ID,
		Name:           "cloudflare-batch-route-account",
		CredentialType: model.SiteCredentialTypeAPIKey,
		APIKey:         "cf-api-token",
		Enabled:        true,
	}
	if err := SiteAccountCreate(account, ctx); err != nil {
		t.Fatalf("SiteAccountCreate failed: %v", err)
	}
	rows := []model.SiteModel{
		{SiteAccountID: account.ID, GroupKey: model.SiteDefaultGroupKey, ModelName: "model-a", RouteType: model.SiteModelRouteTypeOpenAIChat, RouteSource: model.SiteModelRouteSourceSyncInferred},
		{SiteAccountID: account.ID, GroupKey: model.SiteDefaultGroupKey, ModelName: "model-b", RouteType: model.SiteModelRouteTypeOpenAIChat, RouteSource: model.SiteModelRouteSourceSyncInferred},
	}
	if err := dbpkg.GetDB().WithContext(ctx).Create(&rows).Error; err != nil {
		t.Fatalf("create site models failed: %v", err)
	}

	err := SiteModelRoutesUpdate(site.ID, account.ID, []model.SiteModelRouteUpdateRequest{
		{GroupKey: model.SiteDefaultGroupKey, ModelName: "model-a", RouteType: model.SiteModelRouteTypeOpenAIChat, RouteRawPayload: "would-be-written"},
		{GroupKey: model.SiteDefaultGroupKey, ModelName: "model-b", RouteType: model.SiteModelRouteTypeAnthropic},
	}, ctx)
	if err == nil || !strings.Contains(err.Error(), "only supports the openai chat route") {
		t.Fatalf("expected batch validation error, got %v", err)
	}

	var reloaded model.SiteModel
	if err := dbpkg.GetDB().WithContext(ctx).First(&reloaded, rows[0].ID).Error; err != nil {
		t.Fatalf("reload first site model failed: %v", err)
	}
	if reloaded.ManualOverride || reloaded.RouteSource != model.SiteModelRouteSourceSyncInferred || reloaded.RouteRawPayload != "" {
		t.Fatalf("expected first item to remain untouched after batch rejection, got %+v", reloaded)
	}
}

func TestSiteUpdateCanonicalizesPersistedCloudflareRouteConfiguration(t *testing.T) {
	ctx := setupSiteOpTestDB(t)
	checkinURL := "https://example.com/signin"
	site := &model.Site{
		Name:     "cloudflare-legacy-route-site",
		Platform: model.SitePlatformCloudflare,
		BaseURL:  "https://api.cloudflare.com/client/v4/accounts/account-id/ai",
		Enabled:  true,
	}
	if err := SiteCreate(site, ctx); err != nil {
		t.Fatalf("SiteCreate failed: %v", err)
	}
	legacy := &model.Site{
		DefaultRouteType: model.SiteModelRouteTypeAnthropic,
		RouteBaseURLs: []model.SiteRouteBaseURL{{
			RouteType: model.SiteModelRouteTypeAnthropic,
			BaseURL:   "https://example.com/anthropic/v1",
		}},
		ExternalCheckinURL: &checkinURL,
	}
	if err := dbpkg.GetDB().WithContext(ctx).
		Model(&model.Site{}).
		Where("id = ?", site.ID).
		Select("default_route_type", "route_base_urls", "external_checkin_url").
		Updates(legacy).Error; err != nil {
		t.Fatalf("seed legacy Cloudflare route fields failed: %v", err)
	}

	name := "cloudflare-legacy-route-site-renamed"
	updated, err := SiteUpdate(&model.SiteUpdateRequest{ID: site.ID, Name: &name}, ctx)
	if err != nil {
		t.Fatalf("SiteUpdate failed: %v", err)
	}
	if updated.DefaultRouteType != model.SiteModelRouteTypeOpenAIChat || len(updated.RouteBaseURLs) != 0 {
		t.Fatalf("expected Cloudflare route fields to be canonicalized, got route=%q overrides=%+v", updated.DefaultRouteType, updated.RouteBaseURLs)
	}
	if updated.ExternalCheckinURL != nil {
		t.Fatalf("expected Cloudflare external check-in URL to be cleared, got %q", *updated.ExternalCheckinURL)
	}
}

func TestCloudflareAccountConstraints(t *testing.T) {
	ctx := setupSiteOpTestDB(t)
	site := &model.Site{
		Name:     "cloudflare-account-constraints",
		Platform: model.SitePlatformCloudflare,
		BaseURL:  "https://api.cloudflare.com/client/v4/accounts/account-id/ai",
		Enabled:  true,
	}
	if err := SiteCreate(site, ctx); err != nil {
		t.Fatalf("SiteCreate failed: %v", err)
	}

	unsupported := &model.SiteAccount{
		SiteID:         site.ID,
		Name:           "unsupported-cloudflare-account",
		CredentialType: model.SiteCredentialTypeUsernamePassword,
		Username:       "user",
		Password:       "pass",
		Enabled:        true,
	}
	if err := SiteAccountCreate(unsupported, ctx); err == nil || !strings.Contains(err.Error(), "only supports access token or api key") {
		t.Fatalf("expected username/password credentials to be rejected, got %v", err)
	}

	account := &model.SiteAccount{
		SiteID:         site.ID,
		Name:           "cloudflare-api-key-account",
		CredentialType: model.SiteCredentialTypeAPIKey,
		APIKey:         "cf-api-token",
		Enabled:        true,
		AutoCheckin:    true,
		RandomCheckin:  true,
	}
	if err := SiteAccountCreate(account, ctx); err != nil {
		t.Fatalf("SiteAccountCreate failed: %v", err)
	}
	reloaded, err := SiteAccountGet(account.ID, ctx)
	if err != nil {
		t.Fatalf("SiteAccountGet failed: %v", err)
	}
	if reloaded.AutoCheckin || reloaded.RandomCheckin || reloaded.NextAutoCheckinAt != nil {
		t.Fatalf("expected Cloudflare check-in state to be disabled, got %+v", reloaded)
	}

	next := model.SiteCredentialTypeUsernamePassword
	username := "user"
	password := "pass"
	if _, err := SiteAccountUpdate(&model.SiteAccountUpdateRequest{
		ID:             account.ID,
		CredentialType: &next,
		Username:       &username,
		Password:       &password,
	}, ctx); err == nil || !strings.Contains(err.Error(), "only supports access token or api key") {
		t.Fatalf("expected account update to reject username/password credentials, got %v", err)
	}
}

func TestSiteUpdateRejectsCloudflareSwitchWithUsernamePasswordAccounts(t *testing.T) {
	ctx := setupSiteOpTestDB(t)
	site := &model.Site{
		Name:     "switch-source-site",
		Platform: model.SitePlatformNewAPI,
		BaseURL:  "https://example.com",
		Enabled:  true,
	}
	if err := SiteCreate(site, ctx); err != nil {
		t.Fatalf("SiteCreate failed: %v", err)
	}
	account := &model.SiteAccount{
		SiteID:         site.ID,
		Name:           "password-account",
		CredentialType: model.SiteCredentialTypeUsernamePassword,
		Username:       "user",
		Password:       "pass",
		Enabled:        true,
	}
	if err := SiteAccountCreate(account, ctx); err != nil {
		t.Fatalf("SiteAccountCreate failed: %v", err)
	}

	platform := model.SitePlatformCloudflare
	baseURL := "https://api.cloudflare.com/client/v4/accounts/account-id/ai"
	if _, err := SiteUpdate(&model.SiteUpdateRequest{ID: site.ID, Platform: &platform, BaseURL: &baseURL}, ctx); err == nil ||
		!strings.Contains(err.Error(), "cannot switch site to cloudflare workers ai") ||
		!strings.Contains(err.Error(), "username/password accounts exist") {
		t.Fatalf("expected explicit Cloudflare migration rejection, got %v", err)
	}
	reloaded, err := SiteGet(site.ID, ctx)
	if err != nil {
		t.Fatalf("SiteGet failed: %v", err)
	}
	if reloaded.Platform != model.SitePlatformNewAPI || reloaded.BaseURL != "https://example.com" {
		t.Fatalf("expected rejected switch to leave site unchanged, got platform=%q base=%q", reloaded.Platform, reloaded.BaseURL)
	}
}

func TestSiteUpdatePlatformOnlySwitchPersistsCanonicalCloudflareBaseURL(t *testing.T) {
	ctx := setupSiteOpTestDB(t)
	site := &model.Site{
		Name:     "platform-only-cloudflare-switch",
		Platform: model.SitePlatformAPI,
		BaseURL:  "https://API.Cloudflare.COM:443/CLIENT/V4/ACCOUNTS/CaseID/AI/V1/",
		Enabled:  true,
	}
	if err := SiteCreate(site, ctx); err != nil {
		t.Fatalf("SiteCreate failed: %v", err)
	}

	platform := model.SitePlatformCloudflare
	updated, err := SiteUpdate(&model.SiteUpdateRequest{ID: site.ID, Platform: &platform}, ctx)
	if err != nil {
		t.Fatalf("platform-only SiteUpdate failed: %v", err)
	}
	want := "https://api.cloudflare.com/client/v4/accounts/CaseID/ai/v1"
	if updated.BaseURL != want {
		t.Fatalf("expected canonical returned base URL %q, got %q", want, updated.BaseURL)
	}
	var persisted model.Site
	if err := dbpkg.GetDB().WithContext(ctx).First(&persisted, site.ID).Error; err != nil {
		t.Fatalf("reload site failed: %v", err)
	}
	if persisted.BaseURL != want {
		t.Fatalf("expected canonical persisted base URL %q, got %q", want, persisted.BaseURL)
	}
}

func TestSiteUpdateSwitchToCloudflareNormalizesAccountsAndModels(t *testing.T) {
	ctx := setupSiteOpTestDB(t)
	site := &model.Site{
		Name:     "compatible-switch-source",
		Platform: model.SitePlatformNewAPI,
		BaseURL:  "https://example.com",
		Enabled:  true,
	}
	if err := SiteCreate(site, ctx); err != nil {
		t.Fatalf("SiteCreate failed: %v", err)
	}
	nextCheckin := time.Unix(1711929600, 0)
	account := &model.SiteAccount{
		SiteID:            site.ID,
		Name:              "token-account",
		CredentialType:    model.SiteCredentialTypeAccessToken,
		AccessToken:       "access-token",
		Enabled:           true,
		AutoCheckin:       true,
		RandomCheckin:     true,
		NextAutoCheckinAt: &nextCheckin,
	}
	if err := SiteAccountCreate(account, ctx); err != nil {
		t.Fatalf("SiteAccountCreate failed: %v", err)
	}
	routeUpdatedAt := time.Unix(1711929000, 0)
	siteModel := &model.SiteModel{
		SiteAccountID:   account.ID,
		GroupKey:        model.SiteDefaultGroupKey,
		ModelName:       "legacy-model",
		RouteType:       model.SiteModelRouteTypeAnthropic,
		RouteSource:     model.SiteModelRouteSourceManualOverride,
		ManualOverride:  true,
		RouteRawPayload: "legacy-route",
		RouteUpdatedAt:  &routeUpdatedAt,
	}
	if err := dbpkg.GetDB().WithContext(ctx).Create(siteModel).Error; err != nil {
		t.Fatalf("create legacy model failed: %v", err)
	}

	platform := model.SitePlatformCloudflare
	baseURL := "https://api.cloudflare.com/client/v4/accounts/account-id/ai"
	updated, err := SiteUpdate(&model.SiteUpdateRequest{ID: site.ID, Platform: &platform, BaseURL: &baseURL}, ctx)
	if err != nil {
		t.Fatalf("SiteUpdate failed: %v", err)
	}
	if updated.Platform != model.SitePlatformCloudflare || updated.DefaultRouteType != model.SiteModelRouteTypeOpenAIChat {
		t.Fatalf("expected Cloudflare site canonicalization, got %+v", updated)
	}
	reloadedAccount, err := SiteAccountGet(account.ID, ctx)
	if err != nil {
		t.Fatalf("SiteAccountGet failed: %v", err)
	}
	if reloadedAccount.AutoCheckin || reloadedAccount.RandomCheckin || reloadedAccount.NextAutoCheckinAt != nil {
		t.Fatalf("expected check-in state to be cleared during switch, got %+v", reloadedAccount)
	}
	var reloadedModel model.SiteModel
	if err := dbpkg.GetDB().WithContext(ctx).First(&reloadedModel, siteModel.ID).Error; err != nil {
		t.Fatalf("reload model failed: %v", err)
	}
	if reloadedModel.RouteType != model.SiteModelRouteTypeOpenAIChat ||
		reloadedModel.RouteSource != model.SiteModelRouteSourceSyncInferred ||
		reloadedModel.ManualOverride || reloadedModel.RouteRawPayload != "" {
		t.Fatalf("expected model route to be normalized during switch, got %+v", reloadedModel)
	}
	if reloadedModel.RouteUpdatedAt == nil || !reloadedModel.RouteUpdatedAt.After(routeUpdatedAt) {
		t.Fatalf("expected route update timestamp to advance, got %v", reloadedModel.RouteUpdatedAt)
	}
}
