package op

import (
	"context"
	"strings"
	"testing"

	dbpkg "github.com/bestruirui/octopus/internal/db"
	"github.com/bestruirui/octopus/internal/model"
)

func createCloudflareRouteReviewSite(t *testing.T, ctx context.Context, siteName string, accountName string, accountPath string) (*model.Site, *model.SiteAccount, *model.SiteModel) {
	t.Helper()
	site := &model.Site{
		Name:     siteName,
		Platform: model.SitePlatformCloudflare,
		BaseURL:  "https://api.cloudflare.com/client/v4/accounts/" + accountPath + "/ai",
		Enabled:  true,
	}
	if err := SiteCreate(site, ctx); err != nil {
		t.Fatalf("SiteCreate failed: %v", err)
	}
	account := &model.SiteAccount{
		SiteID:         site.ID,
		Name:           accountName,
		CredentialType: model.SiteCredentialTypeAPIKey,
		APIKey:         "test-cloudflare-token",
		Enabled:        true,
	}
	if err := SiteAccountCreate(account, ctx); err != nil {
		t.Fatalf("SiteAccountCreate failed: %v", err)
	}
	siteModel := &model.SiteModel{
		SiteAccountID: account.ID,
		GroupKey:      model.SiteDefaultGroupKey,
		ModelName:     "@cf/test/" + accountPath,
		Source:        "sync",
		RouteType:     model.SiteModelRouteTypeOpenAIChat,
		RouteSource:   model.SiteModelRouteSourceSyncInferred,
	}
	if err := dbpkg.GetDB().WithContext(ctx).Create(siteModel).Error; err != nil {
		t.Fatalf("create site model failed: %v", err)
	}
	return site, account, siteModel
}

func TestSiteModelRoutesUpdateRejectsCrossSiteAccount(t *testing.T) {
	ctx := setupSiteOpTestDB(t)
	siteA, _, _ := createCloudflareRouteReviewSite(t, ctx, "route-owner-a", "route-account-a", "owner-a")
	_, accountB, modelB := createCloudflareRouteReviewSite(t, ctx, "route-owner-b", "route-account-b", "owner-b")

	err := SiteModelRoutesUpdate(siteA.ID, accountB.ID, []model.SiteModelRouteUpdateRequest{{
		GroupKey:  model.SiteDefaultGroupKey,
		ModelName: modelB.ModelName,
		RouteType: model.SiteModelRouteTypeOpenAIChat,
	}}, ctx)
	if err == nil || !strings.Contains(err.Error(), "site account not found") {
		t.Fatalf("expected cross-site account rejection, got %v", err)
	}

	var reloaded model.SiteModel
	if err := dbpkg.GetDB().WithContext(ctx).First(&reloaded, modelB.ID).Error; err != nil {
		t.Fatalf("reload site model failed: %v", err)
	}
	if reloaded.ManualOverride || reloaded.RouteSource != model.SiteModelRouteSourceSyncInferred {
		t.Fatalf("expected rejected cross-site update to leave model unchanged, got %+v", reloaded)
	}
}

func TestSiteModelsDisabledUpdateRejectsCrossSiteAccountAndMissingTarget(t *testing.T) {
	ctx := setupSiteOpTestDB(t)
	siteA, _, _ := createCloudflareRouteReviewSite(t, ctx, "disabled-owner-a", "disabled-account-a", "disabled-a")
	_, accountB, modelB := createCloudflareRouteReviewSite(t, ctx, "disabled-owner-b", "disabled-account-b", "disabled-b")

	err := SiteModelsDisabledUpdate(siteA.ID, accountB.ID, []model.SiteModelDisableUpdateRequest{{
		GroupKey:  model.SiteDefaultGroupKey,
		ModelName: modelB.ModelName,
		Disabled:  true,
	}}, ctx)
	if err == nil || !strings.Contains(err.Error(), "site account not found") {
		t.Fatalf("expected cross-site disabled update rejection, got %v", err)
	}
	var reloaded model.SiteModel
	if err := dbpkg.GetDB().WithContext(ctx).First(&reloaded, modelB.ID).Error; err != nil {
		t.Fatalf("reload site model failed: %v", err)
	}
	if reloaded.Disabled {
		t.Fatal("expected cross-site disabled update not to modify the model")
	}

	err = SiteModelsDisabledUpdate(siteA.ID, accountB.ID, []model.SiteModelDisableUpdateRequest{{
		GroupKey:  model.SiteDefaultGroupKey,
		ModelName: "missing-model",
		Disabled:  true,
	}}, ctx)
	if err == nil || !strings.Contains(err.Error(), "site account not found") {
		t.Fatalf("expected cross-site missing-model rejection, got %v", err)
	}
}

func TestSiteModelsDisabledUpdateRejectsMissingTargetAtomically(t *testing.T) {
	ctx := setupSiteOpTestDB(t)
	site, account, existing := createCloudflareRouteReviewSite(t, ctx, "disabled-missing-site", "disabled-missing-account", "disabled-missing")
	err := SiteModelsDisabledUpdate(site.ID, account.ID, []model.SiteModelDisableUpdateRequest{
		{GroupKey: model.SiteDefaultGroupKey, ModelName: existing.ModelName, Disabled: true},
		{GroupKey: model.SiteDefaultGroupKey, ModelName: "missing-model", Disabled: true},
	}, ctx)
	if err == nil || !strings.Contains(err.Error(), "site model not found") {
		t.Fatalf("expected missing model rejection, got %v", err)
	}
	var reloaded model.SiteModel
	if err := dbpkg.GetDB().WithContext(ctx).First(&reloaded, existing.ID).Error; err != nil {
		t.Fatalf("reload site model failed: %v", err)
	}
	if reloaded.Disabled {
		t.Fatal("expected valid target to remain unchanged after batch rejection")
	}
}

func TestSiteModelRoutesUpdateRejectsMissingTargetAtomically(t *testing.T) {
	ctx := setupSiteOpTestDB(t)
	site, account, existing := createCloudflareRouteReviewSite(t, ctx, "route-atomic-site", "route-atomic-account", "atomic")

	err := SiteModelRoutesUpdate(site.ID, account.ID, []model.SiteModelRouteUpdateRequest{
		{
			GroupKey:        model.SiteDefaultGroupKey,
			ModelName:       existing.ModelName,
			RouteType:       model.SiteModelRouteTypeOpenAIChat,
			RouteRawPayload: "must-not-be-written",
		},
		{
			GroupKey:  model.SiteDefaultGroupKey,
			ModelName: "@cf/test/missing",
			RouteType: model.SiteModelRouteTypeOpenAIChat,
		},
	}, ctx)
	if err == nil || !strings.Contains(err.Error(), "site model not found") {
		t.Fatalf("expected missing model rejection, got %v", err)
	}

	var reloaded model.SiteModel
	if err := dbpkg.GetDB().WithContext(ctx).First(&reloaded, existing.ID).Error; err != nil {
		t.Fatalf("reload site model failed: %v", err)
	}
	if reloaded.ManualOverride || reloaded.RouteSource != model.SiteModelRouteSourceSyncInferred || reloaded.RouteRawPayload != "" {
		t.Fatalf("expected the valid target to remain untouched, got %+v", reloaded)
	}
}

func TestCloudflareRouteUpdatesAndResetClearRawMetadata(t *testing.T) {
	ctx := setupSiteOpTestDB(t)
	site, account, existing := createCloudflareRouteReviewSite(t, ctx, "route-metadata-site", "route-metadata-account", "metadata")
	payload := model.SiteModelRouteMetadata{
		RouteSupported: true,
		RouteType:      model.SiteModelRouteTypeOpenAIChat,
		EnableGroups:   []string{"other-group"},
	}.Marshal()

	if err := SiteModelRoutesUpdate(site.ID, account.ID, []model.SiteModelRouteUpdateRequest{{
		GroupKey:        model.SiteDefaultGroupKey,
		ModelName:       existing.ModelName,
		RouteType:       model.SiteModelRouteTypeOpenAIChat,
		RouteRawPayload: payload,
	}}, ctx); err != nil {
		t.Fatalf("SiteModelRoutesUpdate failed: %v", err)
	}
	var reloaded model.SiteModel
	if err := dbpkg.GetDB().WithContext(ctx).First(&reloaded, existing.ID).Error; err != nil {
		t.Fatalf("reload site model failed: %v", err)
	}
	if reloaded.RouteRawPayload != "" || reloaded.RouteType != model.SiteModelRouteTypeOpenAIChat {
		t.Fatalf("expected Cloudflare route metadata to be canonicalized, got %+v", reloaded)
	}

	if err := dbpkg.GetDB().WithContext(ctx).Model(&model.SiteModel{}).Where("id = ?", existing.ID).Updates(map[string]any{
		"route_raw_payload": payload,
		"route_type":        model.SiteModelRouteTypeAnthropic,
		"manual_override":   true,
	}).Error; err != nil {
		t.Fatalf("seed legacy route metadata failed: %v", err)
	}
	if err := SiteChannelResetAccountRoutes(site.ID, account.ID, ctx); err != nil {
		t.Fatalf("SiteChannelResetAccountRoutes failed: %v", err)
	}
	if err := dbpkg.GetDB().WithContext(ctx).First(&reloaded, existing.ID).Error; err != nil {
		t.Fatalf("reload reset site model failed: %v", err)
	}
	if reloaded.RouteRawPayload != "" || reloaded.RouteType != model.SiteModelRouteTypeOpenAIChat || reloaded.ManualOverride {
		t.Fatalf("expected reset Cloudflare route to be canonical, got %+v", reloaded)
	}
}

func TestCloudflareSiteChannelViewIgnoresLegacyRouteGroupMetadata(t *testing.T) {
	ctx := setupSiteOpTestDB(t)
	site, account, existing := createCloudflareRouteReviewSite(t, ctx, "route-view-site", "route-view-account", "route-view")
	payload := model.SiteModelRouteMetadata{
		RouteSupported: true,
		RouteType:      model.SiteModelRouteTypeOpenAIChat,
		EnableGroups:   []string{"other-group"},
	}.Marshal()
	if err := dbpkg.GetDB().WithContext(ctx).Model(&model.SiteModel{}).
		Where("id = ?", existing.ID).
		Update("route_raw_payload", payload).Error; err != nil {
		t.Fatalf("seed legacy route metadata failed: %v", err)
	}

	view, err := SiteChannelAccountGet(site.ID, account.ID, ctx)
	if err != nil {
		t.Fatalf("SiteChannelAccountGet failed: %v", err)
	}
	if view.ModelCount != 1 {
		t.Fatalf("expected legacy Cloudflare model to remain visible, got %+v", view.Groups)
	}
}
