package op

import (
	"testing"

	dbpkg "github.com/bestruirui/octopus/internal/db"
	"github.com/bestruirui/octopus/internal/model"
	"github.com/bestruirui/octopus/internal/transformer/outbound"
)

// TestDBImportCloudflareHTTPURLFailsAtomically verifies that a Cloudflare site
// using plain HTTP (now rejected by the strict HTTPS-only validator) aborts the
// whole import transaction: rows from earlier tables (channels, ordinary
// sites) must not survive.
func TestDBImportCloudflareHTTPURLFailsAtomically(t *testing.T) {
	ctx := setupBackupTestDB(t)

	dump := &model.DBDump{
		Version:      1,
		IncludeLogs:  false,
		IncludeStats: false,
		Channels: []model.Channel{
			{ID: 1, Name: "normal-channel", Type: outbound.OutboundTypeOpenAIChat, Enabled: true},
		},
		Sites: []model.Site{
			{ID: 1, Name: "normal-site", Platform: model.SitePlatformNewAPI, BaseURL: "https://example.com/custom/v1", Enabled: true},
			{ID: 2, Name: "cf-http", Platform: model.SitePlatformCloudflare, BaseURL: "http://api.cloudflare.com/client/v4/accounts/acc1/ai", Enabled: true},
		},
	}

	if _, err := DBImportIncremental(ctx, dump); err == nil {
		t.Fatalf("expected import to fail for http cloudflare site url, got nil error")
	}

	var siteCount, channelCount int64
	if err := dbpkg.GetDB().Model(&model.Site{}).Count(&siteCount).Error; err != nil {
		t.Fatalf("count sites failed: %v", err)
	}
	if err := dbpkg.GetDB().Model(&model.Channel{}).Count(&channelCount).Error; err != nil {
		t.Fatalf("count channels failed: %v", err)
	}
	if siteCount != 0 || channelCount != 0 {
		t.Fatalf("expected atomic rollback (0 sites, 0 channels), got sites=%d channels=%d", siteCount, channelCount)
	}
}

// TestDBImportCloudflareUsernamePasswordAccountFails verifies Cloudflare
// credential constraints are enforced during backup restore: username/password
// accounts are rejected before any row is committed.
func TestDBImportCloudflareUsernamePasswordAccountFails(t *testing.T) {
	ctx := setupBackupTestDB(t)

	dump := &model.DBDump{
		Version:      1,
		IncludeLogs:  false,
		IncludeStats: false,
		Sites: []model.Site{
			{ID: 1, Name: "cf-ok", Platform: model.SitePlatformCloudflare, BaseURL: "https://api.cloudflare.com/client/v4/accounts/acc2/ai", Enabled: true},
		},
		SiteAccounts: []model.SiteAccount{
			{ID: 1, SiteID: 1, Name: "pw-account", CredentialType: model.SiteCredentialTypeUsernamePassword, Username: "user", Password: "pass", Enabled: true},
		},
	}

	if _, err := DBImportIncremental(ctx, dump); err == nil {
		t.Fatalf("expected import to fail for cloudflare username/password account, got nil error")
	}

	var siteCount, accountCount int64
	if err := dbpkg.GetDB().Model(&model.Site{}).Count(&siteCount).Error; err != nil {
		t.Fatalf("count sites failed: %v", err)
	}
	if err := dbpkg.GetDB().Model(&model.SiteAccount{}).Count(&accountCount).Error; err != nil {
		t.Fatalf("count site accounts failed: %v", err)
	}
	if siteCount != 0 || accountCount != 0 {
		t.Fatalf("expected atomic rollback (0 sites, 0 accounts), got sites=%d accounts=%d", siteCount, accountCount)
	}
}

// TestDBImportCloudflareCanonicalizesAndPreservesAPIPath verifies a valid but
// non-canonical Cloudflare backup (mixed case, explicit default port, trailing
// slash) is restored canonicalized while preserving the account id case; the
// account check-in flags are forced off and persisted; a non-Chat model is
// canonicalized to the four Cloudflare route fields; meanwhile an ordinary API
// site keeps its full base path.
func TestDBImportCloudflareCanonicalizesAndPreservesAPIPath(t *testing.T) {
	ctx := setupBackupTestDB(t)

	dump := &model.DBDump{
		Version:      1,
		IncludeLogs:  false,
		IncludeStats: false,
		Sites: []model.Site{
			{ID: 1, Name: "cf-mixed-case", Platform: model.SitePlatformCloudflare, BaseURL: "https://API.CloudFlare.com:443/CLIENT/V4/ACCOUNTS/CaseID/AI/V1/", Enabled: true},
			{ID: 2, Name: "normal-api", Platform: model.SitePlatformNewAPI, BaseURL: "https://example.com/custom/v1", Enabled: true},
			{ID: 3, Name: "cf-canonical-duplicate", Platform: model.SitePlatformCloudflare, BaseURL: "https://api.cloudflare.com/client/v4/accounts/CaseID/ai/v1", Enabled: true},
		},
		SiteAccounts: []model.SiteAccount{
			{ID: 1, SiteID: 1, Name: "cf-account", CredentialType: model.SiteCredentialTypeAPIKey, APIKey: "cf-token", Enabled: true, AutoCheckin: true, RandomCheckin: true},
		},
		SiteModels: []model.SiteModel{
			{
				ID: 1, SiteAccountID: 1, GroupKey: "default", ModelName: "@cf/zai-org/glm-4.7-flash",
				RouteType:      model.SiteModelRouteTypeAnthropic,
				RouteSource:    model.SiteModelRouteSourceManualOverride,
				ManualOverride: true, RouteRawPayload: `{"enable_groups":["x"]}`,
			},
		},
	}

	result, err := DBImportIncremental(ctx, dump)
	if err != nil {
		t.Fatalf("DBImportIncremental failed: %v", err)
	}
	if result.RowsAffected["sites"] != 2 {
		t.Fatalf("expected 2 sites created, got %d", result.RowsAffected["sites"])
	}

	var cfSite model.Site
	if err := dbpkg.GetDB().Where("platform = ?", model.SitePlatformCloudflare).First(&cfSite).Error; err != nil {
		t.Fatalf("query cloudflare site failed: %v", err)
	}
	wantCanonical := "https://api.cloudflare.com/client/v4/accounts/CaseID/ai/v1"
	if cfSite.BaseURL != wantCanonical {
		t.Fatalf("expected canonical cloudflare base url %q, got %q", wantCanonical, cfSite.BaseURL)
	}
	var cfSiteCount int64
	if err := dbpkg.GetDB().Model(&model.Site{}).Where("platform = ?", model.SitePlatformCloudflare).Count(&cfSiteCount).Error; err != nil {
		t.Fatalf("count cloudflare sites failed: %v", err)
	}
	if cfSiteCount != 1 {
		t.Fatalf("canonical-equivalent cloudflare sites must deduplicate, got %d", cfSiteCount)
	}

	var normalSite model.Site
	if err := dbpkg.GetDB().Where("platform = ? AND base_url = ?", model.SitePlatformNewAPI, "https://example.com/custom/v1").First(&normalSite).Error; err != nil {
		t.Fatalf("normal api site with full path not preserved: %v", err)
	}

	var cfAccount model.SiteAccount
	if err := dbpkg.GetDB().Where("site_id = ?", cfSite.ID).First(&cfAccount).Error; err != nil {
		t.Fatalf("query cloudflare account failed: %v", err)
	}
	if cfAccount.AutoCheckin {
		t.Fatalf("expected cloudflare account auto_checkin to persist as false, got true")
	}
	if cfAccount.RandomCheckin {
		t.Fatalf("expected cloudflare account random_checkin to persist as false, got true")
	}
	if cfAccount.NextAutoCheckinAt != nil {
		t.Fatalf("expected cloudflare account next_auto_checkin_at to be nil, got %v", cfAccount.NextAutoCheckinAt)
	}

	var cfModel model.SiteModel
	if err := dbpkg.GetDB().Where("site_account_id = ?", cfAccount.ID).First(&cfModel).Error; err != nil {
		t.Fatalf("query cloudflare site model failed: %v", err)
	}
	if cfModel.RouteType != model.SiteModelRouteTypeOpenAIChat {
		t.Fatalf("expected canonical route type openai_chat, got %q", cfModel.RouteType)
	}
	if cfModel.RouteSource != model.SiteModelRouteSourceSyncInferred {
		t.Fatalf("expected canonical route source sync_inferred, got %q", cfModel.RouteSource)
	}
	if cfModel.ManualOverride {
		t.Fatalf("expected manual_override to be false, got true")
	}
	if cfModel.RouteRawPayload != "" {
		t.Fatalf("expected route_raw_payload to be cleared, got %q", cfModel.RouteRawPayload)
	}
}

// TestDBImportCloudflareBindingRequiresChatChannel verifies Cloudflare channel
// bindings referencing a non-Chat channel abort the transaction, while a split
// group key pointing at a real Chat channel is restored as the unsplit base key.
func TestDBImportCloudflareBindingRequiresChatChannel(t *testing.T) {
	ctx := setupBackupTestDB(t)

	rejected := cloudflareBindingDump(outbound.OutboundTypeAnthropic, "default::anthropic")
	if _, err := DBImportIncremental(ctx, rejected); err == nil {
		t.Fatalf("expected import to fail for cloudflare binding to anthropic channel, got nil error")
	}
	var siteCount, bindingCount, channelCount int64
	if err := dbpkg.GetDB().Model(&model.Site{}).Count(&siteCount).Error; err != nil {
		t.Fatalf("count sites failed: %v", err)
	}
	if err := dbpkg.GetDB().Model(&model.SiteChannelBinding{}).Count(&bindingCount).Error; err != nil {
		t.Fatalf("count bindings failed: %v", err)
	}
	if err := dbpkg.GetDB().Model(&model.Channel{}).Count(&channelCount).Error; err != nil {
		t.Fatalf("count channels failed: %v", err)
	}
	if siteCount != 0 || bindingCount != 0 || channelCount != 0 {
		t.Fatalf("expected atomic rollback (0 sites, 0 bindings, 0 channels), got sites=%d bindings=%d channels=%d", siteCount, bindingCount, channelCount)
	}

	// Fresh database: a split key bound to a genuine Chat channel is accepted
	// and canonicalized back to the unsplit base key.
	if err := dbpkg.Close(); err != nil {
		t.Fatalf("close db failed: %v", err)
	}
	ctx = setupBackupTestDB(t)

	accepted := cloudflareBindingDump(outbound.OutboundTypeOpenAIChat, "default::openai-response")
	if _, err := DBImportIncremental(ctx, accepted); err != nil {
		t.Fatalf("expected chat-channel split-key binding import to succeed, got %v", err)
	}
	var restored model.SiteChannelBinding
	if err := dbpkg.GetDB().First(&restored).Error; err != nil {
		t.Fatalf("query restored binding failed: %v", err)
	}
	if restored.GroupKey != "default" {
		t.Fatalf("expected canonical unsplit group key %q, got %q", "default", restored.GroupKey)
	}
}

func cloudflareBindingDump(channelType outbound.OutboundType, groupKey string) *model.DBDump {
	return &model.DBDump{
		Version:      1,
		IncludeLogs:  false,
		IncludeStats: false,
		Channels: []model.Channel{
			{ID: 1, Name: "binding-channel", Type: channelType, Enabled: true},
		},
		Sites: []model.Site{
			{ID: 1, Name: "cf-binding", Platform: model.SitePlatformCloudflare, BaseURL: "https://api.cloudflare.com/client/v4/accounts/accBind/ai", Enabled: true},
		},
		SiteAccounts: []model.SiteAccount{
			{ID: 1, SiteID: 1, Name: "cf-binding-account", CredentialType: model.SiteCredentialTypeAPIKey, APIKey: "cf-bind-token", Enabled: true},
		},
		SiteChannelBindings: []model.SiteChannelBinding{
			{ID: 1, SiteID: 1, SiteAccountID: 1, GroupKey: groupKey, ChannelID: 1},
		},
	}
}

func TestDBImportCloudflareResolvesExistingParents(t *testing.T) {
	ctx := setupBackupTestDB(t)
	site := &model.Site{
		Name:     "existing-cloudflare",
		Platform: model.SitePlatformCloudflare,
		BaseURL:  "https://api.cloudflare.com/client/v4/accounts/existingParent/ai",
		Enabled:  true,
	}
	if err := SiteCreate(site, ctx); err != nil {
		t.Fatalf("create existing cloudflare site failed: %v", err)
	}
	account := &model.SiteAccount{
		SiteID:         site.ID,
		Name:           "existing-cloudflare-account",
		CredentialType: model.SiteCredentialTypeAPIKey,
		APIKey:         "existing-token",
		Enabled:        true,
	}
	if err := SiteAccountCreate(account, ctx); err != nil {
		t.Fatalf("create existing cloudflare account failed: %v", err)
	}

	const dumpSiteID = 901
	const dumpAccountID = 902
	modelOnlyDump := &model.DBDump{
		Version: 1,
		Sites: []model.Site{{
			ID: dumpSiteID, Name: site.Name, Platform: site.Platform, BaseURL: site.BaseURL, Enabled: true,
		}},
		SiteAccounts: []model.SiteAccount{{
			ID: dumpAccountID, SiteID: dumpSiteID, Name: account.Name,
			CredentialType: model.SiteCredentialTypeAPIKey, APIKey: "explicit-parent-token", Enabled: true,
		}},
		SiteModels: []model.SiteModel{{
			ID:              91,
			SiteAccountID:   dumpAccountID,
			GroupKey:        "default",
			ModelName:       "@cf/example/existing-parent",
			RouteType:       model.SiteModelRouteTypeAnthropic,
			RouteSource:     model.SiteModelRouteSourceManualOverride,
			ManualOverride:  true,
			RouteRawPayload: `{"stale":true}`,
		}},
	}
	if _, err := DBImportIncremental(ctx, modelOnlyDump); err != nil {
		t.Fatalf("model-only incremental import failed: %v", err)
	}
	var restoredModel model.SiteModel
	if err := dbpkg.GetDB().Where("site_account_id = ? AND model_name = ?", account.ID, "@cf/example/existing-parent").First(&restoredModel).Error; err != nil {
		t.Fatalf("query restored model failed: %v", err)
	}
	if restoredModel.RouteType != model.SiteModelRouteTypeOpenAIChat || restoredModel.RouteSource != model.SiteModelRouteSourceSyncInferred || restoredModel.ManualOverride || restoredModel.RouteRawPayload != "" {
		t.Fatalf("existing-parent cloudflare model was not canonicalized: %+v", restoredModel)
	}

	const invalidDumpSiteID = 903
	invalidAccountDump := &model.DBDump{
		Version: 1,
		Sites: []model.Site{{
			ID: invalidDumpSiteID, Name: site.Name, Platform: site.Platform, BaseURL: site.BaseURL, Enabled: true,
		}},
		SiteAccounts: []model.SiteAccount{{
			ID: 92, SiteID: invalidDumpSiteID, Name: "invalid-existing-parent-account",
			CredentialType: model.SiteCredentialTypeUsernamePassword,
			Username:       "user",
			Password:       "pass",
			Enabled:        true,
		}},
	}
	if _, err := DBImportIncremental(ctx, invalidAccountDump); err == nil {
		t.Fatal("expected existing cloudflare site to reject username/password account")
	}
	var invalidAccountCount int64
	if err := dbpkg.GetDB().Model(&model.SiteAccount{}).Where("name = ?", "invalid-existing-parent-account").Count(&invalidAccountCount).Error; err != nil {
		t.Fatalf("count invalid imported accounts failed: %v", err)
	}
	if invalidAccountCount != 0 {
		t.Fatalf("invalid account must not persist, got %d", invalidAccountCount)
	}

	nonChatChannel := &model.Channel{Name: "existing-parent-anthropic", Type: outbound.OutboundTypeAnthropic, Enabled: true}
	if err := ChannelCreate(nonChatChannel, ctx); err != nil {
		t.Fatalf("create non-chat channel failed: %v", err)
	}
	const bindingDumpChannelID = 904
	const bindingDumpSiteID = 905
	const bindingDumpAccountID = 906
	bindingOnlyDump := &model.DBDump{
		Version: 1,
		Channels: []model.Channel{{
			ID: bindingDumpChannelID, Name: nonChatChannel.Name, Type: nonChatChannel.Type, Enabled: true,
		}},
		Sites: []model.Site{{
			ID: bindingDumpSiteID, Name: site.Name, Platform: site.Platform, BaseURL: site.BaseURL, Enabled: true,
		}},
		SiteAccounts: []model.SiteAccount{{
			ID: bindingDumpAccountID, SiteID: bindingDumpSiteID, Name: account.Name,
			CredentialType: model.SiteCredentialTypeAPIKey, APIKey: "explicit-binding-parent-token", Enabled: true,
		}},
		SiteChannelBindings: []model.SiteChannelBinding{{
			ID: 93, SiteID: bindingDumpSiteID, SiteAccountID: bindingDumpAccountID,
			GroupKey: "default::anthropic", ChannelID: bindingDumpChannelID,
		}},
	}
	if _, err := DBImportIncremental(ctx, bindingOnlyDump); err == nil {
		t.Fatal("expected existing cloudflare account binding to reject non-chat channel")
	}
	var bindingCount int64
	if err := dbpkg.GetDB().Model(&model.SiteChannelBinding{}).Where("site_account_id = ?", account.ID).Count(&bindingCount).Error; err != nil {
		t.Fatalf("count imported bindings failed: %v", err)
	}
	if bindingCount != 0 {
		t.Fatalf("invalid binding must not persist, got %d", bindingCount)
	}
}
