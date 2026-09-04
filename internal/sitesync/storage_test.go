package sitesync

import (
	"context"
	"testing"
	"time"

	dbpkg "github.com/bestruirui/octopus/internal/db"
	"github.com/bestruirui/octopus/internal/model"
	"github.com/bestruirui/octopus/internal/op"
)

func TestSiteMaskedTokenMatchesIgnoresOptionalSKPrefix(t *testing.T) {
	tests := []struct {
		name      string
		fullToken string
		masked    string
	}{
		{name: "full has sk prefix", fullToken: "sk-yzFyREALREALOTkb", masked: "yzFy**********OTkb"},
		{name: "masked has sk prefix", fullToken: "yzFyREALREALOTkb", masked: "sk-yzFy**********OTkb"},
		{name: "both have sk prefix", fullToken: "sk-yzFyREALREALOTkb", masked: "sk-yzFy**********OTkb"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if !siteMaskedTokenMatches(tt.fullToken, tt.masked) {
				t.Fatalf("expected %q to match %q", tt.fullToken, tt.masked)
			}
		})
	}
}

func TestApplyPersistedRouteStateGuessesLegacyUnknownRoute(t *testing.T) {
	legacyPayload := model.SiteModelRouteMetadata{
		Source:                 "/api/pricing",
		RouteSupported:         false,
		SupportedEndpointTypes: []string{"/vendor/embeddings"},
		UnsupportedReason:      "site reports endpoint types outside current supported route buckets",
	}.Marshal()
	existing := &model.SiteModel{
		ModelName:       "vendor-embedding-x",
		RouteType:       model.SiteModelRouteTypeUnknown,
		RouteSource:     model.SiteModelRouteSourceSyncInferred,
		RouteRawPayload: legacyPayload,
	}
	item := &model.SiteModel{ModelName: "vendor-embedding-x"}

	applyPersistedRouteState(item, existing, time.Unix(1711929600, 0))

	if item.RouteType != model.SiteModelRouteTypeOpenAIEmbedding {
		t.Fatalf("expected legacy unknown route to be guessed as %q, got %q", model.SiteModelRouteTypeOpenAIEmbedding, item.RouteType)
	}
	metadata, ok := model.ParseSiteModelRouteMetadata(item.RouteRawPayload)
	if !ok {
		t.Fatalf("expected guessed route metadata to parse")
	}
	if !metadata.RouteSupported || !metadata.RouteGuessed {
		t.Fatalf("expected guessed route metadata to mark supported name guess, got %+v", metadata)
	}
	if metadata.RouteType != model.SiteModelRouteTypeOpenAIEmbedding {
		t.Fatalf("expected guessed metadata route type %q, got %q", model.SiteModelRouteTypeOpenAIEmbedding, metadata.RouteType)
	}
}

func TestApplyPersistedRouteStateKeepsManualOverrideUntouched(t *testing.T) {
	existing := &model.SiteModel{
		ModelName:      "vendor-embedding-x",
		RouteType:      model.SiteModelRouteTypeOpenAIChat,
		RouteSource:    model.SiteModelRouteSourceManualOverride,
		ManualOverride: true,
	}
	item := &model.SiteModel{ModelName: "vendor-embedding-x"}

	applyPersistedRouteState(item, existing, time.Unix(1711929600, 0))

	if item.RouteType != model.SiteModelRouteTypeOpenAIChat {
		t.Fatalf("expected manual override route to be preserved, got %q", item.RouteType)
	}
	if !item.ManualOverride {
		t.Fatalf("expected manual override flag to be preserved")
	}
}

func TestApplyPersistedRouteStateForCloudflareClearsHistoricalOverride(t *testing.T) {
	previous := time.Unix(1711929000, 0)
	now := time.Unix(1711929600, 0)
	existing := &model.SiteModel{
		ModelName:       "claude-3-5-sonnet",
		RouteType:       model.SiteModelRouteTypeAnthropic,
		RouteSource:     model.SiteModelRouteSourceManualOverride,
		ManualOverride:  true,
		RouteRawPayload: "legacy-route-metadata",
		RouteUpdatedAt:  &previous,
	}
	item := &model.SiteModel{
		ModelName:       "claude-3-5-sonnet",
		RouteType:       model.SiteModelRouteTypeAnthropic,
		RouteSource:     model.SiteModelRouteSourceRuntimeLearned,
		RouteRawPayload: "incoming-route-metadata",
	}

	applyPersistedRouteStateForPlatform(item, existing, model.SitePlatformCloudflare, now)

	if item.RouteType != model.SiteModelRouteTypeOpenAIChat {
		t.Fatalf("expected Cloudflare route openai_chat, got %q", item.RouteType)
	}
	if item.RouteSource != model.SiteModelRouteSourceSyncInferred || item.ManualOverride || item.RouteRawPayload != "" {
		t.Fatalf("expected historical route state to be cleared, got %+v", item)
	}
	if item.RouteUpdatedAt == nil || !item.RouteUpdatedAt.Equal(now) {
		t.Fatalf("expected route update timestamp %v, got %#v", now, item.RouteUpdatedAt)
	}
}

func TestMergePersistedSiteTokensPreservesManualFullTokenWhenIncomingIsMasked(t *testing.T) {
	now := time.Unix(1711929600, 0)
	existing := []model.SiteToken{{
		ID:            41,
		SiteAccountID: 9,
		Name:          "primary",
		Token:         "sk-yzFyREALREALOTkb",
		GroupKey:      model.SiteDefaultGroupKey,
		GroupName:     model.SiteDefaultGroupName,
		Enabled:       true,
		ValueStatus:   model.SiteTokenValueStatusReady,
		Source:        "manual",
	}}
	incoming := []model.SiteToken{{
		Name:        "primary",
		Token:       "yzFy**********OTkb",
		GroupKey:    model.SiteDefaultGroupKey,
		GroupName:   model.SiteDefaultGroupName,
		Enabled:     true,
		ValueStatus: model.SiteTokenValueStatusMaskedPending,
		Source:      "sync",
	}}

	merged := mergePersistedSiteTokens(9, existing, incoming, now)
	if len(merged) != 1 {
		t.Fatalf("expected exactly one merged token, got %+v", merged)
	}
	if merged[0].Token != "sk-yzFyREALREALOTkb" {
		t.Fatalf("expected merged token to keep full manual value, got %q", merged[0].Token)
	}
	if merged[0].ValueStatus != model.SiteTokenValueStatusReady {
		t.Fatalf("expected merged token to remain ready, got %q", merged[0].ValueStatus)
	}
	if !merged[0].Enabled {
		t.Fatalf("expected merged token to remain enabled")
	}
}

func TestMergePersistedSiteTokensTreatsOptionalSKPrefixAsSameReadyToken(t *testing.T) {
	now := time.Unix(1711929600, 0)
	existing := []model.SiteToken{{
		ID:            7,
		SiteAccountID: 9,
		Name:          "primary",
		Token:         "sk-abc123",
		GroupKey:      model.SiteDefaultGroupKey,
		GroupName:     model.SiteDefaultGroupName,
		Enabled:       true,
		ValueStatus:   model.SiteTokenValueStatusReady,
		Source:        "manual",
	}}
	incoming := []model.SiteToken{{
		Name:      "primary",
		Token:     "abc123",
		GroupKey:  model.SiteDefaultGroupKey,
		GroupName: model.SiteDefaultGroupName,
		Enabled:   true,
		Source:    "sync",
	}}

	merged := mergePersistedSiteTokens(9, existing, incoming, now)
	if len(merged) != 1 {
		t.Fatalf("expected exactly one merged token, got %+v", merged)
	}
	if merged[0].Token != "sk-abc123" {
		t.Fatalf("expected merged token to preserve stored full token format, got %q", merged[0].Token)
	}
	if merged[0].ValueStatus != model.SiteTokenValueStatusReady {
		t.Fatalf("expected merged token to remain ready, got %q", merged[0].ValueStatus)
	}
}

func TestMergePersistedSiteTokensPreservesLocalDisabledStateForReadyToken(t *testing.T) {
	now := time.Unix(1711929600, 0)
	existing := []model.SiteToken{{
		ID:            8,
		SiteAccountID: 9,
		Name:          "primary",
		Token:         "sk-local-disabled",
		GroupKey:      model.SiteDefaultGroupKey,
		GroupName:     model.SiteDefaultGroupName,
		Enabled:       false,
		ValueStatus:   model.SiteTokenValueStatusReady,
		Source:        "manual",
	}}
	incoming := []model.SiteToken{{
		Name:      "primary",
		Token:     "sk-local-disabled",
		GroupKey:  model.SiteDefaultGroupKey,
		GroupName: model.SiteDefaultGroupName,
		Enabled:   true,
		Source:    "sync",
	}}

	merged := mergePersistedSiteTokens(9, existing, incoming, now)
	if len(merged) != 1 {
		t.Fatalf("expected exactly one merged token, got %+v", merged)
	}
	if merged[0].Enabled {
		t.Fatalf("expected local disabled state to be preserved, got enabled token: %+v", merged[0])
	}
}

func TestMergePersistedSiteTokensPreservesLocalEnabledStateWhenIncomingDisabled(t *testing.T) {
	now := time.Unix(1711929600, 0)
	existing := []model.SiteToken{{
		ID:            9,
		SiteAccountID: 9,
		Name:          "primary",
		Token:         "sk-local-enabled",
		GroupKey:      model.SiteDefaultGroupKey,
		GroupName:     model.SiteDefaultGroupName,
		Enabled:       true,
		ValueStatus:   model.SiteTokenValueStatusReady,
		Source:        "sync",
	}}
	incoming := []model.SiteToken{{
		Name:      "primary",
		Token:     "sk-local-enabled",
		GroupKey:  model.SiteDefaultGroupKey,
		GroupName: model.SiteDefaultGroupName,
		Enabled:   false,
		Source:    "sync",
	}}

	merged := mergePersistedSiteTokens(9, existing, incoming, now)
	if len(merged) != 1 {
		t.Fatalf("expected exactly one merged token, got %+v", merged)
	}
	if !merged[0].Enabled {
		t.Fatalf("expected local enabled state to be preserved, got disabled token: %+v", merged[0])
	}
}

func TestMergePersistedSiteTokensKeepsMaskedPendingWhenMatchIsAmbiguous(t *testing.T) {
	now := time.Unix(1711929600, 0)
	existing := []model.SiteToken{
		{
			ID:            1,
			SiteAccountID: 9,
			Name:          "alpha",
			Token:         "sk-yzFyONEOTkb",
			GroupKey:      model.SiteDefaultGroupKey,
			GroupName:     model.SiteDefaultGroupName,
			Enabled:       true,
			ValueStatus:   model.SiteTokenValueStatusReady,
			Source:        "manual",
		},
		{
			ID:            2,
			SiteAccountID: 9,
			Name:          "beta",
			Token:         "sk-yzFyTWOOTkb",
			GroupKey:      model.SiteDefaultGroupKey,
			GroupName:     model.SiteDefaultGroupName,
			Enabled:       true,
			ValueStatus:   model.SiteTokenValueStatusReady,
			Source:        "manual",
		},
	}
	incoming := []model.SiteToken{{
		Name:        "",
		Token:       "yzFy**********OTkb",
		GroupKey:    model.SiteDefaultGroupKey,
		GroupName:   model.SiteDefaultGroupName,
		Enabled:     true,
		ValueStatus: model.SiteTokenValueStatusMaskedPending,
		Source:      "sync",
	}}

	merged := mergePersistedSiteTokens(9, existing, incoming, now)
	if len(merged) != 3 {
		t.Fatalf("expected masked pending token plus two preserved manual tokens, got %+v", merged)
	}
	maskedCount := 0
	for _, item := range merged {
		if item.Token == "yzFy**********OTkb" {
			maskedCount++
			if item.ValueStatus != model.SiteTokenValueStatusMaskedPending {
				t.Fatalf("expected ambiguous incoming token to remain masked_pending, got %+v", item)
			}
			if item.Enabled {
				t.Fatalf("expected ambiguous masked_pending token to stay disabled")
			}
		}
	}
	if maskedCount != 1 {
		t.Fatalf("expected exactly one preserved masked_pending token, got %+v", merged)
	}
}

func TestMergePersistedSiteTokensDemotesReadyTokenWhenMaskedPatternMismatches(t *testing.T) {
	now := time.Unix(1711929600, 0)
	existing := []model.SiteToken{{
		ID:            5,
		SiteAccountID: 9,
		Name:          "primary",
		Token:         "sk-different-full-token",
		GroupKey:      model.SiteDefaultGroupKey,
		GroupName:     model.SiteDefaultGroupName,
		Enabled:       true,
		ValueStatus:   model.SiteTokenValueStatusReady,
		Source:        "manual",
	}}
	incoming := []model.SiteToken{{
		Name:        "primary",
		Token:       "yzFy**********OTkb",
		GroupKey:    model.SiteDefaultGroupKey,
		GroupName:   model.SiteDefaultGroupName,
		Enabled:     true,
		ValueStatus: model.SiteTokenValueStatusMaskedPending,
		Source:      "sync",
	}}

	merged := mergePersistedSiteTokens(9, existing, incoming, now)
	if len(merged) != 1 {
		t.Fatalf("expected exactly one merged token, got %+v", merged)
	}
	if merged[0].Token != "yzFy**********OTkb" {
		t.Fatalf("expected stale ready token to be replaced by incoming masked value, got %q", merged[0].Token)
	}
	if merged[0].ValueStatus != model.SiteTokenValueStatusMaskedPending {
		t.Fatalf("expected merged token to be demoted to masked_pending, got %q", merged[0].ValueStatus)
	}
	if merged[0].Enabled {
		t.Fatalf("expected demoted token to be disabled until the user re-fills it")
	}
}

func TestMergePersistedSiteTokensRestoresCreatedPlaintextKey(t *testing.T) {
	now := time.Unix(1711929600, 0)
	existing := []model.SiteToken{{
		ID:            12,
		SiteAccountID: 9,
		Name:          "created-name",
		Token:         "sk-cre**********-key",
		GroupKey:      "vip",
		GroupName:     "VIP",
		Enabled:       false,
		ValueStatus:   model.SiteTokenValueStatusMaskedPending,
		Source:        "sync",
	}}
	incoming := []model.SiteToken{{
		Name:        "created-name",
		Token:       "sk-created-plain-key",
		GroupKey:    "vip",
		GroupName:   "VIP",
		Enabled:     true,
		ValueStatus: model.SiteTokenValueStatusReady,
		Source:      siteTokenSourceCreated,
	}}

	merged := mergePersistedSiteTokens(9, existing, incoming, now)
	if len(merged) != 1 {
		t.Fatalf("expected exactly one merged token, got %+v", merged)
	}
	if merged[0].Token != "sk-created-plain-key" || merged[0].ValueStatus != model.SiteTokenValueStatusReady {
		t.Fatalf("expected created plaintext key to replace masked value, got %+v", merged[0])
	}
	if !merged[0].Enabled {
		t.Fatalf("expected created plaintext key to be enabled")
	}

	incoming[0].Source = "sync"
	incoming[0].Token = "sk-cre**********-key"
	incoming[0].ValueStatus = model.SiteTokenValueStatusMaskedPending
	ordinary := mergePersistedSiteTokens(9, existing, incoming, now)
	if len(ordinary) != 1 || ordinary[0].ValueStatus != model.SiteTokenValueStatusMaskedPending || ordinary[0].Enabled {
		t.Fatalf("expected ordinary masked sync to preserve disabled masked pending state, got %+v", ordinary)
	}
}

func TestPersistSyncSnapshotPreservesGroupProjectionDisabled(t *testing.T) {
	ctx := setupProjectTestDB(t)
	_, account := createProjectionFixture(t, ctx)

	vipGroup := model.SiteUserGroup{SiteAccountID: account.ID, GroupKey: "vip", Name: "VIP", ProjectionDisabled: true}
	if err := dbpkg.GetDB().WithContext(ctx).Create(&vipGroup).Error; err != nil {
		t.Fatalf("create vip group failed: %v", err)
	}

	snapshot := &syncSnapshot{
		accessToken: account.AccessToken,
		groups: []model.SiteUserGroup{
			{GroupKey: "vip", Name: "VIP Renamed"},
		},
		tokens: []model.SiteToken{
			{Name: "vip", Token: "key-vip", GroupKey: "vip", GroupName: "VIP", Enabled: true, Source: "sync"},
		},
		status:  model.SiteExecutionStatusSuccess,
		message: "ok",
	}

	if err := persistSyncSnapshot(ctx, account.ID, snapshot); err != nil {
		t.Fatalf("persistSyncSnapshot returned error: %v", err)
	}

	var reloaded model.SiteUserGroup
	if err := dbpkg.GetDB().WithContext(ctx).Where("site_account_id = ? AND group_key = ?", account.ID, "vip").First(&reloaded).Error; err != nil {
		t.Fatalf("query reloaded group failed: %v", err)
	}
	if !reloaded.ProjectionDisabled {
		t.Fatalf("expected projection_disabled to be preserved")
	}
	if reloaded.Name != "VIP Renamed" {
		t.Fatalf("expected synced group metadata to be updated, got %q", reloaded.Name)
	}
}

func TestPersistSyncSnapshotPreservesChannelDisabled(t *testing.T) {
	ctx := setupProjectTestDB(t)
	_, account := createProjectionFixture(t, ctx)

	group := model.SiteUserGroup{SiteAccountID: account.ID, GroupKey: "vip", Name: "VIP", ChannelDisabled: true}
	if err := dbpkg.GetDB().WithContext(ctx).Create(&group).Error; err != nil {
		t.Fatalf("create vip group failed: %v", err)
	}
	snapshot := &syncSnapshot{
		accessToken: account.AccessToken,
		groups:      []model.SiteUserGroup{{GroupKey: "vip", Name: "VIP Renamed"}},
		tokens:      []model.SiteToken{{Name: "vip", Token: "key-vip", GroupKey: "vip", GroupName: "VIP", Enabled: true, Source: "sync"}},
		status:      model.SiteExecutionStatusSuccess,
		message:     "ok",
	}
	if err := persistSyncSnapshot(ctx, account.ID, snapshot); err != nil {
		t.Fatalf("persistSyncSnapshot returned error: %v", err)
	}

	var reloaded model.SiteUserGroup
	if err := dbpkg.GetDB().WithContext(ctx).Where("site_account_id = ? AND group_key = ?", account.ID, "vip").First(&reloaded).Error; err != nil {
		t.Fatalf("query reloaded group failed: %v", err)
	}
	if !reloaded.ChannelDisabled {
		t.Fatalf("expected channel_disabled to be preserved")
	}
}

func TestPersistSyncSnapshotReplacesOnlyAuthoritativeGroups(t *testing.T) {
	ctx := setupProjectTestDB(t)
	_, account := createProjectionFixture(t, ctx)

	vipGroup := model.SiteUserGroup{SiteAccountID: account.ID, GroupKey: "vip", Name: "VIP"}
	if err := dbpkg.GetDB().WithContext(ctx).Create(&vipGroup).Error; err != nil {
		t.Fatalf("create vip group failed: %v", err)
	}
	vipToken := model.SiteToken{SiteAccountID: account.ID, Name: "vip", Token: "key-vip", GroupKey: "vip", GroupName: "VIP", Enabled: true}
	if err := dbpkg.GetDB().WithContext(ctx).Create(&vipToken).Error; err != nil {
		t.Fatalf("create vip token failed: %v", err)
	}
	vipModel := model.SiteModel{SiteAccountID: account.ID, GroupKey: "vip", ModelName: "gpt-4o-vip-old", Source: "sync", RouteType: model.SiteModelRouteTypeOpenAIChat, RouteSource: model.SiteModelRouteSourceSyncInferred}
	if err := dbpkg.GetDB().WithContext(ctx).Create(&vipModel).Error; err != nil {
		t.Fatalf("create vip model failed: %v", err)
	}

	snapshot := &syncSnapshot{
		accessToken: account.AccessToken,
		groups: []model.SiteUserGroup{
			{GroupKey: model.SiteDefaultGroupKey, Name: model.SiteDefaultGroupName},
			{GroupKey: "vip", Name: "VIP"},
		},
		tokens: []model.SiteToken{
			{Name: "primary", Token: "key-primary-new", GroupKey: model.SiteDefaultGroupKey, GroupName: model.SiteDefaultGroupName, Enabled: true, Source: "sync"},
			{Name: "vip", Token: "key-vip-new", GroupKey: "vip", GroupName: "VIP", Enabled: true, Source: "sync"},
		},
		models: []model.SiteModel{
			{GroupKey: model.SiteDefaultGroupKey, ModelName: "gpt-4.1", Source: "sync", RouteType: model.SiteModelRouteTypeOpenAIChat, RouteSource: model.SiteModelRouteSourceSyncInferred},
		},
		groupResults: []siteGroupSyncResult{
			{GroupKey: model.SiteDefaultGroupKey, GroupName: model.SiteDefaultGroupName, HasKey: true, Status: siteGroupSyncStatusSynced, Authoritative: true, ModelCount: 1, Message: "同步到 1 个模型"},
			{GroupKey: "vip", GroupName: "VIP", HasKey: true, Status: siteGroupSyncStatusFailed, Authoritative: false, Message: "unauthorized"},
		},
		status:  model.SiteExecutionStatusPartial,
		message: "部分分组同步完成：更新 1 个分组，保留 1 个分组的历史投影",
	}

	if err := persistSyncSnapshot(ctx, account.ID, snapshot); err != nil {
		t.Fatalf("persistSyncSnapshot returned error: %v", err)
	}

	var models []model.SiteModel
	if err := dbpkg.GetDB().WithContext(ctx).Where("site_account_id = ?", account.ID).Order("group_key ASC, model_name ASC").Find(&models).Error; err != nil {
		t.Fatalf("query models failed: %v", err)
	}
	if len(models) != 2 {
		t.Fatalf("expected one refreshed default model and one preserved vip model, got %+v", models)
	}
	modelsByGroup := make(map[string][]string)
	for _, item := range models {
		modelsByGroup[item.GroupKey] = append(modelsByGroup[item.GroupKey], item.ModelName)
	}
	if len(modelsByGroup[model.SiteDefaultGroupKey]) != 1 || modelsByGroup[model.SiteDefaultGroupKey][0] != "gpt-4.1" {
		t.Fatalf("expected default group to be fully replaced, got %+v", modelsByGroup)
	}
	if len(modelsByGroup["vip"]) != 1 || modelsByGroup["vip"][0] != "gpt-4o-vip-old" {
		t.Fatalf("expected vip group to keep historical model, got %+v", modelsByGroup)
	}

	reloaded, err := op.SiteAccountGet(account.ID, context.Background())
	if err != nil {
		t.Fatalf("SiteAccountGet failed: %v", err)
	}
	if reloaded.LastSyncStatus != model.SiteExecutionStatusPartial {
		t.Fatalf("expected partial last_sync_status, got %q", reloaded.LastSyncStatus)
	}
	if reloaded.LastSyncMessage != snapshot.message {
		t.Fatalf("expected last_sync_message %q, got %q", snapshot.message, reloaded.LastSyncMessage)
	}

	var vipReloaded model.SiteUserGroup
	if err := dbpkg.GetDB().WithContext(ctx).Where("site_account_id = ? AND group_key = ?", account.ID, "vip").First(&vipReloaded).Error; err != nil {
		t.Fatalf("query vip group failed: %v", err)
	}
	if vipReloaded.ProjectionSuspended {
		t.Fatalf("expected failed vip group projection to keep historical projection active")
	}
	if vipReloaded.ModelSyncStatus != model.SiteGroupModelSyncStatusFailed {
		t.Fatalf("expected failed vip model sync status, got %q", vipReloaded.ModelSyncStatus)
	}
	if vipReloaded.ModelSyncFailureCount != 1 {
		t.Fatalf("expected vip failure count 1, got %d", vipReloaded.ModelSyncFailureCount)
	}
}

func TestPreserveHistoricalSiteGroupResultsDegradesDestructiveStatuses(t *testing.T) {
	results := preserveHistoricalSiteGroupResults([]siteGroupSyncResult{
		{GroupKey: "removed", Status: siteGroupSyncStatusRemoved, Authoritative: true},
		{GroupKey: "missing", Status: siteGroupSyncStatusMissingKey},
		{GroupKey: "empty", Status: siteGroupSyncStatusEmpty, Authoritative: true},
		{GroupKey: "synced", Status: siteGroupSyncStatusSynced, Authoritative: true},
	})
	for _, result := range results {
		switch result.GroupKey {
		case "removed", "missing", "empty":
			if result.Status != siteGroupSyncStatusUnresolved || result.Authoritative {
				t.Fatalf("expected destructive status to degrade for %s, got %+v", result.GroupKey, result)
			}
		case "synced":
			if result.Status != siteGroupSyncStatusSynced || !result.Authoritative {
				t.Fatalf("expected synced status to remain authoritative, got %+v", result)
			}
		}
	}
}

func TestPersistSyncSnapshotPreservesIncompleteDiscoveryHistory(t *testing.T) {
	ctx := setupProjectTestDB(t)
	_, account := createProjectionFixture(t, ctx)

	vipGroup := model.SiteUserGroup{SiteAccountID: account.ID, GroupKey: "vip", Name: "VIP"}
	if err := dbpkg.GetDB().WithContext(ctx).Create(&vipGroup).Error; err != nil {
		t.Fatalf("create vip group failed: %v", err)
	}
	vipToken := model.SiteToken{SiteAccountID: account.ID, Name: "vip", Token: "key-vip", GroupKey: "vip", GroupName: "VIP", Enabled: true, Source: "sync"}
	if err := dbpkg.GetDB().WithContext(ctx).Create(&vipToken).Error; err != nil {
		t.Fatalf("create vip token failed: %v", err)
	}
	vipModel := model.SiteModel{SiteAccountID: account.ID, GroupKey: "vip", ModelName: "gpt-4o-vip", Source: "sync", RouteType: model.SiteModelRouteTypeOpenAIChat, RouteSource: model.SiteModelRouteSourceSyncInferred}
	if err := dbpkg.GetDB().WithContext(ctx).Create(&vipModel).Error; err != nil {
		t.Fatalf("create vip model failed: %v", err)
	}

	if _, err := ProjectAccount(ctx, account.ID); err != nil {
		t.Fatalf("initial ProjectAccount failed: %v", err)
	}

	snapshot := &syncSnapshot{
		accessToken: account.AccessToken,
		groups: []model.SiteUserGroup{
			{GroupKey: model.SiteDefaultGroupKey, Name: model.SiteDefaultGroupName},
		},
		tokens: []model.SiteToken{
			{Name: "primary", Token: "key-primary-new", GroupKey: model.SiteDefaultGroupKey, GroupName: model.SiteDefaultGroupName, Enabled: true, Source: "sync"},
		},
		models: []model.SiteModel{
			{GroupKey: model.SiteDefaultGroupKey, ModelName: "gpt-4.1", Source: "sync", RouteType: model.SiteModelRouteTypeOpenAIChat, RouteSource: model.SiteModelRouteSourceSyncInferred},
		},
		groupResults: []siteGroupSyncResult{
			{GroupKey: model.SiteDefaultGroupKey, GroupName: model.SiteDefaultGroupName, HasKey: true, Status: siteGroupSyncStatusSynced, Authoritative: true, ModelCount: 1, Message: "同步到 1 个模型"},
		},
		preserveHistoricalGroups: true,
		status:                   model.SiteExecutionStatusPartial,
		message:                  "部分分组同步完成：保留历史投影",
	}
	if err := persistSyncSnapshot(ctx, account.ID, snapshot); err != nil {
		t.Fatalf("persistSyncSnapshot returned error: %v", err)
	}
	if _, err := ProjectAccount(ctx, account.ID); err != nil {
		t.Fatalf("ProjectAccount after incomplete sync failed: %v", err)
	}

	var vipGroupCount int64
	if err := dbpkg.GetDB().WithContext(ctx).Model(&model.SiteUserGroup{}).Where("site_account_id = ? AND group_key = ?", account.ID, "vip").Count(&vipGroupCount).Error; err != nil {
		t.Fatalf("count vip groups failed: %v", err)
	}
	if vipGroupCount != 1 {
		t.Fatalf("expected historical vip group to remain, got %d", vipGroupCount)
	}
	var vipTokenCount int64
	if err := dbpkg.GetDB().WithContext(ctx).Model(&model.SiteToken{}).Where("site_account_id = ? AND group_key = ?", account.ID, "vip").Count(&vipTokenCount).Error; err != nil {
		t.Fatalf("count vip tokens failed: %v", err)
	}
	if vipTokenCount != 1 {
		t.Fatalf("expected historical vip token to remain, got %d", vipTokenCount)
	}
	var vipModelCount int64
	if err := dbpkg.GetDB().WithContext(ctx).Model(&model.SiteModel{}).Where("site_account_id = ? AND group_key = ?", account.ID, "vip").Count(&vipModelCount).Error; err != nil {
		t.Fatalf("count vip models failed: %v", err)
	}
	if vipModelCount != 1 {
		t.Fatalf("expected historical vip model to remain, got %d", vipModelCount)
	}
	var vipBindingCount int64
	if err := dbpkg.GetDB().WithContext(ctx).Model(&model.SiteChannelBinding{}).Where("site_account_id = ? AND group_key LIKE ?", account.ID, "vip%").Count(&vipBindingCount).Error; err != nil {
		t.Fatalf("count vip bindings failed: %v", err)
	}
	if vipBindingCount != 1 {
		t.Fatalf("expected historical vip channel binding to remain, got %d", vipBindingCount)
	}
}

func TestPersistSyncSnapshotNormalizesEntireCloudflareModelSet(t *testing.T) {
	ctx := setupProjectTestDB(t)
	site := &model.Site{
		Name:     "Cloudflare Storage Site",
		Platform: model.SitePlatformCloudflare,
		BaseURL:  "https://api.cloudflare.com/client/v4/accounts/account-id/ai",
		Enabled:  true,
	}
	if err := op.SiteCreate(site, ctx); err != nil {
		t.Fatalf("SiteCreate failed: %v", err)
	}
	account := &model.SiteAccount{
		SiteID:         site.ID,
		Name:           "Cloudflare Account",
		CredentialType: model.SiteCredentialTypeAccessToken,
		AccessToken:    "access-token",
		Enabled:        true,
	}
	if err := op.SiteAccountCreate(account, ctx); err != nil {
		t.Fatalf("SiteAccountCreate failed: %v", err)
	}
	previous := time.Unix(1711929000, 0)
	historical := model.SiteModel{
		SiteAccountID:   account.ID,
		GroupKey:        "vip",
		ModelName:       "historical-vip-model",
		Source:          "sync",
		RouteType:       model.SiteModelRouteTypeAnthropic,
		RouteSource:     model.SiteModelRouteSourceManualOverride,
		ManualOverride:  true,
		RouteRawPayload: "historical-route",
		RouteUpdatedAt:  &previous,
	}
	if err := dbpkg.GetDB().WithContext(ctx).Create(&historical).Error; err != nil {
		t.Fatalf("create historical model failed: %v", err)
	}

	snapshot := &syncSnapshot{
		accessToken: account.AccessToken,
		groups: []model.SiteUserGroup{
			{GroupKey: model.SiteDefaultGroupKey, Name: model.SiteDefaultGroupName},
			{GroupKey: "vip", Name: "VIP"},
		},
		models: []model.SiteModel{{
			GroupKey:        model.SiteDefaultGroupKey,
			ModelName:       "fresh-default-model",
			Source:          "sync",
			RouteType:       model.SiteModelRouteTypeGemini,
			RouteSource:     model.SiteModelRouteSourceRuntimeLearned,
			ManualOverride:  true,
			RouteRawPayload: "incoming-route",
		}},
		groupResults: []siteGroupSyncResult{
			{GroupKey: model.SiteDefaultGroupKey, GroupName: model.SiteDefaultGroupName, HasKey: true, Status: siteGroupSyncStatusSynced, Authoritative: true, ModelCount: 1},
			{GroupKey: "vip", GroupName: "VIP", HasKey: true, Status: siteGroupSyncStatusFailed, Authoritative: false, Message: "temporary failure"},
		},
		status:  model.SiteExecutionStatusPartial,
		message: "partial",
	}
	if err := persistSyncSnapshot(ctx, account.ID, snapshot); err != nil {
		t.Fatalf("persistSyncSnapshot returned error: %v", err)
	}

	var persisted []model.SiteModel
	if err := dbpkg.GetDB().WithContext(ctx).
		Where("site_account_id = ?", account.ID).
		Order("group_key ASC, model_name ASC").
		Find(&persisted).Error; err != nil {
		t.Fatalf("query persisted models failed: %v", err)
	}
	if len(persisted) != 2 {
		t.Fatalf("expected fresh and preserved models, got %+v", persisted)
	}
	for _, item := range persisted {
		if item.RouteType != model.SiteModelRouteTypeOpenAIChat ||
			item.RouteSource != model.SiteModelRouteSourceSyncInferred ||
			item.ManualOverride || item.RouteRawPayload != "" {
			t.Fatalf("expected every Cloudflare model route to be canonical, got %+v", item)
		}
		if item.RouteUpdatedAt == nil {
			t.Fatalf("expected route timestamp for %+v", item)
		}
	}
	for _, item := range persisted {
		if item.ModelName == historical.ModelName && !item.RouteUpdatedAt.After(previous) {
			t.Fatalf("expected preserved historical route timestamp to advance, got %v", item.RouteUpdatedAt)
		}
	}
}

func TestPersistSyncSnapshotEmptySuspendsWithoutAdvancingSuccessTime(t *testing.T) {
	ctx := setupProjectTestDB(t)
	_, account := createProjectionFixture(t, ctx)

	previousSuccess := time.Unix(1700000000, 0)
	group := model.SiteUserGroup{
		SiteAccountID:          account.ID,
		GroupKey:               model.SiteDefaultGroupKey,
		Name:                   model.SiteDefaultGroupName,
		ModelSyncStatus:        model.SiteGroupModelSyncStatusSynced,
		LastModelSyncSuccessAt: &previousSuccess,
	}
	if err := dbpkg.GetDB().WithContext(ctx).Create(&group).Error; err != nil {
		t.Fatalf("create group failed: %v", err)
	}

	snapshot := &syncSnapshot{
		accessToken: account.AccessToken,
		groups:      []model.SiteUserGroup{{GroupKey: model.SiteDefaultGroupKey, Name: model.SiteDefaultGroupName}},
		tokens:      []model.SiteToken{{Name: "primary", Token: "key-primary", GroupKey: model.SiteDefaultGroupKey, GroupName: model.SiteDefaultGroupName, Enabled: true, Source: "sync"}},
		groupResults: []siteGroupSyncResult{
			{GroupKey: model.SiteDefaultGroupKey, GroupName: model.SiteDefaultGroupName, HasKey: true, Status: siteGroupSyncStatusEmpty, Authoritative: true, Message: "上游当前没有可用模型"},
		},
		status:  model.SiteExecutionStatusSuccess,
		message: "上游当前无可用模型，已清空历史模型",
	}
	if err := persistSyncSnapshot(ctx, account.ID, snapshot); err != nil {
		t.Fatalf("persistSyncSnapshot returned error: %v", err)
	}

	var reloaded model.SiteUserGroup
	if err := dbpkg.GetDB().WithContext(ctx).Where("site_account_id = ? AND group_key = ?", account.ID, model.SiteDefaultGroupKey).First(&reloaded).Error; err != nil {
		t.Fatalf("query reloaded group failed: %v", err)
	}
	if !reloaded.ProjectionSuspended {
		t.Fatalf("expected empty group projection to be suspended")
	}
	if reloaded.LastModelSyncSuccessAt == nil || !reloaded.LastModelSyncSuccessAt.Equal(previousSuccess) {
		t.Fatalf("expected empty sync to preserve last success time %v, got %v", previousSuccess, reloaded.LastModelSyncSuccessAt)
	}
}

func TestMergePersistedDirectTokenReplacesLegacyManualToken(t *testing.T) {
	now := time.Unix(1711929600, 0)
	existing := []model.SiteToken{{
		ID: 41, SiteAccountID: 9, Name: "default", Token: "old-direct-token",
		GroupKey: model.SiteDefaultGroupKey, GroupName: model.SiteDefaultGroupName,
		Enabled: true, ValueStatus: model.SiteTokenValueStatusReady, Source: "manual",
	}}
	incoming := []model.SiteToken{{
		Name: "default", Token: "new-direct-token", GroupKey: model.SiteDefaultGroupKey,
		GroupName: model.SiteDefaultGroupName, Enabled: true,
		ValueStatus: model.SiteTokenValueStatusReady, Source: "direct",
	}}

	merged := mergePersistedSiteTokens(9, existing, incoming, now)
	if len(merged) != 1 || merged[0].Token != "new-direct-token" || merged[0].Source != "direct" {
		t.Fatalf("expected direct sync to replace the legacy generated token, got %+v", merged)
	}
}

func TestMergePersistedSyncKeepsManualToken(t *testing.T) {
	now := time.Unix(1711929600, 0)
	existing := []model.SiteToken{{
		ID: 42, SiteAccountID: 9, Name: "default", Token: "user-manual-token",
		GroupKey: model.SiteDefaultGroupKey, GroupName: model.SiteDefaultGroupName,
		Enabled: true, ValueStatus: model.SiteTokenValueStatusReady, Source: "manual",
	}}
	incoming := []model.SiteToken{{
		Name: "default", Token: "new-sync-token", GroupKey: model.SiteDefaultGroupKey,
		GroupName: model.SiteDefaultGroupName, Enabled: true,
		ValueStatus: model.SiteTokenValueStatusReady, Source: "sync",
	}}

	merged := mergePersistedSiteTokens(9, existing, incoming, now)
	if len(merged) != 2 {
		t.Fatalf("expected sync token plus manual token to coexist, got %+v", merged)
	}
	foundManual, foundSync := false, false
	for _, token := range merged {
		if token.Source == "manual" && token.Token == "user-manual-token" {
			foundManual = true
		}
		if token.Source == "sync" && token.Token == "new-sync-token" {
			foundSync = true
		}
	}
	if !foundManual || !foundSync {
		t.Fatalf("sync removed a manual token: %+v", merged)
	}
}

// TestPersistSyncSnapshotPreservesExplicitlyDisabledTokens locks in the
// contract that tokens persisted with enabled=false survive the
// delete-and-recreate cycle in persistSyncSnapshot. GORM omits zero-valued
// (false) fields from the INSERT when the column carries a database default
// (SiteToken.Enabled defaults to true), which would silently re-enable both
// tokens that inherit a disabled state from an existing row (old ID) and
// brand-new tokens persisted with an explicit enabled=false (new ID).
func TestPersistSyncSnapshotPreservesExplicitlyDisabledTokens(t *testing.T) {
	ctx := setupProjectTestDB(t)
	_, account := createProjectionFixture(t, ctx)

	legacy := model.SiteToken{
		SiteAccountID: account.ID,
		Name:          "legacy-disabled",
		Token:         "key-legacy-disabled",
		GroupKey:      "default",
		GroupName:     "default",
		Enabled:       false,
		ValueStatus:   model.SiteTokenValueStatusReady,
		Source:        "sync",
	}
	if err := dbpkg.GetDB().WithContext(ctx).Create(&legacy).Error; err != nil {
		t.Fatalf("create legacy token failed: %v", err)
	}
	// Seed the persisted false explicitly because the model carries a
	// database default and a plain GORM Create omits false on INSERT.
	if err := dbpkg.GetDB().WithContext(ctx).Model(&model.SiteToken{}).Where("id = ?", legacy.ID).UpdateColumn("enabled", false).Error; err != nil {
		t.Fatalf("disable legacy token failed: %v", err)
	}

	snapshot := &syncSnapshot{
		accessToken: account.AccessToken,
		tokens: []model.SiteToken{
			// Re-matched against the legacy row by token value: the merged token
			// must inherit Enabled=false from the persisted row (old-ID path).
			{Name: "legacy-disabled", Token: "key-legacy-disabled", GroupKey: "default", GroupName: "default", Enabled: true, Source: "sync"},
			// Brand-new token persisted with an explicit enabled=false (new-ID path).
			{Name: "fresh-disabled", Token: "key-fresh-disabled", GroupKey: "default", GroupName: "default", Enabled: false, Source: "sync"},
			{Name: "enabled-kept", Token: "key-enabled-kept", GroupKey: "default", GroupName: "default", Enabled: true, Source: "sync"},
		},
		status:  model.SiteExecutionStatusSuccess,
		message: "ok",
	}

	if err := persistSyncSnapshot(ctx, account.ID, snapshot); err != nil {
		t.Fatalf("persistSyncSnapshot returned error: %v", err)
	}

	var tokens []model.SiteToken
	if err := dbpkg.GetDB().WithContext(ctx).
		Where("site_account_id = ?", account.ID).
		Order("name ASC").
		Find(&tokens).Error; err != nil {
		t.Fatalf("query reloaded tokens failed: %v", err)
	}
	byName := make(map[string]model.SiteToken, len(tokens))
	for _, token := range tokens {
		byName[token.Name] = token
	}
	if len(byName) != 3 {
		t.Fatalf("expected 3 tokens after sync, got %d: %+v", len(byName), byName)
	}
	for _, name := range []string{"legacy-disabled", "fresh-disabled"} {
		token, ok := byName[name]
		if !ok {
			t.Fatalf("expected token %q to persist after sync", name)
		}
		if token.Enabled {
			t.Fatalf("expected token %q to stay disabled after snapshot persist, got enabled", name)
		}
	}
	if token, ok := byName["enabled-kept"]; !ok || !token.Enabled {
		t.Fatalf("expected enabled-kept token to stay enabled, got %#v", token)
	}
}
