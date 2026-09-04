package op

import (
	"context"
	"testing"

	dbpkg "github.com/bestruirui/octopus/internal/db"
	"github.com/bestruirui/octopus/internal/model"
	"github.com/bestruirui/octopus/internal/transformer/outbound"
)

// resetOpenAIProtocolTestCaches isolates each test from channel cache entries
// left over by earlier tests in the package: the caches are package-global and
// survive the per-test sqlite recreation performed by setupBackupTestDB.
func resetOpenAIProtocolTestCaches() {
	channelCache.Clear()
	channelKeyCache.Clear()
	channelKeyCacheNeedUpdateLock.Lock()
	channelKeyCacheNeedUpdate = make(map[int]struct{})
	channelKeyCacheNeedUpdateLock.Unlock()
}

func setupOpenAIProtocolTestDB(t *testing.T) context.Context {
	t.Helper()
	ctx := setupBackupTestDB(t)
	resetOpenAIProtocolTestCaches()
	return ctx
}

// TestDBImportOpenAIProtocolNewCloudflareChannelCachedAsChatOnly verifies that
// a channel with a Cloudflare Workers AI base URL created by a backup import
// is immediately visible through ChannelGet as Chat-only, with its imported
// key material served from the refreshed cache.
func TestDBImportOpenAIProtocolNewCloudflareChannelCachedAsChatOnly(t *testing.T) {
	ctx := setupOpenAIProtocolTestDB(t)

	dump := &model.DBDump{
		Version: 1,
		Channels: []model.Channel{{
			ID:                 11,
			Name:               "imported-cf-channel",
			Type:               outbound.OutboundTypeOpenAIChat,
			Enabled:            true,
			BaseUrls:           []model.BaseUrl{{URL: "https://api.cloudflare.com/client/v4/accounts/accNew/ai/v1"}},
			OpenAIProtocolMode: model.OpenAIProtocolModeAuto,
		}},
		ChannelKeys: []model.ChannelKey{
			{ID: 1, ChannelID: 11, Enabled: true, ChannelKey: "sk-fake-cf-key"},
		},
	}

	if _, err := DBImportIncremental(ctx, dump); err != nil {
		t.Fatalf("DBImportIncremental failed: %v", err)
	}

	var stored model.Channel
	if err := dbpkg.GetDB().Where("name = ?", "imported-cf-channel").First(&stored).Error; err != nil {
		t.Fatalf("query imported channel failed: %v", err)
	}

	cached, err := ChannelGet(stored.ID, ctx)
	if err != nil {
		t.Fatalf("ChannelGet after import failed: %v", err)
	}
	if cached.Type != outbound.OutboundTypeOpenAIChat {
		t.Fatalf("expected forced openai chat type, got %q", cached.Type)
	}
	if cached.OpenAIProtocolMode != model.OpenAIProtocolModeChatOnly {
		t.Fatalf("expected chat_only protocol mode in cache, got %q", cached.OpenAIProtocolMode)
	}
	if cached.OpenAIChatCapability != model.OpenAIProtocolCapabilitySupported {
		t.Fatalf("expected supported chat capability in cache, got %q", cached.OpenAIChatCapability)
	}
	if cached.OpenAIResponsesCapability != model.OpenAIProtocolCapabilityUnsupported {
		t.Fatalf("expected unsupported responses capability in cache, got %q", cached.OpenAIResponsesCapability)
	}
	if effective := cached.EffectiveOpenAIProtocolCapability(outbound.OutboundTypeOpenAIChat); effective != model.OpenAIProtocolCapabilitySupported {
		t.Fatalf("expected chat to stay supported, got %q", effective)
	}
	if effective := cached.EffectiveOpenAIProtocolCapability(outbound.OutboundTypeOpenAIResponse); effective != model.OpenAIProtocolCapabilityUnsupported {
		t.Fatalf("expected responses to be unsupported, got %q", effective)
	}
	if len(cached.Keys) != 1 || cached.Keys[0].ChannelKey != "sk-fake-cf-key" {
		t.Fatalf("expected imported key in refreshed cache, got %+v", cached.Keys)
	}
}

// TestDBImportOpenAIProtocolCloudflareBindingIsolatesConflictingChannel
// verifies that a same-name dump channel with a different upstream identity
// is renamed and bound independently, leaving the local channel untouched.
func TestDBImportOpenAIProtocolCloudflareBindingIsolatesConflictingChannel(t *testing.T) {
	ctx := setupOpenAIProtocolTestDB(t)

	existing := &model.Channel{
		Name:     "existing-openai-channel",
		Type:     outbound.OutboundTypeOpenAIChat,
		Enabled:  true,
		BaseUrls: []model.BaseUrl{{URL: "https://api.example.com/v1"}},
	}
	if err := ChannelCreate(existing, ctx); err != nil {
		t.Fatalf("create existing channel failed: %v", err)
	}

	dump := &model.DBDump{
		Version: 1,
		Channels: []model.Channel{
			{ID: 21, Name: existing.Name, Type: outbound.OutboundTypeOpenAIChat, Enabled: true},
		},
		Sites: []model.Site{
			{ID: 1, Name: "cf-binding-site", Platform: model.SitePlatformCloudflare, BaseURL: "https://api.cloudflare.com/client/v4/accounts/accBind/ai", Enabled: true},
		},
		SiteAccounts: []model.SiteAccount{
			{ID: 1, SiteID: 1, Name: "cf-binding-account", CredentialType: model.SiteCredentialTypeAPIKey, APIKey: "sk-fake-bind-token", Enabled: true},
		},
		SiteChannelBindings: []model.SiteChannelBinding{
			{ID: 1, SiteID: 1, SiteAccountID: 1, GroupKey: "default", ChannelID: 21},
		},
	}

	if _, err := DBImportIncremental(ctx, dump); err != nil {
		t.Fatalf("DBImportIncremental failed: %v", err)
	}

	var local model.Channel
	if err := dbpkg.GetDB().First(&local, existing.ID).Error; err != nil {
		t.Fatalf("query local channel failed: %v", err)
	}
	if local.OpenAIProtocolMode != model.OpenAIProtocolModeAuto {
		t.Fatalf("conflicting local channel was rewritten, got %q", local.OpenAIProtocolMode)
	}
	var imported model.Channel
	if err := dbpkg.GetDB().Where("name = ?", "existing-openai-channel (2)").First(&imported).Error; err != nil {
		t.Fatalf("query isolated imported channel failed: %v", err)
	}
	if imported.ID == existing.ID || imported.OpenAIProtocolMode != model.OpenAIProtocolModeChatOnly ||
		imported.OpenAIChatCapability != model.OpenAIProtocolCapabilitySupported ||
		imported.OpenAIResponsesCapability != model.OpenAIProtocolCapabilityUnsupported {
		t.Fatalf("isolated imported channel was not forced Chat-only: %+v", imported)
	}
	var binding model.SiteChannelBinding
	if err := dbpkg.GetDB().Where("channel_id = ?", imported.ID).First(&binding).Error; err != nil {
		t.Fatalf("query isolated binding failed: %v", err)
	}
	cachedLocal, err := ChannelGet(existing.ID, ctx)
	if err != nil || cachedLocal.OpenAIProtocolMode != model.OpenAIProtocolModeAuto {
		t.Fatalf("local channel cache was rewritten: channel=%+v err=%v", cachedLocal, err)
	}
	cachedImported, err := ChannelGet(imported.ID, ctx)
	if err != nil || cachedImported.OpenAIProtocolMode != model.OpenAIProtocolModeChatOnly {
		t.Fatalf("imported channel cache was not refreshed: channel=%+v err=%v", cachedImported, err)
	}
}

// TestDBImportOpenAIProtocolFailureLeavesNoCacheResidue verifies that a failed
// import transaction never refreshes runtime caches: the phantom Cloudflare
// channel (which would have been created and forced to Chat-only) stays out of
// the database and the channel/key caches, and a channel that the first
// binding already rewrote inside the transaction keeps its original protocol
// state in both the database and ChannelGet after the rollback.
func TestDBImportOpenAIProtocolFailureLeavesNoCacheResidue(t *testing.T) {
	ctx := setupOpenAIProtocolTestDB(t)

	existing := &model.Channel{
		Name:     "existing-channel-for-rollback",
		Type:     outbound.OutboundTypeOpenAIChat,
		Enabled:  true,
		BaseUrls: []model.BaseUrl{{URL: "https://api.example.com/v1"}},
	}
	if err := ChannelCreate(existing, ctx); err != nil {
		t.Fatalf("create existing channel failed: %v", err)
	}

	dump := &model.DBDump{
		Version: 1,
		Channels: []model.Channel{
			{ID: 31, Name: existing.Name, Type: outbound.OutboundTypeOpenAIChat, Enabled: true},
			{ID: 32, Name: "phantom-cf-channel", Type: outbound.OutboundTypeOpenAIChat, Enabled: true,
				BaseUrls: []model.BaseUrl{{URL: "https://api.cloudflare.com/client/v4/accounts/accPhantom/ai"}}},
		},
		ChannelKeys: []model.ChannelKey{
			{ID: 2, ChannelID: 32, Enabled: true, ChannelKey: "sk-fake-phantom-key"},
		},
		Sites: []model.Site{
			{ID: 1, Name: "cf-rollback-site", Platform: model.SitePlatformCloudflare, BaseURL: "https://api.cloudflare.com/client/v4/accounts/accRollback/ai", Enabled: true},
		},
		SiteAccounts: []model.SiteAccount{
			{ID: 1, SiteID: 1, Name: "cf-rollback-account", CredentialType: model.SiteCredentialTypeAPIKey, APIKey: "sk-fake-rollback-token", Enabled: true},
		},
		SiteChannelBindings: []model.SiteChannelBinding{
			// The first binding rewrites the existing channel to Chat-only and
			// would be created inside the transaction.
			{ID: 1, SiteID: 1, SiteAccountID: 1, GroupKey: "default", ChannelID: 31},
			// The second binding points at a channel that does not exist, which
			// must abort the whole import transaction.
			{ID: 2, SiteID: 1, SiteAccountID: 1, GroupKey: "default", ChannelID: 999},
		},
	}

	if _, err := DBImportIncremental(ctx, dump); err == nil {
		t.Fatalf("expected import to fail for binding to missing channel, got nil error")
	}

	var phantomCount int64
	if err := dbpkg.GetDB().Model(&model.Channel{}).Where("name = ?", "phantom-cf-channel").Count(&phantomCount).Error; err != nil {
		t.Fatalf("count phantom channels failed: %v", err)
	}
	if phantomCount != 0 {
		t.Fatalf("phantom channel persisted after rollback, got %d rows", phantomCount)
	}
	for id, cached := range channelCache.GetAll() {
		if cached.Name == "phantom-cf-channel" {
			t.Fatalf("phantom channel leaked into channel cache at id %d", id)
		}
	}
	for id, key := range channelKeyCache.GetAll() {
		if key.ChannelKey == "sk-fake-phantom-key" {
			t.Fatalf("phantom channel key leaked into key cache at id %d", id)
		}
	}

	var persisted model.Channel
	if err := dbpkg.GetDB().First(&persisted, existing.ID).Error; err != nil {
		t.Fatalf("query existing channel failed: %v", err)
	}
	if persisted.OpenAIProtocolMode != model.OpenAIProtocolModeAuto {
		t.Fatalf("expected existing channel to keep auto mode in db after rollback, got %q", persisted.OpenAIProtocolMode)
	}
	cached, err := ChannelGet(existing.ID, ctx)
	if err != nil {
		t.Fatalf("ChannelGet after rollback failed: %v", err)
	}
	if cached.OpenAIProtocolMode != model.OpenAIProtocolModeAuto {
		t.Fatalf("expected existing channel to keep auto mode in cache after rollback, got %q", cached.OpenAIProtocolMode)
	}

	var bindingCount int64
	if err := dbpkg.GetDB().Model(&model.SiteChannelBinding{}).Count(&bindingCount).Error; err != nil {
		t.Fatalf("count bindings failed: %v", err)
	}
	if bindingCount != 0 {
		t.Fatalf("expected no bindings persisted after rollback, got %d", bindingCount)
	}
}
