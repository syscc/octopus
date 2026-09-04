package op

import (
	"fmt"
	"strings"
	"testing"

	dbpkg "github.com/bestruirui/octopus/internal/db"
	"github.com/bestruirui/octopus/internal/model"
	"github.com/bestruirui/octopus/internal/transformer/outbound"
	logpkg "github.com/bestruirui/octopus/internal/utils/log"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func legacyCloudflareChannelDump(channelType outbound.OutboundType, channelName string, withBinding bool) *model.DBDump {
	dump := &model.DBDump{
		Version:      1,
		IncludeLogs:  false,
		IncludeStats: false,
		Channels: []model.Channel{{
			ID:       30,
			Name:     channelName,
			Type:     channelType,
			Enabled:  true,
			BaseUrls: []model.BaseUrl{{URL: "https://api.cloudflare.com/client/v4/accounts/accLegacyCompat/ai"}},
		}},
	}
	if withBinding {
		dump.ChannelKeys = []model.ChannelKey{
			{ID: 30, ChannelID: 30, Enabled: true, ChannelKey: "sk-legacy-compat"},
		}
		dump.Sites = []model.Site{{
			ID: 1, Name: "legacy-compat-site", Platform: model.SitePlatformCloudflare,
			BaseURL: "https://api.cloudflare.com/client/v4/accounts/accLegacyCompat/ai", Enabled: true,
		}}
		dump.SiteAccounts = []model.SiteAccount{{
			ID: 1, SiteID: 1, Name: "legacy-compat-account",
			CredentialType: model.SiteCredentialTypeAPIKey, APIKey: "legacy-compat-token", Enabled: true,
		}}
		dump.SiteChannelBindings = []model.SiteChannelBinding{{
			ID: 1, SiteID: 1, SiteAccountID: 1, GroupKey: "default::anthropic", ChannelID: 30,
		}}
	}
	return dump
}

// TestDBImportLegacyNonOpenAICloudflareChannelPreservedUnchanged verifies the
// historical Cloudflare compatibility fix: a dump row combining a Cloudflare
// Workers AI base URL with a non-OpenAI channel type plus its associated
// Cloudflare site/account/binding is restored unchanged instead of failing the
// strict validation or being rewritten to a chat-only OpenAI channel.
func TestDBImportLegacyNonOpenAICloudflareChannelPreservedUnchanged(t *testing.T) {
	ctx := setupOpenAIProtocolTestDB(t)

	dump := legacyCloudflareChannelDump(outbound.OutboundTypeAnthropic, "legacy-cf-anthropic", true)
	result, err := DBImportIncremental(ctx, dump)
	if err != nil {
		t.Fatalf("legacy cloudflare channel import failed: %v", err)
	}
	if result.RowsAffected["channels"] != 1 {
		t.Fatalf("expected 1 channel imported, got %d", result.RowsAffected["channels"])
	}

	var legacy model.Channel
	if err := dbpkg.GetDB().Where("name = ?", "legacy-cf-anthropic").First(&legacy).Error; err != nil {
		t.Fatalf("query legacy channel failed: %v", err)
	}
	if legacy.Type != outbound.OutboundTypeAnthropic {
		t.Fatalf("legacy channel type rewritten: want %q got %q", outbound.OutboundTypeAnthropic, legacy.Type)
	}
	if legacy.OpenAIProtocolMode != model.OpenAIProtocolModeAuto {
		t.Fatalf("legacy channel protocol mode rewritten: want auto got %q", legacy.OpenAIProtocolMode)
	}
	if legacy.OpenAIChatCapability != model.OpenAIProtocolCapabilityUnknown || legacy.OpenAIResponsesCapability != model.OpenAIProtocolCapabilityUnknown {
		t.Fatalf("legacy channel capabilities rewritten: chat=%q responses=%q", legacy.OpenAIChatCapability, legacy.OpenAIResponsesCapability)
	}
	if len(legacy.BaseUrls) != 1 || legacy.BaseUrls[0].URL != "https://api.cloudflare.com/client/v4/accounts/accLegacyCompat/ai" {
		t.Fatalf("legacy channel base urls changed: %+v", legacy.BaseUrls)
	}

	var keyCount int64
	if err := dbpkg.GetDB().Model(&model.ChannelKey{}).Where("channel_id = ?", legacy.ID).Count(&keyCount).Error; err != nil {
		t.Fatalf("count legacy channel keys failed: %v", err)
	}
	if keyCount != 1 {
		t.Fatalf("expected 1 channel key remapped to the legacy channel, got %d", keyCount)
	}

	var binding model.SiteChannelBinding
	if err := dbpkg.GetDB().Where("channel_id = ?", legacy.ID).First(&binding).Error; err != nil {
		t.Fatalf("query restored binding failed: %v", err)
	}
	if binding.GroupKey != "default" {
		t.Fatalf("expected binding group key normalized to base key, got %q", binding.GroupKey)
	}

	cached, err := ChannelGet(legacy.ID, ctx)
	if err != nil {
		t.Fatalf("ChannelGet after import failed: %v", err)
	}
	if cached.Type != outbound.OutboundTypeAnthropic || cached.OpenAIProtocolMode != model.OpenAIProtocolModeAuto {
		t.Fatalf("cached legacy channel was rewritten: type=%q mode=%q", cached.Type, cached.OpenAIProtocolMode)
	}
}

// TestDBImportLegacyBindingPreservesExistingLocalChannel verifies the binding
// import path: a Cloudflare binding restored on top of an already existing
// local legacy non-OpenAI Cloudflare channel keeps the channel untouched
// instead of rejecting the import or forcing chat-only protocol state.
func TestDBImportLegacyBindingDoesNotAdoptExistingLocalChannel(t *testing.T) {
	ctx := setupOpenAIProtocolTestDB(t)

	legacy := &model.Channel{
		Name:               "existing-legacy-cf-channel",
		Type:               outbound.OutboundTypeAnthropic,
		Enabled:            true,
		BaseUrls:           []model.BaseUrl{{URL: "https://api.cloudflare.com/client/v4/accounts/accExistingLegacy/ai"}},
		OpenAIProtocolMode: model.OpenAIProtocolModeAuto,
	}
	if err := dbpkg.GetDB().Create(legacy).Error; err != nil {
		t.Fatalf("seed legacy channel directly: %v", err)
	}
	if err := channelRefreshCacheByID(legacy.ID, ctx); err != nil {
		t.Fatalf("refresh legacy channel cache: %v", err)
	}

	dump := legacyCloudflareChannelDump(outbound.OutboundTypeAnthropic, legacy.Name, true)
	dump.Channels[0].BaseUrls[0].URL = legacy.BaseUrls[0].URL
	if _, err := DBImportIncremental(ctx, dump); err != nil {
		t.Fatalf("legacy binding import failed: %v", err)
	}

	var stored model.Channel
	if err := dbpkg.GetDB().First(&stored, legacy.ID).Error; err != nil {
		t.Fatalf("load legacy channel after import: %v", err)
	}
	if stored.Type != outbound.OutboundTypeAnthropic || stored.OpenAIProtocolMode != model.OpenAIProtocolModeAuto {
		t.Fatalf("legacy channel rewritten by binding import: type=%q mode=%q", stored.Type, stored.OpenAIProtocolMode)
	}
	cached, err := ChannelGet(legacy.ID, ctx)
	if err != nil {
		t.Fatalf("ChannelGet after legacy binding import failed: %v", err)
	}
	if cached.Type != outbound.OutboundTypeAnthropic || cached.OpenAIProtocolMode != model.OpenAIProtocolModeAuto {
		t.Fatalf("cached legacy channel rewritten: type=%q mode=%q", cached.Type, cached.OpenAIProtocolMode)
	}

	var bindingCount int64
	if err := dbpkg.GetDB().Model(&model.SiteChannelBinding{}).Where("channel_id = ?", legacy.ID).Count(&bindingCount).Error; err != nil {
		t.Fatalf("count restored bindings failed: %v", err)
	}
	if bindingCount != 0 {
		t.Fatalf("expected existing local legacy channel not to be adopted, got %d bindings", bindingCount)
	}
}

// TestDBImportGenuineInvalidCloudflareCombinationsStillRejected keeps the
// strict create/update semantics for dumps: without the historical Cloudflare
// binding context, both the channel-level (Cloudflare base URL + non-OpenAI
// type) and the binding-level (plain non-OpenAI channel attached to a
// Cloudflare account) invalid combinations are rejected and roll back
// atomically.
func TestDBImportGenuineInvalidCloudflareCombinationsStillRejected(t *testing.T) {
	t.Run("channel without binding context", func(t *testing.T) {
		ctx := setupOpenAIProtocolTestDB(t)
		// Same shape as a hand-crafted dump with a brand-new invalid
		// combination: Cloudflare base URL + non-OpenAI type, but no binding.
		dump := legacyCloudflareChannelDump(outbound.OutboundTypeAnthropic, "invalid-cf-no-binding", false)
		if _, err := DBImportIncremental(ctx, dump); err == nil || !strings.Contains(err.Error(), "requires an openai") {
			t.Fatalf("expected channel-level cloudflare rejection, got %v", err)
		}
		assertLegacyImportRolledBack(t, "channel-without-binding-context")
	})

	t.Run("plain anthropic channel bound to cloudflare account", func(t *testing.T) {
		ctx := setupOpenAIProtocolTestDB(t)
		dump := legacyCloudflareChannelDump(outbound.OutboundTypeOpenAIChat, "plain-chat-channel", true)
		dump.Channels[0].BaseUrls = []model.BaseUrl{{URL: "https://api.example.com/v1"}}
		dump.Channels[0].Type = outbound.OutboundTypeAnthropic
		if _, err := DBImportIncremental(ctx, dump); err == nil || !strings.Contains(err.Error(), "requires openai chat channel") {
			t.Fatalf("expected binding-level cloudflare rejection, got %v", err)
		}
		assertLegacyImportRolledBack(t, "plain-anthropic-bound-to-cloudflare")
	})
}

// TestDBImportRelayLogsRemapChannelOwnership verifies relay log import rewrites
// ChannelId through the dump channel oldID->newID map, never attaches logs to a
// local channel that happens to share the same numeric ID, and keeps the
// existing zero/duplicate ID safety policy.
func TestDBImportRelayLogsRemapChannelOwnership(t *testing.T) {
	ctx := setupOpenAIProtocolTestDB(t)

	core, observed := observer.New(zap.WarnLevel)
	originalLogger := logpkg.Logger
	logpkg.Logger = zap.New(core).Sugar()
	t.Cleanup(func() { logpkg.Logger = originalLogger })

	// The pre-existing local channel occupies ID 1; the dump re-uses old ID 1
	// for a different channel, which is exactly the collision the remap must
	// resolve instead of attaching the log to the local row.
	local := &model.Channel{Name: "local-unrelated", Type: outbound.OutboundTypeOpenAIChat, Enabled: true}
	if err := dbpkg.GetDB().Create(local).Error; err != nil {
		t.Fatalf("create local channel failed: %v", err)
	}
	if local.ID != 1 {
		t.Fatalf("test assumes local channel gets ID 1, got %d", local.ID)
	}

	dump := &model.DBDump{
		Version:      1,
		IncludeLogs:  true,
		IncludeStats: false,
		Channels: []model.Channel{
			{ID: 1, Name: "dump-channel-one", Type: outbound.OutboundTypeOpenAIChat, Enabled: true},
			{ID: 2, Name: "dump-channel-two", Type: outbound.OutboundTypeOpenAIChat, Enabled: true},
		},
		RelayLogs: []model.RelayLog{
			{ID: 1001, Time: 1, RequestModelName: "gpt-test", ChannelId: 1, ChannelName: "dump-channel-one"},
			{ID: 1002, Time: 2, RequestModelName: "gpt-test", ChannelId: 2, ChannelName: "dump-channel-two"},
			{ID: 1003, Time: 3, RequestModelName: "gpt-test", ChannelId: 999, ChannelName: "ghost-channel"},
			{ID: 1004, Time: 4, RequestModelName: "gpt-test", ChannelId: 0},
			{ID: 0, Time: 5, RequestModelName: "gpt-test", ChannelId: 1},
			{ID: 1001, Time: 6, RequestModelName: "gpt-test", ChannelId: 2},
		},
	}

	result, err := DBImportIncremental(ctx, dump)
	if err != nil {
		t.Fatalf("relay log import failed: %v", err)
	}
	if result.RowsAffected["relay_logs"] != 4 {
		t.Fatalf("expected 4 relay logs imported, got %d", result.RowsAffected["relay_logs"])
	}

	var dumpOne, dumpTwo model.Channel
	if err := dbpkg.GetDB().Where("name = ?", "dump-channel-one").First(&dumpOne).Error; err != nil {
		t.Fatalf("query dump-channel-one failed: %v", err)
	}
	if err := dbpkg.GetDB().Where("name = ?", "dump-channel-two").First(&dumpTwo).Error; err != nil {
		t.Fatalf("query dump-channel-two failed: %v", err)
	}

	var logOne, logTwo, logThree, logFour model.RelayLog
	if err := dbpkg.GetDB().Where("id = ?", 1001).First(&logOne).Error; err != nil {
		t.Fatalf("query relay log 1001 failed: %v", err)
	}
	if err := dbpkg.GetDB().Where("id = ?", 1002).First(&logTwo).Error; err != nil {
		t.Fatalf("query relay log 1002 failed: %v", err)
	}
	if err := dbpkg.GetDB().Where("id = ?", 1003).First(&logThree).Error; err != nil {
		t.Fatalf("query relay log 1003 failed: %v", err)
	}
	if err := dbpkg.GetDB().Where("id = ?", 1004).First(&logFour).Error; err != nil {
		t.Fatalf("query relay log 1004 failed: %v", err)
	}

	if logOne.ChannelId != dumpOne.ID {
		t.Fatalf("relay log 1001 not remapped to dump channel: want %d got %d", dumpOne.ID, logOne.ChannelId)
	}
	if logTwo.ChannelId != dumpTwo.ID {
		t.Fatalf("relay log 1002 not remapped to dump channel: want %d got %d", dumpTwo.ID, logTwo.ChannelId)
	}
	if logThree.ChannelId != 0 {
		t.Fatalf("unmapped relay log 1003 must lose channel attribution instead of keeping ID 999, got %d", logThree.ChannelId)
	}
	if logFour.ChannelId != 0 {
		t.Fatalf("relay log 1004 with zero channel must stay zero, got %d", logFour.ChannelId)
	}
	if logOne.RequestModelName != "gpt-test" || logOne.ChannelName != "dump-channel-one" {
		t.Fatalf("relay log row content not preserved: %+v", logOne)
	}

	var attachedToLocal int64
	if err := dbpkg.GetDB().Model(&model.RelayLog{}).Where("channel_id = ?", local.ID).Count(&attachedToLocal).Error; err != nil {
		t.Fatalf("count logs attached to local channel failed: %v", err)
	}
	if attachedToLocal != 0 {
		t.Fatalf("imported logs attached to unrelated local channel %d", local.ID)
	}

	dropped := observed.FilterMessage("dropped unsafe relay log rows during import").All()
	if len(dropped) != 1 {
		t.Fatalf("expected one dropped-rows warning, got %d", len(dropped))
	}
	cleared := observed.FilterMessage("cleared unmapped channel attribution on relay log rows during import").All()
	if len(cleared) != 1 {
		t.Fatalf("expected one cleared-attribution warning, got %d", len(cleared))
	}
	warned := dropped[0].Message + fmt.Sprint(dropped[0].ContextMap()) + cleared[0].Message + fmt.Sprint(cleared[0].ContextMap())
	for _, forbidden := range []string{"gpt-test", "ghost-channel", "dump-channel"} {
		if strings.Contains(warned, forbidden) {
			t.Fatalf("import warning leaked relay log payload/channel data %q: %s", forbidden, warned)
		}
	}
}

// TestDBImportLegacyCloudflareDumpRollsBackAtomically verifies that a failure
// in a later import step aborts the whole transaction, including the restored
// legacy Cloudflare channel, its binding and the relay logs.
func TestDBImportLegacyCloudflareDumpRollsBackAtomically(t *testing.T) {
	ctx := setupOpenAIProtocolTestDB(t)

	dump := legacyCloudflareChannelDump(outbound.OutboundTypeAnthropic, "legacy-cf-rollback", true)
	dump.Channels[0].BaseUrls[0].URL = "https://api.cloudflare.com/client/v4/accounts/accLegacyRollback/ai"
	dump.Sites[0].BaseURL = "https://api.cloudflare.com/client/v4/accounts/accLegacyRollback/ai"
	dump.SiteChannelBindings = append(dump.SiteChannelBindings, model.SiteChannelBinding{
		ID: 2, SiteID: 1, SiteAccountID: 1, GroupKey: "default", ChannelID: 999,
	})
	dump.IncludeLogs = true
	dump.RelayLogs = []model.RelayLog{
		{ID: 2001, Time: 1, RequestModelName: "gpt-rollback", ChannelId: 30},
	}

	if _, err := DBImportIncremental(ctx, dump); err == nil || !strings.Contains(err.Error(), "unmapped") {
		t.Fatalf("expected explicit unmapped foreign key error, got %v", err)
	}
	assertLegacyImportRolledBack(t, "legacy-cloudflare-rollback")

	var logCount int64
	if err := dbpkg.GetDB().Model(&model.RelayLog{}).Count(&logCount).Error; err != nil {
		t.Fatalf("count relay logs after rollback: %v", err)
	}
	if logCount != 0 {
		t.Fatalf("relay logs persisted after rolled back import: %d", logCount)
	}
}

func TestDBImportInvalidatesSiteBindingCacheWithoutAdoptingLocalChannel(t *testing.T) {
	ctx := setupOpenAIProtocolTestDB(t)
	invalidateSiteBindingCache()
	channel := createOpenAIProtocolTestChannel(t, ctx, "binding-cache-channel", outbound.OutboundTypeOpenAIChat, nil)
	if cached, err := lookupChannelSiteBinding(channel.ID); err != nil || cached.Found {
		t.Fatalf("expected an initial cached binding miss, got %#v err=%v", cached, err)
	}
	const sentinelChannelID = 987654
	siteBindingByChannelCache.Set(sentinelChannelID, channelSiteBinding{Found: false})

	dump := &model.DBDump{
		Version:             1,
		Channels:            []model.Channel{{ID: 11, Name: channel.Name, Type: outbound.OutboundTypeOpenAIChat, Enabled: true}},
		Sites:               []model.Site{{ID: 12, Name: "binding-cache-site", Platform: model.SitePlatformNewAPI, BaseURL: "https://binding-cache.example.com", Enabled: true}},
		SiteAccounts:        []model.SiteAccount{{ID: 13, SiteID: 12, Name: "binding-cache-account", CredentialType: model.SiteCredentialTypeAPIKey, APIKey: "binding-cache-key", Enabled: true}},
		SiteChannelBindings: []model.SiteChannelBinding{{ID: 14, SiteID: 12, SiteAccountID: 13, GroupKey: "default", ChannelID: 11}},
	}
	if _, err := DBImportIncremental(ctx, dump); err != nil {
		t.Fatalf("binding import failed: %v", err)
	}
	if _, stillCached := siteBindingByChannelCache.Get(sentinelChannelID); stillCached {
		t.Fatal("binding cache was not invalidated after import")
	}
	cached, err := lookupChannelSiteBinding(channel.ID)
	if err != nil {
		t.Fatalf("lookup local binding failed: %v", err)
	}
	if cached.Found {
		t.Fatalf("existing local channel was unexpectedly adopted: %#v", cached)
	}
}

func TestDBImportRejectsBindingWithMismatchedSiteAndAccountParents(t *testing.T) {
	ctx := setupOpenAIProtocolTestDB(t)
	dump := &model.DBDump{
		Version:  1,
		Channels: []model.Channel{{ID: 21, Name: "mismatched-binding-channel", Type: outbound.OutboundTypeOpenAIChat, Enabled: true}},
		Sites: []model.Site{
			{ID: 22, Name: "mismatched-site-one", Platform: model.SitePlatformNewAPI, BaseURL: "https://mismatch-one.example.com", Enabled: true},
			{ID: 23, Name: "mismatched-site-two", Platform: model.SitePlatformNewAPI, BaseURL: "https://mismatch-two.example.com", Enabled: true},
		},
		SiteAccounts:        []model.SiteAccount{{ID: 24, SiteID: 22, Name: "mismatched-account", CredentialType: model.SiteCredentialTypeAPIKey, APIKey: "mismatched-key", Enabled: true}},
		SiteChannelBindings: []model.SiteChannelBinding{{ID: 25, SiteID: 23, SiteAccountID: 24, GroupKey: "default", ChannelID: 21}},
	}
	if _, err := DBImportIncremental(ctx, dump); err == nil || !strings.Contains(err.Error(), "does not match") {
		t.Fatalf("expected binding parent mismatch rejection, got %v", err)
	}
	assertLegacyImportRolledBack(t, "mismatched-binding-parents")
}

func assertLegacyImportRolledBack(t *testing.T, label string) {
	t.Helper()
	for _, check := range []struct {
		name string
		row  any
	}{
		{"channels", &model.Channel{}},
		{"channel_keys", &model.ChannelKey{}},
		{"sites", &model.Site{}},
		{"site_accounts", &model.SiteAccount{}},
		{"site_channel_bindings", &model.SiteChannelBinding{}},
	} {
		var count int64
		if err := dbpkg.GetDB().Model(check.row).Count(&count).Error; err != nil {
			t.Fatalf("[%s] count %s after rejected import: %v", label, check.name, err)
		}
		if count != 0 {
			t.Fatalf("[%s] rejected import polluted %s: %d rows", label, check.name, count)
		}
	}
}
