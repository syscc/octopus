package op

import (
	"fmt"
	"strings"
	"testing"

	dbpkg "github.com/bestruirui/octopus/internal/db"
	"github.com/bestruirui/octopus/internal/model"
	logpkg "github.com/bestruirui/octopus/internal/utils/log"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

// TestDBImportPreservesDisabledBooleansFromDump locks in the contract that a
// dump value of false survives import for every boolean column whose database
// default is true. GORM omits zero-valued fields from INSERT when the column
// has a default, so without the explicit write-back the import would silently
// re-enable disabled proxies, channels, channel keys, sites, accounts, tokens
// and API keys.
func TestDBImportPreservesDisabledBooleansFromDump(t *testing.T) {
	ctx := setupBackupTestDB(t)

	dump := &model.DBDump{
		Version: 1,
		ProxyConfigurations: []model.ProxyConfiguration{
			{ID: 1, Name: "disabled-proxy", URL: "http://disabled-proxy.example:8080", Enabled: false},
		},
		Channels: []model.Channel{
			{ID: 1, Name: "disabled-channel", Enabled: false},
		},
		ChannelKeys: []model.ChannelKey{
			{ID: 1, ChannelID: 1, Enabled: false, ChannelKey: "disabled-channel-key"},
		},
		Sites: []model.Site{
			{ID: 1, Name: "disabled-site", Platform: model.SitePlatformNewAPI, BaseURL: "https://disabled.example.com", Enabled: false},
		},
		SiteAccounts: []model.SiteAccount{
			{ID: 1, SiteID: 1, Name: "disabled-account", CredentialType: model.SiteCredentialTypeAPIKey, APIKey: "sk-disabled-account", Enabled: false, AutoSync: false, AutoCheckin: false, RandomCheckin: true, CheckinRandomWindowMinutes: 0},
		},
		SiteTokens: []model.SiteToken{
			{ID: 1, SiteAccountID: 1, Name: "disabled-token", Token: "disabled-token-value", GroupKey: "default", Enabled: false},
		},
		APIKeys: []model.APIKey{
			{ID: 1, Name: "disabled-api-key", APIKey: "sk-octopus-disabled-api-key", Enabled: false},
		},
	}
	if _, err := DBImportIncremental(ctx, dump); err != nil {
		t.Fatalf("DBImportIncremental failed: %v", err)
	}

	var proxy model.ProxyConfiguration
	requireImportedRow(t, &proxy, "name = ?", "disabled-proxy")
	if proxy.Enabled {
		t.Errorf("proxy imported enabled despite dump enabled=false")
	}

	var channel model.Channel
	requireImportedRow(t, &channel, "name = ?", "disabled-channel")
	if channel.Enabled {
		t.Errorf("channel imported enabled despite dump enabled=false")
	}

	var channelKey model.ChannelKey
	requireImportedRow(t, &channelKey, "channel_key = ?", "disabled-channel-key")
	if channelKey.Enabled {
		t.Errorf("channel key imported enabled despite dump enabled=false")
	}

	var site model.Site
	requireImportedRow(t, &site, "base_url = ?", "https://disabled.example.com")
	if site.Enabled {
		t.Errorf("site imported enabled despite dump enabled=false")
	}

	var account model.SiteAccount
	requireImportedRow(t, &account, "name = ?", "disabled-account")
	if account.Enabled {
		t.Errorf("site account imported enabled despite dump enabled=false")
	}
	if account.AutoSync {
		t.Errorf("site account auto_sync imported true despite dump auto_sync=false")
	}
	if account.AutoCheckin {
		t.Errorf("site account auto_checkin imported true despite dump auto_checkin=false")
	}
	if !account.RandomCheckin || account.CheckinRandomWindowMinutes != 0 {
		t.Errorf("site account random check-in window changed, random=%t window=%d", account.RandomCheckin, account.CheckinRandomWindowMinutes)
	}

	var token model.SiteToken
	requireImportedRow(t, &token, "token = ?", "disabled-token-value")
	if token.Enabled {
		t.Errorf("site token imported enabled despite dump enabled=false")
	}

	var apiKey model.APIKey
	requireImportedRow(t, &apiKey, "api_key = ?", "sk-octopus-disabled-api-key")
	if apiKey.Enabled {
		t.Errorf("api key imported enabled despite dump enabled=false")
	}
}

// TestDBImportCloudflareAccountKeepsDisabledFlags verifies the Cloudflare
// branch keeps its platform-forced check-in shutdown while also honoring dump
// values for enabled and auto_sync.
func TestDBImportCloudflareAccountKeepsDisabledFlags(t *testing.T) {
	ctx := setupBackupTestDB(t)

	dump := &model.DBDump{
		Version: 1,
		Sites: []model.Site{
			{ID: 1, Name: "cf-site", Platform: model.SitePlatformCloudflare, BaseURL: "https://api.cloudflare.com/client/v4/accounts/disabled-cf/ai", Enabled: true},
		},
		SiteAccounts: []model.SiteAccount{
			{ID: 1, SiteID: 1, Name: "cf-account", CredentialType: model.SiteCredentialTypeAPIKey, APIKey: "cf-key", Enabled: false, AutoSync: false, AutoCheckin: true, RandomCheckin: true},
		},
	}
	if _, err := DBImportIncremental(ctx, dump); err != nil {
		t.Fatalf("DBImportIncremental failed: %v", err)
	}

	var account model.SiteAccount
	requireImportedRow(t, &account, "name = ?", "cf-account")
	if account.Enabled {
		t.Errorf("cloudflare account imported enabled despite dump enabled=false")
	}
	if account.AutoSync {
		t.Errorf("cloudflare account auto_sync imported true despite dump auto_sync=false")
	}
	if account.AutoCheckin {
		t.Errorf("cloudflare account auto_checkin must stay forced false")
	}
	if account.RandomCheckin {
		t.Errorf("cloudflare account random_checkin must stay forced false")
	}
}

// TestDBImportChildFollowsDumpParentMappingOnIDCollision verifies that child
// rows are attached through the oldID->newID mapping built from the dump
// itself, even when a local row already owns the same numeric parent ID.
func TestDBImportChildFollowsDumpParentMappingOnIDCollision(t *testing.T) {
	ctx := setupBackupTestDB(t)

	// Local rows whose numeric IDs collide with the dump's parent IDs.
	localSite := &model.Site{Name: "local-site", Platform: model.SitePlatformOneAPI, BaseURL: "https://local.example.com", Enabled: true}
	if err := dbpkg.GetDB().Create(localSite).Error; err != nil {
		t.Fatalf("create local site: %v", err)
	}
	localAccount := &model.SiteAccount{SiteID: localSite.ID, Name: "local-account", CredentialType: model.SiteCredentialTypeAPIKey, APIKey: "sk-local-account", Enabled: true}
	if err := dbpkg.GetDB().Create(localAccount).Error; err != nil {
		t.Fatalf("create local account: %v", err)
	}
	if localSite.ID != 1 || localAccount.ID != 1 {
		t.Fatalf("precondition failed: expected local IDs 1/1, got site=%d account=%d", localSite.ID, localAccount.ID)
	}

	dump := &model.DBDump{
		Version: 1,
		Sites: []model.Site{
			{ID: 1, Name: "dump-site", Platform: model.SitePlatformNewAPI, BaseURL: "https://dump.example.com", Enabled: true},
		},
		SiteAccounts: []model.SiteAccount{
			{ID: 1, SiteID: 1, Name: "dump-account", CredentialType: model.SiteCredentialTypeAPIKey, APIKey: "sk-dump-account", Enabled: true},
		},
	}
	if _, err := DBImportIncremental(ctx, dump); err != nil {
		t.Fatalf("DBImportIncremental failed: %v", err)
	}

	var dumpAccount model.SiteAccount
	requireImportedRow(t, &dumpAccount, "name = ?", "dump-account")
	var dumpSite model.Site
	requireImportedRow(t, &dumpSite, "base_url = ?", "https://dump.example.com")
	if dumpAccount.SiteID != dumpSite.ID {
		t.Fatalf("dump account attached to site %d, want freshly created dump site %d (local collision site is %d)", dumpAccount.SiteID, dumpSite.ID, localSite.ID)
	}
	if dumpAccount.SiteID == localSite.ID {
		t.Fatalf("dump account was attached to the local colliding site %d", localSite.ID)
	}

	// The pre-existing local account must remain untouched on the local site.
	var stillLocal model.SiteAccount
	requireImportedRow(t, &stillLocal, "name = ?", "local-account")
	if stillLocal.SiteID != localSite.ID {
		t.Fatalf("local account moved from local site: got site %d, want %d", stillLocal.SiteID, localSite.ID)
	}
}

// TestDBImportRelayLogSkipsUnsafeIDs verifies that relay logs without a
// positive Snowflake ID and duplicate IDs from a damaged dump are dropped
// instead of polluting existing rows or half-applying the batch, and that the
// resulting warnings never contain relay log payloads.
func TestDBImportRelayLogSkipsUnsafeIDs(t *testing.T) {
	ctx := setupBackupTestDB(t)

	core, observed := observer.New(zap.WarnLevel)
	originalLogger := logpkg.Logger
	logpkg.Logger = zap.New(core).Sugar()
	t.Cleanup(func() { logpkg.Logger = originalLogger })

	existing := &model.RelayLog{ID: 1001, Time: 100, RequestModelName: "existing-model", RequestContent: "existing-request-content"}
	if err := dbpkg.GetDB().Create(existing).Error; err != nil {
		t.Fatalf("seed existing relay log: %v", err)
	}

	dump := &model.DBDump{
		Version:     1,
		IncludeLogs: true,
		RelayLogs: []model.RelayLog{
			{ID: 0, Time: 1, RequestModelName: "zero-id"},
			{ID: -7, Time: 2, RequestModelName: "negative-id"},
			{ID: 1001, Time: 3, RequestModelName: "collides-with-existing", RequestContent: "should-not-overwrite"},
			{ID: 2002, Time: 4, RequestModelName: "duplicate-first"},
			{ID: 2002, Time: 5, RequestModelName: "duplicate-second"},
			{ID: 3003, Time: 6, RequestModelName: "valid-snowflake"},
		},
	}
	result, err := DBImportIncremental(ctx, dump)
	if err != nil {
		t.Fatalf("DBImportIncremental failed: %v", err)
	}
	if result.RowsAffected["relay_logs"] != 2 {
		t.Fatalf("expected 2 relay logs imported, got %d", result.RowsAffected["relay_logs"])
	}

	var count int64
	if err := dbpkg.GetDB().Model(&model.RelayLog{}).Count(&count).Error; err != nil {
		t.Fatalf("count relay logs: %v", err)
	}
	if count != 3 { // existing + duplicate-first + valid-snowflake
		t.Fatalf("expected 3 relay logs after import, got %d", count)
	}

	var zero model.RelayLog
	if err := dbpkg.GetDB().Where("id = ?", 0).First(&zero).Error; err == nil {
		t.Fatalf("zero-ID relay log was imported")
	}
	var negative model.RelayLog
	if err := dbpkg.GetDB().Where("id = ?", -7).First(&negative).Error; err == nil {
		t.Fatalf("negative-ID relay log was imported")
	}

	var collided model.RelayLog
	requireImportedRow(t, &collided, "id = ?", int64(1001))
	if collided.RequestModelName != "existing-model" || collided.RequestContent != "existing-request-content" {
		t.Fatalf("existing relay log was overwritten by import: %#v", collided)
	}

	var duplicated model.RelayLog
	requireImportedRow(t, &duplicated, "id = ?", int64(2002))
	if duplicated.RequestModelName != "duplicate-first" {
		t.Fatalf("expected first duplicate row to win, got %q", duplicated.RequestModelName)
	}

	var valid model.RelayLog
	requireImportedRow(t, &valid, "id = ?", int64(3003))

	for _, entry := range observed.All() {
		logged := entry.Message + fmt.Sprint(entry.ContextMap())
		for _, forbidden := range []string{"zero-id", "negative-id", "duplicate-second", "existing-request-content", "should-not-overwrite"} {
			if strings.Contains(logged, forbidden) {
				t.Fatalf("import log exposed relay log payload %q: %s", forbidden, logged)
			}
		}
	}
}

func requireImportedRow(t *testing.T, dest any, query string, args ...any) {
	t.Helper()
	if err := dbpkg.GetDB().Where(query, args...).First(dest).Error; err != nil {
		t.Fatalf("query %q failed: %v", query, err)
	}
}

// TestRemapRelayLogChannelIDsCoversNestedAttempts verifies the remap contract:
// the top-level ChannelId and every nested Attempts[].ChannelID are rewritten
// through the same explicit oldID->newID map; unmapped positive IDs are reset
// to 0 (even when they collide with an unrelated local row) and non-positive
// IDs are left untouched. The returned count covers every cleared attribution.
func TestRemapRelayLogChannelIDsCoversNestedAttempts(t *testing.T) {
	rows := []model.RelayLog{
		{
			ID: 1001, ChannelId: 5, ChannelName: "dump-channel",
			Attempts: []model.ChannelAttempt{
				{ChannelID: 5, AttemptNum: 1},
				{ChannelID: 9, AttemptNum: 2},
				{ChannelID: 0, AttemptNum: 3},
			},
		},
		{
			ID: 1002, ChannelId: 1, ChannelName: "collision",
			Attempts: []model.ChannelAttempt{{ChannelID: 1, AttemptNum: 1}},
		},
	}

	remapped, cleared := remapRelayLogChannelIDs(rows, map[int]int{5: 42})
	if cleared != 3 {
		t.Fatalf("expected 3 cleared attributions, got %d", cleared)
	}
	if len(remapped) != len(rows) {
		t.Fatalf("expected rows returned in place, got %d rows", len(remapped))
	}
	if rows[0].ChannelId != 42 {
		t.Fatalf("expected top-level channel id remapped to 42, got %d", rows[0].ChannelId)
	}
	if rows[0].Attempts[0].ChannelID != 42 {
		t.Fatalf("expected mapped attempt channel id remapped to 42, got %d", rows[0].Attempts[0].ChannelID)
	}
	if rows[0].Attempts[1].ChannelID != 0 {
		t.Fatalf("expected unmapped attempt channel id cleared, got %d", rows[0].Attempts[1].ChannelID)
	}
	if rows[0].Attempts[2].ChannelID != 0 {
		t.Fatalf("expected non-positive attempt channel id untouched, got %d", rows[0].Attempts[2].ChannelID)
	}
	if rows[1].ChannelId != 0 {
		t.Fatalf("expected unmapped top-level channel id colliding with a local row to be cleared, got %d", rows[1].ChannelId)
	}
	if rows[1].Attempts[0].ChannelID != 0 {
		t.Fatalf("expected unmapped attempt channel id colliding with a local row to be cleared, got %d", rows[1].Attempts[0].ChannelID)
	}
	if rows[0].ChannelName != "dump-channel" || rows[1].ChannelName != "collision" {
		t.Fatalf("expected channel names preserved for traceability")
	}
}

// TestDBImportRemapsRelayLogNestedAttemptChannelIDs verifies end to end that a
// relay log imported from a dump gets its top-level and nested attempt channel
// attributions rewritten to the channel ID assigned by this import, and that
// unmapped IDs never attach to a local channel that happens to share the same
// numeric ID.
func TestDBImportRemapsRelayLogNestedAttemptChannelIDs(t *testing.T) {
	ctx := setupBackupTestDB(t)

	// Local channel whose numeric ID would collide with unmapped dump IDs.
	local := &model.Channel{Name: "local-collision-channel", Enabled: true}
	if err := dbpkg.GetDB().Create(local).Error; err != nil {
		t.Fatalf("create local channel: %v", err)
	}

	dump := &model.DBDump{
		Version: 1,
		Channels: []model.Channel{
			{ID: 5, Name: "dump-remap-channel", Enabled: true},
		},
		IncludeLogs: true,
		RelayLogs: []model.RelayLog{
			{
				ID: 4001, Time: 1, RequestModelName: "mapped-log", ChannelId: 5, ChannelName: "dump-remap-channel",
				Attempts: []model.ChannelAttempt{
					{ChannelID: 5, AttemptNum: 1, Status: model.AttemptSuccess},
					{ChannelID: 1, AttemptNum: 2, Status: model.AttemptSuccess},
				},
			},
		},
	}
	if _, err := DBImportIncremental(ctx, dump); err != nil {
		t.Fatalf("DBImportIncremental failed: %v", err)
	}

	var importedChannel model.Channel
	requireImportedRow(t, &importedChannel, "name = ?", "dump-remap-channel")

	var imported model.RelayLog
	requireImportedRow(t, &imported, "id = ?", int64(4001))
	if imported.ChannelId != importedChannel.ID {
		t.Fatalf("expected top-level channel id remapped to %d, got %d", importedChannel.ID, imported.ChannelId)
	}
	if len(imported.Attempts) != 2 {
		t.Fatalf("expected 2 attempts preserved, got %d", len(imported.Attempts))
	}
	if imported.Attempts[0].ChannelID != importedChannel.ID {
		t.Fatalf("expected mapped attempt channel id remapped to %d, got %d", importedChannel.ID, imported.Attempts[0].ChannelID)
	}
	if imported.Attempts[1].ChannelID != 0 {
		t.Fatalf("expected unmapped attempt channel id cleared instead of attaching to local channel %d, got %d", local.ID, imported.Attempts[1].ChannelID)
	}
}
