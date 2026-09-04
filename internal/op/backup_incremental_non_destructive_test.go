package op

import (
	"testing"

	dbpkg "github.com/bestruirui/octopus/internal/db"
	"github.com/bestruirui/octopus/internal/model"
	"github.com/bestruirui/octopus/internal/transformer/outbound"
)

func TestDBImportIncrementalPreservesExistingStatsOnConflict(t *testing.T) {
	ctx := setupBackupTestDB(t)
	channel := &model.Channel{
		Name: "stats-preserve-channel", Type: outbound.OutboundTypeOpenAIChat, Enabled: true,
		BaseUrls: []model.BaseUrl{{URL: "https://local.example.com/v1"}},
	}
	if err := ChannelCreate(channel, ctx); err != nil {
		t.Fatalf("create local channel: %v", err)
	}
	localMetrics := model.StatsMetrics{InputToken: 100, OutputToken: 50, InputCost: 3.5, RequestSuccess: 10, RequestFailed: 2}
	for _, row := range []any{
		&model.StatsTotal{ID: 1, StatsMetrics: localMetrics},
		&model.StatsDaily{Date: "20260829", StatsMetrics: localMetrics},
		&model.StatsHourly{Hour: 123, Date: "20260829", StatsMetrics: localMetrics},
		&model.StatsChannel{ChannelID: channel.ID, StatsMetrics: localMetrics},
		&model.StatsModel{Name: "model-a", ChannelID: channel.ID, StatsMetrics: localMetrics},
	} {
		if err := dbpkg.GetDB().WithContext(ctx).Create(row).Error; err != nil {
			t.Fatalf("create local stats %T: %v", row, err)
		}
	}

	dumpChannel := *channel
	dumpChannel.ID = 77
	dumpChannel.Keys = nil
	dumpMetrics := model.StatsMetrics{InputToken: 1, OutputToken: 1, InputCost: 0.1, RequestSuccess: 1}
	dump := &model.DBDump{
		Version: dbDumpVersion, IncludeStats: true,
		Channels:     []model.Channel{dumpChannel},
		StatsTotal:   []model.StatsTotal{{ID: 1, StatsMetrics: dumpMetrics}},
		StatsDaily:   []model.StatsDaily{{Date: "20260829", StatsMetrics: dumpMetrics}},
		StatsHourly:  []model.StatsHourly{{Hour: 123, Date: "20260829", StatsMetrics: dumpMetrics}},
		StatsChannel: []model.StatsChannel{{ChannelID: 77, StatsMetrics: dumpMetrics}},
		StatsModel: []model.StatsModel{
			{ID: 900, Name: "model-a", ChannelID: 77, StatsMetrics: dumpMetrics},
			{ID: 901, Name: "model-a", ChannelID: 77, StatsMetrics: dumpMetrics},
		},
	}
	if _, err := DBImportIncremental(ctx, dump); err != nil {
		t.Fatalf("incremental import: %v", err)
	}
	if _, err := DBImportIncremental(ctx, dump); err != nil {
		t.Fatalf("repeated incremental import: %v", err)
	}

	var total model.StatsTotal
	var daily model.StatsDaily
	var hourly model.StatsHourly
	var perChannel model.StatsChannel
	if err := dbpkg.GetDB().WithContext(ctx).First(&total, 1).Error; err != nil {
		t.Fatalf("load total stats: %v", err)
	}
	if err := dbpkg.GetDB().WithContext(ctx).First(&daily, "date = ?", "20260829").Error; err != nil {
		t.Fatalf("load daily stats: %v", err)
	}
	if err := dbpkg.GetDB().WithContext(ctx).First(&hourly, "hour = ?", 123).Error; err != nil {
		t.Fatalf("load hourly stats: %v", err)
	}
	if err := dbpkg.GetDB().WithContext(ctx).First(&perChannel, "channel_id = ?", channel.ID).Error; err != nil {
		t.Fatalf("load channel stats: %v", err)
	}
	var modelRows []model.StatsModel
	if err := dbpkg.GetDB().WithContext(ctx).Where("channel_id = ? AND name = ?", channel.ID, "model-a").Find(&modelRows).Error; err != nil {
		t.Fatalf("load model stats: %v", err)
	}
	if len(modelRows) != 1 || modelRows[0].StatsMetrics != localMetrics {
		t.Fatalf("model stats were duplicated or overwritten: %+v", modelRows)
	}
	for name, got := range map[string]model.StatsMetrics{
		"total": total.StatsMetrics, "daily": daily.StatsMetrics,
		"hourly": hourly.StatsMetrics, "channel": perChannel.StatsMetrics,
	} {
		if got != localMetrics {
			t.Fatalf("%s stats were overwritten: want %+v got %+v", name, localMetrics, got)
		}
	}
}

func TestDBImportIncrementalRenamesConflictingChannelIdentity(t *testing.T) {
	ctx := setupBackupTestDB(t)
	local := &model.Channel{
		Name: "shared-channel-name", Type: outbound.OutboundTypeOpenAIChat, Enabled: true,
		BaseUrls: []model.BaseUrl{{URL: "https://local.example.com/v1"}},
		Keys:     []model.ChannelKey{{Enabled: true, ChannelKey: "local-test-key"}},
	}
	if err := ChannelCreate(local, ctx); err != nil {
		t.Fatalf("create local channel: %v", err)
	}
	dump := &model.DBDump{
		Version: dbDumpVersion,
		Channels: []model.Channel{{
			ID: 88, Name: local.Name, Type: outbound.OutboundTypeOpenAIChat, Enabled: true,
			BaseUrls: []model.BaseUrl{{URL: "https://imported.example.com/v1"}},
		}},
		ChannelKeys: []model.ChannelKey{{ID: 99, ChannelID: 88, Enabled: true, ChannelKey: "imported-test-key"}},
	}
	if _, err := DBImportIncremental(ctx, dump); err != nil {
		t.Fatalf("incremental import: %v", err)
	}

	var unchanged model.Channel
	if err := dbpkg.GetDB().WithContext(ctx).Preload("Keys").First(&unchanged, local.ID).Error; err != nil {
		t.Fatalf("load local channel: %v", err)
	}
	if len(unchanged.Keys) != 1 || unchanged.Keys[0].ChannelKey != "local-test-key" || unchanged.BaseUrls[0].URL != "https://local.example.com/v1" {
		t.Fatalf("local channel adopted imported identity or key: %+v", unchanged)
	}
	var imported model.Channel
	if err := dbpkg.GetDB().WithContext(ctx).Preload("Keys").Where("name = ?", "shared-channel-name (2)").First(&imported).Error; err != nil {
		t.Fatalf("load renamed imported channel: %v", err)
	}
	if imported.ID == local.ID || len(imported.Keys) != 1 || imported.Keys[0].ChannelKey != "imported-test-key" || imported.BaseUrls[0].URL != "https://imported.example.com/v1" {
		t.Fatalf("imported channel was not isolated: %+v", imported)
	}
}

func TestDBImportIncrementalDoesNotAdoptExistingChannel(t *testing.T) {
	ctx := setupBackupTestDB(t)
	baseURL := "https://api.cloudflare.com/client/v4/accounts/accExisting/ai/v1"
	local := &model.Channel{
		Name: "existing-identical-channel", Type: outbound.OutboundTypeOpenAIChat, Enabled: true,
		BaseUrls: []model.BaseUrl{{URL: baseURL}},
		Keys:     []model.ChannelKey{{Enabled: true, ChannelKey: "local-existing-key"}},
	}
	if err := ChannelCreate(local, ctx); err != nil {
		t.Fatalf("create local channel: %v", err)
	}
	dump := &model.DBDump{
		Version:             dbDumpVersion,
		Channels:            []model.Channel{{ID: 101, Name: local.Name, Type: local.Type, Enabled: true, BaseUrls: []model.BaseUrl{{URL: baseURL}}}},
		ChannelKeys:         []model.ChannelKey{{ID: 102, ChannelID: 101, Enabled: true, ChannelKey: "dump-key-must-not-be-adopted"}},
		Sites:               []model.Site{{ID: 201, Name: "imported-cf-site", Platform: model.SitePlatformCloudflare, BaseURL: "https://api.cloudflare.com/client/v4/accounts/accExisting/ai", Enabled: true}},
		SiteAccounts:        []model.SiteAccount{{ID: 202, SiteID: 201, Name: "imported-cf-account", CredentialType: model.SiteCredentialTypeAPIKey, APIKey: "test-account-key", Enabled: true}},
		SiteChannelBindings: []model.SiteChannelBinding{{ID: 203, SiteID: 201, SiteAccountID: 202, ChannelID: 101, GroupKey: model.SiteDefaultGroupKey}},
	}
	if _, err := DBImportIncremental(ctx, dump); err != nil {
		t.Fatalf("incremental import: %v", err)
	}
	var reloaded model.Channel
	if err := dbpkg.GetDB().WithContext(ctx).Preload("Keys").First(&reloaded, local.ID).Error; err != nil {
		t.Fatalf("load local channel: %v", err)
	}
	if len(reloaded.Keys) != 1 || reloaded.Keys[0].ChannelKey != "local-existing-key" {
		t.Fatalf("existing channel adopted dump key: %+v", reloaded.Keys)
	}
	var bindingCount int64
	if err := dbpkg.GetDB().WithContext(ctx).Model(&model.SiteChannelBinding{}).Where("channel_id = ?", local.ID).Count(&bindingCount).Error; err != nil {
		t.Fatalf("count local bindings: %v", err)
	}
	if bindingCount != 0 {
		t.Fatalf("existing channel was adopted as managed: bindings=%d", bindingCount)
	}
}

func TestDBImportIncrementalKeepsExistingProxyDisabled(t *testing.T) {
	ctx := setupBackupTestDB(t)
	proxy := &model.ProxyConfiguration{Name: "local-disabled-proxy", URL: "http://127.0.0.1:18080", Enabled: false}
	if err := dbpkg.GetDB().WithContext(ctx).Create(proxy).Error; err != nil {
		t.Fatalf("create proxy: %v", err)
	}
	if err := dbpkg.GetDB().WithContext(ctx).Model(proxy).UpdateColumn("enabled", false).Error; err != nil {
		t.Fatalf("disable proxy: %v", err)
	}
	dump := &model.DBDump{Version: dbDumpVersion, ProxyConfigurations: []model.ProxyConfiguration{{ID: 301, Name: "dump-enabled-proxy", URL: proxy.URL, Enabled: true}}}
	if _, err := DBImportIncremental(ctx, dump); err != nil {
		t.Fatalf("incremental import: %v", err)
	}
	var reloaded model.ProxyConfiguration
	if err := dbpkg.GetDB().WithContext(ctx).First(&reloaded, proxy.ID).Error; err != nil {
		t.Fatalf("load proxy: %v", err)
	}
	if reloaded.Enabled {
		t.Fatal("incremental import re-enabled a locally disabled proxy")
	}
}

func TestChannelsSameImportIdentityNormalizesURLOrderAndDelay(t *testing.T) {
	left := &model.Channel{Type: outbound.OutboundTypeOpenAIChat, BaseUrls: []model.BaseUrl{
		{URL: "https://b.example.com/v1/", Delay: 1},
		{URL: " https://a.example.com/v1 ", Delay: 2},
	}}
	right := &model.Channel{Type: outbound.OutboundTypeOpenAIChat, BaseUrls: []model.BaseUrl{
		{URL: "https://a.example.com/v1", Delay: 900},
		{URL: "https://b.example.com/v1", Delay: 800},
	}}
	if !channelsSameImportIdentity(left, right) {
		t.Fatal("equivalent URL sets with different order/delay were treated as different identities")
	}
	right.Type = outbound.OutboundTypeOpenAIResponse
	if channelsSameImportIdentity(left, right) {
		t.Fatal("different protocol types were treated as the same identity")
	}
}
