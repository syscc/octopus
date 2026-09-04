package op

import (
	"strings"
	"testing"

	dbpkg "github.com/bestruirui/octopus/internal/db"
	"github.com/bestruirui/octopus/internal/model"
	"github.com/bestruirui/octopus/internal/transformer/outbound"
)

func TestChannelCreateRejectsCloudflareURLForNonOpenAIType(t *testing.T) {
	ctx := setupOpenAIProtocolTestDB(t)
	channel := &model.Channel{
		Name: "invalid-cloudflare-create-type", Type: outbound.OutboundTypeAnthropic, Enabled: true,
		BaseUrls: []model.BaseUrl{{URL: "https://api.cloudflare.com/client/v4/accounts/createType/ai"}},
	}
	if err := ChannelCreate(channel, ctx); err == nil || !strings.Contains(err.Error(), "requires an openai") {
		t.Fatalf("expected non-OpenAI Cloudflare channel create to fail, got %v", err)
	}
	var count int64
	if err := dbpkg.GetDB().Model(&model.Channel{}).Where("name = ?", channel.Name).Count(&count).Error; err != nil {
		t.Fatalf("count rejected channel: %v", err)
	}
	if count != 0 {
		t.Fatalf("rejected non-OpenAI Cloudflare channel persisted: %d", count)
	}
}

func TestChannelUpdateRejectsAddingCloudflareURLToNonOpenAIType(t *testing.T) {
	ctx := setupOpenAIProtocolTestDB(t)
	channel := &model.Channel{
		Name: "invalid-cloudflare-update-type", Type: outbound.OutboundTypeAnthropic, Enabled: true,
		BaseUrls: []model.BaseUrl{{URL: "https://anthropic.example.com/v1"}},
	}
	if err := ChannelCreate(channel, ctx); err != nil {
		t.Fatalf("create ordinary Anthropic channel: %v", err)
	}

	cloudflareURLs := []model.BaseUrl{{URL: "https://api.cloudflare.com/client/v4/accounts/updateType/ai"}}
	if _, err := ChannelUpdate(&model.ChannelUpdateRequest{ID: channel.ID, BaseUrls: &cloudflareURLs}, ctx); err == nil || !strings.Contains(err.Error(), "requires an openai") {
		t.Fatalf("expected non-OpenAI Cloudflare URL update to fail, got %v", err)
	}

	var stored model.Channel
	if err := dbpkg.GetDB().First(&stored, channel.ID).Error; err != nil {
		t.Fatalf("load channel after rejected update: %v", err)
	}
	if stored.Type != outbound.OutboundTypeAnthropic || len(stored.BaseUrls) != 1 || stored.BaseUrls[0].URL != "https://anthropic.example.com/v1" {
		t.Fatalf("rejected update changed persisted channel: %+v", stored)
	}
	cached, err := ChannelGet(channel.ID, ctx)
	if err != nil {
		t.Fatalf("load cached channel after rejected update: %v", err)
	}
	if cached.Type != outbound.OutboundTypeAnthropic || len(cached.BaseUrls) != 1 || cached.BaseUrls[0].URL != "https://anthropic.example.com/v1" {
		t.Fatalf("rejected update changed cached channel: %+v", cached)
	}
}

func TestChannelUpdateAllowsUnrelatedEditOnLegacyNonOpenAICloudflareRow(t *testing.T) {
	ctx := setupOpenAIProtocolTestDB(t)
	legacy := &model.Channel{
		Name: "legacy-non-openai-cloudflare", Type: outbound.OutboundTypeAnthropic, Enabled: true,
		BaseUrls:           []model.BaseUrl{{URL: "https://api.cloudflare.com/client/v4/accounts/legacyType/ai"}},
		OpenAIProtocolMode: model.OpenAIProtocolModeAuto,
	}
	if err := dbpkg.GetDB().Create(legacy).Error; err != nil {
		t.Fatalf("seed legacy channel directly: %v", err)
	}
	if err := channelRefreshCacheByID(legacy.ID, ctx); err != nil {
		t.Fatalf("refresh legacy channel cache: %v", err)
	}

	newName := "legacy-non-openai-cloudflare-renamed"
	if _, err := ChannelUpdate(&model.ChannelUpdateRequest{ID: legacy.ID, Name: &newName}, ctx); err != nil {
		t.Fatalf("unrelated legacy channel edit should remain available: %v", err)
	}
	stored, err := ChannelGet(legacy.ID, ctx)
	if err != nil {
		t.Fatalf("load edited legacy channel: %v", err)
	}
	if stored.Name != newName || stored.Type != outbound.OutboundTypeAnthropic || stored.OpenAIProtocolMode.Normalize() != model.OpenAIProtocolModeAuto {
		t.Fatalf("legacy edit changed protocol identity: %+v", stored)
	}
}

func TestDBImportRejectsCloudflareURLForNonOpenAIChannel(t *testing.T) {
	ctx := setupBackupTestDB(t)
	dump := &model.DBDump{Version: 1, Channels: []model.Channel{{
		ID: 1, Name: "invalid-cloudflare-import-type", Type: outbound.OutboundTypeAnthropic, Enabled: true,
		BaseUrls: []model.BaseUrl{{URL: "https://api.cloudflare.com/client/v4/accounts/importType/ai"}},
	}}}
	if _, err := DBImportIncremental(ctx, dump); err == nil || !strings.Contains(err.Error(), "requires an openai") {
		t.Fatalf("expected non-OpenAI Cloudflare channel import to fail, got %v", err)
	}
	var count int64
	if err := dbpkg.GetDB().Model(&model.Channel{}).Count(&count).Error; err != nil {
		t.Fatalf("count channels after rejected import: %v", err)
	}
	if count != 0 {
		t.Fatalf("rejected non-OpenAI Cloudflare import persisted channels: %d", count)
	}
}
