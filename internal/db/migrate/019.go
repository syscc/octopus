package migrate

import (
	"fmt"

	"github.com/bestruirui/octopus/internal/model"
	"github.com/bestruirui/octopus/internal/transformer/outbound"
	"gorm.io/gorm"
)

func init() {
	RegisterAfterAutoMigration(Migration{
		Version: 19,
		Up:      migrateChannelOpenAIProtocolCapabilities,
	})
}

// migrateChannelOpenAIProtocolCapabilities runs the column backfill and
// Cloudflare normalization in one transaction where the database supports
// transactional DDL. MySQL may implicitly commit ALTER TABLE statements, but
// the migration remains idempotent and safe to resume if a later step fails.
func migrateChannelOpenAIProtocolCapabilities(db *gorm.DB) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	if !db.Migrator().HasTable(&model.Channel{}) {
		return nil
	}
	return db.Transaction(func(tx *gorm.DB) error {
		return migrateChannelOpenAIProtocolCapabilitiesTx(tx)
	})
}

func migrateChannelOpenAIProtocolCapabilitiesTx(db *gorm.DB) error {
	for _, field := range []string{"OpenAIProtocolMode", "OpenAIChatCapability", "OpenAIResponsesCapability"} {
		if db.Migrator().HasColumn(&model.Channel{}, field) {
			continue
		}
		if err := db.Migrator().AddColumn(&model.Channel{}, field); err != nil {
			return err
		}
	}
	if err := db.Model(&model.Channel{}).
		Where("openai_protocol_mode IS NULL OR openai_protocol_mode = ''").
		Update("openai_protocol_mode", model.OpenAIProtocolModeAuto).Error; err != nil {
		return err
	}
	if err := db.Model(&model.Channel{}).
		Where("openai_chat_capability IS NULL OR openai_chat_capability = ''").
		Update("openai_chat_capability", model.OpenAIProtocolCapabilityUnknown).Error; err != nil {
		return err
	}
	if err := db.Model(&model.Channel{}).
		Where("openai_responses_capability IS NULL OR openai_responses_capability = ''").
		Update("openai_responses_capability", model.OpenAIProtocolCapabilityUnknown).Error; err != nil {
		return err
	}

	cloudflareChannelIDSet := make(map[int]struct{})
	var channels []model.Channel
	if err := db.Select("id", "base_urls").Find(&channels).Error; err != nil {
		return err
	}
	for index := range channels {
		for _, baseURL := range channels[index].BaseUrls {
			if _, ok := model.CanonicalCloudflareWorkersAIBaseURL(baseURL.URL); ok {
				cloudflareChannelIDSet[channels[index].ID] = struct{}{}
				break
			}
		}
	}

	if db.Migrator().HasTable(&model.SiteChannelBinding{}) &&
		db.Migrator().HasTable(&model.SiteAccount{}) &&
		db.Migrator().HasTable(&model.Site{}) {
		var boundChannelIDs []int
		if err := db.Table("channels").
			Distinct("channels.id").
			Joins("JOIN site_channel_bindings ON site_channel_bindings.channel_id = channels.id").
			Joins("JOIN site_accounts ON site_accounts.id = site_channel_bindings.site_account_id").
			Joins("JOIN sites ON sites.id = site_accounts.site_id").
			Where("sites.platform = ?", model.SitePlatformCloudflare).
			Pluck("channels.id", &boundChannelIDs).Error; err != nil {
			return err
		}
		for _, channelID := range boundChannelIDs {
			cloudflareChannelIDSet[channelID] = struct{}{}
		}
	}

	cloudflareChannelIDs := make([]int, 0, len(cloudflareChannelIDSet))
	for channelID := range cloudflareChannelIDSet {
		cloudflareChannelIDs = append(cloudflareChannelIDs, channelID)
	}
	if len(cloudflareChannelIDs) == 0 {
		return nil
	}
	// Only retype channels that are already OpenAI text types: converting a
	// non-OpenAI channel (e.g. Anthropic or Gemini behind a gateway) bound to
	// a Cloudflare site to openai_chat would silently break a working setup,
	// so those keep their type and only receive the chat-only protocol state.
	return db.Model(&model.Channel{}).
		Where("id IN ? AND type IN ?", cloudflareChannelIDs, []outbound.OutboundType{
			outbound.OutboundTypeOpenAIChat,
			outbound.OutboundTypeOpenAIResponse,
		}).
		Updates(map[string]any{
			"type":                        outbound.OutboundTypeOpenAIChat,
			"openai_protocol_mode":        model.OpenAIProtocolModeChatOnly,
			"openai_chat_capability":      model.OpenAIProtocolCapabilitySupported,
			"openai_responses_capability": model.OpenAIProtocolCapabilityUnsupported,
		}).Error
}
