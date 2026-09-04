package op

import (
	"archive/zip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/bestruirui/octopus/internal/db"
	"github.com/bestruirui/octopus/internal/model"
	"github.com/bestruirui/octopus/internal/transformer/outbound"
	"github.com/bestruirui/octopus/internal/utils/log"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	dbDumpVersion = 1

	// Keep import batches small enough for SQLite builds with low SQL variable limits.
	// Some exported tables (for example relay_logs) have many columns, so a conservative
	// row count avoids "too many SQL variables" during bulk insert/upsert.
	dbImportBatchSize    = 20
	dbExportLogBatchSize = 1000
)

func DBExportAll(ctx context.Context, includeLogs, includeStats bool) (*model.DBDump, error) {
	conn := db.GetDB().WithContext(ctx)

	d := &model.DBDump{
		Version:      dbDumpVersion,
		ExportedAt:   time.Now().UTC(),
		IncludeLogs:  includeLogs,
		IncludeStats: includeStats,
	}

	if err := conn.Find(&d.Channels).Error; err != nil {
		return nil, fmt.Errorf("export channels: %w", err)
	}
	if err := conn.Find(&d.ChannelKeys).Error; err != nil {
		return nil, fmt.Errorf("export channel_keys: %w", err)
	}
	if err := conn.Find(&d.ProxyConfigurations).Error; err != nil {
		return nil, fmt.Errorf("export proxy_configurations: %w", err)
	}
	if err := conn.Find(&d.Sites).Error; err != nil {
		return nil, fmt.Errorf("export sites: %w", err)
	}
	if err := conn.Find(&d.SiteAccounts).Error; err != nil {
		return nil, fmt.Errorf("export site_accounts: %w", err)
	}
	if err := conn.Find(&d.SiteTokens).Error; err != nil {
		return nil, fmt.Errorf("export site_tokens: %w", err)
	}
	if err := conn.Find(&d.SiteUserGroups).Error; err != nil {
		return nil, fmt.Errorf("export site_user_groups: %w", err)
	}
	if err := conn.Find(&d.SiteModels).Error; err != nil {
		return nil, fmt.Errorf("export site_models: %w", err)
	}
	if err := conn.Find(&d.SiteChannelBindings).Error; err != nil {
		return nil, fmt.Errorf("export site_channel_bindings: %w", err)
	}
	if err := conn.Find(&d.Groups).Error; err != nil {
		return nil, fmt.Errorf("export groups: %w", err)
	}
	if err := conn.Find(&d.GroupItems).Error; err != nil {
		return nil, fmt.Errorf("export group_items: %w", err)
	}
	if err := conn.Find(&d.LLMInfos).Error; err != nil {
		return nil, fmt.Errorf("export llm_infos: %w", err)
	}
	if err := conn.Find(&d.APIKeys).Error; err != nil {
		return nil, fmt.Errorf("export api_keys: %w", err)
	}
	if err := conn.Find(&d.Settings).Error; err != nil {
		return nil, fmt.Errorf("export settings: %w", err)
	}

	if includeStats {
		if err := conn.Find(&d.StatsTotal).Error; err != nil {
			return nil, fmt.Errorf("export stats_total: %w", err)
		}
		if err := conn.Find(&d.StatsDaily).Error; err != nil {
			return nil, fmt.Errorf("export stats_daily: %w", err)
		}
		if err := conn.Find(&d.StatsHourly).Error; err != nil {
			return nil, fmt.Errorf("export stats_hourly: %w", err)
		}
		if err := conn.Find(&d.StatsModel).Error; err != nil {
			return nil, fmt.Errorf("export stats_model: %w", err)
		}
		if err := conn.Find(&d.StatsChannel).Error; err != nil {
			return nil, fmt.Errorf("export stats_channel: %w", err)
		}
		if err := conn.Find(&d.StatsAPIKey).Error; err != nil {
			return nil, fmt.Errorf("export stats_api_key: %w", err)
		}
		if err := conn.Find(&d.StatsSiteModelHourly).Error; err != nil {
			return nil, fmt.Errorf("export stats_site_model_hourly: %w", err)
		}
	}

	if includeLogs {
		if err := exportRelayLogsPaged(ctx, conn, d); err != nil {
			return nil, err
		}
	}

	return d, nil
}

func exportRelayLogsPaged(ctx context.Context, conn *gorm.DB, d *model.DBDump) error {
	var lastID int64
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		var batch []model.RelayLog
		if err := conn.Where("id > ?", lastID).Order("id ASC").Limit(dbExportLogBatchSize).Find(&batch).Error; err != nil {
			return fmt.Errorf("export relay_logs: %w", err)
		}
		if len(batch) == 0 {
			break
		}
		d.RelayLogs = append(d.RelayLogs, batch...)
		lastID = batch[len(batch)-1].ID
		if len(batch) < dbExportLogBatchSize {
			break
		}
	}
	return nil
}

func importedSitePlatform(tx *gorm.DB, siteID int, cache map[int]model.SitePlatform) (model.SitePlatform, bool, error) {
	if platform, ok := cache[siteID]; ok {
		return platform, true, nil
	}
	var site model.Site
	if err := tx.Select("id", "platform").First(&site, siteID).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return "", false, nil
		}
		return "", false, err
	}
	cache[siteID] = site.Platform
	return site.Platform, true, nil
}

func importedAccountPlatform(tx *gorm.DB, accountID int, accountCache map[int]model.SitePlatform, siteCache map[int]model.SitePlatform) (model.SitePlatform, bool, error) {
	if platform, ok := accountCache[accountID]; ok {
		return platform, true, nil
	}
	var account model.SiteAccount
	if err := tx.Select("id", "site_id").First(&account, accountID).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return "", false, nil
		}
		return "", false, err
	}
	platform, ok, err := importedSitePlatform(tx, account.SiteID, siteCache)
	if err != nil || !ok {
		return "", ok, err
	}
	accountCache[accountID] = platform
	return platform, true, nil
}

// isLegacyNonOpenAICloudflareChannel reports whether channel is a historical
// legacy row: a non-OpenAI type pointing at a Cloudflare Workers AI base URL.
// Migration 019 deliberately kept such rows (only OpenAI text channels are
// retyped to chat-only) and ChannelUpdate preserves them on unrelated edits,
// so backup import must restore them unchanged instead of rewriting them to
// chat. Such rows are only honored when the dump still shows their Cloudflare
// binding context; without it the combination is treated as genuinely invalid
// new data and rejected.
func isLegacyNonOpenAICloudflareChannel(channel *model.Channel) bool {
	return channelUsesCloudflareWorkersAI(channel) && !model.IsOpenAITextChannelType(channel.Type)
}

// dumpCloudflareBoundChannelIDs collects dump channel IDs that a site channel
// binding in this dump attaches to a Cloudflare platform site. It is the
// historical-context marker that lets legacy non-OpenAI Cloudflare channels
// pass import unchanged.
func dumpCloudflareBoundChannelIDs(dump *model.DBDump) map[int]struct{} {
	sitePlatformByOldID := make(map[int]model.SitePlatform, len(dump.Sites))
	for i := range dump.Sites {
		sitePlatformByOldID[dump.Sites[i].ID] = dump.Sites[i].Platform
	}
	accountSiteByOldID := make(map[int]int, len(dump.SiteAccounts))
	for i := range dump.SiteAccounts {
		accountSiteByOldID[dump.SiteAccounts[i].ID] = dump.SiteAccounts[i].SiteID
	}
	bound := make(map[int]struct{})
	for i := range dump.SiteChannelBindings {
		binding := dump.SiteChannelBindings[i]
		if sitePlatformByOldID[binding.SiteID] != model.SitePlatformCloudflare ||
			accountSiteByOldID[binding.SiteAccountID] != binding.SiteID {
			continue
		}
		bound[binding.ChannelID] = struct{}{}
	}
	return bound
}

func DBImportIncremental(ctx context.Context, dump *model.DBDump) (*model.DBImportResult, error) {
	if dump == nil {
		return nil, fmt.Errorf("empty dump")
	}

	if dump.Version != 0 && dump.Version != dbDumpVersion {
		return nil, fmt.Errorf("unsupported dump version: %d", dump.Version)
	}

	conn := db.GetDB().WithContext(ctx)
	res := &model.DBImportResult{RowsAffected: map[string]int64{}}

	// Channel IDs touched by this import: dedup-mapped names, freshly created
	// rows and channels whose OpenAI protocol got rewritten by a Cloudflare
	// binding. IDs are collected while the transaction runs, but only consumed
	// after a successful commit, so a rolled-back import never mutates runtime
	// caches.
	touchedChannelIDs := make([]int, 0, len(dump.Channels))
	trackTouchedChannel := func(id int) {
		if id > 0 {
			touchedChannelIDs = append(touchedChannelIDs, id)
		}
	}

	err := conn.Transaction(func(tx *gorm.DB) error {
		channelIDMap := make(map[int]int)
		newChannelIDs := make(map[int]struct{})
		proxyConfigIDMap := make(map[int]int)
		siteIDMap := make(map[int]int)
		accountIDMap := make(map[int]int)
		userGroupIDMap := make(map[int]int)
		groupIDMap := make(map[int]int)
		apiKeyIDMap := make(map[int]int)
		// Platform of every imported site/account, keyed by the ID assigned
		// inside this transaction (existing dedup target or freshly created
		// row). Later child-table loops use it to enforce Cloudflare invariants.
		sitePlatformByImportedID := make(map[int]model.SitePlatform)
		accountPlatformByImportedID := make(map[int]model.SitePlatform)

		cloudflareBoundChannelIDs := dumpCloudflareBoundChannelIDs(dump)
		migrateLegacyDumpProxyFields(dump)

		// 1. ProxyConfigurations (dedup by url; disambiguate name conflicts)
		for i := range dump.ProxyConfigurations {
			proxyConfig := dump.ProxyConfigurations[i]
			oldID := proxyConfig.ID
			proxyConfig.ID = 0
			proxyConfig.ReferenceCount = 0
			if err := proxyConfig.Validate(); err != nil {
				return fmt.Errorf("import proxy_configurations: %w", err)
			}

			var existing model.ProxyConfiguration
			if err := tx.Where("url = ?", proxyConfig.URL).First(&existing).Error; err == nil {
				proxyConfigIDMap[oldID] = existing.ID
				continue
			} else if !errors.Is(err, gorm.ErrRecordNotFound) {
				return fmt.Errorf("import proxy_configurations: %w", err)
			}
			if err := tx.Where("name = ?", proxyConfig.Name).First(&existing).Error; err == nil {
				oldName := proxyConfig.Name
				proxyConfig.Name = uniqueProxyConfigName(proxyConfig.Name, tx)
				log.Warnw("proxy configuration name conflict during import",
					"old_id", oldID,
					"existing_id", existing.ID,
					"old_name", oldName,
					"new_name", proxyConfig.Name,
				)
			} else if !errors.Is(err, gorm.ErrRecordNotFound) {
				return fmt.Errorf("import proxy_configurations: %w", err)
			}
			if err := tx.Create(&proxyConfig).Error; err != nil {
				return fmt.Errorf("import proxy_configurations: %w", err)
			}
			// GORM omits zero-valued (false) fields from the INSERT when the
			// column has a database default, so an exported disabled proxy would
			// be re-enabled. Re-apply the dump value explicitly.
			if !dump.ProxyConfigurations[i].Enabled {
				if err := preserveImportedBooleans(tx, &model.ProxyConfiguration{}, proxyConfig.ID, map[string]any{"enabled": false}); err != nil {
					return fmt.Errorf("import proxy_configurations: %w", err)
				}
			}
			proxyConfigIDMap[oldID] = proxyConfig.ID
			res.RowsAffected["proxy_configurations"]++
		}

		// 2. Channels (dedup by name)
		for i := range dump.Channels {
			ch := dump.Channels[i]
			oldID := ch.ID
			ch.ID = 0
			ch.Keys = nil
			ch.Stats = nil
			remapProxyConfigID(&ch.ProxyMode, &ch.ProxyConfigID, proxyConfigIDMap)
			if _, cloudflareBound := cloudflareBoundChannelIDs[oldID]; isLegacyNonOpenAICloudflareChannel(&ch) && cloudflareBound {
				// Historical backup row: migration 019 kept non-OpenAI types on
				// Cloudflare base URLs and ChannelUpdate preserves them, so a
				// restore must not fail here nor rewrite the row to chat-only.
				// Non-OpenAI types carry no OpenAI protocol state; reset it the
				// same way normalizeChannelProxyFields does for live edits.
				ch.OpenAIProtocolMode = model.OpenAIProtocolModeAuto
				ch.ResetOpenAIProtocolCapabilities()
			} else {
				if err := validateCloudflareChannelType(&ch); err != nil {
					return fmt.Errorf("import channels: %w", err)
				}
				forceCloudflareChannelProtocol(&ch)
			}
			if err := ch.NormalizeOpenAIProtocolSettings(); err != nil {
				return fmt.Errorf("import channels: %w", err)
			}

			var existing model.Channel
			if err := tx.Where("name = ?", ch.Name).First(&existing).Error; err == nil {
				if channelsSameImportIdentity(&existing, &ch) {
					channelIDMap[oldID] = existing.ID
					trackTouchedChannel(existing.ID)
					continue
				}
				oldName := ch.Name
				ch.Name = uniqueChannelName(ch.Name, tx)
				log.Warnw("channel name conflict during import; creating a distinct channel",
					"old_id", oldID,
					"existing_id", existing.ID,
					"old_name", oldName,
					"new_name", ch.Name,
				)
			} else if !errors.Is(err, gorm.ErrRecordNotFound) {
				return fmt.Errorf("import channels: %w", err)
			}
			if err := tx.Omit("Keys", "Stats").Create(&ch).Error; err != nil {
				return fmt.Errorf("import channels: %w", err)
			}
			if !dump.Channels[i].Enabled {
				if err := preserveImportedBooleans(tx, &model.Channel{}, ch.ID, map[string]any{"enabled": false}); err != nil {
					return fmt.Errorf("import channels: %w", err)
				}
			}
			channelIDMap[oldID] = ch.ID
			trackTouchedChannel(ch.ID)
			newChannelIDs[ch.ID] = struct{}{}
			res.RowsAffected["channels"]++
		}

		// 3. ChannelKeys (remap channel_id, dedup by channel_id+channel_key)
		for i := range dump.ChannelKeys {
			key := dump.ChannelKeys[i]
			key.ID = 0
			oldChannelID := key.ChannelID
			newChannelID, ok := channelIDMap[oldChannelID]
			if !ok {
				return fmt.Errorf("import channel_keys: unmapped channel_id %d", oldChannelID)
			}
			if _, createdByImport := newChannelIDs[newChannelID]; !createdByImport {
				continue
			}
			key.ChannelID = newChannelID
			var existing model.ChannelKey
			if err := tx.Where("channel_id = ? AND channel_key = ?", key.ChannelID, key.ChannelKey).First(&existing).Error; err == nil {
				continue
			} else if !errors.Is(err, gorm.ErrRecordNotFound) {
				return fmt.Errorf("import channel_keys: %w", err)
			}
			if err := tx.Create(&key).Error; err != nil {
				return fmt.Errorf("import channel_keys: %w", err)
			}
			if !dump.ChannelKeys[i].Enabled {
				if err := preserveImportedBooleans(tx, &model.ChannelKey{}, key.ID, map[string]any{"enabled": false}); err != nil {
					return fmt.Errorf("import channel_keys: %w", err)
				}
			}
			res.RowsAffected["channel_keys"]++
		}

		// 4. Sites (dedup by platform+base_url)
		for i := range dump.Sites {
			site := dump.Sites[i]
			oldID := site.ID
			site.ID = 0
			site.Accounts = nil
			remapProxyConfigID(&site.ProxyMode, &site.ProxyConfigID, proxyConfigIDMap)

			// site.Validate runs Site.Normalize first: ordinary platforms only
			// trim the base URL (native backups keep their full path), while
			// Cloudflare sites are canonicalized (case/host/default-port and
			// /ai[/v1] depth) and strictly validated against the documented
			// account-scoped Workers AI base.
			if err := site.Validate(); err != nil {
				return fmt.Errorf("import sites: %w", err)
			}

			var existing model.Site
			if err := tx.Where("platform = ? AND base_url = ?", site.Platform, site.BaseURL).First(&existing).Error; err == nil {
				siteIDMap[oldID] = existing.ID
				sitePlatformByImportedID[existing.ID] = existing.Platform
				continue
			} else if !errors.Is(err, gorm.ErrRecordNotFound) {
				return fmt.Errorf("import sites: %w", err)
			}
			site.Name = uniqueSiteName(tx, site.Name)
			if err := tx.Omit("Accounts").Create(&site).Error; err != nil {
				return fmt.Errorf("import sites: %w", err)
			}
			if !dump.Sites[i].Enabled {
				if err := preserveImportedBooleans(tx, &model.Site{}, site.ID, map[string]any{"enabled": false}); err != nil {
					return fmt.Errorf("import sites: %w", err)
				}
			}
			siteIDMap[oldID] = site.ID
			sitePlatformByImportedID[site.ID] = site.Platform
			res.RowsAffected["sites"]++
		}

		// 5. SiteAccounts (remap site_id, dedup by site_id+name)
		for i := range dump.SiteAccounts {
			account := dump.SiteAccounts[i]
			oldID := account.ID
			account.ID = 0
			account.Tokens = nil
			account.UserGroups = nil
			account.Models = nil
			account.ChannelBindings = nil
			remapProxyConfigID(&account.ProxyMode, &account.ProxyConfigID, proxyConfigIDMap)

			oldSiteID := account.SiteID
			newSiteID, ok := siteIDMap[oldSiteID]
			if !ok {
				return fmt.Errorf("import site_accounts: unmapped site_id %d", oldSiteID)
			}
			account.SiteID = newSiteID

			sitePlatform, hasSitePlatform, err := importedSitePlatform(tx, account.SiteID, sitePlatformByImportedID)
			if err != nil {
				return fmt.Errorf("import site_accounts: %w", err)
			}
			if hasSitePlatform {
				if err := normalizeSiteAccountForPlatform(&account, sitePlatform); err != nil {
					return fmt.Errorf("import site_accounts: %w", err)
				}
			}
			if err := account.Validate(); err != nil {
				return fmt.Errorf("import site_accounts: %w", err)
			}

			var existing model.SiteAccount
			if err := tx.Where("site_id = ? AND name = ?", account.SiteID, strings.TrimSpace(account.Name)).First(&existing).Error; err == nil {
				accountIDMap[oldID] = existing.ID
				if hasSitePlatform {
					accountPlatformByImportedID[existing.ID] = sitePlatform
				}
				continue
			} else if !errors.Is(err, gorm.ErrRecordNotFound) {
				return fmt.Errorf("import site_accounts: %w", err)
			}
			if err := tx.Omit("Tokens", "UserGroups", "Models", "ChannelBindings").Create(&account).Error; err != nil {
				return fmt.Errorf("import site_accounts: %w", err)
			}
			// GORM omits zero-valued columns from struct creates when a column
			// default exists (enabled/auto_sync/auto_checkin default to true),
			// so values imported as false must be persisted explicitly.
			// Cloudflare accounts additionally have their check-in flags forced
			// off by normalizeSiteAccountForPlatform regardless of dump values.
			accountFixes := map[string]any{}
			if !dump.SiteAccounts[i].Enabled {
				accountFixes["enabled"] = false
			}
			if !dump.SiteAccounts[i].AutoSync {
				accountFixes["auto_sync"] = false
			}
			if hasSitePlatform && sitePlatform == model.SitePlatformCloudflare {
				accountFixes["auto_checkin"] = false
				accountFixes["random_checkin"] = false
				accountFixes["next_auto_checkin_at"] = nil
			} else if !dump.SiteAccounts[i].AutoCheckin {
				accountFixes["auto_checkin"] = false
			}
			if dump.SiteAccounts[i].CheckinRandomWindowMinutes == 0 {
				accountFixes["checkin_random_window_minutes"] = 0
			}
			if err := preserveImportedBooleans(tx, &model.SiteAccount{}, account.ID, accountFixes); err != nil {
				return fmt.Errorf("import site_accounts: %w", err)
			}
			accountIDMap[oldID] = account.ID
			if hasSitePlatform {
				accountPlatformByImportedID[account.ID] = sitePlatform
			}
			res.RowsAffected["site_accounts"]++
		}

		// 6. SiteTokens (remap site_account_id, dedup by site_account_id+token+group_key)
		for i := range dump.SiteTokens {
			token := dump.SiteTokens[i]
			token.ID = 0
			oldAccountID := token.SiteAccountID
			newAccountID, ok := accountIDMap[oldAccountID]
			if !ok {
				return fmt.Errorf("import site_tokens: unmapped site_account_id %d", oldAccountID)
			}
			token.SiteAccountID = newAccountID
			var existing model.SiteToken
			if err := tx.Where("site_account_id = ? AND token = ? AND group_key = ?", token.SiteAccountID, token.Token, token.GroupKey).First(&existing).Error; err == nil {
				continue
			} else if !errors.Is(err, gorm.ErrRecordNotFound) {
				return fmt.Errorf("import site_tokens: %w", err)
			}
			if err := tx.Create(&token).Error; err != nil {
				return fmt.Errorf("import site_tokens: %w", err)
			}
			if !dump.SiteTokens[i].Enabled {
				if err := preserveImportedBooleans(tx, &model.SiteToken{}, token.ID, map[string]any{"enabled": false}); err != nil {
					return fmt.Errorf("import site_tokens: %w", err)
				}
			}
			res.RowsAffected["site_tokens"]++
		}

		// 7. SiteUserGroups (remap site_account_id, dedup by uniqueIndex)
		for i := range dump.SiteUserGroups {
			group := dump.SiteUserGroups[i]
			oldID := group.ID
			group.ID = 0
			oldAccountID := group.SiteAccountID
			newAccountID, ok := accountIDMap[oldAccountID]
			if !ok {
				return fmt.Errorf("import site_user_groups: unmapped site_account_id %d", oldAccountID)
			}
			group.SiteAccountID = newAccountID
			var existing model.SiteUserGroup
			if err := tx.Where("site_account_id = ? AND group_key = ?", group.SiteAccountID, group.GroupKey).First(&existing).Error; err == nil {
				userGroupIDMap[oldID] = existing.ID
				continue
			} else if !errors.Is(err, gorm.ErrRecordNotFound) {
				return fmt.Errorf("import site_user_groups: %w", err)
			}
			if err := tx.Create(&group).Error; err != nil {
				return fmt.Errorf("import site_user_groups: %w", err)
			}
			userGroupIDMap[oldID] = group.ID
			res.RowsAffected["site_user_groups"]++
		}

		// 8. SiteModels (remap site_account_id, dedup by uniqueIndex)
		for i := range dump.SiteModels {
			m := dump.SiteModels[i]
			m.ID = 0
			oldAccountID := m.SiteAccountID
			newAccountID, ok := accountIDMap[oldAccountID]
			if !ok {
				return fmt.Errorf("import site_models: unmapped site_account_id %d", oldAccountID)
			}
			m.SiteAccountID = newAccountID
			// Cloudflare models are Chat-only: strip any persisted override or
			// legacy raw route metadata before dedup so restored rows cannot
			// re-enter the sync flow as manual overrides.
			sitePlatform, hasAccountPlatform, err := importedAccountPlatform(tx, m.SiteAccountID, accountPlatformByImportedID, sitePlatformByImportedID)
			if err != nil {
				return fmt.Errorf("import site_models: %w", err)
			}
			if hasAccountPlatform && sitePlatform == model.SitePlatformCloudflare {
				m.RouteType = model.SiteModelRouteTypeOpenAIChat
				m.RouteSource = model.SiteModelRouteSourceSyncInferred
				m.ManualOverride = false
				m.RouteRawPayload = ""
			}
			var existing model.SiteModel
			if err := tx.Where("site_account_id = ? AND group_key = ? AND model_name = ?", m.SiteAccountID, m.GroupKey, m.ModelName).First(&existing).Error; err == nil {
				continue
			} else if !errors.Is(err, gorm.ErrRecordNotFound) {
				return fmt.Errorf("import site_models: %w", err)
			}
			if err := tx.Create(&m).Error; err != nil {
				return fmt.Errorf("import site_models: %w", err)
			}
			res.RowsAffected["site_models"]++
		}

		// 9. SiteChannelBindings (remap all FKs, dedup by both unique constraints)
		for i := range dump.SiteChannelBindings {
			binding := dump.SiteChannelBindings[i]
			binding.ID = 0

			oldSiteID := binding.SiteID
			newSiteID, ok := siteIDMap[oldSiteID]
			if !ok {
				return fmt.Errorf("import site_channel_bindings: unmapped site_id %d", oldSiteID)
			}
			oldAccountID := binding.SiteAccountID
			newAccountID, ok := accountIDMap[oldAccountID]
			if !ok {
				return fmt.Errorf("import site_channel_bindings: unmapped site_account_id %d", oldAccountID)
			}
			oldChannelID := binding.ChannelID
			newChannelID, ok := channelIDMap[oldChannelID]
			if !ok {
				return fmt.Errorf("import site_channel_bindings: unmapped channel_id %d", oldChannelID)
			}
			binding.SiteID = newSiteID
			binding.SiteAccountID = newAccountID
			binding.ChannelID = newChannelID
			if binding.SiteUserGroupID != nil {
				oldUserGroupID := *binding.SiteUserGroupID
				newUserGroupID, mapped := userGroupIDMap[oldUserGroupID]
				if !mapped {
					return fmt.Errorf("import site_channel_bindings: unmapped site_user_group_id %d", oldUserGroupID)
				}
				binding.SiteUserGroupID = &newUserGroupID
			}

			// A binding carries both site_id and site_account_id. Verify they
			// resolve to the same parent site after remapping; otherwise a
			// malformed dump could attach an account from one site to another
			// and inherit the wrong Cloudflare policy.
			var accountSite struct {
				SiteID int `gorm:"column:site_id"`
			}
			if err := tx.Model(&model.SiteAccount{}).Select("site_id").Where("id = ?", binding.SiteAccountID).First(&accountSite).Error; err != nil {
				return fmt.Errorf("import site_channel_bindings: %w", err)
			}
			if accountSite.SiteID != binding.SiteID {
				return fmt.Errorf("import site_channel_bindings: site_id %d does not match site account %d parent site %d", binding.SiteID, binding.SiteAccountID, accountSite.SiteID)
			}

			sitePlatform, hasAccountPlatform, err := importedAccountPlatform(tx, binding.SiteAccountID, accountPlatformByImportedID, sitePlatformByImportedID)
			if err != nil {
				return fmt.Errorf("import site_channel_bindings: %w", err)
			}
			cloudflareBinding := hasAccountPlatform && sitePlatform == model.SitePlatformCloudflare
			if cloudflareBinding {
				// Cloudflare accounts support a single unsplit default group:
				// normalize the key and strip any route suffix (e.g. ::anthropic).
				binding.GroupKey = model.NormalizeSiteGroupKey(binding.GroupKey)
				baseKey, _ := model.ParseSiteChannelBindingKey(binding.GroupKey)
				if baseKey == "" {
					return fmt.Errorf("import site_channel_bindings: invalid group key")
				}
				binding.GroupKey = baseKey
			}
			if _, createdByImport := newChannelIDs[binding.ChannelID]; !createdByImport {
				// Existing local channels are never adopted by imported bindings. Still
				// validate Cloudflare protocol compatibility so a malformed dump cannot
				// bypass the same parent/platform checks used for newly created rows.
				if cloudflareBinding {
					var boundChannel model.Channel
					if err := tx.Select("id", "type", "base_urls").First(&boundChannel, binding.ChannelID).Error; err != nil {
						return fmt.Errorf("import site_channel_bindings: %w", err)
					}
					if !isLegacyNonOpenAICloudflareChannel(&boundChannel) && boundChannel.Type != outbound.OutboundTypeOpenAIChat {
						return fmt.Errorf("import site_channel_bindings: cloudflare workers ai binding requires openai chat channel")
					}
				}
				continue
			}

			var existing model.SiteChannelBinding
			if err := tx.Where("site_account_id = ? AND group_key = ?", binding.SiteAccountID, binding.GroupKey).First(&existing).Error; err == nil {
				continue
			} else if !errors.Is(err, gorm.ErrRecordNotFound) {
				return fmt.Errorf("import site_channel_bindings: %w", err)
			}
			if err := tx.Where("channel_id = ?", binding.ChannelID).First(&existing).Error; err == nil {
				continue
			} else if !errors.Is(err, gorm.ErrRecordNotFound) {
				return fmt.Errorf("import site_channel_bindings: %w", err)
			}

			if cloudflareBinding {
				var boundChannel model.Channel
				if err := tx.Select("id", "type", "base_urls").First(&boundChannel, binding.ChannelID).Error; err != nil {
					return fmt.Errorf("import site_channel_bindings: %w", err)
				}
				if !isLegacyNonOpenAICloudflareChannel(&boundChannel) {
					if boundChannel.Type != outbound.OutboundTypeOpenAIChat {
						return fmt.Errorf("import site_channel_bindings: cloudflare workers ai binding requires openai chat channel")
					}
					if err := tx.Model(&model.Channel{}).Where("id = ?", binding.ChannelID).Updates(map[string]any{
						"type":                        outbound.OutboundTypeOpenAIChat,
						"openai_protocol_mode":        model.OpenAIProtocolModeChatOnly,
						"openai_chat_capability":      model.OpenAIProtocolCapabilitySupported,
						"openai_responses_capability": model.OpenAIProtocolCapabilityUnsupported,
					}).Error; err != nil {
						return fmt.Errorf("import site_channel_bindings: %w", err)
					}
					trackTouchedChannel(binding.ChannelID)
				}
				// A historical non-OpenAI Cloudflare channel keeps its type and
				// binding untouched: no chat-only rewrite, no type validation.
			}

			if err := tx.Create(&binding).Error; err != nil {
				return fmt.Errorf("import site_channel_bindings: %w", err)
			}
			res.RowsAffected["site_channel_bindings"]++
		}

		// 10. Groups (dedup by name)
		for i := range dump.Groups {
			g := dump.Groups[i]
			oldID := g.ID
			g.ID = 0
			g.Items = nil

			var existing model.Group
			if err := tx.Where("name = ?", g.Name).First(&existing).Error; err == nil {
				groupIDMap[oldID] = existing.ID
				continue
			} else if !errors.Is(err, gorm.ErrRecordNotFound) {
				return fmt.Errorf("import groups: %w", err)
			}
			if err := tx.Omit("Items").Create(&g).Error; err != nil {
				return fmt.Errorf("import groups: %w", err)
			}
			groupIDMap[oldID] = g.ID
			res.RowsAffected["groups"]++
		}

		// 11. GroupItems (remap group_id+channel_id, dedup by uniqueIndex)
		for i := range dump.GroupItems {
			item := dump.GroupItems[i]
			item.ID = 0
			oldGroupID := item.GroupID
			newGroupID, ok := groupIDMap[oldGroupID]
			if !ok {
				return fmt.Errorf("import group_items: unmapped group_id %d", oldGroupID)
			}
			oldChannelID := item.ChannelID
			newChannelID, ok := channelIDMap[oldChannelID]
			if !ok {
				return fmt.Errorf("import group_items: unmapped channel_id %d", oldChannelID)
			}
			if _, createdByImport := newChannelIDs[newChannelID]; !createdByImport {
				continue
			}
			item.GroupID = newGroupID
			item.ChannelID = newChannelID
			var existing model.GroupItem
			if err := tx.Where("group_id = ? AND channel_id = ? AND model_name = ?", item.GroupID, item.ChannelID, item.ModelName).First(&existing).Error; err == nil {
				continue
			} else if !errors.Is(err, gorm.ErrRecordNotFound) {
				return fmt.Errorf("import group_items: %w", err)
			}
			if err := tx.Create(&item).Error; err != nil {
				return fmt.Errorf("import group_items: %w", err)
			}
			res.RowsAffected["group_items"]++
		}

		// 12. LLMInfos (upsert by name - unchanged)
		if n, err := createUpsertAll(tx, dump.LLMInfos, []clause.Column{{Name: "name"}}); err != nil {
			return fmt.Errorf("import llm_infos: %w", err)
		} else {
			res.RowsAffected["llm_infos"] = n
		}

		// 13. APIKeys (dedup by api_key field)
		for i := range dump.APIKeys {
			key := dump.APIKeys[i]
			oldID := key.ID
			key.ID = 0

			var existing model.APIKey
			if err := tx.Where("api_key = ?", key.APIKey).First(&existing).Error; err == nil {
				apiKeyIDMap[oldID] = existing.ID
				continue
			} else if !errors.Is(err, gorm.ErrRecordNotFound) {
				return fmt.Errorf("import api_keys: %w", err)
			}
			if err := tx.Create(&key).Error; err != nil {
				return fmt.Errorf("import api_keys: %w", err)
			}
			if !dump.APIKeys[i].Enabled {
				if err := preserveImportedBooleans(tx, &model.APIKey{}, key.ID, map[string]any{"enabled": false}); err != nil {
					return fmt.Errorf("import api_keys: %w", err)
				}
			}
			apiKeyIDMap[oldID] = key.ID
			res.RowsAffected["api_keys"]++
		}

		// 14. Settings (upsert by key - unchanged)
		if n, err := createUpsertSettings(tx, dump.Settings); err != nil {
			return fmt.Errorf("import settings: %w", err)
		} else {
			res.RowsAffected["settings"] = n
		}

		// 15. Stats (remap FK IDs, then upsert)
		if dump.IncludeStats {
			if n, err := createDoNothing(tx, dump.StatsTotal); err != nil {
				return fmt.Errorf("import stats_total: %w", err)
			} else {
				res.RowsAffected["stats_total"] = n
			}
			if n, err := createDoNothing(tx, dump.StatsDaily); err != nil {
				return fmt.Errorf("import stats_daily: %w", err)
			} else {
				res.RowsAffected["stats_daily"] = n
			}
			if n, err := createDoNothing(tx, dump.StatsHourly); err != nil {
				return fmt.Errorf("import stats_hourly: %w", err)
			} else {
				res.RowsAffected["stats_hourly"] = n
			}

			// StatsModel: remap ChannelID, clear ID. Skip orphaned rows whose channel
			// is not present in the dump, otherwise SQLite foreign keys can fail.
			filteredStatsModel := make([]model.StatsModel, 0, len(dump.StatsModel))
			seenStatsModel := make(map[struct {
				ChannelID int
				Name      string
			}]struct{})
			for _, row := range dump.StatsModel {
				newID, ok := channelIDMap[row.ChannelID]
				if !ok {
					continue
				}
				row.ID = 0
				row.ChannelID = newID
				key := struct {
					ChannelID int
					Name      string
				}{ChannelID: row.ChannelID, Name: row.Name}
				if _, duplicate := seenStatsModel[key]; duplicate {
					continue
				}
				seenStatsModel[key] = struct{}{}
				var count int64
				if err := tx.Model(&model.StatsModel{}).Where("channel_id = ? AND name = ?", row.ChannelID, row.Name).Count(&count).Error; err != nil {
					return fmt.Errorf("import stats_model: %w", err)
				}
				if count == 0 {
					filteredStatsModel = append(filteredStatsModel, row)
				}
			}
			if n, err := createDoNothing(tx, filteredStatsModel); err != nil {
				return fmt.Errorf("import stats_model: %w", err)
			} else {
				res.RowsAffected["stats_model"] = n
			}

			// StatsChannel: remap ChannelID (which is the PK). Skip orphaned rows whose
			// channel is not present in the dump, otherwise SQLite foreign keys can fail.
			filteredStatsChannel := make([]model.StatsChannel, 0, len(dump.StatsChannel))
			for _, row := range dump.StatsChannel {
				newID, ok := channelIDMap[row.ChannelID]
				if !ok {
					continue
				}
				row.ChannelID = newID
				filteredStatsChannel = append(filteredStatsChannel, row)
			}
			if n, err := createDoNothing(tx, filteredStatsChannel); err != nil {
				return fmt.Errorf("import stats_channel: %w", err)
			} else {
				res.RowsAffected["stats_channel"] = n
			}

			// StatsAPIKey: remap APIKeyID (which is the PK). Skip orphaned rows whose
			// API key is not present in the dump, otherwise SQLite foreign keys can fail.
			filteredStatsAPIKey := make([]model.StatsAPIKey, 0, len(dump.StatsAPIKey))
			for _, row := range dump.StatsAPIKey {
				newID, ok := apiKeyIDMap[row.APIKeyID]
				if !ok {
					continue
				}
				row.APIKeyID = newID
				filteredStatsAPIKey = append(filteredStatsAPIKey, row)
			}
			if n, err := createDoNothing(tx, filteredStatsAPIKey); err != nil {
				return fmt.Errorf("import stats_api_key: %w", err)
			} else {
				res.RowsAffected["stats_api_key"] = n
			}

			// StatsSiteModelHourly: remap SiteAccountID (composite PK)
			filteredSiteModelHourly := make([]model.StatsSiteModelHourly, 0, len(dump.StatsSiteModelHourly))
			for _, row := range dump.StatsSiteModelHourly {
				newID, ok := accountIDMap[row.SiteAccountID]
				if !ok {
					continue
				}
				row.SiteAccountID = newID
				filteredSiteModelHourly = append(filteredSiteModelHourly, row)
			}
			if n, err := createDoNothing(tx, filteredSiteModelHourly); err != nil {
				return fmt.Errorf("import stats_site_model_hourly: %w", err)
			} else {
				res.RowsAffected["stats_site_model_hourly"] = n
			}
		}

		// 16. RelayLogs (Snowflake IDs - keep createDoNothing). Rows without a
		// positive ID and duplicate IDs inside one dump are dropped first: they
		// cannot be addressed reliably afterwards, and conflict clauses alone
		// would leave a damaged dump half-applied or auto-assigned IDs.
		// Channel ownership is then rewritten through the dump channel ID map;
		// rows whose original channel is not part of the dump lose their
		// attribution instead of silently pointing at an unrelated local channel
		// that happens to share the same numeric ID.
		if dump.IncludeLogs {
			safeRelayLogs, skippedUnsafeIDs, skippedDuplicateIDs := sanitizeRelayLogsForImport(dump.RelayLogs)
			safeRelayLogs, clearedChannelIDs := remapRelayLogChannelIDs(safeRelayLogs, channelIDMap)
			if skippedUnsafeIDs > 0 || skippedDuplicateIDs > 0 {
				log.Warnw("dropped unsafe relay log rows during import",
					"operation", "db_import_incremental",
					"skipped_non_positive_ids", skippedUnsafeIDs,
					"skipped_duplicate_ids", skippedDuplicateIDs,
				)
			}
			if clearedChannelIDs > 0 {
				log.Warnw("cleared unmapped channel attribution on relay log rows during import",
					"operation", "db_import_incremental",
					"cleared_channel_ids", clearedChannelIDs,
				)
			}
			if n, err := createDoNothing(tx, safeRelayLogs); err != nil {
				return fmt.Errorf("import relay_logs: %w", err)
			} else {
				res.RowsAffected["relay_logs"] = n
			}
		}

		return nil
	})
	if err != nil {
		return nil, err
	}
	if len(dump.SiteChannelBindings) > 0 {
		// Binding changes affect statistics attribution, including a cached
		// "not found" result for newly imported channels.
		invalidateSiteBindingCache()
	}
	// The import transaction has already committed; cache refresh failures are non-fatal
	// and can be recovered by a later InitCache/refresh cycle.
	refreshImportedChannelCaches(ctx, touchedChannelIDs)
	if err := proxyConfigurationRefreshCache(ctx); err != nil {
		log.Warnw("refresh proxy configuration cache after import failed",
			"operation", "db_import_incremental",
			"error", err,
		)
	}
	return res, nil
}

// refreshImportedChannelCaches reloads the runtime caches (channel row, keys
// and OpenAI protocol fields) of channels touched by a committed import so the
// proxy path immediately honors the restored configuration, then drops stale
// balancer state for them. Failures are non-fatal: rows are already committed
// and a later InitCache/refresh cycle reconciles the cache. Only non-sensitive
// identifiers are logged; key or token material is never included.
func refreshImportedChannelCaches(ctx context.Context, channelIDs []int) {
	if len(channelIDs) == 0 {
		return
	}
	seen := make(map[int]struct{}, len(channelIDs))
	for _, channelID := range channelIDs {
		if channelID <= 0 {
			continue
		}
		if _, ok := seen[channelID]; ok {
			continue
		}
		seen[channelID] = struct{}{}
		if err := channelRefreshCacheByID(channelID, ctx); err != nil {
			log.Warnw("refresh channel cache after import failed",
				"operation", "db_import_incremental",
				"channel_id", channelID,
				"error", err,
			)
		}
	}
	resetBalancerStateForChannels(channelIDs...)
}

func migrateLegacyDumpProxyFields(dump *model.DBDump) {
	if dump == nil {
		return
	}
	proxyIDByURL := make(map[string]int)
	for _, proxyConfig := range dump.ProxyConfigurations {
		if normalized, err := model.NormalizeProxyURL(proxyConfig.URL); err == nil && proxyConfig.ID > 0 {
			proxyIDByURL[normalized] = proxyConfig.ID
		}
	}
	ensureProxyConfig := func(raw string) *int {
		normalized, err := model.NormalizeProxyURL(raw)
		if err != nil {
			return nil
		}
		if id, ok := proxyIDByURL[normalized]; ok {
			return &id
		}
		id := -len(proxyIDByURL) - 1
		proxyIDByURL[normalized] = id
		dump.ProxyConfigurations = append(dump.ProxyConfigurations, model.ProxyConfiguration{
			ID:      id,
			Name:    fmt.Sprintf("Imported Proxy %d", len(proxyIDByURL)),
			URL:     normalized,
			Enabled: true,
			Remark:  "由历史备份代理配置迁移生成",
		})
		return &id
	}
	for i := range dump.Channels {
		ch := &dump.Channels[i]
		if ch.ProxyMode != "" {
			continue
		}
		if !ch.Proxy {
			ch.ProxyMode = model.ProxyUsageModeDirect
			ch.ProxyConfigID = nil
		} else if ch.ChannelProxy != nil && strings.TrimSpace(*ch.ChannelProxy) != "" {
			ch.ProxyMode = model.ProxyUsageModePool
			ch.ProxyConfigID = ensureProxyConfig(*ch.ChannelProxy)
		} else {
			ch.ProxyMode = model.ProxyUsageModeSystem
			ch.ProxyConfigID = nil
		}
	}
	for i := range dump.Sites {
		site := &dump.Sites[i]
		if site.ProxyMode != "" {
			continue
		}
		if site.Proxy {
			if site.SiteProxy != nil && strings.TrimSpace(*site.SiteProxy) != "" {
				site.ProxyMode = model.ProxyUsageModePool
				site.ProxyConfigID = ensureProxyConfig(*site.SiteProxy)
			} else {
				site.ProxyMode = model.ProxyUsageModeSystem
				site.ProxyConfigID = nil
			}
		} else if site.UseSystemProxy {
			site.ProxyMode = model.ProxyUsageModeSystem
			site.ProxyConfigID = nil
		} else {
			site.ProxyMode = model.ProxyUsageModeDirect
			site.ProxyConfigID = nil
		}
	}
	for i := range dump.SiteAccounts {
		account := &dump.SiteAccounts[i]
		if account.ProxyMode != "" {
			continue
		}
		if account.AccountProxy != nil && strings.TrimSpace(*account.AccountProxy) != "" {
			account.ProxyMode = model.ProxyUsageModePool
			account.ProxyConfigID = ensureProxyConfig(*account.AccountProxy)
		} else {
			account.ProxyMode = model.ProxyUsageModeInherit
			account.ProxyConfigID = nil
		}
	}
}

func uniqueProxyConfigName(baseName string, tx *gorm.DB) string {
	baseName = strings.TrimSpace(baseName)
	if baseName == "" {
		baseName = "imported-proxy"
	}
	candidate := baseName
	index := 2
	for {
		var count int64
		if err := tx.Model(&model.ProxyConfiguration{}).Where("name = ?", candidate).Count(&count).Error; err != nil {
			return candidate
		}
		if count == 0 {
			return candidate
		}
		candidate = fmt.Sprintf("%s (%d)", baseName, index)
		index++
	}
}

func channelsSameImportIdentity(left, right *model.Channel) bool {
	if left == nil || right == nil || left.Type != right.Type || len(left.BaseUrls) != len(right.BaseUrls) {
		return false
	}
	normalizeURLs := func(items []model.BaseUrl) []string {
		urls := make([]string, 0, len(items))
		for _, item := range items {
			value := strings.TrimRight(strings.TrimSpace(item.URL), "/")
			if canonical, ok := model.CanonicalCloudflareWorkersAIBaseURL(value); ok {
				value = canonical
			}
			urls = append(urls, value)
		}
		sort.Strings(urls)
		return urls
	}
	leftURLs := normalizeURLs(left.BaseUrls)
	rightURLs := normalizeURLs(right.BaseUrls)
	for index := range leftURLs {
		if leftURLs[index] != rightURLs[index] {
			return false
		}
	}
	return true
}

func uniqueChannelName(baseName string, tx *gorm.DB) string {
	baseName = strings.TrimSpace(baseName)
	if baseName == "" {
		baseName = "imported-channel"
	}
	candidate := baseName
	for index := 2; ; index++ {
		var count int64
		if err := tx.Model(&model.Channel{}).Where("name = ?", candidate).Count(&count).Error; err != nil || count == 0 {
			return candidate
		}
		candidate = fmt.Sprintf("%s (%d)", baseName, index)
	}
}

func remapProxyConfigID(mode *model.ProxyUsageMode, id **int, idMap map[int]int) {
	if mode == nil || id == nil || *mode != model.ProxyUsageModePool {
		if id != nil {
			*id = nil
		}
		return
	}
	if *id == nil {
		log.Warnw("remapProxyConfigID downgraded proxy mode",
			"original_mode", *mode,
			"proxy_config_id", nil,
			"reason", "nil",
		)
		*mode = model.ProxyUsageModeDirect
		*id = nil
		return
	}
	if newID, ok := idMap[**id]; ok {
		*id = &newID
		return
	}
	if **id <= 0 {
		log.Warnw("remapProxyConfigID downgraded proxy mode",
			"original_mode", *mode,
			"proxy_config_id", **id,
			"reason", "invalid",
		)
		*mode = model.ProxyUsageModeDirect
		*id = nil
		return
	}
	log.Warnw("remapProxyConfigID downgraded proxy mode",
		"original_mode", *mode,
		"proxy_config_id", **id,
		"reason", "not found in idMap",
	)
	*mode = model.ProxyUsageModeDirect
	*id = nil
}

func createDoNothing[T any](tx *gorm.DB, rows []T) (int64, error) {
	if len(rows) == 0 {
		return 0, nil
	}
	result := tx.Clauses(clause.OnConflict{DoNothing: true}).CreateInBatches(&rows, dbImportBatchSize)
	return result.RowsAffected, result.Error
}

func createUpsertAll[T any](tx *gorm.DB, rows []T, columns []clause.Column) (int64, error) {
	if len(rows) == 0 {
		return 0, nil
	}
	result := tx.Clauses(clause.OnConflict{
		Columns:   columns,
		UpdateAll: true,
	}).CreateInBatches(&rows, dbImportBatchSize)
	return result.RowsAffected, result.Error
}

// preserveImportedBooleans re-applies dump values for boolean columns after a
// create. GORM omits zero-valued (false) struct fields from the INSERT when
// the column carries a database default (for example "enabled" defaulting to
// true), which would silently re-enable rows exported as disabled. Writing the
// columns back with an explicit UPDATE keeps the dump values and behaves
// identically on SQLite, MySQL and PostgreSQL.
func preserveImportedBooleans(tx *gorm.DB, dest any, id int, columns map[string]any) error {
	if id <= 0 || len(columns) == 0 {
		return nil
	}
	return tx.Model(dest).Where("id = ?", id).UpdateColumns(columns).Error
}

// sanitizeRelayLogsForImport drops relay log rows that cannot be imported
// safely: rows without a positive Snowflake ID, and duplicate IDs inside one
// dump where only the first occurrence wins. Only aggregate counts are
// returned for logging; log payloads never appear in log output.
func sanitizeRelayLogsForImport(rows []model.RelayLog) ([]model.RelayLog, int, int) {
	if len(rows) == 0 {
		return nil, 0, 0
	}
	safe := make([]model.RelayLog, 0, len(rows))
	seen := make(map[int64]struct{}, len(rows))
	skippedUnsafeIDs, skippedDuplicateIDs := 0, 0
	for _, row := range rows {
		if row.ID <= 0 {
			skippedUnsafeIDs++
			continue
		}
		if _, ok := seen[row.ID]; ok {
			skippedDuplicateIDs++
			continue
		}
		seen[row.ID] = struct{}{}
		safe = append(safe, row)
	}
	return safe, skippedUnsafeIDs, skippedDuplicateIDs
}

// remapRelayLogChannelIDs rewrites each relay log's dump channel ID to the ID
// assigned by this import (dedup target or freshly created row). Both the
// top-level attribution and every nested Attempts[].ChannelID are rewritten
// through the same explicit oldID->newID map. Rows whose original channel is
// not part of the dump keep their history but lose the channel attribution
// (IDs reset to 0) instead of silently attaching to an unrelated local channel
// that happens to share the same numeric ID; ChannelName is preserved so the
// rows stay traceable. The returned count covers every cleared attribution
// (top-level rows and nested attempts) for logging; log payloads never appear
// in log output.
func remapRelayLogChannelIDs(rows []model.RelayLog, channelIDMap map[int]int) ([]model.RelayLog, int) {
	cleared := 0
	for i := range rows {
		if rows[i].ChannelId > 0 {
			if newID, ok := channelIDMap[rows[i].ChannelId]; ok {
				rows[i].ChannelId = newID
			} else {
				rows[i].ChannelId = 0
				cleared++
			}
		}
		for j := range rows[i].Attempts {
			if rows[i].Attempts[j].ChannelID <= 0 {
				continue
			}
			if newID, ok := channelIDMap[rows[i].Attempts[j].ChannelID]; ok {
				rows[i].Attempts[j].ChannelID = newID
			} else {
				rows[i].Attempts[j].ChannelID = 0
				cleared++
			}
		}
	}
	return rows, cleared
}

func createUpsertSettings(tx *gorm.DB, rows []model.Setting) (int64, error) {
	if len(rows) == 0 {
		return 0, nil
	}
	result := tx.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "key"}},
		DoUpdates: clause.AssignmentColumns([]string{"value"}),
	}).CreateInBatches(&rows, dbImportBatchSize)
	return result.RowsAffected, result.Error
}

// DBExportZip streams the database dump as a ZIP archive: small tables become
// JSON files, relay_logs become NDJSON to avoid building a giant in-memory
// slice. The writer is consumed once; failures partway through cannot return a
// JSON error to the client, so callers should validate inputs before invoking.
func DBExportZip(ctx context.Context, w io.Writer, includeLogs, includeStats bool) (err error) {
	zw := zip.NewWriter(w)
	defer func() {
		if closeErr := zw.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}()

	conn := db.GetDB().WithContext(ctx)

	manifest := map[string]any{
		"version":       dbDumpVersion,
		"exported_at":   time.Now().UTC().Format(time.RFC3339),
		"include_logs":  includeLogs,
		"include_stats": includeStats,
		"format":        "zip-v1",
	}
	if err := writeZipJSON(zw, "manifest.json", manifest); err != nil {
		return err
	}

	if err := writeZipTable(ctx, zw, conn, "channels.json", &[]model.Channel{}); err != nil {
		return err
	}
	if err := writeZipTable(ctx, zw, conn, "channel_keys.json", &[]model.ChannelKey{}); err != nil {
		return err
	}
	if err := writeZipTable(ctx, zw, conn, "proxy_configurations.json", &[]model.ProxyConfiguration{}); err != nil {
		return err
	}
	if err := writeZipTable(ctx, zw, conn, "sites.json", &[]model.Site{}); err != nil {
		return err
	}
	if err := writeZipTable(ctx, zw, conn, "site_accounts.json", &[]model.SiteAccount{}); err != nil {
		return err
	}
	if err := writeZipTable(ctx, zw, conn, "site_tokens.json", &[]model.SiteToken{}); err != nil {
		return err
	}
	if err := writeZipTable(ctx, zw, conn, "site_user_groups.json", &[]model.SiteUserGroup{}); err != nil {
		return err
	}
	if err := writeZipTable(ctx, zw, conn, "site_models.json", &[]model.SiteModel{}); err != nil {
		return err
	}
	if err := writeZipTable(ctx, zw, conn, "site_channel_bindings.json", &[]model.SiteChannelBinding{}); err != nil {
		return err
	}
	if err := writeZipTable(ctx, zw, conn, "groups.json", &[]model.Group{}); err != nil {
		return err
	}
	if err := writeZipTable(ctx, zw, conn, "group_items.json", &[]model.GroupItem{}); err != nil {
		return err
	}
	if err := writeZipTable(ctx, zw, conn, "llm_infos.json", &[]model.LLMInfo{}); err != nil {
		return err
	}
	if err := writeZipTable(ctx, zw, conn, "api_keys.json", &[]model.APIKey{}); err != nil {
		return err
	}
	if err := writeZipTable(ctx, zw, conn, "settings.json", &[]model.Setting{}); err != nil {
		return err
	}

	if includeStats {
		if err := writeZipTable(ctx, zw, conn, "stats_total.json", &[]model.StatsTotal{}); err != nil {
			return err
		}
		if err := writeZipTable(ctx, zw, conn, "stats_daily.json", &[]model.StatsDaily{}); err != nil {
			return err
		}
		if err := writeZipTable(ctx, zw, conn, "stats_hourly.json", &[]model.StatsHourly{}); err != nil {
			return err
		}
		if err := writeZipTable(ctx, zw, conn, "stats_model.json", &[]model.StatsModel{}); err != nil {
			return err
		}
		if err := writeZipTable(ctx, zw, conn, "stats_channel.json", &[]model.StatsChannel{}); err != nil {
			return err
		}
		if err := writeZipTable(ctx, zw, conn, "stats_api_key.json", &[]model.StatsAPIKey{}); err != nil {
			return err
		}
		if err := writeZipTable(ctx, zw, conn, "stats_site_model_hourly.json", &[]model.StatsSiteModelHourly{}); err != nil {
			return err
		}
	}

	if includeLogs {
		if err := writeZipRelayLogsNDJSON(ctx, zw, conn); err != nil {
			return err
		}
	}

	return nil
}

func writeZipJSON(zw *zip.Writer, name string, value any) error {
	f, err := zw.Create(name)
	if err != nil {
		return fmt.Errorf("zip create %s: %w", name, err)
	}
	enc := json.NewEncoder(f)
	enc.SetIndent("", "")
	if err := enc.Encode(value); err != nil {
		return fmt.Errorf("zip encode %s: %w", name, err)
	}
	return nil
}

func writeZipTable[T any](ctx context.Context, zw *zip.Writer, conn *gorm.DB, name string, dest *[]T) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	if err := conn.Find(dest).Error; err != nil {
		return fmt.Errorf("zip read %s: %w", name, err)
	}
	return writeZipJSON(zw, name, dest)
}

func writeZipRelayLogsNDJSON(ctx context.Context, zw *zip.Writer, conn *gorm.DB) error {
	f, err := zw.Create("relay_logs.ndjson")
	if err != nil {
		return fmt.Errorf("zip create relay_logs.ndjson: %w", err)
	}
	enc := json.NewEncoder(f)
	var lastID int64
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		var batch []model.RelayLog
		if err := conn.Where("id > ?", lastID).Order("id ASC").Limit(dbExportLogBatchSize).Find(&batch).Error; err != nil {
			return fmt.Errorf("zip read relay_logs: %w", err)
		}
		if len(batch) == 0 {
			break
		}
		for i := range batch {
			if err := enc.Encode(&batch[i]); err != nil {
				return fmt.Errorf("zip encode relay_log: %w", err)
			}
		}
		lastID = batch[len(batch)-1].ID
		if len(batch) < dbExportLogBatchSize {
			break
		}
	}
	return nil
}
