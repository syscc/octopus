package op

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/bestruirui/octopus/internal/db"
	"github.com/bestruirui/octopus/internal/model"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

func SiteList(ctx context.Context) ([]model.Site, error) {
	var sites []model.Site
	if err := db.GetDB().WithContext(ctx).
		Preload("Accounts").
		Preload("Accounts.Tokens").
		Preload("Accounts.UserGroups").
		Preload("Accounts.Models").
		Preload("Accounts.ChannelBindings").
		Where("archived = ?", false).
		Order("is_pinned DESC, sort_order ASC, id ASC").
		Find(&sites).Error; err != nil {
		return nil, err
	}
	for i := range sites {
		normalizeSiteProxyFields(&sites[i])
	}
	return sites, nil
}

func SiteListArchived(ctx context.Context) ([]model.Site, error) {
	var sites []model.Site
	if err := db.GetDB().WithContext(ctx).
		Preload("Accounts").
		Preload("Accounts.Tokens").
		Preload("Accounts.UserGroups").
		Preload("Accounts.Models").
		Preload("Accounts.ChannelBindings").
		Where("archived = ?", true).
		Order("archived_at DESC, id ASC").
		Find(&sites).Error; err != nil {
		return nil, err
	}
	for i := range sites {
		normalizeSiteProxyFields(&sites[i])
	}
	return sites, nil
}

func SiteGet(id int, ctx context.Context) (*model.Site, error) {
	var site model.Site
	if err := db.GetDB().WithContext(ctx).
		Preload("Accounts").
		Preload("Accounts.Tokens").
		Preload("Accounts.UserGroups").
		Preload("Accounts.Models").
		Preload("Accounts.ChannelBindings").
		First(&site, id).Error; err != nil {
		return nil, err
	}
	normalizeSiteProxyFields(&site)
	return &site, nil
}

func normalizeSiteProxyFields(site *model.Site) {
	if site == nil {
		return
	}
	if site.ProxyMode == "" {
		site.ProxyMode = model.ProxyUsageModeDirect
	}
	if site.ProxyMode != model.ProxyUsageModePool {
		site.ProxyConfigID = nil
	}
	site.Proxy = site.ProxyMode != model.ProxyUsageModeDirect
	site.UseSystemProxy = site.ProxyMode == model.ProxyUsageModeSystem
	site.SiteProxy = nil
	for i := range site.Accounts {
		normalizeSiteAccountProxyFields(&site.Accounts[i])
	}
}

func normalizeSiteAccountProxyFields(account *model.SiteAccount) {
	if account == nil {
		return
	}
	if account.ProxyMode == "" {
		account.ProxyMode = model.ProxyUsageModeInherit
	}
	if account.ProxyMode != model.ProxyUsageModePool {
		account.ProxyConfigID = nil
	}
	account.AccountProxy = nil
}

func SiteCreate(site *model.Site, ctx context.Context) error {
	if site == nil {
		return fmt.Errorf("site is nil")
	}
	if err := site.Validate(); err != nil {
		return err
	}
	// Accounts nested in client payloads must never be cascade-inserted: they
	// would bypass SiteAccountCreate's ownership, platform, credential and
	// proxy validation. Accounts are only created through SiteAccountCreate.
	site.Accounts = nil
	if site.ProxyMode == model.ProxyUsageModePool && site.ProxyConfigID != nil {
		if _, err := ProxyURLForConfig(*site.ProxyConfigID, ctx); err != nil {
			return err
		}
	}
	if site.EnabledSet && !site.Enabled {
		err := db.GetDB().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			if err := tx.Omit(clause.Associations).Create(site).Error; err != nil {
				return err
			}
			return tx.Model(&model.Site{}).Where("id = ?", site.ID).Update("enabled", false).Error
		})
		site.Enabled = false
		return err
	}
	return db.GetDB().WithContext(ctx).Omit(clause.Associations).Create(site).Error
}

func appendUniqueString(items []string, value string) []string {
	for _, item := range items {
		if item == value {
			return items
		}
	}
	return append(items, value)
}
func lockSiteByID(tx *gorm.DB, siteID int) (*model.Site, error) {
	if tx == nil {
		return nil, fmt.Errorf("database transaction is nil")
	}
	var site model.Site
	result := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&site, siteID)
	if result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return nil, fmt.Errorf("site not found")
		}
		return nil, result.Error
	}
	return &site, nil
}

func validateProxyConfigurationTx(tx *gorm.DB, proxyMode model.ProxyUsageMode, proxyConfigID *int) error {
	if proxyMode != model.ProxyUsageModePool || proxyConfigID == nil {
		return nil
	}
	var config model.ProxyConfiguration
	if err := tx.Select("id", "enabled").First(&config, *proxyConfigID).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return fmt.Errorf("proxy configuration not found")
		}
		return err
	}
	if !config.Enabled {
		return fmt.Errorf("proxy configuration is disabled")
	}
	return nil
}

func mergeSiteUpdateRequest(site model.Site, req *model.SiteUpdateRequest) (model.Site, []string, error) {
	merged := site
	selectFields := make([]string, 0, 16)
	if req.Name != nil {
		merged.Name = *req.Name
		selectFields = append(selectFields, "name")
	}
	if req.Platform != nil {
		merged.Platform = *req.Platform
		selectFields = append(selectFields, "platform")
	}
	if req.BaseURL != nil {
		merged.BaseURL = *req.BaseURL
		selectFields = append(selectFields, "base_url")
	}
	if req.DefaultRouteType != nil {
		// 空串是合法输入，含义是"清空，交给下次同步重新探测"。只有 API 直连会用到
		// 这个状态；其它平台的空串由 ResolveDefaultRouteType 兜回 openai_chat，
		// Cloudflare 还会被 Normalize 强制写回。非空值仍必须是可投影的路由类型。
		if *req.DefaultRouteType != "" && !model.IsProjectedSiteModelRouteType(*req.DefaultRouteType) {
			return model.Site{}, nil, fmt.Errorf("invalid default route type")
		}
		merged.DefaultRouteType = *req.DefaultRouteType
		selectFields = append(selectFields, "default_route_type")
	}
	if req.Enabled != nil {
		merged.Enabled = *req.Enabled
		selectFields = append(selectFields, "enabled")
	}
	if req.ProxyMode != nil {
		merged.ProxyMode = *req.ProxyMode
		selectFields = append(selectFields, "proxy_mode")
	}
	if req.ProxyConfigIDSet || (req.ProxyMode != nil && *req.ProxyMode != model.ProxyUsageModePool) {
		if req.ProxyMode != nil && *req.ProxyMode != model.ProxyUsageModePool {
			merged.ProxyConfigID = nil
		} else {
			merged.ProxyConfigID = req.ProxyConfigID
		}
		selectFields = append(selectFields, "proxy_config_id")
	}
	if req.ExternalCheckinSet {
		merged.ExternalCheckinURL = req.ExternalCheckinURL
		selectFields = append(selectFields, "external_checkin_url")
	}
	if req.IsPinned != nil {
		merged.IsPinned = *req.IsPinned
		selectFields = append(selectFields, "is_pinned")
	}
	if req.SortOrder != nil {
		merged.SortOrder = *req.SortOrder
		selectFields = append(selectFields, "sort_order")
	}
	if req.GlobalWeight != nil {
		merged.GlobalWeight = *req.GlobalWeight
		selectFields = append(selectFields, "global_weight")
	}
	if req.CustomHeader != nil {
		merged.CustomHeader = *req.CustomHeader
		selectFields = append(selectFields, "custom_header")
	}
	if req.RouteBaseURLs != nil {
		merged.RouteBaseURLs = *req.RouteBaseURLs
		selectFields = append(selectFields, "route_base_urls")
	}
	if req.Tags != nil {
		merged.Tags = *req.Tags
		selectFields = append(selectFields, "tags")
	}
	if len(selectFields) == 0 {
		return merged, nil, nil
	}
	if err := merged.Validate(); err != nil {
		return model.Site{}, nil, err
	}
	if merged.Platform == model.SitePlatformCloudflare {
		// Validate canonicalizes Cloudflare host/fixed path/default port in
		// memory. Persist that value even when the request only switched the
		// platform, otherwise the stored URL can remain a valid-but-broken
		// mixed-case endpoint and bypass canonical deduplication.
		selectFields = appendUniqueString(selectFields, "base_url")
		selectFields = appendUniqueString(selectFields, "default_route_type")
		selectFields = appendUniqueString(selectFields, "external_checkin_url")
		selectFields = appendUniqueString(selectFields, "route_base_urls")
	}
	return merged, selectFields, nil
}

func SiteUpdate(req *model.SiteUpdateRequest, ctx context.Context) (*model.Site, error) {
	if req == nil {
		return nil, fmt.Errorf("site update request is nil")
	}

	err := db.GetDB().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		current, err := lockSiteByID(tx, req.ID)
		if err != nil {
			return err
		}
		merged, selectFields, err := mergeSiteUpdateRequest(*current, req)
		if err != nil {
			return err
		}
		if len(selectFields) == 0 {
			return nil
		}
		if err := validateProxyConfigurationTx(tx, merged.ProxyMode, merged.ProxyConfigID); err != nil {
			return err
		}

		switchingToCloudflare := current.Platform != model.SitePlatformCloudflare && merged.Platform == model.SitePlatformCloudflare
		if switchingToCloudflare {
			var incompatibleAccounts int64
			if err := tx.Model(&model.SiteAccount{}).
				Where("site_id = ?", req.ID).
				Where("credential_type NOT IN ?", []model.SiteCredentialType{model.SiteCredentialTypeAccessToken, model.SiteCredentialTypeAPIKey}).
				Count(&incompatibleAccounts).Error; err != nil {
				return fmt.Errorf("failed to validate cloudflare account migration: %w", err)
			}
			if incompatibleAccounts > 0 {
				return fmt.Errorf("cannot switch site to cloudflare workers ai while username/password accounts exist; migrate them to access token or api key first")
			}
		}

		if err := tx.Model(&model.Site{}).
			Where("id = ?", req.ID).
			Select(selectFields).
			Updates(&merged).Error; err != nil {
			return fmt.Errorf("failed to update site: %w", err)
		}
		if !switchingToCloudflare {
			return nil
		}
		if err := tx.Model(&model.SiteAccount{}).
			Where("site_id = ?", req.ID).
			Updates(map[string]any{
				"auto_checkin":         false,
				"random_checkin":       false,
				"next_auto_checkin_at": nil,
			}).Error; err != nil {
			return fmt.Errorf("failed to normalize cloudflare accounts: %w", err)
		}
		accountIDs := tx.Model(&model.SiteAccount{}).Select("id").Where("site_id = ?", req.ID)
		if err := tx.Model(&model.SiteModel{}).
			Where("site_account_id IN (?)", accountIDs).
			Updates(map[string]any{
				"route_type":        model.SiteModelRouteTypeOpenAIChat,
				"route_source":      model.SiteModelRouteSourceSyncInferred,
				"manual_override":   false,
				"route_raw_payload": "",
				"route_updated_at":  time.Now(),
			}).Error; err != nil {
			return fmt.Errorf("failed to normalize cloudflare model routes: %w", err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return SiteGet(req.ID, ctx)
}

// mergeHeaders 将 upserts 合并进 existing：按 header key 大小写不敏感匹配，命中则仅
// 更新值并保留已存的原始 key 大小写，未命中则追加；随后按 deleteKeys（大小写不敏感）
// 删除，delete 在 upsert 之后执行（优先）。输出顺序稳定，空白 key 跳过。
func mergeHeaders(existing, upserts []model.CustomHeader, deleteKeys []string) []model.CustomHeader {
	order := make([]string, 0, len(existing)+len(upserts))
	byLower := make(map[string]model.CustomHeader, len(existing)+len(upserts))

	put := func(key, value string) {
		k := strings.TrimSpace(key)
		if k == "" {
			return
		}
		lk := strings.ToLower(k)
		if cur, ok := byLower[lk]; ok {
			cur.HeaderValue = value
			byLower[lk] = cur
			return
		}
		order = append(order, lk)
		byLower[lk] = model.CustomHeader{HeaderKey: k, HeaderValue: value}
	}

	for _, h := range existing {
		put(h.HeaderKey, h.HeaderValue)
	}
	for _, u := range upserts {
		put(u.HeaderKey, strings.TrimSpace(u.HeaderValue))
	}
	for _, dk := range deleteKeys {
		lk := strings.ToLower(strings.TrimSpace(dk))
		if lk == "" {
			continue
		}
		delete(byLower, lk)
	}

	out := make([]model.CustomHeader, 0, len(order))
	for _, lk := range order {
		if h, ok := byLower[lk]; ok {
			out = append(out, h)
		}
	}
	return out
}

// SiteBatchEdit 将一组修改补丁（添加/移除标签、设置/删除 Header）逐站点合并落库，
// 每站点一次加载、一次更新；单站失败计入 FailedItems 后继续。
// 标签先添加后移除（同名时移除优先），传入的标签需已规范化。
// 返回结果与需要刷新投影的站点 ID（仅 Header 变更影响投影）。
func SiteBatchEdit(req *model.SiteBatchEditRequest, ctx context.Context) (*model.SiteBatchResult, []int, error) {
	if req == nil {
		return nil, nil, fmt.Errorf("site batch edit request is nil")
	}
	editTags := len(req.AddTags) > 0 || len(req.RemoveTags) > 0
	editHeader := len(req.Upserts) > 0 || len(req.DeleteKeys) > 0
	if !editTags && !editHeader {
		return nil, nil, fmt.Errorf("nothing to edit")
	}
	removed := make(map[string]struct{}, len(req.RemoveTags))
	for _, tag := range req.RemoveTags {
		removed[tag] = struct{}{}
	}
	result := &model.SiteBatchResult{
		SuccessIDs:  make([]int, 0, len(req.IDs)),
		FailedItems: make([]model.SiteBatchFailure, 0),
	}
	affected := make([]int, 0, len(req.IDs))
	for _, id := range req.IDs {
		err := db.GetDB().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			var site model.Site
			if err := tx.First(&site, id).Error; err != nil {
				return fmt.Errorf("site not found")
			}
			columns := make([]string, 0, 2)
			updates := model.Site{ID: id}
			if editTags {
				merged := make([]string, 0, len(site.Tags)+len(req.AddTags))
				for _, tag := range site.Tags {
					if _, ok := removed[tag]; !ok {
						merged = append(merged, tag)
					}
				}
				for _, tag := range req.AddTags {
					if _, ok := removed[tag]; !ok {
						merged = append(merged, tag)
					}
				}
				next := model.NormalizeSiteTags(merged)
				if err := model.ValidateSiteTags(next); err != nil {
					return err
				}
				updates.Tags = next
				columns = append(columns, "tags")
			}
			if editHeader {
				updates.CustomHeader = mergeHeaders(site.CustomHeader, req.Upserts, req.DeleteKeys)
				columns = append(columns, "custom_header")
			}
			return tx.Model(&model.Site{}).
				Where("id = ?", id).
				Select(columns).
				Updates(&updates).Error
		})
		if err != nil {
			result.FailedItems = append(result.FailedItems, model.SiteBatchFailure{ID: id, Message: err.Error()})
			continue
		}
		result.SuccessIDs = append(result.SuccessIDs, id)
		if editHeader {
			affected = append(affected, id)
		}
	}
	return result, affected, nil
}

// SiteBatchApply 对一组站点逐个执行批量动作（enable/disable/delete），
// 单站失败计入 FailedItems 后继续。deleteSite 由调用方注入，避免 op 反向依赖站点同步层。
// 返回结果与需要刷新投影的站点 ID（仅 enable/disable 影响投影）。
func SiteBatchApply(req *model.SiteBatchRequest, deleteSite func(context.Context, int) error, ctx context.Context) (*model.SiteBatchResult, []int, error) {
	if req == nil {
		return nil, nil, fmt.Errorf("site batch request is nil")
	}
	result := &model.SiteBatchResult{
		SuccessIDs:  make([]int, 0, len(req.IDs)),
		FailedItems: make([]model.SiteBatchFailure, 0),
	}
	for _, id := range req.IDs {
		var err error
		switch req.Action {
		case "enable":
			err = SiteEnabled(id, true, ctx)
		case "disable":
			err = SiteEnabled(id, false, ctx)
		case "delete":
			err = deleteSite(ctx, id)
		default:
			err = fmt.Errorf("invalid action: %s", req.Action)
		}
		if err != nil {
			result.FailedItems = append(result.FailedItems, model.SiteBatchFailure{ID: id, Message: err.Error()})
			continue
		}
		result.SuccessIDs = append(result.SuccessIDs, id)
	}
	var affected []int
	if req.Action == "enable" || req.Action == "disable" {
		affected = result.SuccessIDs
	}
	return result, affected, nil
}

func SiteEnabled(id int, enabled bool, ctx context.Context) error {
	return db.GetDB().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Model(&model.Site{}).Where("id = ?", id).Update("enabled", enabled).Error; err != nil {
			return err
		}
		return tx.Model(&model.SiteAccount{}).Where("site_id = ?", id).Update("enabled", enabled).Error
	})
}

func SiteDel(id int, ctx context.Context) error {
	var affectedAccountIDs []int
	if err := db.GetDB().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var accountIDs []int
		if err := tx.Model(&model.SiteAccount{}).Where("site_id = ?", id).Pluck("id", &accountIDs).Error; err != nil {
			return err
		}
		affectedAccountIDs = accountIDs
		if len(accountIDs) > 0 {
			// Delete bindings before groups/accounts so FK-constrained databases do not
			// reject removing rows that bindings may still reference.
			if err := tx.Where("site_account_id IN ?", accountIDs).Delete(&model.SiteChannelBinding{}).Error; err != nil {
				return err
			}
			if err := tx.Where("site_account_id IN ?", accountIDs).Delete(&model.SiteToken{}).Error; err != nil {
				return err
			}
			if err := tx.Where("site_account_id IN ?", accountIDs).Delete(&model.SiteUserGroup{}).Error; err != nil {
				return err
			}
			if err := tx.Where("site_account_id IN ?", accountIDs).Delete(&model.SiteModel{}).Error; err != nil {
				return err
			}
			if err := tx.Where("site_account_id IN ?", accountIDs).Delete(&model.StatsSiteModelHourly{}).Error; err != nil {
				return err
			}
			if err := deleteLegacySitePricesByAccountIDs(tx, accountIDs); err != nil {
				return err
			}
			if err := tx.Where("id IN ?", accountIDs).Delete(&model.SiteAccount{}).Error; err != nil {
				return err
			}
		}
		return tx.Delete(&model.Site{}, id).Error
	}); err != nil {
		return err
	}
	if len(affectedAccountIDs) > 0 {
		invalidateSiteBindingCache()
		deleteSiteModelHourlyCacheForAccounts(affectedAccountIDs)
	}
	return nil
}

func SiteArchive(id int, ctx context.Context) error {
	now := time.Now()
	return db.GetDB().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Model(&model.Site{}).Where("id = ?", id).Updates(map[string]any{
			"archived":    true,
			"archived_at": &now,
			"enabled":     false,
		}).Error; err != nil {
			return err
		}
		return tx.Model(&model.SiteAccount{}).Where("site_id = ?", id).Update("enabled", false).Error
	})
}

func SiteRestore(id int, ctx context.Context) error {
	return db.GetDB().WithContext(ctx).Model(&model.Site{}).
		Where("id = ?", id).
		Updates(map[string]any{
			"archived":    false,
			"archived_at": gorm.Expr("NULL"),
		}).Error
}

func SiteAccountGet(id int, ctx context.Context) (*model.SiteAccount, error) {
	var account model.SiteAccount
	if err := db.GetDB().WithContext(ctx).
		Preload("Tokens").
		Preload("UserGroups").
		Preload("Models").
		Preload("ChannelBindings").
		First(&account, id).Error; err != nil {
		return nil, err
	}
	normalizeSiteAccountProxyFields(&account)
	return &account, nil
}

func normalizeSiteAccountForPlatform(account *model.SiteAccount, platform model.SitePlatform) error {
	if account == nil || platform != model.SitePlatformCloudflare {
		return nil
	}
	switch account.CredentialType {
	case model.SiteCredentialTypeAccessToken, model.SiteCredentialTypeAPIKey:
	default:
		return fmt.Errorf("cloudflare workers ai only supports access token or api key credentials")
	}
	account.AutoCheckin = false
	account.RandomCheckin = false
	account.NextAutoCheckinAt = nil
	return nil
}

func SiteAccountCreate(account *model.SiteAccount, ctx context.Context) error {
	if account == nil {
		return fmt.Errorf("site account is nil")
	}

	return db.GetDB().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		site, err := lockSiteByID(tx, account.SiteID)
		if err != nil {
			return err
		}
		if err := normalizeSiteAccountForPlatform(account, site.Platform); err != nil {
			return err
		}
		if site.Platform == model.SitePlatformCloudflare {
			account.AutoCheckinSet = true
		}
		if err := account.Validate(); err != nil {
			return err
		}
		if err := validateProxyConfigurationTx(tx, account.ProxyMode, account.ProxyConfigID); err != nil {
			return err
		}

		explicitEnabled := account.Enabled
		explicitAutoSync := account.AutoSync
		explicitAutoCheckin := account.AutoCheckin
		updates := map[string]any{}
		if account.EnabledSet && !account.Enabled {
			updates["enabled"] = false
		}
		if account.AutoSyncSet && !account.AutoSync {
			updates["auto_sync"] = false
		}
		if account.AutoCheckinSet && !account.AutoCheckin {
			updates["auto_checkin"] = false
		}
		// Tokens/groups/models/bindings nested in client payloads must never be
		// cascade-inserted: they bypass ownership, platform and route validation.
		// Children are only created through their dedicated op APIs.
		account.Tokens = nil
		account.UserGroups = nil
		account.Models = nil
		account.ChannelBindings = nil
		if err := tx.Omit(clause.Associations).Create(account).Error; err != nil {
			return err
		}
		if len(updates) > 0 {
			if err := tx.Model(&model.SiteAccount{}).Where("id = ?", account.ID).Updates(updates).Error; err != nil {
				return err
			}
		}
		if account.EnabledSet {
			account.Enabled = explicitEnabled
		}
		if account.AutoSyncSet {
			account.AutoSync = explicitAutoSync
		}
		if account.AutoCheckinSet {
			account.AutoCheckin = explicitAutoCheckin
		}
		return nil
	})
}

func mergeSiteAccountUpdateRequest(account model.SiteAccount, req *model.SiteAccountUpdateRequest) (model.SiteAccount, []string) {
	merged := account
	selectFields := make([]string, 0, 16)
	if req.Name != nil {
		merged.Name = *req.Name
		selectFields = append(selectFields, "name")
	}
	if req.CredentialType != nil {
		merged.CredentialType = *req.CredentialType
		selectFields = append(selectFields, "credential_type")
	}
	if req.Username != nil {
		merged.Username = *req.Username
		selectFields = append(selectFields, "username")
	}
	if req.Password != nil {
		merged.Password = *req.Password
		selectFields = append(selectFields, "password")
	}
	if req.AccessToken != nil {
		merged.AccessToken = *req.AccessToken
		selectFields = append(selectFields, "access_token")
	}
	if req.APIKey != nil {
		merged.APIKey = *req.APIKey
		selectFields = append(selectFields, "api_key")
	}
	if req.RefreshToken != nil {
		merged.RefreshToken = *req.RefreshToken
		selectFields = append(selectFields, "refresh_token")
	}
	if req.TokenExpiresAt != nil {
		merged.TokenExpiresAt = *req.TokenExpiresAt
		selectFields = append(selectFields, "token_expires_at")
	}
	if req.PlatformUserIDSet {
		merged.PlatformUserID = req.PlatformUserID
		selectFields = append(selectFields, "platform_user_id")
	}
	if req.ProxyMode != nil {
		merged.ProxyMode = *req.ProxyMode
		selectFields = append(selectFields, "proxy_mode")
	}
	if req.ProxyConfigIDSet || (req.ProxyMode != nil && *req.ProxyMode != model.ProxyUsageModePool) {
		if req.ProxyMode != nil && *req.ProxyMode != model.ProxyUsageModePool {
			merged.ProxyConfigID = nil
		} else {
			merged.ProxyConfigID = req.ProxyConfigID
		}
		selectFields = append(selectFields, "proxy_config_id")
	}
	if req.Enabled != nil {
		merged.Enabled = *req.Enabled
		selectFields = append(selectFields, "enabled")
	}
	if req.AutoSync != nil {
		merged.AutoSync = *req.AutoSync
		selectFields = append(selectFields, "auto_sync")
	}
	if req.AutoCheckin != nil {
		merged.AutoCheckin = *req.AutoCheckin
		selectFields = append(selectFields, "auto_checkin")
	}
	if req.RandomCheckin != nil {
		merged.RandomCheckin = *req.RandomCheckin
		selectFields = append(selectFields, "random_checkin")
	}
	if req.CheckinIntervalHours != nil {
		merged.CheckinIntervalHours = *req.CheckinIntervalHours
		selectFields = append(selectFields, "checkin_interval_hours")
	}
	if req.CheckinRandomWindowMinutes != nil {
		merged.CheckinRandomWindowMinutes = *req.CheckinRandomWindowMinutes
		selectFields = append(selectFields, "checkin_random_window_minutes")
	}
	return merged, selectFields
}

func SiteAccountUpdate(req *model.SiteAccountUpdateRequest, ctx context.Context) (*model.SiteAccount, error) {
	if req == nil {
		return nil, fmt.Errorf("site account update request is nil")
	}

	err := db.GetDB().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var accountRef struct {
			SiteID int
		}
		result := tx.Model(&model.SiteAccount{}).Select("site_id").Where("id = ?", req.ID).Take(&accountRef)
		if result.Error != nil {
			if errors.Is(result.Error, gorm.ErrRecordNotFound) {
				return fmt.Errorf("site account not found")
			}
			return result.Error
		}
		site, err := lockSiteByID(tx, accountRef.SiteID)
		if err != nil {
			return err
		}
		var account model.SiteAccount
		result = tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("id = ? AND site_id = ?", req.ID, site.ID).
			First(&account)
		if result.Error != nil {
			if errors.Is(result.Error, gorm.ErrRecordNotFound) {
				return fmt.Errorf("site account not found")
			}
			return result.Error
		}

		merged, selectFields := mergeSiteAccountUpdateRequest(account, req)
		if err := normalizeSiteAccountForPlatform(&merged, site.Platform); err != nil {
			return err
		}
		if site.Platform == model.SitePlatformCloudflare {
			if req.AutoCheckin != nil || account.AutoCheckin {
				selectFields = appendUniqueString(selectFields, "auto_checkin")
			}
			if req.RandomCheckin != nil || account.RandomCheckin {
				selectFields = appendUniqueString(selectFields, "random_checkin")
			}
			if account.NextAutoCheckinAt != nil {
				selectFields = appendUniqueString(selectFields, "next_auto_checkin_at")
			}
		}
		if len(selectFields) == 0 {
			return nil
		}
		if err := merged.Validate(); err != nil {
			return err
		}
		if err := validateProxyConfigurationTx(tx, merged.ProxyMode, merged.ProxyConfigID); err != nil {
			return err
		}
		if err := tx.Model(&model.SiteAccount{}).
			Where("id = ? AND site_id = ?", req.ID, site.ID).
			Select(selectFields).
			Updates(&merged).Error; err != nil {
			return fmt.Errorf("failed to update site account: %w", err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return SiteAccountGet(req.ID, ctx)
}

func SiteAccountEnabled(id int, enabled bool, ctx context.Context) error {
	return db.GetDB().WithContext(ctx).Model(&model.SiteAccount{}).Where("id = ?", id).Update("enabled", enabled).Error
}

func deleteLegacySitePricesByAccountIDs(tx *gorm.DB, accountIDs []int) error {
	if tx == nil || len(accountIDs) == 0 {
		return nil
	}
	if !tx.Migrator().HasTable("site_prices") {
		return nil
	}
	if err := tx.Exec("DELETE FROM site_prices WHERE site_account_id IN ?", accountIDs).Error; err != nil {
		return fmt.Errorf("failed to delete legacy site prices: %w", err)
	}
	return nil
}

func SiteAccountDel(id int, ctx context.Context) error {
	if err := db.GetDB().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// Delete bindings before groups/accounts so FK-constrained databases do not
		// reject removing rows that bindings may still reference.
		if err := tx.Where("site_account_id = ?", id).Delete(&model.SiteChannelBinding{}).Error; err != nil {
			return err
		}
		if err := tx.Where("site_account_id = ?", id).Delete(&model.SiteToken{}).Error; err != nil {
			return err
		}
		if err := tx.Where("site_account_id = ?", id).Delete(&model.SiteUserGroup{}).Error; err != nil {
			return err
		}
		if err := tx.Where("site_account_id = ?", id).Delete(&model.SiteModel{}).Error; err != nil {
			return err
		}
		if err := tx.Where("site_account_id = ?", id).Delete(&model.StatsSiteModelHourly{}).Error; err != nil {
			return err
		}
		if err := deleteLegacySitePricesByAccountIDs(tx, []int{id}); err != nil {
			return err
		}
		return tx.Delete(&model.SiteAccount{}, id).Error
	}); err != nil {
		return err
	}
	invalidateSiteBindingCache()
	deleteSiteModelHourlyCacheForAccounts([]int{id})
	return nil
}

func SiteAvailableModels(siteID int, ctx context.Context) ([]string, error) {
	var rows []model.SiteModel
	if err := db.GetDB().WithContext(ctx).
		Joins("JOIN site_accounts ON site_accounts.id = site_models.site_account_id").
		Where("site_accounts.site_id = ? AND site_models.disabled = ?", siteID, false).
		Find(&rows).Error; err != nil {
		return nil, err
	}
	seen := make(map[string]struct{})
	models := make([]string, 0, len(rows))
	for _, row := range rows {
		trimmed := strings.TrimSpace(row.ModelName)
		if trimmed == "" {
			continue
		}
		if _, ok := seen[trimmed]; ok {
			continue
		}
		seen[trimmed] = struct{}{}
		models = append(models, trimmed)
	}
	sort.Strings(models)
	return models, nil
}

func validateSiteModelRouteForPlatform(platform model.SitePlatform, routeType model.SiteModelRouteType) (model.SiteModelRouteType, error) {
	if platform != model.SitePlatformCloudflare {
		return model.NormalizeSiteModelRouteType(routeType), nil
	}
	trimmed := model.SiteModelRouteType(strings.TrimSpace(string(routeType)))
	if trimmed != model.SiteModelRouteTypeOpenAIChat {
		return "", fmt.Errorf("unsupported route type: cloudflare workers ai only supports the openai chat route")
	}
	return trimmed, nil
}

func lockSiteAndAccount(tx *gorm.DB, siteID int, accountID int) (*model.Site, error) {
	site, err := lockSiteByID(tx, siteID)
	if err != nil {
		return nil, err
	}
	var account model.SiteAccount
	result := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
		Select("id", "site_id").
		Where("id = ? AND site_id = ?", accountID, siteID).
		Take(&account)
	if result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return nil, newSiteChannelAccountNotFoundError()
		}
		return nil, result.Error
	}
	return site, nil
}

func lockSiteForAccount(tx *gorm.DB, accountID int) (*model.Site, error) {
	var accountRef struct {
		SiteID int
	}
	result := tx.Model(&model.SiteAccount{}).Select("site_id").Where("id = ?", accountID).Take(&accountRef)
	if result.Error != nil {
		if errors.Is(result.Error, gorm.ErrRecordNotFound) {
			return nil, fmt.Errorf("site account not found")
		}
		return nil, result.Error
	}
	return lockSiteAndAccount(tx, accountRef.SiteID, accountID)
}

func normalizeSiteModelRouteUpdateForPlatform(platform model.SitePlatform, routeType model.SiteModelRouteType, routeRawPayload string) (model.SiteModelRouteType, string, error) {
	normalizedRouteType, err := validateSiteModelRouteForPlatform(platform, routeType)
	if err != nil {
		return "", "", err
	}
	normalizedPayload := strings.TrimSpace(routeRawPayload)
	if platform == model.SitePlatformCloudflare {
		normalizedPayload = ""
	}
	return normalizedRouteType, normalizedPayload, nil
}

func SiteModelRouteUpdate(accountID int, groupKey string, modelName string, routeType model.SiteModelRouteType, source model.SiteModelRouteSource, manualOverride bool, routeRawPayload string, ctx context.Context) error {
	return db.GetDB().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		site, err := lockSiteForAccount(tx, accountID)
		if err != nil {
			return err
		}
		normalizedRouteType, normalizedPayload, err := normalizeSiteModelRouteUpdateForPlatform(site.Platform, routeType, routeRawPayload)
		if err != nil {
			return err
		}
		now := time.Now()
		return tx.Model(&model.SiteModel{}).
			Where("site_account_id = ? AND group_key = ? AND model_name = ?", accountID, model.NormalizeSiteGroupKey(groupKey), strings.TrimSpace(modelName)).
			Updates(map[string]any{
				"route_type":        normalizedRouteType,
				"route_source":      model.NormalizeSiteModelRouteSource(source, manualOverride),
				"manual_override":   manualOverride,
				"route_raw_payload": normalizedPayload,
				"route_updated_at":  &now,
			}).Error
	})
}

// SiteModelRoutesUpdate validates account ownership and every target before
// writing, then persists the complete batch atomically.
func SiteModelRoutesUpdate(siteID int, accountID int, items []model.SiteModelRouteUpdateRequest, ctx context.Context) error {
	type normalizedUpdate struct {
		modelID                 int
		routeType               model.SiteModelRouteType
		routeRawPayload         string
		disableProtocolFallback *bool
	}

	return db.GetDB().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		site, err := lockSiteAndAccount(tx, siteID, accountID)
		if err != nil {
			return err
		}
		normalized := make([]normalizedUpdate, 0, len(items))
		seen := make(map[string]struct{}, len(items))
		for _, item := range items {
			groupKey := model.NormalizeSiteGroupKey(item.GroupKey)
			modelName := strings.TrimSpace(item.ModelName)
			if modelName == "" {
				return fmt.Errorf("model name is required")
			}
			key := groupKey + "\x00" + modelName
			if _, exists := seen[key]; exists {
				return fmt.Errorf("duplicate model route target: %s/%s", groupKey, modelName)
			}
			seen[key] = struct{}{}
			routeType, routeRawPayload, err := normalizeSiteModelRouteUpdateForPlatform(site.Platform, item.RouteType, item.RouteRawPayload)
			if err != nil {
				return err
			}
			var target model.SiteModel
			result := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
				Select("id").
				Where("site_account_id = ? AND group_key = ? AND model_name = ?", accountID, groupKey, modelName).
				Take(&target)
			if result.Error != nil {
				if errors.Is(result.Error, gorm.ErrRecordNotFound) {
					return fmt.Errorf("site model not found: %s/%s", groupKey, modelName)
				}
				return result.Error
			}
			normalized = append(normalized, normalizedUpdate{
				modelID:                 target.ID,
				routeType:               routeType,
				routeRawPayload:         routeRawPayload,
				disableProtocolFallback: item.DisableProtocolFallback,
			})
		}

		now := time.Now()
		for _, update := range normalized {
			updates := map[string]any{
				"route_type":        update.routeType,
				"route_source":      model.SiteModelRouteSourceManualOverride,
				"manual_override":   true,
				"route_raw_payload": update.routeRawPayload,
				"route_updated_at":  &now,
			}
			// 只在请求显式携带协议勾选时改写，未携带的旧客户端保持原值。
			// 非 OpenAI 文本路由不存在协议降级，统一归零避免留下无意义的锁定。
			if update.routeType != model.SiteModelRouteTypeOpenAIChat &&
				update.routeType != model.SiteModelRouteTypeOpenAIResponse {
				updates["disable_protocol_fallback"] = false
			} else if update.disableProtocolFallback != nil {
				updates["disable_protocol_fallback"] = *update.disableProtocolFallback
			}
			if err := tx.Model(&model.SiteModel{}).
				Where("id = ? AND site_account_id = ?", update.modelID, accountID).
				Updates(updates).Error; err != nil {
				return err
			}
		}
		return nil
	})
}

func SiteModelRouteUpdateIfNotManual(accountID int, groupKey string, modelName string, routeType model.SiteModelRouteType, source model.SiteModelRouteSource, routeRawPayload string, ctx context.Context) (bool, error) {
	updated := false
	err := db.GetDB().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		site, err := lockSiteForAccount(tx, accountID)
		if err != nil {
			return err
		}
		normalizedRouteType, normalizedPayload, err := normalizeSiteModelRouteUpdateForPlatform(site.Platform, routeType, routeRawPayload)
		if err != nil {
			return err
		}
		now := time.Now()
		result := tx.Model(&model.SiteModel{}).
			Where("site_account_id = ? AND group_key = ? AND model_name = ? AND manual_override = ? AND (route_type IS NULL OR route_type <> ?)", accountID, model.NormalizeSiteGroupKey(groupKey), strings.TrimSpace(modelName), false, normalizedRouteType).
			Updates(map[string]any{
				"route_type":        normalizedRouteType,
				"route_source":      model.NormalizeSiteModelRouteSource(source, false),
				"manual_override":   false,
				"route_raw_payload": normalizedPayload,
				"route_updated_at":  &now,
			})
		if result.Error != nil {
			return result.Error
		}
		updated = result.RowsAffected > 0
		return nil
	})
	return updated, err
}

func SiteModelsDisabledUpdate(siteID int, accountID int, items []model.SiteModelDisableUpdateRequest, ctx context.Context) error {
	type normalizedUpdate struct {
		modelID  int
		disabled bool
	}

	return db.GetDB().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if _, err := lockSiteAndAccount(tx, siteID, accountID); err != nil {
			return err
		}
		normalized := make([]normalizedUpdate, 0, len(items))
		seen := make(map[string]struct{}, len(items))
		for _, item := range items {
			groupKey := model.NormalizeSiteGroupKey(item.GroupKey)
			modelName := strings.TrimSpace(item.ModelName)
			if modelName == "" {
				return fmt.Errorf("model name is required")
			}
			key := groupKey + "\x00" + modelName
			if _, exists := seen[key]; exists {
				return fmt.Errorf("duplicate model disabled target: %s/%s", groupKey, modelName)
			}
			seen[key] = struct{}{}
			var target model.SiteModel
			result := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
				Select("id").
				Where("site_account_id = ? AND group_key = ? AND model_name = ?", accountID, groupKey, modelName).
				Take(&target)
			if result.Error != nil {
				if errors.Is(result.Error, gorm.ErrRecordNotFound) {
					return fmt.Errorf("site model not found: %s/%s", groupKey, modelName)
				}
				return result.Error
			}
			normalized = append(normalized, normalizedUpdate{
				modelID:  target.ID,
				disabled: item.Disabled,
			})
		}
		for _, update := range normalized {
			if err := tx.Model(&model.SiteModel{}).
				Where("id = ? AND site_account_id = ?", update.modelID, accountID).
				Update("disabled", update.disabled).Error; err != nil {
				return err
			}
		}
		return nil
	})
}
