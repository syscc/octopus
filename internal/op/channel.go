package op

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/bestruirui/octopus/internal/db"
	"github.com/bestruirui/octopus/internal/model"
	model2 "github.com/bestruirui/octopus/internal/transformer/outbound"
	"github.com/bestruirui/octopus/internal/utils/cache"
	"github.com/bestruirui/octopus/internal/utils/log"
	"github.com/bestruirui/octopus/internal/utils/xstrings"
	"gorm.io/gorm"
)

const channelMutationLockShards = 64

var ErrChannelNotFound = errors.New("channel not found")

var channelCache = cache.New[int, model.Channel](16)
var channelKeyCache = cache.New[int, model.ChannelKey](16)
var channelKeyCacheNeedUpdate = make(map[int]struct{})
var channelKeyCacheNeedUpdateLock sync.Mutex
var channelMutationLocks [channelMutationLockShards]sync.Mutex

func lockChannelMutation(channelID int) func() {
	lock := &channelMutationLocks[uint(channelID)%channelMutationLockShards]
	lock.Lock()
	return lock.Unlock
}

func ChannelList(ctx context.Context) ([]model.Channel, error) {
	channels := make([]model.Channel, 0, channelCache.Len())
	for _, channel := range channelCache.GetAll() {
		normalizeChannelProxyFields(&channel)
		channels = append(channels, channel)
	}
	return channels, nil
}

func normalizeChannelProxyFields(channel *model.Channel) {
	if channel == nil {
		return
	}
	if channel.ProxyMode == "" {
		channel.ProxyMode = model.ProxyUsageModeDirect
	}
	if channel.ProxyMode != model.ProxyUsageModePool {
		channel.ProxyConfigID = nil
	}
	channel.Proxy = channel.ProxyMode != model.ProxyUsageModeDirect
	channel.ChannelProxy = nil
	channel.OpenAIProtocolMode = channel.OpenAIProtocolMode.Normalize()
	channel.OpenAIChatCapability = channel.OpenAIChatCapability.Normalize()
	channel.OpenAIResponsesCapability = channel.OpenAIResponsesCapability.Normalize()
	if !model.IsOpenAITextChannelType(channel.Type) {
		channel.OpenAIProtocolMode = model.OpenAIProtocolModeAuto
		channel.ResetOpenAIProtocolCapabilities()
	}
}

func channelUsesCloudflareWorkersAI(channel *model.Channel) bool {
	if channel == nil {
		return false
	}
	for _, baseURL := range channel.BaseUrls {
		if _, ok := model.CanonicalCloudflareWorkersAIBaseURL(baseURL.URL); ok {
			return true
		}
	}
	return false
}

func forceCloudflareChannelProtocol(channel *model.Channel) {
	if channelUsesCloudflareWorkersAI(channel) {
		channel.ForceOpenAIChatOnly()
	}
}

func validateCloudflareChannelType(channel *model.Channel) error {
	if channelUsesCloudflareWorkersAI(channel) && !model.IsOpenAITextChannelType(channel.Type) {
		return fmt.Errorf("cloudflare workers ai base url requires an openai chat or responses channel")
	}
	return nil
}

func baseURLsEqual(left, right []model.BaseUrl) bool {
	if len(left) != len(right) {
		return false
	}
	normalize := func(items []model.BaseUrl) []string {
		urls := make([]string, len(items))
		for index := range items {
			urls[index] = strings.TrimRight(strings.TrimSpace(items[index].URL), "/")
		}
		sort.Strings(urls)
		return urls
	}
	leftURLs := normalize(left)
	rightURLs := normalize(right)
	for index := range leftURLs {
		if leftURLs[index] != rightURLs[index] {
			return false
		}
	}
	return true
}

func ChannelCreate(channel *model.Channel, ctx context.Context) error {
	if channel == nil {
		return fmt.Errorf("channel is nil")
	}
	if !channel.OpenAIProtocolMode.Valid() {
		return fmt.Errorf("invalid openai protocol mode: %s", channel.OpenAIProtocolMode)
	}
	channel.ResetOpenAIProtocolCapabilities()
	if err := validateCloudflareChannelType(channel); err != nil {
		return err
	}
	forceCloudflareChannelProtocol(channel)
	if err := channel.NormalizeOpenAIProtocolSettings(); err != nil {
		return err
	}
	if channel.ProxyMode == "" {
		channel.ProxyMode = model.ProxyUsageModeDirect
	}
	if err := channel.ProxyMode.Validate(false); err != nil {
		return err
	}
	if channel.ProxyMode == model.ProxyUsageModePool {
		if channel.ProxyConfigID == nil || *channel.ProxyConfigID <= 0 {
			return fmt.Errorf("proxy config id is required when proxy mode is pool")
		}
		if _, err := ProxyURLForConfig(*channel.ProxyConfigID, ctx); err != nil {
			return err
		}
	} else {
		channel.ProxyConfigID = nil
	}
	if err := db.GetDB().WithContext(ctx).Create(channel).Error; err != nil {
		return err
	}
	normalizeChannelProxyFields(channel)
	channelCache.Set(channel.ID, *channel)
	for _, k := range channel.Keys {
		if k.ID != 0 {
			channelKeyCache.Set(k.ID, k)
		}
	}
	return nil
}

// ChannelKeyUpdate 仅更新 ChannelKey 的内存缓存（不落库），并标记为需要在 SaveCache 时写入数据库。
func ChannelKeyUpdate(key model.ChannelKey) error {
	unlock := lockChannelMutation(key.ChannelID)
	defer unlock()

	if key.ID == 0 || key.ChannelID == 0 {
		return fmt.Errorf("invalid channel key")
	}
	ch, ok := channelCache.Get(key.ChannelID)
	if !ok {
		return fmt.Errorf("channel not found")
	}
	if len(ch.Keys) > 0 {
		keys := make([]model.ChannelKey, len(ch.Keys))
		copy(keys, ch.Keys)
		found := false
		for i := range keys {
			if keys[i].ID == key.ID {
				keys[i] = key
				found = true
				break
			}
		}
		if !found {
			// The key is no longer part of the cached channel (deleted by a
			// committed ChannelUpdate or a cache refresh): a late runtime
			// update must not resurrect it in the key cache or the pending
			// save set.
			return nil
		}
		ch.Keys = keys
	} else {
		return nil
	}
	channelCache.Set(key.ChannelID, ch)
	channelKeyCache.Set(key.ID, key)
	channelKeyCacheNeedUpdateLock.Lock()
	channelKeyCacheNeedUpdate[key.ID] = struct{}{}
	channelKeyCacheNeedUpdateLock.Unlock()
	return nil
}
func ChannelBaseUrlUpdate(channelID int, baseUrl []model.BaseUrl) error {
	unlock := lockChannelMutation(channelID)
	defer unlock()

	ch, ok := channelCache.Get(channelID)
	if !ok {
		return fmt.Errorf("channel not found")
	}
	// Copy to decouple callers from internal cache storage.
	if baseUrl == nil {
		ch.BaseUrls = nil
	} else {
		cp := make([]model.BaseUrl, len(baseUrl))
		copy(cp, baseUrl)
		ch.BaseUrls = cp
	}
	channelCache.Set(channelID, ch)
	return nil
}

// channelProtocolDBState is the authoritative type and protocol state of one
// channel as stored in the database.
type channelProtocolDBState struct {
	Type        model2.OutboundType
	Mode        model.OpenAIProtocolMode
	Chat        model.OpenAIProtocolCapability
	Responses   model.OpenAIProtocolCapability
	ChatAt      int64
	ResponsesAt int64
}

// capabilityFor returns the stored capability for the given protocol type.
func (s *channelProtocolDBState) capabilityFor(protocol model2.OutboundType) model.OpenAIProtocolCapability {
	if protocol == model2.OutboundTypeOpenAIResponse {
		return s.Responses
	}
	return s.Chat
}

// capabilityAtFor returns the stored observation stamp for the given protocol.
func (s *channelProtocolDBState) capabilityAtFor(protocol model2.OutboundType) int64 {
	if protocol == model2.OutboundTypeOpenAIResponse {
		return s.ResponsesAt
	}
	return s.ChatAt
}

// setCapabilityAt updates the observation stamp for the given protocol type.
func (s *channelProtocolDBState) setCapabilityAt(protocol model2.OutboundType, at int64) {
	if s == nil {
		return
	}
	if protocol == model2.OutboundTypeOpenAIResponse {
		s.ResponsesAt = at
	} else {
		s.ChatAt = at
	}
}

// readChannelProtocolState loads the type and the protocol columns of one
// channel in a single query. Key material is never selected. A nil state
// means the channel row no longer exists.
func readChannelProtocolState(ctx context.Context, tx *gorm.DB, channelID int) (*channelProtocolDBState, error) {
	var rows []channelProtocolDBState
	if err := tx.WithContext(ctx).
		Model(&model.Channel{}).
		Select(
			"type AS type",
			"openai_protocol_mode AS mode",
			"openai_chat_capability AS chat",
			"openai_responses_capability AS responses",
			"openai_chat_capability_at AS chat_at",
			"openai_responses_capability_at AS responses_at",
		).
		Where("id = ?", channelID).
		Limit(1).
		Scan(&rows).Error; err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, nil
	}
	state := rows[0]
	state.Mode = state.Mode.Normalize()
	state.Chat = state.Chat.Normalize()
	state.Responses = state.Responses.Normalize()
	return &state, nil
}

// syncChannelProtocolCache overlays the authoritative protocol columns onto
// the cached channel so the runtime cache mirrors the committed DB state.
// Callers hold the channel mutation lock. Without a cache entry there is
// nothing to sync: the next refresh rebuilds the entry from the database.
func syncChannelProtocolCache(channelID int, cached model.Channel, hadCached bool, state *channelProtocolDBState) {
	if !hadCached || state == nil {
		return
	}
	cached.OpenAIProtocolMode = state.Mode
	cached.OpenAIChatCapability = state.Chat
	cached.OpenAIResponsesCapability = state.Responses
	cached.OpenAIChatCapabilityAt = state.ChatAt
	cached.OpenAIResponsesCapabilityAt = state.ResponsesAt
	channelCache.Set(channelID, cached)
}

// resolveOpenAIProtocolRecordAfterNoopUpdate runs when the guarded capability
// UPDATE inside ChannelRecordOpenAIProtocolCapability reports RowsAffected ==
// 0 even though the authoritative pre-read decided the write was needed. Only
// changes that bypassed the channel mutation lock between the read and the
// write (external edits), or writes swallowed by the database, can land
// here. It reloads the authoritative type and protocol columns with one
// query (never the key material) and re-decides under the same rules as the
// pre-read:
//   - row gone: the channel was deleted concurrently, nothing to sync;
//   - non-OpenAI type or manual mode: that committed state wins, the cache
//     is synced to it and no retry is attempted;
//   - column already holds the recorded capability: nothing to retry, the
//     cache is synced to the authoritative state;
//   - still auto with the target column below the recorded capability: the
//     write was swallowed (e.g. MySQL reports zero affected rows for
//     value-identical updates, or a trigger ignored the write), so the
//     guarded UPDATE is retried once and the state re-read; if it still has
//     not landed, a diagnostic error is returned instead of faking a cached
//     success.
func resolveOpenAIProtocolRecordAfterNoopUpdate(
	ctx context.Context,
	tx *gorm.DB,
	channelID int,
	column string,
	stampColumn string,
	observedAt int64,
	cached model.Channel,
	hadCached bool,
	protocol model2.OutboundType,
	capability model.OpenAIProtocolCapability,
) error {
	state, err := readChannelProtocolState(ctx, tx, channelID)
	if err != nil {
		return err
	}
	if state == nil {
		return nil
	}
	if !model.IsOpenAITextChannelType(state.Type) || state.Mode != model.OpenAIProtocolModeAuto ||
		state.capabilityFor(protocol) == capability {
		syncChannelProtocolCache(channelID, cached, hadCached, state)
		return nil
	}
	retry := tx.WithContext(ctx).
		Model(&model.Channel{}).
		Where("id = ? AND (openai_protocol_mode = ? OR openai_protocol_mode = '' OR openai_protocol_mode IS NULL) AND type IN ?",
			channelID, model.OpenAIProtocolModeAuto,
			[]model2.OutboundType{model2.OutboundTypeOpenAIChat, model2.OutboundTypeOpenAIResponse}).
		Updates(map[string]any{column: capability, stampColumn: observedAt})
	if retry.Error != nil {
		return retry.Error
	}
	state, err = readChannelProtocolState(ctx, tx, channelID)
	if err != nil {
		return err
	}
	if state == nil {
		return nil
	}
	// Sync all three authoritative columns onto the cached channel so the
	// cache reflects the committed DB state whatever the outcome below is.
	syncChannelProtocolCache(channelID, cached, hadCached, state)
	if state.Mode == model.OpenAIProtocolModeAuto && model.IsOpenAITextChannelType(state.Type) &&
		state.capabilityFor(protocol) != capability {
		return fmt.Errorf(
			"openai protocol capability update for channel %d did not persist: column %s is still %q after one retry",
			channelID, column, state.capabilityFor(protocol),
		)
	}
	return nil
}

// ChannelRecordOpenAIProtocolCapability persists an automatic protocol
// observation. Manual modes are authoritative and never changed by runtime
// learning; unknown is never persisted (it is the reset state, not an
// observation).
//
// 并发边界：探测/请求流程先经 ChannelGetAuthoritative 取得配置，再经历网络
// 阶段，最后才调用本函数；期间 ChannelUpdate 可能已提交新的 BaseUrls/Type/
// 协议模式。op 层拿不到"探测发起时的配置身份"，因此这里在同一 channel
// mutation lock 下重新读取权威 DB 行作为唯一决策依据（缓存可能被 Base URL
// 剪枝、refresh 竞态窗口或外部直写暂时带偏）：
//   - 手动模式始终胜出：观察不落库，缓存同步为权威协议状态；
//   - 渠道已并发变为非 OpenAI 类型时绝不把观察写进该行；
//   - 权威行已持有该观察值时不重复写，并顺带对齐可能陈旧的缓存列；
//   - UPDATE 的 WHERE 同时钉住 auto 模式与 OpenAI 文本类型，即使有绕过
//     mutation lock 的外部写入也不会污染手动模式行或非 OpenAI 行。
//
// 残余边界（无法在 op 层防止）：若管理员在网络阶段变更了 BaseUrls，迟到的
// 旧观察会作为新 auto 配置的首次学习写入。该写入保持 DB 与缓存协议列一致，
// 且后续矛盾观察会再次修正该列；上游 relay/probe 如需严格身份校验，必须
// 自行比对探测时的配置或显式传入。
func ChannelRecordOpenAIProtocolCapability(channelID int, protocol model2.OutboundType, capability model.OpenAIProtocolCapability, ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if channelID <= 0 || !model.IsOpenAITextChannelType(protocol) || !capability.Valid() {
		return fmt.Errorf("invalid openai protocol capability update")
	}
	capability = capability.Normalize()
	if capability == model.OpenAIProtocolCapabilityUnknown {
		// unknown 不是观察结果（它是重置态），绝不通过本路径落库，
		// 也不能借它清空已学习的能力列。
		return nil
	}

	unlock := lockChannelMutation(channelID)
	defer unlock()

	// 唯一决策依据是权威 DB 行：缓存在手动模式切换、类型变更、Base URL
	// 剪枝或外部直写下都可能误判，这里不再依据缓存做最终判定。读取与写入
	// 都在同一 mutation lock 内完成，op 层的 channel 变更无法插到读与写之间。
	state, err := readChannelProtocolState(ctx, db.GetDB(), channelID)
	if err != nil {
		return err
	}
	if state == nil {
		// 渠道已被并发删除：不写、不复活、不重建缓存。
		return nil
	}

	cached, hadCached := channelCache.Get(channelID)
	if hadCached {
		normalizeChannelProxyFields(&cached)
	}

	if !model.IsOpenAITextChannelType(state.Type) {
		// 渠道已并发改为非 OpenAI 类型：绝不把协议观察写进该行；
		// 缓存协议列同步为权威值，残留的类型差异由下一次刷新收敛。
		syncChannelProtocolCache(channelID, cached, hadCached, state)
		return nil
	}
	if state.Mode != model.OpenAIProtocolModeAuto {
		// 手动模式始终胜出：不写入，缓存同步为权威协议状态。
		syncChannelProtocolCache(channelID, cached, hadCached, state)
		return nil
	}
	observedAt := time.Now().Unix()
	column := "openai_chat_capability"
	stampColumn := "openai_chat_capability_at"
	if protocol == model2.OutboundTypeOpenAIResponse {
		column = "openai_responses_capability"
		stampColumn = "openai_responses_capability_at"
	}

	if state.capabilityFor(protocol) == capability {
		// 权威行已持有该观察值：能力列无需改写，但观察时间必须续期，
		// 否则一个反复被证实的 unsupported 会因首次时间戳陈旧而到期，
		// 白白浪费一次探路请求。时间戳单调递增，不会被旧观察回拨。
		if state.capabilityAtFor(protocol) < observedAt {
			if err := db.GetDB().WithContext(ctx).
				Model(&model.Channel{}).
				Where("id = ? AND (openai_protocol_mode = ? OR openai_protocol_mode = '' OR openai_protocol_mode IS NULL) AND type IN ? AND "+column+" = ?",
					channelID, model.OpenAIProtocolModeAuto,
					[]model2.OutboundType{model2.OutboundTypeOpenAIChat, model2.OutboundTypeOpenAIResponse},
					capability).
				Update(stampColumn, observedAt).Error; err != nil {
				return err
			}
			state.setCapabilityAt(protocol, observedAt)
		}
		syncChannelProtocolCache(channelID, cached, hadCached, state)
		return nil
	}

	result := db.GetDB().WithContext(ctx).
		Model(&model.Channel{}).
		Where("id = ? AND (openai_protocol_mode = ? OR openai_protocol_mode = '' OR openai_protocol_mode IS NULL) AND type IN ?",
			channelID, model.OpenAIProtocolModeAuto,
			[]model2.OutboundType{model2.OutboundTypeOpenAIChat, model2.OutboundTypeOpenAIResponse}).
		Updates(map[string]any{column: capability, stampColumn: observedAt})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected > 0 {
		if hadCached {
			cached.OpenAIProtocolMode = model.OpenAIProtocolModeAuto
			cached.OpenAIChatCapability = state.Chat
			cached.OpenAIResponsesCapability = state.Responses
			cached.OpenAIChatCapabilityAt = state.ChatAt
			cached.OpenAIResponsesCapabilityAt = state.ResponsesAt
			cached.SetOpenAIProtocolCapabilityAt(protocol, capability, observedAt)
			channelCache.Set(channelID, cached)
		}
		return nil
	}
	// RowsAffected == 0 只可能来自绕过 mutation lock 的外部变化或被吞的
	// 写入；以权威 DB 状态裁决，绝不假设是手动模式。
	return resolveOpenAIProtocolRecordAfterNoopUpdate(ctx, db.GetDB(), channelID, column, stampColumn, observedAt, cached, hadCached, protocol, capability)
}

// ChannelKeySaveDB 将运行时更新过的 ChannelKey 缓存写入数据库。
// 只 UPDATE 已存在行的运行时三列（status_code / last_use_time_stamp /
// total_cost）：不 INSERT（已删除的 key 不能复活），也不覆盖
// channel_key / enabled / remark 等管理员字段。写失败的 key 会把 pending
// 标记放回，等待下一次保存重试。
func ChannelKeySaveDB(ctx context.Context) error {
	keyIDs := takeChannelKeyPendingIDs()
	if len(keyIDs) == 0 {
		return nil
	}

	var firstErr error
	failedIDs := make([]int, 0, len(keyIDs))
	for _, id := range keyIDs {
		key, ok := channelKeyCache.Get(id)
		if !ok {
			// The key-cache entry is gone (e.g. the key was deleted); there
			// is nothing left to persist and the pending marker stays dropped.
			continue
		}
		result := db.GetDB().WithContext(ctx).Model(&model.ChannelKey{}).
			Where("id = ?", id).
			Updates(map[string]interface{}{
				"status_code":         key.StatusCode,
				"last_use_time_stamp": key.LastUseTimeStamp,
				"total_cost":          key.TotalCost,
			})
		if result.Error != nil {
			failedIDs = append(failedIDs, id)
			if firstErr == nil {
				firstErr = result.Error
			}
			continue
		}
		// RowsAffected == 0 without an error means the row is gone (the key
		// was deleted): it must not be resurrected, so the pending marker is
		// simply not restored.
	}
	if len(failedIDs) > 0 {
		restoreChannelKeyPendingIDs(failedIDs)
		return fmt.Errorf("failed to persist %d channel keys: %w", len(failedIDs), firstErr)
	}
	return nil
}

// takeChannelKeyPendingIDs swaps out the pending-save key ID set.
func takeChannelKeyPendingIDs() []int {
	channelKeyCacheNeedUpdateLock.Lock()
	ids := make([]int, 0, len(channelKeyCacheNeedUpdate))
	for id := range channelKeyCacheNeedUpdate {
		ids = append(ids, id)
	}
	channelKeyCacheNeedUpdate = make(map[int]struct{})
	channelKeyCacheNeedUpdateLock.Unlock()
	return ids
}

// restoreChannelKeyPendingIDs puts failed key IDs back into the pending-save
// set so the next ChannelKeySaveDB retries them.
func restoreChannelKeyPendingIDs(ids []int) {
	if len(ids) == 0 {
		return
	}
	channelKeyCacheNeedUpdateLock.Lock()
	for _, id := range ids {
		channelKeyCacheNeedUpdate[id] = struct{}{}
	}
	channelKeyCacheNeedUpdateLock.Unlock()
}

// channelKeyPendingSnapshot returns a copy of the key IDs that have a pending
// runtime update. Callers must not hold channelKeyCacheNeedUpdateLock.
func channelKeyPendingSnapshot() map[int]struct{} {
	channelKeyCacheNeedUpdateLock.Lock()
	snapshot := make(map[int]struct{}, len(channelKeyCacheNeedUpdate))
	for id := range channelKeyCacheNeedUpdate {
		snapshot[id] = struct{}{}
	}
	channelKeyCacheNeedUpdateLock.Unlock()
	return snapshot
}

// dropChannelKeyCacheEntry removes one key from the key cache together with
// its pending-save marker. Callers must hold the owning channel's mutation
// lock; the pending-map lock is always acquired afterwards (lock order:
// channel mutation lock -> channelKeyCacheNeedUpdateLock).
func dropChannelKeyCacheEntry(keyID int) {
	if keyID == 0 {
		return
	}
	channelKeyCache.Del(keyID)
	channelKeyCacheNeedUpdateLock.Lock()
	delete(channelKeyCacheNeedUpdate, keyID)
	channelKeyCacheNeedUpdateLock.Unlock()
}

func ChannelUpdate(req *model.ChannelUpdateRequest, ctx context.Context) (*model.Channel, error) {
	if req == nil {
		return nil, fmt.Errorf("channel update request is nil")
	}
	unlock := lockChannelMutation(req.ID)
	defer unlock()

	existingChannel, ok := channelCache.Get(req.ID)
	if !ok {
		return nil, fmt.Errorf("channel not found")
	}
	// Keep cached key runtime fields for post-commit reconciliation, but use the
	// database row as the authority for persisted channel fields. Availability
	// probes may prune unreachable BaseUrls from the runtime cache without
	// persisting that subset.
	previousKeys := existingChannel.Keys
	var persistedChannel model.Channel
	if err := db.GetDB().WithContext(ctx).First(&persistedChannel, req.ID).Error; err != nil {
		return nil, fmt.Errorf("failed to load channel for update: %w", err)
	}
	persistedChannel.Keys = previousKeys
	existingChannel = persistedChannel
	normalizeChannelProxyFields(&existingChannel)
	if !req.BypassManagedCheck {
		if _, managed, err := ChannelManagedBinding(req.ID, ctx); err != nil {
			return nil, err
		} else if managed {
			return nil, fmt.Errorf("managed site channel is read-only; please edit it from the site account")
		}
	}

	candidate := existingChannel
	existingMode := existingChannel.OpenAIProtocolMode.Normalize()
	existingCloudflare := channelUsesCloudflareWorkersAI(&existingChannel)
	if req.OpenAIProtocolMode != nil && !req.OpenAIProtocolMode.Valid() {
		return nil, fmt.Errorf("invalid openai protocol mode: %s", *req.OpenAIProtocolMode)
	}
	if req.Type != nil {
		candidate.Type = *req.Type
	}
	if req.BaseUrls != nil {
		candidate.BaseUrls = *req.BaseUrls
	}
	if req.OpenAIProtocolMode != nil {
		candidate.OpenAIProtocolMode = req.OpenAIProtocolMode.Normalize()
	}

	typeChanged := candidate.Type != existingChannel.Type
	baseURLsChanged := !baseURLsEqual(candidate.BaseUrls, existingChannel.BaseUrls)
	returningToAuto := req.OpenAIProtocolMode != nil &&
		candidate.OpenAIProtocolMode == model.OpenAIProtocolModeAuto &&
		existingMode != model.OpenAIProtocolModeAuto
	leavingCloudflare := req.OpenAIProtocolMode == nil && existingCloudflare &&
		!channelUsesCloudflareWorkersAI(&candidate) && existingMode == model.OpenAIProtocolModeChatOnly
	if leavingCloudflare {
		candidate.OpenAIProtocolMode = model.OpenAIProtocolModeAuto
	}
	resetProtocolCapabilities := typeChanged || baseURLsChanged || returningToAuto || leavingCloudflare
	if resetProtocolCapabilities {
		candidate.ResetOpenAIProtocolCapabilities()
	}
	if !model.IsOpenAITextChannelType(candidate.Type) {
		if req.OpenAIProtocolMode != nil && req.OpenAIProtocolMode.Normalize() != model.OpenAIProtocolModeAuto {
			return nil, fmt.Errorf("openai protocol mode is only valid for openai text channels")
		}
		candidate.OpenAIProtocolMode = model.OpenAIProtocolModeAuto
		candidate.ResetOpenAIProtocolCapabilities()
	}
	candidateUsesCloudflare := channelUsesCloudflareWorkersAI(&candidate)
	preserveLegacyNonOpenAICloudflare := existingCloudflare && candidateUsesCloudflare &&
		!model.IsOpenAITextChannelType(existingChannel.Type) &&
		!typeChanged && !baseURLsChanged
	if !preserveLegacyNonOpenAICloudflare {
		if err := validateCloudflareChannelType(&candidate); err != nil {
			return nil, err
		}
		forceCloudflareChannelProtocol(&candidate)
	}
	if err := candidate.NormalizeOpenAIProtocolSettings(); err != nil {
		return nil, err
	}

	tx := db.GetDB().WithContext(ctx).Begin()
	defer func() {
		if r := recover(); r != nil {
			tx.Rollback()
		}
	}()

	var selectFields []string
	updates := model.Channel{ID: req.ID}

	if req.Name != nil {
		selectFields = append(selectFields, "name")
		updates.Name = *req.Name
	}
	if req.Type != nil || candidate.Type != existingChannel.Type {
		selectFields = append(selectFields, "type")
		updates.Type = candidate.Type
	}
	if req.Enabled != nil {
		selectFields = append(selectFields, "enabled")
		updates.Enabled = *req.Enabled
	}
	if req.BaseUrls != nil {
		selectFields = append(selectFields, "base_urls")
		updates.BaseUrls = *req.BaseUrls
	}
	if req.Model != nil {
		selectFields = append(selectFields, "model")
		updates.Model = *req.Model
	}
	if req.CustomModel != nil {
		selectFields = append(selectFields, "custom_model")
		updates.CustomModel = *req.CustomModel
	}
	effectiveProxyMode := existingChannel.ProxyMode
	effectiveProxyConfigID := existingChannel.ProxyConfigID
	proxyTouched := false
	if req.ProxyMode != nil {
		proxyTouched = true
		effectiveProxyMode = *req.ProxyMode
		selectFields = append(selectFields, "proxy_mode")
		updates.ProxyMode = *req.ProxyMode
	}
	if req.ProxyConfigID != nil || req.ProxyMode != nil {
		proxyTouched = true
		if effectiveProxyMode == model.ProxyUsageModePool {
			if req.ProxyConfigID != nil {
				selectFields = append(selectFields, "proxy_config_id")
				effectiveProxyConfigID = req.ProxyConfigID
				updates.ProxyConfigID = req.ProxyConfigID
			}
		} else {
			selectFields = append(selectFields, "proxy_config_id")
			effectiveProxyConfigID = nil
			updates.ProxyConfigID = nil
		}
	}
	if proxyTouched {
		if effectiveProxyMode == "" {
			effectiveProxyMode = model.ProxyUsageModeDirect
		}
		if err := effectiveProxyMode.Validate(false); err != nil {
			tx.Rollback()
			return nil, err
		}
		if effectiveProxyMode == model.ProxyUsageModePool {
			if effectiveProxyConfigID == nil || *effectiveProxyConfigID <= 0 {
				tx.Rollback()
				return nil, fmt.Errorf("proxy config id is required when proxy mode is pool")
			}
			if _, err := ProxyURLForConfig(*effectiveProxyConfigID, ctx); err != nil {
				tx.Rollback()
				return nil, err
			}
		}
	}
	if req.AutoSync != nil {
		selectFields = append(selectFields, "auto_sync")
		updates.AutoSync = *req.AutoSync
	}
	if req.AutoGroup != nil {
		selectFields = append(selectFields, "auto_group")
		updates.AutoGroup = *req.AutoGroup
	}
	if req.CustomHeader != nil {
		selectFields = append(selectFields, "custom_header")
		updates.CustomHeader = *req.CustomHeader
	}
	if req.WSMode != nil {
		selectFields = append(selectFields, "ws_mode")
		updates.WSMode = req.WSMode.Normalize()
	}
	if req.OpenAIProtocolMode != nil || candidate.OpenAIProtocolMode != existingChannel.OpenAIProtocolMode.Normalize() {
		selectFields = append(selectFields, "openai_protocol_mode")
		updates.OpenAIProtocolMode = candidate.OpenAIProtocolMode
	}
	if resetProtocolCapabilities || candidate.OpenAIChatCapability != existingChannel.OpenAIChatCapability.Normalize() {
		selectFields = append(selectFields, "openai_chat_capability")

		updates.OpenAIChatCapability = candidate.OpenAIChatCapability
	}
	if resetProtocolCapabilities || candidate.OpenAIResponsesCapability != existingChannel.OpenAIResponsesCapability.Normalize() {
		selectFields = append(selectFields, "openai_responses_capability")

		updates.OpenAIResponsesCapability = candidate.OpenAIResponsesCapability
	}
	if req.ParamOverride != nil {
		selectFields = append(selectFields, "param_override")
		updates.ParamOverride = req.ParamOverride
	}
	if req.MatchRegex != nil {
		selectFields = append(selectFields, "match_regex")
		updates.MatchRegex = req.MatchRegex
	}

	// 只有当有字段需要更新时才执行 UPDATE
	if len(selectFields) > 0 {
		if err := tx.Model(&model.Channel{}).Where("id = ?", req.ID).Select(selectFields).Updates(&updates).Error; err != nil {
			tx.Rollback()
			return nil, fmt.Errorf("failed to update channel: %w", err)
		}
	}

	// 删除 keys
	if len(req.KeysToDelete) > 0 {
		if err := tx.Where("id IN ? AND channel_id = ?", req.KeysToDelete, req.ID).Delete(&model.ChannelKey{}).Error; err != nil {
			tx.Rollback()
			return nil, fmt.Errorf("failed to delete channel keys: %w", err)
		}
	}

	// 更新 keys（逐条，只更新提供的字段）
	if len(req.KeysToUpdate) > 0 {
		for _, ku := range req.KeysToUpdate {
			updates := map[string]interface{}{}
			if ku.Enabled != nil {
				updates["enabled"] = *ku.Enabled
			}
			if ku.ChannelKey != nil {
				updates["channel_key"] = *ku.ChannelKey
			}
			if ku.Remark != nil {
				updates["remark"] = *ku.Remark
			}
			if len(updates) == 0 {
				continue
			}
			if err := tx.Model(&model.ChannelKey{}).
				Where("id = ? AND channel_id = ?", ku.ID, req.ID).
				Updates(updates).Error; err != nil {
				tx.Rollback()
				return nil, fmt.Errorf("failed to update channel key %d: %w", ku.ID, err)
			}
		}
	}

	// 新增 keys
	var committedNewKeys []model.ChannelKey
	if len(req.KeysToAdd) > 0 {
		newKeys := make([]model.ChannelKey, 0, len(req.KeysToAdd))
		for _, ka := range req.KeysToAdd {
			newKeys = append(newKeys, model.ChannelKey{
				ChannelID:  req.ID,
				Enabled:    ka.Enabled,
				ChannelKey: ka.ChannelKey,
				Remark:     ka.Remark,
			})
		}
		if err := tx.Create(&newKeys).Error; err != nil {
			tx.Rollback()
			return nil, fmt.Errorf("failed to create channel keys: %w", err)
		}
		// Keep the created rows (with their backfilled IDs) for the fallback
		// cache rebuild in case the post-commit refresh fails.
		committedNewKeys = newKeys
	}

	if err := tx.Commit().Error; err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	// committedKeys mirrors the exact key set the transaction just committed:
	// deletions and updates applied, created keys carrying their new IDs. It
	// lets the fallback below rebuild the runtime caches so deleted keys
	// never linger when the authoritative DB refresh fails.
	committedKeys := make([]model.ChannelKey, 0, len(previousKeys)+len(committedNewKeys))
	for _, key := range previousKeys {
		deleted := false
		for _, id := range req.KeysToDelete {
			if id == key.ID {
				deleted = true
				break
			}
		}
		if deleted {
			continue
		}
		for _, ku := range req.KeysToUpdate {
			if ku.ID != key.ID {
				continue
			}
			if ku.Enabled != nil {
				key.Enabled = *ku.Enabled
			}
			if ku.ChannelKey != nil {
				key.ChannelKey = *ku.ChannelKey
			}
			if ku.Remark != nil {
				key.Remark = *ku.Remark
			}
			break
		}
		committedKeys = append(committedKeys, key)
	}
	committedKeys = append(committedKeys, committedNewKeys...)

	// 刷新缓存并返回最新数据
	if err := channelRefreshCacheByIDLocked(req.ID, ctx); err != nil {
		// The update is already committed, so the committed protocol/type/base
		// URL state and the committed key set must be reflected in the cache
		// before surfacing the refresh error; otherwise a manual protocol mode
		// would keep running on the stale pre-update cache and deleted keys
		// would keep being served.
		log.Errorf("refresh channel cache after update failed (channel %d): %v", req.ID, err)
		candidate.Keys = committedKeys
		applyCommittedChannelUpdateToCache(req.ID, &candidate, committedKeys, previousKeys)
		return nil, err
	}

	channel, _ := channelCache.Get(req.ID)
	normalizeChannelProxyFields(&channel)
	channelCache.Set(req.ID, channel)
	resetBalancerStateForChannel(req.ID)
	return &channel, nil
}

func ChannelEnabled(id int, enabled bool, ctx context.Context) error {
	unlock := lockChannelMutation(id)
	defer unlock()

	oldChannel, ok := channelCache.Get(id)
	if !ok {
		return fmt.Errorf("channel not found")
	}
	if _, managed, err := ChannelManagedBinding(id, ctx); err != nil {
		return err
	} else if managed {
		return fmt.Errorf("managed site channel is read-only; please enable or disable it from the site account")
	}
	if err := db.GetDB().WithContext(ctx).Model(&model.Channel{}).Where("id = ?", id).Update("enabled", enabled).Error; err != nil {
		return err
	}
	oldChannel.Enabled = enabled
	normalizeChannelProxyFields(&oldChannel)
	channelCache.Set(id, oldChannel)
	resetBalancerStateForChannel(id)
	return nil
}

func ChannelEnabledManaged(id int, enabled bool, ctx context.Context) error {
	unlock := lockChannelMutation(id)
	defer unlock()

	oldChannel, ok := channelCache.Get(id)
	if !ok {
		return fmt.Errorf("channel not found")
	}
	if err := db.GetDB().WithContext(ctx).Model(&model.Channel{}).Where("id = ?", id).Update("enabled", enabled).Error; err != nil {
		return err
	}
	oldChannel.Enabled = enabled
	normalizeChannelProxyFields(&oldChannel)
	channelCache.Set(id, oldChannel)
	resetBalancerStateForChannel(id)
	return nil
}

func ChannelDel(id int, ctx context.Context) error {
	return channelDel(id, ctx, false)
}

func ChannelDelManaged(id int, ctx context.Context) error {
	if _, managed, err := ChannelManagedBinding(id, ctx); err != nil {
		return err
	} else if !managed {
		return fmt.Errorf("channel is not a managed site channel")
	}
	return channelDel(id, ctx, true)
}

func channelDel(id int, ctx context.Context, bypassManagedCheck bool) error {
	unlock := lockChannelMutation(id)
	defer unlock()
	ch, ok := channelCache.Get(id)
	if !ok {
		return fmt.Errorf("channel not found")
	}
	if !bypassManagedCheck {
		if _, managed, err := ChannelManagedBinding(id, ctx); err != nil {
			return err
		} else if managed {
			return fmt.Errorf("managed site channel cannot be deleted directly; delete the site account or site binding instead")
		}
	}

	// 开启事务
	tx := db.GetDB().WithContext(ctx).Begin()
	defer func() {
		if r := recover(); r != nil {
			tx.Rollback()
		}
	}()

	// 获取所有受影响的 GroupID，用于刷新缓存
	var affectedGroupIDs []int
	if err := tx.Model(&model.GroupItem{}).
		Where("channel_id = ?", id).
		Pluck("group_id", &affectedGroupIDs).Error; err != nil {
		tx.Rollback()
		return fmt.Errorf("failed to get affected groups: %w", err)
	}

	// 删除所有引用该渠道的 GroupItem
	if err := tx.Where("channel_id = ?", id).Delete(&model.GroupItem{}).Error; err != nil {
		tx.Rollback()
		return fmt.Errorf("failed to delete group items: %w", err)
	}

	// 删除渠道 keys
	if err := tx.Where("channel_id = ?", id).Delete(&model.ChannelKey{}).Error; err != nil {
		tx.Rollback()
		return fmt.Errorf("failed to delete channel keys: %w", err)
	}

	// 删除统计数据
	if err := tx.Where("channel_id = ?", id).Delete(&model.StatsChannel{}).Error; err != nil {
		tx.Rollback()
		return fmt.Errorf("failed to delete channel stats: %w", err)
	}

	// 删除渠道
	if err := tx.Delete(&model.Channel{}, id).Error; err != nil {
		tx.Rollback()
		return fmt.Errorf("failed to delete channel: %w", err)
	}

	if err := tx.Commit().Error; err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	// 删除缓存
	channelCache.Del(id)
	for _, k := range ch.Keys {
		if k.ID != 0 {
			dropChannelKeyCacheEntry(k.ID)
		}
	}
	StatsChannelDel(id)
	resetBalancerStateForChannel(id)

	// 刷新受影响的分组缓存
	for _, groupID := range affectedGroupIDs {
		if err := groupRefreshCacheByID(groupID, ctx); err != nil {
			log.Warnf("failed to refresh group cache for group %d: %v", groupID, err)
		}
	}

	return nil
}

func ChannelLLMList(ctx context.Context) ([]model.LLMChannel, error) {
	channelsByID := channelCache.GetAll()
	channelIDs := make([]int, 0, len(channelsByID))
	for channelID := range channelsByID {
		channelIDs = append(channelIDs, channelID)
	}
	bindingMap, err := SiteChannelBindingMapByChannelIDs(channelIDs, ctx)
	if err != nil {
		return nil, err
	}
	siteCache := make(map[int]*model.Site)
	accountCache := make(map[int]*model.SiteAccount)

	models := []model.LLMChannel{}
	for _, channel := range channelsByID {
		var binding *model.SiteChannelBinding
		if item, ok := bindingMap[channel.ID]; ok {
			copy := item
			binding = &copy
		}
		siteName := ""
		siteAccountName := ""
		siteGroupKey := ""
		siteGroupName := ""
		endpointType := "openai"
		var siteID *int
		var siteAccountID *int
		if binding != nil {
			siteID = &binding.SiteID
			siteAccountID = &binding.SiteAccountID
			siteGroupKey = model.NormalizeSiteGroupKey(binding.GroupKey)
			if site, ok := siteCache[binding.SiteID]; ok {
				siteName = site.Name
			} else if site, getErr := SiteGet(binding.SiteID, ctx); getErr == nil {
				siteCache[binding.SiteID] = site
				siteName = site.Name
			}
			if account, ok := accountCache[binding.SiteAccountID]; ok {
				siteAccountName = account.Name
			} else if account, getErr := SiteAccountGet(binding.SiteAccountID, ctx); getErr == nil {
				accountCache[binding.SiteAccountID] = account
				siteAccountName = account.Name
			}
			siteGroupName = siteGroupKey
			if binding.SiteUserGroupID != nil && *binding.SiteUserGroupID > 0 {
				if account := accountCache[binding.SiteAccountID]; account != nil {
					for _, group := range account.UserGroups {
						if group.ID == *binding.SiteUserGroupID {
							siteGroupName = model.NormalizeSiteGroupName(group.GroupKey, group.Name)
							siteGroupKey = model.NormalizeSiteGroupKey(group.GroupKey)
							break
						}
					}
				}
			}
			if siteGroupName == "" {
				siteGroupName = model.NormalizeSiteGroupName(siteGroupKey, "")
			}
			switch channel.Type {
			case model2.OutboundTypeAnthropic:
				endpointType = "anthropic"
			case model2.OutboundTypeGemini:
				endpointType = "gemini"
			default:
				endpointType = "openai"
			}
		}
		modelNames := xstrings.SplitTrimCompact(",", channel.Model, channel.CustomModel)
		for _, modelName := range modelNames {
			if modelName == "" {
				continue
			}
			models = append(models, model.LLMChannel{
				Name:            modelName,
				Enabled:         channel.Enabled,
				ChannelID:       channel.ID,
				ChannelName:     channel.Name,
				SiteID:          siteID,
				SiteAccountID:   siteAccountID,
				SiteGroupKey:    siteGroupKey,
				SiteGroupName:   siteGroupName,
				SiteName:        siteName,
				SiteAccountName: siteAccountName,
				EndpointType:    endpointType,
			})
		}
	}
	return models, nil
}

func ChannelGet(id int, ctx context.Context) (*model.Channel, error) {
	channel, ok := channelCache.Get(id)
	if !ok {
		return nil, fmt.Errorf("channel not found")
	}
	normalizeChannelProxyFields(&channel)
	return &channel, nil
}

// ChannelGetAuthoritative reloads a channel and its enabled-key candidates from
// the database, then reconciles the runtime caches. Callers that need to make a
// management decision must use this instead of the cache-only ChannelGet: the
// relay may temporarily prune Base URLs while updating delay information.
func ChannelGetAuthoritative(id int, ctx context.Context) (*model.Channel, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if id <= 0 {
		return nil, ErrChannelNotFound
	}
	unlock := lockChannelMutation(id)
	defer unlock()
	if err := channelRefreshCacheByIDLocked(id, ctx); err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrChannelNotFound
		}
		return nil, err
	}
	channel, ok := channelCache.Get(id)
	if !ok {
		return nil, ErrChannelNotFound
	}
	normalizeChannelProxyFields(&channel)
	return &channel, nil
}

func ChannelGetByName(name string, ctx context.Context) (*model.Channel, error) {
	trimmed := strings.TrimSpace(name)
	if trimmed == "" {
		return nil, fmt.Errorf("channel name is empty")
	}

	var channel model.Channel
	if err := db.GetDB().WithContext(ctx).
		Preload("Keys").
		Where("name = ?", trimmed).
		First(&channel).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			for id, cached := range channelCache.GetAll() {
				if cached.Name != trimmed {
					continue
				}
				unlock := lockChannelMutation(id)
				current, ok := channelCache.Get(id)
				if ok && current.Name == trimmed {
					channelCache.Del(id)
					for _, key := range current.Keys {
						if key.ID != 0 {
							channelKeyCache.Del(key.ID)
						}
					}
				}
				unlock()
			}
		}
		return nil, err
	}

	unlock := lockChannelMutation(channel.ID)
	defer unlock()
	if err := channelRefreshCacheByIDLocked(channel.ID, ctx); err != nil {
		return nil, err
	}
	refreshed, ok := channelCache.Get(channel.ID)
	if !ok {
		return nil, fmt.Errorf("channel not found")
	}
	return &refreshed, nil
}

// applyCommittedChannelUpdateToCache is the fallback used when a committed
// ChannelUpdate cannot be re-read from the database. It copies the committed
// protocol/type/base URL state of the candidate onto the cached channel (or
// caches the candidate directly if the channel was evicted) and rebuilds the
// cached Keys from the exact key set the transaction committed (created keys
// carry their new IDs, deleted keys are gone), so the runtime stops serving
// the pre-update protocol state and deleted keys can no longer be selected
// via GetChannelKey. Callers must hold the channel mutation lock.
func applyCommittedChannelUpdateToCache(id int, candidate *model.Channel, committedKeys []model.ChannelKey, previousKeys []model.ChannelKey) {
	keys := make([]model.ChannelKey, len(committedKeys))
	copy(keys, committedKeys)

	if current, ok := channelCache.Get(id); ok {
		channel := current
		channel.Type = candidate.Type
		if candidate.BaseUrls == nil {
			channel.BaseUrls = nil
		} else {
			baseURLs := make([]model.BaseUrl, len(candidate.BaseUrls))
			copy(baseURLs, candidate.BaseUrls)
			channel.BaseUrls = baseURLs
		}
		channel.OpenAIProtocolMode = candidate.OpenAIProtocolMode
		channel.OpenAIChatCapability = candidate.OpenAIChatCapability
		channel.OpenAIResponsesCapability = candidate.OpenAIResponsesCapability
		channel.Keys = keys
		channelCache.Set(id, channel)
	} else {
		candidate.Keys = keys
		channelCache.Set(id, *candidate)
	}

	pendingKeyIDs := channelKeyPendingSnapshot()
	committedKeyIDs := make(map[int]struct{}, len(keys))
	for _, key := range keys {
		if key.ID == 0 {
			continue
		}
		committedKeyIDs[key.ID] = struct{}{}
		merged := key
		if _, isPending := pendingKeyIDs[key.ID]; isPending {
			if cachedKey, ok := channelKeyCache.Get(key.ID); ok {
				// The admin update only touches enabled/channel_key/remark;
				// a runtime update not yet persisted must not be rolled back
				// to the stale DB values the update request was built from.
				merged.StatusCode = cachedKey.StatusCode
				merged.LastUseTimeStamp = cachedKey.LastUseTimeStamp
				merged.TotalCost = cachedKey.TotalCost
			}
		}
		channelKeyCache.Set(key.ID, merged)
	}
	for _, key := range previousKeys {
		if key.ID == 0 {
			continue
		}
		if _, stillCommitted := committedKeyIDs[key.ID]; stillCommitted {
			continue
		}
		dropChannelKeyCacheEntry(key.ID)
	}
	resetBalancerStateForChannel(id)
}

// channelRefreshCache rebuilds the whole channel cache from the database.
// The initial query only fetches channel IDs; each channel row is re-read
// from the database while holding that channel's mutation lock, so the
// refresh can never overwrite a committed protocol learning result or a
// committed channel update with the stale ID snapshot taken before the
// locks. Keys with a pending runtime update keep their in-memory runtime
// fields, and channels that vanished from the database are dropped only
// after re-checking the database under the channel lock, so a channel
// concurrently (re-)created after the ID snapshot is never evicted.
func channelRefreshCache(ctx context.Context) error {
	var channelIDs []int
	if err := db.GetDB().WithContext(ctx).
		Model(&model.Channel{}).
		Order("id").
		Pluck("id", &channelIDs).Error; err != nil {
		log.Warnf("failed to get channel ids: %v", err)
		return err
	}

	dbChannelIDs := make(map[int]struct{}, len(channelIDs))
	for _, channelID := range channelIDs {
		dbChannelIDs[channelID] = struct{}{}
	}

	for _, channelID := range channelIDs {
		if err := channelRefreshCacheByID(channelID, ctx); err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				// The channel was deleted between the ID snapshot and the
				// locked re-read; the stale cache entry is handled below.
				continue
			}
			log.Warnf("failed to refresh channel cache for channel %d: %v", channelID, err)
			return err
		}
	}

	// Drop cache entries of channels that no longer exist in the database.
	// The ID list is snapshotted first: iterating the live cache map while
	// acquiring mutation locks could deadlock with concurrent cache writers.
	// Each candidate is re-checked against the database while holding the
	// channel lock so a concurrently re-created channel is never evicted.
	cachedIDs := make([]int, 0, channelCache.Len())
	for channelID := range channelCache.GetAll() {
		cachedIDs = append(cachedIDs, channelID)
	}
	for _, channelID := range cachedIDs {
		if _, ok := dbChannelIDs[channelID]; ok {
			continue
		}
		unlock := lockChannelMutation(channelID)
		var count int64
		if err := db.GetDB().WithContext(ctx).Model(&model.Channel{}).
			Where("id = ?", channelID).Limit(1).Count(&count).Error; err != nil || count > 0 {
			unlock()
			continue
		}
		if ch, stillCached := channelCache.Get(channelID); stillCached {
			channelCache.Del(channelID)
			for _, k := range ch.Keys {
				dropChannelKeyCacheEntry(k.ID)
			}
		}
		unlock()
	}
	return nil
}

func channelRefreshCacheByID(id int, ctx context.Context) error {
	unlock := lockChannelMutation(id)
	defer unlock()
	return channelRefreshCacheByIDLocked(id, ctx)
}

func channelRefreshCacheByIDLocked(id int, ctx context.Context) error {
	// Read the latest channel from the database first: only after a
	// successful read are the old key-cache entries reconciled, so a failed
	// refresh never leaves the channel cache stale *and* the key cache empty.
	// Callers hold the channel mutation lock, so everything committed before
	// this call (protocol learning, channel updates, key deletions) is
	// visible in the re-read row.
	var channel model.Channel
	if err := db.GetDB().WithContext(ctx).
		Preload("Keys").
		First(&channel, id).Error; err != nil {
		return err
	}

	previous, hadPrevious := channelCache.Get(id)
	previousKeyIDs := make(map[int]struct{}, len(previous.Keys))
	if hadPrevious {
		for _, k := range previous.Keys {
			if k.ID != 0 {
				previousKeyIDs[k.ID] = struct{}{}
			}
		}
	}

	// Keys with a pending runtime update keep their in-memory runtime fields
	// (status_code / last_use_time_stamp / total_cost): the database row may
	// predate the latest ChannelKeyUpdate that ChannelKeySaveDB has not
	// persisted yet, and the pending marker must survive the refresh.
	pendingKeyIDs := channelKeyPendingSnapshot()
	normalizeChannelProxyFields(&channel)
	channel.Stats = nil
	if len(channel.Keys) > 0 && len(pendingKeyIDs) > 0 {
		for i := range channel.Keys {
			loaded := channel.Keys[i]
			if _, isPending := pendingKeyIDs[loaded.ID]; !isPending {
				continue
			}
			if cachedKey, ok := channelKeyCache.Get(loaded.ID); ok {
				loaded.StatusCode = cachedKey.StatusCode
				loaded.LastUseTimeStamp = cachedKey.LastUseTimeStamp
				loaded.TotalCost = cachedKey.TotalCost
				channel.Keys[i] = loaded
			}
		}
	}

	channelCache.Set(channel.ID, channel)
	for _, k := range channel.Keys {
		if k.ID != 0 {
			channelKeyCache.Set(k.ID, k)
		}
	}

	// Keys that disappeared from the committed database row were deleted:
	// drop them from the key cache together with their pending markers so
	// they cannot be resurrected by a later ChannelKeyUpdate or save.
	for keyID := range previousKeyIDs {
		stillCurrent := false
		for _, k := range channel.Keys {
			if k.ID == keyID {
				stillCurrent = true
				break
			}
		}
		if !stillCurrent {
			dropChannelKeyCacheEntry(keyID)
		}
	}
	return nil
}

// ChannelExpireOpenAIProtocolUnsupported 把过了 TTL 的 unsupported 判定回落为
// unknown，好让上游后来补上的协议还有机会被重新试探。运行时学习是单调的
// （只写 supported/unsupported，从不写回 unknown），没有这个到期机制，一个
// 曾经不支持的上游会被永久钉在候选末位，即使它后来支持了也永远不再被选中。
//
// 只处理 auto 模式：手动打勾是管理员的决定，永不到期。每个渠道都在自己的
// mutation lock 内复核后再写，与 ChannelRecordOpenAIProtocolCapability 的
// 并发写入互斥，并复用同一套缓存同步路径。返回被重置的协议判定条数。
func ChannelExpireOpenAIProtocolUnsupported(ctx context.Context, ttl time.Duration, now time.Time) (int, error) {
	if ttl <= 0 {
		return 0, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	cutoff := now.Add(-ttl).Unix()

	// 候选只用于收窄要加锁的渠道集合，真正的判定在锁内用权威行重做一遍。
	var candidateIDs []int
	if err := db.GetDB().WithContext(ctx).
		Model(&model.Channel{}).
		Where("(openai_protocol_mode = ? OR openai_protocol_mode = '' OR openai_protocol_mode IS NULL) AND type IN ?",
			model.OpenAIProtocolModeAuto,
			[]model2.OutboundType{model2.OutboundTypeOpenAIChat, model2.OutboundTypeOpenAIResponse}).
		Where("(openai_chat_capability = ? AND openai_chat_capability_at <= ?) OR (openai_responses_capability = ? AND openai_responses_capability_at <= ?)",
			model.OpenAIProtocolCapabilityUnsupported, cutoff,
			model.OpenAIProtocolCapabilityUnsupported, cutoff).
		Pluck("id", &candidateIDs).Error; err != nil {
		return 0, err
	}

	reset := 0
	var firstErr error
	for _, channelID := range candidateIDs {
		if err := ctx.Err(); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			break
		}
		count, err := expireChannelOpenAIProtocolUnsupported(ctx, channelID, cutoff)
		if err != nil {
			log.Warnf("failed to expire openai protocol capability for channel %d: %v", channelID, err)
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		reset += count
	}
	return reset, firstErr
}

// expireChannelOpenAIProtocolUnsupported 在单个渠道的 mutation lock 内复核并
// 重置到期的 unsupported 列。cutoff 之后写入的观察不受影响。
func expireChannelOpenAIProtocolUnsupported(ctx context.Context, channelID int, cutoff int64) (int, error) {
	unlock := lockChannelMutation(channelID)
	defer unlock()

	state, err := readChannelProtocolState(ctx, db.GetDB(), channelID)
	if err != nil {
		return 0, err
	}
	if state == nil {
		// 渠道已被并发删除：不写、不复活缓存。
		return 0, nil
	}
	if !model.IsOpenAITextChannelType(state.Type) || state.Mode != model.OpenAIProtocolModeAuto {
		// 类型或模式在候选查询之后被改了：权威状态胜出，只对齐缓存。
		cached, hadCached := cachedChannelForProtocolSync(channelID)
		syncChannelProtocolCache(channelID, cached, hadCached, state)
		return 0, nil
	}

	updates := make(map[string]any, 4)
	if state.Chat == model.OpenAIProtocolCapabilityUnsupported && state.ChatAt <= cutoff {
		updates["openai_chat_capability"] = model.OpenAIProtocolCapabilityUnknown
		updates["openai_chat_capability_at"] = 0
	}
	if state.Responses == model.OpenAIProtocolCapabilityUnsupported && state.ResponsesAt <= cutoff {
		updates["openai_responses_capability"] = model.OpenAIProtocolCapabilityUnknown
		updates["openai_responses_capability_at"] = 0
	}
	if len(updates) == 0 {
		return 0, nil
	}

	// WHERE 再次带上 auto 模式与类型约束：即使锁外发生了外部直写，也不会把
	// 手动选择或已改类型的渠道误重置。
	if err := db.GetDB().WithContext(ctx).
		Model(&model.Channel{}).
		Where("id = ? AND (openai_protocol_mode = ? OR openai_protocol_mode = '' OR openai_protocol_mode IS NULL) AND type IN ?",
			channelID, model.OpenAIProtocolModeAuto,
			[]model2.OutboundType{model2.OutboundTypeOpenAIChat, model2.OutboundTypeOpenAIResponse}).
		Updates(updates).Error; err != nil {
		return 0, err
	}

	// 把重置结果落到 state 上，再走与写入路径相同的缓存同步。
	if _, ok := updates["openai_chat_capability"]; ok {
		state.Chat = model.OpenAIProtocolCapabilityUnknown
		state.ChatAt = 0
	}
	if _, ok := updates["openai_responses_capability"]; ok {
		state.Responses = model.OpenAIProtocolCapabilityUnknown
		state.ResponsesAt = 0
	}
	cached, hadCached := cachedChannelForProtocolSync(channelID)
	syncChannelProtocolCache(channelID, cached, hadCached, state)
	return len(updates) / 2, nil
}

// cachedChannelForProtocolSync 取出缓存中的渠道副本并做代理字段归一化，
// 供协议列同步使用。缺失缓存条目时第二个返回值为 false。
func cachedChannelForProtocolSync(channelID int) (model.Channel, bool) {
	cached, hadCached := channelCache.Get(channelID)
	if hadCached {
		normalizeChannelProxyFields(&cached)
	}
	return cached, hadCached
}
