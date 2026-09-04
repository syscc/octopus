package op

import (
	"context"
	"testing"
	"time"

	dbpkg "github.com/bestruirui/octopus/internal/db"
	"github.com/bestruirui/octopus/internal/model"
	"github.com/bestruirui/octopus/internal/transformer/outbound"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setChannelProtocolCapabilityAt 直接修改数据库时间戳，绕过 ChannelUpdate 的
// 业务逻辑，用于测试 TTL 到期任务。
func setChannelProtocolCapabilityAt(t *testing.T, ctx context.Context, channelID int, protocol outbound.OutboundType, capability model.OpenAIProtocolCapability, observedAt int64) {
	t.Helper()
	var column, stampColumn string
	if protocol == outbound.OutboundTypeOpenAIChat {
		column = "openai_chat_capability"
		stampColumn = "openai_chat_capability_at"
	} else {
		column = "openai_responses_capability"
		stampColumn = "openai_responses_capability_at"
	}
	err := dbpkg.GetDB().WithContext(ctx).
		Model(&model.Channel{}).
		Where("id = ?", channelID).
		Updates(map[string]interface{}{
			column:      capability,
			stampColumn: observedAt,
		}).Error
	require.NoError(t, err)
}

func TestChannelOpenAIProtocolUnsupportedExpiryResetsAfterTTL(t *testing.T) {
	ctx := setupOpenAIProtocolTestDB(t)

	now := time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC)
	chatCh := createOpenAIProtocolTestChannel(t, ctx, "expiry-chat", outbound.OutboundTypeOpenAIChat, []model.BaseUrl{{URL: "https://api.example.com"}})
	respCh := createOpenAIProtocolTestChannel(t, ctx, "expiry-resp", outbound.OutboundTypeOpenAIResponse, []model.BaseUrl{{URL: "https://api.example.com"}})

	// 标记两个渠道的主协议为 unsupported，时间戳在 8 天前
	old := now.Add(-8 * 24 * time.Hour)
	setChannelProtocolCapabilityAt(t, ctx, chatCh.ID, outbound.OutboundTypeOpenAIChat, model.OpenAIProtocolCapabilityUnsupported, old.Unix())
	setChannelProtocolCapabilityAt(t, ctx, respCh.ID, outbound.OutboundTypeOpenAIResponse, model.OpenAIProtocolCapabilityUnsupported, old.Unix())

	// 运行过期任务，TTL = 7 天
	reset, err := ChannelExpireOpenAIProtocolUnsupported(ctx, 7*24*time.Hour, now)
	require.NoError(t, err)
	assert.Equal(t, 2, reset, "应该重置 2 个协议列")

	// 验证回落到 unknown + 时间戳清零
	chatState := loadChannelProtocolStateFromDB(t, ctx, chatCh.ID)
	assert.Equal(t, model.OpenAIProtocolCapabilityUnknown, chatState.Chat)

	respState := loadChannelProtocolStateFromDB(t, ctx, respCh.ID)
	assert.Equal(t, model.OpenAIProtocolCapabilityUnknown, respState.Responses)
}

func TestChannelOpenAIProtocolUnsupportedExpiryKeepsRecentVerdicts(t *testing.T) {
	ctx := setupOpenAIProtocolTestDB(t)

	now := time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC)
	ch := createOpenAIProtocolTestChannel(t, ctx, "recent", outbound.OutboundTypeOpenAIChat, []model.BaseUrl{{URL: "https://api.example.com"}})

	// 6 天前标记的 unsupported，还在 7 天 TTL 内
	recent := now.Add(-6 * 24 * time.Hour)
	setChannelProtocolCapabilityAt(t, ctx, ch.ID, outbound.OutboundTypeOpenAIChat, model.OpenAIProtocolCapabilityUnsupported, recent.Unix())

	reset, err := ChannelExpireOpenAIProtocolUnsupported(ctx, 7*24*time.Hour, now)
	require.NoError(t, err)
	assert.Equal(t, 0, reset, "TTL 内的判定不应被重置")

	state := loadChannelProtocolStateFromDB(t, ctx, ch.ID)
	assert.Equal(t, model.OpenAIProtocolCapabilityUnsupported, state.Chat)
}

func TestChannelOpenAIProtocolUnsupportedExpiryIgnoresManualMode(t *testing.T) {
	ctx := setupOpenAIProtocolTestDB(t)

	now := time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC)
	ch := createOpenAIProtocolTestChannel(t, ctx, "manual", outbound.OutboundTypeOpenAIChat, []model.BaseUrl{{URL: "https://api.example.com"}})

	// 8 天前标记，但现在是 chat_only 手动模式
	old := now.Add(-8 * 24 * time.Hour)
	setChannelProtocolCapabilityAt(t, ctx, ch.ID, outbound.OutboundTypeOpenAIChat, model.OpenAIProtocolCapabilityUnsupported, old.Unix())

	// 改成手动模式
	name := ch.Name
	typ := ch.Type
	enabled := ch.Enabled
	baseUrls := ch.BaseUrls
	mode := model.OpenAIProtocolModeChatOnly
	req := &model.ChannelUpdateRequest{
		ID:                 ch.ID,
		Name:               &name,
		Type:               &typ,
		Enabled:            &enabled,
		BaseUrls:           &baseUrls,
		OpenAIProtocolMode: &mode,
	}
	_, err := ChannelUpdate(req, ctx)
	require.NoError(t, err)

	reset, err := ChannelExpireOpenAIProtocolUnsupported(ctx, 7*24*time.Hour, now)
	require.NoError(t, err)
	assert.Equal(t, 0, reset, "手动模式的判定永不过期")

	state := loadChannelProtocolStateFromDB(t, ctx, ch.ID)
	assert.Equal(t, model.OpenAIProtocolModeChatOnly, state.Mode)
	assert.Equal(t, model.OpenAIProtocolCapabilityUnsupported, state.Chat)
}

func TestChannelOpenAIProtocolUnsupportedExpiryIgnoresSupportedAndUnknown(t *testing.T) {
	ctx := setupOpenAIProtocolTestDB(t)

	now := time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC)
	old := now.Add(-8 * 24 * time.Hour)

	supportedCh := createOpenAIProtocolTestChannel(t, ctx, "supported", outbound.OutboundTypeOpenAIChat, []model.BaseUrl{{URL: "https://api.example.com"}})
	setChannelProtocolCapabilityAt(t, ctx, supportedCh.ID, outbound.OutboundTypeOpenAIChat, model.OpenAIProtocolCapabilitySupported, old.Unix())

	unknownCh := createOpenAIProtocolTestChannel(t, ctx, "unknown", outbound.OutboundTypeOpenAIResponse, []model.BaseUrl{{URL: "https://api.example.com"}})
	setChannelProtocolCapabilityAt(t, ctx, unknownCh.ID, outbound.OutboundTypeOpenAIResponse, model.OpenAIProtocolCapabilityUnknown, old.Unix())

	reset, err := ChannelExpireOpenAIProtocolUnsupported(ctx, 7*24*time.Hour, now)
	require.NoError(t, err)
	assert.Equal(t, 0, reset, "supported 和 unknown 不会被过期任务改变")

	supState := loadChannelProtocolStateFromDB(t, ctx, supportedCh.ID)
	assert.Equal(t, model.OpenAIProtocolCapabilitySupported, supState.Chat)

	unkState := loadChannelProtocolStateFromDB(t, ctx, unknownCh.ID)
	assert.Equal(t, model.OpenAIProtocolCapabilityUnknown, unkState.Responses)
}

func TestChannelOpenAIProtocolUnsupportedExpiryHandlesBothColumns(t *testing.T) {
	ctx := setupOpenAIProtocolTestDB(t)

	now := time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC)
	oldChat := now.Add(-8 * 24 * time.Hour)
	recentResp := now.Add(-6 * 24 * time.Hour)

	ch := createOpenAIProtocolTestChannel(t, ctx, "mixed", outbound.OutboundTypeOpenAIChat, []model.BaseUrl{{URL: "https://api.example.com"}})
	setChannelProtocolCapabilityAt(t, ctx, ch.ID, outbound.OutboundTypeOpenAIChat, model.OpenAIProtocolCapabilityUnsupported, oldChat.Unix())
	setChannelProtocolCapabilityAt(t, ctx, ch.ID, outbound.OutboundTypeOpenAIResponse, model.OpenAIProtocolCapabilityUnsupported, recentResp.Unix())

	reset, err := ChannelExpireOpenAIProtocolUnsupported(ctx, 7*24*time.Hour, now)
	require.NoError(t, err)
	assert.Equal(t, 1, reset, "只有 chat 列过期，responses 列保持")

	state := loadChannelProtocolStateFromDB(t, ctx, ch.ID)
	assert.Equal(t, model.OpenAIProtocolCapabilityUnknown, state.Chat)
	assert.Equal(t, model.OpenAIProtocolCapabilityUnsupported, state.Responses)
}

func TestChannelOpenAIProtocolUnsupportedExpirySyncsCache(t *testing.T) {
	ctx := setupOpenAIProtocolTestDB(t)

	now := time.Date(2026, 9, 3, 12, 0, 0, 0, time.UTC)
	old := now.Add(-8 * 24 * time.Hour)

	ch := createOpenAIProtocolTestChannel(t, ctx, "cache-sync", outbound.OutboundTypeOpenAIChat, []model.BaseUrl{{URL: "https://api.example.com"}})

	// 直接改 DB，此时缓存里还是初始 unknown
	setChannelProtocolCapabilityAt(t, ctx, ch.ID, outbound.OutboundTypeOpenAIChat, model.OpenAIProtocolCapabilityUnsupported, old.Unix())

	// ChannelGetAuthoritative 会从 DB 重载并回填缓存（ChannelGet 是纯缓存读）
	got, err := ChannelGetAuthoritative(ch.ID, ctx)
	require.NoError(t, err)
	assert.Equal(t, model.OpenAIProtocolCapabilityUnsupported, got.OpenAIChatCapability, "重载后应该看到 DB 里的 unsupported")

	// 缓存里现在是 unsupported
	cached := cachedChannelProtocolState(t, ctx, ch.ID)
	assert.Equal(t, model.OpenAIProtocolCapabilityUnsupported, cached.Chat)

	// 运行过期任务
	reset, err := ChannelExpireOpenAIProtocolUnsupported(ctx, 7*24*time.Hour, now)
	require.NoError(t, err)
	assert.Equal(t, 1, reset)

	// 缓存已同步为 unknown
	cachedAfter := cachedChannelProtocolState(t, ctx, ch.ID)
	assert.Equal(t, model.OpenAIProtocolCapabilityUnknown, cachedAfter.Chat)
}
