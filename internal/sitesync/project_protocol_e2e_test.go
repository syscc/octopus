package sitesync

import (
	"testing"

	dbpkg "github.com/bestruirui/octopus/internal/db"
	"github.com/bestruirui/octopus/internal/model"
	"github.com/bestruirui/octopus/internal/op"
	"github.com/bestruirui/octopus/internal/transformer/outbound"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// 端到端：站点模型的协议勾选 → 投影渠道的 protocol_mode → relay 排序时的
// EffectiveOpenAIProtocolCapability。这是用户"勾两个协议"后网关智能路由的完整链路。
func TestProtocolFallbackProjectionEndToEnd(t *testing.T) {
	ctx := setupProjectTestDB(t)
	site, account := createProjectionFixture(t, ctx)

	// 修改其中一个模型：锁定单协议
	disableOne := true
	require.NoError(t, op.SiteModelRoutesUpdate(site.ID, account.ID, []model.SiteModelRouteUpdateRequest{
		{
			GroupKey:                model.SiteDefaultGroupKey,
			ModelName:               "gpt-4o-mini",
			RouteType:               model.SiteModelRouteTypeOpenAIChat,
			DisableProtocolFallback: &disableOne,
		},
	}, ctx))

	// 再加一个允许降级的 OpenAI 模型
	allowModel := model.SiteModel{
		SiteAccountID: account.ID,
		GroupKey:      model.SiteDefaultGroupKey,
		ModelName:     "gpt-4o",
		Source:        "sync",
		RouteType:     model.SiteModelRouteTypeOpenAIChat,
		RouteSource:   model.SiteModelRouteSourceSyncInferred,
		// DisableProtocolFallback 默认 false
	}
	require.NoError(t, dbpkg.GetDB().WithContext(ctx).Create(&allowModel).Error)

	// 投影
	_, err := ProjectAccount(ctx, account.ID)
	require.NoError(t, err)

	// 验证：桶内有模型允许降级，投影出的 OpenAI 渠道模式应该是 auto。
	// binding 的 group_key 是 "default::0"（复合 key 带 outbound type）。
	channelsByGroup := loadProjectedChannelsByGroupKey(t, ctx, account.ID)
	openaiChannel := findChannelByOutboundType(t, channelsByGroup, outbound.OutboundTypeOpenAIChat)

	assert.Equal(t, model.OpenAIProtocolModeAuto, openaiChannel.OpenAIProtocolMode,
		"桶内有模型允许降级，渠道模式应该是 auto")

	// 验证协议排序时的行为：auto 模式下返回运行时学到的真实能力
	// 默认 unknown，所以两个协议排名相同
	chatCap := openaiChannel.EffectiveOpenAIProtocolCapability(outbound.OutboundTypeOpenAIChat)
	respCap := openaiChannel.EffectiveOpenAIProtocolCapability(outbound.OutboundTypeOpenAIResponse)
	assert.Equal(t, model.OpenAIProtocolCapabilityUnknown, chatCap)
	assert.Equal(t, model.OpenAIProtocolCapabilityUnknown, respCap)

	// 模拟运行时学习：Chat 支持，Responses 不支持
	channelID := openaiChannel.ID
	require.NoError(t, dbpkg.GetDB().WithContext(ctx).
		Model(&model.Channel{}).
		Where("id = ?", channelID).
		Updates(map[string]interface{}{
			"openai_chat_capability":      model.OpenAIProtocolCapabilitySupported,
			"openai_responses_capability": model.OpenAIProtocolCapabilityUnsupported,
		}).Error)

	// 重新加载渠道,验证能力排序
	channelsByGroup = loadProjectedChannelsByGroupKey(t, ctx, account.ID)
	openaiChannel = findChannelByOutboundType(t, channelsByGroup, outbound.OutboundTypeOpenAIChat)
	chatCap = openaiChannel.EffectiveOpenAIProtocolCapability(outbound.OutboundTypeOpenAIChat)
	respCap = openaiChannel.EffectiveOpenAIProtocolCapability(outbound.OutboundTypeOpenAIResponse)
	assert.Equal(t, model.OpenAIProtocolCapabilitySupported, chatCap,
		"运行时学到的 supported 应该生效")
	assert.Equal(t, model.OpenAIProtocolCapabilityUnsupported, respCap,
		"运行时学到的 unsupported 应该生效")
}

// 边界：用户手动把桶内所有 OpenAI 模型都锁死在同一个单协议后，投影出的渠道
// 必须是手动模式，禁止运行时探测，避免第一次请求浪费一次尝试。
func TestProtocolFallbackAllModelsLockedProjectsToChatOnly(t *testing.T) {
	ctx := setupProjectTestDB(t)
	site, account := createProjectionFixture(t, ctx)

	// 给账号加第二个 OpenAI 模型
	secondModel := model.SiteModel{
		SiteAccountID: account.ID,
		GroupKey:      model.SiteDefaultGroupKey,
		ModelName:     "gpt-4o",
		Source:        "sync",
		RouteType:     model.SiteModelRouteTypeOpenAIChat,
		RouteSource:   model.SiteModelRouteSourceSyncInferred,
	}
	require.NoError(t, dbpkg.GetDB().WithContext(ctx).Create(&secondModel).Error)

	// 两个模型全部锁死在 Chat
	disable := true
	require.NoError(t, op.SiteModelRoutesUpdate(site.ID, account.ID, []model.SiteModelRouteUpdateRequest{
		{
			GroupKey:                model.SiteDefaultGroupKey,
			ModelName:               "gpt-4o-mini",
			RouteType:               model.SiteModelRouteTypeOpenAIChat,
			DisableProtocolFallback: &disable,
		},
		{
			GroupKey:                model.SiteDefaultGroupKey,
			ModelName:               "gpt-4o",
			RouteType:               model.SiteModelRouteTypeOpenAIChat,
			DisableProtocolFallback: &disable,
		},
	}, ctx))

	_, err := ProjectAccount(ctx, account.ID)
	require.NoError(t, err)

	channelsByGroup := loadProjectedChannelsByGroupKey(t, ctx, account.ID)
	openaiChannel := findChannelByOutboundType(t, channelsByGroup, outbound.OutboundTypeOpenAIChat)

	assert.Equal(t, model.OpenAIProtocolModeChatOnly, openaiChannel.OpenAIProtocolMode,
		"桶内所有模型锁定单协议，渠道必须是手动模式")
}

// findChannelByOutboundType 从投影结果里挑出指定 outbound type 的渠道。
// binding 的 group_key 是复合 key（"default::<outboundType>"），这里按渠道
// Type 直接匹配，避免测试依赖 key 的拼接格式。
func findChannelByOutboundType(t *testing.T, channelsByGroup map[string]model.Channel, obType outbound.OutboundType) model.Channel {
	t.Helper()
	for _, channel := range channelsByGroup {
		if channel.Type == obType {
			return channel
		}
	}
	t.Fatalf("no projected channel with outbound type %d, got %d channels", obType, len(channelsByGroup))
	return model.Channel{}
}

// 边界：桶内既有锁定 Chat 的又有锁定 Responses 的，分歧导致投影出的渠道
// 必须回退到 auto，让运行时探测决定真实能力，不能盲目选一个手动模式。
func TestProtocolFallbackMixedLockProjectsToAuto(t *testing.T) {
	ctx := setupProjectTestDB(t)
	site, account := createProjectionFixture(t, ctx)

	// 加第二个 OpenAI 模型
	secondModel := model.SiteModel{
		SiteAccountID: account.ID,
		GroupKey:      model.SiteDefaultGroupKey,
		ModelName:     "gpt-4o",
		Source:        "sync",
		RouteType:     model.SiteModelRouteTypeOpenAIResponse,
		RouteSource:   model.SiteModelRouteSourceSyncInferred,
	}
	require.NoError(t, dbpkg.GetDB().WithContext(ctx).Create(&secondModel).Error)

	// 一个锁 Chat，一个锁 Responses
	disable := true
	require.NoError(t, op.SiteModelRoutesUpdate(site.ID, account.ID, []model.SiteModelRouteUpdateRequest{
		{
			GroupKey:                model.SiteDefaultGroupKey,
			ModelName:               "gpt-4o-mini",
			RouteType:               model.SiteModelRouteTypeOpenAIChat,
			DisableProtocolFallback: &disable,
		},
		{
			GroupKey:                model.SiteDefaultGroupKey,
			ModelName:               "gpt-4o",
			RouteType:               model.SiteModelRouteTypeOpenAIResponse,
			DisableProtocolFallback: &disable,
		},
	}, ctx))

	projected, err := ProjectAccount(ctx, account.ID)
	require.NoError(t, err)

	// 应该投影出 4 个渠道：Chat、Responses、Anthropic、Gemini
	assert.Len(t, projected, 4, "两个 OpenAI 协议 + Anthropic + Gemini 共 4 个渠道")

	channelsByGroup := loadProjectedChannelsByGroupKey(t, ctx, account.ID)
	chatChannel := findChannelByOutboundType(t, channelsByGroup, outbound.OutboundTypeOpenAIChat)
	respChannel := findChannelByOutboundType(t, channelsByGroup, outbound.OutboundTypeOpenAIResponse)

	assert.Equal(t, model.OpenAIProtocolModeChatOnly, chatChannel.OpenAIProtocolMode)
	assert.Equal(t, model.OpenAIProtocolModeResponsesOnly, respChannel.OpenAIProtocolMode)
}
