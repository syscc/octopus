package op

import (
	"context"
	"testing"

	dbpkg "github.com/bestruirui/octopus/internal/db"
	"github.com/bestruirui/octopus/internal/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createProtocolFallbackTestSite 建一个普通 OpenAI 站点，带两个默认允许协议
// 降级的模型，用来验证协议勾选在 op 层的读写语义。
func createProtocolFallbackTestSite(t *testing.T, ctx context.Context) (*model.Site, *model.SiteAccount) {
	t.Helper()
	site := &model.Site{
		Name:     "protocol-fallback-site",
		Platform: model.SitePlatformAPI,
		BaseURL:  "https://api.protocol-test.com",
		Enabled:  true,
	}
	require.NoError(t, SiteCreate(site, ctx))

	account := &model.SiteAccount{
		SiteID:         site.ID,
		Name:           "protocol-fallback-account",
		CredentialType: model.SiteCredentialTypeAPIKey,
		APIKey:         "sk-test",
		Enabled:        true,
	}
	require.NoError(t, SiteAccountCreate(account, ctx))

	for _, item := range []struct {
		name      string
		routeType model.SiteModelRouteType
	}{
		{"gpt-4", model.SiteModelRouteTypeOpenAIChat},
		{"gpt-4o", model.SiteModelRouteTypeOpenAIChat},
	} {
		siteModel := &model.SiteModel{
			SiteAccountID: account.ID,
			GroupKey:      model.SiteDefaultGroupKey,
			ModelName:     item.name,
			Source:        "sync",
			RouteType:     item.routeType,
			RouteSource:   model.SiteModelRouteSourceSyncInferred,
		}
		require.NoError(t, dbpkg.GetDB().WithContext(ctx).Create(siteModel).Error)
	}
	return site, account
}

func loadProtocolFallback(t *testing.T, ctx context.Context, accountID int, modelName string) model.SiteModel {
	t.Helper()
	var loaded model.SiteModel
	require.NoError(t, dbpkg.GetDB().WithContext(ctx).
		Where("site_account_id = ? AND group_key = ? AND model_name = ?",
			accountID, model.SiteDefaultGroupKey, modelName).
		Take(&loaded).Error)
	return loaded
}

// 新建的站点模型必须默认允许降级：Go 零值 false 与 gorm 默认值一致，
// 这是"两个协议都打勾"的初始状态。
func TestSiteModelDefaultsToProtocolFallbackAllowed(t *testing.T) {
	ctx := setupSiteOpTestDB(t)
	_, account := createProtocolFallbackTestSite(t, ctx)

	loaded := loadProtocolFallback(t, ctx, account.ID, "gpt-4")
	assert.False(t, loaded.DisableProtocolFallback, "新模型应默认允许两个协议")
	assert.Equal(t, model.OpenAIProtocolModeAuto, loaded.ProjectedOpenAIProtocolMode())
}

// 只勾一个协议：请求带 disable_protocol_fallback=true，落库后投影模式收敛为
// 单协议手动模式。
func TestSiteModelRoutesUpdateLocksSingleProtocol(t *testing.T) {
	ctx := setupSiteOpTestDB(t)
	site, account := createProtocolFallbackTestSite(t, ctx)

	disable := true
	err := SiteModelRoutesUpdate(site.ID, account.ID, []model.SiteModelRouteUpdateRequest{{
		GroupKey:                model.SiteDefaultGroupKey,
		ModelName:               "gpt-4",
		RouteType:               model.SiteModelRouteTypeOpenAIResponse,
		DisableProtocolFallback: &disable,
	}}, ctx)
	require.NoError(t, err)

	loaded := loadProtocolFallback(t, ctx, account.ID, "gpt-4")
	assert.True(t, loaded.DisableProtocolFallback)
	assert.Equal(t, model.SiteModelRouteTypeOpenAIResponse, loaded.RouteType)
	assert.Equal(t, model.OpenAIProtocolModeResponsesOnly, loaded.ProjectedOpenAIProtocolMode())
}

// 重新勾上两个协议：disable_protocol_fallback=false 必须写回，投影模式回到
// auto，让运行时探测重新决定真实能力。
func TestSiteModelRoutesUpdateRestoresProtocolFallback(t *testing.T) {
	ctx := setupSiteOpTestDB(t)
	site, account := createProtocolFallbackTestSite(t, ctx)

	disable := true
	require.NoError(t, SiteModelRoutesUpdate(site.ID, account.ID, []model.SiteModelRouteUpdateRequest{{
		GroupKey:                model.SiteDefaultGroupKey,
		ModelName:               "gpt-4",
		RouteType:               model.SiteModelRouteTypeOpenAIChat,
		DisableProtocolFallback: &disable,
	}}, ctx))
	require.True(t, loadProtocolFallback(t, ctx, account.ID, "gpt-4").DisableProtocolFallback)

	allow := false
	require.NoError(t, SiteModelRoutesUpdate(site.ID, account.ID, []model.SiteModelRouteUpdateRequest{{
		GroupKey:                model.SiteDefaultGroupKey,
		ModelName:               "gpt-4",
		RouteType:               model.SiteModelRouteTypeOpenAIChat,
		DisableProtocolFallback: &allow,
	}}, ctx))

	loaded := loadProtocolFallback(t, ctx, account.ID, "gpt-4")
	assert.False(t, loaded.DisableProtocolFallback)
	assert.Equal(t, model.OpenAIProtocolModeAuto, loaded.ProjectedOpenAIProtocolMode())
}

// 未携带该字段的请求（老客户端）不能把勾选静默重置。
func TestSiteModelRoutesUpdateKeepsProtocolFallbackWhenOmitted(t *testing.T) {
	ctx := setupSiteOpTestDB(t)
	site, account := createProtocolFallbackTestSite(t, ctx)

	disable := true
	require.NoError(t, SiteModelRoutesUpdate(site.ID, account.ID, []model.SiteModelRouteUpdateRequest{{
		GroupKey:                model.SiteDefaultGroupKey,
		ModelName:               "gpt-4",
		RouteType:               model.SiteModelRouteTypeOpenAIChat,
		DisableProtocolFallback: &disable,
	}}, ctx))

	// 第二次只改 route_type，不带协议字段
	require.NoError(t, SiteModelRoutesUpdate(site.ID, account.ID, []model.SiteModelRouteUpdateRequest{{
		GroupKey:  model.SiteDefaultGroupKey,
		ModelName: "gpt-4",
		RouteType: model.SiteModelRouteTypeOpenAIResponse,
	}}, ctx))

	loaded := loadProtocolFallback(t, ctx, account.ID, "gpt-4")
	assert.True(t, loaded.DisableProtocolFallback, "省略该字段时原有锁定必须保留")
	assert.Equal(t, model.SiteModelRouteTypeOpenAIResponse, loaded.RouteType)
}

// 换成非 OpenAI 文本路由时协议锁定没有意义，必须归零，避免日后换回
// OpenAI 时带着一个谁都没设过的锁。
func TestSiteModelRoutesUpdateClearsProtocolLockForNonOpenAIRoute(t *testing.T) {
	ctx := setupSiteOpTestDB(t)
	site, account := createProtocolFallbackTestSite(t, ctx)

	disable := true
	require.NoError(t, SiteModelRoutesUpdate(site.ID, account.ID, []model.SiteModelRouteUpdateRequest{{
		GroupKey:                model.SiteDefaultGroupKey,
		ModelName:               "gpt-4",
		RouteType:               model.SiteModelRouteTypeOpenAIChat,
		DisableProtocolFallback: &disable,
	}}, ctx))

	require.NoError(t, SiteModelRoutesUpdate(site.ID, account.ID, []model.SiteModelRouteUpdateRequest{{
		GroupKey:  model.SiteDefaultGroupKey,
		ModelName: "gpt-4",
		RouteType: model.SiteModelRouteTypeAnthropic,
	}}, ctx))

	loaded := loadProtocolFallback(t, ctx, account.ID, "gpt-4")
	assert.False(t, loaded.DisableProtocolFallback, "非 OpenAI 路由不应保留协议锁定")
	assert.Equal(t, model.OpenAIProtocolModeAuto, loaded.ProjectedOpenAIProtocolMode())
}
