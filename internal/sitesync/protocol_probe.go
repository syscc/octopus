package sitesync

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/bestruirui/octopus/internal/helper"
	"github.com/bestruirui/octopus/internal/model"
	"github.com/bestruirui/octopus/internal/utils/log"
)

// siteProtocolProbeTimeout 给整轮探测封顶。三个协议并发、每个协议最多串行试两个
// URL 变体，同步任务本身的 ctx 可能有好几分钟，不能让一个挂着不响应的上游把它拖完。
const siteProtocolProbeTimeout = 30 * time.Second

// siteProtocolProbeInFlight 防止同一站点被多个同步流程同时探测。定时同步、手动同步、
// 编辑站点触发的重探可以并发跑（项目对同步没有互斥锁），同一站点被探两次就是白打
// 6 个上游请求。让后来者跳过，它随后读到的落库值就是先到者写的。
var siteProtocolProbeInFlight sync.Map

// probeSiteDefaultRouteType 在 API 直连站点还没探过协议时，用真实凭据探测上游实际
// 讲哪些协议。命中后立刻改写内存里的 siteRecord —— 本次同步后续选端点
// （siteModelFetchVersionSuffix）和分类模型都要用到它；落库交给 persistSyncSnapshot，
// 那里已经持有 site 行锁。
//
// 返回三样：兜底协议、探测顺手拿到的模型列表（调用方直接复用就省掉第二次请求）、
// 上游支持的全部协议。探不出来就返回空值，调用方按 ResolveDefaultRouteType 的兜底
// 继续走，不阻塞同步。
func probeSiteDefaultRouteType(
	ctx context.Context,
	siteRecord *model.Site,
	account *model.SiteAccount,
	token string,
) (model.SiteModelRouteType, []string, []model.SiteModelRouteType) {
	if siteRecord == nil || siteRecord.Platform != model.SitePlatformAPI {
		return "", nil, nil
	}
	// 兜底协议和支持集合都有值了才跳过。清空默认协议（UI 上选「自动探测」）会让这里
	// 重新跑；支持集合还空着的存量站点也会补探一次。
	if siteRecord.DefaultRouteType != "" && len(siteRecord.SupportedRouteTypes) > 0 {
		return "", nil, nil
	}

	// 同一站点被多个同步流程同时探测没有意义 —— 结果一样，白打上游三次协议各两
	// 个变体。后来者拿不到探测结果就走原来的 fetchModelsForSiteToken 兜底路径，
	// 功能不受影响。
	if _, busy := siteProtocolProbeInFlight.LoadOrStore(siteRecord.ID, struct{}{}); busy {
		return "", nil, nil
	}
	defer siteProtocolProbeInFlight.Delete(siteRecord.ID)

	baseURL := strings.TrimRight(strings.TrimSpace(siteRecord.BaseURL), "/")
	if baseURL == "" || strings.TrimSpace(token) == "" {
		return "", nil, nil
	}

	proxyMode, proxyConfigID := resolveSiteAccountProxy(siteRecord, account)
	probeChannel := model.Channel{
		BaseUrls:      []model.BaseUrl{{URL: baseURL, Delay: 0}},
		Keys:          []model.ChannelKey{{Enabled: true, ChannelKey: model.NormalizeSiteSyncTokenValueForPlatform(siteRecord.Platform, token)}},
		ProxyMode:     proxyMode,
		ProxyConfigID: proxyConfigID,
		CustomHeader:  siteRecord.CustomHeader,
	}

	probeCtx, cancel := context.WithTimeout(ctx, siteProtocolProbeTimeout)
	defer cancel()

	result, err := helper.ProbeModelProtocol(probeCtx, probeChannel)
	if err != nil || result.Primary == "" {
		log.Debugf("site protocol probe found nothing (site=%d): %v", siteRecord.ID, err)
		// 站点已经有兜底协议（存量站点只是来补支持集合）时，把集合落成"只有兜底
		// 协议"：兜底协议本来就在工作，其他协议这次没探到。不落的话每次同步都会
		// 重探一遍 —— 对一个 key 已经失效的站点就是每半小时白打六个请求。
		//
		// 新站点（兜底协议还空着）不落任何东西：它必须探出协议才能正确投影，下次
		// 同步继续试。
		if siteRecord.DefaultRouteType != "" {
			fallbackOnly := []model.SiteModelRouteType{siteRecord.DefaultRouteType}
			siteRecord.SupportedRouteTypes = fallbackOnly
			return "", nil, fallbackOnly
		}
		return "", nil, nil
	}

	supported := model.NormalizeSiteSupportedRouteTypes(result.Supported)
	// 用户手填过兜底协议就尊重他的选择，本次只补支持集合。
	if siteRecord.DefaultRouteType == "" {
		siteRecord.DefaultRouteType = result.Primary
	}
	siteRecord.SupportedRouteTypes = supported
	log.Infof("site protocol probe detected primary=%s supported=%v (site=%d, models=%d)",
		result.Primary, supported, siteRecord.ID, len(result.Models))
	return result.Primary, normalizeModelNames(result.Models), supported
}
