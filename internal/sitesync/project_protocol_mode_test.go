package sitesync

import (
	"testing"

	"github.com/bestruirui/octopus/internal/model"
	"github.com/stretchr/testify/assert"
)

// aggregateProjectedProtocolMode 的取舍是"桶内分歧时取最宽松"：协议模式是
// 渠道级开关，而勾选是模型级的，宁可多给 relay 一次换协议的机会，也不要因为
// 个别模型的手动限制把整桶模型锁死在单协议上。
func TestAggregateProjectedProtocolMode(t *testing.T) {
	allow := model.SiteModel{DisableProtocolFallback: false}
	deny := model.SiteModel{DisableProtocolFallback: true}

	tests := []struct {
		name      string
		routeType model.SiteModelRouteType
		items     []model.SiteModel
		want      model.OpenAIProtocolMode
	}{
		{
			name:      "空桶 → auto",
			routeType: model.SiteModelRouteTypeOpenAIChat,
			items:     nil,
			want:      model.OpenAIProtocolModeAuto,
		},
		{
			name:      "全部允许降级 → auto",
			routeType: model.SiteModelRouteTypeOpenAIChat,
			items:     []model.SiteModel{allow, allow},
			want:      model.OpenAIProtocolModeAuto,
		},
		{
			name:      "Chat 桶全部禁止降级 → chat_only",
			routeType: model.SiteModelRouteTypeOpenAIChat,
			items:     []model.SiteModel{deny, deny},
			want:      model.OpenAIProtocolModeChatOnly,
		},
		{
			name:      "Responses 桶全部禁止降级 → responses_only",
			routeType: model.SiteModelRouteTypeOpenAIResponse,
			items:     []model.SiteModel{deny},
			want:      model.OpenAIProtocolModeResponsesOnly,
		},
		{
			name:      "桶内分歧取最宽松 → auto",
			routeType: model.SiteModelRouteTypeOpenAIChat,
			items:     []model.SiteModel{deny, allow, deny},
			want:      model.OpenAIProtocolModeAuto,
		},
		{
			name:      "非 OpenAI 文本路由忽略勾选 → auto",
			routeType: model.SiteModelRouteTypeAnthropic,
			items:     []model.SiteModel{deny, deny},
			want:      model.OpenAIProtocolModeAuto,
		},
		{
			name:      "Embedding 路由忽略勾选 → auto",
			routeType: model.SiteModelRouteTypeOpenAIEmbedding,
			items:     []model.SiteModel{deny},
			want:      model.OpenAIProtocolModeAuto,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, aggregateProjectedProtocolMode(tt.routeType, tt.items))
		})
	}
}
