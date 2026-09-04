package model

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// 允许降级时刻意映射到 auto 而不是 both：both 是手动模式，
// EffectiveOpenAIProtocolCapability 会对两个协议都硬返回 supported，
// 运行时学习的 SQL 也只认 auto，一旦写成 both，上游真实不支持的协议
// 永远学不到、每次请求都要白试一次。auto 才能让探测结果决定一切。
func TestSiteModelProjectedOpenAIProtocolMode(t *testing.T) {
	tests := []struct {
		name                    string
		routeType               SiteModelRouteType
		disableProtocolFallback bool
		expectedMode            OpenAIProtocolMode
	}{
		{
			name:                    "OpenAI Chat 允许降级 → auto（交给运行时探测）",
			routeType:               SiteModelRouteTypeOpenAIChat,
			disableProtocolFallback: false,
			expectedMode:            OpenAIProtocolModeAuto,
		},
		{
			name:                    "OpenAI Chat 禁止降级 → chat_only",
			routeType:               SiteModelRouteTypeOpenAIChat,
			disableProtocolFallback: true,
			expectedMode:            OpenAIProtocolModeChatOnly,
		},
		{
			name:                    "OpenAI Response 允许降级 → auto（交给运行时探测）",
			routeType:               SiteModelRouteTypeOpenAIResponse,
			disableProtocolFallback: false,
			expectedMode:            OpenAIProtocolModeAuto,
		},
		{
			name:                    "OpenAI Response 禁止降级 → responses_only",
			routeType:               SiteModelRouteTypeOpenAIResponse,
			disableProtocolFallback: true,
			expectedMode:            OpenAIProtocolModeResponsesOnly,
		},
		{
			name:                    "Anthropic 忽略降级开关 → auto",
			routeType:               SiteModelRouteTypeAnthropic,
			disableProtocolFallback: false,
			expectedMode:            OpenAIProtocolModeAuto,
		},
		{
			name:                    "Anthropic 禁止降级也是 auto",
			routeType:               SiteModelRouteTypeAnthropic,
			disableProtocolFallback: true,
			expectedMode:            OpenAIProtocolModeAuto,
		},
		{
			name:                    "nil 安全",
			routeType:               "",
			disableProtocolFallback: false,
			expectedMode:            OpenAIProtocolModeAuto,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &SiteModel{
				RouteType:               tt.routeType,
				DisableProtocolFallback: tt.disableProtocolFallback,
			}
			got := m.ProjectedOpenAIProtocolMode()
			assert.Equal(t, tt.expectedMode, got)
		})
	}

	var nilModel *SiteModel
	assert.Equal(t, OpenAIProtocolModeAuto, nilModel.ProjectedOpenAIProtocolMode())
}
