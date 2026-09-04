package sitesync

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/bestruirui/octopus/internal/model"
)

// 探测的 URL 候选和稳态拉取的候选必须一致。这条曾经不成立：探测在 {base}/v1 上
// 认出 anthropic 写进协议，下一次同步不再探测、只试 {base}/models，直接 404。
func TestBuildModelFetchBaseURLsCoversDetectedProtocols(t *testing.T) {
	tests := []struct {
		name     string
		site     model.Site
		expected []string
	}{
		{
			name:     "openai api site tries /v1",
			site:     model.Site{Platform: model.SitePlatformAPI, BaseURL: "https://relay.example.com", DefaultRouteType: model.SiteModelRouteTypeOpenAIChat},
			expected: []string{"https://relay.example.com", "https://relay.example.com/v1"},
		},
		{
			name:     "anthropic api site also tries /v1",
			site:     model.Site{Platform: model.SitePlatformAPI, BaseURL: "https://api.anthropic.com", DefaultRouteType: model.SiteModelRouteTypeAnthropic},
			expected: []string{"https://api.anthropic.com", "https://api.anthropic.com/v1"},
		},
		{
			name:     "gemini api site tries /v1beta",
			site:     model.Site{Platform: model.SitePlatformAPI, BaseURL: "https://gemini.example.com", DefaultRouteType: model.SiteModelRouteTypeGemini},
			expected: []string{"https://gemini.example.com", "https://gemini.example.com/v1beta"},
		},
		{
			name:     "undetected api site falls back to /v1",
			site:     model.Site{Platform: model.SitePlatformAPI, BaseURL: "https://relay.example.com"},
			expected: []string{"https://relay.example.com", "https://relay.example.com/v1"},
		},
		{
			name:     "a pinned v1 address is taken literally",
			site:     model.Site{Platform: model.SitePlatformAPI, BaseURL: "https://relay.example.com/v1", DefaultRouteType: model.SiteModelRouteTypeOpenAIChat},
			expected: []string{"https://relay.example.com/v1"},
		},
		{
			name:     "a pinned v1beta gemini address is taken literally too",
			site:     model.Site{Platform: model.SitePlatformAPI, BaseURL: "https://gemini.example.com/v1beta", DefaultRouteType: model.SiteModelRouteTypeGemini},
			expected: []string{"https://gemini.example.com/v1beta"},
		},
		{
			name:     "a gemini site pinned to v1 is not rewritten",
			site:     model.Site{Platform: model.SitePlatformAPI, BaseURL: "https://gemini.example.com/v1", DefaultRouteType: model.SiteModelRouteTypeGemini},
			expected: []string{"https://gemini.example.com/v1"},
		},
		{
			name:     "an upstream under its own path is tried both ways",
			site:     model.Site{Platform: model.SitePlatformAPI, BaseURL: "https://opencode.ai/zen/go", DefaultRouteType: model.SiteModelRouteTypeOpenAIChat},
			expected: []string{"https://opencode.ai/zen/go", "https://opencode.ai/zen/go/v1"},
		},
		{
			name:     "management platform keeps the /v1 convention",
			site:     model.Site{Platform: model.SitePlatformNewAPI, BaseURL: "https://newapi.example.com"},
			expected: []string{"https://newapi.example.com", "https://newapi.example.com/v1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual := buildModelFetchBaseURLs(&tt.site)
			if len(actual) != len(tt.expected) {
				t.Fatalf("expected %v, got %v", tt.expected, actual)
			}
			for i := range tt.expected {
				if actual[i] != tt.expected[i] {
					t.Fatalf("expected %v, got %v", tt.expected, actual)
				}
			}
		})
	}
}

func TestProbeSiteDefaultRouteTypeSkipsWhenNotEligible(t *testing.T) {
	called := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	tests := []struct {
		name string
		site model.Site
	}{
		{
			name: "management platform is never probed",
			site: model.Site{ID: 1, Platform: model.SitePlatformNewAPI, BaseURL: server.URL},
		},
		{
			name: "empty base url has nothing to probe",
			site: model.Site{ID: 3, Platform: model.SitePlatformAPI},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			called = false
			site := tt.site
			routeType, models, supported := probeSiteDefaultRouteType(context.Background(), &site, &model.SiteAccount{}, "probe-key")
			if routeType != "" || models != nil || supported != nil {
				t.Fatalf("expected no detection, got routeType=%q models=%v supported=%v", routeType, models, supported)
			}
			if called {
				t.Fatalf("upstream should not have been contacted")
			}
			if site.DefaultRouteType != tt.site.DefaultRouteType {
				t.Fatalf("site protocol was mutated: %q -> %q", tt.site.DefaultRouteType, site.DefaultRouteType)
			}
		})
	}
}

func TestProbeSiteDefaultRouteTypeDetectsAnthropicAndMutatesSite(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/models" || r.Header.Get("X-Api-Key") == "" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":[{"type":"model","id":"claude-opus-4-8","display_name":"Claude Opus 4.8"}],"has_more":false}`))
	}))
	defer server.Close()

	site := model.Site{ID: 7, Platform: model.SitePlatformAPI, BaseURL: server.URL}
	routeType, models, supported := probeSiteDefaultRouteType(context.Background(), &site, &model.SiteAccount{}, "probe-key")

	if routeType != model.SiteModelRouteTypeAnthropic {
		t.Fatalf("expected anthropic, got %q", routeType)
	}
	if len(models) != 1 || models[0] != "claude-opus-4-8" {
		t.Fatalf("unexpected models: %v", models)
	}
	if len(supported) != 1 || supported[0] != model.SiteModelRouteTypeAnthropic {
		t.Fatalf("expected anthropic in the supported set, got %v", supported)
	}
	// 内存里必须当场生效：本次同步后续的端点选择和模型分类都读这两个字段。
	if site.DefaultRouteType != model.SiteModelRouteTypeAnthropic {
		t.Fatalf("expected the in-memory site to be updated, got %q", site.DefaultRouteType)
	}
	if len(site.SupportedRouteTypes) != 1 {
		t.Fatalf("expected the in-memory supported set to be updated, got %v", site.SupportedRouteTypes)
	}
}

func TestProbeSiteDefaultRouteTypeLeavesSiteAloneWhenProbeFails(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer server.Close()

	site := model.Site{ID: 9, Platform: model.SitePlatformAPI, BaseURL: server.URL}
	routeType, models, supported := probeSiteDefaultRouteType(context.Background(), &site, &model.SiteAccount{}, "probe-key")

	if routeType != "" || models != nil || supported != nil {
		t.Fatalf("expected no detection, got routeType=%q models=%v supported=%v", routeType, models, supported)
	}
	if site.DefaultRouteType != "" {
		t.Fatalf("a failed probe must not pin a protocol, got %q", site.DefaultRouteType)
	}
	if len(site.SupportedRouteTypes) != 0 {
		t.Fatalf("a failed probe must not claim any protocol, got %v", site.SupportedRouteTypes)
	}
}

// 站点已经探过（兜底协议和支持集合都有值）就不再打上游 —— 这是"存量零回归"的
// 保证，也避免每次同步都白跑三次探测。
func TestProbeSiteDefaultRouteTypeSkipsWhenAlreadyProbed(t *testing.T) {
	called := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	site := model.Site{
		ID:                  11,
		Platform:            model.SitePlatformAPI,
		BaseURL:             server.URL,
		DefaultRouteType:    model.SiteModelRouteTypeOpenAIChat,
		SupportedRouteTypes: []model.SiteModelRouteType{model.SiteModelRouteTypeOpenAIChat},
	}
	routeType, _, supported := probeSiteDefaultRouteType(context.Background(), &site, &model.SiteAccount{}, "probe-key")

	if routeType != "" || supported != nil {
		t.Fatalf("expected no re-probe, got routeType=%q supported=%v", routeType, supported)
	}
	if called {
		t.Fatalf("upstream should not have been contacted for an already-probed site")
	}
}

// 用户手填过兜底协议、但支持集合还空着（存量站点）时补探一次：只写支持集合，
// 不能把用户选的兜底协议改掉。
func TestProbeSiteDefaultRouteTypeBackfillsSupportedWithoutTouchingManualDefault(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/models" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"object":"list","data":[{"id":"claude-opus-5","object":"model"},{"id":"gpt-5-6","object":"model"}]}`))
	}))
	defer server.Close()

	site := model.Site{
		ID:               13,
		Platform:         model.SitePlatformAPI,
		BaseURL:          server.URL,
		DefaultRouteType: model.SiteModelRouteTypeAnthropic, // 用户手动指定的
	}
	_, _, supported := probeSiteDefaultRouteType(context.Background(), &site, &model.SiteAccount{}, "probe-key")

	if site.DefaultRouteType != model.SiteModelRouteTypeAnthropic {
		t.Fatalf("a manually chosen fallback must survive the backfill, got %q", site.DefaultRouteType)
	}
	if len(supported) == 0 {
		t.Fatalf("expected the supported set to be filled in")
	}
}

// 分桶门槛：探测没确认的协议不能独立成渠道，否则会拿着错的 base URL 打不通。
// hub.linux.do 就踩过这个 —— 站点只探到 openai_chat + anthropic，95 个 gemini
// 模型却被拆成一个 Gemini 渠道，base 是 /v1（Gemini 原生要 /v1beta）。
func TestPartitionSiteModelsByRouteTypeGatesUnconfirmedProtocols(t *testing.T) {
	items := []model.SiteModel{
		{ModelName: "gpt-5-6", RouteType: model.SiteModelRouteTypeOpenAIChat},
		{ModelName: "claude-opus-5", RouteType: model.SiteModelRouteTypeAnthropic},
		{ModelName: "gemini-3-pro", RouteType: model.SiteModelRouteTypeGemini},
		{ModelName: "text-embedding-3-large", RouteType: model.SiteModelRouteTypeOpenAIEmbedding},
	}

	tests := []struct {
		name  string
		site  model.Site
		items []model.SiteModel
		// want 是每个桶预期的模型数
		want map[model.SiteModelRouteType]int
	}{
		{
			name: "api site keeps only probed protocols as their own bucket",
			site: model.Site{
				Platform:            model.SitePlatformAPI,
				DefaultRouteType:    model.SiteModelRouteTypeOpenAIChat,
				SupportedRouteTypes: []model.SiteModelRouteType{model.SiteModelRouteTypeOpenAIChat, model.SiteModelRouteTypeAnthropic},
			},
			items: items,
			want: map[model.SiteModelRouteType]int{
				// gemini 未探到 → 收回兜底桶，和 gpt-5-6 同桶
				model.SiteModelRouteTypeOpenAIChat:      2,
				model.SiteModelRouteTypeAnthropic:       1,
				model.SiteModelRouteTypeOpenAIEmbedding: 1,
			},
		},
		{
			name: "management platform metadata is authoritative and never gated",
			site: model.Site{
				Platform:         model.SitePlatformNewAPI,
				DefaultRouteType: model.SiteModelRouteTypeOpenAIChat,
			},
			items: items,
			want: map[model.SiteModelRouteType]int{
				model.SiteModelRouteTypeOpenAIChat:      1,
				model.SiteModelRouteTypeAnthropic:       1,
				model.SiteModelRouteTypeGemini:          1,
				model.SiteModelRouteTypeOpenAIEmbedding: 1,
			},
		},
		{
			name: "a per-route base URL override outranks the probe",
			site: model.Site{
				Platform:         model.SitePlatformAPI,
				DefaultRouteType: model.SiteModelRouteTypeOpenAIChat,
				RouteBaseURLs: []model.SiteRouteBaseURL{
					{RouteType: model.SiteModelRouteTypeGemini, BaseURL: "https://relay.example.com/gemini/v1beta"},
				},
			},
			items: items,
			want: map[model.SiteModelRouteType]int{
				// gemini 有覆盖 → 独立；anthropic 没探到也没覆盖 → 收回兜底桶
				model.SiteModelRouteTypeOpenAIChat:      2,
				model.SiteModelRouteTypeGemini:          1,
				model.SiteModelRouteTypeOpenAIEmbedding: 1,
			},
		},
		{
			name: "a manual override is always honoured",
			site: model.Site{
				Platform:         model.SitePlatformAPI,
				DefaultRouteType: model.SiteModelRouteTypeOpenAIChat,
			},
			items: []model.SiteModel{
				{ModelName: "gpt-5-6", RouteType: model.SiteModelRouteTypeOpenAIChat},
				{ModelName: "gemini-3-pro", RouteType: model.SiteModelRouteTypeGemini, ManualOverride: true},
			},
			want: map[model.SiteModelRouteType]int{
				model.SiteModelRouteTypeOpenAIChat: 1,
				model.SiteModelRouteTypeGemini:     1,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			site := tt.site
			buckets := partitionSiteModelsByRouteType(tt.items, true, &site)
			if len(buckets) != len(tt.want) {
				t.Fatalf("expected %d buckets %v, got %d %v", len(tt.want), tt.want, len(buckets), bucketSizes(buckets))
			}
			for routeType, count := range tt.want {
				if len(buckets[routeType]) != count {
					t.Fatalf("bucket %s: expected %d models, got %v", routeType, count, bucketSizes(buckets))
				}
			}
		})
	}
}

func bucketSizes(buckets map[model.SiteModelRouteType][]model.SiteModel) map[model.SiteModelRouteType]int {
	sizes := make(map[model.SiteModelRouteType]int, len(buckets))
	for routeType, items := range buckets {
		sizes[routeType] = len(items)
	}
	return sizes
}
func TestShouldSplitForAccountHonoursProbedProtocolSupport(t *testing.T) {
	claudeAndGPT := []model.SiteModel{
		{ModelName: "claude-opus-5", RouteType: model.SiteModelRouteTypeAnthropic},
		{ModelName: "gpt-5-6", RouteType: model.SiteModelRouteTypeOpenAIChat},
	}

	tests := []struct {
		name     string
		site     model.Site
		models   []model.SiteModel
		expected bool
	}{
		{
			name: "probed multi-protocol relay splits on inferred anthropic",
			site: model.Site{
				Platform:            model.SitePlatformAPI,
				DefaultRouteType:    model.SiteModelRouteTypeOpenAIChat,
				SupportedRouteTypes: []model.SiteModelRouteType{model.SiteModelRouteTypeOpenAIChat, model.SiteModelRouteTypeAnthropic},
			},
			models:   claudeAndGPT,
			expected: true,
		},
		{
			name: "openai-only relay keeps claude models in the fallback bucket",
			site: model.Site{
				Platform:            model.SitePlatformAPI,
				DefaultRouteType:    model.SiteModelRouteTypeOpenAIChat,
				SupportedRouteTypes: []model.SiteModelRouteType{model.SiteModelRouteTypeOpenAIChat},
			},
			models:   claudeAndGPT,
			expected: false,
		},
		{
			name: "unprobed site behaves like before",
			site: model.Site{
				Platform:         model.SitePlatformAPI,
				DefaultRouteType: model.SiteModelRouteTypeOpenAIChat,
			},
			models:   claudeAndGPT,
			expected: false,
		},
		{
			name: "manual override still splits without any probe result",
			site: model.Site{
				Platform:         model.SitePlatformAPI,
				DefaultRouteType: model.SiteModelRouteTypeOpenAIChat,
			},
			models: []model.SiteModel{
				{ModelName: "grok-4.6", RouteType: model.SiteModelRouteTypeOpenAIResponse, ManualOverride: true},
			},
			expected: true,
		},
		{
			name: "disabled models never trigger a split",
			site: model.Site{
				Platform:            model.SitePlatformAPI,
				DefaultRouteType:    model.SiteModelRouteTypeOpenAIChat,
				SupportedRouteTypes: []model.SiteModelRouteType{model.SiteModelRouteTypeOpenAIChat, model.SiteModelRouteTypeAnthropic},
			},
			models: []model.SiteModel{
				{ModelName: "claude-opus-5", RouteType: model.SiteModelRouteTypeAnthropic, Disabled: true},
				{ModelName: "gpt-5-6", RouteType: model.SiteModelRouteTypeOpenAIChat},
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			site := tt.site
			account := model.SiteAccount{Models: tt.models}
			if actual := shouldSplitForAccount(&account, &site); actual != tt.expected {
				t.Fatalf("expected split=%v, got %v", tt.expected, actual)
			}
		})
	}
}
