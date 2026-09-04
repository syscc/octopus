package helper

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/bestruirui/octopus/internal/model"
)

func probeChannel(baseURL string) model.Channel {
	return model.Channel{
		BaseUrls: []model.BaseUrl{{URL: baseURL, Delay: 0}},
		Keys:     []model.ChannelKey{{Enabled: true, ChannelKey: "probe-key"}},
	}
}

// probeResult 把结果摊平成 (兜底协议, 模型, err)，这是大多数断言关心的三样。
// 需要检查 Supported 集合的用例直接调 ProbeModelProtocol。
func probeResult(ctx context.Context, channel model.Channel) (model.SiteModelRouteType, []string, error) {
	result, err := ProbeModelProtocol(ctx, channel)
	return result.Primary, result.Models, err
}

// pathRecorder 收集 httptest 收到的请求。探测是三协议并发的，handler 会被多个
// goroutine 同时调用 —— 不加锁不只是 go test -race 会红，记录本身也会丢。
type pathRecorder struct {
	mu     sync.Mutex
	paths  []string
	values map[string]string
}

func newPathRecorder() *pathRecorder {
	return &pathRecorder{values: make(map[string]string)}
}

func (r *pathRecorder) add(path string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.paths = append(r.paths, path)
}

func (r *pathRecorder) set(key, value string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.values[key] = value
}

func (r *pathRecorder) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.paths...)
}

func (r *pathRecorder) value(key string) string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.values[key]
}

// 这是真实站点上实测到的陷阱：tokenrhythm.studio / unlimitds.chat 这类中转站
// 不校验鉴权头的名字，x-api-key 也照样返回 OpenAI 格式的完整模型列表。只看
// "HTTP 200 + 有模型" 会把它们误判成 Anthropic，站点随后一个模型都拉不到。
func TestProbeModelProtocolIgnoresAuthHeaderAgnosticRelay(t *testing.T) {
	openAIList := `{"object":"list","data":[{"id":"gpt-5.6-sol","object":"model"},{"id":"claude-opus-4-8","object":"model"}]}`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/models" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		// 无论带的是 Authorization / x-api-key / x-goog-api-key 都一视同仁。
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(openAIList))
	}))
	defer server.Close()

	routeType, models, err := probeResult(context.Background(), probeChannel(server.URL))
	if err != nil {
		t.Fatalf("ProbeModelProtocol returned error: %v", err)
	}
	if routeType != model.SiteModelRouteTypeOpenAIChat {
		t.Fatalf("expected openai_chat for an auth-header-agnostic relay, got %q", routeType)
	}
	if len(models) != 2 {
		t.Fatalf("expected 2 models, got %d (%v)", len(models), models)
	}
}

func TestProbeModelProtocolDetectsAnthropicByEnvelope(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/models" || r.Header.Get("X-Api-Key") == "" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":[{"type":"model","id":"claude-opus-4-8","display_name":"Claude Opus 4.8"}],"first_id":"claude-opus-4-8","has_more":false,"last_id":"claude-opus-4-8"}`))
	}))
	defer server.Close()

	routeType, models, err := probeResult(context.Background(), probeChannel(server.URL))
	if err != nil {
		t.Fatalf("ProbeModelProtocol returned error: %v", err)
	}
	if routeType != model.SiteModelRouteTypeAnthropic {
		t.Fatalf("expected anthropic, got %q", routeType)
	}
	if len(models) != 1 || models[0] != "claude-opus-4-8" {
		t.Fatalf("unexpected models: %v", models)
	}
}

func TestProbeModelProtocolDetectsGemini(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1beta/models" || r.Header.Get("X-Goog-Api-Key") == "" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"models":[{"name":"models/gemini-3-pro"},{"name":"models/gemini-3-flash"}]}`))
	}))
	defer server.Close()

	routeType, models, err := probeResult(context.Background(), probeChannel(server.URL))
	if err != nil {
		t.Fatalf("ProbeModelProtocol returned error: %v", err)
	}
	if routeType != model.SiteModelRouteTypeGemini {
		t.Fatalf("expected gemini, got %q", routeType)
	}
	if len(models) != 2 || models[0] != "gemini-3-pro" {
		t.Fatalf("expected the models/ prefix stripped, got %v", models)
	}
}

// pipio 的实测形态：它真的实现了 Anthropic 的 /v1/models（响应带 has_more），
// 但列表是空的。有指纹没模型不算命中，站点应该落到 OpenAI。
func TestProbeModelProtocolIgnoresAnthropicEnvelopeWithoutModels(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/models" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if r.Header.Get("X-Api-Key") != "" {
			_, _ = w.Write([]byte(`{"data":[],"has_more":false}`))
			return
		}
		_, _ = w.Write([]byte(`{"object":"list","data":[{"id":"grok-4.6","object":"model"}]}`))
	}))
	defer server.Close()

	routeType, models, err := probeResult(context.Background(), probeChannel(server.URL))
	if err != nil {
		t.Fatalf("ProbeModelProtocol returned error: %v", err)
	}
	if routeType != model.SiteModelRouteTypeOpenAIChat {
		t.Fatalf("expected openai_chat when the Anthropic list is empty, got %q", routeType)
	}
	if len(models) != 1 || models[0] != "grok-4.6" {
		t.Fatalf("unexpected models: %v", models)
	}
}

// 站点地址已经带 /v1 时不能再叠一层，否则只会去打 /v1/v1/models。
func TestProbeModelProtocolDoesNotStackVersionSuffix(t *testing.T) {
	recorder := newPathRecorder()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		recorder.add(r.URL.Path)
		if r.URL.Path != "/v1/models" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"object":"list","data":[{"id":"gpt-5.6-sol","object":"model"}]}`))
	}))
	defer server.Close()

	routeType, _, err := probeResult(context.Background(), probeChannel(server.URL+"/v1"))
	if err != nil {
		t.Fatalf("ProbeModelProtocol returned error: %v", err)
	}
	if routeType != model.SiteModelRouteTypeOpenAIChat {
		t.Fatalf("expected openai_chat, got %q", routeType)
	}
	observedPaths := recorder.snapshot()
	for _, path := range observedPaths {
		if path == "/v1/v1/models" || path == "/v1/v1beta/models" {
			t.Fatalf("probe stacked a second version segment: %v", observedPaths)
		}
	}
}

// 顶层的 has_more/first_id/last_id 是 OpenAI list 分页约定，兼容层照抄很常见，
// 不能单凭它判 Anthropic —— 真 Anthropic 的每个 item 都带 type/display_name。
func TestProbeModelProtocolRejectsEnvelopeOnlyAnthropicFingerprint(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/models" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		// 抄了分页字段但 item 是 OpenAI 形状，且对任何鉴权头都照答。
		_, _ = w.Write([]byte(`{"object":"list","data":[{"id":"glm-5.3","object":"model"}],"has_more":false,"first_id":"glm-5.3","last_id":"glm-5.3"}`))
	}))
	defer server.Close()

	routeType, _, err := probeResult(context.Background(), probeChannel(server.URL))
	if err != nil {
		t.Fatalf("ProbeModelProtocol returned error: %v", err)
	}
	if routeType != model.SiteModelRouteTypeOpenAIChat {
		t.Fatalf("expected openai_chat when only the envelope looks Anthropic, got %q", routeType)
	}
}

// api.999555999.com 的真实形态，也是第一版判定被绕过的原因：这个中转站为了两边
// 兼容，把 OpenAI 的 object/created/owned_by 和 Anthropic 的 type/display_name
// 全塞进同一个 item，模型却全是 gpt-*。光看"有没有 Anthropic 字段"会判成
// Anthropic，整站渠道类型就错了。
func TestProbeModelProtocolRejectsDualVocabularyRelay(t *testing.T) {
	payload := `{"data":[` +
		`{"id":"gpt-5.5","object":"model","created":1776873600,"owned_by":"openai","type":"model","display_name":"GPT-5.5"},` +
		`{"id":"gpt-5.6-sol","object":"model","created":1780876800,"owned_by":"openai","type":"model","display_name":"GPT-5.6 Sol"},` +
		`{"id":"codex-auto-review","object":"model","created":1776902400,"owned_by":"openai","type":"model","display_name":"Codex Auto Review"}` +
		`],"object":"list","has_more":false}`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/models" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(payload))
	}))
	defer server.Close()

	routeType, models, err := probeResult(context.Background(), probeChannel(server.URL))
	if err != nil {
		t.Fatalf("ProbeModelProtocol returned error: %v", err)
	}
	if routeType != model.SiteModelRouteTypeOpenAIChat {
		t.Fatalf("expected openai_chat for a dual-vocabulary relay serving gpt models, got %q", routeType)
	}
	if len(models) != 3 {
		t.Fatalf("expected 3 models, got %v", models)
	}
}

// 一个站点即使真的说 Anthropic 协议，只要它对 OpenAI 请求返回同一份模型集，就说明
// 它不区分协议 —— 判 OpenAI 更安全，网关自己会做协议转换。
func TestProbeModelProtocolPrefersOpenAIWhenModelSetsMatch(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/models" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if r.Header.Get("X-Api-Key") != "" {
			// 干净的 Anthropic 形状 + claude 模型，单看这份响应会命中 Anthropic。
			_, _ = w.Write([]byte(`{"data":[{"type":"model","id":"claude-opus-4-8","display_name":"Claude Opus 4.8"}],"has_more":false}`))
			return
		}
		_, _ = w.Write([]byte(`{"object":"list","data":[{"id":"claude-opus-4-8","object":"model"}]}`))
	}))
	defer server.Close()

	routeType, _, err := probeResult(context.Background(), probeChannel(server.URL))
	if err != nil {
		t.Fatalf("ProbeModelProtocol returned error: %v", err)
	}
	if routeType != model.SiteModelRouteTypeOpenAIChat {
		t.Fatalf("expected openai_chat when both protocols return the same models, got %q", routeType)
	}
}

// 反向保险：Anthropic 形状 + claude 模型，但 OpenAI 探测拿不到东西 —— 这才是真
// Anthropic 站点，必须判 anthropic。
func TestProbeModelProtocolKeepsAnthropicWhenOpenAIRejected(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/models" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if r.Header.Get("X-Api-Key") == "" {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"type":"error","error":{"type":"authentication_error","message":"invalid x-api-key"}}`))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":[{"type":"model","id":"claude-opus-4-8","display_name":"Claude Opus 4.8"}],"has_more":false}`))
	}))
	defer server.Close()

	routeType, _, err := probeResult(context.Background(), probeChannel(server.URL))
	if err != nil {
		t.Fatalf("ProbeModelProtocol returned error: %v", err)
	}
	if routeType != model.SiteModelRouteTypeAnthropic {
		t.Fatalf("expected anthropic for a real Anthropic upstream, got %q", routeType)
	}
}

// Anthropic 形状但模型名是 gpt-* —— 名字自洽这一条是中转站唯一伪造不了的信号。
func TestProbeModelProtocolRejectsAnthropicShapeWithForeignModelNames(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/models" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if r.Header.Get("X-Api-Key") != "" {
			// 没有 object 字段、带 display_name，但模型是 glm —— 不是 Anthropic。
			_, _ = w.Write([]byte(`{"data":[{"type":"model","id":"glm-5.3","display_name":"GLM 5.3"}],"has_more":false}`))
			return
		}
		_, _ = w.Write([]byte(`{"object":"list","data":[{"id":"glm-5.3","object":"model"},{"id":"deepseek-v4-pro","object":"model"}]}`))
	}))
	defer server.Close()

	routeType, models, err := probeResult(context.Background(), probeChannel(server.URL))
	if err != nil {
		t.Fatalf("ProbeModelProtocol returned error: %v", err)
	}
	if routeType != model.SiteModelRouteTypeOpenAIChat {
		t.Fatalf("expected openai_chat when no model name is native to Anthropic, got %q", routeType)
	}
	if len(models) != 2 {
		t.Fatalf("expected the OpenAI list, got %v", models)
	}
}

// 这是方案的核心：一个同时讲 OpenAI 和 Anthropic 的中转站，兜底选 OpenAI，但
// Supported 必须把 anthropic 也记上 —— 否则它的 claude 模型只能让网关转一遍，
// 走不到上游原生的 /v1/messages。
func TestProbeModelProtocolRecordsEveryProtocolItSpeaks(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/models" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		// 两个协议都回同一个模型池（含 claude 和 gpt），这正是 seekai.cc 那类站点。
		_, _ = w.Write([]byte(`{"object":"list","data":[` +
			`{"id":"claude-opus-5","object":"model"},` +
			`{"id":"gpt-5-6","object":"model"},` +
			`{"id":"deepseek-v4-pro","object":"model"}` +
			`]}`))
	}))
	defer server.Close()

	result, err := ProbeModelProtocol(context.Background(), probeChannel(server.URL))
	if err != nil {
		t.Fatalf("ProbeModelProtocol returned error: %v", err)
	}
	if result.Primary != model.SiteModelRouteTypeOpenAIChat {
		t.Fatalf("expected openai_chat as the fallback, got %q", result.Primary)
	}
	supported := map[model.SiteModelRouteType]bool{}
	for _, value := range result.Supported {
		supported[value] = true
	}
	if !supported[model.SiteModelRouteTypeOpenAIChat] {
		t.Fatalf("expected openai_chat in Supported, got %v", result.Supported)
	}
	if !supported[model.SiteModelRouteTypeAnthropic] {
		t.Fatalf("expected anthropic in Supported (the relay serves claude models), got %v", result.Supported)
	}
	if supported[model.SiteModelRouteTypeGemini] {
		t.Fatalf("gemini must not be claimed without a gemini model, got %v", result.Supported)
	}
}

// 反面：站点对 x-api-key 也回 OpenAI 列表，但一个 claude 模型都没有 —— 不能凭
// "端点回了 200" 就记成支持 Anthropic，否则以后它加了 claude 模型就会拆出打不通
// 的渠道。
func TestProbeModelProtocolDoesNotClaimAnthropicWithoutClaudeModels(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/models" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"object":"list","data":[{"id":"glm-5.3","object":"model"},{"id":"deepseek-v4-pro","object":"model"}]}`))
	}))
	defer server.Close()

	result, err := ProbeModelProtocol(context.Background(), probeChannel(server.URL))
	if err != nil {
		t.Fatalf("ProbeModelProtocol returned error: %v", err)
	}
	for _, value := range result.Supported {
		if value == model.SiteModelRouteTypeAnthropic {
			t.Fatalf("anthropic must not be claimed without claude models, got %v", result.Supported)
		}
	}
}

// 站点地址填到哪一层决定探测在哪里试。裸地址和自定义路径都要试两种写法；已经填到
// 版本段的地址按字面用，绝不改写 —— 路径是有语义的，同一个 host 上 /zen/go/v1 和
// /zen/v1 的模型池就不一样。
func TestBuildVersionedBaseURLs(t *testing.T) {
	tests := []struct {
		name     string
		baseURL  string
		suffix   string
		expected []string
	}{
		{
			name:     "bare host gets the segment appended",
			baseURL:  "https://relay.example.com",
			suffix:   "/v1",
			expected: []string{"https://relay.example.com", "https://relay.example.com/v1"},
		},
		{
			name:     "bare host for gemini gets v1beta",
			baseURL:  "https://relay.example.com",
			suffix:   "/v1beta",
			expected: []string{"https://relay.example.com", "https://relay.example.com/v1beta"},
		},
		{
			name:     "an upstream mounted under its own path is tried both ways",
			baseURL:  "https://opencode.ai/zen/go",
			suffix:   "/v1",
			expected: []string{"https://opencode.ai/zen/go", "https://opencode.ai/zen/go/v1"},
		},
		{
			name:     "a pinned v1 address is taken literally",
			baseURL:  "https://relay.example.com/v1",
			suffix:   "/v1",
			expected: []string{"https://relay.example.com/v1"},
		},
		{
			name:     "a pinned v1 address is not rewritten to v1beta for gemini",
			baseURL:  "https://relay.example.com/v1",
			suffix:   "/v1beta",
			expected: []string{"https://relay.example.com/v1"},
		},
		{
			name:     "a pinned v1beta address is not rewritten to v1 either",
			baseURL:  "https://gemini.example.com/v1beta",
			suffix:   "/v1",
			expected: []string{"https://gemini.example.com/v1beta"},
		},
		{
			name:     "trailing slash is trimmed",
			baseURL:  "https://relay.example.com/v1/",
			suffix:   "/v1",
			expected: []string{"https://relay.example.com/v1"},
		},
		{
			name:     "empty base has nothing to try",
			baseURL:  "  ",
			suffix:   "/v1",
			expected: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual := BuildVersionedBaseURLs(tt.baseURL, tt.suffix)
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

// opencode zen 的实测形态：base 填 /zen/go，模型端点在 /zen/go/v1/models，而
// /zen/go/models 是 404。自定义路径的上游必须能被探到。
func TestProbeModelProtocolFindsUpstreamMountedUnderCustomPath(t *testing.T) {
	recorder := newPathRecorder()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		recorder.add(r.URL.Path)
		if r.URL.Path != "/zen/go/v1/models" {
			w.Header().Set("Content-Type", "text/html")
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte("<!DOCTYPE html><html><head><title>Not found</title></head></html>"))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"object":"list","data":[{"id":"minimax-m3","object":"model","owned_by":"minimax"}]}`))
	}))
	defer server.Close()

	routeType, models, err := probeResult(context.Background(), probeChannel(server.URL+"/zen/go"))
	if err != nil {
		t.Fatalf("ProbeModelProtocol returned error: %v", err)
	}
	if routeType != model.SiteModelRouteTypeOpenAIChat {
		t.Fatalf("expected openai_chat, got %q", routeType)
	}
	if len(models) != 1 || models[0] != "minimax-m3" {
		t.Fatalf("unexpected models: %v", models)
	}
	// 绝不能往上剥路径去试 /zen/v1/models —— 那是另一个模型池。
	probed := recorder.snapshot()
	for _, path := range probed {
		if path == "/zen/models" || path == "/zen/v1/models" {
			t.Fatalf("probe walked up the configured path: %v", probed)
		}
	}
}

// 填到 /v1 就只在 /v1 上试三种鉴权头，不去猜 /v1beta。
func TestProbeModelProtocolRespectsPinnedVersionPath(t *testing.T) {
	recorder := newPathRecorder()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		recorder.add(r.URL.Path)
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer server.Close()

	_, _, _ = probeResult(context.Background(), probeChannel(server.URL+"/v1"))

	probed := recorder.snapshot()
	for _, path := range probed {
		if path != "/v1/models" {
			t.Fatalf("a pinned /v1 address must only probe /v1/models, got %v", probed)
		}
	}
	if len(probed) != len(protocolProbes) {
		t.Fatalf("expected one request per protocol, got %v", probed)
	}
}

func TestProbeModelProtocolFailsWhenNothingMatches(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"error":{"message":"invalid key"}}`))
	}))
	defer server.Close()

	routeType, models, err := probeResult(context.Background(), probeChannel(server.URL))
	if err == nil {
		t.Fatalf("expected an error when every protocol is rejected, got routeType=%q models=%v", routeType, models)
	}
	if routeType != "" {
		t.Fatalf("expected an empty route type alongside the error, got %q", routeType)
	}
}

// 站点自定义 header 不能顶掉探测正在测试的那份凭据 —— 否则每个协议都会带上
// 同一个头，探测结果就没有意义了。
func TestProbeModelProtocolKeepsProbeCredentialOverCustomHeader(t *testing.T) {
	recorder := newPathRecorder()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/models" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if auth := r.Header.Get("Authorization"); auth != "" {
			recorder.set("authorization", auth)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"object":"list","data":[{"id":"gpt-5.6-sol","object":"model"}]}`))
	}))
	defer server.Close()

	channel := probeChannel(server.URL)
	channel.CustomHeader = []model.CustomHeader{
		{HeaderKey: "Authorization", HeaderValue: "Bearer hijacked"},
		{HeaderKey: "X-Trace", HeaderValue: "kept"},
	}

	if _, _, err := probeResult(context.Background(), channel); err != nil {
		t.Fatalf("ProbeModelProtocol returned error: %v", err)
	}
	if observed := recorder.value("authorization"); observed != "Bearer probe-key" {
		t.Fatalf("expected the probe credential to survive, got %q", observed)
	}
}
