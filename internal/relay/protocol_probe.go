package relay

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/bestruirui/octopus/internal/helper"
	dbmodel "github.com/bestruirui/octopus/internal/model"
	"github.com/bestruirui/octopus/internal/op"
	transformerModel "github.com/bestruirui/octopus/internal/transformer/model"
	"github.com/bestruirui/octopus/internal/transformer/outbound"
	"github.com/bestruirui/octopus/internal/utils/log"
	"github.com/bestruirui/octopus/internal/utils/xstrings"
	"golang.org/x/sync/singleflight"
)

// OpenAI 协议主动探测。
//
// 探测入口通过 channel ID 从 op 权威缓存加载配置，对每个配置的 Base URL
// 分别请求 /chat/completions 与 /responses，并按 auto 模式持久化结论。
// 探测完全独立于普通 relay 转发：不经过 balancer / 熔断 / outlier，不写
// StatsChannel、ChannelKey 运行时状态或 relay metrics。
const (
	// protocolProbeTotalTimeout 约束整个探测过程（所有 Base URL × 两个 endpoint）。
	protocolProbeTotalTimeout = 30 * time.Second
	// protocolProbeRequestTimeout 约束单次上游请求。
	protocolProbeRequestTimeout = 10 * time.Second
	// protocolProbeMaxBodyBytes 限制错误响应的读取长度：响应体只用于错误
	// 分类，不进入日志或探测结果，避免记录完整上游响应。
	protocolProbeMaxBodyBytes = 8 * 1024
	// protocolProbeMaxRequestBytes 限制探测请求体的内存与体积：覆盖 ParamOverride
	// 配置本身与合并/裁剪后的最终请求体，防止超大配置把能力探测变成内存放大。
	protocolProbeMaxRequestBytes = 64 * 1024
	// protocolProbePersistTimeout 约束探测结论落库。
	protocolProbePersistTimeout = 5 * time.Second
)

// protocolProbeInflight merges concurrent manual and automatic probes for the
// same channel into one in-flight run. Callers only read the shared report.
var protocolProbeInflight singleflight.Group

// ProtocolProbeOutcome 是单个 endpoint 的探测结论。
type ProtocolProbeOutcome string

const (
	ProtocolProbeSupported   ProtocolProbeOutcome = "supported"
	ProtocolProbeUnsupported ProtocolProbeOutcome = "unsupported"
	ProtocolProbeUnknown     ProtocolProbeOutcome = "unknown"
)

// ProtocolProbeAttempt is internal evidence for one base URL. It is deliberately
// excluded from JSON responses: a channel Base URL may contain private routing
// information, and the UI only needs the aggregate capability state.
type ProtocolProbeAttempt struct {
	BaseURL    string               `json:"-"`
	StatusCode int                  `json:"-"`
	Outcome    ProtocolProbeOutcome `json:"-"`
	Reason     string               `json:"-"`
}

// ProtocolProbeEndpointResult is the internal aggregate for one protocol.
type ProtocolProbeEndpointResult struct {
	Protocol outbound.OutboundType  `json:"-"`
	Outcome  ProtocolProbeOutcome   `json:"-"`
	Attempts []ProtocolProbeAttempt `json:"-"`
}

// ProtocolProbeReport is kept internal to the relay package. Use
// BuildProtocolProbeAPIResponse before returning a report from an HTTP handler.
type ProtocolProbeReport struct {
	Chat       ProtocolProbeEndpointResult `json:"-"`
	Responses  ProtocolProbeEndpointResult `json:"-"`
	Skipped    bool                        `json:"-"`
	SkipReason string                      `json:"-"`
	Error      string                      `json:"-"`

	channelID                  int
	mode                       dbmodel.OpenAIProtocolMode
	currentChatCapability      dbmodel.OpenAIProtocolCapability
	currentResponsesCapability dbmodel.OpenAIProtocolCapability
}

// ProtocolProbeAPIEndpointResult is the safe, stable management API shape.
// It contains no Base URL, key, request body, or upstream response text.
type ProtocolProbeAPIEndpointResult struct {
	Endpoint   string                           `json:"endpoint"`
	Current    dbmodel.OpenAIProtocolCapability `json:"current"`
	Observed   dbmodel.OpenAIProtocolCapability `json:"observed"`
	Capability dbmodel.OpenAIProtocolCapability `json:"capability"`
	Outcome    string                           `json:"outcome"`
	Status     *int                             `json:"status,omitempty"`
	Message    string                           `json:"message,omitempty"`
}

// ProtocolProbeAPIResponse is returned by the explicit probe endpoint.
type ProtocolProbeAPIResponse struct {
	ChannelID int                              `json:"channel_id"`
	Mode      dbmodel.OpenAIProtocolMode       `json:"mode"`
	Chat      dbmodel.OpenAIProtocolCapability `json:"chat"`
	Responses dbmodel.OpenAIProtocolCapability `json:"responses"`
	Skipped   bool                             `json:"skipped"`
	Endpoints []ProtocolProbeAPIEndpointResult `json:"endpoints"`
}

// NewProtocolProbeReport creates an internal report and snapshots the state that
// was visible before probing. The snapshot lets the API distinguish an unknown
// observation from a pre-existing capability that was intentionally retained.
func NewProtocolProbeReport(channelID int, channel *dbmodel.Channel) *ProtocolProbeReport {
	report := &ProtocolProbeReport{
		Chat:                       ProtocolProbeEndpointResult{Protocol: outbound.OutboundTypeOpenAIChat, Outcome: ProtocolProbeUnknown},
		Responses:                  ProtocolProbeEndpointResult{Protocol: outbound.OutboundTypeOpenAIResponse, Outcome: ProtocolProbeUnknown},
		channelID:                  channelID,
		mode:                       dbmodel.OpenAIProtocolModeAuto,
		currentChatCapability:      dbmodel.OpenAIProtocolCapabilityUnknown,
		currentResponsesCapability: dbmodel.OpenAIProtocolCapabilityUnknown,
	}
	if channel != nil {
		report.mode = channel.OpenAIProtocolMode.Normalize()
		report.currentChatCapability = channel.OpenAIChatCapability.Normalize()
		report.currentResponsesCapability = channel.OpenAIResponsesCapability.Normalize()
	}
	return report
}

// BuildProtocolProbeAPIResponse converts internal probe evidence into the
// stable, sanitized management response. channel should be the authoritative
// post-persistence channel when available.
func BuildProtocolProbeAPIResponse(channelID int, report *ProtocolProbeReport, channel *dbmodel.Channel) ProtocolProbeAPIResponse {
	response := ProtocolProbeAPIResponse{
		ChannelID: channelID,
		Mode:      dbmodel.OpenAIProtocolModeAuto,
		Chat:      dbmodel.OpenAIProtocolCapabilityUnknown,
		Responses: dbmodel.OpenAIProtocolCapabilityUnknown,
		Skipped:   report != nil && report.Skipped,
		Endpoints: make([]ProtocolProbeAPIEndpointResult, 0, 2),
	}
	if report == nil {
		return response
	}
	if report.channelID > 0 && channelID <= 0 {
		response.ChannelID = report.channelID
	}

	response.Mode = report.mode.Normalize()
	response.Chat = report.currentChatCapability.Normalize()
	response.Responses = report.currentResponsesCapability.Normalize()
	if channel != nil {
		response.Mode = channel.OpenAIProtocolMode.Normalize()
		response.Chat = effectiveOpenAIProtocolCapability(channel, outbound.OutboundTypeOpenAIChat)
		response.Responses = effectiveOpenAIProtocolCapability(channel, outbound.OutboundTypeOpenAIResponse)
	}

	response.Endpoints = append(response.Endpoints,
		buildProtocolProbeAPIEndpoint("chat", report.Chat, report.currentChatCapability, response.Chat, report.Skipped),
		buildProtocolProbeAPIEndpoint("responses", report.Responses, report.currentResponsesCapability, response.Responses, report.Skipped),
	)
	return response
}

func buildProtocolProbeAPIEndpoint(name string, result ProtocolProbeEndpointResult, current, capability dbmodel.OpenAIProtocolCapability, skipped bool) ProtocolProbeAPIEndpointResult {
	if current == "" {
		current = dbmodel.OpenAIProtocolCapabilityUnknown
	}
	if capability == "" {
		capability = dbmodel.OpenAIProtocolCapabilityUnknown
	}
	outcome := "failed"
	message := "no conclusive endpoint evidence"
	if result.Outcome == ProtocolProbeSupported || result.Outcome == ProtocolProbeUnsupported {
		// Fixed provider rules can avoid network I/O while still producing a
		// conclusive capability. Preserve that endpoint result even though the
		// report-level action remains skipped.
		outcome = "probed"
		message = ""
	} else if skipped {
		outcome = "skipped"
		message = "probe skipped"
	}
	return ProtocolProbeAPIEndpointResult{
		Endpoint:   name,
		Current:    current.Normalize(),
		Observed:   probeOutcomeCapability(result.Outcome),
		Capability: capability.Normalize(),
		Outcome:    outcome,
		Status:     protocolProbeRepresentativeStatus(result),
		Message:    message,
	}
}

func probeOutcomeCapability(outcome ProtocolProbeOutcome) dbmodel.OpenAIProtocolCapability {
	switch outcome {
	case ProtocolProbeSupported:
		return dbmodel.OpenAIProtocolCapabilitySupported
	case ProtocolProbeUnsupported:
		return dbmodel.OpenAIProtocolCapabilityUnsupported
	default:
		return dbmodel.OpenAIProtocolCapabilityUnknown
	}
}

func protocolProbeRepresentativeStatus(result ProtocolProbeEndpointResult) *int {
	for _, attempt := range result.Attempts {
		if attempt.StatusCode >= 200 && attempt.StatusCode < 300 && result.Outcome == ProtocolProbeSupported {
			status := attempt.StatusCode
			return &status
		}
	}
	for _, attempt := range result.Attempts {
		if attempt.StatusCode > 0 {
			status := attempt.StatusCode
			return &status
		}
	}
	return nil
}

// classifyOpenAIProtocolProbeResponse 是纯分类函数：把一次 endpoint 请求的
// 结果归类为 supported / unsupported / unknown。
//
// 规则与 relay 运行时协议学习（shouldTryProtocolFallback）保持一致：
//   - HTTP 2xx                      -> supported；
//   - 明确 endpoint/path 不兼容证据  -> unsupported（route/endpoint/method
//     标记、"Invalid URL (POST /v1/...)"，常见于 404/405/501）；
//   - 其余一律 unknown：401/403、429、model-not-found、quota、普通结构化
//     invalid_request、无标记的 5xx、网络错误。
//
// 普通资源型 404 / not_found_error 没有 endpoint/path 证据，必须保持
// unknown，不能把所有 404 都当成 unsupported。
func classifyOpenAIProtocolProbeResponse(statusCode int, body string, transportErr error) (ProtocolProbeOutcome, string) {
	// A successful HTTP status is conclusive endpoint evidence even if reading
	// the response body fails after the headers were received.
	if statusCode >= 200 && statusCode < 300 {
		return ProtocolProbeSupported, fmt.Sprintf("http %d", statusCode)
	}
	if transportErr != nil {
		if shouldTryProtocolFallback(0, transportErr) {
			return ProtocolProbeUnsupported, "transport error carries endpoint mismatch evidence"
		}
		return ProtocolProbeUnknown, "transport error"
	}

	// Reuse the runtime classifier without placing provider-controlled body text
	// in the error string. The typed error keeps bounded private evidence solely
	// for classification.
	synthetic := newUpstreamHTTPError(statusCode, []byte(body))
	if shouldTryProtocolFallback(statusCode, synthetic) {
		if isOpenAIEndpointRoutingError(statusCode, synthetic) {
			return ProtocolProbeUnsupported, "upstream reported invalid endpoint url"
		}
		return ProtocolProbeUnsupported, fmt.Sprintf("http %d endpoint unavailable", statusCode)
	}

	// Runtime fallback treats an unstructured route status from a non-native
	// endpoint as endpoint evidence. A direct probe has the same evidence, so
	// classify it identically. Structured OpenAI errors remain unknown unless
	// they explicitly describe routing; plain auth/quota/model markers also
	// remain unknown.
	switch statusCode {
	case http.StatusNotFound, http.StatusMethodNotAllowed, http.StatusNotImplemented:
		_, structured := parseOpenAIErrorDetail(body)
		if !structured && !hasNonEndpointErrorMarker(strings.ToLower(strings.TrimSpace(body))) {
			return ProtocolProbeUnsupported, fmt.Sprintf("http %d endpoint unavailable", statusCode)
		}
	}
	return ProtocolProbeUnknown, fmt.Sprintf("http %d", statusCode)
}

// aggregateProtocolProbeOutcomes 聚合一个 endpoint 在多个 Base URL 上的结论：
// 任一 supported 即 supported（一个可用 URL 不被另一个失败 URL 覆盖）；
// 存在 unknown 时保持 unknown；全部明确 unsupported 才 unsupported。
func aggregateProtocolProbeOutcomes(outcomes []ProtocolProbeOutcome) ProtocolProbeOutcome {
	if len(outcomes) == 0 {
		return ProtocolProbeUnknown
	}
	sawSupported := false
	sawUnknown := false
	for _, outcome := range outcomes {
		switch outcome {
		case ProtocolProbeSupported:
			sawSupported = true
		case ProtocolProbeUnknown:
			sawUnknown = true
		}
	}
	if sawSupported {
		return ProtocolProbeSupported
	}
	if sawUnknown {
		return ProtocolProbeUnknown
	}
	return ProtocolProbeUnsupported
}

// buildOpenAIProtocolProbeInternalRequest 构造低成本有界探测请求：
// stream=false、单条 "ping" 消息、极小 token 上限。
func buildOpenAIProtocolProbeInternalRequest(protocol outbound.OutboundType, modelName string) *transformerModel.InternalLLMRequest {
	stream := false
	ping := "ping"
	one := int64(1)
	// OpenAI Responses API 要求 max_output_tokens >= 16。
	sixteen := int64(16)
	if protocol == outbound.OutboundTypeOpenAIResponse {
		return &transformerModel.InternalLLMRequest{
			Model:               modelName,
			RawAPIFormat:        transformerModel.APIFormatOpenAIResponse,
			Messages:            []transformerModel.Message{{Role: "user", Content: transformerModel.MessageContent{Content: &ping}}},
			Stream:              &stream,
			MaxCompletionTokens: &sixteen,
		}
	}
	return &transformerModel.InternalLLMRequest{
		Model:        modelName,
		RawAPIFormat: transformerModel.APIFormatOpenAIChatCompletion,
		Messages:     []transformerModel.Message{{Role: "user", Content: transformerModel.MessageContent{Content: &ping}}},
		Stream:       &stream,
		MaxTokens:    &one,
	}
}

// buildOpenAIProtocolProbeRequest 复用 outbound adapter 构建 endpoint 请求
// （URL 拼接与 channel key 语义和普通转发一致），随后应用安全的 custom
// headers；凭据类自定义头被拒绝，选定 channel key 始终保持权威。
func buildOpenAIProtocolProbeRequest(
	ctx context.Context,
	protocol outbound.OutboundType,
	baseURL string,
	apiKey string,
	modelName string,
	customHeaders []dbmodel.CustomHeader,
	paramOverride *string,
) (*http.Request, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	adapter := outbound.Get(protocol)
	if adapter == nil {
		return nil, fmt.Errorf("unsupported outbound type: %d", protocol)
	}
	// Bound both the configured override and the adapter-generated body before
	// invoking the shared helper. ApplyParamOverride reads its body eagerly.
	if paramOverride != nil && len(*paramOverride) > protocolProbeMaxRequestBytes {
		return nil, fmt.Errorf("probe param override exceeds bounded size")
	}
	internalRequest := buildOpenAIProtocolProbeInternalRequest(protocol, modelName)
	request, err := adapter.TransformRequest(ctx, internalRequest, baseURL, apiKey)
	if err != nil {
		return nil, err
	}
	for _, header := range customHeaders {
		if isBlockedChannelHeader(header.HeaderKey) {
			continue
		}
		request.Header.Set(header.HeaderKey, header.HeaderValue)
	}
	applySelectedCredentialHeader(request.Header, protocol, apiKey)
	if request.Header.Get("User-Agent") == "" {
		// 与转发路径一致，避免 Go 默认 User-Agent 泄露到上游。
		request.Header.Set("User-Agent", "")
	}
	if err := boundProbeRequestBody(request); err != nil {
		return nil, err
	}
	if err := helper.ApplyParamOverride(request, paramOverride); err != nil {
		return nil, err
	}
	if err := sanitizeOpenAIProtocolProbeRequest(request, protocol, modelName); err != nil {
		return nil, err
	}
	return request, nil
}

// boundProbeRequestBody makes the body size bound effective before any shared
// helper can read it without a limit. It also restores a rewindable body for
// the subsequent override merge.
func boundProbeRequestBody(request *http.Request) error {
	if request == nil || request.Body == nil {
		return fmt.Errorf("probe request body is empty")
	}
	if request.ContentLength > protocolProbeMaxRequestBytes {
		_ = request.Body.Close()
		return fmt.Errorf("probe request body exceeds bounded size")
	}
	body, err := io.ReadAll(io.LimitReader(request.Body, protocolProbeMaxRequestBytes+1))
	_ = request.Body.Close()
	if err != nil {
		return fmt.Errorf("failed to read probe request body")
	}
	if len(body) > protocolProbeMaxRequestBytes {
		return fmt.Errorf("probe request body exceeds bounded size")
	}
	request.Body = io.NopCloser(bytes.NewReader(body))
	request.ContentLength = int64(len(body))
	request.GetBody = func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(body)), nil
	}
	return nil
}

// sanitizeOpenAIProtocolProbeRequest applies the non-negotiable probe bounds
// after ParamOverride. A channel may legitimately use ParamOverride for benign
// provider knobs, but it must not be able to turn a capability check into a
// different model, a streaming request, a tool/background job, or an
// unbounded-output request.
func sanitizeOpenAIProtocolProbeRequest(request *http.Request, protocol outbound.OutboundType, modelName string) error {
	if request == nil || request.Body == nil {
		return fmt.Errorf("probe request body is empty")
	}
	// The body at this point may include merged ParamOverride content; read it
	// under a hard cap so the merge result cannot turn into unbounded memory.
	body, err := io.ReadAll(io.LimitReader(request.Body, protocolProbeMaxRequestBytes+1))
	if err != nil {
		return fmt.Errorf("failed to read probe request body")
	}
	if len(body) > protocolProbeMaxRequestBytes {
		return fmt.Errorf("probe request body exceeds bounded size")
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(body, &fields); err != nil {
		return fmt.Errorf("failed to parse probe request body: %w", err)
	}

	allowed := map[string]struct{}{
		"model": {}, "stream": {}, "temperature": {}, "top_p": {},
		"metadata": {}, "service_tier": {},
	}
	if protocol == outbound.OutboundTypeOpenAIChat {
		allowed["frequency_penalty"] = struct{}{}
		allowed["presence_penalty"] = struct{}{}
		allowed["seed"] = struct{}{}
		allowed["max_tokens"] = struct{}{}
		allowed["max_completion_tokens"] = struct{}{}
	} else {
		allowed["max_output_tokens"] = struct{}{}
		allowed["truncation"] = struct{}{}
	}
	safeFields := make(map[string]json.RawMessage, len(allowed)+3)
	for key, value := range fields {
		if _, ok := allowed[key]; ok {
			safeFields[key] = value
		}
	}

	modelJSON, err := json.Marshal(modelName)
	if err != nil {
		return fmt.Errorf("failed to encode probe model: %w", err)
	}
	falseJSON := json.RawMessage("false")
	safeFields["model"] = modelJSON
	safeFields["stream"] = falseJSON
	if protocol == outbound.OutboundTypeOpenAIChat {
		messagesJSON, marshalErr := json.Marshal([]map[string]string{{"role": "user", "content": "ping"}})
		if marshalErr != nil {
			return fmt.Errorf("failed to encode probe messages: %w", marshalErr)
		}
		safeFields["messages"] = messagesJSON
		delete(safeFields, "input")
		delete(safeFields, "max_output_tokens")
		delete(safeFields, "max_completion_tokens")
		delete(safeFields, "max_tokens")
		if isProbeReasoningModel(modelName) {
			safeFields["max_completion_tokens"] = json.RawMessage("1")
		} else {
			safeFields["max_tokens"] = json.RawMessage("1")
		}
	} else {
		inputJSON, marshalErr := json.Marshal("ping")
		if marshalErr != nil {
			return fmt.Errorf("failed to encode probe input: %w", marshalErr)
		}
		safeFields["input"] = inputJSON
		delete(safeFields, "messages")
		delete(safeFields, "max_tokens")
		delete(safeFields, "max_completion_tokens")
		safeFields["max_output_tokens"] = json.RawMessage("16")
		safeFields["store"] = json.RawMessage("false")
	}

	modified, err := json.Marshal(safeFields)
	if err != nil {
		return fmt.Errorf("failed to encode bounded probe request")
	}
	if len(modified) > protocolProbeMaxRequestBytes {
		return fmt.Errorf("probe request exceeds bounded size")
	}
	request.Body = io.NopCloser(bytes.NewReader(modified))
	request.ContentLength = int64(len(modified))
	request.GetBody = func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(modified)), nil
	}
	return nil
}

func isProbeReasoningModel(modelName string) bool {
	name := strings.ToLower(strings.TrimSpace(modelName))
	return strings.HasPrefix(name, "o1") || strings.HasPrefix(name, "o3") ||
		strings.HasPrefix(name, "o4") || strings.HasPrefix(name, "gpt-5")
}

// executeOpenAIProtocolProbeRequest 执行一次探测请求，返回状态码与有界读取
// 的响应体。响应体只用于错误分类，从不进入日志或探测结果。
func executeOpenAIProtocolProbeRequest(ctx context.Context, httpClient *http.Client, request *http.Request) (int, string, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if httpClient == nil {
		return 0, "", fmt.Errorf("probe http client is nil")
	}
	if request == nil {
		return 0, "", fmt.Errorf("probe request is nil")
	}
	// The caller supplies the authoritative timeout/cancellation boundary. Bind it
	// even when the adapter already attached a background context, so this helper
	// cannot silently ignore its ctx argument.
	request = request.WithContext(ctx)
	response, err := httpClient.Do(request)
	if err != nil {
		return 0, "", err
	}
	if response.Body == nil {
		return response.StatusCode, "", fmt.Errorf("probe response body is nil")
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, protocolProbeMaxBodyBytes))
	if err != nil {
		return response.StatusCode, "", err
	}
	return response.StatusCode, string(body), nil
}

// protocolProbeConfig 是纯配置探测的输入，与 op/DB 解耦，测试可直接构造。
type protocolProbeConfig struct {
	BaseURLs      []string
	APIKey        string
	ModelName     string
	CustomHeaders []dbmodel.CustomHeader
	ParamOverride *string
	ProxyMode     dbmodel.ProxyUsageMode
	ProxyConfigID *int
	// ClientFactory 覆盖 HTTP client 构建（测试注入）；nil 时按 proxy 配置创建。
	ClientFactory func(ctx context.Context) (*http.Client, error)
	// TotalTimeout 覆盖默认探测总超时；<=0 使用默认值。
	TotalTimeout time.Duration
}

// protocolProbeAttemptTimeout gives every remaining endpoint a fair share of
// the hard total deadline while retaining the independent per-request ceiling.
// Without this allocation, two slow requests can consume the full budget and
// prevent later configured base URLs from being checked at all.
func protocolProbeAttemptTimeout(ctx context.Context, remainingAttempts int) time.Duration {
	timeout := protocolProbeRequestTimeout
	if ctx == nil || remainingAttempts <= 0 {
		return timeout
	}
	deadline, ok := ctx.Deadline()
	if !ok {
		return timeout
	}
	remaining := time.Until(deadline)
	if remaining <= 0 {
		return 0
	}
	fairShare := remaining / time.Duration(remainingAttempts)
	if fairShare < timeout {
		timeout = fairShare
	}
	return timeout
}

func (cfg *protocolProbeConfig) httpClient(ctx context.Context) (*http.Client, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if cfg.ClientFactory != nil {
		client, err := cfg.ClientFactory(ctx)
		if err != nil {
			return nil, err
		}
		if client == nil {
			return nil, fmt.Errorf("probe http client is nil")
		}
		return client, nil
	}
	// proxy 语义与普通转发一致：direct/system/pool 都经 helper 解析。
	return helper.ChannelHTTPClientWithContext(ctx, &dbmodel.Channel{
		ProxyMode:     cfg.ProxyMode,
		ProxyConfigID: cfg.ProxyConfigID,
	})
}

// probeOpenAIProtocolConfig 按配置顺序逐个尝试每个 Base URL，对每个 URL
// 分别请求 /chat/completions 与 /responses，然后按 endpoint 聚合结论。
// 总超时约束整个探测过程；单个请求另有超时上限。
func probeOpenAIProtocolConfig(ctx context.Context, cfg protocolProbeConfig) ProtocolProbeReport {
	if ctx == nil {
		ctx = context.Background()
	}
	report := ProtocolProbeReport{
		Chat:      ProtocolProbeEndpointResult{Protocol: outbound.OutboundTypeOpenAIChat, Outcome: ProtocolProbeUnknown},
		Responses: ProtocolProbeEndpointResult{Protocol: outbound.OutboundTypeOpenAIResponse, Outcome: ProtocolProbeUnknown},
	}

	totalTimeout := cfg.TotalTimeout
	if totalTimeout <= 0 {
		totalTimeout = protocolProbeTotalTimeout
	}
	probeCtx, cancel := context.WithTimeout(ctx, totalTimeout)
	defer cancel()

	httpClient, err := cfg.httpClient(probeCtx)
	if err != nil {
		report.Error = "failed to build probe http client"
		return report
	}
	// Never forward the channel key to a redirect target. Keep the configured
	// transport/proxy, but make redirects a terminal, non-evidence response.
	probeClient := *httpClient
	probeClient.CheckRedirect = func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}

	baseURLs := make([]string, 0, len(cfg.BaseURLs))
	for _, baseURL := range cfg.BaseURLs {
		if trimmed := strings.TrimSpace(baseURL); trimmed != "" {
			baseURLs = append(baseURLs, trimmed)
		}
	}
	chatOutcomes := make([]ProtocolProbeOutcome, 0, len(baseURLs))
	responsesOutcomes := make([]ProtocolProbeOutcome, 0, len(baseURLs))
	remainingAttempts := len(baseURLs) * 2
	interrupted := false
	for _, baseURL := range baseURLs {
		// Stop scheduling new requests once the hard total budget is exhausted.
		if probeCtx.Err() != nil {
			interrupted = true
			break
		}
		chatAttempt := probeProtocolEndpointOnce(
			probeCtx, &probeClient, cfg, outbound.OutboundTypeOpenAIChat, baseURL,
			protocolProbeAttemptTimeout(probeCtx, remainingAttempts),
		)
		remainingAttempts--
		report.Chat.Attempts = append(report.Chat.Attempts, chatAttempt)
		chatOutcomes = append(chatOutcomes, chatAttempt.Outcome)

		if probeCtx.Err() != nil {
			interrupted = true
			break
		}
		responsesAttempt := probeProtocolEndpointOnce(
			probeCtx, &probeClient, cfg, outbound.OutboundTypeOpenAIResponse, baseURL,
			protocolProbeAttemptTimeout(probeCtx, remainingAttempts),
		)
		remainingAttempts--
		report.Responses.Attempts = append(report.Responses.Attempts, responsesAttempt)
		responsesOutcomes = append(responsesOutcomes, responsesAttempt.Outcome)
	}
	if probeCtx.Err() != nil {
		interrupted = true
	}
	report.Chat.Outcome = aggregateProtocolProbeOutcomes(chatOutcomes)
	report.Responses.Outcome = aggregateProtocolProbeOutcomes(responsesOutcomes)
	if interrupted {
		// An interrupted run has not observed every configured base URL, so it
		// cannot conclude unsupported. Keep positive evidence: a confirmed
		// supported endpoint stays supported even when the budget ran out later.
		if report.Chat.Outcome != ProtocolProbeSupported {
			report.Chat.Outcome = ProtocolProbeUnknown
		}
		if report.Responses.Outcome != ProtocolProbeSupported {
			report.Responses.Outcome = ProtocolProbeUnknown
		}
		report.Error = "probe budget exhausted before all base urls were checked"
	}
	return report
}

// probeProtocolEndpointOnce 对一个 Base URL 上的一个 endpoint 执行一次探测。
func probeProtocolEndpointOnce(ctx context.Context, httpClient *http.Client, cfg protocolProbeConfig, protocol outbound.OutboundType, baseURL string, requestTimeout time.Duration) ProtocolProbeAttempt {
	if ctx == nil {
		ctx = context.Background()
	}
	attempt := ProtocolProbeAttempt{BaseURL: baseURL, Outcome: ProtocolProbeUnknown}
	if requestTimeout <= 0 {
		attempt.Reason = "probe request budget exhausted"
		return attempt
	}
	requestCtx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	request, err := buildOpenAIProtocolProbeRequest(
		requestCtx, protocol, baseURL, cfg.APIKey, cfg.ModelName, cfg.CustomHeaders, cfg.ParamOverride,
	)
	if err != nil {
		attempt.Reason = "failed to build probe request"
		return attempt
	}
	statusCode, body, execErr := executeOpenAIProtocolProbeRequest(requestCtx, httpClient, request)
	outcome, reason := classifyOpenAIProtocolProbeResponse(statusCode, body, execErr)
	attempt.StatusCode = statusCode
	attempt.Outcome = outcome
	attempt.Reason = reason
	return attempt
}

// ProbeChannelOpenAIProtocols 是按 channel ID 的主动探测入口：通过
// op.ChannelGet 加载权威配置（含 Keys），对每个配置的 Base URL 分别探测
// /chat/completions 与 /responses，并把 supported/unsupported 结论按 auto
// 模式持久化。manual 模式（chat_only / responses_only / both）与
// Cloudflare Workers AI 的 Chat-only 固定规则不会被探测覆盖。同一渠道的
// 并发探测（手动 + 自动触发）共享同一次在途探测，避免重复上游请求。
func ProbeChannelOpenAIProtocols(channelID int, ctx context.Context) (*ProtocolProbeReport, error) {
	if channelID <= 0 {
		return nil, fmt.Errorf("invalid channel id")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	// The merged callers share one report pointer; the report is treated as
	// read-only by handlers once returned. The flight itself must not inherit a
	// request context: an aborted first caller must not cancel other waiters.
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	resultCh := protocolProbeInflight.DoChan(strconv.Itoa(channelID), func() (any, error) {
		flightCtx, cancel := context.WithTimeout(context.Background(), protocolProbeTotalTimeout+protocolProbePersistTimeout)
		defer cancel()
		return probeChannelOpenAIProtocols(channelID, flightCtx)
	})
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case result := <-resultCh:
		if result.Err != nil {
			return nil, result.Err
		}
		resultValue := result.Val
		report, _ := resultValue.(*ProtocolProbeReport)
		if report == nil {
			return nil, fmt.Errorf("invalid probe report")
		}
		return report, nil
	}
}

func probeChannelOpenAIProtocols(channelID int, ctx context.Context) (*ProtocolProbeReport, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	channel, err := op.ChannelGetAuthoritative(channelID, ctx)
	if err != nil {
		return nil, err
	}
	report := NewProtocolProbeReport(channelID, channel)
	if !isOpenAIProtocolChannel(channel.Type) {
		report.Skipped = true
		report.SkipReason = "channel type does not use openai text protocols"
		logProbeReport(channelID, report)
		return report, nil
	}
	// Cloudflare Workers AI 的既有 Chat-only 规则：与运行时
	// effectiveOpenAIProtocolCapability 的强制语义一致，直接给固定结论，
	// 不发探测请求。
	if isCloudflareWorkersAIChannel(channel) {
		report.Skipped = true
		report.SkipReason = "cloudflare workers ai base url is chat-only"
		report.Chat.Outcome = ProtocolProbeSupported
		report.Responses.Outcome = ProtocolProbeUnsupported
		persistOpenAIProtocolProbeReport(channelID, report, ctx)
		logProbeReport(channelID, report)
		return report, nil
	}
	// Manual modes are authoritative. There is no reason to spend a real
	// upstream request when the observation cannot change the persisted state.
	if channel.OpenAIProtocolMode.Normalize() != dbmodel.OpenAIProtocolModeAuto {
		report.Skipped = true
		report.SkipReason = "channel uses manual protocol mode"
		logProbeReport(channelID, report)
		return report, nil
	}
	modelName := probeModelNameForChannel(channel)
	baseURLs := probeBaseURLsForChannel(channel)
	if modelName == "" || len(baseURLs) == 0 {
		report.Skipped = true
		if modelName == "" {
			report.SkipReason = "channel has no model configured"
		} else {
			report.SkipReason = "channel has no base url configured"
		}
		logProbeReport(channelID, report)
		return report, nil
	}
	apiKey := channel.GetChannelKey().ChannelKey
	if strings.TrimSpace(apiKey) == "" {
		report.Skipped = true
		report.SkipReason = "channel has no enabled key"
		logProbeReport(channelID, report)
		return report, nil
	}
	cfg := protocolProbeConfig{
		BaseURLs:      baseURLs,
		APIKey:        apiKey,
		ModelName:     modelName,
		CustomHeaders: channel.CustomHeader,
		ParamOverride: channel.ParamOverride,
		ProxyMode:     channel.ProxyMode,
		ProxyConfigID: channel.ProxyConfigID,
	}
	probed := probeOpenAIProtocolConfig(ctx, cfg)
	report.Chat = probed.Chat
	report.Responses = probed.Responses
	report.Error = probed.Error
	persistOpenAIProtocolProbeReport(channelID, report, ctx)
	logProbeReport(channelID, report)
	return report, nil
}

// persistOpenAIProtocolProbeReport 把 supported/unsupported 结论交给 op 层
// 持久化。op.ChannelRecordOpenAIProtocolCapability 只在 auto 模式下生效；
// unknown 不落库，保持三态中的 unknown 语义。
func persistOpenAIProtocolProbeReport(channelID int, report *ProtocolProbeReport, parent context.Context) {
	if report == nil {
		return
	}
	if parent == nil {
		parent = context.Background()
	}
	persistCtx, cancel := context.WithTimeout(context.WithoutCancel(parent), protocolProbePersistTimeout)
	defer cancel()
	for _, endpoint := range []ProtocolProbeEndpointResult{report.Chat, report.Responses} {
		capability, ok := protocolProbeOutcomeToCapability(endpoint.Outcome)
		if !ok {
			continue
		}
		if err := op.ChannelRecordOpenAIProtocolCapability(channelID, endpoint.Protocol, capability, persistCtx); err != nil {
			log.Warnf("failed to persist openai protocol probe result (channel=%d protocol=%d)", channelID, endpoint.Protocol)
		}
	}
}

func protocolProbeOutcomeToCapability(outcome ProtocolProbeOutcome) (dbmodel.OpenAIProtocolCapability, bool) {
	switch outcome {
	case ProtocolProbeSupported:
		return dbmodel.OpenAIProtocolCapabilitySupported, true
	case ProtocolProbeUnsupported:
		return dbmodel.OpenAIProtocolCapabilityUnsupported, true
	default:
		return "", false
	}
}

// logProbeReport 输出探测汇总日志。只记录结论与状态码级别的短语，
// 不记录请求凭据、上游响应体或完整响应。
func logProbeReport(channelID int, report *ProtocolProbeReport) {
	if report == nil {
		return
	}
	if report.Skipped {
		log.Infof("openai protocol probe skipped (channel=%d): %s", channelID, report.SkipReason)
		return
	}
	if report.Error != "" {
		log.Infof("openai protocol probe (channel=%d): chat=%s responses=%s error=%s", channelID, report.Chat.Outcome, report.Responses.Outcome, report.Error)
		return
	}
	log.Infof("openai protocol probe (channel=%d): chat=%s responses=%s", channelID, report.Chat.Outcome, report.Responses.Outcome)
}

// probeModelNameForChannel 取渠道配置的第一个模型名，解析方式与
// ChannelLLMList 一致。没有可用模型时返回空，调用方跳过探测。
func probeModelNameForChannel(channel *dbmodel.Channel) string {
	if channel == nil {
		return ""
	}
	models := xstrings.SplitTrimCompact(",", channel.Model, channel.CustomModel)
	if len(models) == 0 {
		return ""
	}
	return models[0]
}

// probeBaseURLsForChannel 返回渠道配置的全部非空 Base URL（配置顺序）。
// 探测逐个尝试，并在结果里保留每个 URL 的独立结论。
func probeBaseURLsForChannel(channel *dbmodel.Channel) []string {
	if channel == nil {
		return nil
	}
	urls := make([]string, 0, len(channel.BaseUrls))
	for _, baseURL := range channel.BaseUrls {
		if strings.TrimSpace(baseURL.URL) == "" {
			continue
		}
		urls = append(urls, baseURL.URL)
	}
	return urls
}
