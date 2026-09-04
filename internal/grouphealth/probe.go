package grouphealth

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/bestruirui/octopus/internal/helper"
	"github.com/bestruirui/octopus/internal/model"
	"github.com/bestruirui/octopus/internal/sitesync"
	transformerModel "github.com/bestruirui/octopus/internal/transformer/model"
	"github.com/bestruirui/octopus/internal/transformer/outbound"
)

type ProbeResult struct {
	Success           bool
	HTTPStatus        int
	DurationMS        int64
	ErrorMessage      string
	CloudflareBlocked bool
	Header            http.Header // 上游响应头，供 POR 门3 做 Cloudflare 指纹识别
}

type Prober struct {
	CandidateTimeout time.Duration
}

func NewProber() *Prober {
	return &Prober{
		CandidateTimeout: 12 * time.Second,
	}
}

func (p *Prober) RunCandidate(ctx context.Context, channel model.Channel, usedKey model.ChannelKey, modelName string) ProbeResult {
	startedAt := time.Now()
	result := ProbeResult{}

	timeout := p.CandidateTimeout
	if timeout <= 0 {
		timeout = 12 * time.Second
	}

	probeCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	request, err := buildProbeRequest(probeCtx, &channel, &usedKey, modelName)
	if err != nil {
		result.ErrorMessage = safeProbeError(err, "probe request invalid")
		result.DurationMS = time.Since(startedAt).Milliseconds()
		return result
	}

	applyCustomHeaders(request, channel.CustomHeader)
	applySelectedProbeCredentialHeader(request.Header, channel.Type, usedKey.ChannelKey)
	// 防止 Go 默认 User-Agent 泄露到上游
	if request.Header.Get("User-Agent") == "" {
		request.Header.Set("User-Agent", "")
	}
	if err := helper.ApplyParamOverride(request, channel.ParamOverride); err != nil {
		result.ErrorMessage = safeProbeError(err, "probe request override failed")
		result.DurationMS = time.Since(startedAt).Milliseconds()
		return result
	}

	httpClient, err := helper.ChannelHTTPClientWithContext(probeCtx, &channel)
	if err != nil {
		result.ErrorMessage = safeProbeError(err, "probe client unavailable")
		result.DurationMS = time.Since(startedAt).Milliseconds()
		return result
	}

	response, err := httpClient.Do(request)
	if err != nil {
		result.ErrorMessage = safeProbeError(err, "probe request failed")
		result.DurationMS = time.Since(startedAt).Milliseconds()
		return result
	}
	defer response.Body.Close()

	result.HTTPStatus = response.StatusCode
	result.Header = response.Header.Clone()
	result.DurationMS = time.Since(startedAt).Milliseconds()

	if response.StatusCode >= 200 && response.StatusCode < 300 {
		result.Success = true
		return result
	}

	body, _ := io.ReadAll(io.LimitReader(response.Body, 8*1024))
	result.CloudflareBlocked = sitesync.IsCloudflareProtectionResponse(response.StatusCode, result.Header, body)
	result.ErrorMessage = fmt.Sprintf("upstream error: %d", response.StatusCode)
	return result
}

func safeProbeError(err error, fallback string) string {
	if err == nil {
		return fallback
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return fallback + ": timed out"
	}
	if errors.Is(err, context.Canceled) {
		return fallback + ": canceled"
	}
	return fallback
}

func buildProbeRequest(ctx context.Context, channel *model.Channel, usedKey *model.ChannelKey, modelName string) (*http.Request, error) {
	if channel == nil {
		return nil, fmt.Errorf("channel is nil")
	}
	if usedKey == nil {
		return nil, fmt.Errorf("channel key is nil")
	}
	if strings.TrimSpace(usedKey.ChannelKey) == "" {
		return nil, fmt.Errorf("channel key is empty")
	}
	if strings.TrimSpace(modelName) == "" {
		return nil, fmt.Errorf("model name is empty")
	}

	request := buildProbeInternalRequest(channel.Type, modelName)
	adapter := outbound.Get(channel.Type)
	if adapter == nil {
		return nil, fmt.Errorf("unsupported outbound type: %d", channel.Type)
	}
	return adapter.TransformRequest(ctx, request, channel.GetBaseUrl(), usedKey.ChannelKey)
}

func buildProbeInternalRequest(channelType outbound.OutboundType, modelName string) *transformerModel.InternalLLMRequest {
	stream := false
	ping := "ping"
	one := int64(1)

	switch channelType {
	case outbound.OutboundTypeOpenAIEmbedding:
		return &transformerModel.InternalLLMRequest{
			Model:        modelName,
			RawAPIFormat: transformerModel.APIFormatOpenAIEmbedding,
			EmbeddingInput: &transformerModel.EmbeddingInput{
				Single: &ping,
			},
		}
	case outbound.OutboundTypeOpenAIResponse:
		return &transformerModel.InternalLLMRequest{
			Model:               modelName,
			RawAPIFormat:        transformerModel.APIFormatOpenAIResponse,
			Messages:            []transformerModel.Message{{Role: "user", Content: transformerModel.MessageContent{Content: &ping}}},
			Stream:              &stream,
			MaxCompletionTokens: &one,
		}
	case outbound.OutboundTypeAnthropic:
		return &transformerModel.InternalLLMRequest{
			Model:        modelName,
			RawAPIFormat: transformerModel.APIFormatAnthropicMessage,
			Messages:     []transformerModel.Message{{Role: "user", Content: transformerModel.MessageContent{Content: &ping}}},
			Stream:       &stream,
			MaxTokens:    &one,
		}
	case outbound.OutboundTypeGemini:
		return &transformerModel.InternalLLMRequest{
			Model:        modelName,
			RawAPIFormat: transformerModel.APIFormatGeminiContents,
			Messages:     []transformerModel.Message{{Role: "user", Content: transformerModel.MessageContent{Content: &ping}}},
			Stream:       &stream,
			MaxTokens:    &one,
		}
	case outbound.OutboundTypeVolcengine:
		return &transformerModel.InternalLLMRequest{
			Model:        modelName,
			RawAPIFormat: transformerModel.APIFormatOpenAIChatCompletion,
			Messages:     []transformerModel.Message{{Role: "user", Content: transformerModel.MessageContent{Content: &ping}}},
			Stream:       &stream,
			MaxTokens:    &one,
		}
	default:
		return &transformerModel.InternalLLMRequest{
			Model:        modelName,
			RawAPIFormat: transformerModel.APIFormatOpenAIChatCompletion,
			Messages:     []transformerModel.Message{{Role: "user", Content: transformerModel.MessageContent{Content: &ping}}},
			Stream:       &stream,
			MaxTokens:    &one,
		}
	}
}

func applyCustomHeaders(request *http.Request, headers []model.CustomHeader) {
	if request == nil {
		return
	}
	for _, header := range headers {
		key := strings.TrimSpace(header.HeaderKey)
		if !shouldProxyProbeChannelHeader(key) {
			continue
		}
		request.Header.Set(key, header.HeaderValue)
	}
}

var blockedProbeChannelHeaders = map[string]struct{}{
	"authorization":       {},
	"x-api-key":           {},
	"x-goog-api-key":      {},
	"api-key":             {},
	"set-cookie":          {},
	"connection":          {},
	"keep-alive":          {},
	"proxy-authenticate":  {},
	"proxy-authorization": {},
	"te":                  {},
	"trailer":             {},
	"transfer-encoding":   {},
	"upgrade":             {},
	"content-length":      {},
	"host":                {},
	"accept-encoding":     {},
	"x-forwarded-for":     {},
	"x-forwarded-host":    {},
	"x-forwarded-proto":   {},
	"x-forwarded-port":    {},
	"x-real-ip":           {},
	"forwarded":           {},
	"cf-connecting-ip":    {},
	"true-client-ip":      {},
	"x-client-ip":         {},
	"x-cluster-client-ip": {},
}

func shouldProxyProbeChannelHeader(name string) bool {
	lowerName := strings.ToLower(strings.TrimSpace(name))
	if lowerName == "" || strings.HasPrefix(lowerName, "sec-websocket-") {
		return false
	}
	_, blocked := blockedProbeChannelHeaders[lowerName]
	return !blocked
}

func applySelectedProbeCredentialHeader(headers http.Header, protocol outbound.OutboundType, key string) {
	if headers == nil {
		return
	}
	for _, name := range []string{"Authorization", "X-API-Key", "X-Goog-Api-Key", "Api-Key"} {
		for headerName := range headers {
			if strings.EqualFold(headerName, name) {
				delete(headers, headerName)
			}
		}
	}
	switch protocol {
	case outbound.OutboundTypeAnthropic:
		headers.Set("X-API-Key", key)
	case outbound.OutboundTypeGemini:
		headers.Set("X-Goog-Api-Key", key)
	default:
		headers.Set("Authorization", "Bearer "+key)
	}
}
