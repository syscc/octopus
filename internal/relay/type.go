package relay

import (
	"context"
	"io"
	"net/http"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/bestruirui/octopus/internal/conf"
	dbmodel "github.com/bestruirui/octopus/internal/model"
	"github.com/bestruirui/octopus/internal/relay/balancer"
	"github.com/bestruirui/octopus/internal/transformer/model"
	"github.com/bestruirui/octopus/internal/transformer/outbound"
	"github.com/gin-gonic/gin"
)

// maxSSEEventSize 定义 SSE 事件的最大大小。
// 对于图像生成模型（如 gemini-3-pro-image-preview），返回的 base64 编码图像数据
// 可能非常大（高分辨率图像可能超过 10MB），因此需要设置足够大的缓冲区。
// 默认 32MB，可通过环境变量 OCTOPUS_RELAY_MAX_SSE_EVENT_SIZE 覆盖。
var maxSSEEventSize = 32 * 1024 * 1024

const wsWriteTimeout = 10 * time.Second
const wsPassthroughDrainTimeout = 5 * time.Second

func init() {
	if raw := strings.TrimSpace(os.Getenv(strings.ToUpper(conf.APP_NAME) + "_RELAY_MAX_SSE_EVENT_SIZE")); raw != "" {
		if v, err := strconv.Atoi(raw); err == nil && v > 0 {
			maxSSEEventSize = v
		}
	}
}

// hopByHopHeaders 定义不应转发的 HTTP 头。除了 RFC hop-by-hop 字段，
// relay 还必须隔离客户端会话与上游 channel 凭据，防止跨租户泄漏或覆盖。
var hopByHopHeaders = map[string]bool{
	"authorization":       true,
	"x-api-key":           true,
	"x-goog-api-key":      true,
	"api-key":             true,
	"cookie":              true,
	"set-cookie":          true,
	"connection":          true,
	"keep-alive":          true,
	"proxy-authenticate":  true,
	"proxy-authorization": true,
	"te":                  true,
	"trailer":             true,
	"transfer-encoding":   true,
	"upgrade":             true,
	"content-length":      true,
	"host":                true,
	"accept-encoding":     true,
	"x-forwarded-for":     true,
	"x-forwarded-host":    true,
	"x-forwarded-proto":   true,
	"x-forwarded-port":    true,
	"x-real-ip":           true,
	"forwarded":           true,
	"cf-connecting-ip":    true,
	"true-client-ip":      true,
	"x-client-ip":         true,
	"x-cluster-client-ip": true,
}

func isBlockedChannelHeader(name string) bool {
	lowerName := strings.ToLower(strings.TrimSpace(name))
	// Cookie is a deliberate channel-level authentication/configuration value.
	// It is still blocked when it originates from the client (the client path
	// uses isBlockedUpstreamHeader), but it must remain usable for a channel.
	if lowerName == "cookie" {
		return false
	}
	return isBlockedUpstreamHeader(lowerName)
}

func isBlockedUpstreamHeader(name string) bool {
	lowerName := strings.ToLower(strings.TrimSpace(name))
	if lowerName == "" {
		return true
	}
	return hopByHopHeaders[lowerName]
}

func deleteHeaderCaseInsensitive(headers http.Header, name string) {
	if headers == nil {
		return
	}
	normalizedName := strings.TrimSpace(name)
	for key := range headers {
		if strings.EqualFold(strings.TrimSpace(key), normalizedName) {
			delete(headers, key)
		}
	}
}

type normalizedHeaderEntry struct {
	name   string
	values []string
}

func collectNormalizedHeaderEntries(src http.Header, allow func(string) bool) map[string]normalizedHeaderEntry {
	entries := make(map[string]normalizedHeaderEntry, len(src))
	keys := make([]string, 0, len(src))
	for key := range src {
		keys = append(keys, key)
	}
	slices.SortFunc(keys, func(a, b string) int {
		lowerA := strings.ToLower(strings.TrimSpace(a))
		lowerB := strings.ToLower(strings.TrimSpace(b))
		if lowerA == lowerB {
			return strings.Compare(a, b)
		}
		return strings.Compare(lowerA, lowerB)
	})
	for _, key := range keys {
		values := src[key]
		name := strings.TrimSpace(key)
		if name == "" || (allow != nil && !allow(name)) {
			continue
		}
		lowerName := strings.ToLower(name)
		entry := entries[lowerName]
		if entry.name == "" {
			entry.name = name
		}
		entry.values = append(entry.values, values...)
		entries[lowerName] = entry
	}
	return entries
}

func headerValuesCaseInsensitive(headers http.Header, name string) []string {
	if headers == nil {
		return nil
	}
	normalizedName := strings.TrimSpace(name)
	entries := collectNormalizedHeaderEntries(headers, func(key string) bool {
		return strings.EqualFold(strings.TrimSpace(key), normalizedName)
	})
	entry := entries[strings.ToLower(normalizedName)]
	return append([]string(nil), entry.values...)
}

func setHeaderValuesCaseInsensitive(headers http.Header, name string, values []string) {
	if headers == nil {
		return
	}
	name = strings.TrimSpace(name)
	deleteHeaderCaseInsensitive(headers, name)
	if name == "" || len(values) == 0 {
		return
	}
	headers.Set(name, values[0])
	for _, value := range values[1:] {
		headers.Add(name, value)
	}
}

func applySelectedCredentialHeader(headers http.Header, protocol outbound.OutboundType, key string) {
	if headers == nil {
		return
	}
	for _, name := range []string{"Authorization", "X-API-Key", "X-Goog-Api-Key", "Api-Key"} {
		deleteHeaderCaseInsensitive(headers, name)
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

// StreamWriter abstracts writing responses to the client (HTTP SSE or WebSocket).
type StreamWriter interface {
	Write(data []byte) (int, error)
	Flush()
	Written() bool
	Header() http.Header
	WriteHeader(code int)
}

// UpstreamReader abstracts reading events from upstream (SSE or WebSocket).
type UpstreamReader interface {
	// ReadEvent reads the next event data. Returns io.EOF at end of stream.
	ReadEvent(ctx context.Context) ([]byte, error)
	// StatusCode returns the HTTP status code (for error handling).
	StatusCode() int
	// Headers returns the response headers.
	Headers() http.Header
	// Body returns the raw response body for non-stream scenarios.
	Body() io.ReadCloser
	Close() error
}

type relayRequest struct {
	c               *gin.Context
	ctx             context.Context // used when c is nil (WebSocket mode)
	inAdapter       model.Inbound
	internalRequest *model.InternalLLMRequest
	metrics         *RelayMetrics
	apiKeyID        int
	requestModel    string
	groupID         int
	groupSessionTTL int
	iter            *balancer.Iterator

	// newInboundAdapter 每次真实网络 attempt 前创建全新的入站 adapter。
	// 失败 attempt 的 usage / 流式聚合状态必须随旧 adapter 一起丢弃，
	// 不能泄漏到同渠道重试、协议回落或下一个候选渠道。nil 表示沿用
	// 请求级解析 adapter（旧测试直接构造 relayRequest 时的兼容路径）。
	newInboundAdapter func() model.Inbound

	// rawBody 保存客户端原始请求 body，用于同格式（如 Anthropic→Anthropic）直通转发时
	// 绕过内部模型来回转换，以保证 beta 字段、内容块顺序、thinking 签名等完全透传。
	rawBody []byte

	// streamWriter allows overriding the response writer (nil = use c.Writer)
	streamWriter StreamWriter

	// heartbeat 管理可选的上游流建立前延迟心跳；默认 no-op。
	heartbeat *earlyHeartbeat

	streamPayloadWritten atomic.Bool
	responseCollected    atomic.Bool
}

// requestContext returns the request context from gin or the standalone context.
func (r *relayRequest) requestContext() context.Context {
	if r.c != nil {
		return r.c.Request.Context()
	}
	return r.ctx
}

// relayAttempt 尝试级上下文
type relayAttempt struct {
	*relayRequest // 嵌入请求级上下文

	outAdapter            model.Outbound
	activeOutboundType    outbound.OutboundType
	activeOutboundTypeSet bool
	channel               *dbmodel.Channel
	usedKey               dbmodel.ChannelKey
	firstTokenTimeOutSec  int
	firstTokenBudget      *firstTokenBudget
	retryAfter            time.Duration // forward() 提取后暂存

	// attemptUsedWS 记录本次 attempt 最终是否由上游 WebSocket 提供结果。
	// WS 降级到 HTTP（协议回退或 WS 不可用回落）后置 false，attempt 收尾时
	// 同步到请求级 metrics，避免最终日志仍报告上一次 WS 尝试的传输标记。
	attemptUsedWS bool
	// passthroughOutcome records the raw protocol terminal observed by the
	// attempt. It is set before the stream processor returns so result handling
	// can distinguish a legal incomplete terminal from a transport interruption.
	passthroughOutcome model.PassthroughTerminalOutcome
}

// attemptResult 封装单次尝试的结果
type attemptResult struct {
	Success           bool                             // 是否成功
	Written           bool                             // 流式响应是否已开始写入（不可重试）
	Canceled          bool                             // 是否由下游请求取消或超时触发
	ResetConversation bool                             // 是否需要立即重置连续会话并停止后续 failover
	FirstTokenTimeout bool                             // 是否由首字超时触发，用于直接切换渠道
	Err               error                            // 失败时的错误
	UpstreamErr       error                            // 未附加渠道名的原始上游错误，仅用于协议/能力分类
	StatusCode        int                              // 上游 HTTP 状态码（0 = 连接错误）
	RetryAfter        time.Duration                    // 解析的 Retry-After 值
	OutboundType      outbound.OutboundType            // 本次实际使用的出站协议
	TerminalOutcome   model.PassthroughTerminalOutcome // passthrough 协议终态结果

	// inboundAdapter 是本次 attempt 实际使用的入站 adapter。成功时 replay
	// 状态保存、失败时指标收集都必须用它读取，而不是解析请求时的旧实例。
	inboundAdapter model.Inbound
}

func (r attemptResult) protocolError() error {
	if r.UpstreamErr != nil {
		return r.UpstreamErr
	}
	return r.Err
}

// syncWSTransportMetrics 在 attempt 收尾时把请求级 WS 传输标记对齐到本次
// attempt 实际使用的传输：WS 降级到 HTTP（协议回退或 WS 不可用回落）后，
// 最终结果由 HTTP 提供，UsedWS/WSExecMode 不能继续报告上一次 WS 尝试；
// WSRecovery（downgrade 等）与 WSMode 不受影响。
func (ra *relayAttempt) syncWSTransportMetrics() {
	if ra == nil || ra.metrics == nil {
		return
	}
	ra.metrics.UsedWS = ra.attemptUsedWS
	if !ra.attemptUsedWS {
		ra.metrics.WSExecMode = nil
	}
}
