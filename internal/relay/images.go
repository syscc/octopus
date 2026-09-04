package relay

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/bestruirui/octopus/internal/conf"
	"github.com/bestruirui/octopus/internal/helper"
	"github.com/bestruirui/octopus/internal/model"
	"github.com/bestruirui/octopus/internal/op"
	"github.com/bestruirui/octopus/internal/outlierwindow"
	"github.com/bestruirui/octopus/internal/price"
	"github.com/bestruirui/octopus/internal/relay/balancer"
	"github.com/bestruirui/octopus/internal/relay/bodycache"
	"github.com/bestruirui/octopus/internal/relay/stream"
	"github.com/bestruirui/octopus/internal/server/resp"
	transformerModel "github.com/bestruirui/octopus/internal/transformer/model"
	"github.com/bestruirui/octopus/internal/transformer/outbound"
	"github.com/bestruirui/octopus/internal/utils/log"
	"github.com/gin-gonic/gin"
)

const imagesUpstreamErrorBodyLimit = 16 * 1024

// errDownstreamWriteFailed is shared with the regular stream relay so all
// downstream writer failures receive the same health-accounting classification.
var errDownstreamWriteFailed = stream.ErrDownstreamWriteFailed

// downstreamWriteError wraps a downstream writer error while preserving its
// underlying chain for diagnostics.
func downstreamWriteError(err error) error {
	return stream.WrapDownstreamWriteError(err)
}

// ImagesHandler 是 OpenAI Images API 的统一 relay 入口。
// endpoint 形如：/images/generations、/images/edits、/images/variations（不含 /v1 前缀）。
func ImagesHandler(endpoint string, c *gin.Context) {
	ctx := c.Request.Context()

	apiKeyID := c.GetInt("api_key_id")

	// 缓存请求体，支持多次重试重放
	bc, err := bodycache.New(c.Request.Body)
	if err != nil {
		var tooLarge *bodycache.BodyTooLargeError
		if errors.As(err, &tooLarge) {
			resp.Error(c, http.StatusRequestEntityTooLarge, "request body too large")
			return
		}
		resp.Error(c, http.StatusInternalServerError, err.Error())
		return
	}
	defer func() {
		if cerr := bc.Close(); cerr != nil {
			log.Warnf("failed to close images body cache: %v", cerr)
		}
	}()

	contentType := c.GetHeader("Content-Type")
	isMultipart := strings.Contains(strings.ToLower(contentType), "multipart/form-data")

	// 解析 requestModel 与 stream（严格模式：model 必填）
	var (
		requestModel string
		stream       bool
		boundary     string
		jsonPayload  map[string]any
	)
	if isMultipart {
		_, params, perr := mime.ParseMediaType(contentType)
		if perr != nil {
			resp.Error(c, http.StatusBadRequest, "invalid multipart content-type")
			return
		}
		boundary = strings.TrimSpace(params["boundary"])
		if boundary == "" {
			resp.Error(c, http.StatusBadRequest, "invalid multipart boundary")
			return
		}
		m, s, perr := parseMultipartModelAndStream(bc, boundary)
		if perr != nil {
			resp.Error(c, http.StatusBadRequest, perr.Error())
			return
		}
		requestModel = m
		stream = s
	} else {
		payload, m, s, perr := parseJSONModelAndStream(bc)
		if perr != nil {
			resp.Error(c, http.StatusBadRequest, perr.Error())
			return
		}
		jsonPayload = payload
		requestModel = m
		stream = s
	}

	// supported_models 校验（复用 APIKeyAuth 注入）
	supportedModels := strings.TrimSpace(c.GetString("supported_models"))
	if supportedModels != "" {
		supportedModelsArray := strings.Split(supportedModels, ",")
		if !slices.Contains(supportedModelsArray, requestModel) {
			resp.ErrorWithCode(c, http.StatusBadRequest, CodeRelayModelNotSupported, "model not supported")
			return
		}
	}

	// 获取通道分组
	group, err := op.GroupGetEnabledMap(requestModel, ctx)
	if err != nil {
		resp.ErrorWithCode(c, http.StatusNotFound, CodeRelayModelNotFound, "model not found")
		return
	}

	// 创建迭代器（策略排序 + 粘性优先）
	iter := balancer.NewIterator(group, apiKeyID, requestModel)
	if iter.Len() == 0 {
		resp.ErrorWithCode(c, http.StatusServiceUnavailable, CodeRelayNoAvailableChannel, "no available channel")
		return
	}

	// 初始化 Metrics（Images 独立，避免 b64_json 内存膨胀）
	metrics := newImagesRelayMetrics(apiKeyID, requestModel)
	metrics.RequestContent = buildImagesRequestContentForLog(isMultipart, bc, jsonPayload)

	// === 早期心跳 ===
	// 流式：启动早期心跳协程，覆盖前置阶段（连接慢、failover、退避）期间向客户端发 SSE 注释字节
	// 非流式：无法发送 SSE 注释（破坏 application/json 协议），不施加本地超时
	hb := startEarlyHeartbeat(c, stream)
	defer func() { hb.Stop() }()

	var (
		lastErr        error
		lastStatusCode int
		lastRetryAfter time.Duration
	)

	for iter.Next() {
		select {
		case <-ctx.Done():
			log.Debugf("request context canceled, stopping retry")
			metrics.SaveWithChannelStats(ctx, false, context.Canceled, iter.Attempts(), false)
			return
		default:
		}

		// proxySSE hands exclusive writer ownership to StreamProcessor. If that
		// attempt produces no payload and fails over, start a fresh pre-stream
		// heartbeat for the next candidate's connection/response wait.
		if hb.NeedsRestart() {
			hb = startEarlyHeartbeat(c, stream)
		}

		item := iter.Item()

		// 获取通道
		channel, err := op.ChannelGet(item.ChannelID, ctx)
		if err != nil {
			log.Warnf("failed to get channel %d: %v", item.ChannelID, err)
			iter.Skip(item.ChannelID, 0, fmt.Sprintf("channel_%d", item.ChannelID), fmt.Sprintf("channel not found: %v", err))
			lastErr = err
			continue
		}
		if !channel.Enabled {
			iter.Skip(channel.ID, 0, channel.Name, "channel disabled")
			continue
		}

		// channel.Type 限制：仅 OpenAI Chat/Responses
		if channel.Type != outbound.OutboundTypeOpenAIChat && channel.Type != outbound.OutboundTypeOpenAIResponse {
			iter.Skip(channel.ID, 0, channel.Name, fmt.Sprintf("unsupported channel type: %d", channel.Type))
			continue
		}

		selectOpts := model.ChannelKeySelectOptions{
			ExcludeKeyIDs:  make(map[int]struct{}),
			PreferredKeyID: iter.StickyKeyID(),
		}
		var usedKey model.ChannelKey
		for {
			usedKey = channel.GetChannelKey(selectOpts)
			if usedKey.ChannelKey == "" {
				break
			}
			// A tripped credential does not make the entire channel unavailable.
			// Continue through the channel's remaining enabled keys, matching the
			// ordinary Relay and Compact selection policy.
			if !iter.SkipCircuitBreak(channel.ID, usedKey.ID, channel.Name) {
				break
			}
			selectOpts.ExcludeKeyIDs[usedKey.ID] = struct{}{}
			usedKey = model.ChannelKey{}
		}
		if usedKey.ChannelKey == "" {
			if len(selectOpts.ExcludeKeyIDs) == 0 {
				iter.Skip(channel.ID, 0, channel.Name, "no available key")
			}
			continue
		}

		log.Debugf("images request model %s, mode: %d, forwarding to channel: %s model: %s (attempt %d/%d, sticky=%t, stream=%t)",
			requestModel, group.Mode, channel.Name, item.ModelName,
			iter.Index()+1, iter.Len(), iter.IsSticky(), stream)

		span := iter.StartAttempt(channel.ID, usedKey.ID, channel.Name)

		// 尝试一次转发
		metrics.resetAttemptUsage(item.ModelName)
		statusCode, written, usage, upstreamCT, retryAfter, fwdErr := imagesAttempt(ctx, endpoint, c, bc, isMultipart, boundary, jsonPayload, stream, channel, usedKey.ChannelKey, group.FirstTokenTimeOut, metrics, item.ModelName, hb)

		// 更新 channel key 状态，并保留已经完成的上游 usage，即使下游
		// 随后的写出失败。此类请求仍可能已经由 provider 计费。
		usedKey.StatusCode = statusCode
		usedKey.LastUseTimeStamp = time.Now().Unix()
		if usage != nil {
			metrics.SetUsageFromImages(item.ModelName, *usage)
			metrics.ResponseContent = buildImagesResponseContentForLog(stream, upstreamCT, usage)
		}

		if fwdErr == nil {
			// ====== 成功 ======
			usedKey.TotalCost += metrics.Stats.InputCost + metrics.Stats.OutputCost
			op.ChannelKeyUpdate(usedKey)

			span.End(model.AttemptSuccess, statusCode, "")

			// Channel 维度统计
			op.StatsChannelUpdate(channel.ID, model.StatsMetrics{
				WaitTime:       span.Duration().Milliseconds(),
				RequestSuccess: 1,
			})

			// 熔断器：记录成功，并补充 outlier 健康记账（与普通 relay 一致）
			balancer.RecordSuccess(channel.ID, usedKey.ID, item.ModelName)
			outlierwindow.Report(channel.ID, true, statusCode, time.Now())
			// 会话保持：更新粘性记录
			balancer.SetSticky(apiKeyID, requestModel, channel.ID, usedKey.ID)

			metrics.SaveWithChannelStats(ctx, true, nil, iter.Attempts(), false)
			return
		}

		// ====== 失败 ======
		lastStatusCode = statusCode
		lastRetryAfter = retryAfter
		if usage != nil {
			usedKey.TotalCost += metrics.Stats.InputCost + metrics.Stats.OutputCost
		}
		op.ChannelKeyUpdate(usedKey)
		span.End(model.AttemptFailed, statusCode, fwdErr.Error())

		// 客户端取消与普通 relay 语义一致：不算上游失败/成功，直接结束。
		if isClientCancellation(ctx, fwdErr) {
			metrics.SaveWithChannelStats(ctx, false, fwdErr, iter.Attempts(), false)
			return
		}

		// 下游写断开：错误源自向客户端写出失败，与上游健康无关；
		// payload 已可见时同时禁止 retry/failover。
		if errors.Is(fwdErr, errDownstreamWriteFailed) {
			// A broken downstream cannot be repaired by retrying another provider.
			// Stop even when the failed write accepted zero bytes.
			metrics.SaveWithChannelStats(ctx, false, fwdErr, iter.Attempts(), false)
			return
		}

		// Channel 维度统计
		op.StatsChannelUpdate(channel.ID, model.StatsMetrics{
			WaitTime:      span.Duration().Milliseconds(),
			RequestFailed: 1,
		})

		// 熔断器：记录失败；真实上游失败同时补充 outlier 记账
		balancer.RecordFailure(channel.ID, usedKey.ID, item.ModelName, circuitFailureKind(group.RetryEnabled, statusCode))
		outlierwindow.Report(channel.ID, false, statusCode, time.Now())

		if written {
			// 上游在 payload 已输出后失败：渠道仍需失败记账（与 relay 的
			// incomplete 语义一致），但 payload 已可见，禁止 retry/failover。
			metrics.SaveWithChannelStats(ctx, false, fwdErr, iter.Attempts(), false)
			return
		}

		lastErr = fmt.Errorf("channel %s failed: %v", channel.Name, fwdErr)
	}

	// 所有通道都失败。无上游响应或 2xx 语义失败统一为 502；明确的
	// upstream 状态保留给调用方，429/503 同时透传有界 Retry-After。
	metrics.SaveWithChannelStats(ctx, false, lastErr, iter.Attempts(), false)
	if lastStatusCode <= 0 || (lastStatusCode >= 200 && lastStatusCode < 300) {
		lastStatusCode = http.StatusBadGateway
	}
	if isPassthroughStatus(lastStatusCode) && lastRetryAfter > 0 {
		c.Header("Retry-After", fmt.Sprintf("%d", int(lastRetryAfter.Seconds())))
	}
	hb.FlushOrError(c, lastStatusCode, "all channels failed")
}

type imagesUsage struct {
	InputTokens  int `json:"input_tokens"`
	OutputTokens int `json:"output_tokens"`
	TotalTokens  int `json:"total_tokens"`
}

type imagesRelayMetrics struct {
	APIKeyID     int
	RequestModel string
	ActualModel  string
	StartTime    time.Time
	FirstToken   time.Time

	Stats model.StatsMetrics

	RequestContent  string
	ResponseContent string
}

func newImagesRelayMetrics(apiKeyID int, requestModel string) *imagesRelayMetrics {
	return &imagesRelayMetrics{
		APIKeyID:     apiKeyID,
		RequestModel: requestModel,
		StartTime:    time.Now(),
	}
}

func (m *imagesRelayMetrics) SetFirstTokenTime(t time.Time) {
	if m.FirstToken.IsZero() {
		m.FirstToken = t
	}
}

func (m *imagesRelayMetrics) resetAttemptUsage(actualModel string) {
	m.ActualModel = actualModel
	m.Stats.InputToken = 0
	m.Stats.OutputToken = 0
	m.Stats.InputCost = 0
	m.Stats.OutputCost = 0
	m.ResponseContent = ""
}

func (m *imagesRelayMetrics) SetUsageFromImages(actualModel string, u imagesUsage) {
	m.resetAttemptUsage(actualModel)
	m.Stats.InputToken = int64(u.InputTokens)
	m.Stats.OutputToken = int64(u.OutputTokens)

	modelPrice := price.GetLLMPrice(actualModel)
	if modelPrice == nil {
		return
	}

	m.Stats.InputCost = float64(u.InputTokens) * modelPrice.Input * 1e-6
	m.Stats.OutputCost = float64(u.OutputTokens) * modelPrice.Output * 1e-6
}

func (m *imagesRelayMetrics) Save(ctx context.Context, success bool, err error, attempts []model.ChannelAttempt) {
	m.SaveWithChannelStats(ctx, success, err, attempts, true)
}

func (m *imagesRelayMetrics) SaveWithChannelStats(ctx context.Context, success bool, err error, attempts []model.ChannelAttempt, updateChannelStats bool) {
	duration := time.Since(m.StartTime)

	globalStats := model.StatsMetrics{
		WaitTime:    duration.Milliseconds(),
		InputToken:  m.Stats.InputToken,
		OutputToken: m.Stats.OutputToken,
		InputCost:   m.Stats.InputCost,
		OutputCost:  m.Stats.OutputCost,
	}
	if success {
		globalStats.RequestSuccess = 1
	} else {
		globalStats.RequestFailed = 1
	}

	channelID, channelName := finalChannel(attempts)
	op.StatsTotalUpdate(globalStats)
	op.StatsHourlyUpdate(globalStats)
	op.StatsDailyUpdate(context.Background(), globalStats)
	op.StatsAPIKeyUpdate(m.APIKeyID, globalStats)
	if updateChannelStats {
		op.StatsChannelUpdate(channelID, globalStats)
	} else {
		updateFinalChannelUsageStats(channelID, globalStats)
	}
	op.StatsSiteModelHourlyRecordAttempts(attempts, m.ActualModel)

	if conf.AppConfig.Log.Relay.Summary || !success {
		fields := []interface{}{
			"model", m.RequestModel,
			"actual_model", m.ActualModel,
			"channel_id", channelID,
			"channel", channelName,
			"success", success,
			"duration_ms", duration.Milliseconds(),
			"input_token", m.Stats.InputToken,
			"output_token", m.Stats.OutputToken,
			"input_cost", m.Stats.InputCost,
			"output_cost", m.Stats.OutputCost,
			"total_cost", m.Stats.InputCost + m.Stats.OutputCost,
			"attempts", len(attempts),
		}
		if success {
			log.Infow("relay.images.complete", fields...)
		} else {
			log.Warnw("relay.images.complete", fields...)
		}
	}

	m.saveLog(ctx, success, err, duration, attempts, channelID, channelName)
}

func (m *imagesRelayMetrics) saveLog(ctx context.Context, success bool, err error, duration time.Duration, attempts []model.ChannelAttempt, channelID int, channelName string) {
	actualModel := m.ActualModel
	if actualModel == "" {
		actualModel = m.RequestModel
	}

	relayLog := model.RelayLog{
		Time:             m.StartTime.Unix(),
		RequestModelName: m.RequestModel,
		ChannelName:      channelName,
		ChannelId:        channelID,
		ActualModelName:  actualModel,
		UseTime:          int(duration.Milliseconds()),
		Attempts:         attempts,
		TotalAttempts:    len(attempts),
		RequestContent:   m.RequestContent,
		ResponseContent:  m.ResponseContent,
	}

	if apiKey, getErr := op.APIKeyGet(m.APIKeyID, ctx); getErr == nil {
		relayLog.RequestAPIKeyName = apiKey.Name
	}

	// 首字时间
	if !m.FirstToken.IsZero() {
		relayLog.Ftut = int(m.FirstToken.Sub(m.StartTime).Milliseconds())
	}

	// Usage
	if m.Stats.InputToken > 0 || m.Stats.OutputToken > 0 {
		relayLog.InputTokens = int(m.Stats.InputToken)
		relayLog.OutputTokens = int(m.Stats.OutputToken)
		relayLog.Cost = m.Stats.InputCost + m.Stats.OutputCost
	}

	if err != nil {
		relayLog.Error = err.Error()
	}
	relayLog.Success = success

	if logErr := op.RelayLogAdd(ctx, relayLog); logErr != nil {
		log.Warnf("failed to save relay log: %v", logErr)
	}
}

func buildImagesRequestContentForLog(isMultipart bool, bc *bodycache.BodyCache, jsonPayload map[string]any) string {
	if isMultipart {
		// multipart 可能包含图片文件，避免落库
		return fmt.Sprintf(`{"content_type":"multipart/form-data","size_bytes":%d,"note":"multipart request content omitted for storage"}`, bc.Size())
	}
	if jsonPayload == nil {
		return ""
	}
	b, err := json.Marshal(jsonPayload)
	if err != nil {
		return ""
	}
	return truncateString(string(b), 8*1024)
}

func buildImagesResponseContentForLog(stream bool, upstreamCT string, usage *imagesUsage) string {
	if usage == nil {
		return ""
	}
	// 不记录 b64_json，仅记录 usage
	type respForLog struct {
		Stream      bool         `json:"stream"`
		ContentType string       `json:"content_type,omitempty"`
		Usage       *imagesUsage `json:"usage,omitempty"`
		Note        string       `json:"note,omitempty"`
	}
	obj := respForLog{
		Stream:      stream,
		ContentType: upstreamCT,
		Usage:       usage,
		Note:        "image data omitted for storage",
	}
	b, err := json.Marshal(obj)
	if err != nil {
		return ""
	}
	return string(b)
}

func truncateString(s string, max int) string {
	if max <= 0 {
		return ""
	}
	if len(s) <= max {
		return s
	}
	return s[:max] + "...(truncated)"
}

func parseJSONModelAndStream(bc *bodycache.BodyCache) (payload map[string]any, modelName string, stream bool, err error) {
	r, err := bc.NewReader()
	if err != nil {
		return nil, "", false, err
	}
	defer r.Close()

	body, err := io.ReadAll(r)
	if err != nil {
		return nil, "", false, err
	}
	if len(bytes.TrimSpace(body)) == 0 {
		return nil, "", false, errors.New("empty body")
	}

	var m map[string]any
	if err := json.Unmarshal(body, &m); err != nil {
		return nil, "", false, errors.New("invalid json")
	}

	rawModel, ok := m["model"]
	if !ok {
		return nil, "", false, errors.New("model is required")
	}
	modelStr, ok := rawModel.(string)
	if !ok || strings.TrimSpace(modelStr) == "" {
		return nil, "", false, errors.New("model is required")
	}

	stream = false
	if v, ok := m["stream"]; ok {
		switch vv := v.(type) {
		case bool:
			stream = vv
		case string:
			stream = strings.EqualFold(strings.TrimSpace(vv), "true")
		case float64:
			stream = vv != 0
		}
	}

	return m, strings.TrimSpace(modelStr), stream, nil
}

func parseMultipartModelAndStream(bc *bodycache.BodyCache, boundary string) (modelName string, stream bool, err error) {
	r, err := bc.NewReader()
	if err != nil {
		return "", false, err
	}
	defer r.Close()

	mr := multipart.NewReader(r, boundary)

	stream = false
	for {
		part, err := mr.NextPart()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return "", false, err
		}

		name := part.FormName()
		if name == "" {
			_, _ = io.Copy(io.Discard, part)
			_ = part.Close()
			continue
		}

		switch name {
		case "model":
			b, _ := io.ReadAll(io.LimitReader(part, 1024))
			modelName = strings.TrimSpace(string(b))
		case "stream":
			b, _ := io.ReadAll(io.LimitReader(part, 16))
			stream = strings.EqualFold(strings.TrimSpace(string(b)), "true")
		default:
			_, _ = io.Copy(io.Discard, part)
		}
		_ = part.Close()
	}

	if strings.TrimSpace(modelName) == "" {
		return "", false, errors.New("model is required")
	}
	return modelName, stream, nil
}

func imagesAttempt(
	ctx context.Context,
	endpoint string,
	c *gin.Context,
	bc *bodycache.BodyCache,
	isMultipart bool,
	boundary string,
	jsonPayload map[string]any,
	stream bool,
	channel *model.Channel,
	channelKey string,
	firstTokenTimeOutSec int,
	metrics *imagesRelayMetrics,
	actualModel string,
	hb *earlyHeartbeat,
) (statusCode int, written bool, usage *imagesUsage, upstreamCT string, retryAfter time.Duration, err error) {
	// 构建 URL（baseUrl.Path 后追加 endpoint）
	baseURL := channel.GetBaseUrl()
	parsedURL, err := url.Parse(strings.TrimSuffix(baseURL, "/"))
	if err != nil {
		return 0, false, nil, "", 0, fmt.Errorf("failed to parse base url: %w", err)
	}
	parsedURL.Path = parsedURL.Path + endpoint

	var bodyReader io.Reader
	var contentType string

	if isMultipart {
		pr, pw := io.Pipe()
		mw := multipart.NewWriter(pw)
		contentType = mw.FormDataContentType()
		bodyReader = pr

		go func() {
			src, err := bc.NewReader()
			if err != nil {
				_ = pw.CloseWithError(err)
				return
			}
			defer src.Close()

			if err := copyMultipartReplaceModel(src, boundary, mw, actualModel); err != nil {
				_ = pw.CloseWithError(err)
				return
			}
			// 先关闭 multipart.Writer 写入结束 boundary，再关闭 pipe writer
			if err := mw.Close(); err != nil {
				_ = pw.CloseWithError(err)
				return
			}
			_ = pw.Close()
		}()
	} else {
		// JSON：仅改写 model 字段，其余保持不变
		// 注意：每次尝试都重新 marshal 生成 body，确保可重试重建
		if jsonPayload == nil {
			return 0, false, nil, "", 0, errors.New("nil json payload")
		}
		jsonPayload["model"] = actualModel
		b, err := json.Marshal(jsonPayload)
		if err != nil {
			return 0, false, nil, "", 0, fmt.Errorf("failed to marshal json: %w", err)
		}
		bodyReader = bytes.NewReader(b)
		contentType = "application/json"
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "", bodyReader)
	if err != nil {
		return 0, false, nil, "", 0, fmt.Errorf("failed to create request: %w", err)
	}
	req.URL = parsedURL
	req.Method = http.MethodPost

	// Header 透传：复制下游 header，过滤 hop-by-hop 与鉴权相关
	copyHeadersToUpstream(req, c, channel, channelKey, contentType, stream)

	// 发送请求
	httpClient, err := helper.ChannelHTTPClientWithContext(ctx, channel)
	if err != nil {
		return 0, false, nil, "", 0, err
	}

	respUp, err := httpClient.Do(req)
	if err != nil {
		return 0, false, nil, "", 0, fmt.Errorf("failed to send request: %w", err)
	}
	defer respUp.Body.Close()

	upstreamCT = respUp.Header.Get("Content-Type")
	retryAfter = parseRetryAfter(respUp.Header.Get("Retry-After"))

	// stream=true：逐行解析 event/data/空行边界透传
	if stream {
		if respUp.StatusCode < 200 || respUp.StatusCode >= 300 {
			b, _ := io.ReadAll(io.LimitReader(respUp.Body, imagesUpstreamErrorBodyLimit))
			return respUp.StatusCode, false, nil, upstreamCT, retryAfter, newUpstreamHTTPError(respUp.StatusCode, b)
		}
		u, w, proxyErr := proxySSE(ctx, c, respUp, firstTokenTimeOutSec, metrics, hb)
		return imagesStatusForError(respUp.StatusCode, proxyErr), w, u, upstreamCT, retryAfter, proxyErr
	}

	// 非流式：2xx 透传，否则读取限长错误体用于错误信息与重试判定
	if respUp.StatusCode < 200 || respUp.StatusCode >= 300 {
		b, _ := io.ReadAll(io.LimitReader(respUp.Body, imagesUpstreamErrorBodyLimit))
		return respUp.StatusCode, false, nil, upstreamCT, retryAfter, newUpstreamHTTPError(respUp.StatusCode, b)
	}

	u, w, proxyErr := proxyNonStream(c, respUp)
	return imagesStatusForError(respUp.StatusCode, proxyErr), w, u, upstreamCT, retryAfter, proxyErr
}

func imagesStatusForError(upstreamStatus int, err error) int {
	if err == nil || isDownstreamWriteError(err) {
		return upstreamStatus
	}
	if terminalStatus := passthroughTerminalStatus(err); terminalStatus > 0 {
		return terminalStatus
	}
	if upstreamStatus >= 400 {
		return upstreamStatus
	}
	return http.StatusBadGateway
}

func copyHeadersToUpstream(req *http.Request, c *gin.Context, channel *model.Channel, channelKey string, contentType string, stream bool) {
	if req == nil {
		return
	}
	if c != nil && c.Request != nil {
		copySafeUpstreamHeaders(req.Header, c.Request.Header)
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	if stream {
		req.Header.Set("Accept", "text/event-stream")
	} else if req.Header.Get("Accept") == "" {
		req.Header.Set("Accept", "application/json")
	}
	applySelectedCredentialHeader(req.Header, outbound.OutboundTypeOpenAIChat, channelKey)

	// 防止 Go 默认 User-Agent 泄露到上游
	if req.Header.Get("User-Agent") == "" {
		req.Header.Set("User-Agent", "")
	}

	if channel != nil {
		applySafeChannelHeaders(req.Header, channel.CustomHeader)
		// CustomHeader is applied after defaults for ordinary metadata, but it
		// can never replace the selected channel credential.
		applySelectedCredentialHeader(req.Header, outbound.OutboundTypeOpenAIChat, channelKey)
	}
}

func copyMultipartReplaceModel(src io.Reader, boundary string, dst *multipart.Writer, newModel string) error {
	mr := multipart.NewReader(src, boundary)

	for {
		part, err := mr.NextPart()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}

		hdr := make(textproto.MIMEHeader, len(part.Header))
		for k, vv := range part.Header {
			cp := make([]string, len(vv))
			copy(cp, vv)
			hdr[k] = cp
		}

		pw, err := dst.CreatePart(hdr)
		if err != nil {
			_ = part.Close()
			return err
		}

		if part.FormName() == "model" && part.FileName() == "" {
			// 丢弃原值，写入替换后的 model（继续复制后续 part）
			_, _ = io.Copy(io.Discard, part)
			_, werr := io.WriteString(pw, newModel)
			_ = part.Close()
			if werr != nil {
				return werr
			}
			continue
		}

		_, err = io.Copy(pw, part)
		_ = part.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

// proxyNonStream 将上游非流式响应原样透传到下游，同时尽量提取 usage（避免解析巨大 b64_json）。
func proxyNonStream(c *gin.Context, respUp *http.Response) (*imagesUsage, bool, error) {
	ct := respUp.Header.Get("Content-Type")
	if ct == "" {
		ct = "application/json"
	}

	scanner := newUsageScanner()
	wrotePayload := false
	buf := make([]byte, 32*1024)
	for {
		n, rerr := respUp.Body.Read(buf)
		if n > 0 {
			chunk := buf[:n]
			scanner.Feed(chunk)
			if !wrotePayload {
				// Keep response metadata tentative until an actual body chunk is ready.
				// An empty/read-failed attempt may still fail over to another channel or
				// end as a JSON relay error.
				c.Header("Content-Type", ct)
				c.Status(respUp.StatusCode)
			}
			written, werr := c.Writer.Write(chunk)
			if werr != nil {
				return scanner.Usage(), written > 0, downstreamWriteError(werr)
			}
			if written != len(chunk) {
				return scanner.Usage(), written > 0, downstreamWriteError(io.ErrShortWrite)
			}
			wrotePayload = true
		}
		if rerr != nil {
			if errors.Is(rerr, io.EOF) {
				break
			}
			return scanner.Usage(), wrotePayload, fmt.Errorf("failed to read upstream image response: %w", rerr)
		}
	}

	if !wrotePayload {
		return scanner.Usage(), false, stream.ErrEmptyUpstreamStream
	}
	return scanner.Usage(), true, nil
}

// proxySSE 将上游 SSE 逐行解析 event/data/空行并透传到下游；首事件计为 FirstTokenTime；支持 FirstTokenTimeOut 切换。
func proxySSE(ctx context.Context, c *gin.Context, respUp *http.Response, firstTokenTimeOutSec int, metrics *imagesRelayMetrics, hb *earlyHeartbeat) (*imagesUsage, bool, error) {
	if ct := respUp.Header.Get("Content-Type"); ct != "" && !strings.Contains(strings.ToLower(ct), "text/event-stream") {
		return nil, false, errors.New("upstream returned non-SSE content-type for image stream")
	}

	// Stop the pre-stream heartbeat before the stream processor becomes the
	// sole owner of the downstream writer.
	if hb != nil {
		hb.Hand()
	}

	var firstTokenTimeout time.Duration
	if firstTokenTimeOutSec > 0 {
		firstTokenTimeout = time.Duration(firstTokenTimeOutSec) * time.Second
	}

	cfg := transformerModel.PassthroughConfig{
		TerminalEvents: map[string]struct{}{
			"image_generation.completed": {},
		},
		FailureEvents: map[string]struct{}{
			"image_generation.failed": {},
			"error":                   {},
		},
		IncompleteEvents: map[string]struct{}{
			"image_generation.incomplete": {},
		},
		CancelledEvents: map[string]struct{}{
			"image_generation.cancelled": {},
			"image_generation.canceled":  {},
		},
	}
	framer := newPassthroughSSETransform(cfg, false)
	completedScanner := newUsageScanner()
	observeUsage := func(result stream.StreamTransformResult) stream.StreamTransformResult {
		if result.Outcome == transformerModel.PassthroughTerminalOutcomeCompleted && len(result.Output) > 0 {
			// Only the terminal block can contain usage. Scanning it avoids retaining
			// or decoding potentially large base64 image payloads.
			completedScanner.Feed(result.Output)
		}
		return result
	}

	processor := stream.NewStreamProcessor(stream.StreamConfig{
		Source: stream.NewRawSource(respUp.Body, 32*1024),
		TransformWithOutcome: func(transformCtx context.Context, data []byte, payloadWritten bool) stream.StreamTransformResult {
			return observeUsage(framer.transform(transformCtx, data, payloadWritten))
		},
		FinalizeWithOutcome: func(finalizeCtx context.Context) stream.StreamTransformResult {
			result := observeUsage(framer.finalize(finalizeCtx, framer.seenPayload))
			if result.Outcome == transformerModel.PassthroughTerminalOutcomeNone && result.Err == nil && framer.seenPayload {
				result.Err = fmt.Errorf("%w: image stream ended without a terminal event", transformerModel.ErrIncompleteUpstreamStream)
			}
			return result
		},
		Writer:            c.Writer,
		Context:           ctx,
		FirstTokenTimeout: firstTokenTimeout,
		HeartbeatInterval: streamHeartbeatInterval(),
		MaxEventSize:      maxSSEEventSize,
		OnFirstToken: func() {
			metrics.SetFirstTokenTime(time.Now())
		},
	})

	err := processor.Run()
	return completedScanner.Usage(), processor.PayloadWritten(), err
}

type usageScanner struct {
	matchIdx       int
	matchedKey     bool
	waitForObject  bool
	collecting     bool
	braceDepth     int
	inString       bool
	escape         bool
	buf            bytes.Buffer
	usage          *imagesUsage
	done           bool
	maxCollectSize int
}

func newUsageScanner() *usageScanner {
	return &usageScanner{maxCollectSize: 64 * 1024}
}

// Feed 逐字节扫描输入，定位 usage 键并仅解析其对象值。
// 键、冒号、对象之间允许 JSON 空白，状态可跨任意 transport chunk 保留。
func (s *usageScanner) Feed(p []byte) {
	if s.done || len(p) == 0 {
		return
	}
	const key = `"usage"`

	for _, b := range p {
		if s.done {
			return
		}

		if s.collecting {
			if s.buf.Len() >= s.maxCollectSize {
				s.collecting = false
				s.done = true
				return
			}
			s.buf.WriteByte(b)

			if s.inString {
				if s.escape {
					s.escape = false
				} else if b == '\\' {
					s.escape = true
				} else if b == '"' {
					s.inString = false
				}
				continue
			}

			if b == '"' {
				s.inString = true
				continue
			}

			switch b {
			case '{':
				s.braceDepth++
			case '}':
				s.braceDepth--
				if s.braceDepth == 0 {
					var usage imagesUsage
					if err := json.Unmarshal(s.buf.Bytes(), &usage); err == nil {
						s.usage = &usage
					}
					s.done = true
					s.collecting = false
					return
				}
			}
			continue
		}

		if s.waitForObject {
			if b == '{' {
				s.collecting = true
				s.braceDepth = 1
				s.buf.Reset()
				s.buf.WriteByte('{')
				s.inString = false
				s.escape = false
				s.waitForObject = false
				continue
			}
			if b == ' ' || b == '\t' || b == '\n' || b == '\r' {
				continue
			}
			s.waitForObject = false
			continue
		}

		if s.matchedKey {
			if b == ':' {
				s.matchedKey = false
				s.waitForObject = true
				continue
			}
			if b == ' ' || b == '\t' || b == '\n' || b == '\r' {
				continue
			}
			s.matchedKey = false
		}

		if b == key[s.matchIdx] {
			s.matchIdx++
			if s.matchIdx == len(key) {
				s.matchIdx = 0
				s.matchedKey = true
			}
			continue
		}
		if b == key[0] {
			s.matchIdx = 1
		} else {
			s.matchIdx = 0
		}
	}
}

func (s *usageScanner) Usage() *imagesUsage {
	return s.usage
}
