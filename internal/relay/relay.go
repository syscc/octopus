package relay

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/bestruirui/octopus/internal/helper"
	dbmodel "github.com/bestruirui/octopus/internal/model"
	"github.com/bestruirui/octopus/internal/op"
	"github.com/bestruirui/octopus/internal/outlierwindow"
	"github.com/bestruirui/octopus/internal/relay/balancer"
	"github.com/bestruirui/octopus/internal/relay/stream"
	"github.com/bestruirui/octopus/internal/server/resp"
	"github.com/bestruirui/octopus/internal/transformer/inbound"
	"github.com/bestruirui/octopus/internal/transformer/model"
	"github.com/bestruirui/octopus/internal/transformer/outbound"
	openaiOutbound "github.com/bestruirui/octopus/internal/transformer/outbound/openai"
	"github.com/bestruirui/octopus/internal/utils/httpbody"
	"github.com/bestruirui/octopus/internal/utils/log"
	"github.com/gin-gonic/gin"
	"github.com/tmaxmax/go-sse"
)

type streamHeartbeatWriter interface {
	Write([]byte) (int, error)
	Flush()
}

func streamHeartbeatInterval() time.Duration {
	interval, err := op.SettingGetInt(dbmodel.SettingKeySSEHeartbeatInterval)
	if err != nil || interval <= 0 {
		return 0
	}
	return time.Duration(interval) * time.Second
}

func newStreamHeartbeatTicker() (*time.Ticker, <-chan time.Time) {
	interval := streamHeartbeatInterval()
	if interval <= 0 {
		return nil, nil
	}
	ticker := time.NewTicker(interval)
	return ticker, ticker.C
}

func writeSSEHeartbeat(writer streamHeartbeatWriter) error {
	if _, err := writer.Write([]byte(":\n\n")); err != nil {
		return err
	}
	writer.Flush()
	return nil
}

func Handler(inboundType inbound.InboundType, c *gin.Context) {
	// 解析请求
	rawBody, internalRequest, inAdapter, err := parseRequest(inboundType, c)
	if err != nil {
		return
	}
	supportedModels := c.GetString("supported_models")
	if supportedModels != "" {
		supportedModelsArray := strings.Split(supportedModels, ",")
		if !slices.Contains(supportedModelsArray, internalRequest.Model) {
			resp.ErrorWithCode(c, http.StatusBadRequest, CodeRelayModelNotSupported, "model not supported")
			return
		}
	}

	requestModel := internalRequest.Model
	apiKeyID := c.GetInt("api_key_id")

	// 获取通道分组
	group, err := op.GroupGetEnabledMap(requestModel, c.Request.Context())
	if err != nil {
		resp.ErrorWithCode(c, http.StatusNotFound, CodeRelayModelNotFound, "model not found")
		return
	}

	// === HTTP Replay 机制 ===
	// 当 HTTP 请求携带 previous_response_id 时，尝试从本地加载上一次成功的 replay 状态，
	// 优先路由到同一渠道/key，并将请求转为自包含形式（合并历史，移除 previous_response_id）。
	var responsesReplayState *wsConversationState
	var responsesReplayTurnRequest *model.InternalLLMRequest
	transportRawBody := rawBody
	if inboundType == inbound.InboundTypeOpenAIResponse && internalRequest.RawAPIFormat == model.APIFormatOpenAIResponse {
		if prevID := internalRequest.OpenAIPreviousResponseID(); prevID != "" {
			responsesReplayState = resolveResponsesReplayState(apiKeyID, group.ID, requestModel, internalRequest)
			if responsesReplayState != nil {
				log.Debugf("loaded HTTP replay state (apikey=%d, group=%d, model=%s, previous_response_id=%s, channel=%d, key=%d)",
					apiKeyID, group.ID, requestModel, prevID, responsesReplayState.ChannelID, responsesReplayState.ChannelKeyID)
				turnRequest := cloneInternalRequest(internalRequest)
				chatReplay := responsesReplayState.LastOutboundTypeSet && responsesReplayState.LastOutboundType == outbound.OutboundTypeOpenAIChat
				var replayed *model.InternalLLMRequest
				if chatReplay {
					replayed = responsesReplayState.BuildChatReplayRequest(internalRequest)
				} else {
					replayed = responsesReplayState.BuildReplayRequest(internalRequest)
				}
				if replayed != nil {
					responsesReplayTurnRequest = turnRequest
					internalRequest = replayed
					// Local replay must use the normalized request. Reusing the original
					// raw body would forward the local previous_response_id upstream.
					transportRawBody = nil
					log.Debugf("HTTP replay request transformed (apikey=%d, removed previous_response_id, merged history)", apiKeyID)
				} else {
					log.Warnf("HTTP replay history merge failed (apikey=%d, group=%d, model=%s, previous_response_id=%s), refusing to forward local response id",
						apiKeyID, group.ID, requestModel, prevID)
					resp.ErrorWithCode(c, http.StatusConflict, CodeRelayContinuationReplayFailed, "本地 previous_response_id 无法安全重放，请重新开启对话")
					return
				}
			} else {
				log.Debugf("no HTTP replay state found (apikey=%d, group=%d, model=%s, previous_response_id=%s)",
					apiKeyID, group.ID, requestModel, prevID)
			}
		}
	}

	// 创建迭代器（策略排序 + 粘性优先）
	// 如果有 replay state，注入为 sticky 偏好
	var preferredSticky *balancer.SessionEntry
	if responsesReplayState != nil {
		preferredSticky = responsesReplayStateToSticky(responsesReplayState)
		if preferredSticky != nil {
			log.Debugf("HTTP replay sticky routing preference (channel=%d, key=%d)", preferredSticky.ChannelID, preferredSticky.ChannelKeyID)
		}
	}
	iter := balancer.NewIteratorWithPreference(group, apiKeyID, requestModel, preferredSticky)
	if iter.Len() == 0 {
		resp.ErrorWithCode(c, http.StatusServiceUnavailable, CodeRelayNoAvailableChannel, "no available channel")
		return
	}

	// Weighted mode must preserve its sampled order; a second protocol sort would
	// override the user-configured provider weights. Other modes retain protocol preference.
	// Chat fallback replay 不再改变协议偏好：每个候选都先尝试客户端原始协议。
	applyProtocolPreferenceForMode(group.Mode, internalRequest, iter, c.Request.Context())

	// === 早期心跳 ===
	// 在所有 forward / 重试 / 退避之前启动早期心跳协程，覆盖前置阶段（连接慢、failover、退避叠加）
	// 期间向客户端发 SSE 注释字节，避免被 Cloudflare 在 120s 零字节阈值上判 524。
	// 仅对流式请求生效；非流式无法发送 SSE 注释（破坏 application/json 协议），
	// 不施加任何本地超时——上游慢响应应让其自然完成或由上游/CF 自身处理。
	isStream := internalRequest.Stream != nil && *internalRequest.Stream
	hb := startEarlyHeartbeat(c, isStream)
	defer func() { hb.Stop() }()

	// 初始化 Metrics
	metrics := NewRelayMetrics(apiKeyID, requestModel, rawBody, internalRequest)
	// 如果触发了 HTTP replay，记录 ws_mode=replay 和 ws_recovery=replay
	if responsesReplayState != nil {
		metrics.SetWSMode(dbmodel.RelayLogWSModeReplay)
		metrics.SetWSRecovery(dbmodel.RelayLogWSRecoveryReplay)
	}
	responsesPassthroughRequired := requiresNativeResponsesUpstream(internalRequest)
	var nativeResponses nativeResponsesAvailability

	// 请求级上下文
	req := &relayRequest{
		c:               c,
		inAdapter:       inAdapter,
		internalRequest: internalRequest,
		metrics:         metrics,
		apiKeyID:        apiKeyID,
		requestModel:    requestModel,
		groupID:         group.ID,
		groupSessionTTL: group.SessionKeepTime,
		iter:            iter,
		rawBody:         transportRawBody,
		heartbeat:       hb,
	}
	// Every inbound adapter accumulates response/stream state. Give every real
	// network attempt a fresh instance; request-derived state is restored through
	// InboundResponseInitializer.
	switch inboundType {
	case inbound.InboundTypeOpenAIChat, inbound.InboundTypeOpenAIResponse, inbound.InboundTypeOpenAIEmbedding, inbound.InboundTypeAnthropic:
		req.newInboundAdapter = func() model.Inbound { return inbound.Get(inboundType) }
	}

	var lastErr error
	var lastResult attemptResult

	// 同通道重试次数：启用时使用配置值，否则 1 次（不重试）
	maxSameChannelRetries := 1
	if group.RetryEnabled {
		maxSameChannelRetries = group.MaxRetries
		if maxSameChannelRetries <= 0 {
			maxSameChannelRetries = 3
		}
	}

	for iter.Next() {
		if hb.NeedsRestart() {
			hb = startEarlyHeartbeat(c, isStream)
			req.heartbeat = hb
		}
		select {
		case <-c.Request.Context().Done():
			log.Debugf("request context canceled, stopping retry")
			metrics.SaveWithChannelStats(c.Request.Context(), false, context.Canceled, iter.Attempts(), false)
			return
		default:
		}

		item := iter.Item()

		// 获取通道
		channel, err := op.ChannelGet(item.ChannelID, c.Request.Context())
		if err != nil {
			log.Warnf("failed to get channel %d: %v", item.ChannelID, err)
			iter.Skip(item.ChannelID, 0, fmt.Sprintf("channel_%d", item.ChannelID), fmt.Sprintf("channel not found: %v", err))
			lastErr = err
			if responsesPassthroughRequired {
				nativeResponses.markUnavailable()
			}
			continue
		}
		if !channel.Enabled {
			iter.Skip(channel.ID, 0, channel.Name, "channel disabled")
			if responsesPassthroughRequired && isOpenAIProtocolChannel(channel.Type) {
				nativeResponses.markCandidate(channel.Type)
				nativeResponses.markUnavailable()
			}
			continue
		}
		if responsesPassthroughRequired {
			nativeResponses.markCandidate(channel.Type)
			if !isOpenAIProtocolChannel(channel.Type) {
				iter.Skip(channel.ID, 0, channel.Name, "native openai responses upstream required")
				continue
			}
		}

		outboundType, protocolCompatible := outboundTypeForChannel(internalRequest, channel)
		if !protocolCompatible {
			iter.Skip(channel.ID, 0, channel.Name, "channel protocol capability is incompatible with request")
			if responsesPassthroughRequired && isOpenAIProtocolChannel(channel.Type) {
				nativeResponses.markEndpointUnsupported()
			}
			continue
		}
		outAdapter := outbound.Get(outboundType)
		if outAdapter == nil {
			iter.Skip(channel.ID, 0, channel.Name, fmt.Sprintf("unsupported channel type: %d", channel.Type))
			continue
		}

		// 类型兼容性检查
		if internalRequest.IsEmbeddingRequest() && !outbound.IsEmbeddingChannelType(channel.Type) {
			iter.Skip(channel.ID, 0, channel.Name, "channel type not compatible with embedding request")
			continue
		}
		if internalRequest.IsChatRequest() && !outbound.IsChatChannelType(channel.Type) {
			iter.Skip(channel.ID, 0, channel.Name, "channel type not compatible with chat request")
			continue
		}

		// 设置实际模型
		internalRequest.Model = item.ModelName

		log.Debugf("request model %s, mode: %d, forwarding to channel: %s model: %s (attempt %d/%d, sticky=%t)",
			requestModel, group.Mode, channel.Name, item.ModelName,
			iter.Index()+1, iter.Len(), iter.IsSticky())

		selectOpts := dbmodel.ChannelKeySelectOptions{
			ExcludeKeyIDs:  make(map[int]struct{}),
			PreferredKeyID: iter.StickyKeyID(),
		}
		var usedKey dbmodel.ChannelKey
		for {
			usedKey = channel.GetChannelKey(selectOpts)
			if usedKey.ChannelKey == "" {
				break
			}
			if !iter.SkipCircuitBreak(channel.ID, usedKey.ID, channel.Name) {
				break
			}
			selectOpts.ExcludeKeyIDs[usedKey.ID] = struct{}{}
			usedKey = dbmodel.ChannelKey{}
		}
		if usedKey.ChannelKey == "" {
			if len(selectOpts.ExcludeKeyIDs) == 0 {
				iter.Skip(channel.ID, 0, channel.Name, "no available key")
			}
			if responsesPassthroughRequired && isOpenAIProtocolChannel(channel.Type) {
				nativeResponses.markUnavailable()
			}
			continue
		}

		// 同通道重试循环
		var result attemptResult
		for retryNum := 0; retryNum < maxSameChannelRetries; retryNum++ {
			// 重试前等待退避
			if retryNum > 0 {
				delay := computeBackoff(retryNum, result.RetryAfter)
				log.Infof("same-channel retry %d/%d for %s, waiting %v",
					retryNum, maxSameChannelRetries, channel.Name, delay)
				select {
				case <-c.Request.Context().Done():
					log.Debugf("request context canceled during retry backoff")
					metrics.SaveWithChannelStats(c.Request.Context(), false, context.Canceled, iter.Attempts(), false)
					return
				case <-time.After(delay):
				}

				// 每轮重试从（可能已被上一轮学习更新的）通道能力重选出站协议：
				// 刚学习到 unsupported 的端点不能在同渠道重试中再次被请求；
				// 这里只影响本次尝试的协议选择，不改变迭代器（weighted/sticky）顺序。
				recomputedOutboundType, recomputedCompatible := outboundTypeForChannel(internalRequest, channel)
				if !recomputedCompatible {
					log.Debugf("same-channel retry for %s skipped: protocol capability incompatible after learning", channel.Name)
					break
				}
				outboundType = recomputedOutboundType
				// 重建 outAdapter 以重置流式状态（toolIndex, toolCalls 等）
				outAdapter = outbound.Get(outboundType)
			}

			// 构造尝试级上下文
			ra := &relayAttempt{
				relayRequest:          req,
				outAdapter:            outAdapter,
				activeOutboundType:    outboundType,
				activeOutboundTypeSet: true,
				channel:               channel,
				usedKey:               usedKey,
				firstTokenTimeOutSec:  group.FirstTokenTimeOut,
			}

			result = ra.attempt()
			if result.Success || result.Written || result.Canceled || isDownstreamWriteFailure(result) || result.TerminalOutcome != model.PassthroughTerminalOutcomeNone || result.ResetConversation || result.FirstTokenTimeout || !isRetryableStatus(result.StatusCode) {
				break
			}
		}

		if responsesPassthroughRequired {
			nativeResponses.markAttempt(result, shouldTryProtocolFallbackForAttempt(internalRequest, channel, result.OutboundType, result.StatusCode, result.protocolError()))
		}

		protocolUnavailable := isOpenAIInbound(inboundType) &&
			shouldTryProtocolFallbackForAttempt(internalRequest, channel, result.OutboundType, result.StatusCode, result.protocolError())

		// A truncated stream cannot be retried after payload is visible, but it is
		// still a hard upstream failure for future routing decisions.
		recordIncompleteUpstreamFailure(channel.ID, usedKey.ID, internalRequest.Model, result)
		recordWrittenStructuredUpstreamFailure(channel.ID, usedKey.ID, internalRequest.Model, group.RetryEnabled, result)

		// Capability classification and channel health are independent. A request
		// that still failed after allowed fallback must count toward circuit and
		// outlier health even when the error also proves an endpoint unsupported.
		// Dedicated helpers above own incomplete and typed terminal failures;
		// the final generic pass excludes those observations to avoid duplicates.
		failureKind, failed := recordFinalAttemptFailure(
			channel.ID, usedKey.ID, internalRequest.Model, group.RetryEnabled, false, result,
		)
		if failed && !protocolUnavailable && failureKind == balancer.FailureHard && usedDeclaredChannelProtocol(channel, result) {
			// Only a failure on the declared protocol is evidence for route
			// learning. Errors from a downstream-protocol probe are not.
			maybeLearnManagedRoute(c.Request.Context(), channel.ID, internalRequest.Model, inboundType, result.StatusCode, result.protocolError())
		}

		if result.Success {
			outlierwindow.Report(channel.ID, true, result.StatusCode, time.Now())

			// === HTTP Replay 状态保存 ===
			// Every successful Responses ingress turn receives a replay state. The
			// actual outbound protocol decides whether the next turn uses exact
			// Responses replay or a complete Chat transcript.
			if inboundType == inbound.InboundTypeOpenAIResponse &&
				req.internalRequest.RawAPIFormat == model.APIFormatOpenAIResponse {
				internalResponse := metrics.InternalResponse
				if internalResponse == nil {
					// 读取本次 attempt 实际使用的 adapter；解析时的旧 adapter 可能
					// 已被后续失败 attempt 的状态污染。
					saveAdapter := result.inboundAdapter
					if saveAdapter == nil {
						saveAdapter = inAdapter
					}
					var err error
					internalResponse, err = saveAdapter.GetInternalResponse(c.Request.Context())
					if err != nil {
						log.Debugf("failed to get internal response for replay state save: %v", err)
					}
				}
				if internalResponse != nil {
					var newState *wsConversationState
					if responsesReplayState != nil {
						newState = cloneWSConversationState(responsesReplayState)
					}
					if newState == nil {
						newState = &wsConversationState{RequestModel: requestModel}
					}
					newState.ChannelID = channel.ID
					newState.ChannelKeyID = usedKey.ID
					newState.LastOutboundType = result.OutboundType
					newState.LastOutboundTypeSet = true
					turnRequest := req.internalRequest
					if responsesReplayTurnRequest != nil {
						turnRequest = responsesReplayTurnRequest
					}
					newState.ApplySuccessfulTurn(turnRequest, internalResponse)
					if newState.LastResponseID != "" {
						ttl := wsConversationStateTTL(group.SessionKeepTime)
						storeResponsesReplayState(apiKeyID, group.ID, requestModel, newState, ttl)
						log.Debugf("saved HTTP replay state (apikey=%d, group=%d, model=%s, response_id=%s, channel=%d, key=%d, ttl=%v, is_replay=%t)",
							apiKeyID, group.ID, requestModel, newState.LastResponseID, channel.ID, usedKey.ID, ttl, responsesReplayState != nil)
					}
				}
			}

			metrics.SaveWithChannelStats(c.Request.Context(), true, nil, iter.Attempts(), false)
			return
		}
		if isDownstreamWriteFailure(result) {
			// A failed client write ends this request even when the writer accepted
			// zero bytes. Retrying another upstream cannot repair the downstream and
			// risks duplicate work or side effects.
			metrics.SaveWithChannelStats(c.Request.Context(), false, result.Err, iter.Attempts(), false)
			return
		}
		if result.Canceled {
			metrics.SaveWithChannelStats(c.Request.Context(), false, result.Err, iter.Attempts(), false)
			return
		}
		// Once a streaming payload is visible, the downstream protocol is already
		// committed. Do not append a JSON reset/error response to the same stream.
		if result.Written {
			metrics.SaveWithChannelStats(c.Request.Context(), false, result.Err, iter.Attempts(), false)
			return
		}
		if result.ResetConversation {
			metrics.SaveWithChannelStats(c.Request.Context(), false, result.Err, iter.Attempts(), false)
			if publicErr, ok := classifyWSPublicError(result.Err, result.StatusCode); ok {
				hb.FlushOrError(c, publicErr.Status, publicErr.Message)
			} else {
				hb.FlushOrError(c, result.StatusCode, result.Err.Error())
			}
			return
		}
		lastErr = result.Err
		lastResult = result
	}

	// 所有候选通道均失败
	if responsesPassthroughRequired && nativeResponses.shouldReportCapabilityError() {
		err := protocolFallbackError(internalRequest)
		metrics.SaveWithChannelStats(c.Request.Context(), false, err, iter.Attempts(), false)
		hb.FlushOrError(c, http.StatusBadRequest, "当前请求包含 OpenAI Responses 原生能力，仅支持 OpenAI Responses 通道直通")
		return
	}
	if responsesPassthroughRequired && nativeResponses.unavailableBeforeAttempt() {
		err := fmt.Errorf("native openai responses upstream temporarily unavailable")
		metrics.SaveWithChannelStats(c.Request.Context(), false, err, iter.Attempts(), false)
		hb.FlushOrError(c, http.StatusServiceUnavailable, "OpenAI Responses 上游暂时不可用，请稍后重试")
		return
	}
	metrics.SaveWithChannelStats(c.Request.Context(), false, lastErr, iter.Attempts(), false)

	// 透传 429/503 状态码和 Retry-After 头，让客户端 SDK 的重试机制接管
	if isPassthroughStatus(lastResult.StatusCode) {
		if lastResult.RetryAfter > 0 {
			c.Header("Retry-After", fmt.Sprintf("%d", int(lastResult.RetryAfter.Seconds())))
		}
		hb.FlushOrError(c, lastResult.StatusCode, "channel failed")
		return
	}
	if lastResult.StatusCode > 0 {
		hb.FlushOrError(c, lastResult.StatusCode, "channel failed")
		return
	}
	hb.FlushOrError(c, http.StatusBadGateway, "channel failed")
}

func circuitFailureKind(retryEnabled bool, statusCode int) balancer.FailureKind {
	if retryEnabled && isPassthroughStatus(statusCode) {
		return balancer.FailureSoftRateLimit
	}
	return balancer.FailureHard
}

func isDownstreamWriteError(err error) bool {
	return errors.Is(err, stream.ErrDownstreamWriteFailed)
}

func writeDownstreamResponse(c *gin.Context, statusCode int, contentType string, body []byte) (bool, error) {
	c.Header("Content-Type", contentType)
	if len(body) > 0 {
		c.Header("Content-Length", strconv.Itoa(len(body)))
	}
	c.Status(statusCode)
	n, err := c.Writer.Write(body)
	written := n > 0
	if err != nil {
		return written, fmt.Errorf("write downstream response: %w", stream.WrapDownstreamWriteError(err))
	}
	if n != len(body) {
		return written, fmt.Errorf("write downstream response: %w", stream.WrapDownstreamWriteError(io.ErrShortWrite))
	}
	return written, nil
}

func isDownstreamWriteFailure(result attemptResult) bool {
	return isDownstreamWriteError(result.Err) || isDownstreamWriteError(result.UpstreamErr)
}

func responseStatusForError(upstreamStatus int, err error) int {
	if isDownstreamWriteError(err) {
		return upstreamStatus
	}
	return passthroughTerminalStatus(err)
}

func failurePassthroughOutcome(result attemptResult) model.PassthroughTerminalOutcome {
	if result.TerminalOutcome != model.PassthroughTerminalOutcomeNone {
		return result.TerminalOutcome
	}
	return passthroughOutcomeFromError(result.UpstreamErr)
}

func isFailurePassthroughOutcome(outcome model.PassthroughTerminalOutcome) bool {
	return outcome == model.PassthroughTerminalOutcomeFailed ||
		outcome == model.PassthroughTerminalOutcomeIncomplete ||
		outcome == model.PassthroughTerminalOutcomeCancelled
}

func hasStructuredUpstreamFailure(err error) bool {
	var structured *wsUpstreamEventError
	if errors.As(err, &structured) && structured != nil {
		return true
	}
	var responseErr *model.ResponseError
	return errors.As(err, &responseErr) && responseErr != nil
}

func hasIndependentUpstreamFailure(result attemptResult) bool {
	return errors.Is(result.UpstreamErr, model.ErrIncompleteUpstreamStream) ||
		isFailurePassthroughOutcome(failurePassthroughOutcome(result)) ||
		hasStructuredUpstreamFailure(result.UpstreamErr)
}

func isHealthNeutralDownstreamWriteFailure(result attemptResult) bool {
	return isDownstreamWriteFailure(result) && !hasIndependentUpstreamFailure(result)
}

func recordFinalAttemptFailure(channelID, keyID int, modelName string, retryEnabled bool, forceHard bool, result attemptResult) (balancer.FailureKind, bool) {
	if result.Success || result.Canceled || result.ResetConversation {
		return balancer.FailureHard, false
	}
	if isDownstreamWriteFailure(result) {
		// Delivery failure is terminal for control flow. Any independent typed
		// upstream evidence has already been recorded by a dedicated helper.
		return balancer.FailureHard, false
	}
	if errors.Is(result.UpstreamErr, model.ErrIncompleteUpstreamStream) {
		// recordIncompleteUpstreamFailure owns this hard-health observation;
		// avoid counting a write/finalization error a second time.
		return balancer.FailureHard, false
	}
	if hasStructuredUpstreamFailure(result.UpstreamErr) {
		// recordWrittenStructuredUpstreamFailure already recorded this typed
		// error. Keep it visible to route-learning callers without a second sample.
		return circuitFailureKind(retryEnabled, result.StatusCode), true
	}
	if isFailurePassthroughOutcome(failurePassthroughOutcome(result)) {
		// Failed/incomplete/cancelled protocol terminals are owned by the typed
		// helper whether or not the terminal bytes reached the downstream writer.
		return balancer.FailureHard, false
	}
	failureKind := circuitFailureKind(retryEnabled, result.StatusCode)
	if forceHard {
		failureKind = balancer.FailureHard
	}
	balancer.RecordFailure(channelID, keyID, modelName, failureKind)
	outlierwindow.Report(channelID, false, result.StatusCode, time.Now())
	return failureKind, true
}

// recordIncompleteUpstreamFailure accounts for a stream that delivered partial
// output and then lost its upstream terminal. Payload visibility prevents
// retry/failover, but the channel must still receive hard-failure and outlier
// accounting so repeated truncation does not look healthy.
func recordIncompleteUpstreamFailure(channelID, keyID int, modelName string, result attemptResult) {
	if result.Canceled || result.Success || result.ResetConversation || !errors.Is(result.UpstreamErr, model.ErrIncompleteUpstreamStream) {
		return
	}
	// The typed incomplete sentinel is independent upstream evidence. Preserve
	// exactly one health failure even when delivery of its synthetic terminal
	// later fails downstream; the generic and structured helpers skip that join.
	balancer.RecordFailure(channelID, keyID, modelName, balancer.FailureHard)
	outlierwindow.Report(channelID, false, result.StatusCode, time.Now())
}

func recordWrittenStructuredUpstreamFailure(channelID, keyID int, modelName string, retryEnabled bool, result attemptResult) {
	if result.Canceled || result.Success || result.ResetConversation {
		return
	}
	// The incomplete helper owns joined incomplete/structured evidence.
	if errors.Is(result.UpstreamErr, model.ErrIncompleteUpstreamStream) {
		return
	}
	if isFailurePassthroughOutcome(failurePassthroughOutcome(result)) {
		balancer.RecordFailure(channelID, keyID, modelName, balancer.FailureHard)
		outlierwindow.Report(channelID, false, result.StatusCode, time.Now())
		return
	}
	if !hasStructuredUpstreamFailure(result.UpstreamErr) {
		return
	}
	failureKind := circuitFailureKind(retryEnabled, result.StatusCode)
	balancer.RecordFailure(channelID, keyID, modelName, failureKind)
	outlierwindow.Report(channelID, false, result.StatusCode, time.Now())
}

func usedDeclaredChannelProtocol(channel *dbmodel.Channel, result attemptResult) bool {
	return channel != nil && result.OutboundType == channel.Type
}

func (ra *relayAttempt) recordOpenAIProtocolCapability(protocol outbound.OutboundType, capability dbmodel.OpenAIProtocolCapability) {
	if ra == nil || ra.channel == nil || !isOpenAIProtocolChannel(ra.channel.Type) ||
		ra.channel.OpenAIProtocolMode.Normalize() != dbmodel.OpenAIProtocolModeAuto {
		return
	}
	ctx := ra.requestContext()
	if ctx == nil {
		ctx = context.Background()
	} else {
		ctx = context.WithoutCancel(ctx)
	}
	persistCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if err := op.ChannelRecordOpenAIProtocolCapability(ra.channel.ID, protocol, capability, persistCtx); err != nil {
		log.Warnf("failed to persist openai protocol capability for channel %d: %v", ra.channel.ID, err)
		return
	}
	ra.channel.SetOpenAIProtocolCapability(protocol, capability)
}

// attempt 统一管理一次通道尝试的完整生命周期
func (ra *relayAttempt) attempt() attemptResult {
	span := ra.iter.StartAttempt(ra.channel.ID, ra.usedKey.ID, ra.channel.Name)

	// 转发请求
	statusCode, fwdErr := ra.forward()
	terminalOutcome := ra.passthroughOutcome
	if fwdErr == nil && terminalOutcome != model.PassthroughTerminalOutcomeNone &&
		terminalOutcome != model.PassthroughTerminalOutcomeCompleted {
		// A protocol adapter may report a non-successful terminal without a
		// transport error. Keep that semantic outcome authoritative instead of
		// allowing the ordinary success path to update sticky/replay state.
		fwdErr = stream.NewPassthroughTerminalError(terminalOutcome)
	}

	// 更新 channel key 状态
	ra.usedKey.StatusCode = statusCode
	ra.usedKey.LastUseTimeStamp = time.Now().Unix()

	if fwdErr == nil {
		// ====== 成功 ======
		ra.recordOpenAIProtocolCapability(ra.currentOutboundType(), dbmodel.OpenAIProtocolCapabilitySupported)
		// Passthrough handlers collect response at stream end via PassthroughConfig.CollectMetrics
		ra.collectResponse()
		ra.usedKey.TotalCost += ra.metrics.Stats.InputCost + ra.metrics.Stats.OutputCost
		op.ChannelKeyUpdate(ra.usedKey)

		span.End(dbmodel.AttemptSuccess, statusCode, "")

		// Channel 维度统计
		op.StatsChannelUpdate(ra.channel.ID, dbmodel.StatsMetrics{
			WaitTime:       span.Duration().Milliseconds(),
			RequestSuccess: 1,
		})

		// 熔断器：记录成功
		balancer.RecordSuccess(ra.channel.ID, ra.usedKey.ID, ra.internalRequest.Model)
		// 会话保持：更新粘性记录
		balancer.SetSticky(ra.apiKeyID, ra.requestModel, ra.channel.ID, ra.usedKey.ID)

		ra.syncWSTransportMetrics()
		return attemptResult{Success: true, StatusCode: statusCode, OutboundType: ra.currentOutboundType(), TerminalOutcome: terminalOutcome, inboundAdapter: ra.inAdapter}
	}

	// ====== 失败 ======
	if isClientCancellation(ra.requestContext(), fwdErr) {
		written := ra.streamPayloadWritten.Load()
		if written {
			ra.collectResponse()
		}
		op.ChannelKeyUpdate(ra.usedKey)
		span.End(dbmodel.AttemptFailed, statusCode, fwdErr.Error())
		ra.syncWSTransportMetrics()
		return attemptResult{
			Success:         false,
			Written:         written,
			Canceled:        true,
			Err:             fwdErr,
			UpstreamErr:     fwdErr,
			StatusCode:      statusCode,
			OutboundType:    ra.currentOutboundType(),
			TerminalOutcome: terminalOutcome,
			inboundAdapter:  ra.inAdapter,
		}
	}

	op.ChannelKeyUpdate(ra.usedKey)
	span.End(dbmodel.AttemptFailed, statusCode, fwdErr.Error())

	// Channel 维度统计
	if !isHealthNeutralDownstreamWriteFailure(attemptResult{Err: fwdErr, UpstreamErr: fwdErr}) {
		op.StatsChannelUpdate(ra.channel.ID, dbmodel.StatsMetrics{
			WaitTime:      span.Duration().Milliseconds(),
			RequestFailed: 1,
		})
	}

	// 注意：熔断器记录已移至 Handler() 的同通道重试循环外，
	// 避免重试期间过早触发熔断

	written := ra.streamPayloadWritten.Load()
	if written {
		ra.collectResponse()
	}
	firstTokenTimeout := isFirstTokenTimeout(nil, fwdErr)
	ra.syncWSTransportMetrics()
	return attemptResult{
		Success:           false,
		Written:           written,
		ResetConversation: statusCode == http.StatusConflict && needsConversationRestart(upstreamClassificationMessage(fwdErr)),
		FirstTokenTimeout: firstTokenTimeout,
		// %w 保留 fwdErr 的类型链：classifyWSPublicError / 协议分类依赖
		// errors.As 穿透包装错误找到 wsUpstreamEventError 等结构化错误。
		Err:             fmt.Errorf("channel %s failed: %w", ra.channel.Name, fwdErr),
		UpstreamErr:     fwdErr,
		StatusCode:      statusCode,
		RetryAfter:      ra.retryAfter,
		OutboundType:    ra.currentOutboundType(),
		TerminalOutcome: terminalOutcome,
		inboundAdapter:  ra.inAdapter,
	}
}

// parseRequest 解析并验证入站请求
// 返回值中的 rawBody 为客户端原始请求字节，供同格式直通路径重用。
func parseRequest(inboundType inbound.InboundType, c *gin.Context) ([]byte, *model.InternalLLMRequest, model.Inbound, error) {
	body, err := httpbody.ReadRequest(c.Request, httpbody.MaxLLMRequestBodyBytes)
	if err != nil {
		if errors.Is(err, httpbody.ErrRequestBodyTooLarge) {
			resp.Error(c, http.StatusRequestEntityTooLarge, "request body too large")
		} else {
			resp.Error(c, http.StatusInternalServerError, err.Error())
		}
		return nil, nil, nil, err
	}

	inAdapter := inbound.Get(inboundType)
	internalRequest, err := inAdapter.TransformRequest(c.Request.Context(), body)
	if err != nil {
		resp.Error(c, http.StatusInternalServerError, err.Error())
		return nil, nil, nil, err
	}

	// Pass through the original query parameters
	internalRequest.Query = c.Request.URL.Query()

	if err := internalRequest.Validate(); err != nil {
		resp.Error(c, http.StatusBadRequest, err.Error())
		return nil, nil, nil, err
	}

	return body, internalRequest, inAdapter, nil
}

// forward 转发请求到上游服务
func (ra *relayAttempt) forward() (int, error) {
	ctx := ra.requestContext()
	requestSnapshot := cloneRequestForAttempt(ra.internalRequest)

	// 仅当本次实际使用 Responses 出站协议时尝试上游 WebSocket。
	if ra.currentOutboundType() == outbound.OutboundTypeOpenAIResponse &&
		ra.internalRequest.RawAPIFormat == model.APIFormatOpenAIResponse {

		shouldTryWS := false
		// Passthrough is now handled by forwardViaHTTP via PassthroughCapable interface
		if ra.internalRequest.IsOpenAIExactReplayRequest() {
			shouldTryWS = false
		} else if ra.c == nil {
			wsMode := effectiveResponsesWSMode(ra.channel)
			shouldTryWS = shouldEnableResponsesWS(ra.channel) && wsMode != responsesWSModeOff
		} else if requiresUpstreamWSContinuation(ra.internalRequest) {
			// Safety: HTTP ingress must not proactively use upstream WS for fresh requests,
			// but an explicit continuation cannot be safely failovered as ordinary HTTP.
			shouldTryWS = true
		}

		if shouldTryWS {
			statusCode, err := ra.forwardViaWSIsolated(ctx)
			if statusCode != -1 {
				protocolUnavailable := err != nil && !ra.streamPayloadWritten.Load() &&
					shouldTryProtocolFallbackForAttempt(requestSnapshot, ra.channel, ra.currentOutboundType(), statusCode, err)
				if protocolUnavailable {
					if fallbackStatus, fallbackErr, attempted := ra.forwardViaAlternateProtocol(ctx, requestSnapshot, statusCode, err); attempted {
						ra.metrics.SetWSRecovery(dbmodel.RelayLogWSRecoveryDowngrade)
						return fallbackStatus, fallbackErr
					}
				}
				return statusCode, err
			}
			if requiresUpstreamWSContinuation(ra.internalRequest) {
				balancer.DeleteSticky(ra.apiKeyID, ra.requestModel)
				return http.StatusConflict, fmt.Errorf("upstream continuation transport unavailable; please restart the conversation")
			}
			ra.metrics.SetWSRecovery(dbmodel.RelayLogWSRecoveryDowngrade)
			// statusCode == -1 means WS not available, fall through to HTTP.
			// From here the attempt is served by HTTP: clear the attempt-scoped
			// WS transport markers so a downstream HTTP success no longer reports
			// the aborted WS attempt.
			ra.attemptUsedWS = false
		}
	}

	statusCode, err := ra.forwardViaHTTPIsolated(ctx)
	protocolUnavailable := err != nil && !ra.streamPayloadWritten.Load() &&
		shouldTryProtocolFallbackForAttempt(requestSnapshot, ra.channel, ra.currentOutboundType(), statusCode, err)
	if !protocolUnavailable {
		return statusCode, err
	}
	if fallbackStatus, fallbackErr, attempted := ra.forwardViaAlternateProtocol(ctx, requestSnapshot, statusCode, err); attempted {
		return fallbackStatus, fallbackErr
	}
	return statusCode, err
}

// forwardViaAlternateProtocol performs the only permitted same-channel protocol
// fallback. The caller has already established that the current attempt failed
// before any downstream payload was written.
func (ra *relayAttempt) forwardViaAlternateProtocol(
	ctx context.Context,
	requestSnapshot *model.InternalLLMRequest,
	statusCode int,
	protocolErr error,
) (fallbackStatus int, fallbackErr error, attempted bool) {
	if ra == nil || requestSnapshot == nil || ra.channel == nil ||
		!shouldTryProtocolFallbackForAttempt(requestSnapshot, ra.channel, ra.currentOutboundType(), statusCode, protocolErr) {
		return statusCode, protocolErr, false
	}
	if shouldLearnProtocolUnsupportedForAttempt(requestSnapshot, ra.channel, ra.currentOutboundType(), statusCode, protocolErr) {
		ra.recordOpenAIProtocolCapability(ra.currentOutboundType(), dbmodel.OpenAIProtocolCapabilityUnsupported)
	}

	fallbackType, ok := alternateOutboundType(requestSnapshot, ra.currentOutboundType())
	if !ok || !canFallbackToOutbound(requestSnapshot, fallbackType) || !channelAllowsOutboundProtocol(ra.channel, fallbackType) {
		return statusCode, protocolErr, false
	}
	if outbound.Get(fallbackType) == nil {
		return statusCode, protocolErr, false
	}

	log.Debugf("upstream protocol endpoint unavailable; retrying channel %s with protocol %d", ra.channel.Name, fallbackType)
	ra.closeFirstTokenBudget()
	ra.firstTokenBudget = nil
	ra.retryAfter = 0
	ra.responseCollected.Store(false)
	ra.passthroughOutcome = model.PassthroughTerminalOutcomeNone
	ra.setOutboundType(fallbackType)
	// The attempt is now served by HTTP; WS transport markers from the failed
	// WS probe must not survive a successful HTTP fallback.
	ra.attemptUsedWS = false
	fallbackStatus, fallbackErr = ra.forwardViaHTTPIsolated(ctx)
	if fallbackErr != nil && !ra.streamPayloadWritten.Load() &&
		shouldLearnProtocolUnsupportedForAttempt(requestSnapshot, ra.channel, fallbackType, fallbackStatus, fallbackErr) {
		ra.recordOpenAIProtocolCapability(fallbackType, dbmodel.OpenAIProtocolCapabilityUnsupported)
	}
	return fallbackStatus, fallbackErr, true
}

// forwardViaWS attempts to forward via upstream WebSocket.
// Returns statusCode=-1 if WS is not available (caller should fall through to HTTP).
func (ra *relayAttempt) forwardViaWS(ctx context.Context) (int, error) {
	if ra.c == nil && effectiveResponsesWSMode(ra.channel) == responsesWSModePassthrough && !ra.internalRequest.IsOpenAIExactReplayRequest() {
		return ra.forwardViaWSPassthrough(ctx)
	}
	continuation := requiresUpstreamWSContinuation(ra.internalRequest)
	preferredConnID := ""
	if continuation {
		preferredConnID, _ = getWSResponseConn(currentPreviousResponseID(ra.internalRequest))
	}
	pc := TryUpstreamWSWithPreference(ctx, ra.channel, ra.channel.GetBaseUrl(), ra.usedKey.ChannelKey, ra.usedKey.ID, ra.clientRequestHeaders(), preferredConnID)
	if pc == nil {
		log.Debugf("upstream WS unavailable for channel %s (key=%d, continuation=%t)", ra.channel.Name, ra.usedKey.ID, continuation)
		return -1, nil // WS not available
	}

	log.Debugf("using upstream WebSocket for channel %s (key=%d)", ra.channel.Name, ra.usedKey.ID)
	log.Debugf("upstream WS selected (channel=%s, key=%d, continuation=%t, previous_response_id=%s)",
		ra.channel.Name, ra.usedKey.ID, continuation, currentPreviousResponseID(ra.internalRequest))

	// Build the Responses API request body
	responsesReq := openaiOutbound.ConvertToResponsesRequest(ra.internalRequest)
	reqBody, err := json.Marshal(responsesReq)
	if err != nil {
		wsUpstreamPool.Put(pc)
		return -1, nil // fall through to HTTP
	}
	ra.metrics.SetTransportRequestPayload(reqBody, ra.internalRequest.Model)

	// Send response.create message
	if err := wsUpstreamPool.SendResponseCreate(ctx, pc, reqBody); err != nil {
		log.Warnf("upstream WS send failed for channel %s: %v", ra.channel.Name, err)
		log.Debugf("upstream WS send failed before stream start (channel=%s, key=%d, continuation=%t, err=%v)",
			ra.channel.Name, ra.usedKey.ID, continuation, err)
		wsUpstreamPool.RemoveConn(pc)
		if isUpstreamWSConnectionBroken(err) {
			log.Debugf("upstream WS send failure eligible for redial (channel=%s, key=%d, continuation=%t)",
				ra.channel.Name, ra.usedKey.ID, continuation)
			statusCode, redialErr, recovered := ra.retryViaFreshUpstreamWS(ctx, reqBody)
			if recovered || redialErr != nil {
				return statusCode, redialErr
			}
		}
		wsUpstreamPool.recordWSFailureForRequest(ctx, ra.channel.ID, err)
		if requiresUpstreamWSContinuation(ra.internalRequest) {
			balancer.DeleteSticky(ra.apiKeyID, ra.requestModel)
			return http.StatusConflict, fmt.Errorf("upstream continuation transport unavailable; please restart the conversation")
		}
		return -1, nil // fall through to HTTP
	}

	// Read events from WS and process through the transform pipeline
	ra.metrics.UsedWS = true
	ra.metrics.SetWSExecMode(dbmodel.RelayLogWSExecModeTransform)
	ra.attemptUsedWS = true
	if ra.metrics.WSMode == nil {
		ra.metrics.SetWSMode(defaultWSModeForRequest(ra.internalRequest))
	}
	reader := newWSUpstreamReader(pc, ra.channel.ID, ra.usedKey.ID)
	err = ra.handleWSStreamResponseV2(ctx, reader)
	if err != nil {
		reader.CloseWithError()
		log.Debugf("upstream WS stream failed (channel=%s, key=%d, continuation=%t, written=%t, status=%d, err=%v)",
			ra.channel.Name, ra.usedKey.ID, continuation, ra.getStreamWriter().Written(), reader.StatusCode(), err)
		if requiresUpstreamWSContinuation(ra.internalRequest) && !ra.streamPayloadWritten.Load() && shouldReconnectUpstreamWSBeforeReplay(err) {
			log.Debugf("upstream WS stream failure eligible for reconnect before replay (channel=%s, key=%d, previous_response_id=%s)",
				ra.channel.Name, ra.usedKey.ID, currentPreviousResponseID(ra.internalRequest))
			statusCode, redialErr, recovered := ra.retryViaFreshUpstreamWS(ctx, reqBody)
			if recovered || redialErr != nil {
				return statusCode, redialErr
			}
		}
		if !isDownstreamWriteError(err) || hasIndependentWSTransportFailure(err) {
			wsUpstreamPool.recordWSFailureForRequest(ra.requestContext(), ra.channel.ID, err)
		}
		if requiresUpstreamWSContinuation(ra.internalRequest) && isContinuationTransportFailure(err) {
			balancer.DeleteSticky(ra.apiKeyID, ra.requestModel)
			return http.StatusConflict, fmt.Errorf("upstream continuation transport unavailable; please restart the conversation")
		}
		return reader.StatusCode(), err
	}

	reader.Close()
	wsUpstreamPool.RecordWSSuccess(ra.channel.ID)
	ra.recordSuccessfulWSAffinity(pc)
	return 200, nil
}

func (ra *relayAttempt) retryViaFreshUpstreamWS(ctx context.Context, reqBody []byte) (int, error, bool) {
	log.Debugf("attempting fresh upstream WS redial (channel=%s, key=%d, previous_response_id=%s)",
		ra.channel.Name, ra.usedKey.ID, currentPreviousResponseID(ra.internalRequest))
	redialed := TryUpstreamWS(ctx, ra.channel, ra.channel.GetBaseUrl(), ra.usedKey.ChannelKey, ra.usedKey.ID, ra.clientRequestHeaders(), true)
	if redialed == nil {
		log.Debugf("fresh upstream WS redial unavailable (channel=%s, key=%d)", ra.channel.Name, ra.usedKey.ID)
		return 0, nil, false
	}

	retryErr := wsUpstreamPool.SendResponseCreate(ctx, redialed, reqBody)
	if retryErr != nil {
		log.Warnf("upstream WS redial send failed for channel %s: %v", ra.channel.Name, retryErr)
		log.Debugf("fresh upstream WS redial send failed (channel=%s, key=%d, err=%v)", ra.channel.Name, ra.usedKey.ID, retryErr)
		wsUpstreamPool.RemoveConn(redialed)
		wsUpstreamPool.recordWSFailureForRequest(ctx, ra.channel.ID, retryErr)
		if requiresUpstreamWSContinuation(ra.internalRequest) {
			balancer.DeleteSticky(ra.apiKeyID, ra.requestModel)
			return http.StatusConflict, fmt.Errorf("upstream continuation transport unavailable; please restart the conversation"), true
		}
		return -1, nil, true
	}

	ra.metrics.UsedWS = true
	ra.metrics.SetWSExecMode(dbmodel.RelayLogWSExecModeTransform)
	ra.attemptUsedWS = true
	if ra.metrics.WSMode == nil {
		ra.metrics.SetWSMode(defaultWSModeForRequest(ra.internalRequest))
	}
	ra.metrics.SetWSRecovery(dbmodel.RelayLogWSRecoveryReconnect)
	// 重拨是一次全新的上游流：入站 adapter 必须同步刷新，避免旧连接的
	// usage / 聚合状态混入本次结果。
	ra.metrics.ClearWSUsage()
	ra.passthroughOutcome = model.PassthroughTerminalOutcomeNone
	if err := ra.beginNetworkAttempt(ra.internalRequest); err != nil {
		wsUpstreamPool.RemoveConn(redialed)
		return 0, err, true
	}
	reader := newWSUpstreamReader(redialed, ra.channel.ID, ra.usedKey.ID)
	streamErr := ra.handleWSStreamResponseV2(ctx, reader)
	if streamErr != nil {
		reader.CloseWithError()
		log.Debugf("fresh upstream WS redial stream failed (channel=%s, key=%d, status=%d, err=%v)",
			ra.channel.Name, ra.usedKey.ID, reader.StatusCode(), streamErr)
		if !isDownstreamWriteError(streamErr) || hasIndependentWSTransportFailure(streamErr) {
			wsUpstreamPool.recordWSFailureForRequest(ra.requestContext(), ra.channel.ID, streamErr)
		}
		if requiresUpstreamWSContinuation(ra.internalRequest) && isContinuationTransportFailure(streamErr) {
			balancer.DeleteSticky(ra.apiKeyID, ra.requestModel)
			return http.StatusConflict, fmt.Errorf("upstream continuation transport unavailable; please restart the conversation"), true
		}
		return reader.StatusCode(), streamErr, true
	}
	log.Debugf("fresh upstream WS redial succeeded (channel=%s, key=%d, previous_response_id=%s)",
		ra.channel.Name, ra.usedKey.ID, currentPreviousResponseID(ra.internalRequest))
	reader.Close()
	wsUpstreamPool.RecordWSSuccess(ra.channel.ID)
	ra.recordSuccessfulWSAffinity(redialed)
	return http.StatusOK, nil, true
}

func isContinuationTransportFailure(err error) bool {
	if isDownstreamWriteError(err) {
		return false
	}
	// Check for empty stream error (both old message and new error type)
	if errors.Is(err, stream.ErrEmptyUpstreamStream) {
		return true
	}
	message := upstreamClassificationMessage(err)
	return isUpstreamWSConnectionBroken(err) ||
		needsConversationRestart(message) ||
		strings.Contains(message, "ws stream ended before first event")
}

func (ra *relayAttempt) clientRequestHeaders() http.Header {
	if ra == nil || ra.c == nil || ra.c.Request == nil {
		return nil
	}
	return ra.c.Request.Header
}

func (ra *relayAttempt) handleWSStreamResponseV2(ctx context.Context, reader *wsUpstreamReader) error {
	defer ra.closeFirstTokenBudget()

	// Hand off early heartbeat
	ra.heartbeat.Hand()

	// Build transform function
	transform := func(ctx context.Context, data []byte) ([]byte, error) {
		return ra.transformStreamData(ctx, string(data))
	}

	// Determine first token timeout
	var firstTokenTimeout time.Duration
	if ra.firstTokenTimeOutSec > 0 && ra.firstTokenBudget == nil {
		firstTokenTimeout = time.Duration(ra.firstTokenTimeOutSec) * time.Second
	}

	// 上游 WS 流中断后同样依赖入站 adapter 的聚合状态合成唯一协议终态。
	interruptedFinalize := interruptedStreamFinalize(ra.inAdapter)
	terminalObserver := inboundStreamTerminalObserver(ra.inAdapter)

	// Create StreamProcessor
	processor := stream.NewStreamProcessor(stream.StreamConfig{
		Source:            stream.NewWSSource(reader),
		Transform:         transform,
		Finalize:          ra.finalizeInboundStream,
		Writer:            ra.getStreamWriter(),
		Context:           ctx,
		FirstTokenTimeout: firstTokenTimeout,
		HeartbeatInterval: streamHeartbeatInterval(),
		MaxEventSize:      maxSSEEventSize,
		OnInterrupted:     interruptedFinalize,
		TerminalObserver:  terminalObserver,
		OnFirstToken: func() {
			ra.metrics.SetFirstTokenTime(time.Now())
			ra.stopFirstTokenTimer()
		},
	})

	// Run processor
	err := processor.Run()
	// ReadEvent delivers a structured terminal frame first and retains its
	// matching provider error for the next read. Consume that pending error for
	// accounting without synthesizing or finalizing a second terminal.
	if pendingErr := reader.PendingError(); pendingErr != nil {
		if err == nil {
			err = pendingErr
		} else {
			err = errors.Join(err, pendingErr)
		}
	}
	ra.passthroughOutcome = processor.PassthroughOutcome()

	// Track payload written for metrics collection
	if processor.PayloadWritten() {
		ra.streamPayloadWritten.Store(true)
	}

	// Handle first token timeout specifically
	if err != nil && !isDownstreamWriteError(err) && strings.Contains(err.Error(), "first token timeout") {
		return ra.firstTokenTimeoutError()
	}

	// Check for context cancellation with first token timeout
	if err != nil {
		if timeoutErr := ra.firstTokenTimeoutIfNeeded(ctx, err); timeoutErr != nil {
			return timeoutErr
		}
	}

	return err
}

func cloneRequestForAttempt(req *model.InternalLLMRequest) *model.InternalLLMRequest {
	cloned := cloneInternalRequest(req)
	if cloned == nil {
		return nil
	}
	if req.StreamOptions != nil {
		streamOptions := *req.StreamOptions
		cloned.StreamOptions = &streamOptions
	}
	if req.TransformOptions.ArrayInputs != nil {
		arrayInputs := *req.TransformOptions.ArrayInputs
		cloned.TransformOptions.ArrayInputs = &arrayInputs
	}
	return cloned
}

// forwardViaHTTPIsolated gives every protocol attempt a private request snapshot.
// Some outbound adapters normalize request fields in place; those mutations must
// not leak into a same-channel protocol fallback or a later channel attempt.
func (ra *relayAttempt) forwardViaHTTPIsolated(ctx context.Context) (statusCode int, err error) {
	originalRequest := ra.internalRequest
	if originalRequest == nil {
		return 0, fmt.Errorf("internal request is nil")
	}
	ra.passthroughOutcome = model.PassthroughTerminalOutcomeNone
	ra.internalRequest = cloneRequestForAttempt(originalRequest)
	defer func() {
		ra.internalRequest = originalRequest
	}()
	if err := ra.beginNetworkAttempt(ra.internalRequest); err != nil {
		return 0, err
	}
	return ra.forwardViaHTTP(ctx)
}

func (ra *relayAttempt) forwardViaWSIsolated(ctx context.Context) (statusCode int, err error) {
	originalRequest := ra.internalRequest
	if originalRequest == nil {
		return 0, fmt.Errorf("internal request is nil")
	}
	ra.passthroughOutcome = model.PassthroughTerminalOutcomeNone
	ra.internalRequest = cloneRequestForAttempt(originalRequest)
	defer func() {
		ra.internalRequest = originalRequest
	}()
	if err := ra.beginNetworkAttempt(ra.internalRequest); err != nil {
		return 0, err
	}
	return ra.forwardViaWS(ctx)
}

// beginNetworkAttempt 在真正接触上游之前为本次 attempt 换上全新的入站 adapter。
// 流式响应的 usage / 聚合状态都积累在入站 adapter 内：如果失败的 attempt 复用
// 同一个 adapter，它遗留的 usage 会污染下一个候选渠道的成功响应。替换后同时
// 重置 responseCollected，让本次 attempt 重新收集指标。factory 为 nil 时保持
// 旧语义（直接构造 relayRequest 的测试路径），不视为错误。
func (ra *relayRequest) beginNetworkAttempt(request *model.InternalLLMRequest) error {
	ra.responseCollected.Store(false)
	if ra.newInboundAdapter == nil {
		return nil
	}
	adapter := ra.newInboundAdapter()
	if adapter == nil {
		return fmt.Errorf("inbound adapter factory returned nil for type")
	}
	if init, ok := adapter.(model.InboundResponseInitializer); ok && request != nil {
		init.InitializeResponse(request)
	}
	ra.inAdapter = adapter
	return nil
}

// forwardViaHTTP forwards the request using traditional HTTP.
func (ra *relayAttempt) forwardViaHTTP(ctx context.Context) (int, error) {
	// Raw passthrough is allowed only when the inbound API format, active
	// outbound protocol, and transformer capability agree. This keeps the raw
	// request/response contract symmetric for Responses and Anthropic Messages.
	if pt, ok := ra.outAdapter.(model.PassthroughCapable); ok &&
		len(ra.rawBody) > 0 &&
		canPassthroughProtocol(ra.internalRequest.RawAPIFormat, ra.currentOutboundType()) &&
		pt.CanPassthrough(ra.internalRequest.RawAPIFormat) {
		// Replay/continuation requests need the normalized path. Fresh HTTP and
		// downstream WebSocket requests may preserve native Responses bytes.
		if ra.internalRequest.RawAPIFormat == model.APIFormatOpenAIResponse {
			if ra.internalRequest.IsOpenAIExactReplayRequest() || requiresUpstreamWSContinuation(ra.internalRequest) {
				// Fall through to standard path.
			} else if ra.c != nil || requiresNativeResponsesUpstream(ra.internalRequest) {
				return ra.forwardViaHTTPPassthrough(ctx, pt)
			}
		} else {
			return ra.forwardViaHTTPPassthrough(ctx, pt)
		}
	}

	return ra.forwardViaHTTPStandard(ctx)
}

// forwardViaHTTPPassthrough handles unified passthrough for any PassthroughCapable transformer.
func (ra *relayAttempt) forwardViaHTTPPassthrough(ctx context.Context, pt model.PassthroughCapable) (int, error) {
	// Build request via TransformRequestRaw
	outboundRequest, err := pt.TransformRequestRaw(
		ctx,
		ra.rawBody,
		ra.internalRequest.Model,
		ra.channel.GetBaseUrl(),
		ra.usedKey.ChannelKey,
		ra.internalRequest.Query,
	)
	if err != nil {
		log.Warnf("failed to create passthrough request: %v", err)
		return 0, fmt.Errorf("failed to create request: %w", err)
	}

	// Apply param overrides
	if err := ra.applyParamOverride(outboundRequest); err != nil {
		return 0, err
	}

	// Copy headers and keep the outbound protocol's JSON content type.
	ra.copyHeaders(outboundRequest)
	if ra.currentOutboundType() == outbound.OutboundTypeOpenAIResponse {
		outboundRequest.Header.Set("Content-Type", "application/json")
	}

	// Send request
	response, err := ra.sendRequest(outboundRequest)
	if err != nil {
		return 0, fmt.Errorf("failed to send request: %w", err)
	}
	defer response.Body.Close()

	// Check status
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		ra.retryAfter = parseRetryAfter(response.Header.Get("Retry-After"))
		body, _ := io.ReadAll(io.LimitReader(response.Body, 64*1024))
		statusCode := normalizeUpstreamStatusCode(response.StatusCode, string(body))
		log.Warnf("upstream error from channel %s: status=%d", ra.channel.Name, response.StatusCode)
		return statusCode, newUpstreamHTTPError(statusCode, body)
	}

	// Get passthrough config
	cfg := pt.PassthroughConfig()

	// Branch: streaming vs non-streaming
	if ra.internalRequest.Stream != nil && *ra.internalRequest.Stream {
		if err := ra.handleStreamResponsePassthroughV2(ctx, response, cfg); err != nil {
			return responseStatusForError(response.StatusCode, err), err
		}
		return response.StatusCode, nil
	}
	if err := ra.handleResponsePassthrough(ctx, response, cfg); err != nil {
		return responseStatusForError(response.StatusCode, err), err
	}
	return response.StatusCode, nil

}

func canPassthroughProtocol(inboundFormat model.APIFormat, activeType outbound.OutboundType) bool {
	switch inboundFormat {
	case model.APIFormatOpenAIResponse:
		return activeType == outbound.OutboundTypeOpenAIResponse
	case model.APIFormatAnthropicMessage:
		return activeType == outbound.OutboundTypeAnthropic
	default:
		return false
	}
}

// handleResponsePassthrough handles non-streaming passthrough responses.
func (ra *relayAttempt) handleResponsePassthrough(ctx context.Context, response *http.Response, cfg model.PassthroughConfig) error {
	body, err := httpbody.ReadResponse(response)
	if err != nil {
		return fmt.Errorf("failed to read response body: %w", err)
	}

	if outcomeErr := classifyPassthroughJSONFailure(body, cfg); outcomeErr != nil {
		return outcomeErr
	}

	contentType := response.Header.Get("Content-Type")
	if contentType == "" {
		contentType = "application/json"
	}
	written, err := writeDownstreamResponse(ra.c, http.StatusOK, contentType, body)
	if written {
		ra.streamPayloadWritten.Store(true)
	}
	if err != nil {
		return err
	}

	// Sidecar metrics parse
	sidecarResp := &http.Response{
		StatusCode: response.StatusCode,
		Header:     response.Header.Clone(),
		Body:       io.NopCloser(bytes.NewReader(body)),
	}
	if internalResponse, err := ra.outAdapter.TransformResponse(ctx, sidecarResp); err == nil && internalResponse != nil {
		ra.inAdapter.TransformResponse(ctx, internalResponse)
		if cfg.CollectMetrics {
			ra.collectResponse()
		}
	}

	return nil
}

// forwardViaHTTPStandard 是 forwardViaHTTP 的原路径（直通判定失败时的兜底）。
// 留作显式出口，避免 passthrough 失败时的递归。
func (ra *relayAttempt) forwardViaHTTPStandard(ctx context.Context) (int, error) {
	outboundRequest, err := ra.outAdapter.TransformRequest(
		ctx,
		ra.internalRequest,
		ra.channel.GetBaseUrl(),
		ra.usedKey.ChannelKey,
	)
	if err != nil {
		log.Warnf("failed to create request: %v", err)
		return 0, fmt.Errorf("failed to create request: %w", err)
	}
	if err := ra.applyParamOverride(outboundRequest); err != nil {
		return 0, err
	}

	// 复制请求头，并保持实际出站协议的 JSON Content-Type。
	ra.copyHeaders(outboundRequest)
	if ra.currentOutboundType() == outbound.OutboundTypeOpenAIResponse {
		outboundRequest.Header.Set("Content-Type", "application/json")
	}

	// 发送请求
	response, err := ra.sendRequest(outboundRequest)
	if err != nil {
		return 0, fmt.Errorf("failed to send request: %w", err)
	}
	defer response.Body.Close()

	// 检查响应状态
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		ra.retryAfter = parseRetryAfter(response.Header.Get("Retry-After"))
		body, err := io.ReadAll(io.LimitReader(response.Body, 64*1024))
		if err != nil {
			return response.StatusCode, fmt.Errorf("failed to read response body: %w", err)
		}
		statusCode := normalizeUpstreamStatusCode(response.StatusCode, string(body))
		log.Warnf("upstream error from channel %s: status=%d", ra.channel.Name, response.StatusCode)
		return statusCode, newUpstreamHTTPError(statusCode, body)
	}

	// 处理响应
	if ra.internalRequest.Stream != nil && *ra.internalRequest.Stream {
		// Use V2 StreamProcessor-based implementation
		if err := ra.handleStreamResponseV2(ctx, response); err != nil {
			return responseStatusForError(response.StatusCode, err), err
		}
		return response.StatusCode, nil
	}
	if err := ra.handleResponse(ctx, response); err != nil {
		return responseStatusForError(response.StatusCode, err), err
	}
	return response.StatusCode, nil
}

// applyProtocolPreference uses persisted capability observations for a stable
// ranking. Explicit sticky candidates remain first; group policy order remains
// stable within each rank.
func applyProtocolPreference(req *model.InternalLLMRequest, iter *balancer.Iterator, ctx context.Context) {
	if iter == nil || iter.Len() < 2 || req == nil {
		return
	}
	switch req.RawAPIFormat {
	case model.APIFormatOpenAIChatCompletion, model.APIFormatOpenAIResponse:
	default:
		return
	}
	iter.PreferProtocolRank(func(item dbmodel.GroupItem) int {
		channel, err := op.ChannelGet(item.ChannelID, ctx)
		if err != nil {
			return 7
		}
		return protocolCandidateRank(req, channel)
	})
}

func applyProtocolPreferenceForMode(mode dbmodel.GroupMode, req *model.InternalLLMRequest, iter *balancer.Iterator, ctx context.Context) {
	if mode == dbmodel.GroupModeWeighted {
		return
	}
	applyProtocolPreference(req, iter, ctx)
}

func defaultWSModeForRequest(req *model.InternalLLMRequest) dbmodel.RelayLogWSMode {
	if requiresUpstreamWSContinuation(req) {
		return dbmodel.RelayLogWSModeContinuation
	}
	return dbmodel.RelayLogWSModeFresh
}

func readOutboundRequestBody(req *http.Request) ([]byte, error) {
	if req == nil || req.Body == nil {
		return nil, nil
	}
	if httpbody.RequestContentLengthTooLarge(req, httpbody.MaxLLMRequestBodyBytes) {
		return nil, fmt.Errorf("%w: limit %d bytes", httpbody.ErrRequestBodyTooLarge, httpbody.MaxLLMRequestBodyBytes)
	}
	if req.GetBody != nil {
		bodyReader, err := req.GetBody()
		if err != nil {
			return nil, err
		}
		defer bodyReader.Close()
		return httpbody.ReadRequestBody(bodyReader, httpbody.MaxLLMRequestBodyBytes)
	}
	body, err := httpbody.ReadRequestBody(req.Body, httpbody.MaxLLMRequestBodyBytes)
	if err != nil {
		return nil, err
	}
	req.Body = io.NopCloser(bytes.NewReader(body))
	req.ContentLength = int64(len(body))
	return body, nil
}

// getStreamWriter returns the appropriate stream writer for the current request.
func (ra *relayAttempt) getStreamWriter() StreamWriter {
	if ra.streamWriter != nil {
		return ra.streamWriter
	}
	return ra.c.Writer
}

// applyParamOverride merges channel-level JSON request overrides and records the final upstream payload.
func (ra *relayAttempt) applyParamOverride(outboundRequest *http.Request) error {
	if err := helper.ApplyParamOverride(outboundRequest, ra.channel.ParamOverride); err != nil {
		return err
	}
	if requestBody, readErr := readOutboundRequestBody(outboundRequest); readErr == nil {
		ra.metrics.SetTransportRequestPayload(requestBody, ra.internalRequest.Model)
	}
	return nil
}

// copyHeaders 复制请求头，过滤 hop-by-hop 头
func (ra *relayAttempt) copyHeaders(outboundRequest *http.Request) {
	if outboundRequest == nil {
		return
	}
	if ra.c != nil && ra.c.Request != nil {
		copySafeUpstreamHeaders(outboundRequest.Header, ra.c.Request.Header)
	}
	if outboundRequest.Header.Get("User-Agent") == "" {
		outboundRequest.Header.Set("User-Agent", "")
	}
	if ra.channel != nil {
		applySafeChannelHeaders(outboundRequest.Header, ra.channel.CustomHeader)
	}
	// The selected key is authoritative. Client and custom credential headers
	// are intentionally ignored so channel selection cannot be bypassed.
	ra.applySelectedChannelCredentials(outboundRequest.Header)
}

func copySafeUpstreamHeaders(dst, src http.Header) {
	if dst == nil || src == nil {
		return
	}
	entries := collectNormalizedHeaderEntries(src, func(name string) bool {
		return !isBlockedUpstreamHeader(name)
	})
	for lowerName, entry := range entries {
		// Anthropic's beta header is additive: preserve adapter defaults while
		// treating direct mixed-case map entries as one logical header.
		if lowerName == "anthropic-beta" {
			merged := ""
			for _, value := range headerValuesCaseInsensitive(dst, entry.name) {
				merged = mergeBetaHeader(merged, value)
			}
			for _, value := range entry.values {
				merged = mergeBetaHeader(merged, value)
			}
			if merged == "" {
				deleteHeaderCaseInsensitive(dst, entry.name)
			} else {
				setHeaderValuesCaseInsensitive(dst, entry.name, []string{merged})
			}
			continue
		}
		setHeaderValuesCaseInsensitive(dst, entry.name, entry.values)
	}
}

func applySafeChannelHeaders(dst http.Header, headers []dbmodel.CustomHeader) {
	if dst == nil {
		return
	}
	for _, header := range headers {
		name := strings.TrimSpace(header.HeaderKey)
		if isBlockedChannelHeader(name) {
			continue
		}
		setHeaderValuesCaseInsensitive(dst, name, []string{header.HeaderValue})
	}
}

func (ra *relayAttempt) applySelectedChannelCredentials(headers http.Header) {
	if ra == nil || headers == nil || ra.channel == nil {
		return
	}
	applySelectedCredentialHeader(headers, ra.currentOutboundType(), ra.usedKey.ChannelKey)
}

// mergeBetaHeader 合并两个逗号分隔的 anthropic-beta 字段值，去重并保留先后顺序。
func mergeBetaHeader(existing, incoming string) string {
	seen := make(map[string]struct{}, 8)
	merged := make([]string, 0, 8)
	for _, source := range []string{existing, incoming} {
		for _, entry := range strings.Split(source, ",") {
			normalized := strings.TrimSpace(entry)
			if normalized == "" {
				continue
			}
			if _, ok := seen[normalized]; ok {
				continue
			}
			seen[normalized] = struct{}{}
			merged = append(merged, normalized)
		}
	}
	return strings.Join(merged, ",")
}

// sendRequest 发送 HTTP 请求
func (ra *relayAttempt) sendRequest(req *http.Request) (*http.Response, error) {
	httpClient, err := helper.ChannelHTTPClientWithContext(req.Context(), ra.channel)
	if err != nil {
		log.Warnf("failed to get http client: %v", err)
		return nil, err
	}

	req = ra.attachFirstTokenBudget(req)

	response, err := httpClient.Do(req)
	if err != nil {
		if timeoutErr := ra.firstTokenTimeoutIfNeeded(req.Context(), err); timeoutErr != nil {
			ra.closeFirstTokenBudget()
			return nil, timeoutErr
		}
		if isClientCancellation(req.Context(), err) {
			log.Infof("request canceled before upstream response: %v", err)
		} else {
			log.Warnf("failed to send request: %v", err)
		}
		ra.closeFirstTokenBudget()
		return nil, err
	}

	if response != nil && response.Body != nil && ra.firstTokenBudget != nil {
		response.Body = &closeWithFuncReadCloser{
			ReadCloser: response.Body,
			onClose:    ra.closeFirstTokenBudget,
		}
	}

	return response, nil
}

// handleStreamResponseV2 uses StreamProcessor for unified stream handling.
func (ra *relayAttempt) handleStreamResponseV2(ctx context.Context, response *http.Response) error {
	defer ra.closeFirstTokenBudget()

	// Content-Type validation
	if ct := response.Header.Get("Content-Type"); ct != "" && !strings.Contains(strings.ToLower(ct), "text/event-stream") {
		return fmt.Errorf("upstream returned non-SSE content-type %q for stream request", ct)
	}

	// Hand off early heartbeat
	ra.heartbeat.Hand()

	// Build transform function
	transform := func(ctx context.Context, data []byte) ([]byte, error) {
		return ra.transformStreamData(ctx, string(data))
	}

	// Determine first token timeout
	var firstTokenTimeout time.Duration
	if ra.firstTokenTimeOutSec > 0 && ra.firstTokenBudget == nil {
		firstTokenTimeout = time.Duration(ra.firstTokenTimeOutSec) * time.Second
	}

	// interruptedStreamFinalize 报告入站 adapter 是否能在上游流中断后从自身
	// 聚合状态合成唯一协议终态（目前仅 Responses 入站实现）。
	interruptedFinalize := interruptedStreamFinalize(ra.inAdapter)
	terminalObserver := inboundStreamTerminalObserver(ra.inAdapter)

	// Create StreamProcessor
	processor := stream.NewStreamProcessor(stream.StreamConfig{
		Source:            stream.NewSSESource(response.Body, maxSSEEventSize),
		Transform:         transform,
		Finalize:          ra.finalizeInboundStream,
		Writer:            ra.getStreamWriter(),
		Context:           ctx,
		FirstTokenTimeout: firstTokenTimeout,
		HeartbeatInterval: streamHeartbeatInterval(),
		MaxEventSize:      maxSSEEventSize,
		OnInterrupted:     interruptedFinalize,
		TerminalObserver:  terminalObserver,
		OnFirstToken: func() {
			ra.metrics.SetFirstTokenTime(time.Now())
			ra.stopFirstTokenTimer()
		},
	})

	// Run processor
	err := processor.Run()
	ra.passthroughOutcome = processor.PassthroughOutcome()

	// Track payload written for metrics collection
	if processor.PayloadWritten() {
		ra.streamPayloadWritten.Store(true)
	}

	// Handle first token timeout specifically
	if err != nil && !isDownstreamWriteError(err) && strings.Contains(err.Error(), "first token timeout") {
		_ = response.Body.Close()
		return ra.firstTokenTimeoutError()
	}

	// Check for context cancellation with first token timeout
	if err != nil {
		if timeoutErr := ra.firstTokenTimeoutIfNeeded(ctx, err); timeoutErr != nil {
			return timeoutErr
		}
	}

	return err
}

// handleStreamResponsePassthroughV2 uses StreamProcessor for unified passthrough handling.
// Works with any PassthroughCapable transformer (Anthropic, OpenAI Responses, etc.).
func (ra *relayAttempt) handleStreamResponsePassthroughV2(ctx context.Context, response *http.Response, cfg model.PassthroughConfig) error {
	defer ra.closeFirstTokenBudget()

	// Content-Type validation
	if ct := response.Header.Get("Content-Type"); ct != "" && !strings.Contains(strings.ToLower(ct), "text/event-stream") {
		return fmt.Errorf("upstream returned non-SSE content-type %q for stream request", ct)
	}

	// Hand off early heartbeat
	ra.heartbeat.Hand()

	// Determine first token timeout
	var firstTokenTimeout time.Duration
	if ra.firstTokenTimeOutSec > 0 && ra.firstTokenBudget == nil {
		firstTokenTimeout = time.Duration(ra.firstTokenTimeOutSec) * time.Second
	}

	// Buffer raw stream data for cancellation metrics. Normal and incomplete EOF
	// paths also feed the bytes through the existing sidecar transformer exactly
	// once, so the same inbound state machine owns sequence/output/usage state.
	var rawStreamBuf bytes.Buffer
	sidecarCollected := false
	collectSidecar := func(sidecarCtx context.Context, rawStream []byte) {
		if sidecarCollected || len(rawStream) == 0 {
			return
		}
		sidecarCollected = true
		ra.collectPassthroughMetrics(sidecarCtx, rawStream)
	}
	var incompleteFinalize func(context.Context, []byte) ([]byte, error)
	if finalizer, ok := ra.inAdapter.(model.InboundIncompleteFinalizer); ok {
		incompleteFinalize = func(finalizeCtx context.Context, rawStream []byte) ([]byte, error) {
			collectSidecar(finalizeCtx, rawStream)
			return finalizer.FinalizeIncompleteStream(finalizeCtx, rawStream)
		}
	}

	// Frame raw SSE events so structured failure terminals can be classified
	// before they are committed. Responses' legacy [DONE] marker is filtered as
	// before; Anthropic raw events are preserved byte-for-byte.
	passthroughOutcome := newPassthroughSSETransform(
		cfg,
		ra.currentOutboundType() == outbound.OutboundTypeOpenAIResponse,
	)
	passthroughFinalize := func(finalizeCtx context.Context) stream.StreamTransformResult {
		return passthroughOutcome.finalize(finalizeCtx, passthroughOutcome.seenPayload)
	}

	// Create StreamProcessor
	processor := stream.NewStreamProcessor(stream.StreamConfig{
		Source:               stream.NewRawSource(response.Body, 32*1024),
		TransformWithOutcome: passthroughOutcome.transform,
		FinalizeWithOutcome:  passthroughFinalize,
		Writer:               ra.getStreamWriter(),
		Context:              ctx,
		FirstTokenTimeout:    firstTokenTimeout,
		HeartbeatInterval:    streamHeartbeatInterval(),
		MaxEventSize:         maxSSEEventSize,
		BufferRawStream:      true,
		TerminalEvents:       cfg.TerminalEvents,
		IncompleteFinalize:   incompleteFinalize,
		OnInterrupted:        incompleteFinalize,
		OnFirstToken: func() {
			ra.metrics.SetFirstTokenTime(time.Now())
			ra.stopFirstTokenTimer()
		},
		OnFinish: func(ctx context.Context, rawStream []byte) error {
			if len(rawStream) == 0 {
				return stream.ErrEmptyUpstreamStream
			}
			rawStreamBuf.Write(rawStream)
			collectSidecar(ctx, rawStream)

			if cfg.CollectMetrics {
				ra.collectResponse()
			}

			log.Debugf("passthrough stream end")
			return nil
		},
	})

	// Run processor
	err := processor.Run()
	ra.passthroughOutcome = processor.PassthroughOutcome()

	// Track payload and sidecar metrics even when the processor terminates on a
	// structured failure. OnFinish handles successful EOF; failed attempts do not.
	if processor.PayloadWritten() {
		ra.streamPayloadWritten.Store(true)
	}
	if raw := processor.RawStream(); len(raw) > 0 {
		if rawStreamBuf.Len() == 0 {
			rawStreamBuf.Write(raw)
		}
		collectSidecar(context.Background(), raw)
	}

	// Handle first token timeout specifically
	if err != nil && !isDownstreamWriteError(err) && strings.Contains(err.Error(), "first token timeout") {
		_ = response.Body.Close()
		return ra.firstTokenTimeoutError()
	}

	// Check for context cancellation with first token timeout
	if err != nil {
		if timeoutErr := ra.firstTokenTimeoutIfNeeded(ctx, err); timeoutErr != nil {
			return timeoutErr
		}
	}

	// On disconnect with partial data, still try to collect metrics.
	if err != nil && errors.Is(err, context.Canceled) && rawStreamBuf.Len() > 0 {
		collectSidecar(context.Background(), rawStreamBuf.Bytes())
		if cfg.CollectMetrics {
			ra.collectResponse()
		}
	}

	return err
}

// collectPassthroughMetrics parses raw SSE stream for metrics aggregation without mutating response.
func (ra *relayAttempt) collectPassthroughMetrics(ctx context.Context, rawStream []byte) {
	if len(rawStream) == 0 {
		return
	}

	// Try stream event adapter first (preferred)
	outEventAdapter, outOk := ra.outAdapter.(model.OutboundStreamEventTransformer)
	inEventAdapter, inOk := ra.inAdapter.(model.InboundStreamEventTransformer)
	if outOk && inOk {
		readCfg := &sse.ReadConfig{MaxEventSize: maxSSEEventSize}
		for ev, err := range sse.Read(bytes.NewReader(rawStream), readCfg) {
			if err != nil {
				log.Debugf("passthrough metrics parse skipped: %v", err)
				return
			}
			if events, terr := outEventAdapter.TransformStreamEvent(ctx, []byte(ev.Data)); terr == nil && len(events) > 0 {
				_, _ = inEventAdapter.TransformStreamEvents(ctx, events)
			}
		}
		return
	}

	// Fallback to traditional stream transformer
	readCfg := &sse.ReadConfig{MaxEventSize: maxSSEEventSize}
	for ev, err := range sse.Read(bytes.NewReader(rawStream), readCfg) {
		if err != nil {
			log.Debugf("passthrough metrics parse skipped: %v", err)
			return
		}
		if chunk, terr := ra.outAdapter.TransformStream(ctx, []byte(ev.Data)); terr == nil && chunk != nil {
			_, _ = ra.inAdapter.TransformStream(ctx, chunk)
		}
	}
}

// transformStreamData 转换流式数据
func (ra *relayAttempt) transformStreamData(ctx context.Context, data string) ([]byte, error) {
	events, ok, decodeErr := ra.decodeOutboundStreamEvents(ctx, []byte(data))
	if ok {
		inStream, encodeErr := ra.encodeInboundStreamEvents(ctx, events)
		if decodeErr != nil && encodeErr != nil {
			return inStream, errors.Join(decodeErr, encodeErr)
		}
		if decodeErr != nil {
			return inStream, decodeErr
		}
		return inStream, encodeErr
	}
	if decodeErr != nil {
		log.Warnf("failed to transform stream events: %v", decodeErr)
		return nil, decodeErr
	}

	internalStream, err := ra.decodeOutboundStreamResponse(ctx, []byte(data))
	if err != nil {
		log.Warnf("failed to transform stream: %v", err)
		return nil, err
	}
	if internalStream == nil {
		return nil, nil
	}

	return ra.encodeInboundStreamResponse(ctx, internalStream)
}

func (ra *relayAttempt) finalizeInboundStream(ctx context.Context) ([]byte, error) {
	finalizer, ok := ra.inAdapter.(model.InboundStreamFinalizer)
	if !ok {
		return nil, nil
	}
	return finalizer.FinalizeStream(ctx)
}

// interruptedStreamFinalize returns the callback used to synthesize exactly
// one protocol terminal when the upstream stream breaks after payload was
// already written to the client. Transform-mode synthesis relies on the
// inbound adapter's aggregated stream state (rawStream is ignored); adapters
// opt in through InboundInterruptedFinalizer. Passthrough handlers supply
// their own raw-stream-based finalizer.
func interruptedStreamFinalize(adapter model.Inbound) func(context.Context, []byte) ([]byte, error) {
	finalizer, ok := adapter.(model.InboundInterruptedFinalizer)
	if !ok {
		return nil
	}
	return func(finalizeCtx context.Context, _ []byte) ([]byte, error) {
		return finalizer.FinalizeInterruptedStream(finalizeCtx)
	}
}

func inboundStreamTerminalObserver(adapter model.Inbound) stream.StreamTerminalObserver {
	observer, ok := adapter.(model.InboundStreamTerminalObserver)
	if !ok {
		return nil
	}
	return observer.StreamTerminalOutcome
}

func (ra *relayAttempt) decodeOutboundStreamEvents(ctx context.Context, data []byte) ([]model.StreamEvent, bool, error) {
	outEventAdapter, ok := ra.outAdapter.(model.OutboundStreamEventTransformer)
	if !ok {
		return nil, false, nil
	}
	if _, ok := ra.inAdapter.(model.InboundStreamEventTransformer); !ok {
		return nil, false, nil
	}
	events, err := outEventAdapter.TransformStreamEvent(ctx, data)
	return events, true, err
}

func (ra *relayAttempt) encodeInboundStreamEvents(ctx context.Context, events []model.StreamEvent) ([]byte, error) {
	if len(events) == 0 {
		return nil, nil
	}
	inEventAdapter, ok := ra.inAdapter.(model.InboundStreamEventTransformer)
	if !ok {
		return nil, nil
	}
	inStream, err := inEventAdapter.TransformStreamEvents(ctx, events)
	if err != nil {
		log.Warnf("failed to transform inbound stream events: %v", err)
	}
	return inStream, err
}

func (ra *relayAttempt) decodeOutboundStreamResponse(ctx context.Context, data []byte) (*model.InternalLLMResponse, error) {
	return ra.outAdapter.TransformStream(ctx, data)
}

func (ra *relayAttempt) encodeInboundStreamResponse(ctx context.Context, internalStream *model.InternalLLMResponse) ([]byte, error) {
	inStream, err := ra.inAdapter.TransformStream(ctx, internalStream)
	if err != nil {
		log.Warnf("failed to transform stream: %v", err)
	}
	return inStream, err
}

// handleResponse 处理非流式响应
func (ra *relayAttempt) handleResponse(ctx context.Context, response *http.Response) error {
	internalResponse, err := ra.outAdapter.TransformResponse(ctx, response)
	if err != nil {
		log.Warnf("failed to transform response: %v", err)
		return fmt.Errorf("failed to transform outbound response: %w", err)
	}

	inResponse, err := ra.inAdapter.TransformResponse(ctx, internalResponse)
	if err != nil {
		log.Warnf("failed to transform response: %v", err)
		return fmt.Errorf("failed to transform inbound response: %w", err)
	}

	written, err := writeDownstreamResponse(ra.c, http.StatusOK, "application/json", inResponse)
	if written {
		ra.streamPayloadWritten.Store(true)
	}
	return err
}

// collectResponse 收集响应信息
func (ra *relayAttempt) collectResponse() {
	if ra == nil || ra.inAdapter == nil || ra.metrics == nil {
		return
	}
	if !ra.responseCollected.CompareAndSwap(false, true) {
		return
	}
	internalResponse, err := ra.inAdapter.GetInternalResponse(ra.requestContext())
	if err != nil {
		log.Debugf("collectResponse: failed to get internal response: %v", err)
		return
	}
	if internalResponse == nil {
		log.Debugf("collectResponse: internal response is nil (stream may not be complete)")
		return
	}

	actualModel := strings.TrimSpace(internalResponse.Model)
	if actualModel == "" && ra.internalRequest != nil {
		actualModel = strings.TrimSpace(ra.internalRequest.Model)
	}
	ra.metrics.SetInternalResponse(internalResponse, actualModel)
}

func (ra *relayAttempt) collectOpenAIResponsesPassthroughMetrics(ctx context.Context, rawStream []byte) {
	if len(rawStream) == 0 {
		return
	}
	outEventAdapter, outOk := ra.outAdapter.(model.OutboundStreamEventTransformer)
	inEventAdapter, inOk := ra.inAdapter.(model.InboundStreamEventTransformer)
	if outOk && inOk {
		readCfg := &sse.ReadConfig{MaxEventSize: maxSSEEventSize}
		for ev, err := range sse.Read(bytes.NewReader(rawStream), readCfg) {
			if err != nil {
				log.Debugf("openai responses passthrough metrics parse skipped: %v", err)
				return
			}
			if events, terr := outEventAdapter.TransformStreamEvent(ctx, []byte(ev.Data)); terr == nil && len(events) > 0 {
				_, _ = inEventAdapter.TransformStreamEvents(ctx, events)
			}
		}
		return
	}
	readCfg := &sse.ReadConfig{MaxEventSize: maxSSEEventSize}
	for ev, err := range sse.Read(bytes.NewReader(rawStream), readCfg) {
		if err != nil {
			log.Debugf("openai responses passthrough metrics parse skipped: %v", err)
			return
		}
		if internalStream, terr := ra.outAdapter.TransformStream(ctx, []byte(ev.Data)); terr == nil && internalStream != nil {
			_, _ = ra.inAdapter.TransformStream(ctx, internalStream)
		}
	}
}

// responsesPassthroughTerminalEvents / anthropicPassthroughTerminalEvents 定义各协议
// SSE 流的终态事件类型；缓存流中出现终态事件即视为上游响应已完整送达。
var (
	responsesPassthroughTerminalEvents = map[string]struct{}{
		"response.completed":  {},
		"response.failed":     {},
		"response.incomplete": {},
		"error":               {},
	}
	anthropicPassthroughTerminalEvents = map[string]struct{}{
		"message_stop": {},
		"error":        {},
	}
)

// streamReachedTerminalEvent 报告缓存的原始 SSE 流是否已包含协议终态事件。
// 客户端 SDK 收到终态事件后会立即断连而不等上游 EOF，断连取消会沿出站请求
// 传播打断上游读取；此时读取被取消不代表流未完成。
func streamReachedTerminalEvent(rawStream []byte, terminalTypes map[string]struct{}) bool {
	if len(rawStream) == 0 {
		return false
	}
	readCfg := &sse.ReadConfig{MaxEventSize: maxSSEEventSize}
	for ev, err := range sse.Read(bytes.NewReader(rawStream), readCfg) {
		if err != nil {
			break
		}
		typ := strings.TrimSpace(ev.Type)
		if typ == "" {
			var head struct {
				Type string `json:"type"`
			}
			if json.Unmarshal([]byte(ev.Data), &head) == nil {
				typ = head.Type
			}
		}
		if _, ok := terminalTypes[typ]; ok {
			return true
		}
	}
	return false
}

// forwardViaHTTPStandard 是 forwardViaHTTP 的原路径（直通判定失败时的兜底）。
// 留作显式出口，避免 passthrough 失败时的递归。

func (ra *relayAttempt) collectAnthropicPassthroughMetrics(ctx context.Context, rawStream []byte) {
	if len(rawStream) == 0 {
		return
	}
	outEventAdapter, outOk := ra.outAdapter.(model.OutboundStreamEventTransformer)
	inEventAdapter, inOk := ra.inAdapter.(model.InboundStreamEventTransformer)
	if outOk && inOk {
		readCfg := &sse.ReadConfig{MaxEventSize: maxSSEEventSize}
		for ev, err := range sse.Read(bytes.NewReader(rawStream), readCfg) {
			if err != nil {
				log.Debugf("anthropic passthrough metrics parse skipped: %v", err)
				return
			}
			if events, terr := outEventAdapter.TransformStreamEvent(ctx, []byte(ev.Data)); terr == nil && len(events) > 0 {
				_, _ = inEventAdapter.TransformStreamEvents(ctx, events)
			}
		}
		return
	}
	readCfg := &sse.ReadConfig{MaxEventSize: maxSSEEventSize}
	for ev, err := range sse.Read(bytes.NewReader(rawStream), readCfg) {
		if err != nil {
			log.Debugf("anthropic passthrough metrics parse skipped: %v", err)
			return
		}
		if internalStream, terr := ra.outAdapter.TransformStream(ctx, []byte(ev.Data)); terr == nil && internalStream != nil {
			_, _ = ra.inAdapter.TransformStream(ctx, internalStream)
		}
	}
}
