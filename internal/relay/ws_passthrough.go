package relay

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	dbmodel "github.com/bestruirui/octopus/internal/model"
	"github.com/bestruirui/octopus/internal/relay/stream"
	transformerModel "github.com/bestruirui/octopus/internal/transformer/model"
	openaiOutbound "github.com/bestruirui/octopus/internal/transformer/outbound/openai"
	"github.com/bestruirui/octopus/internal/utils/log"
	"github.com/coder/websocket"
)

type wsPassthroughStats struct {
	ResponseID       string
	Model            string
	Usage            *transformerModel.Usage
	RawOutput        json.RawMessage
	Error            *wsUpstreamEventError
	TerminalOutcome  transformerModel.PassthroughTerminalOutcome
	DownstreamBroken bool
}

var errWSPassthroughDownstreamClosed = errors.Join(
	stream.ErrDownstreamWriteFailed,
	errors.New("downstream websocket closed"),
)

type wsUpstreamEventError struct {
	Status  int
	Code    string
	Type    string
	Message string
}

func (e *wsUpstreamEventError) Error() string {
	if e == nil {
		return "upstream ws error"
	}
	if e.Status > 0 {
		return fmt.Sprintf("upstream ws error: %d", e.Status)
	}
	return "upstream ws error"
}

// wsUpstreamErrorStatus 返回结构化上游错误帧应透出的 HTTP 状态。
// 帧自带 status 时直接使用；缺失时按结构化 code/type/message 分类，
// 使 endpoint、鉴权、限流等错误保留真实状态而不是统一的 502。
func wsUpstreamErrorStatus(err error) int {
	var wsErr *wsUpstreamEventError
	if errors.As(err, &wsErr) && wsErr != nil && wsErr.Status > 0 {
		return wsErr.Status
	}
	if publicErr, ok := classifyWSPublicError(err, 0); ok {
		return publicErr.Status
	}
	message := upstreamClassificationMessage(err)
	switch {
	case isUpstreamWSAuthError(message):
		return http.StatusUnauthorized
	case isUpstreamWSPermissionError(message):
		return http.StatusForbidden
	case isOpenAIEndpointRoutingError(http.StatusNotFound, err):
		return http.StatusNotFound
	}
	return http.StatusBadGateway
}

func shouldRecordWSPassthroughTransportFailure(stats *wsPassthroughStats, err error) bool {
	if err == nil {
		return false
	}
	// A synthetic terminal may successfully close the public protocol after an
	// upstream WS close/reset. Preserve that transport evidence even though the
	// semantic outcome is now Failed. The same evidence must also survive a
	// later synthetic-terminal write failure and downstream error join.
	if hasIndependentWSTransportFailure(err) {
		return true
	}
	if errors.Is(err, errWSPassthroughDownstreamClosed) || (stats != nil && stats.DownstreamBroken) {
		return false
	}
	// A delivered protocol terminal is an application outcome, not evidence that
	// the WebSocket transport itself should enter backoff. Its normal relay
	// accounting is handled by the terminal-aware attempt result.
	if stats != nil && stats.TerminalOutcome != transformerModel.PassthroughTerminalOutcomeNone {
		return false
	}
	var structured *wsUpstreamEventError
	return !errors.As(err, &structured)
}

func (ra *relayAttempt) forwardViaWSPassthrough(ctx context.Context) (int, error) {
	continuation := requiresUpstreamWSContinuation(ra.internalRequest)
	preferredConnID := ""
	if continuation {
		preferredConnID, _ = getWSResponseConn(currentPreviousResponseID(ra.internalRequest))
	}
	pc := TryUpstreamWSWithPreference(ctx, ra.channel, ra.channel.GetBaseUrl(), ra.usedKey.ChannelKey, ra.usedKey.ID, ra.clientRequestHeaders(), preferredConnID)
	if pc == nil {
		log.Debugf("upstream WS passthrough unavailable for channel %s (key=%d, continuation=%t)", ra.channel.Name, ra.usedKey.ID, continuation)
		return -1, nil
	}

	payload, err := ra.buildWSPassthroughRequestPayload()
	if err != nil {
		wsUpstreamPool.Put(pc)
		return -1, nil
	}
	ra.metrics.SetTransportRequestPayload(payload, ra.internalRequest.Model)
	if err := wsUpstreamPool.SendRaw(ctx, pc, payload); err != nil {
		log.Warnf("upstream WS passthrough send failed for channel %s: %v", ra.channel.Name, err)
		wsUpstreamPool.RemoveConn(pc)
		if isUpstreamWSConnectionBroken(err) {
			statusCode, redialErr, recovered := ra.retryViaFreshUpstreamWSPassthrough(ctx, payload)
			if recovered || redialErr != nil {
				return statusCode, redialErr
			}
		}
		wsUpstreamPool.recordWSFailureForRequest(ctx, ra.channel.ID, err)
		if continuation {
			return http.StatusConflict, fmt.Errorf("upstream continuation transport unavailable; please restart the conversation")
		}
		return -1, nil
	}

	ra.metrics.UsedWS = true
	ra.metrics.SetWSExecMode(dbmodel.RelayLogWSExecModePassthrough)
	ra.attemptUsedWS = true
	if ra.metrics.WSMode == nil {
		ra.metrics.SetWSMode(defaultWSModeForRequest(ra.internalRequest))
	}
	// Reset attempt-scoped usage before a new upstream WS stream: a failed
	// attempt's usage must never leak into the next success.
	ra.metrics.ClearWSUsage()
	stats, err := ra.handleWSPassthroughStream(ctx, pc)
	if stats != nil && stats.DownstreamBroken {
		// Preserve the downstream origin even when the upstream reader also
		// returned an error while the broken client was being drained.
		err = errors.Join(errWSPassthroughDownstreamClosed, err)
	}
	ra.applyWSPassthroughOutcome(stats)
	if err != nil {
		ra.applyWSPassthroughStats(stats)
		wsUpstreamPool.RemoveConn(pc)
		if continuation && !ra.streamPayloadWritten.Load() && shouldReconnectUpstreamWSBeforeReplay(err) {
			statusCode, redialErr, recovered := ra.retryViaFreshUpstreamWSPassthrough(ctx, payload)
			if recovered || redialErr != nil {
				return statusCode, redialErr
			}
		}
		if shouldRecordWSPassthroughTransportFailure(stats, err) {
			wsUpstreamPool.recordWSFailureForRequest(ra.requestContext(), ra.channel.ID, err)
		}
		if continuation && isContinuationTransportFailure(err) {
			return http.StatusConflict, fmt.Errorf("upstream continuation transport unavailable; please restart the conversation")
		}
		return wsUpstreamErrorStatus(err), err
	}
	wsUpstreamPool.Put(pc)
	wsUpstreamPool.RecordWSSuccess(ra.channel.ID)
	ra.applyWSPassthroughStats(stats)
	ra.recordSuccessfulWSAffinity(pc)
	return http.StatusOK, nil
}

func (ra *relayAttempt) retryViaFreshUpstreamWSPassthrough(ctx context.Context, payload []byte) (int, error, bool) {
	redialed := TryUpstreamWSWithPreference(ctx, ra.channel, ra.channel.GetBaseUrl(), ra.usedKey.ChannelKey, ra.usedKey.ID, ra.clientRequestHeaders(), "", true)
	if redialed == nil {
		return 0, nil, false
	}
	if err := wsUpstreamPool.SendRaw(ctx, redialed, payload); err != nil {
		wsUpstreamPool.RemoveConn(redialed)
		wsUpstreamPool.recordWSFailureForRequest(ctx, ra.channel.ID, err)
		if requiresUpstreamWSContinuation(ra.internalRequest) {
			return http.StatusConflict, fmt.Errorf("upstream continuation transport unavailable; please restart the conversation"), true
		}
		return -1, nil, true
	}
	ra.metrics.UsedWS = true
	ra.metrics.SetWSExecMode(dbmodel.RelayLogWSExecModePassthrough)
	ra.attemptUsedWS = true
	if ra.metrics.WSMode == nil {
		ra.metrics.SetWSMode(defaultWSModeForRequest(ra.internalRequest))
	}
	ra.metrics.SetWSRecovery(dbmodel.RelayLogWSRecoveryReconnect)
	ra.metrics.ClearWSUsage()
	stats, err := ra.handleWSPassthroughStream(ctx, redialed)
	if stats != nil && stats.DownstreamBroken {
		// Preserve the downstream origin even when the upstream reader also
		// returned an error while the broken client was being drained.
		err = errors.Join(errWSPassthroughDownstreamClosed, err)
	}
	ra.applyWSPassthroughOutcome(stats)
	if err != nil {
		ra.applyWSPassthroughStats(stats)
		wsUpstreamPool.RemoveConn(redialed)
		if shouldRecordWSPassthroughTransportFailure(stats, err) {
			wsUpstreamPool.recordWSFailureForRequest(ra.requestContext(), ra.channel.ID, err)
		}
		if requiresUpstreamWSContinuation(ra.internalRequest) && isContinuationTransportFailure(err) {
			return http.StatusConflict, fmt.Errorf("upstream continuation transport unavailable; please restart the conversation"), true
		}
		return wsUpstreamErrorStatus(err), err, true
	}
	wsUpstreamPool.Put(redialed)
	wsUpstreamPool.RecordWSSuccess(ra.channel.ID)
	ra.applyWSPassthroughStats(stats)
	ra.recordSuccessfulWSAffinity(redialed)
	return http.StatusOK, nil, true
}

func (ra *relayAttempt) buildWSPassthroughRequestPayload() ([]byte, error) {
	body := ra.rawBody
	if len(body) == 0 {
		responsesReq := openaiOutbound.ConvertToResponsesRequest(ra.internalRequest)
		var err error
		body, err = json.Marshal(responsesReq)
		if err != nil {
			return nil, err
		}
	}
	var payload map[string]json.RawMessage
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil, err
	}
	payload["type"] = json.RawMessage(`"response.create"`)
	payload["stream"] = json.RawMessage(`true`)
	delete(payload, "background")
	if ra.internalRequest != nil && strings.TrimSpace(ra.internalRequest.Model) != "" {
		modelBytes, err := json.Marshal(ra.internalRequest.Model)
		if err != nil {
			return nil, err
		}
		payload["model"] = modelBytes
	}
	return json.Marshal(payload)
}

func (ra *relayAttempt) handleWSPassthroughStream(ctx context.Context, pc *pooledConn) (*wsPassthroughStats, error) {
	writer := ra.getStreamWriter()
	stats := &wsPassthroughStats{}
	firstEvent := true
	dropDownstream := false
	wroteDownstream := false
	readCtx := ctx
	var firstTokenCancel context.CancelFunc
	if ra.firstTokenTimeOutSec > 0 {
		readCtx, firstTokenCancel = context.WithTimeoutCause(
			ctx,
			time.Duration(ra.firstTokenTimeOutSec)*time.Second,
			errFirstTokenTimeout,
		)
		defer func() {
			if firstTokenCancel != nil {
				firstTokenCancel()
			}
		}()
	}
	for {
		msgType, data, err := pc.conn.Read(readCtx)
		if err != nil {
			if firstEvent {
				if timeoutErr := ra.firstTokenTimeoutIfNeeded(readCtx, err); timeoutErr != nil {
					return stats, timeoutErr
				}
			}
			if stats.DownstreamBroken {
				return stats, errWSPassthroughDownstreamClosed
			}
			if isClientCancellation(ctx, err) {
				if ctxErr := contextError(ctx); ctxErr != nil {
					return stats, ctxErr
				}
				return stats, newWSTransportError("ws passthrough read error", err)
			}
			closeStatus := websocket.CloseStatus(err)
			if closeStatus == websocket.StatusNormalClosure || closeStatus == websocket.StatusGoingAway {
				if firstEvent {
					return stats, fmt.Errorf("ws stream ended before first event")
				}
				// 已写出 payload 但上游未送出任何协议终态：补发唯一一个
				// response.incomplete 终态，并按不完整上游失败返回；payload
				// 已对下游可见，禁止 retry/failover。
				if wroteDownstream {
					if ra.writeWSPassthroughSyntheticTerminal(ctx, writer, stats, &dropDownstream, ra.buildWSPassthroughIncompleteTerminal) {
						stats.TerminalOutcome = transformerModel.PassthroughTerminalOutcomeIncomplete
					}
					incompleteErr := fmt.Errorf("%w: ws passthrough stream ended without a terminal event", transformerModel.ErrIncompleteUpstreamStream)
					return stats, wsPassthroughTerminalError(stats.TerminalOutcome, incompleteErr)
				}
				return stats, nil
			}
			if wroteDownstream {
				// 传输层断裂且 payload 已可见：同样只补发一个失败终态后返回。
				// Synthetic terminal 自身的写失败不能掩盖此前已经发生的
				// 上游断裂；该分支本身就是明确的上游传输失败。
				if ra.writeWSPassthroughSyntheticTerminal(ctx, writer, stats, &dropDownstream, ra.buildWSPassthroughFailureTerminal) {
					stats.TerminalOutcome = transformerModel.PassthroughTerminalOutcomeFailed
				}
				readErr := newWSTransportError("ws passthrough read error", err)
				readErr = errors.Join(readErr, transformerModel.ErrIncompleteUpstreamStream)
				return stats, wsPassthroughTerminalError(stats.TerminalOutcome, readErr)
			}
			return stats, newWSTransportError("ws passthrough read error", err)
		}
		if msgType != websocket.MessageText {
			continue
		}
		if len(data) > maxSSEEventSize {
			if wroteDownstream {
				if ra.writeWSPassthroughSyntheticTerminal(ctx, writer, stats, &dropDownstream, ra.buildWSPassthroughFailureTerminal) {
					stats.TerminalOutcome = transformerModel.PassthroughTerminalOutcomeFailed
				}
			}
			return stats, fmt.Errorf("ws passthrough event exceeds limit %d bytes", maxSSEEventSize)
		}
		if firstEvent {
			firstEvent = false
			if firstTokenCancel != nil {
				readCtx = ctx
				firstTokenCancel()
				firstTokenCancel = nil
			}
			if ra.metrics != nil {
				ra.metrics.SetFirstTokenTime(time.Now())
			}
			ra.stopFirstTokenTimer()
		}
		observeWSPassthroughEvent(stats, data)
		if stats.DownstreamBroken {
			return stats, errWSPassthroughDownstreamClosed
		}
		if stats.Error != nil {
			if wroteDownstream {
				// Downstream already received a delta: append at most one legal
				// Responses failure terminal, then prevent retry/failover.
				if ra.writeWSPassthroughSyntheticTerminal(ctx, writer, stats, &dropDownstream, ra.buildWSPassthroughFailureTerminal) {
					stats.TerminalOutcome = transformerModel.PassthroughTerminalOutcomeFailed
				}
			}
			// A structured upstream failure is already a legal typed terminal.
			// Preserve it for upper-layer classification without adding the
			// incomplete-stream sentinel.
			return stats, stats.Error
		}
		terminalOutcome := wsPassthroughTerminalOutcome(data)
		if !dropDownstream {
			out := ra.rewriteWSPassthroughDownstreamModel(data)
			written, writeErr := writeWSPassthroughDownstream(ctx, writer, out)
			if written {
				wroteDownstream = true
				ra.streamPayloadWritten.Store(true)
			}
			if writeErr != nil {
				stats.DownstreamBroken = true
				// Any downstream write error commits the attempt. A first-frame
				// disconnect may accept zero bytes, but it still must not trigger
				// upstream retry, failover, protocol fallback, or replay.
				ra.streamPayloadWritten.Store(true)
				if isClientCancellation(ctx, writeErr) || isUpstreamWSConnectionBroken(writeErr) {
					log.Debugf("ws passthrough downstream write failed; draining upstream (channel=%d, key=%d): %v", ra.channel.ID, ra.usedKey.ID, writeErr)
					dropDownstream = true
					if readCtx == ctx {
						drainCtx, drainCancel := context.WithTimeout(context.Background(), wsPassthroughDrainTimeout)
						defer drainCancel()
						readCtx = drainCtx
					}
					continue
				}
				if closer, ok := writer.(interface{ CloseWithError() }); ok {
					closer.CloseWithError()
				}
				return stats, errWSPassthroughDownstreamClosed
			}
		}
		if terminalOutcome != transformerModel.PassthroughTerminalOutcomeNone {
			if stats.DownstreamBroken {
				return stats, errWSPassthroughDownstreamClosed
			}
			if !dropDownstream && wroteDownstream {
				stats.TerminalOutcome = terminalOutcome
			}
			return stats, wsPassthroughTerminalError(stats.TerminalOutcome, nil)
		}
	}
}

// buildWSPassthroughFailureTerminal 在下游已收到部分输出后，合成符合
// Responses WS 协议的唯一一个 response.failed 终态。原始上游 error 帧
// 不直接转发：裸 error 事件无法为客户端流收尾。复用已解析的 stats 与
// 既有模型改写 helper，与 HTTP/WS replay 状态保持隔离。
func (ra *relayAttempt) buildWSPassthroughFailureTerminal(stats *wsPassthroughStats) ([]byte, error) {
	responseID := ""
	model := ""
	if stats != nil {
		responseID = stats.ResponseID
		model = stats.Model
	}

	// Provider error fields are internal classification evidence only. The
	// synthetic downstream event uses fixed public wording so an upstream cannot
	// inject arbitrary code/type/message into the client's protocol.
	encoded, err := json.Marshal(map[string]any{
		"type": "response.failed",
		"response": map[string]any{
			"id":     responseID,
			"object": "response",
			"model":  model,
			"status": "failed",
			"error": map[string]any{
				"code":    "upstream_error",
				"message": "The upstream request failed.",
			},
		},
	})
	if err != nil {
		return nil, err
	}
	if ra != nil {
		encoded = ra.rewriteWSPassthroughDownstreamModel(encoded)
	}
	return encoded, nil
}

// writeWSPassthroughSyntheticTerminal 在下游已收到部分输出后补写唯一一个
// 协议允许的合成终态（failed/incomplete）。写入失败只记录 debug 日志：
// 终态是尽力而为的收尾，不能改变本就失败的尝试语义，也不得引发任何
// 重试。dropDownstream 置位时下游已断开，跳过写入。
func (ra *relayAttempt) writeWSPassthroughSyntheticTerminal(
	ctx context.Context,
	writer StreamWriter,
	stats *wsPassthroughStats,
	dropDownstream *bool,
	build func(*wsPassthroughStats) ([]byte, error),
) bool {
	if dropDownstream != nil && *dropDownstream {
		return false
	}
	out, buildErr := build(stats)
	if buildErr != nil {
		log.Debugf("ws passthrough: failed to build synthetic terminal (channel=%d, key=%d): %v", ra.channel.ID, ra.usedKey.ID, buildErr)
		return false
	}
	if written, writeErr := writeWSPassthroughDownstream(ctx, writer, out); writeErr != nil {
		if stats != nil {
			stats.DownstreamBroken = true
		}
		if dropDownstream != nil {
			*dropDownstream = true
		}
		if written {
			ra.streamPayloadWritten.Store(true)
		}
		if closer, ok := writer.(interface{ CloseWithError() }); ok {
			closer.CloseWithError()
		}
		log.Debugf("ws passthrough: failed to write synthetic terminal downstream (channel=%d, key=%d): %v", ra.channel.ID, ra.usedKey.ID, writeErr)
		return false
	}
	return true
}

// buildWSPassthroughIncompleteTerminal 在已写出部分输出、上游又正常关闭且
// 未送出任何协议终态时，合成唯一一个 response.incomplete 终态，让客户端
// 流能够收尾。复用已解析的 response 元数据与既有模型改写 helper，与
// HTTP/WS replay 状态保持隔离。
func (ra *relayAttempt) buildWSPassthroughIncompleteTerminal(stats *wsPassthroughStats) ([]byte, error) {
	responseID := ""
	modelName := ""
	if stats != nil {
		responseID = stats.ResponseID
		modelName = stats.Model
	}
	// Raw provider output is retained only for internal accounting. Never copy it
	// into a synthetic public terminal, where it could contain untrusted or
	// sensitive provider-controlled content.
	encoded, err := json.Marshal(map[string]any{
		"type": "response.incomplete",
		"response": map[string]any{
			"id":     responseID,
			"object": "response",
			"model":  modelName,
			"status": "incomplete",
			"output": []any{},
		},
	})
	if err != nil {
		return nil, err
	}
	if ra != nil {
		encoded = ra.rewriteWSPassthroughDownstreamModel(encoded)
	}
	return encoded, nil
}

func writeWSPassthroughDownstream(ctx context.Context, writer StreamWriter, out []byte) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	var compact bytes.Buffer
	if json.Compact(&compact, out) == nil {
		out = compact.Bytes()
	}
	frame := []byte("data: " + string(out) + "\n\n")
	n, writeErr := writer.Write(frame)
	written := n > 0 || writer.Written()
	if writeErr != nil {
		return written, writeErr
	}
	if n < len(frame) {
		return written, io.ErrShortWrite
	}
	writer.Flush()
	return written, nil
}

func (ra *relayAttempt) rewriteWSPassthroughDownstreamModel(data []byte) []byte {
	if ra == nil || ra.internalRequest == nil || strings.TrimSpace(ra.requestModel) == "" || strings.TrimSpace(ra.internalRequest.Model) == strings.TrimSpace(ra.requestModel) {
		return data
	}
	var payload any
	if err := json.Unmarshal(data, &payload); err != nil {
		return data
	}
	if replaceModelInJSONValue(payload, ra.internalRequest.Model, ra.requestModel) {
		if rewritten, err := json.Marshal(payload); err == nil {
			return rewritten
		}
	}
	return data
}

func replaceModelInJSONValue(value any, upstreamModel, downstreamModel string) bool {
	switch v := value.(type) {
	case map[string]any:
		changed := false
		for key, child := range v {
			if key == "model" {
				if s, ok := child.(string); ok && s == upstreamModel {
					v[key] = downstreamModel
					changed = true
				}
				continue
			}
			if replaceModelInJSONValue(child, upstreamModel, downstreamModel) {
				changed = true
			}
		}
		return changed
	case []any:
		changed := false
		for _, child := range v {
			if replaceModelInJSONValue(child, upstreamModel, downstreamModel) {
				changed = true
			}
		}
		return changed
	default:
		return false
	}
}

func observeWSPassthroughEvent(stats *wsPassthroughStats, data []byte) {
	if stats == nil || len(data) == 0 {
		return
	}
	var event map[string]any
	if err := json.Unmarshal(data, &event); err != nil {
		return
	}
	eventType := wsEventString(event["type"])
	if id := wsEventString(event["id"]); id != "" {
		stats.ResponseID = id
	}
	if modelName := wsEventString(event["model"]); modelName != "" {
		stats.Model = modelName
	}
	if usage, ok := decodeResponsesUsageEvent(event["usage"]); ok {
		stats.Usage = usage.toInternal()
	}
	response, _ := event["response"].(map[string]any)
	if id := wsEventString(response["id"]); id != "" {
		stats.ResponseID = id
	}
	if modelName := wsEventString(response["model"]); modelName != "" {
		stats.Model = modelName
	}
	if rawOutput, ok := response["output"]; ok && rawOutput != nil {
		if encoded, err := json.Marshal(rawOutput); err == nil && !bytes.Equal(bytes.TrimSpace(encoded), []byte("null")) {
			stats.RawOutput = append(json.RawMessage(nil), encoded...)
		}
	}
	if usage, ok := decodeResponsesUsageEvent(response["usage"]); ok {
		stats.Usage = usage.toInternal()
	}

	responseStatus := strings.ToLower(strings.TrimSpace(wsEventString(response["status"])))
	failureEvent := eventType == "error" || eventType == "response.error" || eventType == "response.failed" || responseStatus == "failed"
	if !failureEvent {
		return
	}
	code, message, errorType, present := wsEventErrorFields(event["error"])
	if !present {
		code = normalizeWSUpstreamErrorCode(event["code"])
		message = wsEventString(event["message"])
		errorType = eventType
	}
	if responseCode, responseMessage, responseType, responsePresent := wsEventErrorFields(response["error"]); responsePresent {
		code, message, errorType = responseCode, responseMessage, responseType
	}
	if message == "" {
		message = "upstream response failed"
	}
	status := wsEventStatusCode(event["status"])
	if status == 0 {
		status = http.StatusBadGateway
	}
	stats.Error = &wsUpstreamEventError{Status: status, Code: code, Type: errorType, Message: message}
}

func decodeResponsesUsageEvent(value any) (*responsesUsageEvent, bool) {
	if value == nil {
		return nil, false
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, false
	}
	var usage responsesUsageEvent
	if err := json.Unmarshal(encoded, &usage); err != nil {
		return nil, false
	}
	return &usage, true
}

func normalizeWSUpstreamErrorCode(code any) string {
	switch v := code.(type) {
	case string:
		return limitWSEventText(v, wsEventFieldMaxBytes)
	case float64:
		if math.IsNaN(v) || math.IsInf(v, 0) || v != math.Trunc(v) {
			return ""
		}
		return limitWSEventText(strconv.FormatFloat(v, 'f', -1, 64), wsEventFieldMaxBytes)
	case json.Number:
		if normalized, ok := wsIntegralNumber(v.String()); ok {
			return limitWSEventText(normalized.String(), wsEventFieldMaxBytes)
		}
		return ""
	default:
		// Code is provider-controlled classification data. Do not stringify
		// objects, arrays, or arbitrary values into an internal/public error.
		return ""
	}
}

type responsesUsageEvent struct {
	InputTokens       int64 `json:"input_tokens"`
	InputTokenDetails struct {
		CachedTokens int64 `json:"cached_tokens"`
	} `json:"input_tokens_details"`
	OutputTokens       int64 `json:"output_tokens"`
	OutputTokenDetails struct {
		ReasoningTokens int64 `json:"reasoning_tokens"`
	} `json:"output_tokens_details"`
	TotalTokens int64 `json:"total_tokens"`
}

func (u *responsesUsageEvent) toInternal() *transformerModel.Usage {
	if u == nil {
		return nil
	}
	usage := &transformerModel.Usage{PromptTokens: u.InputTokens, CompletionTokens: u.OutputTokens, TotalTokens: u.TotalTokens}
	if u.InputTokenDetails.CachedTokens > 0 {
		usage.PromptTokensDetails = &transformerModel.PromptTokensDetails{CachedTokens: u.InputTokenDetails.CachedTokens}
	}
	if u.OutputTokenDetails.ReasoningTokens > 0 {
		usage.CompletionTokensDetails = &transformerModel.CompletionTokensDetails{ReasoningTokens: u.OutputTokenDetails.ReasoningTokens}
	}
	return usage
}

func isWSPassthroughTerminal(data []byte) bool {
	return wsPassthroughTerminalOutcome(data) != transformerModel.PassthroughTerminalOutcomeNone
}

func wsPassthroughTerminalOutcome(data []byte) transformerModel.PassthroughTerminalOutcome {
	var event struct {
		Type     string `json:"type"`
		Response *struct {
			Status string `json:"status"`
		} `json:"response"`
	}
	if err := json.Unmarshal(data, &event); err != nil {
		return transformerModel.PassthroughTerminalOutcomeNone
	}
	eventType := strings.TrimSpace(event.Type)
	status := ""
	if event.Response != nil {
		status = strings.ToLower(strings.TrimSpace(event.Response.Status))
	}
	switch {
	case eventType == "response.failed" || eventType == "response.error" || eventType == "error" || status == "failed":
		return transformerModel.PassthroughTerminalOutcomeFailed
	case eventType == "response.incomplete" || status == "incomplete":
		return transformerModel.PassthroughTerminalOutcomeIncomplete
	case eventType == "response.cancelled" || eventType == "response.canceled" || status == "cancelled" || status == "canceled":
		return transformerModel.PassthroughTerminalOutcomeCancelled
	case isWSStreamTerminalEvent(eventType):
		return transformerModel.PassthroughTerminalOutcomeCompleted
	default:
		return transformerModel.PassthroughTerminalOutcomeNone
	}
}

func wsPassthroughTerminalError(outcome transformerModel.PassthroughTerminalOutcome, cause error) error {
	if outcome == transformerModel.PassthroughTerminalOutcomeNone || outcome == transformerModel.PassthroughTerminalOutcomeCompleted {
		return cause
	}
	return stream.NewPassthroughTerminalError(outcome, cause)
}

func (ra *relayAttempt) applyWSPassthroughStats(stats *wsPassthroughStats) {
	if ra == nil || ra.metrics == nil || stats == nil {
		return
	}
	modelName := strings.TrimSpace(stats.Model)
	if modelName == "" && ra.internalRequest != nil {
		modelName = ra.internalRequest.Model
	}
	resp := &transformerModel.InternalLLMResponse{
		ID:                      stats.ResponseID,
		Object:                  "response",
		Created:                 time.Now().Unix(),
		Model:                   modelName,
		Usage:                   stats.Usage,
		RawResponsesOutputItems: stats.RawOutput,
	}
	ra.metrics.SetInternalResponse(resp, modelName)
}

func (ra *relayAttempt) applyWSPassthroughOutcome(stats *wsPassthroughStats) {
	if ra == nil || stats == nil || stats.TerminalOutcome == transformerModel.PassthroughTerminalOutcomeNone {
		return
	}
	ra.passthroughOutcome = stats.TerminalOutcome
}
