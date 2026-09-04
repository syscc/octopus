package relay

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	dbmodel "github.com/bestruirui/octopus/internal/model"
	"github.com/bestruirui/octopus/internal/op"
	"github.com/bestruirui/octopus/internal/outlierwindow"
	"github.com/bestruirui/octopus/internal/relay/balancer"
	"github.com/bestruirui/octopus/internal/transformer/inbound"
	transformerModel "github.com/bestruirui/octopus/internal/transformer/model"
	"github.com/bestruirui/octopus/internal/transformer/outbound"
	"github.com/bestruirui/octopus/internal/utils/log"
	"github.com/coder/websocket"
	"github.com/gin-gonic/gin"
)

const (
	wsClientMaxAge    = 60 * time.Minute
	wsClientReadLimit = 16 * 1024 * 1024 // 16MB per message
)

type wsRelayResult struct {
	Success           bool
	ResponseID        string
	OutboundType      outbound.OutboundType
	ResetConversation bool
	Written           bool
	Canceled          bool
	Err               error
	PublicError       *wsPublicError
}

// HandleWSResponse handles WebSocket upgrade for /v1/responses.
func HandleWSResponse(c *gin.Context) {
	conn, err := websocket.Accept(c.Writer, c.Request, &websocket.AcceptOptions{
		InsecureSkipVerify: true, // Allow cross-origin
	})
	if err != nil {
		log.Warnf("websocket upgrade failed: %v", err)
		return
	}
	defer conn.CloseNow()

	conn.SetReadLimit(wsClientReadLimit)

	ctx, cancel := context.WithTimeout(c.Request.Context(), wsClientMaxAge)
	defer cancel()

	apiKeyID := c.GetInt("api_key_id")
	supportedModels := c.GetString("supported_models")

	log.Debugf("ws client connected (apikey=%d)", apiKeyID)

	downstreamSessionID := fmt.Sprintf("ws_%d", time.Now().UnixNano())
	var conversationState *wsConversationState

	// Message loop
	for {
		select {
		case <-ctx.Done():
			writeWSError(ctx, conn, 400, "websocket_connection_limit_reached",
				"Responses websocket connection limit reached (60 minutes). Create a new websocket connection to continue.")
			conn.Close(websocket.StatusNormalClosure, "connection limit reached")
			return
		default:
		}

		msgType, data, err := conn.Read(ctx)
		if err != nil {
			closeStatus := websocket.CloseStatus(err)
			if closeStatus == websocket.StatusNormalClosure || closeStatus == websocket.StatusGoingAway {
				log.Debugf("ws client disconnected normally (apikey=%d)", apiKeyID)
			} else {
				log.Warnf("ws client read error (apikey=%d): %v", apiKeyID, err)
			}
			return
		}

		if msgType != websocket.MessageText {
			continue
		}

		var msg struct {
			Type string `json:"type"`
		}
		if err := json.Unmarshal(data, &msg); err != nil {
			writeWSError(ctx, conn, 400, "invalid_request", "Failed to parse message")
			continue
		}

		if msg.Type != "response.create" {
			writeWSError(ctx, conn, 400, "invalid_request",
				fmt.Sprintf("Unknown message type: %s", msg.Type))
			continue
		}

		conversationState = processWSResponseCreate(ctx, conn, data, apiKeyID, supportedModels, downstreamSessionID, conversationState)
	}
}

func processWSResponseCreate(
	ctx context.Context,
	conn *websocket.Conn,
	data []byte,
	apiKeyID int,
	supportedModels string,
	downstreamSessionID string,
	conversationState *wsConversationState,
) *wsConversationState {
	var reqBody map[string]json.RawMessage
	if err := json.Unmarshal(data, &reqBody); err != nil {
		writeWSError(ctx, conn, 400, "invalid_request", "Failed to parse request body")
		return conversationState
	}

	// Remove WS-only fields
	delete(reqBody, "type")
	requestModel := strings.TrimSpace(extractWSRequestModel(reqBody))
	allowStoredRestore := wsRequestExplicitlyRequestsContinuation(reqBody)
	requestedPreviousResponseID := ""
	if raw, ok := reqBody["previous_response_id"]; ok && len(raw) > 0 {
		_ = json.Unmarshal(raw, &requestedPreviousResponseID)
		requestedPreviousResponseID = strings.TrimSpace(requestedPreviousResponseID)
	}
	hadLocalState := conversationState != nil
	conversationState = resolveWSConversationState(apiKeyID, requestModel, conversationState, allowStoredRestore, downstreamSessionID)
	hasResolvedState := conversationState != nil
	resolvedLastResponseID := ""
	if conversationState != nil {
		resolvedLastResponseID = strings.TrimSpace(conversationState.LastResponseID)
	}
	log.Debugf("ws response.create state resolved (apikey=%d, request_model=%s, requested_prev=%s, explicit_continuation=%t, had_local_state=%t, resolved_state=%t, resolved_last_response_id=%s)",
		apiKeyID, requestModel, requestedPreviousResponseID, allowStoredRestore, hadLocalState, hasResolvedState, resolvedLastResponseID)
	if conversationState != nil {
		conversationState.DownstreamSessionID = downstreamSessionID
	}
	rewriteWSPreviousResponseID(reqBody, conversationState)
	preferredSticky := wsConversationStateToSticky(conversationState)
	if preferredSticky == nil && requestedPreviousResponseID != "" {
		if group, err := op.GroupGetEnabledMap(requestModel, ctx); err == nil {
			scope := wsAffinityScope{APIKeyID: apiKeyID, GroupID: group.ID, RequestModel: requestModel, ResponseID: requestedPreviousResponseID}
			if entry, ok := getWSAffinityStore().Get(ctx, scope); ok {
				preferredSticky = &balancer.SessionEntry{ChannelID: entry.ChannelID, ChannelKeyID: entry.ChannelKeyID, Timestamp: time.Now()}
				log.Debugf("ws response affinity hit (apikey=%d, group=%d, request_model=%s, previous_response_id=%s, channel=%d, key=%d)",
					apiKeyID, group.ID, requestModel, requestedPreviousResponseID, entry.ChannelID, entry.ChannelKeyID)
			}
		}
	}

	// Check for generate: false (warmup). Codex-style clients use this as a
	// prewarm probe and do not expect a synthetic completed response turn.
	// Acknowledging it locally caused some clients to wait forever for a normal
	// response lifecycle. Prime the upstream pool best-effort, then stay silent.
	if genRaw, ok := reqBody["generate"]; ok {
		var generate bool
		if json.Unmarshal(genRaw, &generate) == nil && !generate {
			if err := bestEffortWarmupUpstreamWS(ctx, apiKeyID, supportedModels, reqBody); err != nil {
				log.Warnf("ws warmup failed (apikey=%d): %v", apiKeyID, err)
			} else {
				log.Debugf("ws warmup ready (apikey=%d)", apiKeyID)
			}
			return conversationState
		}
		delete(reqBody, "generate")
	}

	injectWSPreviousResponseID(reqBody, conversationState)

	// Force stream mode
	reqBody["stream"] = json.RawMessage("true")

	bodyBytes, err := json.Marshal(reqBody)
	if err != nil {
		writeWSError(ctx, conn, 500, "server_error", "Failed to build request")
		return conversationState
	}

	// Parse request
	inAdapter := inbound.Get(inbound.InboundTypeOpenAIResponse)
	internalRequest, err := inAdapter.TransformRequest(ctx, bodyBytes)
	if err != nil {
		writeWSError(ctx, conn, 400, "invalid_request", err.Error())
		return conversationState
	}
	originalRequest := cloneInternalRequest(internalRequest)
	continuationRequested := allowStoredRestore || requestContainsToolOutputs(originalRequest)
	if !continuationRequested {
		deleteWSConversationState(apiKeyID, requestModel, downstreamSessionID)
		conversationState = nil
		preferredSticky = nil
	}
	executionRequest := originalRequest
	if conversationState != nil && continuationRequested {
		if conversationState.ShouldUseNativeContinuation(originalRequest) {
			log.Debugf("ws relay using native continuation (apikey=%d, request_model=%s, previous_response_id=%s)", apiKeyID, requestModel, currentPreviousResponseID(originalRequest))
		} else if conversationState.ShouldUseLocalReplay(originalRequest) {
			var replayedRequest *transformerModel.InternalLLMRequest
			if conversationState.LastOutboundTypeSet && conversationState.LastOutboundType == outbound.OutboundTypeOpenAIChat {
				replayedRequest = conversationState.BuildChatReplayRequest(originalRequest)
			} else {
				replayedRequest = conversationState.BuildReplayRequest(originalRequest)
			}
			if replayedRequest == nil {
				rejectWSLocalReplay(ctx, conn, apiKeyID, requestModel, downstreamSessionID)
				return nil
			}
			executionRequest = replayedRequest
		}
	}

	// Check supported models
	if supportedModels != "" {
		supportedModelsArray := strings.Split(supportedModels, ",")
		found := false
		for _, m := range supportedModelsArray {
			if m == executionRequest.Model {
				found = true
				break
			}
		}
		if !found {
			writeWSError(ctx, conn, 400, "invalid_request", "model not supported")
			return conversationState
		}
	}

	requestModel = executionRequest.Model
	req, group, err := newWSRelayRequest(ctx, conn, inAdapter, apiKeyID, requestModel, cloneInternalRequest(executionRequest), originalRequest, preferredSticky, bodyBytes)
	if err != nil {
		status := 404
		code := "model_not_found"
		if err.Error() == "no available channel" {
			status = 503
			code = "no_available_channel"
		}
		writeWSError(ctx, conn, status, code, err.Error())
		return conversationState
	}

	autoRestart := conversationState != nil && continuationRequested && conversationState.CanAutoRestart(originalRequest)
	failedPreviousResponseID := currentPreviousResponseID(originalRequest)
	log.Debugf("ws relay prepared (apikey=%d, request_model=%s, previous_response_id=%s, auto_replay=%t, preferred_channel=%d, preferred_key=%d)",
		apiKeyID, requestModel, failedPreviousResponseID, autoRestart,
		func() int {
			if preferredSticky == nil {
				return 0
			}
			return preferredSticky.ChannelID
		}(),
		func() int {
			if preferredSticky == nil {
				return 0
			}
			return preferredSticky.ChannelKeyID
		}())
	result := runWSRelay(ctx, req, group)
	if result.ResetConversation && autoRestart && !result.Written && !req.streamWriter.Written() {
		log.Debugf("ws relay switching to replay (apikey=%d, request_model=%s, failed_previous_response_id=%s, reset_conversation=%t)",
			apiKeyID, requestModel, failedPreviousResponseID, result.ResetConversation)
		balancer.DeleteSticky(apiKeyID, requestModel)
		var replayedRequest *transformerModel.InternalLLMRequest
		if conversationState.LastOutboundTypeSet && conversationState.LastOutboundType == outbound.OutboundTypeOpenAIChat {
			replayedRequest = conversationState.BuildChatReplayRequest(originalRequest)
		} else {
			replayedRequest = conversationState.BuildReplayRequest(originalRequest)
		}
		if replayedRequest == nil {
			req.metrics.SaveWithChannelStats(ctx, false, result.Err, req.iter.Attempts(), false)
			rejectWSLocalReplay(ctx, conn, apiKeyID, requestModel, downstreamSessionID)
			return nil
		}
		replayReq, replayGroup, replayErr := newWSRelayRequest(ctx, conn, inAdapter, apiKeyID, requestModel, replayedRequest, originalRequest, preferredSticky, nil)
		if replayErr == nil {
			replayReq.metrics.SetWSMode(dbmodel.RelayLogWSModeReplay)
			replayReq.metrics.SetWSRecovery(dbmodel.RelayLogWSRecoveryReplay)
			// Replay uses a replacement request/iterator, but it is still one
			// client request. Carry the failed continuation decisions forward so
			// the final request-level log retains every attempt and numbering
			// continues after the initial relay phase.
			replayReq.iter.AppendAttemptHistory(req.iter.Attempts())
			req = replayReq
			group = replayGroup
			result = runWSRelay(ctx, req, group)
		}
	}

	result = finalizeWSRelay(ctx, conn, req, result)
	if result.Success {
		if conversationState == nil {
			conversationState = &wsConversationState{DownstreamSessionID: downstreamSessionID}
		}
		conversationState.DownstreamSessionID = downstreamSessionID
		if channelID, keyID := finalChannelKey(req.iter.Attempts()); channelID > 0 {
			conversationState.ChannelID = channelID
			conversationState.ChannelKeyID = keyID
		}
		conversationState.LastOutboundType = result.OutboundType
		conversationState.LastOutboundTypeSet = true
		if req.metrics.WSMode != nil && *req.metrics.WSMode == dbmodel.RelayLogWSModeReplay {
			conversationState.RememberReplayAlias(failedPreviousResponseID)
			conversationState.MarkReplayRecovered(originalRequest)
		} else {
			conversationState.MarkNativeContinuationReady()
		}
		conversationState.ApplySuccessfulTurn(originalRequest, req.metrics.InternalResponse)
		storeWSConversationState(apiKeyID, requestModel, conversationState, wsConversationStateTTL(group.SessionKeepTime))
		log.Debugf("ws relay success state stored (apikey=%d, request_model=%s, ws_mode=%v, ws_recovery=%v, last_response_id=%s, outbound_type=%d, channel=%d, key=%d)",
			apiKeyID, requestModel, req.metrics.WSMode, req.metrics.WSRecovery,
			strings.TrimSpace(conversationState.LastResponseID), conversationState.LastOutboundType,
			conversationState.ChannelID, conversationState.ChannelKeyID)
		return conversationState
	}
	if result.ResetConversation {
		log.Debugf("ws relay clearing conversation state (apikey=%d, request_model=%s, err=%v)", apiKeyID, requestModel, result.Err)
		deleteWSConversationState(apiKeyID, requestModel, downstreamSessionID)
		return nil
	}

	return conversationState
}

func rejectWSLocalReplay(ctx context.Context, conn *websocket.Conn, apiKeyID int, requestModel, downstreamSessionID string) {
	log.Warnf("ws local replay merge failed (apikey=%d, request_model=%s), refusing to forward local response id", apiKeyID, requestModel)
	deleteWSConversationState(apiKeyID, requestModel, downstreamSessionID)
	writeWSError(ctx, conn, http.StatusConflict, "conversation_restart_required", "本地会话无法安全重放，请重新开启对话")
}

func bestEffortWarmupUpstreamWS(
	ctx context.Context,
	apiKeyID int,
	supportedModels string,
	reqBody map[string]json.RawMessage,
) error {
	requestModel := strings.TrimSpace(extractWSRequestModel(reqBody))
	if requestModel == "" {
		return fmt.Errorf("warmup request missing model")
	}

	if supportedModels != "" {
		supportedModelsArray := strings.Split(supportedModels, ",")
		found := false
		for _, modelName := range supportedModelsArray {
			if modelName == requestModel {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("model not supported")
		}
	}

	group, err := op.GroupGetEnabledMap(requestModel, ctx)
	if err != nil {
		return fmt.Errorf("model not found")
	}

	iter := balancer.NewIterator(group, apiKeyID, requestModel)
	if iter.Len() == 0 {
		return fmt.Errorf("no available channel")
	}

	var lastErr error
	for iter.Next() {
		item := iter.Item()

		channel, err := op.ChannelGet(item.ChannelID, ctx)
		if err != nil {
			lastErr = err
			continue
		}
		if !channel.Enabled || !isOpenAIProtocolChannel(channel.Type) ||
			!channelAllowsOutboundProtocol(channel, outbound.OutboundTypeOpenAIResponse) {
			continue
		}

		selectOpts := dbmodel.ChannelKeySelectOptions{
			ExcludeKeyIDs:  make(map[int]struct{}),
			PreferredKeyID: iter.StickyKeyID(),
		}

		for {
			usedKey := channel.GetChannelKey(selectOpts)
			if usedKey.ChannelKey == "" {
				break
			}
			if iter.SkipCircuitBreak(channel.ID, usedKey.ID, channel.Name) {
				selectOpts.ExcludeKeyIDs[usedKey.ID] = struct{}{}
				continue
			}

			if err := warmupUpstreamWSConnection(ctx, channel, usedKey); err != nil {
				lastErr = err
				selectOpts.ExcludeKeyIDs[usedKey.ID] = struct{}{}
				continue
			}

			balancer.SetSticky(apiKeyID, requestModel, channel.ID, usedKey.ID)
			return nil
		}
	}

	if lastErr != nil {
		return lastErr
	}
	return fmt.Errorf("no ws-capable channel available for warmup")
}

func extractWSRequestModel(reqBody map[string]json.RawMessage) string {
	if len(reqBody) == 0 {
		return ""
	}
	modelRaw, ok := reqBody["model"]
	if !ok {
		return ""
	}
	var requestModel string
	if err := json.Unmarshal(modelRaw, &requestModel); err != nil {
		return ""
	}
	return requestModel
}

func warmupUpstreamWSConnection(ctx context.Context, channel *dbmodel.Channel, usedKey dbmodel.ChannelKey) error {
	warmupCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()

	pc := TryUpstreamWS(warmupCtx, channel, channel.GetBaseUrl(), usedKey.ChannelKey, usedKey.ID, nil)
	if pc == nil {
		return fmt.Errorf("upstream ws unavailable")
	}

	wsUpstreamPool.Put(pc)
	return nil
}

func newWSRelayRequest(
	ctx context.Context,
	conn *websocket.Conn,
	inAdapter transformerModel.Inbound,
	apiKeyID int,
	requestModel string,
	executionRequest *transformerModel.InternalLLMRequest,
	metricsRequest *transformerModel.InternalLLMRequest,
	preferredSticky *balancer.SessionEntry,
	rawBody []byte,
) (*relayRequest, *dbmodel.Group, error) {
	if executionRequest == nil {
		return nil, nil, fmt.Errorf("internal request is nil")
	}
	group, err := op.GroupGetEnabledMap(requestModel, ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("model not found")
	}

	iter := balancer.NewIteratorWithPreference(group, apiKeyID, requestModel, preferredSticky)
	if iter.Len() == 0 {
		return nil, nil, fmt.Errorf("no available channel")
	}
	applyProtocolPreferenceForMode(group.Mode, executionRequest, iter, ctx)

	if executionRequest != nil && executionRequest.IsOpenAIExactReplayRequest() {
		rawBody = nil
	}

	return &relayRequest{
		c:                 nil,
		ctx:               ctx,
		inAdapter:         inAdapter,
		newInboundAdapter: func() transformerModel.Inbound { return inbound.Get(inbound.InboundTypeOpenAIResponse) },
		internalRequest:   executionRequest,
		metrics:           NewRelayMetrics(apiKeyID, requestModel, rawBody, metricsRequest),
		apiKeyID:          apiKeyID,
		requestModel:      requestModel,
		groupID:           group.ID,
		groupSessionTTL:   group.SessionKeepTime,
		iter:              iter,
		rawBody:           append([]byte(nil), rawBody...),
		streamWriter:      NewWSStreamWriter(ctx, conn),
	}, &group, nil
}

func runWSRelay(ctx context.Context, req *relayRequest, group *dbmodel.Group) wsRelayResult {
	replayExact := req != nil && req.internalRequest != nil && req.internalRequest.IsOpenAIExactReplayRequest()
	relayCtx := ctx
	if replayExact {
		budget := 15 * time.Second
		if deadline, ok := ctx.Deadline(); ok {
			if remaining := time.Until(deadline); remaining > 0 && remaining < budget {
				budget = remaining
			}
		}
		var cancel context.CancelFunc
		relayCtx, cancel = context.WithTimeoutCause(ctx, budget, errLocalRelayBudgetExceeded)
		defer cancel()
		if req != nil {
			req.ctx = relayCtx
		}
	}

	maxSameChannelRetries := 1
	if group.RetryEnabled {
		maxSameChannelRetries = group.MaxRetries
		if maxSameChannelRetries <= 0 {
			maxSameChannelRetries = 3
		}
	}

	var lastErr error
	var lastResult attemptResult
	nativeResponsesRequired := requiresNativeResponsesUpstream(req.internalRequest)
	var nativeResponses nativeResponsesAvailability
	maxChannelAttempts := req.iter.Len()
	if replayExact && maxChannelAttempts > 3 {
		maxChannelAttempts = 3
	}

	for req.iter.Next() {
		if req.iter.Index() >= maxChannelAttempts {
			break
		}
		select {
		case <-relayCtx.Done():
			if isLocalRelayBudgetExceeded(relayCtx, contextError(relayCtx)) {
				publicErr := wsPublicError{
					Status:  http.StatusGatewayTimeout,
					Code:    "replay_recovery_timeout",
					Message: "exact replay 恢复超过本地 15 秒预算，请重试",
				}
				return wsRelayResult{Err: contextError(relayCtx), PublicError: &publicErr}
			}
			return wsRelayResult{Canceled: true, Err: relayCtx.Err()}
		default:
		}

		item := req.iter.Item()

		channel, err := op.ChannelGet(item.ChannelID, ctx)
		if err != nil {
			req.iter.Skip(item.ChannelID, 0, fmt.Sprintf("channel_%d", item.ChannelID), fmt.Sprintf("channel not found: %v", err))
			lastErr = err
			if nativeResponsesRequired {
				nativeResponses.markUnavailable()
			}
			continue
		}
		if !channel.Enabled {
			req.iter.Skip(channel.ID, 0, channel.Name, "channel disabled")
			if nativeResponsesRequired && isOpenAIProtocolChannel(channel.Type) {
				nativeResponses.markCandidate(channel.Type)
				nativeResponses.markUnavailable()
			}
			continue
		}

		if nativeResponsesRequired {
			nativeResponses.markCandidate(channel.Type)
			if !isOpenAIProtocolChannel(channel.Type) {
				req.iter.Skip(channel.ID, 0, channel.Name, "native openai responses upstream required")
				continue
			}
		}

		outboundType, protocolCompatible := outboundTypeForChannel(req.internalRequest, channel)
		if !protocolCompatible {
			req.iter.Skip(channel.ID, 0, channel.Name, "channel protocol capability is incompatible with request")
			if nativeResponsesRequired && isOpenAIProtocolChannel(channel.Type) {
				nativeResponses.markEndpointUnsupported()
			}
			continue
		}
		outAdapter := outbound.Get(outboundType)
		if outAdapter == nil {
			req.iter.Skip(channel.ID, 0, channel.Name, fmt.Sprintf("unsupported channel type: %d", channel.Type))
			continue
		}

		if !outbound.IsChatChannelType(channel.Type) {
			req.iter.Skip(channel.ID, 0, channel.Name, "channel type not compatible with chat request")
			continue
		}

		req.internalRequest.Model = item.ModelName

		selectOpts := dbmodel.ChannelKeySelectOptions{
			ExcludeKeyIDs:  make(map[int]struct{}),
			PreferredKeyID: req.iter.StickyKeyID(),
		}

		var usedKey dbmodel.ChannelKey
		for {
			usedKey = channel.GetChannelKey(selectOpts)
			if usedKey.ChannelKey == "" {
				break
			}
			if !req.iter.SkipCircuitBreak(channel.ID, usedKey.ID, channel.Name) {
				break
			}
			selectOpts.ExcludeKeyIDs[usedKey.ID] = struct{}{}
			usedKey = dbmodel.ChannelKey{}
		}
		if usedKey.ChannelKey == "" {
			if len(selectOpts.ExcludeKeyIDs) == 0 {
				req.iter.Skip(channel.ID, 0, channel.Name, "no available key")
			}
			if nativeResponsesRequired && isOpenAIProtocolChannel(channel.Type) {
				nativeResponses.markUnavailable()
			}
			continue
		}

		log.Debugf("ws request model %s, forwarding to channel: %s model: %s (attempt %d/%d)",
			req.requestModel, channel.Name, item.ModelName, req.iter.Index()+1, req.iter.Len())

		var result attemptResult
		for retryNum := 0; retryNum < maxSameChannelRetries; retryNum++ {
			if retryNum > 0 {
				delay := computeBackoff(retryNum, result.RetryAfter)
				select {
				case <-relayCtx.Done():
					if isLocalRelayBudgetExceeded(relayCtx, contextError(relayCtx)) {
						publicErr := wsPublicError{
							Status:  http.StatusGatewayTimeout,
							Code:    "replay_recovery_timeout",
							Message: "exact replay 恢复超过本地 15 秒预算，请重试",
						}
						return wsRelayResult{Err: contextError(relayCtx), PublicError: &publicErr}
					}
					return wsRelayResult{Canceled: true, Err: relayCtx.Err()}
				case <-time.After(delay):
				}

				// 与 HTTP Handler 一致：每轮重试从更新后的通道能力重选出站
				// 协议，刚学习 unsupported 的端点不再被同渠道重试打到；
				// 候选顺序（weighted/sticky）保持不变。
				recomputedOutboundType, recomputedCompatible := outboundTypeForChannel(req.internalRequest, channel)
				if !recomputedCompatible {
					log.Debugf("ws same-channel retry for %s skipped: protocol capability incompatible after learning", channel.Name)
					break
				}
				outboundType = recomputedOutboundType
				outAdapter = outbound.Get(outboundType)
			}

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
			if result.Success || result.Written || result.Canceled || isDownstreamWriteFailure(result) || result.TerminalOutcome != transformerModel.PassthroughTerminalOutcomeNone || result.ResetConversation || result.FirstTokenTimeout || !isRetryableStatus(result.StatusCode) {
				break
			}
		}

		if nativeResponsesRequired {
			nativeResponses.markAttempt(result, shouldTryProtocolFallbackForAttempt(req.internalRequest, channel, result.OutboundType, result.StatusCode, result.protocolError()))
		}

		// A partial WS stream is committed to the downstream client and cannot
		// be retried, but it is still a hard failure for circuit/outlier health.
		recordIncompleteUpstreamFailure(channel.ID, usedKey.ID, req.internalRequest.Model, result)
		recordWrittenStructuredUpstreamFailure(channel.ID, usedKey.ID, req.internalRequest.Model, group.RetryEnabled, result)
		forceHard := replayExact && result.StatusCode == http.StatusServiceUnavailable &&
			isNoAvailableAccountError(upstreamClassificationMessage(result.Err))
		recordFinalAttemptFailure(
			channel.ID, usedKey.ID, req.internalRequest.Model, group.RetryEnabled, forceHard, result,
		)

		if result.Success {
			outlierwindow.Report(channel.ID, true, result.StatusCode, time.Now())
			var respID string
			if req.metrics.InternalResponse != nil {
				respID = req.metrics.InternalResponse.ID
			}
			return wsRelayResult{Success: true, ResponseID: respID, OutboundType: result.OutboundType}
		}
		if isDownstreamWriteFailure(result) {
			// The downstream connection is unusable even when its failed write
			// accepted zero bytes. Mark the relay result committed so finalization
			// neither retries another channel nor writes a second error frame.
			return wsRelayResult{Written: true, Err: result.Err}
		}
		if result.Canceled || result.Written || result.TerminalOutcome != transformerModel.PassthroughTerminalOutcomeNone {
			return wsRelayResult{
				ResetConversation: result.ResetConversation,
				Written:           result.Written,
				Canceled:          result.Canceled,
				Err:               result.Err,
			}
		}
		if result.ResetConversation {
			if publicErr, ok := classifyWSPublicError(result.Err, result.StatusCode); ok {
				return wsRelayResult{ResetConversation: publicErr.ResetConversation, Err: result.Err, PublicError: &publicErr}
			}
			return wsRelayResult{ResetConversation: true, Err: result.Err}
		}
		lastErr = result.Err
		lastResult = result
	}

	if nativeResponsesRequired && nativeResponses.shouldReportCapabilityError() {
		publicErr := wsPublicError{
			Status:  http.StatusBadRequest,
			Code:    "native_responses_required",
			Message: "当前请求包含 OpenAI Responses 原生能力，仅支持 OpenAI Responses 通道直通",
		}
		return wsRelayResult{Err: protocolFallbackError(req.internalRequest), PublicError: &publicErr}
	}
	if nativeResponsesRequired && nativeResponses.unavailableBeforeAttempt() {
		publicErr := wsPublicError{
			Status:  http.StatusServiceUnavailable,
			Code:    "native_responses_unavailable",
			Message: "OpenAI Responses 上游暂时不可用，请稍后重试",
		}
		return wsRelayResult{Err: fmt.Errorf("native openai responses upstream temporarily unavailable"), PublicError: &publicErr}
	}
	if publicErr, ok := classifyWSPublicError(lastErr, lastResult.StatusCode); ok {
		return wsRelayResult{ResetConversation: publicErr.ResetConversation, Err: lastErr, PublicError: &publicErr}
	}
	return wsRelayResult{Err: lastErr}
}

func finalizeWSRelay(ctx context.Context, conn *websocket.Conn, req *relayRequest, result wsRelayResult) wsRelayResult {
	if result.Success {
		req.metrics.SaveWithChannelStats(ctx, true, nil, req.iter.Attempts(), false)
		return result
	}

	req.metrics.SaveWithChannelStats(ctx, false, result.Err, req.iter.Attempts(), false)
	if result.Canceled || result.Written {
		return result
	}
	if result.PublicError != nil {
		if result.PublicError.ResetConversation {
			balancer.DeleteSticky(req.apiKeyID, req.requestModel)
		}
		writeWSError(ctx, conn, result.PublicError.Status, result.PublicError.Code, result.PublicError.Message)
		return result
	}
	writeWSError(ctx, conn, 502, "all_channels_failed", "All channels failed")
	return result
}

func injectWSPreviousResponseID(reqBody map[string]json.RawMessage, state *wsConversationState) {
	// Local continuation now prefers exact replay over implicit previous_response_id injection.
	// Explicit client-supplied previous_response_id is still preserved elsewhere.
}

func rewriteWSPreviousResponseID(reqBody map[string]json.RawMessage, state *wsConversationState) {
	if state == nil || len(reqBody) == 0 {
		return
	}
	raw, ok := reqBody["previous_response_id"]
	if !ok || len(raw) == 0 {
		return
	}
	var previousResponseID string
	if err := json.Unmarshal(raw, &previousResponseID); err != nil {
		return
	}
	if !state.ShouldRewritePreviousResponseID(previousResponseID) {
		return
	}
	reqBody["previous_response_id"] = json.RawMessage(fmt.Sprintf("%q", state.LastResponseID))
}

func currentPreviousResponseID(req *transformerModel.InternalLLMRequest) string {
	return req.OpenAIPreviousResponseID()
}

func wsRequestExplicitlyRequestsContinuation(reqBody map[string]json.RawMessage) bool {
	if len(reqBody) == 0 {
		return false
	}
	if raw, ok := reqBody["previous_response_id"]; ok && len(raw) > 0 {
		var previousResponseID string
		if err := json.Unmarshal(raw, &previousResponseID); err == nil && strings.TrimSpace(previousResponseID) != "" {
			return true
		}
	}
	if raw, ok := reqBody["conversation"]; ok && len(raw) > 0 && string(raw) != "null" {
		return true
	}
	return false
}

func writeWSError(ctx context.Context, conn *websocket.Conn, status int, code, message string) {
	errEvent := map[string]interface{}{
		"type":   "error",
		"status": status,
		"error": map[string]interface{}{
			"type":    "invalid_request_error",
			"code":    code,
			"message": message,
		},
	}
	if err := writeWSEvent(ctx, conn, errEvent); err != nil {
		log.Debugf("ws error event write failed: %v", err)
	}
}

func finalChannelKey(attempts []dbmodel.ChannelAttempt) (int, int) {
	var lastChannelID int
	var lastChannelKeyID int
	for i := len(attempts) - 1; i >= 0; i-- {
		attempt := attempts[i]
		if attempt.Status == dbmodel.AttemptSuccess {
			return attempt.ChannelID, attempt.ChannelKeyID
		}
		if attempt.Status == dbmodel.AttemptFailed && lastChannelID == 0 {
			lastChannelID = attempt.ChannelID
			lastChannelKeyID = attempt.ChannelKeyID
		}
	}
	return lastChannelID, lastChannelKeyID
}
