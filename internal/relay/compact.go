package relay

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/bestruirui/octopus/internal/helper"
	dbmodel "github.com/bestruirui/octopus/internal/model"
	"github.com/bestruirui/octopus/internal/op"
	"github.com/bestruirui/octopus/internal/outlierwindow"
	"github.com/bestruirui/octopus/internal/relay/balancer"
	"github.com/bestruirui/octopus/internal/server/resp"
	transformerModel "github.com/bestruirui/octopus/internal/transformer/model"
	"github.com/bestruirui/octopus/internal/transformer/outbound"
	openaiOutbound "github.com/bestruirui/octopus/internal/transformer/outbound/openai"
	"github.com/bestruirui/octopus/internal/utils/httpbody"
	"github.com/bestruirui/octopus/internal/utils/log"
	"github.com/gin-gonic/gin"
)

type responsesCompactRequest struct {
	Model              string          `json:"model"`
	Input              json.RawMessage `json:"input,omitempty"`
	PreviousResponseID *string         `json:"previous_response_id,omitempty"`
}

type responsesCompactResponse struct {
	ID        string                         `json:"id"`
	Object    string                         `json:"object"`
	CreatedAt int64                          `json:"created_at"`
	Output    []openaiOutbound.ResponsesItem `json:"output"`
	Usage     *openaiOutbound.ResponsesUsage `json:"usage,omitempty"`
	Error     *transformerModel.ErrorDetail  `json:"error,omitempty"`
}

// HandleResponsesCompact proxies OpenAI-compatible /responses/compact requests upstream.
func HandleResponsesCompact(c *gin.Context) {
	body, err := httpbody.ReadRequest(c.Request, httpbody.MaxLLMRequestBodyBytes)
	if err != nil {
		if errors.Is(err, httpbody.ErrRequestBodyTooLarge) {
			resp.Error(c, http.StatusRequestEntityTooLarge, "request body too large")
		} else {
			resp.Error(c, http.StatusInternalServerError, err.Error())
		}
		return
	}

	var compactReq responsesCompactRequest
	if err := json.Unmarshal(body, &compactReq); err != nil {
		resp.Error(c, http.StatusBadRequest, fmt.Sprintf("failed to decode responses compact request: %v", err))
		return
	}
	if strings.TrimSpace(compactReq.Model) == "" {
		resp.Error(c, http.StatusBadRequest, "model is required")
		return
	}
	if len(compactReq.Input) == 0 && compactReq.PreviousResponseID == nil {
		resp.Error(c, http.StatusBadRequest, "either input or previous_response_id is required")
		return
	}

	supportedModels := c.GetString("supported_models")
	if supportedModels != "" {
		supportedModelsArray := strings.Split(supportedModels, ",")
		if !slices.Contains(supportedModelsArray, compactReq.Model) {
			resp.ErrorWithCode(c, http.StatusBadRequest, CodeRelayModelNotSupported, "model not supported")
			return
		}
	}

	requestModel := compactReq.Model
	apiKeyID := c.GetInt("api_key_id")

	group, err := op.GroupGetEnabledMap(requestModel, c.Request.Context())
	if err != nil {
		resp.ErrorWithCode(c, http.StatusNotFound, CodeRelayModelNotFound, "model not found")
		return
	}

	iter := balancer.NewIterator(group, apiKeyID, requestModel)
	if iter.Len() == 0 {
		resp.ErrorWithCode(c, http.StatusServiceUnavailable, CodeRelayNoAvailableChannel, "no available channel")
		return
	}

	metricsReq := &transformerModel.InternalLLMRequest{Model: requestModel, RawRequest: body}
	metrics := NewRelayMetrics(apiKeyID, requestModel, body, metricsReq)

	var lastErr error
	var lastStatusCode int
	var lastRetryAfter time.Duration

	maxSameChannelRetries := 1
	if group.RetryEnabled {
		maxSameChannelRetries = group.MaxRetries
		if maxSameChannelRetries <= 0 {
			maxSameChannelRetries = 3
		}
	}

	for iter.Next() {
		select {
		case <-c.Request.Context().Done():
			log.Infof("compact request context canceled, stopping retry")
			metrics.SaveWithChannelStats(c.Request.Context(), false, context.Canceled, iter.Attempts(), false)
			return
		default:
		}

		item := iter.Item()
		metrics.ActualModel = item.ModelName
		channel, err := op.ChannelGet(item.ChannelID, c.Request.Context())
		if err != nil {
			iter.Skip(item.ChannelID, 0, fmt.Sprintf("channel_%d", item.ChannelID), fmt.Sprintf("channel not found: %v", err))
			lastErr = err
			continue
		}
		if !channel.Enabled {
			iter.Skip(channel.ID, 0, channel.Name, "channel disabled")
			continue
		}
		if !supportsResponsesCompact(channel.Type) {
			iter.Skip(channel.ID, 0, channel.Name, "channel type not compatible with responses compact")
			continue
		}

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
			continue
		}

		var attemptErr error
		var statusCode int
		var retryAfter time.Duration
		var success bool

		for retryNum := 0; retryNum < maxSameChannelRetries; retryNum++ {
			if retryNum > 0 {
				delay := computeBackoff(retryNum, retryAfter)
				select {
				case <-c.Request.Context().Done():
					metrics.SaveWithChannelStats(c.Request.Context(), false, context.Canceled, iter.Attempts(), false)
					return
				case <-time.After(delay):
				}
			}

			statusCode, retryAfter, attemptErr = forwardResponsesCompact(c, metrics, iter, channel, usedKey, item.ModelName, body)
			if attemptErr == nil {
				success = true
				break
			}
			if isDownstreamWriteError(attemptErr) || !isRetryableStatus(statusCode) {
				break
			}
		}

		usedKey.StatusCode = statusCode
		usedKey.LastUseTimeStamp = time.Now().Unix()
		op.ChannelKeyUpdate(usedKey)

		if success {
			balancer.RecordSuccess(channel.ID, usedKey.ID, item.ModelName)
			balancer.SetSticky(apiKeyID, requestModel, channel.ID, usedKey.ID)
			outlierwindow.Report(channel.ID, true, statusCode, time.Now())
			metrics.SaveWithChannelStats(c.Request.Context(), true, nil, iter.Attempts(), false)
			return
		}

		if isDownstreamWriteError(attemptErr) {
			// Retrying cannot repair an unusable downstream and would duplicate the
			// already completed upstream compaction request.
			metrics.SaveWithChannelStats(c.Request.Context(), false, attemptErr, iter.Attempts(), false)
			return
		}

		// 客户端取消与普通 relay 语义一致：取消不算上游硬失败，
		// 不记渠道失败统计，也不进熔断/outlier。
		if isClientCancellation(c.Request.Context(), attemptErr) {
			metrics.SaveWithChannelStats(c.Request.Context(), false, attemptErr, iter.Attempts(), false)
			return
		}

		failureKind := circuitFailureKind(group.RetryEnabled, statusCode)
		balancer.RecordFailure(channel.ID, usedKey.ID, item.ModelName, failureKind)
		outlierwindow.Report(channel.ID, false, statusCode, time.Now())
		lastErr = attemptErr
		lastStatusCode = statusCode
		lastRetryAfter = retryAfter
	}

	metrics.SaveWithChannelStats(c.Request.Context(), false, lastErr, iter.Attempts(), false)
	if lastErr == nil && lastStatusCode == 0 {
		resp.ErrorWithCode(c, http.StatusServiceUnavailable, CodeRelayNoAvailableChannel, "no available channel")
		return
	}
	if isPassthroughStatus(lastStatusCode) {
		if lastRetryAfter > 0 {
			c.Header("Retry-After", fmt.Sprintf("%d", int(lastRetryAfter.Seconds())))
		}
		resp.Error(c, lastStatusCode, "channel failed")
		return
	}
	if lastStatusCode > 0 {
		resp.Error(c, lastStatusCode, "channel failed")
		return
	}
	resp.Error(c, http.StatusBadGateway, "channel failed")
}

func supportsResponsesCompact(channelType outbound.OutboundType) bool {
	switch channelType {
	case outbound.OutboundTypeOpenAIResponse:
		return true
	default:
		return false
	}
}

func forwardResponsesCompact(c *gin.Context, metrics *RelayMetrics, iter *balancer.Iterator, channel *dbmodel.Channel, usedKey dbmodel.ChannelKey, actualModel string, requestBody []byte) (statusCode int, retryAfter time.Duration, returnErr error) {
	span := iter.StartAttempt(channel.ID, usedKey.ID, channel.Name)
	defer func() {
		// Keep one channel statistic per real upstream attempt. Downstream write
		// failures and client cancellation are request-scoped and health-neutral.
		stats := dbmodel.StatsMetrics{WaitTime: span.Duration().Milliseconds()}
		if returnErr == nil {
			stats.RequestSuccess = 1
		} else if isDownstreamWriteError(returnErr) || isClientCancellation(c.Request.Context(), returnErr) {
			return
		} else {
			stats.RequestFailed = 1
		}
		_ = op.StatsChannelUpdate(channel.ID, stats)
	}()

	request, err := buildResponsesCompactRequest(c.Request.Context(), channel, usedKey.ChannelKey, actualModel, requestBody)
	if err != nil {
		span.End(dbmodel.AttemptFailed, 0, err.Error())
		return 0, 0, fmt.Errorf("failed to create compact request: %w", err)
	}
	metrics.ActualModel = actualModel
	metrics.SetTransportRequestPayload(requestBody, actualModel)
	copyProxyHeaders(c.Request.Header, channel, request.Header)
	// Normalize all credential aliases after client/channel header merging. The
	// selected key remains authoritative even if a manually constructed header
	// map used a non-canonical spelling.
	applySelectedCredentialHeader(request.Header, channel.Type, usedKey.ChannelKey)

	response, err := sendCompactRequest(channel, request)
	if err != nil {
		span.End(dbmodel.AttemptFailed, 0, err.Error())
		return 0, 0, fmt.Errorf("failed to send compact request: %w", err)
	}
	defer response.Body.Close()

	if response.StatusCode < 200 || response.StatusCode >= 300 {
		body, readErr := io.ReadAll(io.LimitReader(response.Body, httpbody.MaxErrorResponseBodyBytes))
		if readErr != nil {
			span.End(dbmodel.AttemptFailed, response.StatusCode, readErr.Error())
			return response.StatusCode, 0, fmt.Errorf("failed to read compact response body: %w", readErr)
		}
		retryAfter := parseRetryAfter(response.Header.Get("Retry-After"))
		statusCode := normalizeUpstreamStatusCode(response.StatusCode, string(body))
		upstreamErr := newUpstreamHTTPError(statusCode, body)
		span.End(dbmodel.AttemptFailed, statusCode, upstreamErr.Error())
		return statusCode, retryAfter, upstreamErr
	}

	body, readErr := httpbody.ReadAll(response.Body, httpbody.MaxLLMResponseBodyBytes)
	if readErr != nil {
		span.End(dbmodel.AttemptFailed, response.StatusCode, readErr.Error())
		return response.StatusCode, 0, fmt.Errorf("failed to read compact response body: %w", readErr)
	}

	copyProxyResponseHeaders(c.Writer.Header(), response.Header)
	contentType := response.Header.Get("Content-Type")
	if strings.TrimSpace(contentType) == "" {
		contentType = "application/json"
	}
	if _, writeErr := writeDownstreamResponse(c, response.StatusCode, contentType, body); writeErr != nil {
		span.End(dbmodel.AttemptFailed, response.StatusCode, writeErr.Error())
		return response.StatusCode, 0, writeErr
	}

	var compactResp responsesCompactResponse
	if err := json.Unmarshal(body, &compactResp); err == nil {
		metrics.SetInternalResponse(compactResponseToInternalResponse(&compactResp), actualModel)
	}

	span.End(dbmodel.AttemptSuccess, response.StatusCode, "")
	return response.StatusCode, 0, nil
}

func buildResponsesCompactRequest(ctx context.Context, channel *dbmodel.Channel, key, actualModel string, requestBody []byte) (*http.Request, error) {
	parsedURL, err := url.Parse(strings.TrimSuffix(channel.GetBaseUrl(), "/"))
	if err != nil {
		return nil, fmt.Errorf("failed to parse base url: %w", err)
	}
	parsedURL.Path = parsedURL.Path + "/responses/compact"

	payload := make(map[string]json.RawMessage)
	if err := json.Unmarshal(requestBody, &payload); err != nil {
		return nil, fmt.Errorf("failed to decode compact request: %w", err)
	}
	if payload == nil {
		return nil, errors.New("compact request must be a JSON object")
	}
	modelJSON, err := json.Marshal(actualModel)
	if err != nil {
		return nil, fmt.Errorf("failed to encode compact model: %w", err)
	}
	payload["model"] = modelJSON
	encodedBody, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("failed to encode compact request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, parsedURL.String(), bytes.NewReader(encodedBody))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Authorization", "Bearer "+key)
	return req, nil
}

func copyProxyHeaders(src http.Header, channel *dbmodel.Channel, dst http.Header) {
	if dst == nil {
		return
	}
	for _, name := range []string{"Authorization", "X-API-Key", "X-Goog-Api-Key", "Api-Key"} {
		deleteHeaderCaseInsensitive(dst, name)
	}
	entries := collectNormalizedHeaderEntries(src, func(name string) bool {
		return !isBlockedUpstreamHeader(name) && !strings.EqualFold(name, "content-type")
	})
	for _, entry := range entries {
		setHeaderValuesCaseInsensitive(dst, entry.name, entry.values)
	}
	if channel != nil {
		for _, header := range channel.CustomHeader {
			name := strings.TrimSpace(header.HeaderKey)
			if strings.EqualFold(name, "content-type") || isBlockedChannelHeader(name) {
				continue
			}
			setHeaderValuesCaseInsensitive(dst, name, []string{header.HeaderValue})
		}
	}
	// The request is created with the selected channel key. Protected client
	// and custom credential headers are skipped above, while a configured
	// Cookie remains available as channel-scoped authentication.
	if len(headerValuesCaseInsensitive(dst, "User-Agent")) == 0 {
		setHeaderValuesCaseInsensitive(dst, "User-Agent", []string{""})
	}
}

func copyProxyResponseHeaders(dst http.Header, src http.Header) {
	if dst == nil || src == nil {
		return
	}
	entries := collectNormalizedHeaderEntries(src, func(name string) bool {
		return !isBlockedUpstreamHeader(name)
	})
	for _, entry := range entries {
		setHeaderValuesCaseInsensitive(dst, entry.name, entry.values)
	}
}

func sendCompactRequest(channel *dbmodel.Channel, req *http.Request) (*http.Response, error) {
	httpClient, err := helper.ChannelHTTPClientWithContext(req.Context(), channel)
	if err != nil {
		return nil, err
	}
	return httpClient.Do(req)
}

func compactResponseToInternalResponse(resp *responsesCompactResponse) *transformerModel.InternalLLMResponse {
	if resp == nil {
		return nil
	}
	return &transformerModel.InternalLLMResponse{
		ID:      resp.ID,
		Object:  resp.Object,
		Created: resp.CreatedAt,
		Usage:   convertCompactUsage(resp.Usage),
	}
}

func convertCompactUsage(usage *openaiOutbound.ResponsesUsage) *transformerModel.Usage {
	if usage == nil {
		return nil
	}
	result := &transformerModel.Usage{
		PromptTokens:     usage.InputTokens,
		CompletionTokens: usage.OutputTokens,
		TotalTokens:      usage.TotalTokens,
	}
	if usage.InputTokenDetails.CachedTokens > 0 {
		result.PromptTokensDetails = &transformerModel.PromptTokensDetails{
			CachedTokens: usage.InputTokenDetails.CachedTokens,
		}
	}
	if usage.OutputTokenDetails.ReasoningTokens > 0 {
		result.CompletionTokensDetails = &transformerModel.CompletionTokensDetails{
			ReasoningTokens: usage.OutputTokenDetails.ReasoningTokens,
		}
	}
	return result
}
