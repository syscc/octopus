package relay

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/bestruirui/octopus/internal/model"
	"github.com/bestruirui/octopus/internal/op"
	sitesvc "github.com/bestruirui/octopus/internal/site"
	"github.com/bestruirui/octopus/internal/transformer/inbound"
	"github.com/bestruirui/octopus/internal/utils/log"
	"github.com/bestruirui/octopus/internal/utils/safe"
)

func detectRouteMismatchTarget(inboundType inbound.InboundType, statusCode int, err error) (model.SiteModelRouteType, bool) {
	if err == nil || !routeLearningStatusEligible(statusCode) {
		return "", false
	}
	message := upstreamClassificationMessage(err)
	if hasRouteLearningBlockingMarker(message) {
		return "", false
	}
	switch {
	case hasAnthropicRouteLearningEvidence(message):
		return model.SiteModelRouteTypeAnthropic, true
	case hasResponsesRouteLearningEvidence(statusCode, err, message):
		return model.SiteModelRouteTypeOpenAIResponse, true
	case inboundType == inbound.InboundTypeOpenAIChat && hasStreamRouteLearningEvidence(statusCode, message):
		return model.SiteModelRouteTypeOpenAIResponse, true
	default:
		return "", false
	}
}

func hasAnthropicRouteLearningEvidence(message string) bool {
	for _, marker := range []string{
		"use /v1/messages", "use /messages", "send requests to /v1/messages",
		"requires /v1/messages", "expected /v1/messages", "only /v1/messages",
	} {
		if strings.Contains(message, marker) {
			return true
		}
	}
	if !strings.Contains(message, "anthropic-version") {
		return false
	}
	for _, marker := range []string{"required", "missing", "must include"} {
		if strings.Contains(message, marker) {
			return true
		}
	}
	return false
}

func hasResponsesRouteLearningEvidence(statusCode int, err error, message string) bool {
	mentionsEndpoint := strings.Contains(message, "/responses")
	if mentionsEndpoint && (isOpenAIEndpointRoutingError(statusCode, err) || hasOpenAIInvalidEndpointURL(message)) {
		return true
	}
	if mentionsEndpoint && hasEndpointCapabilityMarker(message) {
		switch statusCode {
		case http.StatusNotFound, http.StatusMethodNotAllowed, http.StatusNotImplemented:
			return true
		}
	}
	for _, marker := range []string{
		"use /v1/responses", "use /responses", "use the responses api", "use responses api",
		"requires the responses api", "requires responses api", "only available through the responses api",
		"only supported by the responses api",
	} {
		if strings.Contains(message, marker) {
			return true
		}
	}
	return false
}

func hasStreamRouteLearningEvidence(statusCode int, message string) bool {
	if statusCode != http.StatusOK {
		return false
	}
	for _, marker := range []string{
		"unexpected text/event-stream response",
		"upstream returned text/event-stream",
		"unexpected content-type \"text/event-stream\"",
	} {
		if strings.Contains(message, marker) {
			return true
		}
	}
	return false
}

func routeLearningStatusEligible(statusCode int) bool {
	switch statusCode {
	case http.StatusOK, http.StatusBadRequest, http.StatusNotFound,
		http.StatusMethodNotAllowed, http.StatusUnsupportedMediaType,
		http.StatusUpgradeRequired, http.StatusNotImplemented:
		return true
	default:
		return false
	}
}

func hasRouteLearningBlockingMarker(message string) bool {
	for _, marker := range []string{
		"model_not_found", "model not found", "previous_response_id", "previous response",
		"conversation", "unauthorized", "forbidden", "permission", "authentication",
		"invalid api key", "invalid_api_key", "api key", "credential",
		"insufficient_quota", "quota", "rate_limit", "rate limit",
		"request blocked", "blocked by", "waf",
	} {
		if strings.Contains(message, marker) {
			return true
		}
	}
	return false
}

func maybeLearnManagedRoute(ctx context.Context, channelID int, modelName string, inboundType inbound.InboundType, statusCode int, err error) {
	targetRouteType, ok := detectRouteMismatchTarget(inboundType, statusCode, err)
	if !ok || strings.TrimSpace(modelName) == "" {
		return
	}
	binding, bindingErr := op.SiteChannelBindingGetByChannelID(channelID, ctx)
	if bindingErr != nil || binding == nil {
		return
	}
	groupKey := model.NormalizeSiteGroupKey(binding.GroupKey)
	if strings.Contains(groupKey, "::") {
		base, _, found := strings.Cut(groupKey, "::")
		if found {
			groupKey = model.NormalizeSiteGroupKey(base)
		}
	}
	reason := fmt.Sprintf("upstream route mismatch: target=%s status=%d", targetRouteType, statusCode)
	updated, err := op.SiteModelRouteUpdateIfNotManual(binding.SiteAccountID, groupKey, modelName, targetRouteType, model.SiteModelRouteSourceRuntimeLearned, reason, ctx)
	if err != nil {
		log.Warnf("failed to learn managed route (channel=%d model=%s): %v", channelID, modelName, err)
		return
	}
	if !updated {
		return
	}
	accountID := binding.SiteAccountID
	safe.Go("relay-learned-route-project", func() {
		projCtx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		if _, err := sitesvc.ProjectAccount(projCtx, accountID); err != nil {
			log.Warnf("background ProjectAccount failed (account=%d): %v", accountID, err)
		}
	})
}
