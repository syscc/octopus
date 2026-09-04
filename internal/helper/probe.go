package helper

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"

	"github.com/bestruirui/octopus/internal/model"
	"github.com/bestruirui/octopus/internal/utils/httpbody"
)

// ProtocolProbeResult is what one probing round learned about an upstream.
type ProtocolProbeResult struct {
	// Primary is the fallback protocol - what a model whose name classifies to
	// nothing in particular should use. Determined strictly.
	Primary model.SiteModelRouteType
	// Models is the list Primary's endpoint returned.
	Models []string
	// Supported lists every protocol that answered with models of its own kind.
	// A relay can speak several at once; Primary only names the fallback.
	Supported []model.SiteModelRouteType
}

// ProbeModelProtocol asks the same upstream for its model list once per protocol
// and reports both the fallback protocol and everything the upstream speaks. An
// empty Primary always comes with a non-nil error.
//
// This deliberately does not reuse FetchModels. That helper falls back to
// fetchOpenAIModels when the Anthropic/Gemini decoders come up empty, which is
// the right call for fetching but fatal for probing: plenty of relays ignore the
// *name* of the auth header, so an x-api-key request comes back as a full
// OpenAI-shaped list and the fallback would report a false Anthropic hit.
//
// Primary and Supported use different bars on purpose. Primary demands the
// protocol's own payload structure, because picking the wrong fallback mislabels
// every unclassified model. Supported only asks that the endpoint answered with
// models native to that protocol - a relay whose /v1/models always replies in
// OpenAI shape can still serve /v1/messages, and refusing to record that would
// force claude models through a needless conversion.
func ProbeModelProtocol(ctx context.Context, request model.Channel) (ProtocolProbeResult, error) {
	client, err := ChannelHTTPClientWithContext(ctx, &request)
	if err != nil {
		return ProtocolProbeResult{}, err
	}
	baseURL := strings.TrimRight(strings.TrimSpace(request.GetBaseUrl()), "/")
	if baseURL == "" {
		return ProtocolProbeResult{}, errors.New("base url is empty")
	}
	key := request.GetChannelKey().ChannelKey

	outcomes := make([]protocolProbeOutcome, len(protocolProbes))
	var wg sync.WaitGroup
	for index := range protocolProbes {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			outcomes[index] = protocolProbes[index].run(ctx, client, request, baseURL, key)
		}(index)
	}
	wg.Wait()

	result := ProtocolProbeResult{}
	for index, probe := range protocolProbes {
		if len(outcomes[index].models) == 0 {
			continue
		}
		// 只有列表里真的出现了该协议的原生模型才算支持。这样"对任何鉴权头都回
		// OpenAI 列表"的站点不会被误记成支持 Anthropic，而真的代理 claude 的站点
		// 会被记上 —— 它既然转发 claude，原生 /v1/messages 基本都是通的。
		if !hasNativeModelName(outcomes[index].models, probe.routeType) {
			continue
		}
		result.Supported = append(result.Supported, probe.routeType)
	}

	// 跨协议比对：上游对不同鉴权头返回完全相同的模型集，说明它不区分协议。这只
	// 影响"兜底选谁"（选 OpenAI，网关自己会转），不影响上面记录的支持能力。
	var openAIModels []string
	for index, probe := range protocolProbes {
		if probe.routeType == model.SiteModelRouteTypeOpenAIChat {
			openAIModels = outcomes[index].models
		}
	}

	for index, probe := range protocolProbes {
		if !outcomes[index].strict || len(outcomes[index].models) == 0 {
			continue
		}
		if probe.routeType != model.SiteModelRouteTypeOpenAIChat &&
			sameModelSet(outcomes[index].models, openAIModels) {
			continue
		}
		result.Primary = probe.routeType
		result.Models = outcomes[index].models
		return result, nil
	}

	for index, probe := range protocolProbes {
		if outcomes[index].err != nil {
			return ProtocolProbeResult{}, fmt.Errorf("no protocol matched, %s error: %w", probe.routeType, outcomes[index].err)
		}
	}
	return ProtocolProbeResult{}, errors.New("no protocol matched: upstream returned no models")
}

func sameModelSet(left, right []string) bool {
	if len(left) == 0 || len(left) != len(right) {
		return false
	}
	seen := make(map[string]struct{}, len(left))
	for _, name := range left {
		seen[strings.ToLower(strings.TrimSpace(name))] = struct{}{}
	}
	for _, name := range right {
		if _, ok := seen[strings.ToLower(strings.TrimSpace(name))]; !ok {
			return false
		}
	}
	return true
}

type protocolProbeOutcome struct {
	models []string
	// strict reports whether the payload carried this protocol's own structure,
	// not merely models. Only a strict hit may become Primary.
	strict bool
	err    error
}

type protocolProbe struct {
	routeType model.SiteModelRouteType
	// versionSuffix 是该协议模型端点所在的版本段。站点地址没带版本段时补上它，
	// 带了别的版本段时换成它 —— 详见 BuildVersionedBaseURLs。
	versionSuffix string
	fetch         func(*http.Client, context.Context, model.Channel, string, string) ([]string, bool, error)
}

// protocolProbes is ordered by how hard the protocol is to fake. Anthropic and
// Gemini carry structural fingerprints an OpenAI-compatible relay does not
// produce by accident, so they get to claim the site before the permissive
// OpenAI check runs.
var protocolProbes = []protocolProbe{
	{
		routeType:     model.SiteModelRouteTypeAnthropic,
		versionSuffix: "/v1",
		fetch:         probeAnthropicModels,
	},
	{
		routeType:     model.SiteModelRouteTypeGemini,
		versionSuffix: "/v1beta",
		fetch:         probeGeminiModels,
	},
	{
		routeType:     model.SiteModelRouteTypeOpenAIChat,
		versionSuffix: "/v1",
		fetch:         probeOpenAIModels,
	},
}

func (p protocolProbe) run(
	ctx context.Context,
	client *http.Client,
	request model.Channel,
	baseURL string,
	key string,
) protocolProbeOutcome {
	var firstErr error
	for _, candidate := range BuildVersionedBaseURLs(baseURL, p.versionSuffix) {
		models, strict, err := p.fetch(client, ctx, request, candidate, key)
		if err == nil && len(models) > 0 {
			return protocolProbeOutcome{models: models, strict: strict}
		}
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return protocolProbeOutcome{err: firstErr}
}

// BuildVersionedBaseURLs lists the base URLs to try for one protocol, in order.
//
// An address with no version segment gets both spellings: as given, then with the
// protocol's segment appended. That covers a bare host and equally an upstream
// mounted under its own path - https://opencode.ai/zen/go answers on
// /zen/go/v1/models but 404s on /zen/go/models, so both have to be tried.
//
// An address that already ends in a version segment is taken literally and nothing
// is appended. Filling in .../v1 is the user saying "this is the API root"; probing
// .../v1/v1 or swapping in .../v1beta would be guessing against them. Paths carry
// meaning - /zen/go/v1/models and /zen/v1/models serve different model pools on the
// same host - so a configured path is never rewritten.
//
// Probing and steady-state model fetching share this function on purpose. If they
// disagreed, a site could be detected through one spelling on the first sync and
// then fail to fetch anything on the second.
func BuildVersionedBaseURLs(baseURL string, suffix string) []string {
	baseURL = strings.TrimRight(strings.TrimSpace(baseURL), "/")
	if baseURL == "" {
		return nil
	}
	candidates := []string{baseURL}
	if suffix == "" || BaseURLHasVersionSegment(baseURL) {
		return candidates
	}
	return append(candidates, baseURL+suffix)
}

// BaseURLHasVersionSegment reports whether a configured base URL already ends in
// an API version segment. When it does, no caller should append another one: the
// site owner already said which version to talk to, and stacking a second segment
// only builds paths that cannot exist (/v1/v1beta/models).
func BaseURLHasVersionSegment(baseURL string) bool {
	index := strings.LastIndex(baseURL, "/")
	if index < 0 {
		return false
	}
	segment := strings.ToLower(baseURL[index+1:])
	if len(segment) < 2 || segment[0] != 'v' {
		return false
	}
	return segment[1] >= '0' && segment[1] <= '9'
}

// probeAnthropicModels reports a hit only when the payload carries Anthropic's
// own list envelope. Relays that ignore the auth header name answer this exact
// request with an OpenAI-shaped list, and counting that as Anthropic mislabels
// the site - which is how the whole site then fails to fetch any model.
func probeAnthropicModels(
	client *http.Client,
	ctx context.Context,
	request model.Channel,
	baseURL string,
	key string,
) ([]string, bool, error) {
	body, err := probeModelListBody(client, ctx, request, baseURL, map[string]string{
		"X-Api-Key":         key,
		"Anthropic-Version": "2023-06-01",
	})
	if err != nil {
		return nil, false, err
	}
	var result model.AnthropicModelList
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, false, err
	}
	models := make([]string, 0, len(result.Data))
	for _, item := range result.Data {
		if id := strings.TrimSpace(item.ID); id != "" {
			models = append(models, id)
		}
	}
	return models, isAnthropicModelList(body, result, models), nil
}

// isAnthropicModelList decides whether a payload really is Anthropic's model
// list. Looking only for Anthropic's own fields is not enough: relays exist that
// answer every protocol from one endpoint and stuff both vocabularies into the
// same item - OpenAI's object/created/owned_by next to Anthropic's
// type/display_name - while serving gpt-* models. Three conditions must hold:
//
//  1. an item carries Anthropic's type:"model" or display_name
//  2. no item carries OpenAI's `object` field (Anthropic names an item's kind
//     with `type` and has no `object` at all)
//  3. at least one model name natively belongs to Anthropic
//
// Condition 3 is the one a relay cannot fake. It can copy every field of the
// envelope; it cannot make gpt-5.6 a claude model.
func isAnthropicModelList(body []byte, result model.AnthropicModelList, models []string) bool {
	if hasOpenAIItemMarker(body) {
		return false
	}
	anthropicField := false
	for _, item := range result.Data {
		if strings.TrimSpace(item.DisplayName) != "" || strings.EqualFold(strings.TrimSpace(item.Type), "model") {
			anthropicField = true
			break
		}
	}
	if !anthropicField {
		return false
	}
	return hasNativeModelName(models, model.SiteModelRouteTypeAnthropic)
}

// hasOpenAIItemMarker reports whether the list items carry OpenAI's own `object`
// field, which marks the payload as coming from an OpenAI-shaped endpoint.
func hasOpenAIItemMarker(body []byte) bool {
	var envelope struct {
		Data []map[string]json.RawMessage `json:"data"`
	}
	if json.Unmarshal(body, &envelope) != nil {
		return false
	}
	for _, item := range envelope.Data {
		if _, ok := item["object"]; ok {
			return true
		}
	}
	return false
}

// hasNativeModelName reports whether any model name natively belongs to the given
// route type, per the same naming rules the rest of the project classifies by.
func hasNativeModelName(models []string, routeType model.SiteModelRouteType) bool {
	for _, name := range models {
		if model.InferSiteModelRouteType(name) == routeType {
			return true
		}
	}
	return false
}

func probeGeminiModels(
	client *http.Client,
	ctx context.Context,
	request model.Channel,
	baseURL string,
	key string,
) ([]string, bool, error) {
	body, err := probeModelListBody(client, ctx, request, baseURL, map[string]string{
		"X-Goog-Api-Key": key,
	})
	if err != nil {
		return nil, false, err
	}
	var result model.GeminiModelList
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, false, err
	}
	models := make([]string, 0, len(result.Models))
	for _, item := range result.Models {
		if name := strings.TrimSpace(strings.TrimPrefix(item.Name, "models/")); name != "" {
			models = append(models, name)
		}
	}
	// Gemini 的 envelope 形状本身就够独占了，严格性只差"名字自洽"这一条。
	return models, hasNativeModelName(models, model.SiteModelRouteTypeGemini), nil
}

// probeOpenAIModels is the permissive last resort - a bare `data` array is all
// it asks for, so it must stay last in protocolProbes.
func probeOpenAIModels(
	client *http.Client,
	ctx context.Context,
	request model.Channel,
	baseURL string,
	key string,
) ([]string, bool, error) {
	body, err := probeModelListBody(client, ctx, request, baseURL, map[string]string{
		"Authorization": "Bearer " + key,
	})
	if err != nil {
		return nil, false, err
	}
	var result model.OpenAIModelList
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, false, err
	}
	models := make([]string, 0, len(result.Data))
	for _, item := range result.Data {
		if id := strings.TrimSpace(item.ID); id != "" {
			models = append(models, id)
		}
	}
	return models, len(models) > 0, nil
}

// probeModelListBody performs one model-list request and returns the raw body so
// each prober can judge the payload shape itself. Auth headers are set after
// applyDefaultModelRequestHeaders so a site's custom headers cannot swap the
// credential this probe is testing (isProtectedModelRequestHeader already blocks
// the auth names, this ordering makes it explicit).
func probeModelListBody(
	client *http.Client,
	ctx context.Context,
	request model.Channel,
	baseURL string,
	authHeaders map[string]string,
) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/models", nil)
	if err != nil {
		return nil, err
	}
	applyDefaultModelRequestHeaders(req, request)
	for name, value := range authHeaders {
		req.Header.Set(name, value)
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := httpbody.ReadResponse(resp)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, formatModelHTTPError(resp.StatusCode, resp.Header.Get("Content-Type"), body)
	}
	return body, nil
}
