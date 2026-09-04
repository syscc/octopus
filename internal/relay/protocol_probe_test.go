package relay

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	dbpkg "github.com/bestruirui/octopus/internal/db"
	"github.com/bestruirui/octopus/internal/model"
	"github.com/bestruirui/octopus/internal/op"
	"github.com/bestruirui/octopus/internal/transformer/outbound"
)

func TestClassifyOpenAIProtocolProbeResponseConservatively(t *testing.T) {
	tests := []struct {
		name        string
		status      int
		body        string
		transport   error
		wantOutcome ProtocolProbeOutcome
	}{
		{name: "success", status: http.StatusOK, wantOutcome: ProtocolProbeSupported},
		{
			name:        "success keeps supported when body read fails",
			status:      http.StatusOK,
			transport:   errors.New("read tcp: body read failed"),
			wantOutcome: ProtocolProbeSupported,
		},
		{
			name:        "redirect is not endpoint evidence",
			status:      http.StatusFound,
			wantOutcome: ProtocolProbeUnknown,
		},
		{name: "created success", status: http.StatusCreated, wantOutcome: ProtocolProbeSupported},
		{
			name:        "explicit route mismatch",
			status:      http.StatusNotFound,
			body:        `{"error":{"message":"route not found","type":"invalid_request_error"}}`,
			wantOutcome: ProtocolProbeUnsupported,
		},
		{
			name:        "invalid endpoint url",
			status:      http.StatusBadRequest,
			body:        `{"error":{"message":"Invalid URL (POST /v1/responses)","type":"invalid_request_error"}}`,
			wantOutcome: ProtocolProbeUnsupported,
		},
		{
			name:        "ambiguous resource not found",
			status:      http.StatusNotFound,
			body:        `{"message":"resource missing","type":"not_found"}`,
			wantOutcome: ProtocolProbeUnknown,
		},
		{
			name:        "bare route status",
			status:      http.StatusNotFound,
			wantOutcome: ProtocolProbeUnsupported,
		},
		{
			name:        "unstructured method status",
			status:      http.StatusMethodNotAllowed,
			body:        "Method Not Allowed",
			wantOutcome: ProtocolProbeUnsupported,
		},
		{
			name:        "plain model error remains unknown",
			status:      http.StatusNotFound,
			body:        "model not found",
			wantOutcome: ProtocolProbeUnknown,
		},
		{
			name:        "authentication",
			status:      http.StatusUnauthorized,
			body:        `{"error":{"message":"authentication failed","type":"authentication_error"}}`,
			wantOutcome: ProtocolProbeUnknown,
		},
		{
			name:        "permission",
			status:      http.StatusForbidden,
			body:        `{"error":{"message":"permission denied","type":"forbidden"}}`,
			wantOutcome: ProtocolProbeUnknown,
		},
		{
			name:        "rate limit",
			status:      http.StatusTooManyRequests,
			body:        `{"error":{"message":"rate limit exceeded","type":"rate_limit_error"}}`,
			wantOutcome: ProtocolProbeUnknown,
		},
		{
			name:        "model not found",
			status:      http.StatusNotFound,
			body:        `{"error":{"message":"model not found","type":"model_not_found"}}`,
			wantOutcome: ProtocolProbeUnknown,
		},
		{
			name:        "generic server failure",
			status:      http.StatusBadGateway,
			body:        `{"error":{"message":"upstream unavailable","type":"server_error"}}`,
			wantOutcome: ProtocolProbeUnknown,
		},
		{
			name:        "server body mentions route but status is server error",
			status:      http.StatusInternalServerError,
			body:        `{"error":{"message":"route not found","type":"invalid_request_error"}}`,
			wantOutcome: ProtocolProbeUnknown,
		},
		{
			name:        "network failure",
			transport:   errors.New("dial tcp: connection refused"),
			wantOutcome: ProtocolProbeUnknown,
		},
		{
			name:        "transport endpoint evidence",
			transport:   errors.New("unsupported endpoint"),
			wantOutcome: ProtocolProbeUnsupported,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			outcome, reason := classifyOpenAIProtocolProbeResponse(tt.status, tt.body, tt.transport)
			if outcome != tt.wantOutcome {
				t.Fatalf("outcome = %q, want %q", outcome, tt.wantOutcome)
			}
			if strings.Contains(reason, tt.body) && tt.body != "" {
				t.Fatal("classification reason copied upstream response body")
			}
		})
	}
}

func TestAggregateOpenAIProtocolProbeOutcomes(t *testing.T) {
	tests := []struct {
		name  string
		input []ProtocolProbeOutcome
		want  ProtocolProbeOutcome
	}{
		{name: "empty", input: nil, want: ProtocolProbeUnknown},
		{name: "all unsupported", input: []ProtocolProbeOutcome{ProtocolProbeUnsupported, ProtocolProbeUnsupported}, want: ProtocolProbeUnsupported},
		{name: "unknown blocks unsupported", input: []ProtocolProbeOutcome{ProtocolProbeUnsupported, ProtocolProbeUnknown}, want: ProtocolProbeUnknown},
		{name: "support wins over unknown", input: []ProtocolProbeOutcome{ProtocolProbeUnknown, ProtocolProbeSupported}, want: ProtocolProbeSupported},
		{name: "support wins over unsupported", input: []ProtocolProbeOutcome{ProtocolProbeUnsupported, ProtocolProbeSupported}, want: ProtocolProbeSupported},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := aggregateProtocolProbeOutcomes(tt.input); got != tt.want {
				t.Fatalf("aggregate(%v) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}

func TestOpenAIProtocolProbeRequestUsesConfiguredTransportButKeepsSafeBounds(t *testing.T) {
	var mu sync.Mutex
	paths := make([]string, 0, 2)
	bodies := make(map[string]map[string]json.RawMessage)
	hasProbeHeader := true
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read request body: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(body, &fields); err != nil {
			t.Errorf("request body is not an object")
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		mu.Lock()
		paths = append(paths, r.URL.Path)
		bodies[r.URL.Path] = fields
		if r.Header.Get("Authorization") == "" || r.Header.Get("X-Probe-Config") != "enabled" {
			hasProbeHeader = false
		}
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	override := `{"model":"wrong-model","messages":[{"role":"user","content":"expensive"}],"input":"expensive","stream":true,"max_tokens":999999,"max_output_tokens":999999,"tools":[{"type":"function"}],"background":true,"temperature":0.2,"store":true,"frequency_penalty":0.4,"presence_penalty":0.3,"seed":42}`
	report := probeOpenAIProtocolConfig(context.Background(), protocolProbeConfig{
		BaseURLs:  []string{server.URL},
		APIKey:    "fixture-value",
		ModelName: "configured-model",
		CustomHeaders: []model.CustomHeader{{
			HeaderKey: "X-Probe-Config", HeaderValue: "enabled",
		}},
		ParamOverride: &override,
		ClientFactory: func(context.Context) (*http.Client, error) { return server.Client(), nil },
	})
	if report.Chat.Outcome != ProtocolProbeSupported || report.Responses.Outcome != ProtocolProbeSupported {
		t.Fatalf("unexpected probe outcomes: chat=%q responses=%q", report.Chat.Outcome, report.Responses.Outcome)
	}

	mu.Lock()
	defer mu.Unlock()
	if !hasProbeHeader {
		t.Fatal("configured authorization or custom header was not applied")
	}
	if !reflect.DeepEqual(paths, []string{"/chat/completions", "/responses"}) {
		t.Fatalf("probe paths = %v", paths)
	}
	for _, path := range paths {
		fields := bodies[path]
		var modelName string
		if err := json.Unmarshal(fields["model"], &modelName); err != nil || modelName != "configured-model" {
			t.Fatal("probe model was changed by ParamOverride")
		}
		var stream bool
		if err := json.Unmarshal(fields["stream"], &stream); err != nil || stream {
			t.Fatal("probe stream flag was not forced false")
		}
		for _, dangerous := range []string{"tools", "tool_choice", "background", "previous_response_id", "conversation"} {
			if _, present := fields[dangerous]; present {
				t.Fatalf("unsafe probe field %q was forwarded", dangerous)
			}
		}
		if path == "/chat/completions" {
			if _, present := fields["store"]; present {
				t.Fatal("chat probe must not carry a store field at all")
			}
			var messages []map[string]string
			if err := json.Unmarshal(fields["messages"], &messages); err != nil || len(messages) != 1 || messages[0]["content"] != "ping" {
				t.Fatal("chat probe message was changed")
			}
			var maxTokens int
			if raw := fields["max_tokens"]; len(raw) > 0 {
				if err := json.Unmarshal(raw, &maxTokens); err != nil || maxTokens != 1 {
					t.Fatal("chat probe token bound was changed")
				}
			} else if raw := fields["max_completion_tokens"]; len(raw) > 0 {
				if err := json.Unmarshal(raw, &maxTokens); err != nil || maxTokens != 1 {
					t.Fatal("reasoning chat probe token bound was changed")
				}
			} else {
				t.Fatal("chat probe has no token bound")
			}
		} else {
			var input string
			if err := json.Unmarshal(fields["input"], &input); err != nil || input != "ping" {
				t.Fatal("responses probe input was changed")
			}
			var maxTokens int
			if err := json.Unmarshal(fields["max_output_tokens"], &maxTokens); err != nil || maxTokens != 16 {
				t.Fatal("responses probe token bound was changed")
			}
			var store bool
			if err := json.Unmarshal(fields["store"], &store); err != nil || store {
				t.Fatal("responses probe did not force store=false")
			}
			for _, chatOnly := range []string{"frequency_penalty", "presence_penalty", "seed"} {
				if _, present := fields[chatOnly]; present {
					t.Fatalf("Responses probe forwarded Chat-only field %q", chatOnly)
				}
			}
		}
	}
}

func TestBuildProtocolProbeAPIResponseDoesNotExposeInternalEvidence(t *testing.T) {
	channel := &model.Channel{
		ID:                        7,
		Type:                      outbound.OutboundTypeOpenAIChat,
		OpenAIProtocolMode:        model.OpenAIProtocolModeAuto,
		OpenAIChatCapability:      model.OpenAIProtocolCapabilityUnknown,
		OpenAIResponsesCapability: model.OpenAIProtocolCapabilityUnknown,
	}
	report := NewProtocolProbeReport(channel.ID, channel)
	report.Chat.Outcome = ProtocolProbeSupported
	report.Chat.Attempts = []ProtocolProbeAttempt{{
		BaseURL:    "https://private.example/account/secret",
		StatusCode: http.StatusOK,
		Outcome:    ProtocolProbeSupported,
		Reason:     "upstream body must not be returned",
	}}
	response := BuildProtocolProbeAPIResponse(channel.ID, report, channel)
	encoded, err := json.Marshal(response)
	if err != nil {
		t.Fatalf("marshal response failed: %v", err)
	}
	text := string(encoded)
	for _, forbidden := range []string{"private.example", "account/secret", "upstream body", "base_url"} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("sanitized probe response contains %q", forbidden)
		}
	}
	if response.Endpoints[0].Outcome != "probed" || response.Endpoints[0].Status == nil || *response.Endpoints[0].Status != http.StatusOK {
		t.Fatalf("unexpected safe endpoint summary: %#v", response.Endpoints[0])
	}

	// When the authoritative reload is unavailable, keep the stored-capability
	// snapshot separate from the new observation instead of speculating that a
	// concurrent persistence step succeeded.
	fallback := BuildProtocolProbeAPIResponse(channel.ID, report, nil)
	if fallback.Chat != model.OpenAIProtocolCapabilityUnknown ||
		fallback.Endpoints[0].Observed != model.OpenAIProtocolCapabilitySupported ||
		fallback.Endpoints[0].Capability != model.OpenAIProtocolCapabilityUnknown {
		t.Fatalf("reload fallback conflated observation with capability: %#v", fallback)
	}

	skippedReport := NewProtocolProbeReport(channel.ID, channel)
	skippedReport.Skipped = true
	skippedReport.SkipReason = "private routing configuration at https://private.example/account/secret"
	skippedResponse := BuildProtocolProbeAPIResponse(channel.ID, skippedReport, channel)
	skippedEncoded, err := json.Marshal(skippedResponse)
	if err != nil {
		t.Fatalf("marshal skipped response failed: %v", err)
	}
	if !skippedResponse.Skipped {
		t.Fatal("skipped report did not retain the public skipped flag")
	}
	for _, forbidden := range []string{"private routing configuration", "private.example", "account/secret", "skip_reason"} {
		if strings.Contains(string(skippedEncoded), forbidden) {
			t.Fatalf("skipped probe response contains internal reason %q", forbidden)
		}
	}
	for _, endpoint := range skippedResponse.Endpoints {
		if endpoint.Outcome != "skipped" || endpoint.Message != "probe skipped" {
			t.Fatalf("skipped endpoint exposed a non-generic result: %#v", endpoint)
		}
	}
}

func newProbeRequestCounter() *probeRequestCounter {
	return &probeRequestCounter{counts: make(map[string]int)}
}

// probeRequestCounter is a race-safe per-path request counter used by probe
// httptest servers under -race.
type probeRequestCounter struct {
	mu     sync.Mutex
	counts map[string]int
}

func (c *probeRequestCounter) increment(path string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.counts[path]++
}

type probeWaiterContext struct {
	context.Context
	observed chan struct{}
	once     sync.Once
}

func (c *probeWaiterContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.observed) })
	return c.Context.Done()
}

func (c *probeRequestCounter) count(path string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.counts[path]
}

func TestProbeChannelOpenAIProtocolsPersistsAggregateWithoutRelayAccounting(t *testing.T) {
	ctx := setupRelayTestDB(t)
	fixtureBody := `{"error":{"message":"route not found","type":"invalid_request_error"}}`
	newCountingServer := func(t *testing.T, handler func(string) int) (*httptest.Server, *probeRequestCounter) {
		t.Helper()
		counts := newProbeRequestCounter()
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			counts.increment(r.URL.Path)
			status := handler(r.URL.Path)
			if status != http.StatusOK {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(status)
				_, _ = io.WriteString(w, fixtureBody)
				return
			}
			w.WriteHeader(status)
		})), counts
	}
	serverUnsupported, countsUnsupported := newCountingServer(t, func(string) int { return http.StatusNotFound })
	defer serverUnsupported.Close()
	serverMixed, countsMixed := newCountingServer(t, func(path string) int {
		if path == "/chat/completions" {
			return http.StatusOK
		}
		return http.StatusNotFound
	})
	defer serverMixed.Close()

	channel := &model.Channel{
		Name:               "probe-persistence-fixture",
		Type:               outbound.OutboundTypeOpenAIChat,
		Enabled:            true,
		BaseUrls:           []model.BaseUrl{{URL: serverUnsupported.URL}, {URL: serverMixed.URL}},
		Keys:               []model.ChannelKey{{Enabled: true, ChannelKey: "fixture-value"}},
		Model:              "configured-model",
		ProxyMode:          model.ProxyUsageModeDirect,
		OpenAIProtocolMode: model.OpenAIProtocolModeAuto,
	}
	if err := op.ChannelCreate(channel, ctx); err != nil {
		t.Fatalf("ChannelCreate failed: %v", err)
	}
	var relayLogsBefore, statsBefore int64
	if err := dbpkg.GetDB().Model(&model.RelayLog{}).Count(&relayLogsBefore).Error; err != nil {
		t.Fatalf("count relay logs failed: %v", err)
	}
	if err := dbpkg.GetDB().Model(&model.StatsChannel{}).Where("channel_id = ?", channel.ID).Count(&statsBefore).Error; err != nil {
		t.Fatalf("count channel stats failed: %v", err)
	}

	report, err := ProbeChannelOpenAIProtocols(channel.ID, ctx)
	if err != nil {
		t.Fatalf("ProbeChannelOpenAIProtocols failed: %v", err)
	}
	if report.Chat.Outcome != ProtocolProbeSupported || report.Responses.Outcome != ProtocolProbeUnsupported {
		t.Fatalf("aggregate outcomes = chat:%q responses:%q", report.Chat.Outcome, report.Responses.Outcome)
	}
	var persisted model.Channel
	if err := dbpkg.GetDB().Preload("Keys").First(&persisted, channel.ID).Error; err != nil {
		t.Fatalf("reload channel failed: %v", err)
	}
	if persisted.OpenAIChatCapability != model.OpenAIProtocolCapabilitySupported || persisted.OpenAIResponsesCapability != model.OpenAIProtocolCapabilityUnsupported {
		t.Fatalf("persisted capabilities = chat:%q responses:%q", persisted.OpenAIChatCapability, persisted.OpenAIResponsesCapability)
	}
	var relayLogsAfter, statsAfter int64
	_ = dbpkg.GetDB().Model(&model.RelayLog{}).Count(&relayLogsAfter).Error
	_ = dbpkg.GetDB().Model(&model.StatsChannel{}).Where("channel_id = ?", channel.ID).Count(&statsAfter).Error
	if relayLogsAfter != relayLogsBefore || statsAfter != statsBefore {
		t.Fatalf("probe changed ordinary accounting: relay logs %d->%d stats %d->%d", relayLogsBefore, relayLogsAfter, statsBefore, statsAfter)
	}
	if len(persisted.Keys) != 1 || persisted.Keys[0].StatusCode != 0 || persisted.Keys[0].TotalCost != 0 {
		t.Fatal("probe changed channel-key runtime accounting")
	}

	for _, counts := range []*probeRequestCounter{countsUnsupported, countsMixed} {
		for _, path := range []string{"/chat/completions", "/responses"} {
			if counts.count(path) != 1 {
				t.Fatalf("expected exactly one probe request for %s", path)
			}
		}
	}
}

// TestOpenAIProtocolProbeDoesNotFollowRedirectsAndLeakKey verifies that the
// probe client never contacts a redirect target: the channel key must not be
// forwarded anywhere except the configured base URL, and a 3xx answer carries
// no endpoint capability evidence.
func TestOpenAIProtocolProbeDoesNotFollowRedirectsAndLeakKey(t *testing.T) {
	counts := newProbeRequestCounter()
	var authMu sync.Mutex
	authSeen := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		counts.increment(r.URL.Path)
		if r.URL.Path == "/exfil" {
			if r.Header.Get("Authorization") != "" {
				authMu.Lock()
				authSeen = true
				authMu.Unlock()
			}
			w.WriteHeader(http.StatusOK)
			return
		}
		http.Redirect(w, r, "/exfil", http.StatusFound)
	}))
	defer server.Close()

	report := probeOpenAIProtocolConfig(context.Background(), protocolProbeConfig{
		BaseURLs:      []string{server.URL},
		APIKey:        "fixture-value",
		ModelName:     "configured-model",
		ClientFactory: func(context.Context) (*http.Client, error) { return server.Client(), nil },
	})
	if counts.count("/exfil") != 0 {
		t.Fatalf("redirect target was contacted %d times", counts.count("/exfil"))
	}
	authMu.Lock()
	defer authMu.Unlock()
	if authSeen {
		t.Fatal("channel key reached the redirect target")
	}
	if report.Chat.Outcome != ProtocolProbeUnknown || report.Responses.Outcome != ProtocolProbeUnknown {
		t.Fatalf("redirect responses must stay unknown, got chat=%q responses=%q", report.Chat.Outcome, report.Responses.Outcome)
	}
}

func TestOpenAIProtocolProbeKeepsConfiguredCookieAndSelectedCredential(t *testing.T) {
	request, err := buildOpenAIProtocolProbeRequest(
		context.Background(),
		outbound.OutboundTypeOpenAIChat,
		"https://example.invalid/v1",
		"selected-key",
		"probe-model",
		[]model.CustomHeader{
			{HeaderKey: "Authorization", HeaderValue: "Bearer custom-key"},
			{HeaderKey: "Cookie", HeaderValue: "session=channel-cookie"},
			{HeaderKey: "Set-Cookie", HeaderValue: "session=must-not-send"},
		},
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	if got := request.Header.Get("Authorization"); got != "Bearer selected-key" {
		t.Fatalf("probe selected credential = %q, want selected key", got)
	}
	if got := request.Header.Get("Cookie"); got != "session=channel-cookie" {
		t.Fatalf("probe configured Cookie = %q, want channel cookie", got)
	}
	if got := request.Header.Get("Set-Cookie"); got != "" {
		t.Fatalf("probe Set-Cookie must not be sent upstream: %q", got)
	}
}

// TestProbeChannelOpenAIProtocolsMergesConcurrentProbes verifies that concurrent
// manual and automatic probes for one channel share a single in-flight probe
// instead of duplicating upstream requests.
func TestProbeChannelOpenAIProtocolsMergesConcurrentProbes(t *testing.T) {
	ctx := setupRelayTestDB(t)
	counts := newProbeRequestCounter()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		counts.increment(r.URL.Path)
		// Widen the singleflight merge window so every caller can join.
		time.Sleep(150 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	channel := &model.Channel{
		Name:               "probe-singleflight-fixture",
		Type:               outbound.OutboundTypeOpenAIChat,
		Enabled:            true,
		BaseUrls:           []model.BaseUrl{{URL: server.URL}},
		Keys:               []model.ChannelKey{{Enabled: true, ChannelKey: "fixture-value"}},
		Model:              "configured-model",
		ProxyMode:          model.ProxyUsageModeDirect,
		OpenAIProtocolMode: model.OpenAIProtocolModeAuto,
	}
	if err := op.ChannelCreate(channel, ctx); err != nil {
		t.Fatalf("ChannelCreate failed: %v", err)
	}

	const callers = 8
	start := make(chan struct{})
	var wg sync.WaitGroup
	reports := make([]*ProtocolProbeReport, callers)
	errs := make([]error, callers)
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			reports[i], errs[i] = ProbeChannelOpenAIProtocols(channel.ID, ctx)
		}(i)
	}
	close(start)
	wg.Wait()

	for i := 0; i < callers; i++ {
		if errs[i] != nil {
			t.Fatalf("caller %d failed: %v", i, errs[i])
		}
		if reports[i].Chat.Outcome != ProtocolProbeSupported || reports[i].Responses.Outcome != ProtocolProbeSupported {
			t.Fatalf("caller %d got chat=%q responses=%q", i, reports[i].Chat.Outcome, reports[i].Responses.Outcome)
		}
	}
	for _, path := range []string{"/chat/completions", "/responses"} {
		if got := counts.count(path); got != 1 {
			t.Fatalf("path %s was probed %d times, want 1 (singleflight merge failed)", path, got)
		}
	}
}

func TestProbeChannelOpenAIProtocolsCallerCancellationDoesNotCancelSharedFlight(t *testing.T) {
	ctx := setupRelayTestDB(t)
	entered := make(chan struct{})
	release := make(chan struct{})
	var enteredOnce sync.Once
	counts := newProbeRequestCounter()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		counts.increment(r.URL.Path)
		enteredOnce.Do(func() { close(entered) })
		<-release
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	channel := &model.Channel{
		Name: "probe-cancelled-waiter-fixture", Type: outbound.OutboundTypeOpenAIChat, Enabled: true,
		BaseUrls: []model.BaseUrl{{URL: server.URL}}, Keys: []model.ChannelKey{{Enabled: true, ChannelKey: "fixture-value"}},
		Model: "configured-model", ProxyMode: model.ProxyUsageModeDirect, OpenAIProtocolMode: model.OpenAIProtocolModeAuto,
	}
	if err := op.ChannelCreate(channel, ctx); err != nil {
		t.Fatalf("ChannelCreate failed: %v", err)
	}

	firstCtx, cancelFirst := context.WithCancel(ctx)
	firstResult := make(chan error, 1)
	go func() {
		_, err := ProbeChannelOpenAIProtocols(channel.ID, firstCtx)
		firstResult <- err
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("shared probe did not reach upstream")
	}

	secondCtx := &probeWaiterContext{Context: ctx, observed: make(chan struct{})}
	secondResult := make(chan error, 1)
	go func() {
		_, err := ProbeChannelOpenAIProtocols(channel.ID, secondCtx)
		secondResult <- err
	}()
	select {
	case <-secondCtx.observed:
		// Done is evaluated in the wait select after DoChan registered this caller.
	case <-time.After(time.Second):
		t.Fatal("second caller did not join the shared flight")
	}

	cancelFirst()
	select {
	case err := <-firstResult:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("first cancelled caller error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("cancelled caller did not return")
	}

	close(release)
	select {
	case err := <-secondResult:
		if err != nil {
			t.Fatalf("shared flight was cancelled by first caller: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("second caller did not receive shared probe result")
	}
	for _, path := range []string{"/chat/completions", "/responses"} {
		if got := counts.count(path); got != 1 {
			t.Fatalf("shared probe request count for %s = %d, want 1", path, got)
		}
	}
}

// TestProbeChannelOpenAIProtocolsCloudflareMixedURLShortCircuit verifies that a
// mixed base URL list where any one URL is a documented Cloudflare Workers AI
// base triggers the fixed chat-only short-circuit without network probes.
func TestProbeChannelOpenAIProtocolsCloudflareMixedURLShortCircuit(t *testing.T) {
	ctx := setupRelayTestDB(t)
	counts := newProbeRequestCounter()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		counts.increment(r.URL.Path)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	channel := &model.Channel{
		Name:               "probe-cloudflare-mixed-fixture",
		Type:               outbound.OutboundTypeOpenAIChat,
		Enabled:            true,
		BaseUrls:           []model.BaseUrl{{URL: server.URL}, {URL: "https://api.cloudflare.com/client/v4/accounts/testaccount01/ai/v1"}},
		Keys:               []model.ChannelKey{{Enabled: true, ChannelKey: "fixture-value"}},
		Model:              "configured-model",
		ProxyMode:          model.ProxyUsageModeDirect,
		OpenAIProtocolMode: model.OpenAIProtocolModeAuto,
	}
	if err := op.ChannelCreate(channel, ctx); err != nil {
		t.Fatalf("ChannelCreate failed: %v", err)
	}
	report, err := ProbeChannelOpenAIProtocols(channel.ID, ctx)
	if err != nil {
		t.Fatalf("ProbeChannelOpenAIProtocols failed: %v", err)
	}
	if !report.Skipped || !strings.Contains(report.SkipReason, "cloudflare") {
		t.Fatalf("expected cloudflare short-circuit, got skipped=%v reason=%q", report.Skipped, report.SkipReason)
	}
	if report.Chat.Outcome != ProtocolProbeSupported || report.Responses.Outcome != ProtocolProbeUnsupported {
		t.Fatalf("cloudflare chat-only outcomes = chat:%q responses:%q", report.Chat.Outcome, report.Responses.Outcome)
	}
	if counts.count("/chat/completions") != 0 || counts.count("/responses") != 0 {
		t.Fatal("cloudflare short-circuit must not send probe requests")
	}
	var persisted model.Channel
	if err := dbpkg.GetDB().First(&persisted, channel.ID).Error; err != nil {
		t.Fatalf("reload channel failed: %v", err)
	}
	if persisted.OpenAIChatCapability != model.OpenAIProtocolCapabilitySupported || persisted.OpenAIResponsesCapability != model.OpenAIProtocolCapabilityUnsupported {
		t.Fatalf("persisted capabilities = chat:%q responses:%q", persisted.OpenAIChatCapability, persisted.OpenAIResponsesCapability)
	}

	apiResponse := BuildProtocolProbeAPIResponse(channel.ID, report, &persisted)
	if !apiResponse.Skipped {
		t.Fatal("cloudflare fixed rule must still report that network probing was skipped")
	}
	wantObserved := map[string]model.OpenAIProtocolCapability{
		"chat":      model.OpenAIProtocolCapabilitySupported,
		"responses": model.OpenAIProtocolCapabilityUnsupported,
	}
	for _, endpoint := range apiResponse.Endpoints {
		if endpoint.Outcome != "probed" || endpoint.Observed != wantObserved[endpoint.Endpoint] || endpoint.Capability != wantObserved[endpoint.Endpoint] {
			t.Fatalf("cloudflare fixed endpoint result was hidden by skipped state: %#v", endpoint)
		}
	}
}

// TestProbeOpenAIProtocolConfigStopsAfterTotalTimeout verifies that the base
// URL loop stops scheduling requests once the total probe budget is exhausted,
// and that the incomplete run never concludes unsupported and reports a
// generic interruption reason.
func TestProbeOpenAIProtocolConfigSharesTotalBudgetAcrossConfiguredEndpoints(t *testing.T) {
	slowCounts := newProbeRequestCounter()
	slowServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		slowCounts.increment(r.URL.Path)
		select {
		case <-time.After(300 * time.Millisecond):
			w.WriteHeader(http.StatusOK)
		case <-r.Context().Done():
		}
	}))
	defer slowServer.Close()
	fastCounts := newProbeRequestCounter()
	fastServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fastCounts.increment(r.URL.Path)
		w.WriteHeader(http.StatusOK)
	}))
	defer fastServer.Close()

	started := time.Now()
	report := probeOpenAIProtocolConfig(context.Background(), protocolProbeConfig{
		BaseURLs:      []string{slowServer.URL, fastServer.URL},
		APIKey:        "fixture-value",
		ModelName:     "configured-model",
		TotalTimeout:  240 * time.Millisecond,
		ClientFactory: func(context.Context) (*http.Client, error) { return slowServer.Client(), nil },
	})
	if elapsed := time.Since(started); elapsed >= 500*time.Millisecond {
		t.Fatalf("probe exceeded bounded execution window: %v", elapsed)
	}
	for _, counts := range []*probeRequestCounter{slowCounts, fastCounts} {
		for _, path := range []string{"/chat/completions", "/responses"} {
			if got := counts.count(path); got != 1 {
				t.Fatalf("configured endpoint %s was probed %d times, want 1", path, got)
			}
		}
	}
	if report.Chat.Outcome != ProtocolProbeSupported || report.Responses.Outcome != ProtocolProbeSupported {
		t.Fatalf("later positive evidence was lost: chat=%q responses=%q", report.Chat.Outcome, report.Responses.Outcome)
	}
	if report.Error != "" {
		t.Fatalf("fully scheduled probe reported interruption: %q", report.Error)
	}
}

func TestProbeChannelOpenAIProtocolsReloadsAuthoritativeBaseURLs(t *testing.T) {
	ctx := setupRelayTestDB(t)
	var firstCalls, secondCalls atomic.Int32
	first := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		firstCalls.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer first.Close()
	second := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		secondCalls.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer second.Close()

	channel := &model.Channel{
		Name:               "probe-authoritative-fixture",
		Type:               outbound.OutboundTypeOpenAIChat,
		Enabled:            true,
		BaseUrls:           []model.BaseUrl{{URL: first.URL}, {URL: second.URL}},
		Keys:               []model.ChannelKey{{Enabled: true, ChannelKey: "fixture-value"}},
		Model:              "configured-model",
		ProxyMode:          model.ProxyUsageModeDirect,
		OpenAIProtocolMode: model.OpenAIProtocolModeAuto,
	}
	if err := op.ChannelCreate(channel, ctx); err != nil {
		t.Fatalf("ChannelCreate failed: %v", err)
	}
	// Simulate the runtime delay checker pruning an unreachable URL from the
	// cache. The database still contains both configured URLs.
	if err := op.ChannelBaseUrlUpdate(channel.ID, []model.BaseUrl{{URL: first.URL}}); err != nil {
		t.Fatalf("ChannelBaseUrlUpdate failed: %v", err)
	}
	if _, err := ProbeChannelOpenAIProtocols(channel.ID, ctx); err != nil {
		t.Fatalf("ProbeChannelOpenAIProtocols failed: %v", err)
	}
	if firstCalls.Load() != 2 || secondCalls.Load() != 2 {
		t.Fatalf("authoritative probe calls = first:%d second:%d; expected both endpoints on both URLs", firstCalls.Load(), secondCalls.Load())
	}
}

func TestProbeChannelOpenAIProtocolsPreservesManualMode(t *testing.T) {
	ctx := setupRelayTestDB(t)
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	channel := &model.Channel{
		Name:               "probe-manual-mode-fixture",
		Type:               outbound.OutboundTypeOpenAIChat,
		Enabled:            true,
		BaseUrls:           []model.BaseUrl{{URL: server.URL}},
		Keys:               []model.ChannelKey{{Enabled: true, ChannelKey: "fixture-value"}},
		Model:              "configured-model",
		ProxyMode:          model.ProxyUsageModeDirect,
		OpenAIProtocolMode: model.OpenAIProtocolModeChatOnly,
	}
	if err := op.ChannelCreate(channel, ctx); err != nil {
		t.Fatalf("ChannelCreate failed: %v", err)
	}
	if err := dbpkg.GetDB().Model(&model.Channel{}).Where("id = ?", channel.ID).Updates(map[string]any{
		"openai_chat_capability":      model.OpenAIProtocolCapabilityUnsupported,
		"openai_responses_capability": model.OpenAIProtocolCapabilitySupported,
	}).Error; err != nil {
		t.Fatalf("seed manual capability fields failed: %v", err)
	}
	before, err := op.ChannelGetAuthoritative(channel.ID, ctx)
	if err != nil {
		t.Fatalf("authoritative reload failed: %v", err)
	}
	report, err := ProbeChannelOpenAIProtocols(channel.ID, ctx)
	if err != nil {
		t.Fatalf("ProbeChannelOpenAIProtocols failed: %v", err)
	}
	if !report.Skipped || report.SkipReason != "channel uses manual protocol mode" {
		t.Fatalf("manual mode was not skipped: skipped=%v reason=%q", report.Skipped, report.SkipReason)
	}
	if report.Chat.Outcome != ProtocolProbeUnknown || report.Responses.Outcome != ProtocolProbeUnknown {
		t.Fatalf("manual mode should not produce observations: chat=%q responses=%q", report.Chat.Outcome, report.Responses.Outcome)
	}
	if calls.Load() != 0 {
		t.Fatalf("manual mode sent %d upstream requests", calls.Load())
	}
	var persisted model.Channel
	if err := dbpkg.GetDB().First(&persisted, channel.ID).Error; err != nil {
		t.Fatalf("reload manual channel failed: %v", err)
	}
	if persisted.OpenAIProtocolMode != model.OpenAIProtocolModeChatOnly || persisted.OpenAIChatCapability != before.OpenAIChatCapability || persisted.OpenAIResponsesCapability != before.OpenAIResponsesCapability {
		t.Fatalf("manual mode/capabilities were changed: mode=%q chat=%q responses=%q", persisted.OpenAIProtocolMode, persisted.OpenAIChatCapability, persisted.OpenAIResponsesCapability)
	}
	after, err := op.ChannelGetAuthoritative(channel.ID, ctx)
	if err != nil {
		t.Fatalf("final authoritative reload failed: %v", err)
	}
	apiResponse := BuildProtocolProbeAPIResponse(channel.ID, report, after)
	if apiResponse.Chat != model.OpenAIProtocolCapabilitySupported || apiResponse.Responses != model.OpenAIProtocolCapabilityUnsupported {
		t.Fatalf("manual effective capabilities = chat:%q responses:%q", apiResponse.Chat, apiResponse.Responses)
	}
}

func TestOpenAIProtocolProbeBoundsBodyBeforeApplyingOverride(t *testing.T) {
	tooLargeModel := strings.Repeat("m", protocolProbeMaxRequestBytes)
	if _, err := buildOpenAIProtocolProbeRequest(context.Background(), outbound.OutboundTypeOpenAIChat, "https://example.invalid/v1", "fixture-key", tooLargeModel, nil, nil); err == nil {
		t.Fatal("oversized adapter-generated body was accepted")
	}

	override := strings.Repeat(" ", protocolProbeMaxRequestBytes+1)
	if _, err := buildOpenAIProtocolProbeRequest(context.Background(), outbound.OutboundTypeOpenAIChat, "https://example.invalid/v1", "fixture-key", "fixture-model", nil, &override); err == nil {
		t.Fatal("oversized raw ParamOverride was accepted")
	}
}

func TestProbeChannelOpenAIProtocolsSkipsWithoutEnabledKey(t *testing.T) {
	ctx := setupRelayTestDB(t)
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	channel := &model.Channel{
		Name:               "probe-no-enabled-key-fixture",
		Type:               outbound.OutboundTypeOpenAIChat,
		Enabled:            true,
		BaseUrls:           []model.BaseUrl{{URL: server.URL}},
		Keys:               []model.ChannelKey{{Enabled: true, ChannelKey: ""}},
		Model:              "configured-model",
		ProxyMode:          model.ProxyUsageModeDirect,
		OpenAIProtocolMode: model.OpenAIProtocolModeAuto,
	}
	if err := op.ChannelCreate(channel, ctx); err != nil {
		t.Fatalf("ChannelCreate failed: %v", err)
	}
	report, err := ProbeChannelOpenAIProtocols(channel.ID, ctx)
	if err != nil {
		t.Fatalf("ProbeChannelOpenAIProtocols failed: %v", err)
	}
	if !report.Skipped || report.SkipReason != "channel has no enabled key" {
		t.Fatalf("unexpected no-key skip: skipped=%v reason=%q", report.Skipped, report.SkipReason)
	}
	if calls.Load() != 0 {
		t.Fatalf("probe sent %d requests without an enabled key", calls.Load())
	}
	response := BuildProtocolProbeAPIResponse(channel.ID, report, channel)
	if !response.Skipped || len(response.Endpoints) != 2 {
		t.Fatalf("unexpected skipped API response: %#v", response)
	}
	for _, endpoint := range response.Endpoints {
		if endpoint.Outcome != "skipped" {
			t.Fatalf("endpoint %q outcome=%q, want skipped", endpoint.Endpoint, endpoint.Outcome)
		}
	}
}

func TestOpenAIProtocolProbeNilContextsAreSafe(t *testing.T) {
	request, err := buildOpenAIProtocolProbeRequest(nil, outbound.OutboundTypeOpenAIChat, "https://example.invalid/v1", "fixture-key", "fixture-model", nil, nil)
	if err != nil {
		t.Fatalf("buildOpenAIProtocolProbeRequest with nil context failed: %v", err)
	}
	if request == nil || request.Context() == nil {
		t.Fatal("probe request did not receive a non-nil context")
	}

	called := false
	cfg := protocolProbeConfig{ClientFactory: func(ctx context.Context) (*http.Client, error) {
		called = ctx != nil
		return &http.Client{}, nil
	}}
	if _, err := cfg.httpClient(nil); err != nil {
		t.Fatalf("httpClient with nil context failed: %v", err)
	}
	if !called {
		t.Fatal("client factory received a nil context")
	}

	if _, _, err := executeOpenAIProtocolProbeRequest(nil, nil, request); err == nil {
		t.Fatal("nil probe http client was accepted")
	}
	if _, _, err := executeOpenAIProtocolProbeRequest(nil, &http.Client{}, nil); err == nil {
		t.Fatal("nil probe request was accepted")
	}
}

func TestExecuteOpenAIProtocolProbeRequestBindsProvidedContext(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	request, err := http.NewRequest(http.MethodGet, server.URL, nil)
	if err != nil {
		t.Fatalf("build request failed: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, _, err = executeOpenAIProtocolProbeRequest(ctx, server.Client(), request)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("execute error = %v, want context.Canceled", err)
	}
	if calls.Load() != 0 {
		t.Fatalf("canceled probe reached upstream %d times", calls.Load())
	}
}
