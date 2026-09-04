package sitesync

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/bestruirui/octopus/internal/apperror"
	"github.com/bestruirui/octopus/internal/model"
)

func TestRequestJSONUsesBrowserHeaders(t *testing.T) {
	observedUserAgent := ""
	observedAccept := ""
	observedAcceptLanguage := ""

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		observedUserAgent = r.Header.Get("User-Agent")
		observedAccept = r.Header.Get("Accept")
		observedAcceptLanguage = r.Header.Get("Accept-Language")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"success":true}`))
	}))
	defer server.Close()

	_, err := requestJSON(context.Background(), &model.Site{BaseURL: server.URL}, http.MethodGet, server.URL, nil, nil)
	if err != nil {
		t.Fatalf("requestJSON returned error: %v", err)
	}
	if !strings.Contains(observedUserAgent, "Mozilla/5.0") {
		t.Fatalf("expected browser user-agent, got %q", observedUserAgent)
	}
	if observedAccept == "" {
		t.Fatalf("expected Accept header to be set")
	}
	if observedAcceptLanguage == "" {
		t.Fatalf("expected Accept-Language header to be set")
	}
}

func TestRequestJSONCustomHeaderOverridesUserAgent(t *testing.T) {
	observedUserAgent := ""
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		observedUserAgent = r.Header.Get("User-Agent")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"success":true}`))
	}))
	defer server.Close()

	_, err := requestJSON(
		context.Background(),
		&model.Site{BaseURL: server.URL, CustomHeader: []model.CustomHeader{{HeaderKey: "User-Agent", HeaderValue: "Octopus-Test-UA"}}},
		http.MethodGet,
		server.URL,
		nil,
		nil,
	)
	if err != nil {
		t.Fatalf("requestJSON returned error: %v", err)
	}
	if observedUserAgent != "Octopus-Test-UA" {
		t.Fatalf("expected custom user-agent, got %q", observedUserAgent)
	}
}

func TestRequestJSONFormatsHTMLErrorSummary(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.WriteHeader(http.StatusBadGateway)
		_, _ = w.Write([]byte(`<!DOCTYPE html><html lang="en-US"><head><title>Upstream Error</title></head><body>blocked</body></html>`))
	}))
	defer server.Close()

	_, err := requestJSON(context.Background(), &model.Site{BaseURL: server.URL}, http.MethodGet, server.URL, nil, nil)
	if err == nil {
		t.Fatalf("expected requestJSON to fail")
	}
	if !strings.Contains(err.Error(), "http 502: Upstream Error") {
		t.Fatalf("expected summarized HTML error, got %v", err)
	}
}

func TestRequestJSONDetectsCloudflareAttentionRequired(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("CF-Ray", "abc123-LAX")
		w.Header().Set("Retry-After", "120")
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`<!DOCTYPE html><html><head><title>Attention Required! | Cloudflare</title></head><body>Cloudflare Ray ID: abc123</body></html>`))
	}))
	defer server.Close()

	_, err := requestJSON(context.Background(), &model.Site{BaseURL: server.URL}, http.MethodGet, server.URL, nil, nil)
	if err == nil {
		t.Fatalf("expected requestJSON to fail")
	}
	var cfErr *CloudflareProtectionError
	if !errors.As(err, &cfErr) {
		t.Fatalf("expected CloudflareProtectionError, got %T %v", err, err)
	}
	if cfErr.RetryAfter != 60*time.Second {
		t.Fatalf("expected retry-after capped to 60s, got %s", cfErr.RetryAfter)
	}
	if got := apperror.Code(err); got != CodeSiteUpstreamCloudflareChallenge {
		t.Fatalf("expected error code %q, got %q", CodeSiteUpstreamCloudflareChallenge, got)
	}
}

func TestRequestJSONKeepsJSONForbiddenMessage(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("CF-Ray", "abc123-LAX")
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"message":"token forbidden"}`))
	}))
	defer server.Close()

	_, err := requestJSON(context.Background(), &model.Site{BaseURL: server.URL}, http.MethodGet, server.URL, nil, nil)
	if err == nil {
		t.Fatalf("expected requestJSON to fail")
	}
	if IsCloudflareProtectionError(err) {
		t.Fatalf("expected JSON business error, got Cloudflare error")
	}
	if err.Error() != "http 403: token forbidden" {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := apperror.Code(err); got != CodeSiteUpstreamHTTPError {
		t.Fatalf("expected error code %q, got %q", CodeSiteUpstreamHTTPError, got)
	}
	if got := apperror.Params(err)["statusCode"]; got != http.StatusForbidden {
		t.Fatalf("expected statusCode param %d, got %#v", http.StatusForbidden, got)
	}
}

func TestRequestJSONExtractsCloudflareErrorsArray(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("CF-Ray", "abc123-LAX")
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"success":false,"errors":[{"code":10000,"message":"Workers AI permission denied"}]}`))
	}))
	defer server.Close()

	_, err := requestJSON(context.Background(), &model.Site{BaseURL: server.URL}, http.MethodGet, server.URL, nil, nil)
	if err == nil {
		t.Fatal("expected requestJSON to fail")
	}
	if IsCloudflareProtectionError(err) {
		t.Fatalf("expected Cloudflare API error, got protection error: %v", err)
	}
	if err.Error() != "http 403: Workers AI permission denied" {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRequestJSONExtractsCloudflareErrorEnvelope(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Server", "cloudflare")
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"success":false,"errors":[{"code":10000,"message":"Invalid API Token"}],"messages":[],"result":null}`))
	}))
	defer server.Close()

	_, err := requestJSON(context.Background(), &model.Site{BaseURL: server.URL}, http.MethodGet, server.URL, nil, nil)
	if err == nil {
		t.Fatalf("expected requestJSON to fail")
	}
	if IsCloudflareProtectionError(err) {
		t.Fatalf("expected Cloudflare API error envelope, got protection error: %v", err)
	}
	if !strings.Contains(err.Error(), "Invalid API Token") {
		t.Fatalf("expected API error message, got %v", err)
	}
}

func TestNormalizeModelNamesPreservesCaseDistinctVariants(t *testing.T) {
	models := normalizeModelNames([]string{" GPT-5.5 ", "gpt-5.5", "gpt-5.5", ""})

	if len(models) != 2 {
		t.Fatalf("expected case-distinct model names to be preserved, got %+v", models)
	}
	seen := make(map[string]struct{}, len(models))
	for _, item := range models {
		seen[item] = struct{}{}
	}
	if _, ok := seen["GPT-5.5"]; !ok {
		t.Fatalf("expected GPT-5.5 to be preserved, got %+v", models)
	}
	if _, ok := seen["gpt-5.5"]; !ok {
		t.Fatalf("expected gpt-5.5 to be preserved, got %+v", models)
	}
}

func TestNormalizeModelNamesDropsOverlongNames(t *testing.T) {
	// 模拟 hub.linux.do 返回的畸形"模型名"：3200 字符，内容是几十个真模型名用空格拼接
	overlong := make([]byte, 0, 2100)
	for len(overlong) < 2000 {
		overlong = append(overlong, "abcdefghij "...)
	}

	models := normalizeModelNames([]string{
		"gpt-5.6-sol",
		"claude 4.5", // 合法的带空格模型名，必须保留
		string(overlong),
		"deepseek-v4-pro",
	})

	// 3 个合法 + 1 个超长 → 结果应该是 3 个
	if len(models) != 3 {
		t.Fatalf("expected 3 models (1 overlong dropped), got %d: %v", len(models), models)
	}

	// 所有保留的模型名都不能超过列宽
	for _, m := range models {
		if len(m) > maxSiteModelNameLength {
			t.Fatalf("overlong name survived: %d chars", len(m))
		}
	}

	// "claude 4.5" 必须在结果里（验证不是按空格判断）
	found := false
	for _, m := range models {
		if m == "claude 4.5" {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("legitimate name with space must survive")
	}
}

func TestParseGroupItemsPreservesScalarMapLabels(t *testing.T) {
	groups := parseGroupItems(map[string]any{
		"data": map[string]any{
			"vip":   "VIP Group",
			"trial": map[string]any{"name": "Trial Group"},
		},
	})

	seen := make(map[string]string)
	for _, group := range groups {
		seen[group.GroupKey] = group.Name
	}
	if seen["vip"] != "VIP Group" {
		t.Fatalf("expected scalar group label, got %+v", groups)
	}
	if seen["trial"] != "Trial Group" {
		t.Fatalf("expected nested group label, got %+v", groups)
	}
}
