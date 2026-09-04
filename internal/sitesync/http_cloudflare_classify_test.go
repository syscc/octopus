package sitesync

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/bestruirui/octopus/internal/apperror"
	"github.com/bestruirui/octopus/internal/model"
)

// api.cloudflare.com 的部分 V4 错误只在 errors[].error_chain 里带 message，
// 顶层 message/msg/errors[].message 均为空。这类响应是普通的接口错误，
// 不能被识别成 Cloudflare 挑战页，否则 sitesync 批处理会跳过整批账号。
func TestRequestJSONTreatsCloudflareJSONErrorWithoutMessageAsHTTPError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("CF-Ray", "abc123-LAX")
		w.Header().Set("Server", "cloudflare")
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"success":false,"errors":[{"code":7003,"error_chain":[{"code":7003,"message":"Could not route to /accounts/acc-id/ai/v1/models"}]}],"messages":[],"result":null}`))
	}))
	defer server.Close()

	_, err := requestJSON(context.Background(), &model.Site{BaseURL: server.URL}, http.MethodGet, server.URL, nil, nil)
	if err == nil {
		t.Fatal("expected requestJSON to fail")
	}
	if IsCloudflareProtectionError(err) {
		t.Fatalf("expected plain HTTP error for Cloudflare JSON API error, got protection error: %v", err)
	}
	if got := apperror.Code(err); got != CodeSiteUpstreamHTTPError {
		t.Fatalf("expected error code %q, got %q", CodeSiteUpstreamHTTPError, got)
	}
	if got := apperror.Params(err)["statusCode"]; got != http.StatusForbidden {
		t.Fatalf("expected statusCode param %d, got %#v", http.StatusForbidden, got)
	}
	if !strings.Contains(err.Error(), "Could not route to") {
		t.Fatalf("expected error_chain message to surface in error, got: %v", err)
	}
}

// 站点自身返回的 JSON 错误（键名不在 message/msg/error.message 里）经过
// anyRouter 路径时同样不能被 403 + CF 指纹误判成挑战页。
func TestRequestJSONTreatsUnknownJSONErrorKeyBehindCloudflareAsHTTPError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.Header().Set("Server", "cloudflare")
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"detail":"token disabled","status":403}`))
	}))
	defer server.Close()

	_, err := requestJSON(context.Background(), &model.Site{BaseURL: server.URL}, http.MethodGet, server.URL, nil, nil)
	if err == nil {
		t.Fatal("expected requestJSON to fail")
	}
	if IsCloudflareProtectionError(err) {
		t.Fatalf("expected plain HTTP error for JSON body, got protection error: %v", err)
	}
}

func TestIsCloudflareProtectionResponseClassification(t *testing.T) {
	const challengeHTML = `<!DOCTYPE html><html><head><title>Attention Required! | Cloudflare</title></head><body>Cloudflare Ray ID: abc123</body></html>`

	tests := []struct {
		name       string
		statusCode int
		header     http.Header
		body       string
		expected   bool
	}{
		{
			name:       "html challenge page with cf headers",
			statusCode: http.StatusForbidden,
			header:     http.Header{"Content-Type": {"text/html; charset=utf-8"}, "Cf-Ray": {"abc123-LAX"}},
			body:       challengeHTML,
			expected:   true,
		},
		{
			name:       "just a moment interstitial",
			statusCode: http.StatusForbidden,
			header:     http.Header{"Content-Type": {"text/html"}, "Server": {"cloudflare"}},
			body:       `<html><head><title>Just a moment...</title></head><body></body></html>`,
			expected:   true,
		},
		{
			name:       "plain html forbidden page served by cloudflare",
			statusCode: http.StatusForbidden,
			header:     http.Header{"Content-Type": {"text/html"}, "Server": {"cloudflare"}},
			body:       `<html><body>forbidden</body></html>`,
			expected:   true,
		},
		{
			name:       "error code 1020 text body",
			statusCode: http.StatusForbidden,
			header:     http.Header{"Content-Type": {"text/plain"}, "Cf-Ray": {"abc123-LAX"}},
			body:       "error code: 1020",
			expected:   true,
		},
		{
			name:       "empty body with cf ray",
			statusCode: http.StatusForbidden,
			header:     http.Header{"Cf-Ray": {"abc123-LAX"}},
			body:       "",
			expected:   true,
		},
		{
			name:       "challenge markers win over json content type",
			statusCode: http.StatusForbidden,
			header:     http.Header{"Content-Type": {"application/json"}, "Cf-Ray": {"abc123-LAX"}},
			body:       challengeHTML,
			expected:   true,
		},
		{
			name:       "cloudflare v4 json error envelope",
			statusCode: http.StatusForbidden,
			header:     http.Header{"Content-Type": {"application/json"}, "Cf-Ray": {"abc123-LAX"}, "Server": {"cloudflare"}},
			body:       `{"success":false,"errors":[{"code":10000,"message":"Authentication error"}],"messages":[],"result":null}`,
			expected:   false,
		},
		{
			name:       "json body without content type header",
			statusCode: http.StatusForbidden,
			header:     http.Header{"Server": {"cloudflare"}},
			body:       `{"success":false,"errors":[{"code":10000}]}`,
			expected:   false,
		},
		{
			name:       "por gate3 probe error message with json content type",
			statusCode: http.StatusForbidden,
			header:     http.Header{"Content-Type": {"application/json"}, "Cf-Ray": {"abc123-LAX"}},
			body:       `upstream error: 403: {"success":false,"errors":[{"code":10000,"message":"Authentication error"}]}`,
			expected:   false,
		},
		{
			// 伪装成 JSON Content-Type 的挑战页 HTML 里内嵌 JSON 片段，仍须按 body 特征判定为挑战页。
			name:       "challenge html embedding json blob",
			statusCode: http.StatusForbidden,
			header:     http.Header{"Content-Type": {"application/json"}, "Cf-Ray": {"abc123-LAX"}},
			body:       `<!DOCTYPE html><html><body><script>window.__CF$cv$params={"r":"abc","t":"MTcw"};</script>Just a moment...</body></html>`,
			expected:   true,
		},
		{
			// JSON 接口错误的 message 里出现 cloudflare 字样，也不能被当成挑战页。
			name:       "json error message mentioning cloudflare",
			statusCode: http.StatusForbidden,
			header:     http.Header{"Content-Type": {"application/json"}, "Server": {"cloudflare"}},
			body:       `{"success":false,"errors":[{"code":10001,"message":"Cloudflare Access denied for this token"}]}`,
			expected:   false,
		},
		{
			// 合法 JSON 但不是对象/数组（无结构化错误信息），仍回落到 CF 响应头指纹。
			name:       "json scalar body with cf ray",
			statusCode: http.StatusForbidden,
			header:     http.Header{"Content-Type": {"application/json"}, "Cf-Ray": {"abc123-LAX"}},
			body:       `"forbidden"`,
			expected:   true,
		},
		{
			name:       "non forbidden status",
			statusCode: http.StatusTooManyRequests,
			header:     http.Header{"Content-Type": {"text/html"}, "Cf-Ray": {"abc123-LAX"}},
			body:       challengeHTML,
			expected:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if actual := IsCloudflareProtectionResponse(tt.statusCode, tt.header, []byte(tt.body)); actual != tt.expected {
				t.Fatalf("expected %v, got %v", tt.expected, actual)
			}
		})
	}
}
