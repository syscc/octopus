package relay

import (
	"net/http"
	"slices"
	"strings"
	"testing"

	dbmodel "github.com/bestruirui/octopus/internal/model"
	"github.com/bestruirui/octopus/internal/transformer/outbound"
)

func TestCredentialHeadersAreIsolatedAcrossHTTPAndWS(t *testing.T) {
	clientHeaders := http.Header{
		"Authorization":   {"Bearer client-secret"},
		"X-API-Key":       {"client-api-key"},
		"X-Goog-Api-Key":  {"client-google-key"},
		"Api-Key":         {"client-generic-key"},
		"Cookie":          {"session=client-cookie"},
		"Accept-Language": {"zh-CN"},
	}
	channel := &dbmodel.Channel{
		CustomHeader: []dbmodel.CustomHeader{
			{HeaderKey: "Authorization", HeaderValue: "Bearer custom-secret"},
			{HeaderKey: "X-API-Key", HeaderValue: "custom-api-key"},
			{HeaderKey: "X-Goog-Api-Key", HeaderValue: "custom-google-key"},
			{HeaderKey: "Cookie", HeaderValue: "custom-cookie"},
			{HeaderKey: "Set-Cookie", HeaderValue: "should-not-be-sent"},
			{HeaderKey: "X-Channel-Metadata", HeaderValue: "kept"},
		},
	}

	request, err := http.NewRequest(http.MethodPost, "https://example.invalid/v1/chat/completions", nil)
	if err != nil {
		t.Fatal(err)
	}
	copySafeUpstreamHeaders(request.Header, clientHeaders)
	applySafeChannelHeaders(request.Header, channel.CustomHeader)
	applySelectedCredentialHeader(request.Header, outbound.OutboundTypeOpenAIChat, "selected-key")
	assertIsolatedCredentialHeaders(t, request.Header, "selected-key")
	if got := request.Header.Get("Cookie"); got != "custom-cookie" {
		t.Fatalf("configured channel Cookie = %q, want custom-cookie", got)
	}
	if got := request.Header.Get("Accept-Language"); got != "zh-CN" {
		t.Fatalf("safe client header = %q, want zh-CN", got)
	}
	if got := request.Header.Get("X-Channel-Metadata"); got != "kept" {
		t.Fatalf("safe custom header = %q, want kept", got)
	}

	wsHeaders := buildUpstreamWSHeaders(clientHeaders, channel, "selected-key")
	assertIsolatedCredentialHeaders(t, wsHeaders, "selected-key")
	if got := wsHeaders.Get("Cookie"); got != "custom-cookie" {
		t.Fatalf("configured WS channel Cookie = %q, want custom-cookie", got)
	}
	if got := wsHeaders.Get("X-Channel-Metadata"); got != "kept" {
		t.Fatalf("WS safe custom header = %q, want kept", got)
	}
}

func TestWSHeaderConstructionCollapsesMixedCaseEntries(t *testing.T) {
	headers := buildUpstreamWSHeaders(http.Header{
		"X-Trace": {"first"},
		"x-trace": {"second"},
	}, nil, "selected-key")
	values := headers.Values("X-Trace")
	if len(values) != 2 || !slices.Contains(values, "first") || !slices.Contains(values, "second") {
		t.Fatalf("mixed-case WS header values = %#v, want first and second", values)
	}
	count := 0
	for key := range headers {
		if strings.EqualFold(key, "X-Trace") {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("mixed-case WS header was retained under %d map keys: %#v", count, headers)
	}
}

func TestSafeHeaderCopyRemovesNonCanonicalDestinationKey(t *testing.T) {
	dst := http.Header{"x-trace": {"old"}}
	copySafeUpstreamHeaders(dst, http.Header{"X-Trace": {"new"}})
	if got := headerValuesCaseInsensitive(dst, "X-Trace"); len(got) != 1 || got[0] != "new" {
		t.Fatalf("normalized copied header = %#v, want [new]", got)
	}
	for key := range dst {
		if key == "x-trace" {
			t.Fatal("non-canonical destination header key was not removed")
		}
	}
}

func TestCredentialHeaderSelectionSupportsProtocolSpecificKeys(t *testing.T) {
	base := http.Header{
		"Authorization":  {"Bearer client-secret"},
		"X-API-Key":      {"client-api-key"},
		"X-Goog-Api-Key": {"client-google-key"},
	}
	for _, tc := range []struct {
		name     string
		protocol outbound.OutboundType
		wantName string
		want     string
	}{
		{"openai", outbound.OutboundTypeOpenAIChat, "Authorization", "Bearer selected-key"},
		{"anthropic", outbound.OutboundTypeAnthropic, "X-API-Key", "selected-key"},
		{"gemini", outbound.OutboundTypeGemini, "X-Goog-Api-Key", "selected-key"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			headers := base.Clone()
			applySelectedCredentialHeader(headers, tc.protocol, "selected-key")
			if got := headers.Get(tc.wantName); got != tc.want {
				t.Fatalf("%s = %q, want %q", tc.wantName, got, tc.want)
			}
			for _, name := range []string{"Authorization", "X-API-Key", "X-Goog-Api-Key", "Api-Key"} {
				if name == tc.wantName {
					continue
				}
				if got := headers.Get(name); got != "" {
					t.Fatalf("unexpected credential %s=%q", name, got)
				}
			}
		})
	}
}

func TestCopyProxyResponseHeadersDoesNotForwardSetCookie(t *testing.T) {
	dst := http.Header{}
	copyProxyResponseHeaders(dst, http.Header{
		"Set-Cookie":   {"upstream-session=secret; Path=/"},
		"X-Request-ID": {"safe-id"},
	})
	if got := dst.Values("Set-Cookie"); len(got) != 0 {
		t.Fatalf("Set-Cookie leaked downstream: %#v", got)
	}
	if got := dst.Get("X-Request-ID"); got != "safe-id" {
		t.Fatalf("safe response header = %q, want safe-id", got)
	}
}

func TestWSHeaderSignatureMergesHeaderNamesCaseInsensitively(t *testing.T) {
	mixedCase := http.Header{
		"X-Trace": {"second"},
		"x-trace": {"first"},
	}
	canonical := http.Header{"X-Trace": {"first", "second"}}
	if got, want := wsHeaderSignature(mixedCase), wsHeaderSignature(canonical); got != want {
		t.Fatalf("case-insensitive header merge changed signature: mixed=%q canonical=%q", got, want)
	}
	if got, want := wsHeaderSignature(http.Header{"X-Trace": {"value"}}), wsHeaderSignature(http.Header{"x-trace": {"value"}}); got != want {
		t.Fatalf("header-name casing changed signature: %q != %q", got, want)
	}
}

func TestCompactProxyHeadersKeepChannelCookieAndDropClientCookie(t *testing.T) {
	dst := http.Header{"Authorization": {"Bearer selected-key"}}
	copyProxyHeaders(http.Header{
		"Cookie":        {"session=client-cookie"},
		"Authorization": {"Bearer client-key"},
		"X-Trace":       {"safe"},
	}, &dbmodel.Channel{CustomHeader: []dbmodel.CustomHeader{
		{HeaderKey: "Cookie", HeaderValue: "session=channel-cookie"},
		{HeaderKey: "Authorization", HeaderValue: "Bearer custom-key"},
		{HeaderKey: "Set-Cookie", HeaderValue: "session=must-not-send"},
	}}, dst)
	applySelectedCredentialHeader(dst, outbound.OutboundTypeOpenAIResponse, "selected-key")

	if got := dst.Get("Cookie"); got != "session=channel-cookie" {
		t.Fatalf("compact channel Cookie = %q, want channel cookie", got)
	}
	if got := dst.Get("Authorization"); got != "Bearer selected-key" {
		t.Fatalf("compact selected credential = %q, want selected key", got)
	}
	if got := dst.Get("Set-Cookie"); got != "" {
		t.Fatalf("compact Set-Cookie must not be sent upstream: %q", got)
	}
	if got := dst.Get("X-Trace"); got != "safe" {
		t.Fatalf("compact safe client header = %q, want safe", got)
	}
}

func TestCompactProxyHeadersCollapseMixedCaseAndKeepSelectedCredential(t *testing.T) {
	dst := http.Header{
		"authorization": {"Bearer selected-key"},
		"x-trace":       {"old"},
	}
	copyProxyHeaders(http.Header{
		"X-Trace": {"first"},
		"x-trace": {"second"},
	}, nil, dst)
	applySelectedCredentialHeader(dst, outbound.OutboundTypeOpenAIResponse, "selected-key")

	if got := headerValuesCaseInsensitive(dst, "X-Trace"); len(got) != 2 || got[0] != "first" || got[1] != "second" {
		t.Fatalf("compact mixed-case header values = %#v, want [first second]", got)
	}
	assertIsolatedCredentialHeaders(t, dst, "selected-key")
	for key := range dst {
		if key == "x-trace" || key == "authorization" {
			t.Fatalf("non-canonical compact header key remained: %q", key)
		}
	}
}

func TestWSHeaderSignatureDoesNotExposeHeaderValues(t *testing.T) {
	headers := http.Header{"Authorization": {"Bearer selected-key"}, "X-Trace": {"trace-value"}}
	signature := wsHeaderSignature(headers)
	if len(signature) != 64 {
		t.Fatalf("signature length = %d, want SHA-256 hex length", len(signature))
	}
	if signature == "" || signature == wsHeaderSignature(http.Header{"Authorization": {"Bearer other-key"}, "X-Trace": {"trace-value"}}) {
		t.Fatalf("signature must distinguish credential-affecting headers")
	}
	for _, forbidden := range []string{"selected-key", "trace-value", "authorization"} {
		if containsInsensitive(signature, forbidden) {
			t.Fatalf("signature exposed %q: %q", forbidden, signature)
		}
	}
}

func assertIsolatedCredentialHeaders(t *testing.T, headers http.Header, key string) {
	t.Helper()
	if got := headers.Get("Authorization"); got != "Bearer "+key {
		t.Fatalf("Authorization = %q, want selected key", got)
	}
	for _, name := range []string{"X-API-Key", "X-Goog-Api-Key", "Api-Key", "Set-Cookie"} {
		if got := headers.Get(name); got != "" {
			t.Fatalf("forbidden header %s leaked as %q", name, got)
		}
	}
}

func TestSelectedCredentialRemovesNonCanonicalAliases(t *testing.T) {
	headers := http.Header{
		"authorization":  {"Bearer stale"},
		"x-api-key":      {"stale-api-key"},
		"X-Goog-Api-Key": {"stale-google-key"},
	}
	applySelectedCredentialHeader(headers, outbound.OutboundTypeOpenAIResponse, "selected-key")
	assertIsolatedCredentialHeaders(t, headers, "selected-key")
	for key := range headers {
		if key != "Authorization" {
			t.Fatalf("non-authoritative credential key remained: %q", key)
		}
	}
}

func containsInsensitive(value, fragment string) bool {
	for i := 0; i+len(fragment) <= len(value); i++ {
		matched := true
		for j := range fragment {
			left, right := value[i+j], fragment[j]
			if left >= 'A' && left <= 'Z' {
				left += 'a' - 'A'
			}
			if right >= 'A' && right <= 'Z' {
				right += 'a' - 'A'
			}
			if left != right {
				matched = false
				break
			}
		}
		if matched {
			return true
		}
	}
	return false
}
