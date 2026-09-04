package model

import (
	"fmt"
	"net/url"
	"strings"
)

// CloudflareWorkersAIHost is the only host that serves the documented
// Cloudflare Workers AI REST endpoints.
const CloudflareWorkersAIHost = "api.cloudflare.com"

// cloudflareWorkersAIBase is a parsed documented Cloudflare Workers AI base
// URL path, which must be exactly one of:
//
//	/client/v4/accounts/{account_id}/ai
//	/client/v4/accounts/{account_id}/ai/v1
type cloudflareWorkersAIBase struct {
	accountID        string
	openAICompatible bool
	path             string
}

// IsCloudflareWorkersAIHost reports whether host is the Cloudflare API host.
// Lookalike hosts such as api.cloudflare.com.evil.example are rejected.
func IsCloudflareWorkersAIHost(host string) bool {
	return strings.EqualFold(strings.TrimSpace(host), CloudflareWorkersAIHost)
}

// isCloudflareAccountIDSegment reports whether segment is a plausible single
// Cloudflare account id path segment. Restricting the charset keeps traversal
// (".."), empty and whitespace-only segments out of the canonical path.
func isCloudflareAccountIDSegment(segment string) bool {
	if segment == "" {
		return false
	}
	for i := 0; i < len(segment); i++ {
		c := segment[i]
		switch {
		case c >= 'a' && c <= 'z',
			c >= 'A' && c <= 'Z',
			c >= '0' && c <= '9',
			c == '-', c == '_':
		default:
			return false
		}
	}
	return true
}

// isCloudflareAPIVersionSegment reports whether segment looks like an API
// version selector such as v1 or v2. Only v1 is documented, so any other
// version-shaped segment must never be treated as an endpoint suffix.
func isCloudflareAPIVersionSegment(segment string) bool {
	if len(segment) < 2 || (segment[0] != 'v' && segment[0] != 'V') {
		return false
	}
	for i := 1; i < len(segment); i++ {
		if segment[i] < '0' || segment[i] > '9' {
			return false
		}
	}
	return true
}

// hasCloudflareWorkersAIPortBypass reports whether the URL carries an explicit
// port other than the https default, which would point the "documented" host
// at a different listener. All entry points require the https scheme before
// consulting this helper.
func hasCloudflareWorkersAIPortBypass(parsed *url.URL) bool {
	port := parsed.Port()
	return port != "" && port != "443"
}

// hasCloudflareWorkersAIEncodedPath reports whether the raw path differs from
// the canonical encoding of the decoded path. Such URLs would be validated as
// one path but sent as another (for example /ai%2Fv1 or /%63lient/v4).
func hasCloudflareWorkersAIEncodedPath(parsed *url.URL) bool {
	return parsed.RawPath != ""
}

// parseCloudflareWorkersAIPath matches path from the root, so extra prefixes
// are rejected. When allowTrailing is true the documented base may be followed
// by endpoint segments (for example /chat/completions), which are dropped from
// the canonical path. Unknown API version segments (/ai/v2) are always
// rejected instead of being treated as endpoint suffixes.
func parseCloudflareWorkersAIPath(path string, allowTrailing bool) (cloudflareWorkersAIBase, bool) {
	trimmed := strings.Trim(strings.TrimSpace(path), "/")
	if trimmed == "" {
		return cloudflareWorkersAIBase{}, false
	}
	segments := strings.Split(trimmed, "/")
	if len(segments) < 5 {
		return cloudflareWorkersAIBase{}, false
	}
	for _, segment := range segments {
		if segment == "" {
			return cloudflareWorkersAIBase{}, false
		}
	}
	if !strings.EqualFold(segments[0], "client") ||
		!strings.EqualFold(segments[1], "v4") ||
		!strings.EqualFold(segments[2], "accounts") ||
		!strings.EqualFold(segments[4], "ai") {
		return cloudflareWorkersAIBase{}, false
	}
	accountID := segments[3]
	if !isCloudflareAccountIDSegment(accountID) {
		return cloudflareWorkersAIBase{}, false
	}

	base := cloudflareWorkersAIBase{accountID: accountID}
	end := 5
	if len(segments) > 5 {
		switch {
		case strings.EqualFold(segments[5], "v1"):
			base.openAICompatible = true
			end = 6
		case isCloudflareAPIVersionSegment(segments[5]):
			// /ai/v2 and friends are undocumented API versions, not endpoints
			// below the management base.
			return cloudflareWorkersAIBase{}, false
		}
	}
	if !allowTrailing && len(segments) != end {
		return cloudflareWorkersAIBase{}, false
	}

	base.path = "/client/v4/accounts/" + accountID + "/ai"
	if base.openAICompatible {
		base.path += "/v1"
	}
	return base, true
}

// ValidateCloudflareWorkersAIBaseURL accepts only the documented Cloudflare
// Workers AI base URL forms:
//
//	https://api.cloudflare.com/client/v4/accounts/{account_id}/ai
//	https://api.cloudflare.com/client/v4/accounts/{account_id}/ai/v1
func ValidateCloudflareWorkersAIBaseURL(parsed *url.URL) error {
	if parsed == nil {
		return fmt.Errorf("cloudflare workers ai base url is invalid")
	}
	if parsed.Scheme != "https" {
		return fmt.Errorf("cloudflare workers ai base url must use https")
	}
	if !IsCloudflareWorkersAIHost(parsed.Hostname()) {
		return fmt.Errorf("cloudflare workers ai base url must use %s", CloudflareWorkersAIHost)
	}
	if parsed.User != nil {
		return fmt.Errorf("cloudflare workers ai base url must not include userinfo")
	}
	if hasCloudflareWorkersAIPortBypass(parsed) {
		return fmt.Errorf("cloudflare workers ai base url must not override the default port")
	}
	if parsed.RawQuery != "" || parsed.ForceQuery || parsed.Fragment != "" {
		return fmt.Errorf("cloudflare workers ai base url must not include query parameters or fragments")
	}
	if hasCloudflareWorkersAIEncodedPath(parsed) {
		return fmt.Errorf("cloudflare workers ai base url must not include percent-encoded path segments")
	}
	if _, ok := parseCloudflareWorkersAIPath(parsed.Path, false); !ok {
		return fmt.Errorf("cloudflare workers ai base url path must be /client/v4/accounts/{account_id}/ai or /client/v4/accounts/{account_id}/ai/v1")
	}
	return nil
}

// NormalizeCloudflareWorkersAIBaseURL rewrites a Cloudflare Workers AI URL that
// may point at a deeper endpoint or carry a query/fragment back to its
// documented base URL. It reports false when raw is not a Cloudflare Workers AI
// URL at all, including bare account paths without the /ai segment.
func NormalizeCloudflareWorkersAIBaseURL(raw string) (string, bool) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "", false
	}
	parsed, err := url.Parse(trimmed)
	if err != nil || parsed.User != nil {
		return "", false
	}
	if parsed.Scheme != "https" {
		return "", false
	}
	if !IsCloudflareWorkersAIHost(parsed.Hostname()) {
		return "", false
	}
	if hasCloudflareWorkersAIPortBypass(parsed) || hasCloudflareWorkersAIEncodedPath(parsed) {
		return "", false
	}
	base, ok := parseCloudflareWorkersAIPath(parsed.Path, true)
	if !ok {
		return "", false
	}
	return "https://" + CloudflareWorkersAIHost + base.path, true
}

// CanonicalCloudflareWorkersAIBaseURL rewrites a strict Cloudflare Workers AI
// base URL into its canonical persisted form: https scheme, the canonical
// api.cloudflare.com host, no explicit default port and lowercase fixed path
// segments. The account id segment keeps its original characters, including
// case. It reports false when raw is not a strict documented base URL; such
// URLs must be rejected through ValidateCloudflareWorkersAIBaseURL instead of
// being silently rewritten.
func CanonicalCloudflareWorkersAIBaseURL(raw string) (string, bool) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "", false
	}
	parsed, err := url.Parse(trimmed)
	if err != nil || parsed.User != nil {
		return "", false
	}
	if parsed.Scheme != "https" || !IsCloudflareWorkersAIHost(parsed.Hostname()) {
		return "", false
	}
	if hasCloudflareWorkersAIPortBypass(parsed) || hasCloudflareWorkersAIEncodedPath(parsed) {
		return "", false
	}
	if parsed.RawQuery != "" || parsed.ForceQuery || parsed.Fragment != "" {
		return "", false
	}
	base, ok := parseCloudflareWorkersAIPath(parsed.Path, false)
	if !ok {
		return "", false
	}
	return "https://" + CloudflareWorkersAIHost + base.path, true
}
