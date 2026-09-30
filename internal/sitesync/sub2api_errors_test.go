package sitesync

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/bestruirui/octopus/internal/apperror"
	"github.com/bestruirui/octopus/internal/model"
)

func TestSyncSub2APIKeepsRefreshFailureAfterUnauthorized(t *testing.T) {
	server := newSub2APIRefreshFailureServer(t)
	defer server.Close()

	_, err := syncSub2APIWithCreatedToken(context.Background(), &model.Site{
		Platform: model.SitePlatformSub2API,
		BaseURL:  server.URL,
	}, &model.SiteAccount{
		CredentialType: model.SiteCredentialTypeAccessToken,
		AccessToken:    "old-access-token",
		RefreshToken:   "refresh-secret",
	}, nil)
	if err == nil {
		t.Fatal("expected sync to fail")
	}

	assertSub2APIRefreshFailureVisible(t, err)
	assertOriginalSub2APIErrorMetadata(t, err)
}

func TestCreateSub2APITokenKeepsRefreshFailureAfterUnauthorized(t *testing.T) {
	server := newSub2APIRefreshFailureServer(t)
	defer server.Close()

	_, err := createSub2APIToken(context.Background(), &model.Site{
		Platform: model.SitePlatformSub2API,
		BaseURL:  server.URL,
	}, &model.SiteAccount{
		CredentialType: model.SiteCredentialTypeAccessToken,
		AccessToken:    "old-access-token",
		RefreshToken:   "refresh-secret",
	}, model.SiteDefaultGroupKey, "test")
	if err == nil {
		t.Fatal("expected key creation to fail")
	}

	assertSub2APIRefreshFailureVisible(t, err)
	assertOriginalSub2APIErrorMetadata(t, err)
}

func TestSub2APIProactiveRefreshFailureKeepsVisibleDetail(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/auth/refresh" {
			t.Error("expired access token was used after refresh failed")
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"message":"refresh token expired refresh_token=fixture-secret"}`))
	}))
	defer server.Close()
	site := &model.Site{Platform: model.SitePlatformSub2API, BaseURL: server.URL}
	for _, operation := range []string{"sync", "create"} {
		t.Run(operation, func(t *testing.T) {
			account := &model.SiteAccount{CredentialType: model.SiteCredentialTypeAccessToken,
				AccessToken: "fixture-access", RefreshToken: "fixture-secret", TokenExpiresAt: 1}
			var err error
			if operation == "sync" {
				_, err = syncSub2API(context.Background(), site, account)
			} else {
				_, err = createSub2APIToken(context.Background(), site, account, model.SiteDefaultGroupKey, "fixture")
			}
			if err == nil {
				t.Fatal("expected expired refresh token to fail")
			}
			message := sanitizeSiteStatusMessage(err)
			if !strings.Contains(message, "refresh token expired") || strings.Contains(message, "fixture-secret") {
				t.Fatalf("proactive failure lost its reason or leaked credentials: %q", message)
			}
			if apperror.Params(err)["sub2apiRefreshFailure"] != message {
				t.Fatal("proactive refresh failure did not include visible API detail")
			}
			assertOriginalSub2APIErrorMetadata(t, err)
		})
	}
}

func TestWrapSub2APIRefreshFailurePreservesOriginalAppError(t *testing.T) {
	original := apperror.New(CodeSiteUpstreamHTTPError, "http 401: unauthorized").
		WithStatus(http.StatusBadGateway).
		WithParam("statusCode", 401).
		WithParam("source", "sub2api")
	refreshErr := fmt.Errorf("refresh failed refresh_token=refresh-secret")

	err := wrapSub2APIRefreshFailure(original, refreshErr)
	message := sanitizeSiteStatusMessage(err)
	if !strings.Contains(message, "sub2api refresh failed") || !strings.Contains(message, "refresh failed") {
		t.Fatalf("sanitized message = %q, want refresh failure", message)
	}
	if strings.Contains(message, "refresh-secret") {
		t.Fatalf("sanitized message leaked refresh token: %q", message)
	}
	assertOriginalSub2APIErrorMetadata(t, err)

	sanitized := sanitizeSiteError(err)
	if !strings.Contains(apperror.Message(sanitized), "sub2api refresh failed") {
		t.Fatalf("sanitized app error message = %q, want refresh failure", apperror.Message(sanitized))
	}
	assertOriginalSub2APIErrorMetadata(t, sanitized)
	if !errors.Is(err, original) || !errors.Is(err, refreshErr) {
		t.Fatal("refresh failure wrapper lost its error causes")
	}
	if _, ok := apperror.Params(original)["sub2apiRefreshFailure"]; ok {
		t.Fatal("refresh wrapper modified the original error params")
	}
	if apperror.Params(err)["sub2apiRefreshFailure"] != message {
		t.Fatal("API detail did not preserve the sanitized refresh failure")
	}
}

func TestWrapSub2APIRefreshFailureKeepsRefreshReasonBeforeTruncation(t *testing.T) {
	original := newSiteHTTPError(http.StatusUnauthorized, strings.Repeat("access token detail ", 40))
	refreshErr := newSiteHTTPError(http.StatusUnauthorized, "refresh token expired refresh_token=fixture-secret")
	err := wrapSub2APIRefreshFailure(original, refreshErr)
	message := sanitizeSiteStatusMessage(err)
	if !strings.Contains(message, "refresh token expired") || strings.Contains(message, "fixture-secret") {
		t.Fatalf("refresh reason was lost or leaked a credential: %q", message)
	}
	if utf8.RuneCountInString(message) > maxSiteStatusMessageRunes {
		t.Fatal("refresh error exceeded the status message limit")
	}
	if apperror.Params(err)["sub2apiRefreshFailure"] != message {
		t.Fatal("API detail differs from the sanitized status message")
	}
}

func newSub2APIRefreshFailureServer(t *testing.T) *httptest.Server {
	t.Helper()
	longSensitiveMessage := "refresh failed refresh_token=refresh-secret " + strings.Repeat("provider detail ", 40)
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.URL.Path == "/api/v1/auth/refresh" && r.Method == http.MethodPost:
			w.WriteHeader(http.StatusServiceUnavailable)
			_ = json.NewEncoder(w).Encode(map[string]any{"message": longSensitiveMessage})
		case (r.URL.Path == "/api/v1/keys" || r.URL.Path == "/api/v1/api-keys") && (r.Method == http.MethodGet || r.Method == http.MethodPost):
			w.WriteHeader(http.StatusUnauthorized)
			_ = json.NewEncoder(w).Encode(map[string]any{"message": "unauthorized Authorization Bearer old-access-token"})
		default:
			http.NotFound(w, r)
		}
	}))
}

func assertSub2APIRefreshFailureVisible(t *testing.T, err error) {
	t.Helper()
	message := sanitizeSiteStatusMessage(err)
	if !strings.Contains(message, "sub2api refresh failed") || !strings.Contains(message, "refresh failed") {
		t.Fatalf("sanitized message = %q, want refresh failure", message)
	}
	if strings.Contains(message, "old-access-token") || strings.Contains(message, "refresh-secret") {
		t.Fatalf("sanitized message leaked secret: %q", message)
	}
	if utf8.RuneCountInString(message) > maxSiteStatusMessageRunes {
		t.Fatalf("sanitized message length = %d, want <= %d", utf8.RuneCountInString(message), maxSiteStatusMessageRunes)
	}

	sanitized := sanitizeSiteError(err)
	if !strings.Contains(apperror.Message(sanitized), "sub2api refresh failed") {
		t.Fatalf("sanitized app error message = %q, want refresh failure", apperror.Message(sanitized))
	}
}

func assertOriginalSub2APIErrorMetadata(t *testing.T, err error) {
	t.Helper()
	if got := apperror.Code(err); got != CodeSiteUpstreamHTTPError {
		t.Fatalf("error code = %q, want %q", got, CodeSiteUpstreamHTTPError)
	}
	if got := apperror.Status(err); got != http.StatusBadGateway {
		t.Fatalf("error status = %d, want %d", got, http.StatusBadGateway)
	}
	params := apperror.Params(err)
	if params["statusCode"] != 401 {
		t.Fatalf("error params = %#v, want original params", params)
	}
}
