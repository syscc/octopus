package sitesync

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	dbpkg "github.com/bestruirui/octopus/internal/db"
	"github.com/bestruirui/octopus/internal/model"
	"github.com/bestruirui/octopus/internal/op"
)

func TestRefreshDueSub2APISessionsIncludesAutoSyncDisabledAccounts(t *testing.T) {
	ctx := setupProjectTestDB(t)
	refreshCalls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/auth/refresh" {
			http.NotFound(w, r)
			return
		}
		refreshCalls++
		if r.Header.Get("Authorization") != "" {
			t.Errorf("refresh request must not include the expired access token")
		}
		var body map[string]string
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body["refresh_token"] != "due-refresh" {
			t.Errorf("unexpected refresh payload: %+v, %v", body, err)
		}
		_, _ = w.Write([]byte(`{"code":0,"data":{"access_token":"rotated-access","refresh_token":"rotated-refresh","expires_in":86400}}`))
	}))
	defer server.Close()

	site := &model.Site{Name: "refresh-job-site", Platform: model.SitePlatformSub2API, BaseURL: server.URL, Enabled: true}
	if err := op.SiteCreate(site, ctx); err != nil {
		t.Fatal(err)
	}
	dueAccount := &model.SiteAccount{
		SiteID:         site.ID,
		Name:           "auto-sync-disabled",
		CredentialType: model.SiteCredentialTypeAccessToken,
		AccessToken:    "due-access",
		RefreshToken:   "due-refresh",
		TokenExpiresAt: time.Now().Add(time.Minute).UnixMilli(),
		Enabled:        true,
		AutoSync:       false,
		AutoSyncSet:    true,
		AutoCheckin:    false,
		AutoCheckinSet: true,
	}
	if err := op.SiteAccountCreate(dueAccount, ctx); err != nil {
		t.Fatal(err)
	}
	futureAccount := &model.SiteAccount{
		SiteID:         site.ID,
		Name:           "not-due",
		CredentialType: model.SiteCredentialTypeAccessToken,
		AccessToken:    "future-access",
		RefreshToken:   "future-refresh",
		TokenExpiresAt: time.Now().Add(24 * time.Hour).UnixMilli(),
		Enabled:        true,
	}
	if err := op.SiteAccountCreate(futureAccount, ctx); err != nil {
		t.Fatal(err)
	}

	refreshed, err := RefreshDueSub2APISessions(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if refreshed != 1 || refreshCalls != 1 {
		t.Fatalf("expected only the due account to refresh, refreshed=%d calls=%d", refreshed, refreshCalls)
	}
	var due, future model.SiteAccount
	if err := dbpkg.GetDB().WithContext(ctx).First(&due, dueAccount.ID).Error; err != nil {
		t.Fatal(err)
	}
	if due.AccessToken != "rotated-access" || due.RefreshToken != "rotated-refresh" || due.TokenExpiresAt <= time.Now().UnixMilli() {
		t.Fatalf("rotated credentials were not persisted: %+v", due)
	}
	if err := dbpkg.GetDB().WithContext(ctx).First(&future, futureAccount.ID).Error; err != nil {
		t.Fatal(err)
	}
	if future.AccessToken != "future-access" || future.RefreshToken != "future-refresh" {
		t.Fatalf("not-yet-due credentials unexpectedly changed")
	}
}

func TestRefreshDueSub2APISessionsRefreshesUnknownExpiryOnlyOnce(t *testing.T) {
	ctx := setupProjectTestDB(t)
	refreshCalls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/auth/refresh" {
			http.NotFound(w, r)
			return
		}
		refreshCalls++
		if r.Header.Get("Authorization") != "" {
			t.Errorf("refresh request must not include the access token")
		}
		var body map[string]string
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body["refresh_token"] != "valid-refresh" {
			t.Errorf("unexpected refresh payload: %+v, %v", body, err)
		}
		_, _ = w.Write([]byte(`{"code":0,"data":{"access_token":"rotated-access","refresh_token":"rotated-refresh","expires_in":86400}}`))
	}))
	defer server.Close()

	site := &model.Site{Name: "refresh-unknown-expiry-site", Platform: model.SitePlatformSub2API, BaseURL: server.URL, Enabled: true}
	if err := op.SiteCreate(site, ctx); err != nil {
		t.Fatal(err)
	}
	account := &model.SiteAccount{
		SiteID:         site.ID,
		Name:           "refresh-unknown-expiry",
		CredentialType: model.SiteCredentialTypeAccessToken,
		AccessToken:    "valid-access",
		RefreshToken:   "valid-refresh",
		TokenExpiresAt: 0,
		Enabled:        true,
	}
	if err := op.SiteAccountCreate(account, ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { sub2APIRefreshRetryAfter.Delete(account.ID) })

	refreshed, err := RefreshDueSub2APISessions(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if refreshed != 1 || refreshCalls != 1 {
		t.Fatalf("expected one refresh for unknown expiry, refreshed=%d calls=%d", refreshed, refreshCalls)
	}

	var persisted model.SiteAccount
	if err := dbpkg.GetDB().WithContext(ctx).First(&persisted, account.ID).Error; err != nil {
		t.Fatal(err)
	}
	if persisted.AccessToken != "rotated-access" || persisted.RefreshToken != "rotated-refresh" || persisted.TokenExpiresAt <= time.Now().UnixMilli() {
		t.Fatalf("rotated credentials were not persisted with expiry: %+v", persisted)
	}

	refreshed, err = RefreshDueSub2APISessions(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if refreshed != 0 || refreshCalls != 1 {
		t.Fatalf("expected valid rotated session to be skipped, refreshed=%d calls=%d", refreshed, refreshCalls)
	}
}

func TestRefreshDueSub2APISessionsBacksOffUntilRefreshTokenChanges(t *testing.T) {
	ctx := setupProjectTestDB(t)
	refreshTokens := make([]string, 0, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/auth/refresh" {
			http.NotFound(w, r)
			return
		}
		var body map[string]string
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("decode refresh payload: %v", err)
		}
		refreshTokens = append(refreshTokens, body["refresh_token"])
		if body["refresh_token"] != "manual-refresh" {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"message":"temporary refresh failure"}`))
			return
		}
		_, _ = w.Write([]byte(`{"code":0,"data":{"access_token":"manual-access-rotated","refresh_token":"manual-refresh-rotated","expires_in":86400}}`))
	}))
	defer server.Close()

	site := &model.Site{Name: "refresh-backoff-site", Platform: model.SitePlatformSub2API, BaseURL: server.URL, Enabled: true}
	if err := op.SiteCreate(site, ctx); err != nil {
		t.Fatal(err)
	}
	account := &model.SiteAccount{
		SiteID:         site.ID,
		Name:           "refresh-backoff",
		CredentialType: model.SiteCredentialTypeAccessToken,
		AccessToken:    "expired-access",
		RefreshToken:   "old-refresh",
		TokenExpiresAt: time.Now().Add(-time.Minute).UnixMilli(),
		Enabled:        true,
	}
	if err := op.SiteAccountCreate(account, ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { sub2APIRefreshRetryAfter.Delete(account.ID) })

	refreshed, err := RefreshDueSub2APISessions(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if refreshed != 0 || len(refreshTokens) != 1 || refreshTokens[0] != "old-refresh" {
		t.Fatalf("expected first refresh to fail once, refreshed=%d tokens=%v", refreshed, refreshTokens)
	}
	retry, ok := sub2APIRefreshRetryAfter.Load(account.ID)
	if !ok || !time.Now().Before(retry.(sub2APIRefreshRetry).until) {
		t.Fatalf("expected a temporary retry cooldown, got %v", retry)
	}

	refreshed, err = RefreshDueSub2APISessions(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if refreshed != 0 || len(refreshTokens) != 1 {
		t.Fatalf("expected retry cooldown to suppress the next attempt, refreshed=%d tokens=%v", refreshed, refreshTokens)
	}

	if err := dbpkg.GetDB().WithContext(ctx).Model(&model.SiteAccount{}).Where("id = ?", account.ID).Update("refresh_token", "manual-refresh").Error; err != nil {
		t.Fatal(err)
	}
	refreshed, err = RefreshDueSub2APISessions(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if refreshed != 1 || len(refreshTokens) != 2 || refreshTokens[1] != "manual-refresh" {
		t.Fatalf("expected manual refresh token change to invalidate cooldown, refreshed=%d tokens=%v", refreshed, refreshTokens)
	}

	var persisted model.SiteAccount
	if err := dbpkg.GetDB().WithContext(ctx).First(&persisted, account.ID).Error; err != nil {
		t.Fatal(err)
	}
	if persisted.AccessToken != "manual-access-rotated" || persisted.RefreshToken != "manual-refresh-rotated" {
		t.Fatalf("manual refresh credentials were not persisted: %+v", persisted)
	}
}

func TestRefreshDueSub2APISessionsTimeoutContinuesAndBacksOff(t *testing.T) {
	ctx := setupProjectTestDB(t)
	refreshCalls := make([]string, 0, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/auth/refresh" {
			http.NotFound(w, r)
			return
		}
		var body map[string]string
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("decode refresh payload: %v", err)
			return
		}
		refreshCalls = append(refreshCalls, body["refresh_token"])
		if body["refresh_token"] == "blocked-refresh" {
			<-r.Context().Done()
			return
		}
		_, _ = w.Write([]byte(`{"code":0,"data":{"access_token":"healthy-access","refresh_token":"healthy-refresh-rotated","expires_in":86400}}`))
	}))
	defer server.Close()

	site := &model.Site{Name: "refresh-timeout-site", Platform: model.SitePlatformSub2API, BaseURL: server.URL, Enabled: true}
	if err := op.SiteCreate(site, ctx); err != nil {
		t.Fatal(err)
	}
	blockedAccount := &model.SiteAccount{
		SiteID:         site.ID,
		Name:           "blocked-refresh",
		CredentialType: model.SiteCredentialTypeAccessToken,
		AccessToken:    "blocked-access",
		RefreshToken:   "blocked-refresh",
		TokenExpiresAt: time.Now().Add(-time.Minute).UnixMilli(),
		Enabled:        true,
	}
	if err := op.SiteAccountCreate(blockedAccount, ctx); err != nil {
		t.Fatal(err)
	}
	healthyAccount := &model.SiteAccount{
		SiteID:         site.ID,
		Name:           "healthy-refresh",
		CredentialType: model.SiteCredentialTypeAccessToken,
		AccessToken:    "healthy-old-access",
		RefreshToken:   "healthy-refresh",
		TokenExpiresAt: time.Now().Add(-time.Minute).UnixMilli(),
		Enabled:        true,
	}
	if err := op.SiteAccountCreate(healthyAccount, ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		sub2APIRefreshRetryAfter.Delete(blockedAccount.ID)
		sub2APIRefreshRetryAfter.Delete(healthyAccount.ID)
	})

	refreshed, err := refreshDueSub2APISessions(ctx, 100*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if refreshed != 1 || len(refreshCalls) != 2 {
		t.Fatalf("expected timeout account to be skipped and healthy account to refresh, refreshed=%d calls=%v", refreshed, refreshCalls)
	}
	if refreshCalls[0] != "blocked-refresh" || refreshCalls[1] != "healthy-refresh" {
		t.Fatalf("unexpected refresh order: %v", refreshCalls)
	}
	if retry, ok := sub2APIRefreshRetryAfter.Load(blockedAccount.ID); !ok || !time.Now().Before(retry.(sub2APIRefreshRetry).until) {
		t.Fatalf("expected timed out account to receive a cooldown, got %v", retry)
	}

	refreshed, err = refreshDueSub2APISessions(ctx, 100*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if refreshed != 0 || len(refreshCalls) != 2 {
		t.Fatalf("expected timeout cooldown to suppress the next round, refreshed=%d calls=%v", refreshed, refreshCalls)
	}
}

func TestRefreshDueSub2APISessionsParentCancellationExitsWithoutCooldown(t *testing.T) {
	parentCtx := setupProjectTestDB(t)
	ctx, cancel := context.WithCancel(parentCtx)
	defer cancel()
	refreshStarted := make(chan struct{})
	releaseRefresh := make(chan struct{})
	release := func() {
		select {
		case <-releaseRefresh:
		default:
			close(releaseRefresh)
		}
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/auth/refresh" {
			http.NotFound(w, r)
			return
		}
		if _, err := io.Copy(io.Discard, r.Body); err != nil {
			t.Errorf("read refresh payload: %v", err)
			return
		}
		close(refreshStarted)
		<-releaseRefresh
	}))
	defer func() {
		release()
		server.Close()
	}()

	site := &model.Site{Name: "refresh-cancel-site", Platform: model.SitePlatformSub2API, BaseURL: server.URL, Enabled: true}
	if err := op.SiteCreate(site, parentCtx); err != nil {
		t.Fatal(err)
	}
	account := &model.SiteAccount{
		SiteID:         site.ID,
		Name:           "cancelled-refresh",
		CredentialType: model.SiteCredentialTypeAccessToken,
		AccessToken:    "cancelled-access",
		RefreshToken:   "cancelled-refresh",
		TokenExpiresAt: time.Now().Add(-time.Minute).UnixMilli(),
		Enabled:        true,
	}
	if err := op.SiteAccountCreate(account, parentCtx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { sub2APIRefreshRetryAfter.Delete(account.ID) })

	type refreshResult struct {
		refreshed int
		err       error
	}
	result := make(chan refreshResult, 1)
	go func() {
		refreshed, err := refreshDueSub2APISessions(ctx, time.Second)
		result <- refreshResult{refreshed: refreshed, err: err}
	}()

	select {
	case <-refreshStarted:
		cancel()
		release()
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the refresh request")
	}

	select {
	case got := <-result:
		if got.refreshed != 0 || !errors.Is(got.err, context.Canceled) {
			t.Fatalf("expected parent cancellation to stop refresh without success, got refreshed=%d err=%v", got.refreshed, got.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for cancelled refresh job")
	}
	if _, ok := sub2APIRefreshRetryAfter.Load(account.ID); ok {
		t.Fatal("parent cancellation must not create a refresh cooldown")
	}
}
