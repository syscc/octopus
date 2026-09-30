package sitesync

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/bestruirui/octopus/internal/db"
	"github.com/bestruirui/octopus/internal/model"
)

func TestSub2APIRefreshPersistsRotatedSessionWithoutOldBearer(t *testing.T) {
	ctx := setupProjectTestDB(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/auth/refresh" {
			http.NotFound(w, r)
			return
		}
		if r.Header.Get("Authorization") != "" || r.Header.Get(sub2APIUserUIRequestHeader) != "1" || r.Header.Get("Content-Type") != "application/json" || r.Header.Get("X-Site-Test") != "preserved" {
			t.Errorf("unexpected refresh headers: %v", r.Header)
		}
		var body map[string]string
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body["refresh_token"] != "old-refresh" {
			t.Errorf("unexpected refresh body: %v, %v", body, err)
		}
		_, _ = w.Write([]byte(`{"code":0,"data":{"access_token":"new-access","refresh_token":"new-refresh","expires_in":3600}}`))
	}))
	defer server.Close()
	site := &model.Site{Name: "refresh-test-site", Platform: model.SitePlatformSub2API, BaseURL: server.URL}
	if err := db.GetDB().Create(site).Error; err != nil {
		t.Fatal(err)
	}
	account := &model.SiteAccount{SiteID: site.ID, Name: "refresh-test", CredentialType: model.SiteCredentialTypeAccessToken, AccessToken: "old-access", RefreshToken: "old-refresh", TokenExpiresAt: 1}
	if err := db.GetDB().Create(account).Error; err != nil {
		t.Fatal(err)
	}
	refreshSite := &model.Site{BaseURL: server.URL, CustomHeader: []model.CustomHeader{
		{HeaderKey: " authorization ", HeaderValue: "Bearer obsolete-access"},
		{HeaderKey: "X-Site-Test", HeaderValue: "preserved"},
	}}
	token, err := ensureFreshSub2APIAccessToken(ctx, refreshSite, account, false)
	if err != nil || token != "new-access" {
		t.Fatalf("refresh: token=%q err=%v", token, err)
	}
	var persisted model.SiteAccount
	if err := db.GetDB().First(&persisted, account.ID).Error; err != nil {
		t.Fatal(err)
	}
	if persisted.AccessToken != token || persisted.RefreshToken != "new-refresh" || persisted.TokenExpiresAt <= 1 {
		t.Fatalf("session not persisted together: %+v", persisted)
	}
	if len(refreshSite.CustomHeader) != 2 {
		t.Fatalf("refresh changed original site custom headers: %+v", refreshSite.CustomHeader)
	}
}

func TestSub2APIUsesPersistedSessionAfterConcurrentRotation(t *testing.T) {
	ctx := setupProjectTestDB(t)
	refreshCalls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		refreshCalls++
		t.Errorf("stale credentials were refreshed after the DB session changed: %s", r.URL.Path)
	}))
	defer server.Close()

	site := &model.Site{Name: "concurrent-refresh-site", Platform: model.SitePlatformSub2API, BaseURL: server.URL}
	if err := db.GetDB().Create(site).Error; err != nil {
		t.Fatal(err)
	}
	account := &model.SiteAccount{
		SiteID:         site.ID,
		Name:           "concurrent-refresh",
		CredentialType: model.SiteCredentialTypeAccessToken,
		AccessToken:    "old-access",
		RefreshToken:   "old-refresh",
		TokenExpiresAt: 1,
	}
	if err := db.GetDB().Create(account).Error; err != nil {
		t.Fatal(err)
	}

	latestExpiresAt := time.Now().Add(time.Hour).UnixMilli()
	if err := db.GetDB().Model(&model.SiteAccount{}).Where("id = ?", account.ID).Updates(map[string]any{
		"access_token":     "latest-access",
		"refresh_token":    "latest-refresh",
		"token_expires_at": latestExpiresAt,
	}).Error; err != nil {
		t.Fatal(err)
	}

	token, err := ensureFreshSub2APIAccessToken(ctx, &model.Site{BaseURL: server.URL}, account, true)
	if err != nil {
		t.Fatalf("use persisted session: %v", err)
	}
	if token != "latest-access" || account.AccessToken != "latest-access" || account.RefreshToken != "latest-refresh" || account.TokenExpiresAt != latestExpiresAt {
		t.Fatalf("latest persisted session was not adopted: token=%q account=%+v", token, account)
	}
	if refreshCalls != 0 {
		t.Fatalf("expected no upstream refresh with stale credentials, got %d calls", refreshCalls)
	}
}

func TestSub2APIRefreshDoesNotOverwriteManualAccessTokenDuringRequest(t *testing.T) {
	ctx := setupProjectTestDB(t)
	refreshStarted := make(chan struct{})
	releaseRefresh := make(chan struct{})
	release := func() {
		select {
		case <-releaseRefresh:
		default:
			close(releaseRefresh)
		}
	}
	refreshCalls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/auth/refresh" {
			http.NotFound(w, r)
			return
		}
		refreshCalls++
		if r.Header.Get("Authorization") != "" {
			t.Errorf("refresh request leaked the old bearer: %q", r.Header.Get("Authorization"))
		}
		var body map[string]string
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body["refresh_token"] != "stable-refresh" {
			t.Errorf("unexpected refresh payload: %+v, %v", body, err)
		}
		close(refreshStarted)
		<-releaseRefresh
		_, _ = w.Write([]byte(`{"code":0,"data":{"access_token":"old-refresh-response-access","refresh_token":"rotated-refresh","expires_in":3600}}`))
	}))
	defer func() {
		release()
		server.Close()
	}()

	site := &model.Site{Name: "manual-access-during-refresh", Platform: model.SitePlatformSub2API, BaseURL: server.URL}
	if err := db.GetDB().Create(site).Error; err != nil {
		t.Fatal(err)
	}
	account := &model.SiteAccount{
		SiteID:         site.ID,
		Name:           "manual-access-during-refresh",
		CredentialType: model.SiteCredentialTypeAccessToken,
		AccessToken:    "old-access",
		RefreshToken:   "stable-refresh",
		TokenExpiresAt: 1,
	}
	if err := db.GetDB().Create(account).Error; err != nil {
		t.Fatal(err)
	}

	type refreshResult struct {
		token string
		err   error
	}
	result := make(chan refreshResult, 1)
	go func() {
		token, err := ensureFreshSub2APIAccessToken(ctx, &model.Site{BaseURL: server.URL}, account, true)
		result <- refreshResult{token: token, err: err}
	}()

	select {
	case <-refreshStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the refresh request")
	}
	if err := db.GetDB().WithContext(ctx).Model(&model.SiteAccount{}).Where("id = ?", account.ID).Update("access_token", "manual-access").Error; err != nil {
		t.Fatal(err)
	}
	release()

	select {
	case refreshed := <-result:
		if refreshed.err != nil {
			t.Fatalf("refresh: %v", refreshed.err)
		}
		if refreshed.token != "manual-access" {
			t.Fatalf("expected manually updated access token, got %q", refreshed.token)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for refresh completion")
	}
	var persisted model.SiteAccount
	if err := db.GetDB().WithContext(ctx).First(&persisted, account.ID).Error; err != nil {
		t.Fatal(err)
	}
	if persisted.AccessToken != "manual-access" || persisted.RefreshToken != "rotated-refresh" {
		t.Fatalf("manual credentials were overwritten by stale refresh response: %+v", persisted)
	}
	if persisted.TokenExpiresAt != 1 || account.TokenExpiresAt != 1 {
		t.Fatal("manual access token was assigned the discarded access token's expiry")
	}
	if account.AccessToken != "manual-access" || account.RefreshToken != "rotated-refresh" {
		t.Fatalf("in-memory credentials did not preserve the manual access token and rotated refresh token: %+v", account)
	}
	if refreshCalls != 1 {
		t.Fatalf("expected exactly one upstream refresh request, got %d", refreshCalls)
	}
}

func TestSub2APIRefreshReportsNonZeroCodeMessage(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/auth/refresh" {
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write([]byte(`{"code":401,"message":"refresh token expired","data":null}`))
	}))
	defer server.Close()

	_, err := refreshSub2APIManagedSession(context.Background(), &model.Site{BaseURL: server.URL}, &model.SiteAccount{
		CredentialType: model.SiteCredentialTypeAccessToken,
		RefreshToken:   "expired-refresh",
	})
	if err == nil || !strings.Contains(strings.ToLower(err.Error()), "refresh token expired") {
		t.Fatalf("expected non-zero refresh code reason, got %v", err)
	}
}

func TestSyncSub2APIReportsRefreshFailure(t *testing.T) {
	accessRequests := 0
	refreshCalls := 0
	requests := make([]string, 0, 3)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		requests = append(requests, r.URL.Path)
		if r.URL.Path == "/api/v1/auth/refresh" {
			refreshCalls++
			if r.Header.Get("Authorization") != "" {
				t.Errorf("fallback refresh must not use the old access token")
			}
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"message":"refresh token expired"}`))
			return
		}
		accessRequests++
		if r.Header.Get("Authorization") != "Bearer old-access" {
			t.Errorf("initial sync must use the existing access token, got %q", r.Header.Get("Authorization"))
		}
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"message":"access token expired"}`))
	}))
	defer server.Close()
	_, err := syncSub2API(context.Background(), &model.Site{BaseURL: server.URL}, &model.SiteAccount{CredentialType: model.SiteCredentialTypeAccessToken, AccessToken: "old-access", RefreshToken: "old-refresh"})
	if err == nil || !strings.Contains(err.Error(), "refresh token expired") || !strings.Contains(err.Error(), "access token expired") {
		t.Fatalf("expected both original and refresh errors, got %v", err)
	}
	if len(requests) == 0 || requests[0] == "/api/v1/auth/refresh" {
		t.Fatalf("expected the initial sync request before fallback refresh, requests=%v", requests)
	}
	if accessRequests == 0 || refreshCalls != 1 {
		t.Fatalf("expected initial 401 path and one fallback refresh, access=%d refresh=%d requests=%v", accessRequests, refreshCalls, requests)
	}
}

func TestSyncSub2APIUsesManagedKeyAndAPIModelEndpoint(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		switch r.URL.Path {
		case "/api/v1/keys":
			if r.Header.Get("Authorization") != "Bearer sub2-session-token" || r.Header.Get(sub2APIUserUIRequestHeader) != "1" {
				w.WriteHeader(http.StatusUnauthorized)
				_, _ = w.Write([]byte(`{"message":"unauthorized"}`))
				return
			}
			_, _ = w.Write([]byte(`{"code":0,"data":{"items":[{"id":11,"name":"managed-key","key":"sub2-user-key","group_id":7,"group_name":"VIP 7","enabled":true}]}}`))
		case "/api/v1/groups/available":
			if r.Header.Get("Authorization") != "Bearer sub2-session-token" || r.Header.Get(sub2APIUserUIRequestHeader) != "1" {
				w.WriteHeader(http.StatusNotFound)
				_, _ = w.Write([]byte(`404 page not found`))
				return
			}
			_, _ = w.Write([]byte(`{"code":0,"data":{"groups":[{"id":7,"name":"vip"},{"id":8,"name":"trial"}]}}`))
		case "/api/v1/groups", "/api/v1/group":
			_, _ = w.Write([]byte(`{"code":0,"data":[]}`))
		case "/v1/models":
			http.NotFound(w, r)
		case "/api/v1/models":
			if r.Header.Get("Authorization") != "Bearer sub2-user-key" || r.Header.Get(sub2APIUserUIRequestHeader) != "" {
				w.WriteHeader(http.StatusUnauthorized)
				_, _ = w.Write([]byte(`{"error":"unauthorized"}`))
				return
			}
			_, _ = w.Write([]byte(`{"code":0,"data":{"items":[{"id":"gpt-4o-mini"},{"name":"claude-3-5-sonnet"}]}}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	snapshot, err := syncSub2API(context.Background(), &model.Site{
		BaseURL:  server.URL,
		Platform: model.SitePlatformSub2API,
	}, &model.SiteAccount{
		CredentialType: model.SiteCredentialTypeAccessToken,
		AccessToken:    "Bearer sub2-session-token",
	})
	if err != nil {
		t.Fatalf("syncSub2API returned error: %v", err)
	}
	if len(snapshot.tokens) != 1 {
		t.Fatalf("expected one managed token, got %+v", snapshot.tokens)
	}
	if snapshot.tokens[0].Token != "sub2-user-key" || snapshot.tokens[0].GroupKey != "7" {
		t.Fatalf("expected managed token with group 7, got %+v", snapshot.tokens[0])
	}
	if len(snapshot.groups) != 2 || snapshot.groups[0].GroupKey != "7" || snapshot.groups[0].Name != "vip" || snapshot.groups[1].GroupKey != "8" || snapshot.groups[1].Name != "trial" {
		t.Fatalf("expected parsed groups 7/vip and 8/trial, got %+v", snapshot.groups)
	}
	if len(snapshot.models) != 2 {
		t.Fatalf("expected models discovered from /api/v1/models, got %+v", snapshot.models)
	}
	if !snapshot.groupDiscoveryState.authoritative || !snapshot.groupDiscoveryState.complete || snapshot.preserveHistoricalGroups {
		t.Fatalf("expected authoritative complete group discovery, got state=%+v preserve=%v", snapshot.groupDiscoveryState, snapshot.preserveHistoricalGroups)
	}
	if snapshot.status != model.SiteExecutionStatusPartial {
		t.Fatalf("expected missing-key group to produce partial status, got %q", snapshot.status)
	}
}

func TestFetchSub2APIGroupsUnionsCandidateEndpoints(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Header.Get(sub2APIUserUIRequestHeader) != "1" {
			http.NotFound(w, r)
			return
		}
		switch r.URL.Path {
		case "/api/v1/groups/available":
			http.NotFound(w, r)
		case "/api/v1/groups":
			_, _ = w.Write([]byte(`{"code":0,"data":{"items":[{"id":1,"name":"default"},{"id":2,"name":"vip"}],"pages":1}}`))
		case "/api/v1/group":
			http.NotFound(w, r)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	groups, discoveryState, err := fetchSub2APIGroups(context.Background(), &model.Site{BaseURL: server.URL}, &model.SiteAccount{}, "session", nil)
	if err != nil {
		t.Fatalf("fetch groups failed: %v", err)
	}
	if discoveryState.authoritative {
		t.Fatalf("expected legacy fallback groups to be non-authoritative")
	}
	if len(groups) != 2 || groups[0].GroupKey != "1" || groups[1].GroupKey != "2" {
		t.Fatalf("expected union of legacy candidate group endpoints, got %+v", groups)
	}
}

func TestFetchSub2APIGroupsUsesFengwindProductChannels(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Header.Get(sub2APIUserUIRequestHeader) != "1" {
			http.NotFound(w, r)
			return
		}
		switch r.URL.Path {
		case "/api/v1/groups/available":
			http.NotFound(w, r)
		case "/api/v1/channels/products":
			_, _ = w.Write([]byte(`{"code":0,"data":[{"id":7,"name":"Grok Free","description":"free"},{"channel_id":8,"channel_name":"通用渠道"},{"id":9,"name":"Deepseek 官网反代"}]}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	groups, discoveryState, err := fetchSub2APIGroups(context.Background(), &model.Site{BaseURL: server.URL}, &model.SiteAccount{}, "session", nil)
	if err != nil {
		t.Fatalf("fetch groups failed: %v", err)
	}
	if !discoveryState.authoritative || !discoveryState.complete {
		t.Fatalf("expected authoritative Fengwind product discovery, got %+v", discoveryState)
	}
	if len(groups) != 3 || groups[0].GroupKey != "7" || groups[0].Name != "Grok Free" || groups[1].GroupKey != "8" || groups[1].Name != "通用渠道" || groups[2].GroupKey != "9" {
		t.Fatalf("unexpected Fengwind product groups: %+v", groups)
	}
}

func TestBuildSub2APITokensMapsFengwindChannelFields(t *testing.T) {
	tokens := buildSub2APITokensFromItems([]map[string]any{{
		"name":         "fengwind-key",
		"key":          "sk-fengwind",
		"channel_id":   float64(8),
		"channel_name": "通用渠道",
		"status":       "active",
	}})
	if len(tokens) != 1 || tokens[0].GroupKey != "8" || tokens[0].GroupName != "通用渠道" {
		t.Fatalf("expected Fengwind channel fields to map to group, got %+v", tokens)
	}
}

func TestRequestSub2APIUserJSONRetriesTransientEOF(t *testing.T) {
	attempts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		if attempts < 3 {
			hijacker, ok := w.(http.Hijacker)
			if !ok {
				t.Fatalf("response writer does not support hijacking")
			}
			conn, _, err := hijacker.Hijack()
			if err != nil {
				t.Fatalf("hijack failed: %v", err)
			}
			_ = conn.Close()
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"code":0,"data":[]}`))
	}))
	defer server.Close()

	if _, err := requestSub2APIUserJSON(context.Background(), &model.Site{BaseURL: server.URL}, &model.SiteAccount{}, "session", "/api/v1/keys"); err != nil {
		t.Fatalf("expected transient EOF retry to succeed, got %v", err)
	}
	if attempts != 3 {
		t.Fatalf("expected three attempts, got %d", attempts)
	}
}

func TestFetchSub2APIPagedTokensUsesMetadata(t *testing.T) {
	var pages []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path != "/api/v1/keys" {
			http.NotFound(w, r)
			return
		}
		pages = append(pages, r.URL.Query().Get("page"))
		if r.URL.Query().Get("page") == "1" {
			_, _ = w.Write([]byte(`{"code":0,"data":{"items":[{"id":1,"name":"one","key":"key-one","group_id":1}],"total":2,"page":1,"page_size":1,"pages":2}}`))
			return
		}
		_, _ = w.Write([]byte(`{"code":0,"data":{"items":[{"id":2,"name":"two","key":"key-two","group_id":2}],"total":2,"page":2,"page_size":1,"pages":2}}`))
	}))
	defer server.Close()

	fetchResult, err := fetchSub2APITokens(context.Background(), &model.Site{BaseURL: server.URL}, &model.SiteAccount{}, "session")
	if err != nil {
		t.Fatalf("fetch tokens failed: %v", err)
	}
	if !fetchResult.complete || len(fetchResult.tokens) != 2 {
		t.Fatalf("expected two complete paged tokens, got %+v", fetchResult)
	}
	if len(pages) != 2 || pages[0] != "1" || pages[1] != "2" {
		t.Fatalf("expected pages 1 and 2, got %+v", pages)
	}
}

func TestFetchSub2APIPagedTokensStopsOnRepeatedPage(t *testing.T) {
	var pages []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path != "/api/v1/keys" {
			http.NotFound(w, r)
			return
		}
		pages = append(pages, r.URL.Query().Get("page"))
		_, _ = w.Write([]byte(`{"code":0,"data":{"items":[{"id":11,"name":"key","key":"value","group_id":7}],"has_more":true}}`))
	}))
	defer server.Close()

	fetchResult, err := fetchSub2APITokens(context.Background(), &model.Site{BaseURL: server.URL}, &model.SiteAccount{}, "session")
	tokens := fetchResult.tokens
	if err != nil {
		t.Fatalf("fetch tokens failed: %v", err)
	}
	if len(tokens) != 1 {
		t.Fatalf("expected one deduplicated token, got %+v", tokens)
	}
	if fetchResult.complete {
		t.Fatalf("expected repeated-page detection to mark pagination incomplete")
	}
	if len(pages) != 2 || pages[0] != "1" || pages[1] != "2" {
		t.Fatalf("expected repeated-page detection after pages 1 and 2, got %+v", pages)
	}
}

func TestFetchSub2APIPagedGroupsUsesPageMetadata(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/v1/groups/available", "/api/v1/group":
			http.NotFound(w, r)
		case "/api/v1/groups":
			page := r.URL.Query().Get("page")
			if page == "1" {
				_, _ = w.Write([]byte(`{"code":0,"data":{"items":[{"id":1,"name":"one"}],"pages":2}}`))
				return
			}
			_, _ = w.Write([]byte(`{"code":0,"data":{"items":[{"id":2,"name":"two"}],"pages":2}}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	groups, discoveryState, err := fetchSub2APIGroups(context.Background(), &model.Site{BaseURL: server.URL}, &model.SiteAccount{}, "session", nil)
	if err != nil {
		t.Fatalf("fetch groups failed: %v", err)
	}
	if discoveryState.authoritative || !discoveryState.complete {
		t.Fatalf("expected complete non-authoritative fallback discovery, got %+v", discoveryState)
	}
	if len(groups) != 2 || groups[0].GroupKey != "1" || groups[1].GroupKey != "2" {
		t.Fatalf("expected two paged groups, got %+v", groups)
	}
}

func TestFetchSub2APIGroupsUsesAuthoritativeAvailableEndpoint(t *testing.T) {
	var requested []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		requested = append(requested, r.URL.Path)
		if r.URL.Path != "/api/v1/groups/available" {
			t.Fatalf("unexpected legacy group endpoint request: %s", r.URL.Path)
		}
		_, _ = w.Write([]byte(`{"code":0,"data":[{"id":1,"name":"default"},{"id":2,"name":"vip"}]}`))
	}))
	defer server.Close()

	groups, discoveryState, err := fetchSub2APIGroups(context.Background(), &model.Site{BaseURL: server.URL}, &model.SiteAccount{}, "session", nil)
	if err != nil {
		t.Fatalf("fetch groups failed: %v", err)
	}
	if !discoveryState.authoritative || !discoveryState.complete {
		t.Fatalf("expected authoritative complete discovery, got %+v", discoveryState)
	}
	if len(groups) != 2 || len(requested) != 1 || requested[0] != "/api/v1/groups/available" {
		t.Fatalf("expected only authoritative available endpoint, groups=%+v requested=%+v", groups, requested)
	}
}

func TestParseGroupItemsIgnoresPaginationMetadata(t *testing.T) {
	groups := parseGroupItemsFromAny(map[string]any{
		"default":    map[string]any{"name": "Default"},
		"pagination": map[string]any{"page": 1, "pages": 2},
		"meta":       map[string]any{"total": 1},
		"has_more":   true,
	})
	if len(groups) != 1 || groups[0].GroupKey != "default" {
		t.Fatalf("expected only the real group, got %+v", groups)
	}
}

func TestParseSub2APIProductGroupsSupportsProductsEnvelope(t *testing.T) {
	groups := parseSub2APIProductGroups(map[string]any{
		"data": map[string]any{
			"products": []any{
				map[string]any{"channel_id": 7, "channel_name": "Grok Free"},
				map[string]any{"id": 8, "name": "通用渠道"},
			},
		},
	})
	if len(groups) != 2 || groups[0].GroupKey != "7" || groups[1].GroupKey != "8" {
		t.Fatalf("expected products envelope groups, got %+v", groups)
	}
}

func TestSyncSub2APIRequiresRealAPIKeyWhenKeyListIsEmpty(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		switch r.URL.Path {
		case "/api/v1/keys", "/api/v1/api-keys":
			_, _ = w.Write([]byte(`{"code":0,"data":[]}`))
		case "/api/v1/groups/available", "/api/v1/groups", "/api/v1/group":
			_, _ = w.Write([]byte(`{"code":0,"data":[{"id":7,"name":"vip"}]}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	_, err := syncSub2API(context.Background(), &model.Site{
		BaseURL:  server.URL,
		Platform: model.SitePlatformSub2API,
	}, &model.SiteAccount{
		CredentialType: model.SiteCredentialTypeAccessToken,
		AccessToken:    "sub2-session-token",
	})
	if err == nil {
		t.Fatalf("expected syncSub2API to require an API key")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "api key") {
		t.Fatalf("expected API key error, got %v", err)
	}
}

func TestFetchSub2APIGroupsTreatsUnknownSuccessEnvelopeAsIncomplete(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/v1/groups/available", "/api/v1/channels/products":
			_, _ = w.Write([]byte(`{"code":0,"data":{"unexpected":true}}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	_, state, err := fetchSub2APIGroups(context.Background(), &model.Site{BaseURL: server.URL}, &model.SiteAccount{}, "session", nil)
	if err == nil {
		t.Fatalf("expected unknown successful envelope to fail discovery")
	}
	if state.authoritative || state.complete {
		t.Fatalf("expected unknown envelope to remain non-authoritative, got %+v", state)
	}
}

func TestFetchSub2APIGroupsAcceptsExplicitEmptyList(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/api/v1/groups/available" {
			_, _ = w.Write([]byte(`{"code":0,"data":[]}`))
			return
		}
		http.NotFound(w, r)
	}))
	defer server.Close()

	groups, state, err := fetchSub2APIGroups(context.Background(), &model.Site{BaseURL: server.URL}, &model.SiteAccount{}, "session", nil)
	if err != nil {
		t.Fatalf("expected explicit empty list to be valid, got %v", err)
	}
	if !state.authoritative || !state.complete || len(groups) != 0 {
		t.Fatalf("expected authoritative explicit empty result, groups=%+v state=%+v", groups, state)
	}
}

func TestFetchSub2APITokensReturnsEnvelopeError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"code":401,"message":"token expired","data":null}`))
	}))
	defer server.Close()

	_, err := fetchSub2APITokens(context.Background(), &model.Site{BaseURL: server.URL}, &model.SiteAccount{}, "expired-token")
	if err == nil {
		t.Fatalf("expected envelope error")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "expired") {
		t.Fatalf("expected token expired error, got %v", err)
	}
}

func TestExplicitEmptyCollectionRequiresAllKnownCollectionsEmpty(t *testing.T) {
	if isExplicitEmptyCollection(map[string]any{
		"products": []any{},
		"channels": []any{"live"},
	}) {
		t.Fatal("mixed empty and non-empty collections must not be treated as empty")
	}
	if !isExplicitEmptyCollection(map[string]any{
		"products": []any{},
		"channels": []any{},
	}) {
		t.Fatal("all empty known collections should be treated as explicit empty")
	}
}

func TestFetchSub2APITokensRejectsUnknownSuccessEnvelope(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"code":0,"data":{"unexpected":true}}`))
	}))
	defer server.Close()

	_, err := fetchSub2APITokens(context.Background(), &model.Site{BaseURL: server.URL}, &model.SiteAccount{}, "session")
	if err == nil {
		t.Fatal("expected unknown token envelope to fail")
	}
}

func TestBuildSub2APIModelEndpointURLsIncludesAntigravityV1(t *testing.T) {
	endpoints := buildSub2APIModelEndpointURLs(&model.Site{BaseURL: "https://example.com"})
	for _, endpoint := range endpoints {
		if endpoint == "https://example.com/antigravity/v1/models" {
			return
		}
	}
	t.Fatalf("expected antigravity v1 models endpoint, got %+v", endpoints)
}

func TestParseSub2APIModelNamesReturnsEnvelopeError(t *testing.T) {
	_, err := parseSub2APIModelNames(map[string]any{
		"code":    float64(401),
		"message": "expired key",
	})
	if err == nil {
		t.Fatalf("expected envelope error")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "expired") {
		t.Fatalf("expected expired key error, got %v", err)
	}
}
