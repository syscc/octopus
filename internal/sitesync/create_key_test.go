package sitesync

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/bestruirui/octopus/internal/model"
	"github.com/bestruirui/octopus/internal/op"
)

func TestCreateAccountTokenCreatesManagedKeyAndSyncsAccount(t *testing.T) {
	ctx := setupProjectTestDB(t)

	var createdBody map[string]any
	batchCalled := false

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		switch {
		case r.URL.Path == "/api/user/self":
			if r.Header.Get("Authorization") != "Bearer test-access-token" || r.Header.Get("New-API-User") != "11494" {
				w.WriteHeader(http.StatusUnauthorized)
				_, _ = w.Write([]byte(`{"success":false,"message":"无权进行此操作，未提供 New-Api-User"}`))
				return
			}
			_, _ = w.Write([]byte(`{"success":true,"data":{"id":11494,"username":"managed-user"}}`))
		case r.URL.Path == "/api/token/" && r.Method == http.MethodPost:
			if r.Header.Get("Authorization") != "Bearer test-access-token" || r.Header.Get("New-API-User") != "11494" {
				w.WriteHeader(http.StatusUnauthorized)
				_, _ = w.Write([]byte(`{"success":false,"message":"无权进行此操作，未提供 New-Api-User"}`))
				return
			}
			if err := json.NewDecoder(r.Body).Decode(&createdBody); err != nil {
				t.Fatalf("decode create token body failed: %v", err)
			}
			_, _ = w.Write([]byte(`{"success":true,"data":"sk-managed-created-key"}`))
		case r.URL.Path == "/api/token/" && r.Method == http.MethodGet:
			if r.Header.Get("Authorization") != "Bearer test-access-token" || r.Header.Get("New-API-User") != "11494" {
				w.WriteHeader(http.StatusUnauthorized)
				_, _ = w.Write([]byte(`{"success":false,"message":"无权进行此操作，未提供 New-Api-User"}`))
				return
			}
			_, _ = w.Write([]byte(`{"data":{"items":[]}}`))
		case r.URL.Path == "/api/token/batch/keys" && r.Method == http.MethodPost:
			batchCalled = true
			if r.Header.Get("Authorization") != "Bearer test-access-token" || r.Header.Get("New-API-User") != "11494" {
				w.WriteHeader(http.StatusUnauthorized)
				_, _ = w.Write([]byte(`{"success":false,"message":"unauthorized"}`))
				return
			}
			_, _ = w.Write([]byte(`{"success":true,"data":{"keys":{"1":"sk-managed-created-key"}}}`))
		case r.URL.Path == "/api/user/self/groups":
			if r.Header.Get("Authorization") != "Bearer test-access-token" || r.Header.Get("New-API-User") != "11494" {
				w.WriteHeader(http.StatusUnauthorized)
				_, _ = w.Write([]byte(`{"success":false,"message":"无权进行此操作，未提供 New-Api-User"}`))
				return
			}
			_, _ = w.Write([]byte(`{"data":[{"id":"vip","name":"VIP"}]}`))
		case r.URL.Path == "/models":
			if r.Header.Get("Authorization") != "Bearer sk-managed-created-key" {
				w.WriteHeader(http.StatusUnauthorized)
				_, _ = w.Write([]byte(`{"error":"unauthorized"}`))
				return
			}
			_, _ = w.Write([]byte(`{"data":[{"id":"gpt-4o-mini"}]}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	site := &model.Site{
		Name:     "managed-create-site",
		Platform: model.SitePlatformNewAPI,
		BaseURL:  server.URL,
		Enabled:  true,
	}
	if err := op.SiteCreate(site, ctx); err != nil {
		t.Fatalf("SiteCreate failed: %v", err)
	}

	account := &model.SiteAccount{
		SiteID:         site.ID,
		Name:           "managed-create-account",
		CredentialType: model.SiteCredentialTypeAccessToken,
		AccessToken:    "test-access-token",
		Enabled:        true,
		AutoSync:       true,
	}
	if err := op.SiteAccountCreate(account, ctx); err != nil {
		t.Fatalf("SiteAccountCreate failed: %v", err)
	}

	result, err := CreateAccountToken(ctx, site.ID, account.ID, model.SiteChannelKeyCreateRequest{GroupKey: "vip", Name: "managed-created-name"})
	if err != nil {
		t.Fatalf("CreateAccountToken returned error: %v", err)
	}
	if result == nil || result.TokenCount != 1 {
		t.Fatalf("unexpected sync result: %+v", result)
	}
	if batchCalled {
		t.Fatalf("did not expect batch endpoint when create response contains plaintext key")
	}
	if createdBody["group"] != "vip" {
		t.Fatalf("expected created group to be vip, got %#v", createdBody["group"])
	}
	if createdBody["unlimited_quota"] != true {
		t.Fatalf("expected unlimited_quota=true, got %#v", createdBody["unlimited_quota"])
	}
	createdName, _ := createdBody["name"].(string)
	if createdName != "managed-created-name" {
		t.Fatalf("expected provided token name to be used, got %q", createdName)
	}

	reloaded, err := op.SiteAccountGet(account.ID, ctx)
	if err != nil {
		t.Fatalf("SiteAccountGet failed: %v", err)
	}
	if len(reloaded.Tokens) != 1 || reloaded.Tokens[0].GroupKey != "vip" || reloaded.Tokens[0].Token != "sk-managed-created-key" {
		t.Fatalf("unexpected synced tokens: %+v", reloaded.Tokens)
	}
	if len(reloaded.UserGroups) != 1 || reloaded.UserGroups[0].GroupKey != "vip" {
		t.Fatalf("unexpected synced groups: %+v", reloaded.UserGroups)
	}
	if len(reloaded.Models) != 1 || reloaded.Models[0].GroupKey != "vip" || reloaded.Models[0].ModelName != "gpt-4o-mini" {
		t.Fatalf("unexpected synced models: %+v", reloaded.Models)
	}
	assertSiteChannelKeyProjected(t, ctx, site.ID, account.ID, "vip", "sk-managed-created-key")
}

func TestInitialAccessTokenSyncRestoresExistingKeyAndProjectsChannel(t *testing.T) {
	ctx := setupProjectTestDB(t)

	batchCalled := false
	platformUserID := 7788
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		switch {
		case r.URL.Path == "/api/token/" && r.Method == http.MethodGet:
			if r.Header.Get("Authorization") != "Bearer initial-access-token" || r.Header.Get("New-API-User") != "7788" {
				w.WriteHeader(http.StatusUnauthorized)
				_, _ = w.Write([]byte(`{"success":false,"message":"unauthorized"}`))
				return
			}
			_, _ = w.Write([]byte(`{"data":{"items":[{"id":41,"name":"existing-upstream-key","key":"sk-exis**********-key","group":"vip","status":1}]}}`))
		case r.URL.Path == "/api/token/batch/keys" && r.Method == http.MethodPost:
			batchCalled = true
			if r.Header.Get("Authorization") != "Bearer initial-access-token" || r.Header.Get("New-API-User") != "7788" {
				w.WriteHeader(http.StatusUnauthorized)
				_, _ = w.Write([]byte(`{"success":false,"message":"unauthorized"}`))
				return
			}
			_, _ = w.Write([]byte(`{"success":true,"data":{"keys":{"41":"sk-existing-upstream-key"}}}`))
		case r.URL.Path == "/api/user/self/groups":
			_, _ = w.Write([]byte(`{"data":[{"id":"vip","name":"VIP"}]}`))
		case r.URL.Path == "/api/user/self":
			_, _ = w.Write([]byte(`{"success":true,"data":{"id":7788,"quota":1000000,"used_quota":0}}`))
		case r.URL.Path == "/models":
			if r.Header.Get("Authorization") != "Bearer sk-existing-upstream-key" {
				w.WriteHeader(http.StatusUnauthorized)
				_, _ = w.Write([]byte(`{"error":"unauthorized"}`))
				return
			}
			_, _ = w.Write([]byte(`{"data":[{"id":"gpt-4o-mini"}]}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	site := &model.Site{
		Name:     "initial-access-token-site",
		Platform: model.SitePlatformNewAPI,
		BaseURL:  server.URL,
		Enabled:  true,
	}
	if err := op.SiteCreate(site, ctx); err != nil {
		t.Fatalf("SiteCreate failed: %v", err)
	}

	account := &model.SiteAccount{
		SiteID:         site.ID,
		Name:           "initial-access-token-account",
		CredentialType: model.SiteCredentialTypeAccessToken,
		AccessToken:    "initial-access-token",
		PlatformUserID: &platformUserID,
		Enabled:        true,
		AutoSync:       true,
	}
	if err := op.SiteAccountCreate(account, ctx); err != nil {
		t.Fatalf("SiteAccountCreate failed: %v", err)
	}

	result, err := SyncAccount(ctx, account.ID)
	if err != nil {
		t.Fatalf("initial SyncAccount returned error: %v", err)
	}
	if result == nil || result.TokenCount != 1 || result.ChannelCount != 1 {
		t.Fatalf("unexpected initial sync result: %+v", result)
	}
	if !batchCalled {
		t.Fatalf("expected initial sync to recover existing masked keys from the batch endpoint")
	}

	reloaded, err := op.SiteAccountGet(account.ID, ctx)
	if err != nil {
		t.Fatalf("SiteAccountGet failed: %v", err)
	}
	if len(reloaded.Tokens) != 1 || reloaded.Tokens[0].Token != "sk-existing-upstream-key" || reloaded.Tokens[0].ValueStatus != model.SiteTokenValueStatusReady {
		t.Fatalf("expected existing upstream key to be persisted as ready, got %+v", reloaded.Tokens)
	}
	assertSiteChannelKeyProjected(t, ctx, site.ID, account.ID, "vip", "sk-existing-upstream-key")
}

func TestCreateAccountTokenCreatesSub2APIKeyAndSyncsAccount(t *testing.T) {
	ctx := setupProjectTestDB(t)

	var createdBody map[string]any

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		switch {
		case r.URL.Path == "/api/v1/keys" && r.Method == http.MethodPost:
			if r.Header.Get("Authorization") != "Bearer sub2api-token" {
				w.WriteHeader(http.StatusUnauthorized)
				_, _ = w.Write([]byte(`{"success":false,"message":"unauthorized"}`))
				return
			}
			if err := json.NewDecoder(r.Body).Decode(&createdBody); err != nil {
				t.Fatalf("decode sub2api create body failed: %v", err)
			}
			_, _ = w.Write([]byte(`{"success":true,"data":{"id":31,"key":"sub2api-created-key"}}`))
		case r.URL.Path == "/api/v1/keys":
			if r.Header.Get("Authorization") != "Bearer sub2api-token" {
				w.WriteHeader(http.StatusUnauthorized)
				_, _ = w.Write([]byte(`{"success":false,"message":"unauthorized"}`))
				return
			}
			_, _ = w.Write([]byte(`{"data":[{"name":"manual-sub2api-name","key":"sub2**********-key","group_id":"7","group_name":"VIP 7","status":1}]}`))
		case r.URL.Path == "/models":
			if r.Header.Get("Authorization") != "Bearer sub2api-created-key" {
				w.WriteHeader(http.StatusUnauthorized)
				_, _ = w.Write([]byte(`{"error":"unauthorized"}`))
				return
			}
			_, _ = w.Write([]byte(`{"data":[{"id":"gpt-4o-mini"}]}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	site := &model.Site{
		Name:     "sub2api-create-site",
		Platform: model.SitePlatformSub2API,
		BaseURL:  server.URL,
		Enabled:  true,
	}
	if err := op.SiteCreate(site, ctx); err != nil {
		t.Fatalf("SiteCreate failed: %v", err)
	}

	account := &model.SiteAccount{
		SiteID:         site.ID,
		Name:           "sub2api-create-account",
		CredentialType: model.SiteCredentialTypeAccessToken,
		AccessToken:    "sub2api-token",
		Enabled:        true,
		AutoSync:       true,
	}
	if err := op.SiteAccountCreate(account, ctx); err != nil {
		t.Fatalf("SiteAccountCreate failed: %v", err)
	}

	result, err := CreateAccountToken(context.Background(), site.ID, account.ID, model.SiteChannelKeyCreateRequest{
		GroupKey: "7",
		Name:     "manual-sub2api-name",
	})
	if err != nil {
		t.Fatalf("CreateAccountToken returned error: %v", err)
	}
	if result == nil || result.TokenCount != 1 {
		t.Fatalf("unexpected sync result: %+v", result)
	}
	if createdBody["group_id"] != float64(7) && createdBody["group_id"] != 7 {
		t.Fatalf("expected group_id=7, got %#v", createdBody["group_id"])
	}
	if createdBody["name"] != "manual-sub2api-name" {
		t.Fatalf("expected provided token name to be used, got %#v", createdBody["name"])
	}

	reloaded, err := op.SiteAccountGet(account.ID, ctx)
	if err != nil {
		t.Fatalf("SiteAccountGet failed: %v", err)
	}
	if len(reloaded.Tokens) != 1 || reloaded.Tokens[0].GroupKey != "7" || reloaded.Tokens[0].Token != "sub2api-created-key" {
		t.Fatalf("unexpected synced tokens: %+v", reloaded.Tokens)
	}
	if len(reloaded.UserGroups) != 1 || reloaded.UserGroups[0].GroupKey != "7" {
		t.Fatalf("unexpected synced groups: %+v", reloaded.UserGroups)
	}
}

func TestSiteTokenCreateSucceededFromAnyRequiresExplicitPrimitiveTrue(t *testing.T) {
	for name, value := range map[string]any{
		"false":        false,
		"zero":         0,
		"empty string": "",
		"string true":  "true",
		"non-empty":    "ok",
	} {
		if siteTokenCreateSucceededFromAny(value) {
			t.Fatalf("expected %s primitive to be unsuccessful", name)
		}
	}
	if !siteTokenCreateSucceededFromAny(true) {
		t.Fatalf("expected boolean true primitive to be successful")
	}
}

func TestCreatedSiteTokenExtractionRejectsUntrustedStatusValues(t *testing.T) {
	tests := []struct {
		name    string
		payload any
		want    string
	}{
		{name: "top-level string", payload: "sk-top-level", want: ""},
		{name: "success only", payload: map[string]any{"success": true}, want: ""},
		{name: "message only", payload: map[string]any{"message": "sk-message"}, want: ""},
		{name: "data message", payload: map[string]any{"data": map[string]any{"message": "sk-message"}}, want: ""},
		{name: "data result", payload: map[string]any{"data": "result"}, want: ""},
		{name: "data unauthorized", payload: map[string]any{"data": "unauthorized"}, want: ""},
		{name: "masked data", payload: map[string]any{"data": "sk-cre**********-key"}, want: ""},
		{name: "empty data", payload: map[string]any{"data": " "}, want: ""},
		{name: "numeric key", payload: map[string]any{"data": map[string]any{"key": float64(123)}}, want: ""},
		{name: "numeric token", payload: map[string]any{"data": map[string]any{"token": 123}}, want: ""},
		{name: "numeric string token", payload: map[string]any{"data": "123456"}, want: "123456"},
		{name: "list scalar", payload: map[string]any{"items": []any{"sk-list-value"}}, want: ""},
		{name: "plaintext data", payload: map[string]any{"data": "sk-created-plain-key"}, want: "sk-created-plain-key"},
		{name: "plaintext data key", payload: map[string]any{"data": map[string]any{"key": "sk-created-plain-key"}}, want: "sk-created-plain-key"},
		{name: "token containing error text", payload: map[string]any{"data": "sk-error-marker-is-valid"}, want: "sk-error-marker-is-valid"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := extractSiteTokenValueFromPayload(test.payload); got != test.want {
				t.Fatalf("expected token %q, got %q", test.want, got)
			}
		})
	}
}

func TestCreatedSiteTokenResponseRejectsFailureMarkers(t *testing.T) {
	failurePayloads := map[string]any{
		"success false with token": map[string]any{"success": false, "data": "sk-created-plain-key"},
		"non-zero code":            map[string]any{"code": 401, "data": "sk-created-plain-key"},
		"non-numeric code":         map[string]any{"code": "AUTH_FAILED", "data": "sk-created-plain-key"},
		"non-numeric error code":   map[string]any{"error_code": "TOKEN_DENIED", "data": "sk-created-plain-key"},
		"non-empty errors":         map[string]any{"errors": []any{"denied"}, "data": "sk-created-plain-key"},
		"conflicting success":      map[string]any{"success": true, "code": "AUTH_FAILED", "data": "sk-created-plain-key"},
		"failed status":            map[string]any{"status": "failed", "data": "sk-created-plain-key"},
		"unauthorized data":        map[string]any{"code": 0, "data": "unauthorized"},
	}
	for name, payload := range failurePayloads {
		t.Run(name, func(t *testing.T) {
			if siteTokenCreateSucceededFromAny(payload) {
				t.Fatalf("expected failure response to be rejected: %#v", payload)
			}
			if created := createdSiteTokenFromPayload(payload, "vip", "created-name"); created != nil {
				t.Fatalf("expected no token from failure response, got %+v", created)
			}
		})
	}

	for _, payload := range []map[string]any{
		{
			"code": 0,
			"data": map[string]any{"id": 31, "key": "sk-created-plain-key"},
		},
		{
			"code":   "0",
			"errors": []any{},
			"data":   "sk-created-plain-key",
		},
	} {
		if !siteTokenCreateSucceededFromAny(payload) {
			t.Fatalf("expected successful code envelope to remain valid: %#v", payload)
		}
	}
	if created := createdSiteTokenFromPayload(map[string]any{
		"status": "success",
		"data":   "sk-created-plain-key",
	}, "vip", "created-name"); created == nil {
		t.Fatal("expected success status with plaintext data to remain valid")
	}
	if created := createdSiteTokenFromPayload(map[string]any{
		"success": true,
		"message": "completed without error",
		"data":    "sk-created-plain-key",
	}, "vip", "created-name"); created == nil {
		t.Fatal("expected explicit success to take precedence over informational message text")
	}
}

func TestCreatedSiteTokenFromPayloadRequiresUnmaskedKey(t *testing.T) {
	created := createdSiteTokenFromPayload(map[string]any{
		"success": true,
		"data":    map[string]any{"key": "sk-created-plain-key"},
	}, "vip", "created-name")
	if created == nil {
		t.Fatalf("expected plaintext key to be extracted")
	}
	if created.Token != "sk-created-plain-key" || created.GroupKey != "vip" || created.Name != "created-name" {
		t.Fatalf("unexpected created token: %+v", created)
	}

	if masked := createdSiteTokenFromPayload(map[string]any{
		"success": true,
		"data":    map[string]any{"key": "sk-cre**********-key"},
	}, "vip", "created-name"); masked != nil {
		t.Fatalf("expected masked creation response to be ignored, got %+v", masked)
	}
}

func assertSiteChannelKeyProjected(t *testing.T, ctx context.Context, siteID int, accountID int, groupKey string, token string) {
	t.Helper()

	account, err := op.SiteChannelAccountGet(siteID, accountID, ctx)
	if err != nil {
		t.Fatalf("SiteChannelAccountGet failed: %v", err)
	}

	var target *model.SiteChannelGroup
	for index := range account.Groups {
		if account.Groups[index].GroupKey == groupKey {
			target = &account.Groups[index]
			break
		}
	}
	if target == nil {
		t.Fatalf("expected site channel group %q, got %+v", groupKey, account.Groups)
	}
	if !target.HasProjectedChannel {
		t.Fatalf("expected group %q to have a projected channel", groupKey)
	}

	sourceFound := false
	for _, key := range target.SourceKeys {
		if key.Token == token && key.ValueStatus == model.SiteTokenValueStatusReady && key.Enabled {
			sourceFound = true
			break
		}
	}
	if !sourceFound {
		t.Fatalf("expected ready source key %q in group %q, got %+v", token, groupKey, target.SourceKeys)
	}

	projectedFound := false
	for _, key := range target.ProjectedKeys {
		if key.ChannelKey == token && key.Enabled {
			projectedFound = true
			break
		}
	}
	if !projectedFound {
		t.Fatalf("expected projected channel key %q in group %q, got %+v", token, groupKey, target.ProjectedKeys)
	}
}
