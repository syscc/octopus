package sitesync

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/bestruirui/octopus/internal/model"
)

func TestSyncManagementPlatformPreservesTextPlatformUserID(t *testing.T) {
	for _, userID := range []string{"X5MVNT", "x5mvnt", "00123"} {
		t.Run(userID, func(t *testing.T) {
			var mu sync.Mutex
			observed := make(map[string][]string)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				mu.Lock()
				observed[r.URL.Path] = append(observed[r.URL.Path], r.Header.Get("New-API-User"))
				mu.Unlock()

				switch r.URL.Path {
				case "/api/token/":
					if r.Method != http.MethodGet || r.Header.Get("Authorization") != "Bearer access-token" || r.Header.Get("New-API-User") != userID {
						writeManagedUnauthorized(w)
						return
					}
					writeJSON(w, `{"data":{"items":[{"name":"primary","key":"managed-key","group":"vip","status":1}]}}`)
				case "/api/user/self/groups":
					if r.Header.Get("Authorization") != "Bearer access-token" || r.Header.Get("New-API-User") != userID {
						writeManagedUnauthorized(w)
						return
					}
					writeJSON(w, `{"data":[{"id":"vip","name":"VIP"}]}`)
				case "/api/user/self":
					if r.Header.Get("Authorization") != "Bearer access-token" || r.Header.Get("New-API-User") != userID {
						writeManagedUnauthorized(w)
						return
					}
					writeJSON(w, `{"success":true,"data":{"id":"`+userID+`","quota":1000000,"used_quota":100000,"today_income":500000}}`)
				case "/models":
					if r.Header.Get("Authorization") != "Bearer sk-managed-key" {
						w.WriteHeader(http.StatusUnauthorized)
						writeJSON(w, `{"error":"unauthorized"}`)
						return
					}
					writeJSON(w, `{"data":[{"id":"gpt-4o-mini"}]}`)
				default:
					http.NotFound(w, r)
				}
			}))
			defer server.Close()

			storedID := userID
			account := &model.SiteAccount{
				Name:           "managed-account",
				CredentialType: model.SiteCredentialTypeAccessToken,
				AccessToken:    "access-token",
				PlatformUserID: &storedID,
				Enabled:        true,
				AutoSync:       true,
			}
			snapshot, err := syncManagementPlatform(context.Background(), &model.Site{
				Platform: model.SitePlatformNewAPI,
				BaseURL:  server.URL,
			}, account)
			if err != nil {
				t.Fatalf("syncManagementPlatform returned error: %v", err)
			}
			if len(snapshot.tokens) != 1 || snapshot.tokens[0].Token != "managed-key" || snapshot.tokens[0].GroupKey != "vip" {
				t.Fatalf("unexpected synced tokens: %+v", snapshot.tokens)
			}
			if len(snapshot.groups) != 1 || snapshot.groups[0].GroupKey != "vip" {
				t.Fatalf("unexpected synced groups: %+v", snapshot.groups)
			}
			if len(snapshot.models) != 1 || snapshot.models[0].ModelName != "gpt-4o-mini" {
				t.Fatalf("unexpected synced models: %+v", snapshot.models)
			}
			if snapshot.balance != 2 || snapshot.balanceUsed != 0.2 || snapshot.todayIncome != 1 {
				t.Fatalf("unexpected balance values: balance=%v used=%v income=%v", snapshot.balance, snapshot.balanceUsed, snapshot.todayIncome)
			}
			if account.PlatformUserID == nil || *account.PlatformUserID != userID {
				t.Fatalf("expected stored platform user id %q to remain unchanged, got %#v", userID, account.PlatformUserID)
			}
			for _, path := range []string{"/api/token/", "/api/user/self/groups", "/api/user/self"} {
				for _, observedID := range observed[path] {
					if observedID != userID {
						t.Errorf("%s used New-API-User=%q, want %q", path, observedID, userID)
					}
				}
			}
		})
	}
}

func TestSyncManagementPlatformWithoutPlatformUserIDDoesNotSendZero(t *testing.T) {
	var mu sync.Mutex
	var observed []struct {
		path string
		id   string
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		mu.Lock()
		observed = append(observed, struct {
			path string
			id   string
		}{path: r.URL.Path, id: r.Header.Get("New-API-User")})
		mu.Unlock()

		switch r.URL.Path {
		case "/api/token/":
			if r.Method != http.MethodGet || r.Header.Get("Authorization") != "Bearer access-token" {
				writeManagedUnauthorized(w)
				return
			}
			writeJSON(w, `{"data":{"items":[{"name":"primary","key":"managed-key","group":"default","status":1}]}}`)
		case "/api/user/self/groups":
			writeJSON(w, `{"data":[{"id":"default","name":"default"}]}`)
		case "/api/user/self":
			// Deliberately omit id: this upstream never requires New-API-User and
			// should not cause an empty account field to become a fabricated "0".
			writeJSON(w, `{"success":true,"data":{"quota":1000000,"used_quota":100000,"today_income":500000}}`)
		case "/models":
			if r.Header.Get("Authorization") != "Bearer sk-managed-key" {
				w.WriteHeader(http.StatusUnauthorized)
				writeJSON(w, `{"error":"unauthorized"}`)
				return
			}
			writeJSON(w, `{"data":[{"id":"gpt-4o-mini"}]}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	account := &model.SiteAccount{
		Name:           "managed-account-without-id",
		CredentialType: model.SiteCredentialTypeAccessToken,
		AccessToken:    "access-token",
		Enabled:        true,
		AutoSync:       true,
	}
	snapshot, err := syncManagementPlatform(context.Background(), &model.Site{
		Platform: model.SitePlatformNewAPI,
		BaseURL:  server.URL,
	}, account)
	if err != nil {
		t.Fatalf("syncManagementPlatform returned error: %v", err)
	}
	if snapshot == nil || len(snapshot.tokens) != 1 || len(snapshot.groups) != 1 || len(snapshot.models) != 1 {
		t.Fatalf("unexpected sync snapshot: %+v", snapshot)
	}
	if account.PlatformUserID != nil {
		t.Fatalf("expected platform user id to remain unset, got %#v", account.PlatformUserID)
	}
	for _, request := range observed {
		if request.id == "0" {
			t.Errorf("%s sent fabricated zero user id %q", request.path, request.id)
		}
	}
	if len(observed) == 0 || observed[0].id != "" {
		t.Fatalf("expected the initial management request to omit New-API-User, got %+v", observed)
	}
}

func TestSyncManagementPlatformDiscoversTextUserIDAndRetries(t *testing.T) {
	var tokenUserIDs []string
	var selfUserIDs []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/token/":
			tokenUserIDs = append(tokenUserIDs, r.Header.Get("New-API-User"))
			if r.Header.Get("New-API-User") == "" {
				writeManagedUnauthorized(w)
				return
			}
			if r.Header.Get("New-API-User") != "X5MVNT" {
				writeManagedUnauthorized(w)
				return
			}
			writeJSON(w, `{"data":{"items":[{"name":"primary","key":"managed-key","group":"vip","status":1}]}}`)
		case "/api/user/self":
			selfUserIDs = append(selfUserIDs, r.Header.Get("New-API-User"))
			if r.Header.Get("New-API-User") != "" && r.Header.Get("New-API-User") != "X5MVNT" {
				writeManagedUnauthorized(w)
				return
			}
			writeJSON(w, `{"success":true,"data":{"id":"X5MVNT","quota":1000000,"used_quota":100000,"today_income":500000}}`)
		case "/api/user/self/groups":
			if r.Header.Get("New-API-User") != "X5MVNT" {
				writeManagedUnauthorized(w)
				return
			}
			writeJSON(w, `{"data":[{"id":"vip","name":"VIP"}]}`)
		case "/models":
			if r.Header.Get("Authorization") != "Bearer sk-managed-key" {
				w.WriteHeader(http.StatusUnauthorized)
				writeJSON(w, `{"error":"unauthorized"}`)
				return
			}
			writeJSON(w, `{"data":[{"id":"gpt-4o-mini"}]}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	account := &model.SiteAccount{
		Name:           "managed-account-discovery",
		CredentialType: model.SiteCredentialTypeAccessToken,
		AccessToken:    "access-token",
		Enabled:        true,
		AutoSync:       true,
	}
	snapshot, err := syncManagementPlatform(context.Background(), &model.Site{
		Platform: model.SitePlatformNewAPI,
		BaseURL:  server.URL,
	}, account)
	if err != nil {
		t.Fatalf("syncManagementPlatform returned error: %v", err)
	}
	if len(snapshot.tokens) != 1 || len(snapshot.models) != 1 {
		t.Fatalf("unexpected sync snapshot: %+v", snapshot)
	}
	if len(tokenUserIDs) != 2 || tokenUserIDs[0] != "" || tokenUserIDs[1] != "X5MVNT" {
		t.Fatalf("expected token request to retry with discovered text id, got %v", tokenUserIDs)
	}
	if len(selfUserIDs) == 0 || selfUserIDs[0] != "" {
		t.Fatalf("expected discovery to call /api/user/self without an id, got %v", selfUserIDs)
	}
	if account.PlatformUserID == nil || *account.PlatformUserID != "X5MVNT" {
		t.Fatalf("expected discovered text id to be remembered, got %#v", account.PlatformUserID)
	}
}

func TestManagedCheckinAndCreateKeyUseTextPlatformUserID(t *testing.T) {
	const userID = "X5MVNT"
	var observed []struct {
		path   string
		method string
		id     string
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		observed = append(observed, struct {
			path   string
			method string
			id     string
		}{path: r.URL.Path, method: r.Method, id: r.Header.Get("New-API-User")})

		if r.Header.Get("Authorization") != "Bearer access-token" || r.Header.Get("New-API-User") != userID {
			writeManagedUnauthorized(w)
			return
		}
		switch {
		case r.URL.Path == "/api/user/checkin" && r.Method == http.MethodPost:
			writeJSON(w, `{"success":true,"message":"checked in","data":{"reward":"10"}}`)
		case r.URL.Path == "/api/token/" && r.Method == http.MethodPost:
			writeJSON(w, `{"success":true,"data":"sk-created-key"}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	storedID := userID
	account := &model.SiteAccount{
		Name:           "managed-account-actions",
		CredentialType: model.SiteCredentialTypeAccessToken,
		AccessToken:    "access-token",
		PlatformUserID: &storedID,
		Enabled:        true,
		AutoSync:       true,
	}
	site := &model.Site{Platform: model.SitePlatformNewAPI, BaseURL: server.URL}

	checkin, accessToken, err := checkinAccountState(context.Background(), site, account)
	if err != nil {
		t.Fatalf("checkinAccountState returned error: %v", err)
	}
	if accessToken != "access-token" || checkin == nil || checkin.Status != model.SiteExecutionStatusSuccess || checkin.Reward != "10" {
		t.Fatalf("unexpected checkin result: result=%+v token=%q", checkin, accessToken)
	}

	created, err := createManagementPlatformToken(context.Background(), site, account, "vip", "manual-name")
	if err != nil {
		t.Fatalf("createManagementPlatformToken returned error: %v", err)
	}
	if created == nil || created.Token != "sk-created-key" || created.GroupKey != "vip" {
		t.Fatalf("unexpected created token: %+v", created)
	}
	if len(observed) != 2 {
		t.Fatalf("expected one checkin and one create request, got %+v", observed)
	}
	for _, request := range observed {
		if request.id != userID {
			t.Errorf("%s %s used New-API-User=%q, want %q", request.method, request.path, request.id, userID)
		}
	}
}

func writeManagedUnauthorized(w http.ResponseWriter) {
	w.WriteHeader(http.StatusUnauthorized)
	writeJSON(w, `{"success":false,"message":"未提供 New-API-User"}`)
}

func writeJSON(w http.ResponseWriter, body string) {
	_, _ = w.Write([]byte(body))
}
