package sitesync

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bestruirui/octopus/internal/db"
	"github.com/bestruirui/octopus/internal/model"
)

func TestSub2APIRefreshPreservesConcurrentCredentialEdits(t *testing.T) {
	for _, name := range []string{"access", "access_and_expiry", "expiry", "all_credentials", "refresh"} {
		t.Run(name, func(t *testing.T) {
			ctx := setupProjectTestDB(t)
			originalExpiry := time.Now().Add(time.Minute).UnixMilli()
			manualExpiry := time.Now().Add(2 * time.Minute).UnixMilli()
			updates := map[string]any{}
			wantAccess, wantRefresh, wantExpiry := "old-access", "rotated-refresh", originalExpiry
			switch name {
			case "access", "access_and_expiry", "all_credentials":
				updates["access_token"] = "manual-access"
				wantAccess = "manual-access"
			}
			switch name {
			case "access_and_expiry", "expiry", "all_credentials":
				updates["token_expires_at"] = manualExpiry
				wantExpiry = manualExpiry
			}
			if name == "all_credentials" || name == "refresh" {
				updates["refresh_token"] = "manual-refresh"
				wantRefresh = "manual-refresh"
			}

			var calls atomic.Int32
			account := &model.SiteAccount{}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/api/v1/auth/refresh" || r.Method != http.MethodPost {
					t.Errorf("unexpected refresh request: %s %s", r.Method, r.URL.Path)
					http.NotFound(w, r)
					return
				}
				if r.Header.Get("Authorization") != "" {
					t.Error("refresh request contained an access token")
				}
				var body map[string]string
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					t.Error(err)
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				if calls.Add(1) == 1 {
					if body["refresh_token"] != "old-refresh" {
						t.Error("first refresh used unexpected credentials")
					}
					// The upstream has consumed the refresh token while a user edit commits.
					if err := db.GetDB().Model(&model.SiteAccount{}).Where("id = ?", account.ID).Updates(updates).Error; err != nil {
						t.Error(err)
						w.WriteHeader(http.StatusInternalServerError)
						return
					}
					_, _ = w.Write([]byte(`{"code":0,"data":{"access_token":"rotated-access","refresh_token":"rotated-refresh","expires_in":3600}}`))
					return
				}
				if body["refresh_token"] != wantRefresh {
					t.Error("next refresh reused consumed or overwritten credentials")
					w.WriteHeader(http.StatusUnauthorized)
					return
				}
				_, _ = w.Write([]byte(`{"code":0,"data":{"access_token":"next-access","refresh_token":"next-refresh","expires_in":3600}}`))
			}))
			defer server.Close()

			site := &model.Site{Name: "concurrent-credentials", Platform: model.SitePlatformSub2API, BaseURL: server.URL}
			if err := db.GetDB().Create(site).Error; err != nil {
				t.Fatal(err)
			}
			*account = model.SiteAccount{SiteID: site.ID, Name: name, CredentialType: model.SiteCredentialTypeAccessToken,
				AccessToken: "old-access", RefreshToken: "old-refresh", TokenExpiresAt: originalExpiry}
			if err := db.GetDB().Create(account).Error; err != nil {
				t.Fatal(err)
			}
			token, err := ensureFreshSub2APIAccessToken(ctx, site, account, true)
			if err != nil || token != wantAccess {
				t.Fatalf("refresh did not preserve current access token: %v", err)
			}
			var persisted model.SiteAccount
			if err := db.GetDB().First(&persisted, account.ID).Error; err != nil {
				t.Fatal(err)
			}
			for _, current := range []*model.SiteAccount{account, &persisted} {
				if current.AccessToken != wantAccess || current.RefreshToken != wantRefresh || current.TokenExpiresAt != wantExpiry {
					t.Fatal("refresh lost concurrent edits or failed to persist the rotated refresh token")
				}
			}
			if !shouldProactivelyRefreshSub2API(&persisted) {
				t.Fatal("preserved access token was assigned the wrong expiry")
			}
			token, err = ensureFreshSub2APIAccessToken(ctx, site, account, true)
			if err != nil || token != "next-access" || calls.Load() != 2 {
				t.Fatalf("subsequent refresh failed: %v (requests=%d)", err, calls.Load())
			}
		})
	}
}

func TestSub2APIRefreshLockHonorsContext(t *testing.T) {
	lock := newSub2APIRefreshLock()
	if err := lock.acquire(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := lock.acquire(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("blocked refresh lock ignored the deadline: %v", err)
	}
	lock.release()
	ctx, cancelNext := context.WithTimeout(context.Background(), time.Second)
	defer cancelNext()
	if err := lock.acquire(ctx); err != nil {
		t.Fatalf("canceled waiter leaked the refresh permit: %v", err)
	}
	lock.release()
}
