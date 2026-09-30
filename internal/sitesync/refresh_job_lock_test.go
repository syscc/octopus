package sitesync

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bestruirui/octopus/internal/db"
	"github.com/bestruirui/octopus/internal/model"
)

func TestRefreshDueSub2APISessionsLockedAccountDoesNotStarveOthers(t *testing.T) {
	ctx := setupProjectTestDB(t)
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		var body map[string]string
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body["refresh_token"] != "healthy-refresh" {
			t.Error("locked account sent a refresh request or healthy account used incorrect credentials")
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		_, _ = w.Write([]byte(`{"code":0,"data":{"access_token":"healthy-access","refresh_token":"healthy-rotated-refresh","expires_in":3600}}`))
	}))
	defer server.Close()
	site := model.Site{Name: "refresh-lock", Platform: model.SitePlatformSub2API, BaseURL: server.URL, Enabled: true}
	if err := db.GetDB().Create(&site).Error; err != nil {
		t.Fatal(err)
	}
	accounts := []model.SiteAccount{
		{SiteID: site.ID, Name: "locked", Enabled: true, CredentialType: model.SiteCredentialTypeAccessToken,
			AccessToken: "locked-access", RefreshToken: "locked-refresh", TokenExpiresAt: 1},
		{SiteID: site.ID, Name: "healthy", Enabled: true, CredentialType: model.SiteCredentialTypeAccessToken,
			AccessToken: "healthy-old-access", RefreshToken: "healthy-refresh", TokenExpiresAt: 1},
	}
	if err := db.GetDB().Create(&accounts).Error; err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		for _, account := range accounts {
			sub2APIRefreshRetryAfter.Delete(account.ID)
		}
	})
	value, _ := sub2APIRefreshLocks.LoadOrStore(accounts[0].ID, newSub2APIRefreshLock())
	lock := value.(*sub2APIRefreshLock)
	if err := lock.acquire(ctx); err != nil {
		t.Fatal(err)
	}
	defer lock.release()
	refreshed, err := refreshDueSub2APISessions(ctx, 100*time.Millisecond)
	if err != nil || refreshed != 1 || requests.Load() != 1 {
		t.Fatalf("locked account blocked later refreshes: refreshed=%d requests=%d err=%v", refreshed, requests.Load(), err)
	}
	if _, ok := sub2APIRefreshRetryAfter.Load(accounts[0].ID); !ok {
		t.Fatal("timed out lock wait did not receive a cooldown")
	}
	var healthy model.SiteAccount
	if err := db.GetDB().First(&healthy, accounts[1].ID).Error; err != nil {
		t.Fatal(err)
	}
	if healthy.AccessToken != "healthy-access" || healthy.RefreshToken != "healthy-rotated-refresh" {
		t.Fatal("healthy account credentials were not refreshed")
	}
}
