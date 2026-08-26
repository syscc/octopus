package sitesync

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	dbpkg "github.com/bestruirui/octopus/internal/db"
	"github.com/bestruirui/octopus/internal/model"
	"github.com/bestruirui/octopus/internal/op"
)

func createCheckinSchedulerFixture(t *testing.T, ctx context.Context, handler http.Handler) (*model.Site, *httptest.Server) {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)

	site := &model.Site{
		Name:     "checkin-scheduler-site",
		Platform: model.SitePlatformOneAPI,
		BaseURL:  server.URL,
		Enabled:  true,
	}
	if err := op.SiteCreate(site, ctx); err != nil {
		t.Fatalf("create site failed: %v", err)
	}
	return site, server
}

func createCheckinSchedulerAccount(t *testing.T, ctx context.Context, siteID int, name string, random bool, pending *time.Time) *model.SiteAccount {
	t.Helper()
	account := &model.SiteAccount{
		SiteID:                     siteID,
		Name:                       name,
		CredentialType:             model.SiteCredentialTypeAccessToken,
		AccessToken:                "checkin-test-token",
		Enabled:                    true,
		AutoSync:                   false,
		AutoCheckin:                true,
		RandomCheckin:              random,
		CheckinIntervalHours:       24,
		CheckinRandomWindowMinutes: 60,
		NextAutoCheckinAt:          pending,
	}
	if err := op.SiteAccountCreate(account, ctx); err != nil {
		t.Fatalf("create account %q failed: %v", name, err)
	}
	return account
}

func reloadCheckinSchedulerAccount(t *testing.T, ctx context.Context, accountID int) *model.SiteAccount {
	t.Helper()
	account, err := op.SiteAccountGet(accountID, ctx)
	if err != nil {
		t.Fatalf("reload account failed: %v", err)
	}
	return account
}

func TestScheduledCheckinArmsRandomAccountsWithoutExecutingThem(t *testing.T) {
	ctx := setupProjectTestDB(t)
	var calls atomic.Int32
	var unexpectedPath atomic.Bool
	site, _ := createCheckinSchedulerFixture(t, ctx, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/user/checkin" {
			unexpectedPath.Store(true)
		}
		calls.Add(1)
		_, _ = w.Write([]byte(`{"success":true,"message":"ok"}`))
	}))
	normal := createCheckinSchedulerAccount(t, ctx, site.ID, "normal", false, nil)
	random := createCheckinSchedulerAccount(t, ctx, site.ID, "random", true, nil)

	summary := CheckinAllWithOptions(ctx, SiteBatchOptions{Trigger: SiteBatchTriggerScheduled})
	if summary.Success != 1 || summary.Skipped != 1 {
		t.Fatalf("expected one normal execution and one armed random account, got %+v", summary)
	}
	if calls.Load() != 1 || unexpectedPath.Load() {
		t.Fatalf("expected only normal account to call the check-in endpoint, calls=%d unexpected_path=%v", calls.Load(), unexpectedPath.Load())
	}
	if reloaded := reloadCheckinSchedulerAccount(t, ctx, normal.ID); reloaded.LastCheckinStatus != model.SiteExecutionStatusSuccess {
		t.Fatalf("expected normal account success, got %q", reloaded.LastCheckinStatus)
	}
	if reloaded := reloadCheckinSchedulerAccount(t, ctx, random.ID); reloaded.NextAutoCheckinAt == nil {
		t.Fatal("expected scheduled task to persist a random-account due time")
	}
}

func TestManualCheckinBypassesRandomPendingDelay(t *testing.T) {
	ctx := setupProjectTestDB(t)
	var calls atomic.Int32
	site, _ := createCheckinSchedulerFixture(t, ctx, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		_, _ = w.Write([]byte(`{"success":true,"message":"ok"}`))
	}))
	future := time.Now().Add(time.Hour)
	account := createCheckinSchedulerAccount(t, ctx, site.ID, "random", true, &future)

	summary := CheckinAllWithOptions(ctx, SiteBatchOptions{Trigger: SiteBatchTriggerManual})
	if summary.Success != 1 || calls.Load() != 1 {
		t.Fatalf("expected manual check-in to execute pending random account, summary=%+v calls=%d", summary, calls.Load())
	}
	if reloaded := reloadCheckinSchedulerAccount(t, ctx, account.ID); reloaded.NextAutoCheckinAt != nil {
		t.Fatalf("expected manual execution to consume pending random work, got %s", reloaded.NextAutoCheckinAt)
	}
}

func TestRandomDueFailureIsConsumedUntilNextGlobalTrigger(t *testing.T) {
	ctx := setupProjectTestDB(t)
	var calls atomic.Int32
	site, _ := createCheckinSchedulerFixture(t, ctx, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		_, _ = w.Write([]byte(`{"success":false,"message":"upstream rejected check-in"}`))
	}))
	past := time.Now().Add(-time.Minute)
	account := createCheckinSchedulerAccount(t, ctx, site.ID, "random", true, &past)

	first := CheckinRandomDue(ctx)
	if first.Failed != 1 || calls.Load() != 1 {
		t.Fatalf("expected one failed due execution, summary=%+v calls=%d", first, calls.Load())
	}
	if reloaded := reloadCheckinSchedulerAccount(t, ctx, account.ID); reloaded.NextAutoCheckinAt != nil {
		t.Fatalf("expected failure to consume pending work, got %s", reloaded.NextAutoCheckinAt)
	}

	second := CheckinRandomDue(ctx)
	if second.Total != 0 || calls.Load() != 1 {
		t.Fatalf("expected no immediate retry without a new global trigger, summary=%+v calls=%d", second, calls.Load())
	}
}

func TestRandomDueSkipsAccountsWithoutPersistedWork(t *testing.T) {
	ctx := setupProjectTestDB(t)
	var calls atomic.Int32
	site, _ := createCheckinSchedulerFixture(t, ctx, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	account := createCheckinSchedulerAccount(t, ctx, site.ID, "random", true, nil)

	if summary := CheckinRandomDue(ctx); summary.Total != 0 || calls.Load() != 0 {
		t.Fatalf("expected no due accounts or upstream calls, summary=%+v calls=%d", summary, calls.Load())
	}
	var persisted model.SiteAccount
	if err := dbpkg.GetDB().WithContext(ctx).First(&persisted, account.ID).Error; err != nil {
		t.Fatalf("reload persisted account failed: %v", err)
	}
	if persisted.NextAutoCheckinAt != nil {
		t.Fatalf("expected due scanner not to create pending work, got %s", persisted.NextAutoCheckinAt)
	}
}
