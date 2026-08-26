package sitesync

import (
	"context"
	"testing"
	"time"

	dbpkg "github.com/bestruirui/octopus/internal/db"
	"github.com/bestruirui/octopus/internal/model"
	"github.com/bestruirui/octopus/internal/op"
)

func TestBuildRandomCheckinDueAtAnchorsDelayToBaseline(t *testing.T) {
	baseline := time.Date(2026, 8, 24, 8, 0, 0, 0, time.UTC)
	lastSuccess := baseline.Add(-48 * time.Hour)
	account := &model.SiteAccount{
		Enabled:                    true,
		AutoCheckin:                true,
		RandomCheckin:              true,
		CheckinIntervalHours:       24,
		CheckinRandomWindowMinutes: 120,
		LastCheckinAt:              &lastSuccess,
		LastCheckinStatus:          model.SiteExecutionStatusSuccess,
	}

	due := buildRandomCheckinDueAt(account, baseline)
	if due == nil {
		t.Fatal("expected a random check-in due time")
	}
	latest := baseline.Add(120 * time.Minute)
	if due.Before(baseline) || due.After(latest) {
		t.Fatalf("expected due time between %s and %s, got %s", baseline, latest, due)
	}
}

func TestBuildRandomCheckinDueAtRespectsMinimumInterval(t *testing.T) {
	baseline := time.Date(2026, 8, 24, 8, 0, 0, 0, time.UTC)
	lastSuccess := baseline.Add(-2 * time.Hour)
	account := &model.SiteAccount{
		Enabled:                    true,
		AutoCheckin:                true,
		RandomCheckin:              true,
		CheckinIntervalHours:       24,
		CheckinRandomWindowMinutes: 0,
		LastCheckinAt:              &lastSuccess,
		LastCheckinStatus:          model.SiteExecutionStatusSuccess,
	}

	due := buildRandomCheckinDueAt(account, baseline)
	want := lastSuccess.Add(24 * time.Hour)
	if due == nil || !due.Equal(want) {
		t.Fatalf("expected due time %s, got %v", want, due)
	}
}

func TestBuildRandomCheckinDueAtReturnsNilWhenDisabled(t *testing.T) {
	baseline := time.Now()
	cases := []*model.SiteAccount{
		{Enabled: false, AutoCheckin: true, RandomCheckin: true},
		{Enabled: true, AutoCheckin: false, RandomCheckin: true},
		{Enabled: true, AutoCheckin: true, RandomCheckin: false},
	}
	for index, account := range cases {
		if due := buildRandomCheckinDueAt(account, baseline); due != nil {
			t.Fatalf("case %d: expected nil due time, got %s", index, due)
		}
	}
}

func TestDueRandomCheckinAccountsOnlyReturnsPersistedDueItems(t *testing.T) {
	now := time.Date(2026, 8, 24, 8, 0, 0, 0, time.UTC)
	past := now.Add(-time.Minute)
	future := now.Add(time.Minute)
	items := []siteBatchAccount{
		{account: &model.SiteAccount{RandomCheckin: true, NextAutoCheckinAt: &past}},
		{account: &model.SiteAccount{RandomCheckin: true, NextAutoCheckinAt: &future}},
		{account: &model.SiteAccount{RandomCheckin: true}},
		{account: &model.SiteAccount{RandomCheckin: false, NextAutoCheckinAt: &past}},
	}

	due := dueRandomCheckinAccounts(items, now)
	if len(due) != 1 || due[0].account.NextAutoCheckinAt == nil || !due[0].account.NextAutoCheckinAt.Equal(past) {
		t.Fatalf("expected only persisted past item, got %+v", due)
	}
}

func TestCheckinAccountStateConsumesPendingRandomExecution(t *testing.T) {
	ctx := setupProjectTestDB(t)
	site := &model.Site{
		Name:     "random-checkin-state-site",
		Platform: model.SitePlatformNewAPI,
		BaseURL:  "https://example.com",
		Enabled:  true,
	}
	if err := op.SiteCreate(site, ctx); err != nil {
		t.Fatalf("create site failed: %v", err)
	}
	due := time.Now().Add(-time.Minute)
	account := &model.SiteAccount{
		SiteID:            site.ID,
		Name:              "random-checkin-state",
		CredentialType:    model.SiteCredentialTypeAccessToken,
		AccessToken:       "token",
		Enabled:           true,
		AutoCheckin:       true,
		RandomCheckin:     true,
		NextAutoCheckinAt: &due,
	}
	if err := dbpkg.GetDB().WithContext(ctx).Create(account).Error; err != nil {
		t.Fatalf("create account failed: %v", err)
	}

	if err := updateAccountCheckinState(ctx, account, model.SiteExecutionStatusFailed, "temporary failure", ""); err != nil {
		t.Fatalf("update failed: %v", err)
	}
	reloaded := &model.SiteAccount{}
	if err := dbpkg.GetDB().WithContext(context.Background()).First(reloaded, account.ID).Error; err != nil {
		t.Fatalf("reload account failed: %v", err)
	}
	if reloaded.NextAutoCheckinAt != nil {
		t.Fatalf("expected failed attempt to consume pending execution, got %s", reloaded.NextAutoCheckinAt)
	}
}

func TestRefreshAccountRandomCheckinScheduleClearsDisabledAccount(t *testing.T) {
	ctx := setupProjectTestDB(t)
	site := &model.Site{
		Name:     "random-refresh-site",
		Platform: model.SitePlatformNewAPI,
		BaseURL:  "https://example.com",
		Enabled:  true,
	}
	if err := op.SiteCreate(site, ctx); err != nil {
		t.Fatalf("create site failed: %v", err)
	}
	pending := time.Now().Add(time.Hour)
	account := &model.SiteAccount{
		SiteID:            site.ID,
		Name:              "random-refresh-account",
		CredentialType:    model.SiteCredentialTypeAccessToken,
		AccessToken:       "token",
		Enabled:           true,
		AutoCheckin:       false,
		AutoCheckinSet:    true,
		RandomCheckin:     true,
		NextAutoCheckinAt: &pending,
	}
	if err := op.SiteAccountCreate(account, ctx); err != nil {
		t.Fatalf("create account failed: %v", err)
	}

	if err := RefreshAccountRandomCheckinSchedule(ctx, account.ID); err != nil {
		t.Fatalf("refresh schedule failed: %v", err)
	}
	reloaded := &model.SiteAccount{}
	if err := dbpkg.GetDB().WithContext(ctx).First(reloaded, account.ID).Error; err != nil {
		t.Fatalf("reload account failed: %v", err)
	}
	if reloaded.NextAutoCheckinAt != nil {
		t.Fatalf("expected disabled random scheduling to clear pending work, got %s", reloaded.NextAutoCheckinAt)
	}
}

func TestRefreshAccountRandomCheckinSchedulePreservesEnabledPendingWork(t *testing.T) {
	ctx := setupProjectTestDB(t)
	site := &model.Site{
		Name:     "random-refresh-enabled-site",
		Platform: model.SitePlatformNewAPI,
		BaseURL:  "https://example.com",
		Enabled:  true,
	}
	if err := op.SiteCreate(site, ctx); err != nil {
		t.Fatalf("create site failed: %v", err)
	}
	pending := time.Now().Add(time.Hour).Round(time.Second)
	account := &model.SiteAccount{
		SiteID:            site.ID,
		Name:              "random-refresh-enabled-account",
		CredentialType:    model.SiteCredentialTypeAccessToken,
		AccessToken:       "token",
		Enabled:           true,
		AutoCheckin:       true,
		RandomCheckin:     true,
		NextAutoCheckinAt: &pending,
	}
	if err := op.SiteAccountCreate(account, ctx); err != nil {
		t.Fatalf("create account failed: %v", err)
	}

	if err := RefreshAccountRandomCheckinSchedule(ctx, account.ID); err != nil {
		t.Fatalf("refresh schedule failed: %v", err)
	}
	reloaded := &model.SiteAccount{}
	if err := dbpkg.GetDB().WithContext(ctx).First(reloaded, account.ID).Error; err != nil {
		t.Fatalf("reload account failed: %v", err)
	}
	if reloaded.NextAutoCheckinAt == nil || !reloaded.NextAutoCheckinAt.Equal(pending) {
		t.Fatalf("expected pending work to be preserved, got %v", reloaded.NextAutoCheckinAt)
	}
}
