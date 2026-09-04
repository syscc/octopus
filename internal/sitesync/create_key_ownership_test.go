package sitesync

import (
	"strings"
	"testing"

	dbpkg "github.com/bestruirui/octopus/internal/db"
	"github.com/bestruirui/octopus/internal/model"
	"github.com/bestruirui/octopus/internal/op"
)

func TestCreateAccountTokenRejectsCrossSiteAccount(t *testing.T) {
	ctx := setupProjectTestDB(t)
	siteA := &model.Site{Name: "key-owner-a", Platform: model.SitePlatformNewAPI, BaseURL: "https://key-owner-a.example.com", Enabled: true}
	siteB := &model.Site{Name: "key-owner-b", Platform: model.SitePlatformNewAPI, BaseURL: "https://key-owner-b.example.com", Enabled: true}
	if err := op.SiteCreate(siteA, ctx); err != nil {
		t.Fatalf("create site A failed: %v", err)
	}
	if err := op.SiteCreate(siteB, ctx); err != nil {
		t.Fatalf("create site B failed: %v", err)
	}
	accountB := &model.SiteAccount{
		SiteID:         siteB.ID,
		Name:           "key-account-b",
		CredentialType: model.SiteCredentialTypeAccessToken,
		AccessToken:    "test-session-token",
		Enabled:        true,
	}
	if err := op.SiteAccountCreate(accountB, ctx); err != nil {
		t.Fatalf("create account B failed: %v", err)
	}

	result, err := CreateAccountToken(ctx, siteA.ID, accountB.ID, model.SiteChannelKeyCreateRequest{GroupKey: model.SiteDefaultGroupKey, Name: "must-not-create"})
	if err == nil || !strings.Contains(err.Error(), "site account not found") {
		t.Fatalf("expected cross-site account rejection, got result=%+v err=%v", result, err)
	}
	var tokenCount int64
	if err := dbpkg.GetDB().WithContext(ctx).Model(&model.SiteToken{}).Where("site_account_id = ?", accountB.ID).Count(&tokenCount).Error; err != nil {
		t.Fatalf("count site tokens failed: %v", err)
	}
	if tokenCount != 0 {
		t.Fatalf("expected no token to be created for cross-site request, got %d", tokenCount)
	}
}
