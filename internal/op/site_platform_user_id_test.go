package op

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/bestruirui/octopus/internal/db"
	"github.com/bestruirui/octopus/internal/model"
)

func TestSiteAccountPlatformUserIDTextImportAndUpdate(t *testing.T) {
	ctx := setupSiteOpTestDB(t)
	payload := map[string]any{
		"accounts": map[string]any{
			"accounts": []any{
				map[string]any{
					"id":        "text-account",
					"site_url":  "https://text-id.example.com",
					"site_type": "new-api",
					"site_name": "Text ID Site",
					"authType":  "access_token",
					"account_info": map[string]any{
						"id":           " X5MVNT ",
						"username":     "text-id-user",
						"access_token": "text-id-token",
					},
				},
			},
			"accountTokens": []any{},
			"tokenRoutes":   []any{},
			"routeChannels": []any{},
		},
	}

	result, _, err := SiteImportAllAPIHub(ctx, mustJSONMarshal(t, payload))
	if err != nil {
		t.Fatalf("SiteImportAllAPIHub failed: %v", err)
	}
	if result.CreatedAccounts != 1 {
		t.Fatalf("expected one imported account, got %d", result.CreatedAccounts)
	}

	var account model.SiteAccount
	if err := db.GetDB().Where("name = ?", "text-id-user").First(&account).Error; err != nil {
		t.Fatalf("query imported account failed: %v", err)
	}
	if account.PlatformUserID == nil || *account.PlatformUserID != "X5MVNT" {
		t.Fatalf("imported platform user id = %#v, want X5MVNT", account.PlatformUserID)
	}

	request := model.SiteAccountUpdateRequest{ID: account.ID, PlatformUserID: stringPointerForOp("new-text-id"), PlatformUserIDSet: true}
	updated, err := SiteAccountUpdate(&request, ctx)
	if err != nil {
		t.Fatalf("SiteAccountUpdate failed: %v", err)
	}
	if updated.PlatformUserID == nil || *updated.PlatformUserID != "new-text-id" {
		t.Fatalf("updated platform user id = %#v, want new-text-id", updated.PlatformUserID)
	}

	var omitted model.SiteAccountUpdateRequest
	if err := json.Unmarshal([]byte(`{"id":`+fmt.Sprint(account.ID)+`}`), &omitted); err != nil {
		t.Fatalf("decode omitted update failed: %v", err)
	}
	unchanged, err := SiteAccountUpdate(&omitted, ctx)
	if err != nil {
		t.Fatalf("SiteAccountUpdate omitted field failed: %v", err)
	}
	if unchanged.PlatformUserID == nil || *unchanged.PlatformUserID != "new-text-id" {
		t.Fatalf("omitted update changed platform user id to %#v", unchanged.PlatformUserID)
	}

	cleared, err := SiteAccountUpdate(&model.SiteAccountUpdateRequest{ID: account.ID, PlatformUserIDSet: true}, ctx)
	if err != nil {
		t.Fatalf("SiteAccountUpdate clear failed: %v", err)
	}
	if cleared.PlatformUserID != nil {
		t.Fatalf("expected platform user id to be cleared, got %#v", cleared.PlatformUserID)
	}
}

func TestSiteAccountPlatformUserIDNumericImportsRemainExact(t *testing.T) {
	ctx := setupSiteOpTestDB(t)
	payload := map[string]any{
		"version": "2.1",
		"type":    "accounts",
		"accounts": map[string]any{
			"sites": []any{
				map[string]any{"id": 1, "name": "Numeric ID Site", "url": "https://numeric-id.example.com", "platform": "new-api"},
			},
			"accounts": []any{
				map[string]any{
					"id":          10,
					"siteId":      1,
					"username":    "numeric-id-user",
					"accessToken": "numeric-id-token",
					"status":      "active",
					"extraConfig": `{"platformUserId":9007199254740993}`,
				},
			},
			"accountTokens": []any{},
			"tokenRoutes":   []any{},
			"routeChannels": []any{},
		},
	}

	if _, err := SiteImportMetAPI(ctx, mustJSONMarshal(t, payload)); err != nil {
		t.Fatalf("SiteImportMetAPI failed: %v", err)
	}

	var account model.SiteAccount
	if err := db.GetDB().Where("name = ?", "numeric-id-user").First(&account).Error; err != nil {
		t.Fatalf("query imported account failed: %v", err)
	}
	if account.PlatformUserID == nil || *account.PlatformUserID != "9007199254740993" {
		t.Fatalf("imported platform user id = %#v, want exact integer text", account.PlatformUserID)
	}
}

func TestPlatformUserIDImportJSONUsesNumber(t *testing.T) {
	var payload rawImportObject
	if err := decodeImportJSON([]byte(`{"id":9007199254740993}`), &payload); err != nil {
		t.Fatalf("decodeImportJSON failed: %v", err)
	}
	value := asPlatformUserIDPointer(payload["id"])
	if value == nil || *value != "9007199254740993" {
		t.Fatalf("decoded platform user id = %#v, want exact integer text", value)
	}
}

func stringPointerForOp(value string) *string { return &value }
