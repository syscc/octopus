package db

import (
	"strings"
	"testing"

	"github.com/bestruirui/octopus/internal/model"
	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
)

func TestSiteAccountPlatformUserIDAutoMigratePreservesLegacyIntegerAndAssociations(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	if err != nil {
		t.Fatalf("open sqlite failed: %v", err)
	}

	if err := db.Exec(`CREATE TABLE site_accounts (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		site_id INTEGER NOT NULL,
		name TEXT NOT NULL,
		credential_type TEXT NOT NULL,
		platform_user_id INTEGER
	)`).Error; err != nil {
		t.Fatalf("create legacy site_accounts failed: %v", err)
	}
	if err := db.AutoMigrate(&model.SiteToken{}); err != nil {
		t.Fatalf("create site_tokens failed: %v", err)
	}
	if err := db.Exec("INSERT INTO site_accounts (site_id, name, credential_type, platform_user_id) VALUES (?, ?, ?, ?)", 7, "legacy", "access_token", 9007199254740993).Error; err != nil {
		t.Fatalf("insert legacy account failed: %v", err)
	}
	if err := db.Exec("INSERT INTO site_tokens (site_account_id, name, token, group_key, group_name) VALUES (?, ?, ?, ?, ?)", 1, "legacy-token", "legacy-token-value", "default", "default").Error; err != nil {
		t.Fatalf("insert legacy token failed: %v", err)
	}

	if err := db.AutoMigrate(&model.SiteAccount{}, &model.SiteToken{}); err != nil {
		t.Fatalf("AutoMigrate failed: %v", err)
	}

	var account model.SiteAccount
	if err := db.First(&account, 1).Error; err != nil {
		t.Fatalf("reload migrated account failed: %v", err)
	}
	if account.PlatformUserID == nil || *account.PlatformUserID != "9007199254740993" {
		t.Fatalf("migrated platform user id = %#v, want exact legacy value", account.PlatformUserID)
	}
	columns, err := db.Migrator().ColumnTypes(&model.SiteAccount{})
	if err != nil {
		t.Fatalf("inspect migrated columns failed: %v", err)
	}
	for _, column := range columns {
		if column.Name() == "platform_user_id" && !strings.EqualFold(column.DatabaseTypeName(), "TEXT") {
			t.Fatalf("platform_user_id database type = %q, want TEXT", column.DatabaseTypeName())
		}
	}

	var tokenCount int64
	if err := db.Model(&model.SiteToken{}).Where("site_account_id = ?", account.ID).Count(&tokenCount).Error; err != nil {
		t.Fatalf("count migrated tokens failed: %v", err)
	}
	if tokenCount != 1 {
		t.Fatalf("migrated token count = %d, want 1", tokenCount)
	}
}
