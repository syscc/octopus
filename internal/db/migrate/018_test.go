package migrate

import "testing"

func TestMigrateSiteUserGroupChannelDisabled(t *testing.T) {
	db := openMigrationTestDB(t)
	if err := db.Exec(`CREATE TABLE site_user_groups (
		id INTEGER PRIMARY KEY,
		site_account_id INTEGER NOT NULL,
		group_key TEXT NOT NULL,
		name TEXT
	)`).Error; err != nil {
		t.Fatalf("seed migration test db failed: %v", err)
	}
	if err := db.Exec("INSERT INTO site_user_groups (id, site_account_id, group_key, name) VALUES (1, 2, 'default', 'default')").Error; err != nil {
		t.Fatalf("seed site user group failed: %v", err)
	}

	if err := migrateSiteUserGroupChannelDisabled(db); err != nil {
		t.Fatalf("migrateSiteUserGroupChannelDisabled returned error: %v", err)
	}
	if !db.Migrator().HasColumn("site_user_groups", "channel_disabled") {
		t.Fatalf("expected channel_disabled column after migration")
	}

	var disabled bool
	if err := db.Table("site_user_groups").Select("channel_disabled").Where("id = ?", 1).Scan(&disabled).Error; err != nil {
		t.Fatalf("query migrated row failed: %v", err)
	}
	if disabled {
		t.Fatalf("expected existing groups to default to enabled channels")
	}
}
