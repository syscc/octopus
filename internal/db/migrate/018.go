package migrate

import (
	"fmt"

	"github.com/bestruirui/octopus/internal/model"
	"gorm.io/gorm"
)

func init() {
	RegisterAfterAutoMigration(Migration{
		Version: 18,
		Up:      migrateSiteUserGroupChannelDisabled,
	})
}

func migrateSiteUserGroupChannelDisabled(db *gorm.DB) error {
	if db == nil {
		return fmt.Errorf("db is nil")
	}
	if !db.Migrator().HasTable(&model.SiteUserGroup{}) {
		return nil
	}
	if !db.Migrator().HasColumn(&model.SiteUserGroup{}, "channel_disabled") {
		if err := db.Migrator().AddColumn(&model.SiteUserGroup{}, "ChannelDisabled"); err != nil {
			return err
		}
	}
	return db.Model(&model.SiteUserGroup{}).
		Where("channel_disabled IS NULL").
		Update("channel_disabled", false).Error
}
