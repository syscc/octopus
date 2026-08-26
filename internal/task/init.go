package task

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/bestruirui/octopus/internal/model"
	"github.com/bestruirui/octopus/internal/op"
	"github.com/bestruirui/octopus/internal/price"
	"github.com/bestruirui/octopus/internal/utils/log"
)

const (
	TaskPriceUpdate       = "price_update"
	TaskStatsSave         = "stats_save"
	TaskRelayLogSave      = "relay_log_save"
	TaskSyncLLM           = "sync_llm"
	TaskCleanLLM          = "clean_llm"
	TaskBaseUrlDelay      = "base_url_delay"
	TaskSiteSync          = "site_sync"
	TaskSiteCheckin       = "site_checkin"
	TaskSiteCheckinRandom = "site_checkin_random_due"
	TaskWSAffinityCleanup = "ws_affinity_cleanup"
	TaskWebDAVBackup      = "webdav_backup"
)

// ModelInfoUpdateTask refreshes the model pricing metadata.
func ModelInfoUpdateTask() {
	if err := price.UpdateLLMPrice(context.Background()); err != nil {
		log.Warnf("failed to update price info: %v", err)
	}
}

func buildSiteCheckinSchedule(mode string, intervalValue string, cronValue string) (Schedule, error) {
	mode = strings.ToLower(strings.TrimSpace(mode))
	if mode == "cron" {
		schedule, err := model.ParseSiteCheckinCron(cronValue)
		if err != nil {
			return nil, fmt.Errorf("invalid site check-in cron: %w", err)
		}
		return schedule, nil
	}
	if mode != "" && mode != "interval" {
		return nil, fmt.Errorf("invalid site check-in schedule mode %q", mode)
	}

	hours, err := strconv.Atoi(strings.TrimSpace(intervalValue))
	if err != nil || hours <= 0 || hours > 720 {
		return nil, fmt.Errorf("site check-in interval must be between 1 and 720 hours")
	}
	return NewIntervalSchedule(time.Duration(hours) * time.Hour), nil
}

func siteCheckinScheduleValues() (mode string, intervalValue string, cronValue string, err error) {
	mode, err = op.SettingGetString(model.SettingKeySiteCheckinScheduleMode)
	if err != nil {
		return "", "", "", err
	}
	intervalValue, err = op.SettingGetString(model.SettingKeySiteCheckinInterval)
	if err != nil {
		return "", "", "", err
	}
	cronValue, err = op.SettingGetString(model.SettingKeySiteCheckinCron)
	if err != nil {
		return "", "", "", err
	}
	return mode, intervalValue, cronValue, nil
}

func siteCheckinScheduleFromSettings() (Schedule, error) {
	mode, intervalValue, cronValue, err := siteCheckinScheduleValues()
	if err != nil {
		return nil, err
	}
	return buildSiteCheckinSchedule(mode, intervalValue, cronValue)
}

// ValidateSiteCheckinScheduleChange validates a setting change against the
// companion settings before it is persisted.
func ValidateSiteCheckinScheduleChange(key model.SettingKey, value string) error {
	mode, intervalValue, cronValue, err := siteCheckinScheduleValues()
	if err != nil {
		return err
	}
	switch key {
	case model.SettingKeySiteCheckinScheduleMode:
		mode = value
	case model.SettingKeySiteCheckinInterval:
		intervalValue = value
	case model.SettingKeySiteCheckinCron:
		cronValue = value
	default:
		return nil
	}
	_, err = buildSiteCheckinSchedule(mode, intervalValue, cronValue)
	return err
}

// ConfigureSiteCheckinSchedule reloads the persisted schedule and updates the
// running task without requiring a process restart.
func ConfigureSiteCheckinSchedule() error {
	schedule, err := siteCheckinScheduleFromSettings()
	if err != nil {
		return err
	}
	mode, err := op.SettingGetString(model.SettingKeySiteCheckinScheduleMode)
	if err != nil {
		return err
	}
	runOnStart := strings.ToLower(strings.TrimSpace(mode)) != "cron"
	taskName := string(model.SettingKeySiteCheckinInterval)
	ConfigureSchedule(taskName, schedule, runOnStart, SiteCheckinTask)
	return nil
}

func Init() {
	priceUpdateIntervalHours, err := op.SettingGetInt(model.SettingKeyModelInfoUpdateInterval)
	if err != nil {
		log.Errorf("failed to get model info update interval: %v", err)
		return
	}
	priceUpdateInterval := time.Duration(priceUpdateIntervalHours) * time.Hour
	// 注册价格更新任务
	Register(string(model.SettingKeyModelInfoUpdateInterval), priceUpdateInterval, true, ModelInfoUpdateTask)

	// 注册基础URL延迟任务
	Register(TaskBaseUrlDelay, 24*time.Hour, true, ChannelBaseUrlDelayTask)

	// 注册LLM同步任务
	syncLLMIntervalHours, err := op.SettingGetInt(model.SettingKeySyncLLMInterval)
	if err != nil {
		log.Warnf("failed to get sync LLM interval: %v", err)
		return
	}
	syncLLMInterval := time.Duration(syncLLMIntervalHours) * time.Hour
	Register(string(model.SettingKeySyncLLMInterval), syncLLMInterval, true, SyncModelsTask)

	siteSyncIntervalHours, err := op.SettingGetInt(model.SettingKeySiteSyncInterval)
	if err != nil {
		log.Warnf("failed to get site sync interval: %v", err)
		return
	}
	siteSyncInterval := time.Duration(siteSyncIntervalHours) * time.Hour
	Register(string(model.SettingKeySiteSyncInterval), siteSyncInterval, true, SiteSyncTask)

	siteCheckinSchedule, err := siteCheckinScheduleFromSettings()
	if err != nil {
		log.Warnf("failed to get site check-in schedule: %v; falling back to 24 hours", err)
		siteCheckinSchedule = NewIntervalSchedule(24 * time.Hour)
	}
	checkinScheduleMode, modeErr := op.SettingGetString(model.SettingKeySiteCheckinScheduleMode)
	if modeErr != nil {
		log.Warnf("failed to get site check-in schedule mode: %v; using interval", modeErr)
	}
	RegisterSchedule(
		string(model.SettingKeySiteCheckinInterval),
		siteCheckinSchedule,
		strings.ToLower(strings.TrimSpace(checkinScheduleMode)) != "cron",
		SiteCheckinTask,
	)
	Register(TaskSiteCheckinRandom, time.Minute, true, SiteRandomCheckinTask)

	// 注册统计保存任务
	statsSaveIntervalMinutes, err := op.SettingGetInt(model.SettingKeyStatsSaveInterval)
	if err != nil {
		log.Warnf("failed to get stats save interval: %v", err)
		return
	}
	statsSaveInterval := time.Duration(statsSaveIntervalMinutes) * time.Minute
	Register(TaskStatsSave, statsSaveInterval, false, op.StatsSaveDBTask)
	// 注册中继日志保存任务
	Register(TaskRelayLogSave, time.Hour, false, func() {
		if err := op.RelayLogSaveDBTask(context.Background()); err != nil {
			log.Warnf("relay log save db task failed: %v", err)
		}
	})

	Register(TaskWSAffinityCleanup, 10*time.Minute, false, func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		deleted, err := op.WSResponseAffinityCleanup(ctx, time.Now())
		if err != nil {
			log.Warnf("ws response affinity cleanup failed: %v", err)
			return
		}
		if deleted > 0 {
			log.Debugf("ws response affinity cleanup removed %d expired rows", deleted)
		}
	})

	// 注册被动离群退役(POR)任务（默认间隔 2 分钟，总开关在任务内判断）
	outlierIntervalMinutes, err := op.SettingGetInt(model.SettingKeyOutlierRetireInterval)
	if err != nil || outlierIntervalMinutes <= 0 {
		outlierIntervalMinutes = 2
	}
	Register(string(model.SettingKeyOutlierRetireInterval), time.Duration(outlierIntervalMinutes)*time.Minute, false, SiteOutlierRetireTask)

	// 注册 WebDAV 自动备份任务（间隔为 0 时不运行）
	webdavIntervalHours, err := op.SettingGetInt(model.SettingKeyWebDAVBackupInterval)
	if err != nil {
		log.Warnf("failed to get webdav backup interval: %v", err)
	} else if webdavIntervalHours > 0 {
		webdavInterval := time.Duration(webdavIntervalHours) * time.Hour
		Register(string(model.SettingKeyWebDAVBackupInterval), webdavInterval, false, WebDAVBackupTask)
	}
}
