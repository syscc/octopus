package model

import "testing"

func TestStatsSaveIntervalValidation(t *testing.T) {
	for _, value := range []string{"0", "10"} {
		setting := Setting{Key: SettingKeyStatsSaveInterval, Value: value}
		if err := setting.Validate(); err != nil {
			t.Fatalf("expected stats interval %q to be valid: %v", value, err)
		}
	}

	for _, value := range []string{"-1", "invalid"} {
		setting := Setting{Key: SettingKeyStatsSaveInterval, Value: value}
		if err := setting.Validate(); err == nil {
			t.Fatalf("expected stats interval %q to be rejected", value)
		}
	}
}

func TestSiteCheckinScheduleSettingsValidation(t *testing.T) {
	valid := []Setting{
		{Key: SettingKeySiteCheckinInterval, Value: "24"},
		{Key: SettingKeySiteCheckinScheduleMode, Value: "interval"},
		{Key: SettingKeySiteCheckinScheduleMode, Value: "cron"},
		{Key: SettingKeySiteCheckinCron, Value: "*/15 * * * *"},
	}
	for _, setting := range valid {
		if err := setting.Validate(); err != nil {
			t.Fatalf("expected setting %s=%q to be valid: %v", setting.Key, setting.Value, err)
		}
	}

	invalid := []Setting{
		{Key: SettingKeySiteCheckinInterval, Value: "0"},
		{Key: SettingKeySiteCheckinInterval, Value: "721"},
		{Key: SettingKeySiteCheckinScheduleMode, Value: "timer"},
		{Key: SettingKeySiteCheckinCron, Value: "not a cron"},
	}
	for _, setting := range invalid {
		if err := setting.Validate(); err == nil {
			t.Fatalf("expected setting %s=%q to be rejected", setting.Key, setting.Value)
		}
	}
}
