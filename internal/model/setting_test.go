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
