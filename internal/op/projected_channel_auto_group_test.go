package op

import (
	"testing"

	"github.com/bestruirui/octopus/internal/model"
)

func TestChannelAutoGroupWithModeSkipsDisabledChannel(t *testing.T) {
	ctx := setupSiteOpTestDB(t)
	channel := &model.Channel{
		Name:        "disabled-auto-group-channel",
		Enabled:     true,
		Model:       "disabled-auto-group-model",
		CustomModel: "",
	}
	if err := ChannelCreate(channel, ctx); err != nil {
		t.Fatalf("ChannelCreate failed: %v", err)
	}
	if err := ChannelEnabled(channel.ID, false, ctx); err != nil {
		t.Fatalf("ChannelEnabled failed: %v", err)
	}
	channel.Enabled = false
	group := &model.Group{Name: "disabled-auto-group-model", Mode: model.GroupModeFailover}
	if err := GroupCreate(group, ctx); err != nil {
		t.Fatalf("GroupCreate failed: %v", err)
	}

	ChannelAutoGroupWithMode(channel, model.AutoGroupTypeExact, ctx)

	items, err := GroupItemList(group.ID, ctx)
	if err != nil {
		t.Fatalf("GroupItemList failed: %v", err)
	}
	if len(items) != 0 {
		t.Fatalf("expected disabled channel not to be auto-grouped, got %+v", items)
	}
}
