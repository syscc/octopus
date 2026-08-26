package op

import (
	"slices"
	"testing"

	"github.com/bestruirui/octopus/internal/model"
)

func TestGroupUpdateAffectedChannelIDsResetsAllMembersOnWeightChange(t *testing.T) {
	group := model.Group{Items: []model.GroupItem{
		{ID: 11, ChannelID: 101, Weight: 1},
		{ID: 12, ChannelID: 102, Weight: 1},
	}}
	req := &model.GroupUpdateRequest{ItemsToUpdate: []model.GroupItemUpdateRequest{
		{ID: 11, Priority: 1, Weight: 100},
	}}

	ids := groupUpdateAffectedChannelIDs(group, req)
	slices.Sort(ids)
	if !slices.Equal(ids, []int{101, 102}) {
		t.Fatalf("expected every existing channel to reset after weight change, got %v", ids)
	}
}

func TestGroupUpdateAffectedChannelIDsIncludesNewAndExistingMembers(t *testing.T) {
	group := model.Group{Items: []model.GroupItem{{ID: 11, ChannelID: 101}}}
	req := &model.GroupUpdateRequest{ItemsToAdd: []model.GroupItemAddRequest{{ChannelID: 103, ModelName: "model"}}}

	ids := groupUpdateAffectedChannelIDs(group, req)
	slices.Sort(ids)
	if !slices.Equal(ids, []int{101, 103}) {
		t.Fatalf("expected existing and new channels to reset, got %v", ids)
	}
}

func TestGroupCreateRejectsDisabledChannel(t *testing.T) {
	ctx := setupSiteOpTestDB(t)
	channel := &model.Channel{Name: "disabled-group-create-channel", Enabled: true, Model: "model"}
	if err := ChannelCreate(channel, ctx); err != nil {
		t.Fatalf("ChannelCreate failed: %v", err)
	}
	if err := ChannelEnabled(channel.ID, false, ctx); err != nil {
		t.Fatalf("ChannelEnabled failed: %v", err)
	}
	group := &model.Group{
		Name:  "disabled-group-create",
		Mode:  model.GroupModeFailover,
		Items: []model.GroupItem{{ChannelID: channel.ID, ModelName: "model", Priority: 1, Weight: 1}},
	}
	if err := GroupCreate(group, ctx); err == nil {
		t.Fatal("expected disabled channel to be rejected")
	}
}
