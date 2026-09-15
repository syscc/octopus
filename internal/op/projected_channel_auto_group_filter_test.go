package op

import (
	"context"
	"reflect"
	"sort"
	"testing"

	"github.com/bestruirui/octopus/internal/model"
)

func setupAutoGroupFilterTest(t *testing.T) context.Context {
	t.Helper()
	ctx := setupSiteOpTestDB(t)
	// These package caches outlive the temporary test database.
	groupCache.Clear()
	groupMap.Clear()
	channelCache.Clear()
	oldValue, hadValue := settingCache.Get(model.SettingKeyGlobalAutoGroupModelFilter)
	t.Cleanup(func() {
		groupCache.Clear()
		groupMap.Clear()
		channelCache.Clear()
		if hadValue {
			settingCache.Set(model.SettingKeyGlobalAutoGroupModelFilter, oldValue)
		} else {
			settingCache.Del(model.SettingKeyGlobalAutoGroupModelFilter)
		}
	})
	if err := settingRefreshCache(ctx); err != nil {
		t.Fatal(err)
	}
	return ctx
}

func TestChannelAutoGroupGlobalFilter(t *testing.T) {
	for _, tt := range []struct {
		name, channel, filter string
		want                  []string
	}{
		{"off", "只会喵喵叫/default", `{"mode":"off","keywords":["只会喵喵叫"]}`, []string{"gpt-a", "gpt-b"}},
		{"channel blacklist", "只会喵喵叫/default", `{"mode":"blacklist","keywords":["只会喵喵叫"]}`, []string{}},
		{"model whitelist", "normal", `{"mode":"whitelist","keywords":["missing"," GPT-B ","gpt-b"]}`, []string{"gpt-b"}},
		{"model blacklist", "normal", `{"mode":"blacklist","keywords":["gpt-b"]}`, []string{"gpt-a"}},
		{"empty whitelist", "normal", `{"mode":"whitelist","keywords":[" "]}`, []string{}},
		{"empty blacklist", "normal", `{"mode":"blacklist","keywords":[" "]}`, []string{"gpt-a", "gpt-b"}},
		{"invalid persisted value", "normal", `{"mode":"invalid","keywords":[]}`, []string{}},
	} {
		for _, mode := range []model.AutoGroupType{model.AutoGroupTypeExact, model.AutoGroupTypeFuzzy, model.AutoGroupTypeRegex} {
			t.Run(tt.name+"/"+string(rune('0'+mode)), func(t *testing.T) {
				ctx := setupAutoGroupFilterTest(t)
				// Direct storage deliberately models a corrupt legacy value as well.
				if err := SettingSetString(model.SettingKeyGlobalAutoGroupModelFilter, tt.filter); err != nil {
					t.Fatal(err)
				}
				channel := &model.Channel{Name: tt.channel, Enabled: true, Model: "gpt-a", CustomModel: "gpt-b,gpt-a"}
				if err := ChannelCreate(channel, ctx); err != nil {
					t.Fatal(err)
				}
				for _, name := range []string{"gpt-a", "gpt-b"} {
					group := &model.Group{Name: name, Mode: model.GroupModeFailover, MatchRegex: "^" + name + "$"}
					if err := GroupCreate(group, ctx); err != nil {
						t.Fatal(err)
					}
				}
				ChannelAutoGroupWithMode(channel, mode, ctx)
				got := []string{}
				groups, err := GroupList(ctx)
				if err != nil {
					t.Fatal(err)
				}
				for _, group := range groups {
					items, err := GroupItemList(group.ID, ctx)
					if err != nil {
						t.Fatal(err)
					}
					for _, item := range items {
						got = append(got, item.ModelName)
					}
				}
				sort.Strings(got)
				if !reflect.DeepEqual(got, tt.want) {
					t.Fatalf("members = %v, want %v", got, tt.want)
				}
			})
		}
	}
}

func TestGlobalAutoGroupFilterDoesNotChangeManualMembersOrSave(t *testing.T) {
	ctx := setupAutoGroupFilterTest(t)
	channel := &model.Channel{Name: "blocked", Enabled: true, Model: "existing,manual,saved,new"}
	if err := ChannelCreate(channel, ctx); err != nil {
		t.Fatal(err)
	}
	group := &model.Group{Name: "all", Mode: model.GroupModeFailover, MatchRegex: ".*"}
	if err := GroupCreate(group, ctx); err != nil {
		t.Fatal(err)
	}
	if err := GroupItemBatchAdd(group.ID, []model.GroupIDAndLLMName{{ChannelID: channel.ID, ModelName: "existing"}}, ctx); err != nil {
		t.Fatal(err)
	}
	if err := SettingSetString(model.SettingKeyGlobalAutoGroupModelFilter, `{"mode":"blacklist","keywords":["blocked"]}`); err != nil {
		t.Fatal(err)
	}
	ChannelAutoGroupWithMode(channel, model.AutoGroupTypeRegex, ctx)
	if err := GroupItemAdd(&model.GroupItem{GroupID: group.ID, ChannelID: channel.ID, ModelName: "manual"}, ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := GroupUpdate(&model.GroupUpdateRequest{ID: group.ID, ItemsToAdd: []model.GroupItemAddRequest{{ChannelID: channel.ID, ModelName: "saved"}}}, ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := GroupUpdate(&model.GroupUpdateRequest{ID: group.ID}, ctx); err != nil {
		t.Fatal(err)
	}
	items, err := GroupItemList(group.ID, ctx)
	if err != nil {
		t.Fatal(err)
	}
	got := []string{}
	for _, item := range items {
		got = append(got, item.ModelName)
	}
	sort.Strings(got)
	if !reflect.DeepEqual(got, []string{"existing", "manual", "saved"}) {
		t.Fatalf("members = %v", got)
	}
}

func TestGlobalAutoGroupModelFilterMissingAndInvalid(t *testing.T) {
	oldValue, hadValue := settingCache.Get(model.SettingKeyGlobalAutoGroupModelFilter)
	t.Cleanup(func() {
		if hadValue {
			settingCache.Set(model.SettingKeyGlobalAutoGroupModelFilter, oldValue)
		} else {
			settingCache.Del(model.SettingKeyGlobalAutoGroupModelFilter)
		}
	})
	settingCache.Del(model.SettingKeyGlobalAutoGroupModelFilter)
	filter, err := GlobalAutoGroupModelFilter()
	if err != nil || filter.Mode != model.GlobalAutoGroupModelFilterModeOff {
		t.Fatalf("missing = %+v, %v", filter, err)
	}
	for _, value := range []string{"", "{", `{"mode":"invalid","keywords":[]}`} {
		settingCache.Set(model.SettingKeyGlobalAutoGroupModelFilter, value)
		if _, err := GlobalAutoGroupModelFilter(); err == nil {
			t.Fatalf("accepted invalid persisted setting %q", value)
		}
	}
}
