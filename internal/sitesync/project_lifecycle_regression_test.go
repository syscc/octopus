package sitesync

import (
	"testing"

	"github.com/bestruirui/octopus/internal/db"
	"github.com/bestruirui/octopus/internal/model"
	"github.com/bestruirui/octopus/internal/op"
	"github.com/bestruirui/octopus/internal/transformer/outbound"
)

func TestReuseManagedChannelByNameRejectsDifferentGroup(t *testing.T) {
	ctx := setupProjectTestDB(t)
	site, account := createProjectionFixture(t, ctx)

	channel := &model.Channel{
		Name:     "same-display-name",
		Type:     outbound.OutboundTypeOpenAIChat,
		Enabled:  true,
		BaseUrls: []model.BaseUrl{{URL: "https://example.com/v1"}},
		Keys:     []model.ChannelKey{{Enabled: true, ChannelKey: "group-a-key"}},
	}
	if err := op.ChannelCreate(channel, ctx); err != nil {
		t.Fatalf("create managed channel: %v", err)
	}
	if err := db.GetDB().WithContext(ctx).Create(&model.SiteChannelBinding{
		SiteID: site.ID, SiteAccountID: account.ID, GroupKey: "group-a", ChannelID: channel.ID,
	}).Error; err != nil {
		t.Fatalf("create existing group binding: %v", err)
	}

	payload := model.Channel{Name: channel.Name, Type: outbound.OutboundTypeOpenAIChat}
	_, reused, err := reuseManagedChannelByName(
		ctx,
		site,
		account,
		model.SiteUserGroup{GroupKey: "group-b", Name: "same display name"},
		"group-b",
		payload,
	)
	if err == nil {
		t.Fatalf("expected same-name channel from another group to be rejected, reused=%t", reused)
	}
	if reused {
		t.Fatalf("different-group channel must not be reported as reused: %v", err)
	}
}
