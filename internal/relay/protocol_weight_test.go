package relay

import (
	"testing"

	"github.com/bestruirui/octopus/internal/model"
	"github.com/bestruirui/octopus/internal/op"
	"github.com/bestruirui/octopus/internal/relay/balancer"
	transformerModel "github.com/bestruirui/octopus/internal/transformer/model"
	"github.com/bestruirui/octopus/internal/transformer/outbound"
)

func TestApplyProtocolPreferenceForModePreservesWeightedOrder(t *testing.T) {
	ctx := setupRelayTestDB(t)
	chat := &model.Channel{Name: "weighted-chat", Type: outbound.OutboundTypeOpenAIChat, Enabled: true}
	response := &model.Channel{Name: "weighted-response", Type: outbound.OutboundTypeOpenAIResponse, Enabled: true}
	if err := op.ChannelCreate(chat, ctx); err != nil {
		t.Fatalf("create chat channel: %v", err)
	}
	if err := op.ChannelCreate(response, ctx); err != nil {
		t.Fatalf("create response channel: %v", err)
	}
	group := model.Group{
		Mode: model.GroupModeFailover,
		Items: []model.GroupItem{
			{ChannelID: response.ID, Priority: 1, Weight: 100},
			{ChannelID: chat.ID, Priority: 2, Weight: 1},
		},
	}
	iter := balancer.NewIterator(group, 0, "weighted-model")

	applyProtocolPreferenceForMode(model.GroupModeWeighted, &transformerModel.InternalLLMRequest{
		RawAPIFormat: transformerModel.APIFormatOpenAIChatCompletion,
	}, iter, ctx)

	if !iter.Next() {
		t.Fatal("expected weighted candidate")
	}
	if got := iter.Item().ChannelID; got != response.ID {
		t.Fatalf("expected weighted order to remain unchanged, got channel %d", got)
	}
}
