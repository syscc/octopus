package relay

import (
	"context"
	"testing"
	"time"

	dbmodel "github.com/bestruirui/octopus/internal/model"
	"github.com/bestruirui/octopus/internal/op"
	"github.com/bestruirui/octopus/internal/relay/balancer"
	"github.com/bestruirui/octopus/internal/transformer/inbound"
	transformerModel "github.com/bestruirui/octopus/internal/transformer/model"
	"github.com/bestruirui/octopus/internal/transformer/outbound"
)

func TestNewWSRelayRequestCopiesRawBody(t *testing.T) {
	ctx := setupRelayTestDB(t)
	channel := &dbmodel.Channel{
		Name:     "ws-raw-body",
		Type:     outbound.OutboundTypeOpenAIChat,
		Enabled:  true,
		BaseUrls: []dbmodel.BaseUrl{{URL: "https://unused.invalid/v1"}},
		Model:    "upstream-model",
		Keys:     []dbmodel.ChannelKey{{Enabled: true, ChannelKey: "key"}},
	}
	if err := op.ChannelCreate(channel, ctx); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	group := &dbmodel.Group{Name: "ws-raw-body-group", Mode: dbmodel.GroupModeFailover}
	if err := op.GroupCreate(group, ctx); err != nil {
		t.Fatalf("create group: %v", err)
	}
	if err := op.GroupItemAdd(&dbmodel.GroupItem{GroupID: group.ID, ChannelID: channel.ID, ModelName: "upstream-model"}, ctx); err != nil {
		t.Fatalf("add group item: %v", err)
	}

	content := "hello"
	execution := &transformerModel.InternalLLMRequest{
		Model:        group.Name,
		RawAPIFormat: transformerModel.APIFormatOpenAIResponse,
		Messages: []transformerModel.Message{{
			Role:    "user",
			Content: transformerModel.MessageContent{Content: &content},
		}},
	}
	rawBody := []byte(`{"model":"ws-raw-body-group","input":"hello","future_field":true}`)
	req, _, err := newWSRelayRequest(context.Background(), nil, inbound.Get(inbound.InboundTypeOpenAIResponse), 1, group.Name, execution, execution, nil, rawBody)
	if err != nil {
		t.Fatalf("newWSRelayRequest failed: %v", err)
	}
	rawBody[0] = 'x'
	if string(req.rawBody) != `{"model":"ws-raw-body-group","input":"hello","future_field":true}` {
		t.Fatalf("expected copied raw body, got %q", req.rawBody)
	}
}

func TestNewWSRelayRequestDropsRawBodyForExactReplay(t *testing.T) {
	ctx := setupRelayTestDB(t)
	channel := &dbmodel.Channel{
		Name:     "ws-exact-replay",
		Type:     outbound.OutboundTypeOpenAIResponse,
		Enabled:  true,
		BaseUrls: []dbmodel.BaseUrl{{URL: "https://unused.invalid/v1"}},
		Model:    "upstream-model",
		Keys:     []dbmodel.ChannelKey{{Enabled: true, ChannelKey: "key"}},
	}
	if err := op.ChannelCreate(channel, ctx); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	group := &dbmodel.Group{Name: "ws-exact-replay-group", Mode: dbmodel.GroupModeFailover}
	if err := op.GroupCreate(group, ctx); err != nil {
		t.Fatalf("create group: %v", err)
	}
	if err := op.GroupItemAdd(&dbmodel.GroupItem{GroupID: group.ID, ChannelID: channel.ID, ModelName: "upstream-model"}, ctx); err != nil {
		t.Fatalf("add group item: %v", err)
	}

	content := "hello"
	execution := &transformerModel.InternalLLMRequest{
		Model:        group.Name,
		RawAPIFormat: transformerModel.APIFormatOpenAIResponse,
		Messages: []transformerModel.Message{{
			Role:    "user",
			Content: transformerModel.MessageContent{Content: &content},
		}},
	}
	execution.MarkOpenAIExactReplayRequest()
	req, _, err := newWSRelayRequest(context.Background(), nil, inbound.Get(inbound.InboundTypeOpenAIResponse), 1, group.Name, execution, execution, nil, []byte(`{"previous_response_id":"stale"}`))
	if err != nil {
		t.Fatalf("newWSRelayRequest failed: %v", err)
	}
	if len(req.rawBody) != 0 {
		t.Fatalf("expected exact replay to rebuild payload instead of using stale raw body, got %q", req.rawBody)
	}
}

func TestResolveWSConversationStateFallsBackToStoredState(t *testing.T) {
	resetWSConversationStateStore()
	t.Cleanup(resetWSConversationStateStore)

	stored := &wsConversationState{
		DownstreamSessionID: "ws_a",
		RequestModel:        "gpt-5.4",
		ChannelID:           11,
		ChannelKeyID:        22,
		LastResponseID:      "resp_saved",
	}
	storeWSConversationState(7, "gpt-5.4", stored, time.Minute)

	resolved := resolveWSConversationState(7, "gpt-5.4", nil, true, "ws_a")
	if resolved == nil {
		t.Fatalf("expected stored conversation state to be resolved")
	}
	if resolved.LastResponseID != "resp_saved" || resolved.ChannelID != 11 || resolved.ChannelKeyID != 22 {
		t.Fatalf("unexpected resolved state: %#v", resolved)
	}
	if resolved == stored {
		t.Fatalf("expected resolved state to be cloned, got same pointer")
	}
}

func TestResolveWSConversationStatePrefersMatchingLocalState(t *testing.T) {
	resetWSConversationStateStore()
	t.Cleanup(resetWSConversationStateStore)

	storeWSConversationState(7, "gpt-5.4", &wsConversationState{DownstreamSessionID: "ws_a", RequestModel: "gpt-5.4", LastResponseID: "resp_saved"}, time.Minute)
	local := &wsConversationState{RequestModel: "gpt-5.4", LastResponseID: "resp_local"}

	resolved := resolveWSConversationState(7, "gpt-5.4", local, true, "ws_a")
	if resolved != local {
		t.Fatalf("expected matching local state to be reused")
	}
}

func TestResolveWSConversationStateDoesNotRestoreStoredStateForFreshConnection(t *testing.T) {
	resetWSConversationStateStore()
	t.Cleanup(resetWSConversationStateStore)

	storeWSConversationState(7, "gpt-5.4", &wsConversationState{
		DownstreamSessionID: "ws_a",
		RequestModel:        "gpt-5.4",
		LastResponseID:      "resp_saved",
		Transcript:          []transformerModel.Message{{Role: "assistant"}},
	}, time.Minute)

	resolved := resolveWSConversationState(7, "gpt-5.4", nil, false, "ws_b")
	if resolved != nil {
		t.Fatalf("expected fresh connection to ignore stored continuation state, got %#v", resolved)
	}
}

func TestResolveWSConversationStateRestoresStoredStateForContinuation(t *testing.T) {
	resetWSConversationStateStore()
	t.Cleanup(resetWSConversationStateStore)

	storeWSConversationState(7, "gpt-5.4", &wsConversationState{
		DownstreamSessionID: "ws_a",
		RequestModel:        "gpt-5.4",
		LastResponseID:      "resp_saved",
		Transcript:          []transformerModel.Message{{Role: "assistant"}},
	}, time.Minute)

	local := &wsConversationState{RequestModel: "other-model"}
	resolved := resolveWSConversationState(7, "gpt-5.4", local, true, "ws_a")
	if resolved == nil {
		t.Fatalf("expected stored continuation state to be restored")
	}
	if resolved.LastResponseID != "resp_saved" || len(resolved.Transcript) != 1 {
		t.Fatalf("unexpected restored state: %#v", resolved)
	}
}

func TestShouldRestoreStoredWSConversationState(t *testing.T) {
	if resolveWSConversationState(7, "", nil, true, "ws_a") != nil {
		t.Fatalf("expected empty request model to skip restore")
	}
}

func TestResolveWSConversationStateIsSessionScoped(t *testing.T) {
	resetWSConversationStateStore()
	t.Cleanup(resetWSConversationStateStore)

	storeWSConversationState(7, "gpt-5.4", &wsConversationState{
		DownstreamSessionID: "ws_a",
		RequestModel:        "gpt-5.4",
		LastResponseID:      "resp_saved",
	}, time.Minute)

	if resolved := resolveWSConversationState(7, "gpt-5.4", nil, true, "ws_b"); resolved != nil {
		t.Fatalf("expected different downstream session to not restore state, got %#v", resolved)
	}
}

func TestWSConversationStateToSticky(t *testing.T) {
	entry := wsConversationStateToSticky(&wsConversationState{ChannelID: 5, ChannelKeyID: 9})
	if entry == nil {
		t.Fatalf("expected sticky entry to be created")
	}
	if entry.ChannelID != 5 || entry.ChannelKeyID != 9 {
		t.Fatalf("unexpected sticky entry: %#v", entry)
	}
	if wsConversationStateToSticky(&wsConversationState{}) != nil {
		t.Fatalf("expected empty conversation state to produce nil sticky entry")
	}
}

func TestNewIteratorWithPreferenceMovesPreferredChannelFirst(t *testing.T) {
	group := dbmodel.Group{
		Items: []dbmodel.GroupItem{
			{ChannelID: 1, ModelName: "gpt-5.4", Priority: 1},
			{ChannelID: 2, ModelName: "gpt-5.4", Priority: 2},
		},
	}

	iter := balancer.NewIteratorWithPreference(group, 1, "gpt-5.4", &balancer.SessionEntry{ChannelID: 2, ChannelKeyID: 20})
	if !iter.Next() {
		t.Fatalf("expected iterator to have candidates")
	}
	if first := iter.Item(); first.ChannelID != 2 {
		t.Fatalf("expected preferred channel first, got %#v", first)
	}
	if !iter.IsSticky() || iter.StickyKeyID() != 20 {
		t.Fatalf("expected preferred channel to be treated as sticky")
	}
}

func TestFinalChannelKey(t *testing.T) {
	channelID, keyID := finalChannelKey([]dbmodel.ChannelAttempt{
		{ChannelID: 1, ChannelKeyID: 2, Status: dbmodel.AttemptFailed},
		{ChannelID: 3, ChannelKeyID: 4, Status: dbmodel.AttemptSuccess},
	})
	if channelID != 3 || keyID != 4 {
		t.Fatalf("expected success attempt to win, got channel=%d key=%d", channelID, keyID)
	}

	channelID, keyID = finalChannelKey([]dbmodel.ChannelAttempt{{ChannelID: 8, ChannelKeyID: 6, Status: dbmodel.AttemptFailed}})
	if channelID != 8 || keyID != 6 {
		t.Fatalf("expected last failed attempt when no success, got channel=%d key=%d", channelID, keyID)
	}
}
