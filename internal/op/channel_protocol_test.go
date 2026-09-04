package op

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	dbpkg "github.com/bestruirui/octopus/internal/db"
	"github.com/bestruirui/octopus/internal/model"
	"github.com/bestruirui/octopus/internal/transformer/outbound"
)

// channelProtocolState is the persisted protocol capability state of a
// channel, comparable with == to detect unexpected writes.
type channelProtocolState struct {
	Mode      model.OpenAIProtocolMode
	Chat      model.OpenAIProtocolCapability
	Responses model.OpenAIProtocolCapability
}

// loadChannelProtocolStateFromDB reads the three protocol columns of a
// channel straight from the database with a raw query, so assertions are
// independent of GORM struct mapping.
func loadChannelProtocolStateFromDB(t *testing.T, ctx context.Context, channelID int) channelProtocolState {
	t.Helper()

	var row struct {
		Mode      string
		Chat      string
		Responses string
	}
	if err := dbpkg.GetDB().WithContext(ctx).
		Raw("SELECT openai_protocol_mode AS mode, openai_chat_capability AS chat, openai_responses_capability AS responses FROM channels WHERE id = ?", channelID).
		Scan(&row).Error; err != nil {
		t.Fatalf("query channel protocol state from db failed: %v", err)
	}
	return channelProtocolState{
		Mode:      model.OpenAIProtocolMode(row.Mode).Normalize(),
		Chat:      model.OpenAIProtocolCapability(row.Chat).Normalize(),
		Responses: model.OpenAIProtocolCapability(row.Responses).Normalize(),
	}
}

func cachedChannelProtocolState(t *testing.T, ctx context.Context, channelID int) channelProtocolState {
	t.Helper()

	channel, err := ChannelGet(channelID, ctx)
	if err != nil {
		t.Fatalf("ChannelGet failed: %v", err)
	}
	return channelProtocolState{
		Mode:      channel.OpenAIProtocolMode.Normalize(),
		Chat:      channel.OpenAIChatCapability.Normalize(),
		Responses: channel.OpenAIResponsesCapability.Normalize(),
	}
}

func createOpenAIProtocolTestChannel(t *testing.T, ctx context.Context, name string, channelType outbound.OutboundType, baseURLs []model.BaseUrl) *model.Channel {
	t.Helper()

	channel := &model.Channel{
		Name:     name,
		Type:     channelType,
		Enabled:  true,
		BaseUrls: baseURLs,
	}
	if err := ChannelCreate(channel, ctx); err != nil {
		t.Fatalf("ChannelCreate failed: %v", err)
	}
	return channel
}

func mustRecordOpenAIProtocolCapability(t *testing.T, ctx context.Context, channelID int, protocol outbound.OutboundType, capability model.OpenAIProtocolCapability) {
	t.Helper()

	if err := ChannelRecordOpenAIProtocolCapability(channelID, protocol, capability, ctx); err != nil {
		t.Fatalf("ChannelRecordOpenAIProtocolCapability(%d, %d, %q) failed: %v", channelID, protocol, capability, err)
	}
}

// TestChannelOpenAIProtocolAutoLearningPersistsChatAndResponsesIndependently
// verifies that in auto mode each protocol observation persists to its own DB
// column and to the runtime cache without clobbering the other protocol.
func TestChannelOpenAIProtocolAutoLearningPersistsChatAndResponsesIndependently(t *testing.T) {
	ctx := setupOpenAIProtocolTestDB(t)
	channel := createOpenAIProtocolTestChannel(t, ctx, "auto-learning-channel", outbound.OutboundTypeOpenAIChat,
		[]model.BaseUrl{{URL: "https://api.example.com/v1"}})

	want := channelProtocolState{Mode: model.OpenAIProtocolModeAuto, Chat: model.OpenAIProtocolCapabilityUnknown, Responses: model.OpenAIProtocolCapabilityUnknown}
	if got := loadChannelProtocolStateFromDB(t, ctx, channel.ID); got != want {
		t.Fatalf("expected fresh channel state %#v in db, got %#v", want, got)
	}

	mustRecordOpenAIProtocolCapability(t, ctx, channel.ID, outbound.OutboundTypeOpenAIChat, model.OpenAIProtocolCapabilitySupported)
	want.Chat = model.OpenAIProtocolCapabilitySupported
	if got := loadChannelProtocolStateFromDB(t, ctx, channel.ID); got != want {
		t.Fatalf("expected chat observation to persist only the chat column in db, want %#v, got %#v", want, got)
	}
	if got := cachedChannelProtocolState(t, ctx, channel.ID); got != want {
		t.Fatalf("expected chat observation to persist only the chat column in cache, want %#v, got %#v", want, got)
	}

	mustRecordOpenAIProtocolCapability(t, ctx, channel.ID, outbound.OutboundTypeOpenAIResponse, model.OpenAIProtocolCapabilitySupported)
	want.Responses = model.OpenAIProtocolCapabilitySupported
	if got := loadChannelProtocolStateFromDB(t, ctx, channel.ID); got != want {
		t.Fatalf("expected responses observation to keep the learned chat column in db, want %#v, got %#v", want, got)
	}
	if got := cachedChannelProtocolState(t, ctx, channel.ID); got != want {
		t.Fatalf("expected responses observation to keep the learned chat column in cache, want %#v, got %#v", want, got)
	}

	// A later contradicting observation still updates the same column only.
	mustRecordOpenAIProtocolCapability(t, ctx, channel.ID, outbound.OutboundTypeOpenAIResponse, model.OpenAIProtocolCapabilityUnsupported)
	want.Responses = model.OpenAIProtocolCapabilityUnsupported
	if got := loadChannelProtocolStateFromDB(t, ctx, channel.ID); got != want {
		t.Fatalf("expected responses re-learning to update only the responses column in db, want %#v, got %#v", want, got)
	}
}

// TestChannelOpenAIProtocolManualModesRejectRuntimeLearning verifies that all
// three manual modes are authoritative: runtime capability records are
// rejected without touching the DB or the cache.
func TestChannelOpenAIProtocolManualModesRejectRuntimeLearning(t *testing.T) {
	for _, mode := range []model.OpenAIProtocolMode{
		model.OpenAIProtocolModeChatOnly,
		model.OpenAIProtocolModeResponsesOnly,
		model.OpenAIProtocolModeBoth,
	} {
		t.Run(string(mode), func(t *testing.T) {
			ctx := setupOpenAIProtocolTestDB(t)
			channel := createOpenAIProtocolTestChannel(t, ctx, "manual-"+string(mode), outbound.OutboundTypeOpenAIChat,
				[]model.BaseUrl{{URL: "https://api.example.com/v1"}})

			if _, err := ChannelUpdate(&model.ChannelUpdateRequest{
				ID:                 channel.ID,
				OpenAIProtocolMode: &mode,
			}, ctx); err != nil {
				t.Fatalf("ChannelUpdate to manual mode %q failed: %v", mode, err)
			}

			want := channelProtocolState{Mode: mode, Chat: model.OpenAIProtocolCapabilityUnknown, Responses: model.OpenAIProtocolCapabilityUnknown}
			if got := loadChannelProtocolStateFromDB(t, ctx, channel.ID); got != want {
				t.Fatalf("expected manual state %#v in db, got %#v", want, got)
			}

			// Values deliberately differ from the persisted columns so a lost
			// rejection would be observable.
			mustRecordOpenAIProtocolCapability(t, ctx, channel.ID, outbound.OutboundTypeOpenAIChat, model.OpenAIProtocolCapabilityUnsupported)
			mustRecordOpenAIProtocolCapability(t, ctx, channel.ID, outbound.OutboundTypeOpenAIResponse, model.OpenAIProtocolCapabilitySupported)

			if got := loadChannelProtocolStateFromDB(t, ctx, channel.ID); got != want {
				t.Fatalf("expected manual mode %q to reject learning in db, want %#v, got %#v", mode, want, got)
			}
			if got := cachedChannelProtocolState(t, ctx, channel.ID); got != want {
				t.Fatalf("expected manual mode %q to reject learning in cache, want %#v, got %#v", mode, want, got)
			}

			cached, err := ChannelGet(channel.ID, ctx)
			if err != nil {
				t.Fatalf("ChannelGet failed: %v", err)
			}
			wantChat, wantResponses := model.OpenAIProtocolCapabilitySupported, model.OpenAIProtocolCapabilityUnsupported
			switch mode {
			case model.OpenAIProtocolModeResponsesOnly:
				wantChat, wantResponses = model.OpenAIProtocolCapabilityUnsupported, model.OpenAIProtocolCapabilitySupported
			case model.OpenAIProtocolModeBoth:
				wantChat, wantResponses = model.OpenAIProtocolCapabilitySupported, model.OpenAIProtocolCapabilitySupported
			}
			if got := cached.EffectiveOpenAIProtocolCapability(outbound.OutboundTypeOpenAIChat); got != wantChat {
				t.Fatalf("manual mode %q: expected effective chat %q, got %q", mode, wantChat, got)
			}
			if got := cached.EffectiveOpenAIProtocolCapability(outbound.OutboundTypeOpenAIResponse); got != wantResponses {
				t.Fatalf("manual mode %q: expected effective responses %q, got %q", mode, wantResponses, got)
			}
		})
	}
}

// TestChannelOpenAIProtocolExplicitReturnToAutoResetsCapabilities verifies
// that switching a manual mode back to auto resets the learned capabilities.
func TestChannelOpenAIProtocolExplicitReturnToAutoResetsCapabilities(t *testing.T) {
	ctx := setupOpenAIProtocolTestDB(t)
	channel := createOpenAIProtocolTestChannel(t, ctx, "manual-to-auto-channel", outbound.OutboundTypeOpenAIChat,
		[]model.BaseUrl{{URL: "https://api.example.com/v1"}})

	mustRecordOpenAIProtocolCapability(t, ctx, channel.ID, outbound.OutboundTypeOpenAIChat, model.OpenAIProtocolCapabilitySupported)
	mustRecordOpenAIProtocolCapability(t, ctx, channel.ID, outbound.OutboundTypeOpenAIResponse, model.OpenAIProtocolCapabilitySupported)

	chatOnly := model.OpenAIProtocolModeChatOnly
	if _, err := ChannelUpdate(&model.ChannelUpdateRequest{
		ID:                 channel.ID,
		OpenAIProtocolMode: &chatOnly,
	}, ctx); err != nil {
		t.Fatalf("ChannelUpdate to chat_only failed: %v", err)
	}

	auto := model.OpenAIProtocolModeAuto
	if _, err := ChannelUpdate(&model.ChannelUpdateRequest{
		ID:                 channel.ID,
		OpenAIProtocolMode: &auto,
	}, ctx); err != nil {
		t.Fatalf("ChannelUpdate back to auto failed: %v", err)
	}

	want := channelProtocolState{Mode: model.OpenAIProtocolModeAuto, Chat: model.OpenAIProtocolCapabilityUnknown, Responses: model.OpenAIProtocolCapabilityUnknown}
	if got := loadChannelProtocolStateFromDB(t, ctx, channel.ID); got != want {
		t.Fatalf("expected explicit return to auto to reset capabilities in db, want %#v, got %#v", want, got)
	}
	if got := cachedChannelProtocolState(t, ctx, channel.ID); got != want {
		t.Fatalf("expected explicit return to auto to reset capabilities in cache, want %#v, got %#v", want, got)
	}
}

// TestChannelOpenAIProtocolIdenticalTypeAndBaseUrlsKeepLearnedCapabilities
// verifies that submitting the same Type and BaseUrls as the channel already
// has is not treated as a channel change and keeps learned capabilities.
func TestChannelOpenAIProtocolIdenticalTypeAndBaseUrlsKeepLearnedCapabilities(t *testing.T) {
	ctx := setupOpenAIProtocolTestDB(t)
	baseURLs := []model.BaseUrl{{URL: "https://api.example.com/v1", Delay: 10}}
	channel := createOpenAIProtocolTestChannel(t, ctx, "identical-update-channel", outbound.OutboundTypeOpenAIChat, baseURLs)

	mustRecordOpenAIProtocolCapability(t, ctx, channel.ID, outbound.OutboundTypeOpenAIChat, model.OpenAIProtocolCapabilitySupported)
	mustRecordOpenAIProtocolCapability(t, ctx, channel.ID, outbound.OutboundTypeOpenAIResponse, model.OpenAIProtocolCapabilityUnsupported)

	sameType := outbound.OutboundTypeOpenAIChat
	sameBaseURLs := []model.BaseUrl{{URL: "https://api.example.com/v1", Delay: 10}}
	if _, err := ChannelUpdate(&model.ChannelUpdateRequest{
		ID:       channel.ID,
		Type:     &sameType,
		BaseUrls: &sameBaseURLs,
	}, ctx); err != nil {
		t.Fatalf("ChannelUpdate with identical type/base urls failed: %v", err)
	}

	want := channelProtocolState{Mode: model.OpenAIProtocolModeAuto, Chat: model.OpenAIProtocolCapabilitySupported, Responses: model.OpenAIProtocolCapabilityUnsupported}
	if got := loadChannelProtocolStateFromDB(t, ctx, channel.ID); got != want {
		t.Fatalf("expected identical type/base urls update to keep learned capabilities in db, want %#v, got %#v", want, got)
	}
	if got := cachedChannelProtocolState(t, ctx, channel.ID); got != want {
		t.Fatalf("expected identical type/base urls update to keep learned capabilities in cache, want %#v, got %#v", want, got)
	}
}

// TestChannelOpenAIProtocolBaseUrlsChangeResetsLearnedCapabilities verifies
// that a real base URL change resets the learned capabilities while the mode
// stays auto.
func TestChannelOpenAIProtocolBaseUrlsChangeResetsLearnedCapabilities(t *testing.T) {
	ctx := setupOpenAIProtocolTestDB(t)
	channel := createOpenAIProtocolTestChannel(t, ctx, "base-url-change-channel", outbound.OutboundTypeOpenAIChat,
		[]model.BaseUrl{{URL: "https://old.example.com/v1"}})

	mustRecordOpenAIProtocolCapability(t, ctx, channel.ID, outbound.OutboundTypeOpenAIChat, model.OpenAIProtocolCapabilitySupported)
	mustRecordOpenAIProtocolCapability(t, ctx, channel.ID, outbound.OutboundTypeOpenAIResponse, model.OpenAIProtocolCapabilitySupported)

	newBaseURLs := []model.BaseUrl{{URL: "https://new.example.com/v1"}}
	if _, err := ChannelUpdate(&model.ChannelUpdateRequest{
		ID:       channel.ID,
		BaseUrls: &newBaseURLs,
	}, ctx); err != nil {
		t.Fatalf("ChannelUpdate with new base urls failed: %v", err)
	}

	want := channelProtocolState{Mode: model.OpenAIProtocolModeAuto, Chat: model.OpenAIProtocolCapabilityUnknown, Responses: model.OpenAIProtocolCapabilityUnknown}
	if got := loadChannelProtocolStateFromDB(t, ctx, channel.ID); got != want {
		t.Fatalf("expected base url change to reset learned capabilities in db, want %#v, got %#v", want, got)
	}
	if got := cachedChannelProtocolState(t, ctx, channel.ID); got != want {
		t.Fatalf("expected base url change to reset learned capabilities in cache, want %#v, got %#v", want, got)
	}
}

// TestChannelOpenAIProtocolTypeChangeResetsLearnedCapabilities verifies that
// changing the channel type between OpenAI text types resets the learned
// capabilities while the mode stays auto.
func TestChannelOpenAIProtocolTypeChangeResetsLearnedCapabilities(t *testing.T) {
	ctx := setupOpenAIProtocolTestDB(t)
	channel := createOpenAIProtocolTestChannel(t, ctx, "type-change-channel", outbound.OutboundTypeOpenAIChat,
		[]model.BaseUrl{{URL: "https://api.example.com/v1"}})

	mustRecordOpenAIProtocolCapability(t, ctx, channel.ID, outbound.OutboundTypeOpenAIChat, model.OpenAIProtocolCapabilitySupported)
	mustRecordOpenAIProtocolCapability(t, ctx, channel.ID, outbound.OutboundTypeOpenAIResponse, model.OpenAIProtocolCapabilitySupported)

	newType := outbound.OutboundTypeOpenAIResponse
	if _, err := ChannelUpdate(&model.ChannelUpdateRequest{
		ID:   channel.ID,
		Type: &newType,
	}, ctx); err != nil {
		t.Fatalf("ChannelUpdate with new type failed: %v", err)
	}

	want := channelProtocolState{Mode: model.OpenAIProtocolModeAuto, Chat: model.OpenAIProtocolCapabilityUnknown, Responses: model.OpenAIProtocolCapabilityUnknown}
	if got := loadChannelProtocolStateFromDB(t, ctx, channel.ID); got != want {
		t.Fatalf("expected type change to reset learned capabilities in db, want %#v, got %#v", want, got)
	}
	if got := cachedChannelProtocolState(t, ctx, channel.ID); got != want {
		t.Fatalf("expected type change to reset learned capabilities in cache, want %#v, got %#v", want, got)
	}
}

// TestChannelOpenAIProtocolLeavingCloudflareURLRestoresAutoMode verifies that
// a channel created with a Cloudflare Workers AI base URL is forced to
// chat_only, and that replacing the base URL without touching the mode
// automatically restores auto mode with reset capabilities.
func TestChannelOpenAIProtocolLeavingCloudflareURLRestoresAutoMode(t *testing.T) {
	ctx := setupOpenAIProtocolTestDB(t)
	channel := &model.Channel{
		Name:     "cloudflare-url-channel",
		Type:     outbound.OutboundTypeOpenAIResponse,
		Enabled:  true,
		BaseUrls: []model.BaseUrl{{URL: "https://api.cloudflare.com/client/v4/accounts/acc123/ai/v1"}},
	}
	if err := ChannelCreate(channel, ctx); err != nil {
		t.Fatalf("ChannelCreate failed: %v", err)
	}

	forced := channelProtocolState{Mode: model.OpenAIProtocolModeChatOnly, Chat: model.OpenAIProtocolCapabilitySupported, Responses: model.OpenAIProtocolCapabilityUnsupported}
	if got := loadChannelProtocolStateFromDB(t, ctx, channel.ID); got != forced {
		t.Fatalf("expected cloudflare create to force chat_only in db, want %#v, got %#v", forced, got)
	}
	if got := cachedChannelProtocolState(t, ctx, channel.ID); got != forced {
		t.Fatalf("expected cloudflare create to force chat_only in cache, want %#v, got %#v", forced, got)
	}

	normalBaseURLs := []model.BaseUrl{{URL: "https://api.example.com/v1"}}
	if _, err := ChannelUpdate(&model.ChannelUpdateRequest{
		ID:       channel.ID,
		BaseUrls: &normalBaseURLs,
	}, ctx); err != nil {
		t.Fatalf("ChannelUpdate away from cloudflare failed: %v", err)
	}

	want := channelProtocolState{Mode: model.OpenAIProtocolModeAuto, Chat: model.OpenAIProtocolCapabilityUnknown, Responses: model.OpenAIProtocolCapabilityUnknown}
	if got := loadChannelProtocolStateFromDB(t, ctx, channel.ID); got != want {
		t.Fatalf("expected leaving cloudflare url to restore auto mode in db, want %#v, got %#v", want, got)
	}
	if got := cachedChannelProtocolState(t, ctx, channel.ID); got != want {
		t.Fatalf("expected leaving cloudflare url to restore auto mode in cache, want %#v, got %#v", want, got)
	}

	cached, err := ChannelGet(channel.ID, ctx)
	if err != nil {
		t.Fatalf("ChannelGet failed: %v", err)
	}
	if got := cached.EffectiveOpenAIProtocolCapability(outbound.OutboundTypeOpenAIChat); got != model.OpenAIProtocolCapabilityUnknown {
		t.Fatalf("expected unknown effective chat capability after leaving cloudflare, got %q", got)
	}
}

// TestChannelOpenAIProtocolConcurrentRecordsKeepBothCapabilities verifies
// that concurrent runtime records for different protocols never lose a field:
// after the storm both columns are persisted in the DB and the cache.
func TestChannelOpenAIProtocolConcurrentRecordsKeepBothCapabilities(t *testing.T) {
	ctx := setupOpenAIProtocolTestDB(t)
	channel := createOpenAIProtocolTestChannel(t, ctx, "concurrent-record-channel", outbound.OutboundTypeOpenAIChat,
		[]model.BaseUrl{{URL: "https://api.example.com/v1"}})

	const goroutines = 8
	const iterations = 25
	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		protocol := outbound.OutboundTypeOpenAIChat
		if i%2 == 1 {
			protocol = outbound.OutboundTypeOpenAIResponse
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				if err := ChannelRecordOpenAIProtocolCapability(channel.ID, protocol, model.OpenAIProtocolCapabilitySupported, ctx); err != nil {
					t.Errorf("concurrent record failed: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()

	want := channelProtocolState{Mode: model.OpenAIProtocolModeAuto, Chat: model.OpenAIProtocolCapabilitySupported, Responses: model.OpenAIProtocolCapabilitySupported}
	if got := loadChannelProtocolStateFromDB(t, ctx, channel.ID); got != want {
		t.Fatalf("expected concurrent records to persist both columns in db, want %#v, got %#v", want, got)
	}
	if got := cachedChannelProtocolState(t, ctx, channel.ID); got != want {
		t.Fatalf("expected concurrent records to persist both columns in cache, want %#v, got %#v", want, got)
	}
}

// TestChannelOpenAIProtocolConcurrentRecordsDoNotOverrideManualMode
// verifies that runtime records racing with an admin switch to a manual mode
// can never override it: the final mode is manual, the effective capabilities
// follow the manual mode, and records issued after the switch are no-ops.
func TestChannelOpenAIProtocolConcurrentRecordsDoNotOverrideManualMode(t *testing.T) {
	ctx := setupOpenAIProtocolTestDB(t)
	channel := createOpenAIProtocolTestChannel(t, ctx, "manual-race-channel", outbound.OutboundTypeOpenAIChat,
		[]model.BaseUrl{{URL: "https://api.example.com/v1"}})

	const goroutines = 6
	const iterations = 50
	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				if err := ChannelRecordOpenAIProtocolCapability(channel.ID, outbound.OutboundTypeOpenAIChat, model.OpenAIProtocolCapabilityUnsupported, ctx); err != nil {
					t.Errorf("concurrent chat record failed: %v", err)
					return
				}
				if err := ChannelRecordOpenAIProtocolCapability(channel.ID, outbound.OutboundTypeOpenAIResponse, model.OpenAIProtocolCapabilitySupported, ctx); err != nil {
					t.Errorf("concurrent responses record failed: %v", err)
					return
				}
			}
		}()
	}

	chatOnly := model.OpenAIProtocolModeChatOnly
	if _, err := ChannelUpdate(&model.ChannelUpdateRequest{
		ID:                 channel.ID,
		OpenAIProtocolMode: &chatOnly,
	}, ctx); err != nil {
		t.Fatalf("ChannelUpdate to manual chat_only failed: %v", err)
	}
	wg.Wait()

	if got := loadChannelProtocolStateFromDB(t, ctx, channel.ID); got.Mode != model.OpenAIProtocolModeChatOnly {
		t.Fatalf("expected manual chat_only mode to survive concurrent records in db, got %#v", got)
	}
	cached, err := ChannelGet(channel.ID, ctx)
	if err != nil {
		t.Fatalf("ChannelGet failed: %v", err)
	}
	if cached.OpenAIProtocolMode != model.OpenAIProtocolModeChatOnly {
		t.Fatalf("expected manual chat_only mode to survive concurrent records in cache, got %q", cached.OpenAIProtocolMode)
	}
	if got := cached.EffectiveOpenAIProtocolCapability(outbound.OutboundTypeOpenAIChat); got != model.OpenAIProtocolCapabilitySupported {
		t.Fatalf("expected manual chat_only to keep chat supported, got %q", got)
	}
	if got := cached.EffectiveOpenAIProtocolCapability(outbound.OutboundTypeOpenAIResponse); got != model.OpenAIProtocolCapabilityUnsupported {
		t.Fatalf("expected manual chat_only to force responses unsupported, got %q", got)
	}

	// Records issued after the admin switch must be rejected without writes.
	afterSwitch := loadChannelProtocolStateFromDB(t, ctx, channel.ID)
	mustRecordOpenAIProtocolCapability(t, ctx, channel.ID, outbound.OutboundTypeOpenAIChat, model.OpenAIProtocolCapabilitySupported)
	mustRecordOpenAIProtocolCapability(t, ctx, channel.ID, outbound.OutboundTypeOpenAIResponse, model.OpenAIProtocolCapabilityUnsupported)
	if got := loadChannelProtocolStateFromDB(t, ctx, channel.ID); got != afterSwitch {
		t.Fatalf("expected records after manual switch to be rejected, before %#v, after %#v", afterSwitch, got)
	}
}

// TestChannelOpenAIProtocolDelayOnlyBaseURLChangeKeepsLearnedCapabilities
// verifies that a base URL update that only changes delays keeps the learned
// capabilities: only the URL list identifies the channel, so a delay-only
// change must not reset the Chat/Responses observations.
func TestChannelOpenAIProtocolDelayOnlyBaseURLChangeKeepsLearnedCapabilities(t *testing.T) {
	ctx := setupOpenAIProtocolTestDB(t)
	channel := createOpenAIProtocolTestChannel(t, ctx, "delay-only-change-channel", outbound.OutboundTypeOpenAIChat,
		[]model.BaseUrl{
			{URL: "https://api.example.com/v1", Delay: 10},
			{URL: "https://backup.example.com/v1", Delay: 20},
		})

	mustRecordOpenAIProtocolCapability(t, ctx, channel.ID, outbound.OutboundTypeOpenAIChat, model.OpenAIProtocolCapabilitySupported)
	mustRecordOpenAIProtocolCapability(t, ctx, channel.ID, outbound.OutboundTypeOpenAIResponse, model.OpenAIProtocolCapabilityUnsupported)

	// Same URLs in the same order, delays shuffled: not a channel change.
	delayOnlyBaseURLs := []model.BaseUrl{
		{URL: "https://api.example.com/v1", Delay: 30},
		{URL: "https://backup.example.com/v1", Delay: 5},
	}
	if _, err := ChannelUpdate(&model.ChannelUpdateRequest{
		ID:       channel.ID,
		BaseUrls: &delayOnlyBaseURLs,
	}, ctx); err != nil {
		t.Fatalf("ChannelUpdate with delay-only base url change failed: %v", err)
	}

	want := channelProtocolState{Mode: model.OpenAIProtocolModeAuto, Chat: model.OpenAIProtocolCapabilitySupported, Responses: model.OpenAIProtocolCapabilityUnsupported}
	if got := loadChannelProtocolStateFromDB(t, ctx, channel.ID); got != want {
		t.Fatalf("expected delay-only base url change to keep learned capabilities in db, want %#v, got %#v", want, got)
	}
	if got := cachedChannelProtocolState(t, ctx, channel.ID); got != want {
		t.Fatalf("expected delay-only base url change to keep learned capabilities in cache, want %#v, got %#v", want, got)
	}
}

func TestChannelOpenAIProtocolReorderedBaseURLsKeepLearnedCapabilities(t *testing.T) {
	ctx := setupOpenAIProtocolTestDB(t)
	channel := createOpenAIProtocolTestChannel(t, ctx, "reordered-base-urls-channel", outbound.OutboundTypeOpenAIChat,
		[]model.BaseUrl{
			{URL: "https://api.example.com/v1", Delay: 10},
			{URL: "https://backup.example.com/v1", Delay: 20},
		})
	mustRecordOpenAIProtocolCapability(t, ctx, channel.ID, outbound.OutboundTypeOpenAIChat, model.OpenAIProtocolCapabilitySupported)
	mustRecordOpenAIProtocolCapability(t, ctx, channel.ID, outbound.OutboundTypeOpenAIResponse, model.OpenAIProtocolCapabilityUnsupported)

	reordered := []model.BaseUrl{
		{URL: " https://backup.example.com/v1/ ", Delay: 1},
		{URL: "https://api.example.com/v1/", Delay: 999},
	}
	if _, err := ChannelUpdate(&model.ChannelUpdateRequest{ID: channel.ID, BaseUrls: &reordered}, ctx); err != nil {
		t.Fatalf("ChannelUpdate with reordered equivalent base urls failed: %v", err)
	}
	want := channelProtocolState{Mode: model.OpenAIProtocolModeAuto, Chat: model.OpenAIProtocolCapabilitySupported, Responses: model.OpenAIProtocolCapabilityUnsupported}
	if got := loadChannelProtocolStateFromDB(t, ctx, channel.ID); got != want {
		t.Fatalf("reordered equivalent base urls reset learned capabilities: want %#v got %#v", want, got)
	}
}

func TestBaseURLsEqualPreservesDuplicateMultiplicity(t *testing.T) {
	left := []model.BaseUrl{{URL: "https://a.example/v1"}, {URL: "https://a.example/v1"}}
	right := []model.BaseUrl{{URL: "https://a.example/v1"}, {URL: "https://b.example/v1"}}
	if baseURLsEqual(left, right) {
		t.Fatal("different URL multiplicities must not compare equal")
	}
}

// TestChannelOpenAIProtocolFullRefreshKeepsCommittedLearningResult verifies
// that a full channelRefreshCache never overwrites a committed protocol
// learning result with the stale snapshot taken before the refresh: the
// learning result is committed to the DB while the refresh snapshot already
// holds the pre-learning state, and the refresh must still end up serving
// the committed state.
func TestChannelOpenAIProtocolFullRefreshKeepsCommittedLearningResult(t *testing.T) {
	ctx := setupOpenAIProtocolTestDB(t)
	channel := createOpenAIProtocolTestChannel(t, ctx, "full-refresh-learning-channel", outbound.OutboundTypeOpenAIChat,
		[]model.BaseUrl{{URL: "https://api.example.com/v1"}})

	// Make sure the runtime cache snapshot taken below predates the learning
	// result by a clearly observable margin.
	time.Sleep(5 * time.Millisecond)

	staleSnapshot := channelCache.GetAll()

	if err := ChannelRecordOpenAIProtocolCapability(channel.ID, outbound.OutboundTypeOpenAIChat, model.OpenAIProtocolCapabilitySupported, ctx); err != nil {
		t.Fatalf("ChannelRecordOpenAIProtocolCapability failed: %v", err)
	}

	// Simulate a refresh racing with the committed learning result: the
	// stale snapshot is still in the cache when the refresh starts.
	for channelID, snapshot := range staleSnapshot {
		channelCache.Set(channelID, snapshot)
	}

	if err := channelRefreshCache(ctx); err != nil {
		t.Fatalf("channelRefreshCache failed: %v", err)
	}

	want := channelProtocolState{Mode: model.OpenAIProtocolModeAuto, Chat: model.OpenAIProtocolCapabilitySupported, Responses: model.OpenAIProtocolCapabilityUnknown}
	if got := loadChannelProtocolStateFromDB(t, ctx, channel.ID); got != want {
		t.Fatalf("expected committed learning result to survive the full refresh in db, want %#v, got %#v", want, got)
	}
	if got := cachedChannelProtocolState(t, ctx, channel.ID); got != want {
		t.Fatalf("expected committed learning result to survive the full refresh in cache, want %#v, got %#v", want, got)
	}
}

// TestChannelOpenAIProtocolRecordSwallowedUpdateDoesNotFakeCacheSuccess
// verifies the swallowed-write branch of the RowsAffected == 0 path: when a
// trigger silently ignores guarded chat-capability updates while the channel
// is still in auto mode and the target column has not reached the recorded
// capability, the record retries the guarded update once and then returns a
// diagnostic error instead of faking a cached success. The cache must mirror
// the authoritative DB state.
func TestChannelOpenAIProtocolRecordSwallowedUpdateDoesNotFakeCacheSuccess(t *testing.T) {
	ctx := setupOpenAIProtocolTestDB(t)
	channel := createOpenAIProtocolTestChannel(t, ctx, "noop-update-channel", outbound.OutboundTypeOpenAIChat,
		[]model.BaseUrl{{URL: "https://api.example.com/v1"}})

	// A SQLite trigger that silently ignores guarded chat-capability updates
	// for auto channels, so UPDATE reports RowsAffected == 0 without the
	// channel having been switched to a manual mode.
	if err := dbpkg.GetDB().Exec(
		"CREATE TRIGGER block_openai_chat_capability BEFORE UPDATE OF openai_chat_capability ON channels " +
			"WHEN NEW.openai_protocol_mode = 'auto' BEGIN SELECT RAISE(IGNORE); END",
	).Error; err != nil {
		t.Fatalf("create blocking trigger failed: %v", err)
	}
	t.Cleanup(func() {
		_ = dbpkg.GetDB().Exec("DROP TRIGGER IF EXISTS block_openai_chat_capability").Error
	})

	err := ChannelRecordOpenAIProtocolCapability(channel.ID, outbound.OutboundTypeOpenAIChat, model.OpenAIProtocolCapabilitySupported, ctx)
	if err == nil {
		t.Fatalf("expected a diagnostic error when the capability update is swallowed twice")
	}
	if !strings.Contains(err.Error(), "did not persist") {
		t.Fatalf("expected a did-not-persist diagnostic, got %q", err.Error())
	}

	// The DB write was swallowed by the trigger, so the committed column
	// stays unknown; the cache must mirror that authoritative state instead
	// of pretending the recorded capability landed.
	wantDB := channelProtocolState{Mode: model.OpenAIProtocolModeAuto, Chat: model.OpenAIProtocolCapabilityUnknown, Responses: model.OpenAIProtocolCapabilityUnknown}
	if got := loadChannelProtocolStateFromDB(t, ctx, channel.ID); got != wantDB {
		t.Fatalf("expected swallowed db update to keep the unknown column, want %#v, got %#v", wantDB, got)
	}
	if got := cachedChannelProtocolState(t, ctx, channel.ID); got != wantDB {
		t.Fatalf("expected cache to mirror the authoritative db state, want %#v, got %#v", wantDB, got)
	}
}

// TestChannelOpenAIProtocolRecordSwallowedUpdateSucceedsAfterRetry verifies
// the retry branch: when the first guarded UPDATE is swallowed (RowsAffected
// == 0) but the retry lands, the DB and the cache converge to the recorded
// capability and no error is returned.
func TestChannelOpenAIProtocolRecordSwallowedUpdateSucceedsAfterRetry(t *testing.T) {
	ctx := setupOpenAIProtocolTestDB(t)
	channel := createOpenAIProtocolTestChannel(t, ctx, "noop-retry-channel", outbound.OutboundTypeOpenAIChat,
		[]model.BaseUrl{{URL: "https://api.example.com/v1"}})

	// A trigger that swallows only the first guarded chat-capability update
	// for auto channels: after it fires once, the counter flips and later
	// updates run normally, so the record's single retry must land.
	if err := dbpkg.GetDB().Exec(
		"CREATE TABLE swallow_once (swallowed INTEGER); " +
			"INSERT INTO swallow_once (swallowed) VALUES (0); " +
			"CREATE TRIGGER swallow_first_chat_capability BEFORE UPDATE OF openai_chat_capability ON channels " +
			"WHEN NEW.openai_protocol_mode = 'auto' AND (SELECT swallowed FROM swallow_once) = 0 " +
			"BEGIN UPDATE swallow_once SET swallowed = 1; SELECT RAISE(IGNORE); END",
	).Error; err != nil {
		t.Fatalf("create swallow-once trigger failed: %v", err)
	}
	t.Cleanup(func() {
		_ = dbpkg.GetDB().Exec("DROP TRIGGER IF EXISTS swallow_first_chat_capability").Error
	})

	if err := ChannelRecordOpenAIProtocolCapability(channel.ID, outbound.OutboundTypeOpenAIChat, model.OpenAIProtocolCapabilitySupported, ctx); err != nil {
		t.Fatalf("ChannelRecordOpenAIProtocolCapability failed after one retry: %v", err)
	}

	want := channelProtocolState{Mode: model.OpenAIProtocolModeAuto, Chat: model.OpenAIProtocolCapabilitySupported, Responses: model.OpenAIProtocolCapabilityUnknown}
	if got := loadChannelProtocolStateFromDB(t, ctx, channel.ID); got != want {
		t.Fatalf("expected retry to persist the capability in db, want %#v, got %#v", want, got)
	}
	if got := cachedChannelProtocolState(t, ctx, channel.ID); got != want {
		t.Fatalf("expected retry to persist the capability in cache, want %#v, got %#v", want, got)
	}
}

// TestChannelOpenAIProtocolRecordNoopUpdateKeepsManualModeAuthoritative
// verifies the other RowsAffected == 0 branch: when the guarded UPDATE
// matches no rows because a manual mode is already committed in the DB
// (invisible to the stale cached snapshot), the manual state stays
// authoritative and runtime learning must not overwrite it.
func TestChannelOpenAIProtocolRecordNoopUpdateKeepsManualModeAuthoritative(t *testing.T) {
	ctx := setupOpenAIProtocolTestDB(t)
	channel := createOpenAIProtocolTestChannel(t, ctx, "noop-manual-channel", outbound.OutboundTypeOpenAIChat,
		[]model.BaseUrl{{URL: "https://api.example.com/v1"}})

	// A committed manual switch the cached snapshot has not seen yet.
	if err := dbpkg.GetDB().Exec(
		"UPDATE channels SET openai_protocol_mode = ? WHERE id = ?",
		model.OpenAIProtocolModeChatOnly, channel.ID,
	).Error; err != nil {
		t.Fatalf("set committed manual mode failed: %v", err)
	}

	mustRecordOpenAIProtocolCapability(t, ctx, channel.ID, outbound.OutboundTypeOpenAIChat, model.OpenAIProtocolCapabilitySupported)

	want := channelProtocolState{Mode: model.OpenAIProtocolModeChatOnly, Chat: model.OpenAIProtocolCapabilityUnknown, Responses: model.OpenAIProtocolCapabilityUnknown}
	if got := loadChannelProtocolStateFromDB(t, ctx, channel.ID); got != want {
		t.Fatalf("expected committed manual mode to stay authoritative in db, want %#v, got %#v", want, got)
	}
	// The cache must not apply the learning result either.
	if got := cachedChannelProtocolState(t, ctx, channel.ID); got.Chat != model.OpenAIProtocolCapabilityUnknown {
		t.Fatalf("expected cached chat capability to stay unknown under committed manual mode, got %#v", got)
	}
}

// TestChannelUpdateCommittedCandidateFallbackSyncsCache verifies the
// fallback helper for a committed-but-unrefreshable ChannelUpdate: the
// committed protocol/type/base URL state is written onto the cached channel,
// deleted key IDs are dropped from the key cache and the balancer state is
// reset, so a manual mode no longer runs on the stale pre-update cache.
func TestChannelUpdateCommittedCandidateFallbackSyncsCache(t *testing.T) {
	ctx := setupOpenAIProtocolTestDB(t)
	channel := createOpenAIProtocolTestChannel(t, ctx, "fallback-channel", outbound.OutboundTypeOpenAIChat,
		[]model.BaseUrl{{URL: "https://old.example.com/v1"}})

	cached, ok := channelCache.Get(channel.ID)
	if !ok {
		t.Fatalf("channel cache miss after create")
	}
	staleKeyID := 4242
	channelKeyCache.Set(staleKeyID, model.ChannelKey{ID: staleKeyID, ChannelID: channel.ID})
	previousKeys := append([]model.ChannelKey{{ID: staleKeyID, ChannelID: channel.ID}}, cached.Keys...)

	candidate := cached
	candidate.Type = outbound.OutboundTypeOpenAIChat
	candidate.BaseUrls = []model.BaseUrl{{URL: "https://new.example.com/v1"}}
	chatOnly := model.OpenAIProtocolModeChatOnly
	candidate.OpenAIProtocolMode = chatOnly
	candidate.OpenAIChatCapability = model.OpenAIProtocolCapabilitySupported
	candidate.OpenAIResponsesCapability = model.OpenAIProtocolCapabilityUnsupported

	// committedKeys is the key set the transaction committed (the stale key
	// is gone); previousKeys is the pre-update cache snapshot that still
	// contains it, so the fallback must drop it from the key cache.
	applyCommittedChannelUpdateToCache(channel.ID, &candidate, cached.Keys, previousKeys)

	got, err := ChannelGet(channel.ID, ctx)
	if err != nil {
		t.Fatalf("ChannelGet failed: %v", err)
	}
	if got.OpenAIProtocolMode != chatOnly {
		t.Fatalf("expected committed chat_only mode in cache, got %q", got.OpenAIProtocolMode)
	}
	if got.Type != outbound.OutboundTypeOpenAIChat {
		t.Fatalf("expected committed type in cache, got %q", got.Type)
	}
	if len(got.BaseUrls) != 1 || got.BaseUrls[0].URL != "https://new.example.com/v1" {
		t.Fatalf("expected committed base urls in cache, got %#v", got.BaseUrls)
	}
	if got.OpenAIChatCapability != model.OpenAIProtocolCapabilitySupported ||
		got.OpenAIResponsesCapability != model.OpenAIProtocolCapabilityUnsupported {
		t.Fatalf("expected committed capabilities in cache, got %#v", got)
	}
	if _, stillCached := channelKeyCache.Get(staleKeyID); stillCached {
		t.Fatalf("expected stale key %d to be dropped from the key cache", staleKeyID)
	}
}

func TestChannelUpdateUsesPersistedBaseURLsWhenCacheWasPruned(t *testing.T) {
	ctx := setupOpenAIProtocolTestDB(t)
	channel := createOpenAIProtocolTestChannel(t, ctx, "pruned-cache-channel", outbound.OutboundTypeOpenAIChat,
		[]model.BaseUrl{{URL: "https://api.example.com/v1"}})
	mustRecordOpenAIProtocolCapability(t, ctx, channel.ID, outbound.OutboundTypeOpenAIChat, model.OpenAIProtocolCapabilitySupported)
	mustRecordOpenAIProtocolCapability(t, ctx, channel.ID, outbound.OutboundTypeOpenAIResponse, model.OpenAIProtocolCapabilityUnsupported)

	// Simulate an availability probe removing the only cached URL while the
	// database still contains the authoritative endpoint.
	if err := ChannelBaseUrlUpdate(channel.ID, nil); err != nil {
		t.Fatalf("prune cached base urls: %v", err)
	}
	sameURLs := []model.BaseUrl{{URL: "https://api.example.com/v1"}}
	if _, err := ChannelUpdate(&model.ChannelUpdateRequest{ID: channel.ID, BaseUrls: &sameURLs}, ctx); err != nil {
		t.Fatalf("update with persisted base urls: %v", err)
	}
	want := channelProtocolState{Mode: model.OpenAIProtocolModeAuto, Chat: model.OpenAIProtocolCapabilitySupported, Responses: model.OpenAIProtocolCapabilityUnsupported}
	if got := loadChannelProtocolStateFromDB(t, ctx, channel.ID); got != want {
		t.Fatalf("pruned cache must not make an unchanged URL reset learning, want %#v got %#v", want, got)
	}
}

func TestChannelUpdateUsesPersistedCloudflareURLWhenCacheWasPruned(t *testing.T) {
	ctx := setupOpenAIProtocolTestDB(t)
	channel := &model.Channel{
		Name: "pruned-cloudflare-channel", Type: outbound.OutboundTypeOpenAIChat, Enabled: true,
		BaseUrls: []model.BaseUrl{{URL: "https://api.cloudflare.com/client/v4/accounts/accPruned/ai/v1"}},
	}
	if err := ChannelCreate(channel, ctx); err != nil {
		t.Fatalf("create cloudflare channel: %v", err)
	}
	if err := ChannelBaseUrlUpdate(channel.ID, nil); err != nil {
		t.Fatalf("prune cached cloudflare base urls: %v", err)
	}
	responsesOnly := model.OpenAIProtocolModeResponsesOnly
	updated, err := ChannelUpdate(&model.ChannelUpdateRequest{ID: channel.ID, OpenAIProtocolMode: &responsesOnly}, ctx)
	if err != nil {
		t.Fatalf("update pruned Cloudflare channel: %v", err)
	}
	if updated.OpenAIProtocolMode != model.OpenAIProtocolModeChatOnly {
		t.Fatalf("pruned Cloudflare cache must still force Chat-only, got %q", updated.OpenAIProtocolMode)
	}
	if got := loadChannelProtocolStateFromDB(t, ctx, channel.ID); got.Mode != model.OpenAIProtocolModeChatOnly ||
		got.Chat != model.OpenAIProtocolCapabilitySupported || got.Responses != model.OpenAIProtocolCapabilityUnsupported {
		t.Fatalf("Cloudflare protocol state was not preserved after update: %#v", got)
	}
}

func TestChannelUpdateRejectsManualProtocolModeForNonOpenAIType(t *testing.T) {
	ctx := setupOpenAIProtocolTestDB(t)
	channel := createOpenAIProtocolTestChannel(t, ctx, "manual-mode-non-openai", outbound.OutboundTypeOpenAIChat,
		[]model.BaseUrl{{URL: "https://api.example.com/v1"}})
	anthropic := outbound.OutboundTypeAnthropic
	chatOnly := model.OpenAIProtocolModeChatOnly
	if _, err := ChannelUpdate(&model.ChannelUpdateRequest{
		ID: channel.ID, Type: &anthropic, OpenAIProtocolMode: &chatOnly,
	}, ctx); err == nil || !strings.Contains(err.Error(), "only valid for openai text channels") {
		t.Fatalf("expected explicit manual mode on Anthropic to fail, got %v", err)
	}
	if got := loadChannelProtocolStateFromDB(t, ctx, channel.ID); got.Mode != model.OpenAIProtocolModeAuto {
		t.Fatalf("rejected update changed persisted protocol state: %#v", got)
	}
}

func TestChannelUpdateTypeChangeToNonOpenAIClearsImplicitProtocolState(t *testing.T) {
	ctx := setupOpenAIProtocolTestDB(t)
	channel := createOpenAIProtocolTestChannel(t, ctx, "implicit-mode-non-openai", outbound.OutboundTypeOpenAIChat,
		[]model.BaseUrl{{URL: "https://api.example.com/v1"}})
	chatOnly := model.OpenAIProtocolModeChatOnly
	if _, err := ChannelUpdate(&model.ChannelUpdateRequest{ID: channel.ID, OpenAIProtocolMode: &chatOnly}, ctx); err != nil {
		t.Fatalf("set manual OpenAI mode: %v", err)
	}
	anthropic := outbound.OutboundTypeAnthropic
	updated, err := ChannelUpdate(&model.ChannelUpdateRequest{ID: channel.ID, Type: &anthropic}, ctx)
	if err != nil {
		t.Fatalf("change channel type to Anthropic: %v", err)
	}
	if updated.Type != anthropic || updated.OpenAIProtocolMode != model.OpenAIProtocolModeAuto ||
		updated.OpenAIChatCapability != model.OpenAIProtocolCapabilityUnknown ||
		updated.OpenAIResponsesCapability != model.OpenAIProtocolCapabilityUnknown {
		t.Fatalf("non-OpenAI type did not clear implicit protocol state: %#v", updated)
	}
}
