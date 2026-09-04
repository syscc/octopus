package model

import (
	"strings"
	"testing"

	"github.com/bestruirui/octopus/internal/transformer/outbound"
)

func TestOpenAIProtocolModeNormalizeAndValid(t *testing.T) {
	if got := (OpenAIProtocolMode("")).Normalize(); got != OpenAIProtocolModeAuto {
		t.Fatalf(`expected empty mode to normalize to %q, got %q`, OpenAIProtocolModeAuto, got)
	}
	for _, mode := range []OpenAIProtocolMode{
		OpenAIProtocolModeAuto,
		OpenAIProtocolModeChatOnly,
		OpenAIProtocolModeResponsesOnly,
		OpenAIProtocolModeBoth,
	} {
		if !mode.Valid() {
			t.Fatalf("expected mode %q to be valid", mode)
		}
		if got := mode.Normalize(); got != mode {
			t.Fatalf("expected mode %q to normalize to itself, got %q", mode, got)
		}
	}
	for _, mode := range []OpenAIProtocolMode{"bogus", "AUTO", "chat", "responses-only", "both "} {
		if mode.Valid() {
			t.Fatalf("expected mode %q to be invalid", mode)
		}
		// Normalize only maps the empty string; other values pass through so
		// that NormalizeOpenAIProtocolSettings can reject them.
		if got := mode.Normalize(); got != mode {
			t.Fatalf("expected invalid mode %q to normalize to itself, got %q", mode, got)
		}
	}
}

func TestOpenAIProtocolCapabilityNormalizeAndValid(t *testing.T) {
	if got := (OpenAIProtocolCapability("")).Normalize(); got != OpenAIProtocolCapabilityUnknown {
		t.Fatalf(`expected empty capability to normalize to %q, got %q`, OpenAIProtocolCapabilityUnknown, got)
	}
	for _, capability := range []OpenAIProtocolCapability{
		OpenAIProtocolCapabilityUnknown,
		OpenAIProtocolCapabilitySupported,
		OpenAIProtocolCapabilityUnsupported,
	} {
		if !capability.Valid() {
			t.Fatalf("expected capability %q to be valid", capability)
		}
		if got := capability.Normalize(); got != capability {
			t.Fatalf("expected capability %q to normalize to itself, got %q", capability, got)
		}
	}
	for _, capability := range []OpenAIProtocolCapability{"bogus", "SUPPORTED", "maybe"} {
		if capability.Valid() {
			t.Fatalf("expected capability %q to be invalid", capability)
		}
	}
}

// TestEffectiveOpenAIProtocolCapabilityAcrossModes verifies the effective
// capability matrix for every OpenAIProtocolMode, the distinct unknown state,
// and non-OpenAI channel types.
func TestEffectiveOpenAIProtocolCapabilityAcrossModes(t *testing.T) {
	chat := outbound.OutboundTypeOpenAIChat
	responses := outbound.OutboundTypeOpenAIResponse
	other := outbound.OutboundTypeAnthropic

	newChannel := func(channelType outbound.OutboundType, mode OpenAIProtocolMode, chatCap, responsesCap OpenAIProtocolCapability) *Channel {
		return &Channel{
			Type:                      channelType,
			OpenAIProtocolMode:        mode,
			OpenAIChatCapability:      chatCap,
			OpenAIResponsesCapability: responsesCap,
		}
	}

	tests := []struct {
		name          string
		channel       *Channel
		wantChat      OpenAIProtocolCapability
		wantResponses OpenAIProtocolCapability
		wantOther     OpenAIProtocolCapability
	}{
		{
			name:          "auto unknown keeps distinct unknown state",
			channel:       newChannel(chat, OpenAIProtocolModeAuto, OpenAIProtocolCapabilityUnknown, OpenAIProtocolCapabilityUnknown),
			wantChat:      OpenAIProtocolCapabilityUnknown,
			wantResponses: OpenAIProtocolCapabilityUnknown,
			wantOther:     OpenAIProtocolCapabilityUnsupported,
		},
		{
			name:          "auto falls back to learned columns per protocol",
			channel:       newChannel(chat, OpenAIProtocolModeAuto, OpenAIProtocolCapabilitySupported, OpenAIProtocolCapabilityUnsupported),
			wantChat:      OpenAIProtocolCapabilitySupported,
			wantResponses: OpenAIProtocolCapabilityUnsupported,
			wantOther:     OpenAIProtocolCapabilityUnsupported,
		},
		{
			name:          "auto with responses type reads responses column for chat protocol too",
			channel:       newChannel(responses, OpenAIProtocolModeAuto, OpenAIProtocolCapabilityUnsupported, OpenAIProtocolCapabilitySupported),
			wantChat:      OpenAIProtocolCapabilityUnsupported,
			wantResponses: OpenAIProtocolCapabilitySupported,
			wantOther:     OpenAIProtocolCapabilityUnsupported,
		},
		{
			name:          "chat_only mode is authoritative over learned columns",
			channel:       newChannel(chat, OpenAIProtocolModeChatOnly, OpenAIProtocolCapabilityUnsupported, OpenAIProtocolCapabilitySupported),
			wantChat:      OpenAIProtocolCapabilitySupported,
			wantResponses: OpenAIProtocolCapabilityUnsupported,
			wantOther:     OpenAIProtocolCapabilityUnsupported,
		},
		{
			name:          "responses_only mode is authoritative over learned columns",
			channel:       newChannel(responses, OpenAIProtocolModeResponsesOnly, OpenAIProtocolCapabilitySupported, OpenAIProtocolCapabilityUnsupported),
			wantChat:      OpenAIProtocolCapabilityUnsupported,
			wantResponses: OpenAIProtocolCapabilitySupported,
			wantOther:     OpenAIProtocolCapabilityUnsupported,
		},
		{
			name:          "both mode supports every openai text protocol",
			channel:       newChannel(chat, OpenAIProtocolModeBoth, OpenAIProtocolCapabilityUnsupported, OpenAIProtocolCapabilityUnsupported),
			wantChat:      OpenAIProtocolCapabilitySupported,
			wantResponses: OpenAIProtocolCapabilitySupported,
			wantOther:     OpenAIProtocolCapabilityUnsupported,
		},
		{
			name:          "non openai channel type is always unsupported",
			channel:       newChannel(other, OpenAIProtocolModeBoth, OpenAIProtocolCapabilitySupported, OpenAIProtocolCapabilitySupported),
			wantChat:      OpenAIProtocolCapabilityUnsupported,
			wantResponses: OpenAIProtocolCapabilityUnsupported,
			wantOther:     OpenAIProtocolCapabilityUnsupported,
		},
		{
			name:          "embedding channel type is always unsupported",
			channel:       newChannel(outbound.OutboundTypeOpenAIEmbedding, OpenAIProtocolModeBoth, OpenAIProtocolCapabilitySupported, OpenAIProtocolCapabilitySupported),
			wantChat:      OpenAIProtocolCapabilityUnsupported,
			wantResponses: OpenAIProtocolCapabilityUnsupported,
			wantOther:     OpenAIProtocolCapabilityUnsupported,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.channel.EffectiveOpenAIProtocolCapability(chat); got != tt.wantChat {
				t.Fatalf("chat effective capability: expected %q, got %q", tt.wantChat, got)
			}
			if got := tt.channel.EffectiveOpenAIProtocolCapability(responses); got != tt.wantResponses {
				t.Fatalf("responses effective capability: expected %q, got %q", tt.wantResponses, got)
			}
			if got := tt.channel.EffectiveOpenAIProtocolCapability(other); got != tt.wantOther {
				t.Fatalf("anthropic effective capability: expected %q, got %q", tt.wantOther, got)
			}
		})
	}

	var nilChannel *Channel
	if got := nilChannel.EffectiveOpenAIProtocolCapability(chat); got != OpenAIProtocolCapabilityUnsupported {
		t.Fatalf("expected nil channel chat capability %q, got %q", OpenAIProtocolCapabilityUnsupported, got)
	}
	if got := nilChannel.EffectiveOpenAIProtocolCapability(responses); got != OpenAIProtocolCapabilityUnsupported {
		t.Fatalf("expected nil channel responses capability %q, got %q", OpenAIProtocolCapabilityUnsupported, got)
	}
}

func TestSetOpenAIProtocolCapabilityChangeDetectionAndGuards(t *testing.T) {
	channel := &Channel{Type: outbound.OutboundTypeOpenAIChat}

	if !channel.SetOpenAIProtocolCapability(outbound.OutboundTypeOpenAIChat, OpenAIProtocolCapabilitySupported) {
		t.Fatalf("expected chat capability change from unknown to supported to report true")
	}
	if channel.OpenAIChatCapability != OpenAIProtocolCapabilitySupported {
		t.Fatalf("expected chat capability %q, got %q", OpenAIProtocolCapabilitySupported, channel.OpenAIChatCapability)
	}
	if channel.OpenAIResponsesCapability != OpenAIProtocolCapability("") {
		t.Fatalf("expected responses capability to stay untouched, got %q", channel.OpenAIResponsesCapability)
	}
	if channel.SetOpenAIProtocolCapability(outbound.OutboundTypeOpenAIChat, OpenAIProtocolCapabilitySupported) {
		t.Fatalf("expected re-recording the same chat capability to report false")
	}
	if channel.SetOpenAIProtocolCapability(outbound.OutboundTypeOpenAIChat, OpenAIProtocolCapability("bogus")) {
		t.Fatalf("expected invalid capability to be rejected")
	}
	if channel.OpenAIChatCapability != OpenAIProtocolCapabilitySupported {
		t.Fatalf("expected rejected capability update to leave chat column unchanged, got %q", channel.OpenAIChatCapability)
	}
	if channel.SetOpenAIProtocolCapability(outbound.OutboundTypeAnthropic, OpenAIProtocolCapabilitySupported) {
		t.Fatalf("expected non openai protocol to be rejected")
	}
	// Empty capability normalizes to unknown, which already matches the zero
	// value of the responses column, so this must report "no change".
	if channel.SetOpenAIProtocolCapability(outbound.OutboundTypeOpenAIResponse, OpenAIProtocolCapability("")) {
		t.Fatalf("expected empty capability to normalize to unknown and report false")
	}
	if !channel.SetOpenAIProtocolCapability(outbound.OutboundTypeOpenAIResponse, OpenAIProtocolCapabilityUnsupported) {
		t.Fatalf("expected responses capability change to report true")
	}

	channel.ResetOpenAIProtocolCapabilities()
	if channel.OpenAIChatCapability != OpenAIProtocolCapabilityUnknown || channel.OpenAIResponsesCapability != OpenAIProtocolCapabilityUnknown {
		t.Fatalf("expected reset to clear both columns, got chat=%q responses=%q", channel.OpenAIChatCapability, channel.OpenAIResponsesCapability)
	}

	var nilChannel *Channel
	if nilChannel.SetOpenAIProtocolCapability(outbound.OutboundTypeOpenAIChat, OpenAIProtocolCapabilitySupported) {
		t.Fatalf("expected nil channel capability update to be rejected")
	}
	nilChannel.ResetOpenAIProtocolCapabilities()
}

func TestNormalizeOpenAIProtocolSettings(t *testing.T) {
	var nilChannel *Channel
	if err := nilChannel.NormalizeOpenAIProtocolSettings(); err == nil {
		t.Fatalf("expected nil channel to return an error")
	}

	channel := &Channel{Type: outbound.OutboundTypeOpenAIChat}
	if err := channel.NormalizeOpenAIProtocolSettings(); err != nil {
		t.Fatalf("expected defaults on openai channel to normalize cleanly, got %v", err)
	}
	if channel.OpenAIProtocolMode != OpenAIProtocolModeAuto {
		t.Fatalf("expected empty mode to normalize to auto, got %q", channel.OpenAIProtocolMode)
	}
	if channel.OpenAIChatCapability != OpenAIProtocolCapabilityUnknown || channel.OpenAIResponsesCapability != OpenAIProtocolCapabilityUnknown {
		t.Fatalf("expected empty capabilities to normalize to unknown, got chat=%q responses=%q", channel.OpenAIChatCapability, channel.OpenAIResponsesCapability)
	}

	tests := []struct {
		name       string
		channel    *Channel
		errPattern string
	}{
		{
			name:       "invalid mode",
			channel:    &Channel{Type: outbound.OutboundTypeOpenAIChat, OpenAIProtocolMode: "bogus"},
			errPattern: "invalid openai protocol mode",
		},
		{
			name:       "invalid chat capability",
			channel:    &Channel{Type: outbound.OutboundTypeOpenAIChat, OpenAIChatCapability: "bogus"},
			errPattern: "invalid openai chat capability",
		},
		{
			name:       "invalid responses capability",
			channel:    &Channel{Type: outbound.OutboundTypeOpenAIChat, OpenAIResponsesCapability: "bogus"},
			errPattern: "invalid openai responses capability",
		},
		{
			name:       "non openai channel with manual mode",
			channel:    &Channel{Type: outbound.OutboundTypeAnthropic, OpenAIProtocolMode: OpenAIProtocolModeChatOnly},
			errPattern: "only valid for openai text channels",
		},
		{
			name:       "embedding channel with manual mode",
			channel:    &Channel{Type: outbound.OutboundTypeOpenAIEmbedding, OpenAIProtocolMode: OpenAIProtocolModeBoth},
			errPattern: "only valid for openai text channels",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.channel.NormalizeOpenAIProtocolSettings()
			if err == nil || !strings.Contains(err.Error(), tt.errPattern) {
				t.Fatalf("expected error containing %q, got %v", tt.errPattern, err)
			}
		})
	}

	// Non-OpenAI channel in auto mode keeps working and drops learned columns.
	anthropic := &Channel{
		Type:                      outbound.OutboundTypeAnthropic,
		OpenAIProtocolMode:        OpenAIProtocolModeAuto,
		OpenAIChatCapability:      OpenAIProtocolCapabilitySupported,
		OpenAIResponsesCapability: OpenAIProtocolCapabilityUnsupported,
	}
	if err := anthropic.NormalizeOpenAIProtocolSettings(); err != nil {
		t.Fatalf("expected non openai auto channel to normalize cleanly, got %v", err)
	}
	if anthropic.OpenAIChatCapability != OpenAIProtocolCapabilityUnknown || anthropic.OpenAIResponsesCapability != OpenAIProtocolCapabilityUnknown {
		t.Fatalf("expected capabilities of non openai channel to reset to unknown, got chat=%q responses=%q", anthropic.OpenAIChatCapability, anthropic.OpenAIResponsesCapability)
	}

	// OpenAI channel keeps an explicit manual mode.
	responses := &Channel{
		Type:                      outbound.OutboundTypeOpenAIResponse,
		OpenAIProtocolMode:        OpenAIProtocolModeBoth,
		OpenAIChatCapability:      OpenAIProtocolCapabilitySupported,
		OpenAIResponsesCapability: OpenAIProtocolCapabilitySupported,
	}
	if err := responses.NormalizeOpenAIProtocolSettings(); err != nil {
		t.Fatalf("expected openai manual mode to be retained, got %v", err)
	}
	if responses.OpenAIProtocolMode != OpenAIProtocolModeBoth {
		t.Fatalf("expected manual mode to be retained, got %q", responses.OpenAIProtocolMode)
	}
}

func TestForceOpenAIChatOnly(t *testing.T) {
	channel := &Channel{
		Type:                      outbound.OutboundTypeOpenAIResponse,
		OpenAIProtocolMode:        OpenAIProtocolModeResponsesOnly,
		OpenAIChatCapability:      OpenAIProtocolCapabilityUnsupported,
		OpenAIResponsesCapability: OpenAIProtocolCapabilitySupported,
	}
	channel.ForceOpenAIChatOnly()

	if channel.Type != outbound.OutboundTypeOpenAIChat {
		t.Fatalf("expected forced type %q, got %q", outbound.OutboundTypeOpenAIChat, channel.Type)
	}
	if channel.OpenAIProtocolMode != OpenAIProtocolModeChatOnly {
		t.Fatalf("expected forced mode %q, got %q", OpenAIProtocolModeChatOnly, channel.OpenAIProtocolMode)
	}
	if channel.OpenAIChatCapability != OpenAIProtocolCapabilitySupported {
		t.Fatalf("expected chat capability %q, got %q", OpenAIProtocolCapabilitySupported, channel.OpenAIChatCapability)
	}
	if channel.OpenAIResponsesCapability != OpenAIProtocolCapabilityUnsupported {
		t.Fatalf("expected responses capability %q, got %q", OpenAIProtocolCapabilityUnsupported, channel.OpenAIResponsesCapability)
	}
	if got := channel.EffectiveOpenAIProtocolCapability(outbound.OutboundTypeOpenAIResponse); got != OpenAIProtocolCapabilityUnsupported {
		t.Fatalf("expected responses to be unsupported after force, got %q", got)
	}
	if got := channel.EffectiveOpenAIProtocolCapability(outbound.OutboundTypeOpenAIChat); got != OpenAIProtocolCapabilitySupported {
		t.Fatalf("expected chat to be supported after force, got %q", got)
	}

	var nilChannel *Channel
	nilChannel.ForceOpenAIChatOnly()
}
