package model

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/bestruirui/octopus/internal/transformer/outbound"
)

type AutoGroupType int

const (
	AutoGroupTypeNone  AutoGroupType = 0 //不自动分组
	AutoGroupTypeFuzzy AutoGroupType = 1 //模糊匹配
	AutoGroupTypeExact AutoGroupType = 2 //准确匹配
	AutoGroupTypeRegex AutoGroupType = 3 //正则匹配
)

func (t AutoGroupType) Valid() bool {
	switch t {
	case AutoGroupTypeNone, AutoGroupTypeFuzzy, AutoGroupTypeExact, AutoGroupTypeRegex:
		return true
	default:
		return false
	}
}

func ParseAutoGroupSettingValue(value string) (AutoGroupType, bool) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", "false":
		return AutoGroupTypeNone, true
	case "true":
		return AutoGroupTypeFuzzy, true
	}
	parsed, err := strconv.Atoi(strings.TrimSpace(value))
	if err != nil {
		return AutoGroupTypeNone, false
	}
	mode := AutoGroupType(parsed)
	return mode, mode.Valid()
}

type ChannelWSMode string

const (
	ChannelWSModeInherit     ChannelWSMode = "inherit"
	ChannelWSModeOff         ChannelWSMode = "off"
	ChannelWSModePassthrough ChannelWSMode = "passthrough"
	ChannelWSModeTransform   ChannelWSMode = "transform"
)

func (m ChannelWSMode) Normalize() ChannelWSMode {
	switch m {
	case ChannelWSModeOff, ChannelWSModePassthrough, ChannelWSModeTransform:
		return m
	default:
		return ChannelWSModeInherit
	}
}

type OpenAIProtocolMode string

const (
	OpenAIProtocolModeAuto          OpenAIProtocolMode = "auto"
	OpenAIProtocolModeChatOnly      OpenAIProtocolMode = "chat_only"
	OpenAIProtocolModeResponsesOnly OpenAIProtocolMode = "responses_only"
	OpenAIProtocolModeBoth          OpenAIProtocolMode = "both"
)

func (m OpenAIProtocolMode) Normalize() OpenAIProtocolMode {
	if m == "" {
		return OpenAIProtocolModeAuto
	}
	return m
}

func (m OpenAIProtocolMode) Valid() bool {
	switch m.Normalize() {
	case OpenAIProtocolModeAuto, OpenAIProtocolModeChatOnly, OpenAIProtocolModeResponsesOnly, OpenAIProtocolModeBoth:
		return true
	default:
		return false
	}
}

type OpenAIProtocolCapability string

const (
	OpenAIProtocolCapabilityUnknown     OpenAIProtocolCapability = "unknown"
	OpenAIProtocolCapabilitySupported   OpenAIProtocolCapability = "supported"
	OpenAIProtocolCapabilityUnsupported OpenAIProtocolCapability = "unsupported"
)

// OpenAIProtocolUnsupportedTTL bounds how long a learned unsupported verdict
// stays authoritative. Runtime learning is monotonic: it never writes unknown
// back, so without an expiry a provider that later adds the endpoint would stay
// pinned at the bottom of the candidate ranking forever and never be retried.
// After the TTL the verdict falls back to unknown, which costs at most one
// exploratory attempt per channel per TTL window.
const OpenAIProtocolUnsupportedTTL = 7 * 24 * time.Hour

func (c OpenAIProtocolCapability) Normalize() OpenAIProtocolCapability {
	if c == "" {
		return OpenAIProtocolCapabilityUnknown
	}
	return c
}

func (c OpenAIProtocolCapability) Valid() bool {
	switch c.Normalize() {
	case OpenAIProtocolCapabilityUnknown, OpenAIProtocolCapabilitySupported, OpenAIProtocolCapabilityUnsupported:
		return true
	default:
		return false
	}
}

func IsOpenAITextChannelType(channelType outbound.OutboundType) bool {
	return channelType == outbound.OutboundTypeOpenAIChat || channelType == outbound.OutboundTypeOpenAIResponse
}

type Channel struct {
	ID                        int                      `json:"id" gorm:"primaryKey"`
	Name                      string                   `json:"name" gorm:"unique;not null"`
	Type                      outbound.OutboundType    `json:"type"`
	Enabled                   bool                     `json:"enabled" gorm:"default:true"`
	BaseUrls                  []BaseUrl                `json:"base_urls" gorm:"serializer:json"`
	Keys                      []ChannelKey             `json:"keys" gorm:"foreignKey:ChannelID"`
	Model                     string                   `json:"model"`
	CustomModel               string                   `json:"custom_model"`
	ProxyMode                 ProxyUsageMode           `json:"proxy_mode" gorm:"type:varchar(16);not null;default:'direct'"`
	ProxyConfigID             *int                     `json:"proxy_config_id"`
	Proxy                     bool                     `json:"-" gorm:"default:false"`
	AutoSync                  bool                     `json:"auto_sync" gorm:"default:false"`
	AutoGroup                 AutoGroupType            `json:"auto_group" gorm:"default:0"`
	CustomHeader              []CustomHeader           `json:"custom_header" gorm:"serializer:json"`
	WSMode                    ChannelWSMode            `json:"ws_mode" gorm:"type:varchar(16);not null;default:'inherit'"`
	OpenAIProtocolMode        OpenAIProtocolMode       `json:"openai_protocol_mode" gorm:"column:openai_protocol_mode;type:varchar(24);not null;default:'auto'"`
	OpenAIChatCapability      OpenAIProtocolCapability `json:"openai_chat_capability" gorm:"column:openai_chat_capability;type:varchar(16);not null;default:'unknown'"`
	OpenAIResponsesCapability OpenAIProtocolCapability `json:"openai_responses_capability" gorm:"column:openai_responses_capability;type:varchar(16);not null;default:'unknown'"`
	// Unix seconds when the matching capability column was last recorded by a
	// runtime observation or a probe. Zero means "never recorded", which the
	// unsupported TTL treats as already expired. Server-owned bookkeeping, so
	// it stays out of the channel JSON contract.
	OpenAIChatCapabilityAt      int64                 `json:"-" gorm:"column:openai_chat_capability_at;not null;default:0"`
	OpenAIResponsesCapabilityAt int64                 `json:"-" gorm:"column:openai_responses_capability_at;not null;default:0"`
	ParamOverride               *string               `json:"param_override"`
	ChannelProxy                *string               `json:"-" gorm:"column:channel_proxy"`
	Stats                       *StatsChannel         `json:"stats,omitempty" gorm:"foreignKey:ChannelID"`
	MatchRegex                  *string               `json:"match_regex"`
	Managed                     bool                  `json:"managed" gorm:"-"`
	ManagedSource               *ManagedChannelSource `json:"managed_source,omitempty" gorm:"-"`
}

func (c *Channel) NormalizeOpenAIProtocolSettings() error {
	if c == nil {
		return fmt.Errorf("channel is nil")
	}
	c.OpenAIProtocolMode = c.OpenAIProtocolMode.Normalize()
	c.OpenAIChatCapability = c.OpenAIChatCapability.Normalize()
	c.OpenAIResponsesCapability = c.OpenAIResponsesCapability.Normalize()
	if !c.OpenAIProtocolMode.Valid() {
		return fmt.Errorf("invalid openai protocol mode: %s", c.OpenAIProtocolMode)
	}
	if !c.OpenAIChatCapability.Valid() {
		return fmt.Errorf("invalid openai chat capability: %s", c.OpenAIChatCapability)
	}
	if !c.OpenAIResponsesCapability.Valid() {
		return fmt.Errorf("invalid openai responses capability: %s", c.OpenAIResponsesCapability)
	}
	if !IsOpenAITextChannelType(c.Type) {
		if c.OpenAIProtocolMode != OpenAIProtocolModeAuto {
			return fmt.Errorf("openai protocol mode is only valid for openai text channels")
		}
		c.ResetOpenAIProtocolCapabilities()
	}
	// A capability column without an observation stamp predates TTL tracking
	// (or was written directly). Treating it as "never recorded" makes a stale
	// unsupported verdict expire on the next evaluation instead of persisting
	// forever, which is the conservative direction: it costs one retry.
	if c.OpenAIChatCapability == OpenAIProtocolCapabilityUnknown {
		c.OpenAIChatCapabilityAt = 0
	}
	if c.OpenAIResponsesCapability == OpenAIProtocolCapabilityUnknown {
		c.OpenAIResponsesCapabilityAt = 0
	}
	return nil
}

func (c *Channel) EffectiveOpenAIProtocolCapability(protocol outbound.OutboundType) OpenAIProtocolCapability {
	if c == nil || !IsOpenAITextChannelType(c.Type) {
		return OpenAIProtocolCapabilityUnsupported
	}
	switch c.OpenAIProtocolMode.Normalize() {
	case OpenAIProtocolModeChatOnly:
		if protocol == outbound.OutboundTypeOpenAIChat {
			return OpenAIProtocolCapabilitySupported
		}
		return OpenAIProtocolCapabilityUnsupported
	case OpenAIProtocolModeResponsesOnly:
		if protocol == outbound.OutboundTypeOpenAIResponse {
			return OpenAIProtocolCapabilitySupported
		}
		return OpenAIProtocolCapabilityUnsupported
	case OpenAIProtocolModeBoth:
		if IsOpenAITextChannelType(protocol) {
			return OpenAIProtocolCapabilitySupported
		}
		return OpenAIProtocolCapabilityUnsupported
	}
	if protocol == outbound.OutboundTypeOpenAIChat {
		return c.OpenAIChatCapability.Normalize()
	}
	if protocol == outbound.OutboundTypeOpenAIResponse {
		return c.OpenAIResponsesCapability.Normalize()
	}
	return OpenAIProtocolCapabilityUnsupported
}

// SetOpenAIProtocolCapability records an observation with the current time as
// its stamp. See SetOpenAIProtocolCapabilityAt for the exact semantics.
func (c *Channel) SetOpenAIProtocolCapability(protocol outbound.OutboundType, capability OpenAIProtocolCapability) bool {
	return c.SetOpenAIProtocolCapabilityAt(protocol, capability, time.Now().Unix())
}

// SetOpenAIProtocolCapabilityAt records an observed capability together with
// the time it was observed. The bool reports whether the capability value
// itself changed, preserving the change-detection contract callers rely on.
// The stamp is refreshed even when the value is unchanged, so a repeated
// unsupported observation renews its TTL instead of letting a stale first
// sighting expire a verdict the upstream just confirmed again.
func (c *Channel) SetOpenAIProtocolCapabilityAt(protocol outbound.OutboundType, capability OpenAIProtocolCapability, at int64) bool {
	if c == nil || !capability.Valid() {
		return false
	}
	capability = capability.Normalize()
	// Unknown is the reset state rather than an observation, so it carries no
	// stamp: a zero timestamp keeps "never recorded" distinguishable.
	stamp := at
	if capability == OpenAIProtocolCapabilityUnknown {
		stamp = 0
	}
	switch protocol {
	case outbound.OutboundTypeOpenAIChat:
		changed := c.OpenAIChatCapability.Normalize() != capability
		c.OpenAIChatCapability = capability
		c.OpenAIChatCapabilityAt = stamp
		return changed
	case outbound.OutboundTypeOpenAIResponse:
		changed := c.OpenAIResponsesCapability.Normalize() != capability
		c.OpenAIResponsesCapability = capability
		c.OpenAIResponsesCapabilityAt = stamp
		return changed
	default:
		return false
	}
}

// OpenAIProtocolCapabilityAt returns the unix second stamp of the stored
// capability observation for one protocol. Zero means never recorded.
func (c *Channel) OpenAIProtocolCapabilityAt(protocol outbound.OutboundType) int64 {
	if c == nil {
		return 0
	}
	if protocol == outbound.OutboundTypeOpenAIResponse {
		return c.OpenAIResponsesCapabilityAt
	}
	return c.OpenAIChatCapabilityAt
}

// OpenAIProtocolUnsupportedExpired reports whether a learned unsupported
// verdict for one protocol has outlived ttl and should fall back to unknown.
// Only auto mode is subject to expiry: a manual selection is the operator's
// decision and never ages out. A non-positive ttl disables expiry entirely.
func (c *Channel) OpenAIProtocolUnsupportedExpired(protocol outbound.OutboundType, ttl time.Duration, now time.Time) bool {
	if c == nil || ttl <= 0 || !IsOpenAITextChannelType(c.Type) ||
		c.OpenAIProtocolMode.Normalize() != OpenAIProtocolModeAuto ||
		!IsOpenAITextChannelType(protocol) {
		return false
	}
	if c.storedOpenAIProtocolCapability(protocol) != OpenAIProtocolCapabilityUnsupported {
		return false
	}
	return c.OpenAIProtocolCapabilityAt(protocol) <= now.Add(-ttl).Unix()
}

// storedOpenAIProtocolCapability reads the raw capability column without
// applying the manual-mode overrides of EffectiveOpenAIProtocolCapability.
func (c *Channel) storedOpenAIProtocolCapability(protocol outbound.OutboundType) OpenAIProtocolCapability {
	if c == nil {
		return OpenAIProtocolCapabilityUnknown
	}
	if protocol == outbound.OutboundTypeOpenAIResponse {
		return c.OpenAIResponsesCapability.Normalize()
	}
	return c.OpenAIChatCapability.Normalize()
}

func (c *Channel) ResetOpenAIProtocolCapabilities() {
	if c == nil {
		return
	}
	c.OpenAIChatCapability = OpenAIProtocolCapabilityUnknown
	c.OpenAIResponsesCapability = OpenAIProtocolCapabilityUnknown
	c.OpenAIChatCapabilityAt = 0
	c.OpenAIResponsesCapabilityAt = 0
}

func (c *Channel) ForceOpenAIChatOnly() {
	if c == nil {
		return
	}
	now := time.Now().Unix()
	c.Type = outbound.OutboundTypeOpenAIChat
	c.OpenAIProtocolMode = OpenAIProtocolModeChatOnly
	c.OpenAIChatCapability = OpenAIProtocolCapabilitySupported
	c.OpenAIResponsesCapability = OpenAIProtocolCapabilityUnsupported
	c.OpenAIChatCapabilityAt = now
	c.OpenAIResponsesCapabilityAt = now
}

func (c *Channel) UnmarshalJSON(data []byte) error {
	type alias Channel
	aux := struct {
		*alias
		Proxy        *bool   `json:"proxy"`
		ChannelProxy *string `json:"channel_proxy"`
	}{alias: (*alias)(c)}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}
	if aux.Proxy != nil {
		c.Proxy = *aux.Proxy
	}
	if aux.ChannelProxy != nil {
		c.ChannelProxy = aux.ChannelProxy
	}
	return nil
}

type ManagedChannelSource struct {
	SiteID          int    `json:"site_id"`
	SiteAccountID   int    `json:"site_account_id"`
	SiteUserGroupID *int   `json:"site_user_group_id,omitempty"`
	GroupKey        string `json:"group_key"`
}

type BaseUrl struct {
	URL   string `json:"url"`
	Delay int    `json:"delay"`
}

type CustomHeader struct {
	HeaderKey   string `json:"header_key"`
	HeaderValue string `json:"header_value"`
}

type ChannelKey struct {
	ID               int     `json:"id" gorm:"primaryKey"`
	ChannelID        int     `json:"channel_id"`
	Enabled          bool    `json:"enabled" gorm:"default:true"`
	ChannelKey       string  `json:"channel_key"`
	StatusCode       int     `json:"status_code"`
	LastUseTimeStamp int64   `json:"last_use_time_stamp"`
	TotalCost        float64 `json:"total_cost"`
	Remark           string  `json:"remark"`
}

type ChannelKeySelectOptions struct {
	ExcludeKeyIDs  map[int]struct{}
	PreferredKeyID int
}

// ChannelUpdateRequest 渠道更新请求 - 仅包含变更的数据
type ChannelUpdateRequest struct {
	ID                 int                    `json:"id" binding:"required"`
	Name               *string                `json:"name,omitempty"`
	Type               *outbound.OutboundType `json:"type,omitempty"`
	Enabled            *bool                  `json:"enabled,omitempty"`
	BaseUrls           *[]BaseUrl             `json:"base_urls,omitempty"`
	Model              *string                `json:"model,omitempty"`
	CustomModel        *string                `json:"custom_model,omitempty"`
	ProxyMode          *ProxyUsageMode        `json:"proxy_mode,omitempty"`
	ProxyConfigID      *int                   `json:"proxy_config_id,omitempty"`
	Proxy              *bool                  `json:"-"`
	AutoSync           *bool                  `json:"auto_sync,omitempty"`
	AutoGroup          *AutoGroupType         `json:"auto_group,omitempty"`
	CustomHeader       *[]CustomHeader        `json:"custom_header,omitempty"`
	WSMode             *ChannelWSMode         `json:"ws_mode,omitempty"`
	OpenAIProtocolMode *OpenAIProtocolMode    `json:"openai_protocol_mode,omitempty"`
	ChannelProxy       *string                `json:"-"`
	ParamOverride      *string                `json:"param_override,omitempty"`
	MatchRegex         *string                `json:"match_regex,omitempty"`

	KeysToAdd    []ChannelKeyAddRequest    `json:"keys_to_add,omitempty"`
	KeysToUpdate []ChannelKeyUpdateRequest `json:"keys_to_update,omitempty"`
	KeysToDelete []int                     `json:"keys_to_delete,omitempty"`

	BypassManagedCheck bool `json:"-"` // 内部使用：允许投影逻辑更新 managed channel
}

type ChannelKeyAddRequest struct {
	Enabled    bool   `json:"enabled"`
	ChannelKey string `json:"channel_key" binding:"required"`
	Remark     string `json:"remark"`
}

type ChannelKeyUpdateRequest struct {
	ID         int     `json:"id" binding:"required"`
	Enabled    *bool   `json:"enabled,omitempty"`
	ChannelKey *string `json:"channel_key,omitempty"`
	Remark     *string `json:"remark,omitempty"`
}

// ChannelFetchModelRequest is used by /channel/fetch-model (not persisted).
type ChannelFetchModelRequest struct {
	Type          outbound.OutboundType `json:"type" binding:"required"`
	BaseURL       string                `json:"base_url" binding:"required"`
	Key           string                `json:"key" binding:"required"`
	ProxyMode     ProxyUsageMode        `json:"proxy_mode"`
	ProxyConfigID *int                  `json:"proxy_config_id"`
}

func (c *Channel) GetBaseUrl() string {
	if c == nil || len(c.BaseUrls) == 0 {
		return ""
	}

	bestURL := ""
	bestDelay := 0
	bestSet := false

	for _, bu := range c.BaseUrls {
		if bu.URL == "" {
			continue
		}
		if !bestSet || bu.Delay < bestDelay {
			bestURL = bu.URL
			bestDelay = bu.Delay
			bestSet = true
		}
	}

	return bestURL
}

func (c *Channel) GetChannelKey(opts ...ChannelKeySelectOptions) ChannelKey {
	if c == nil || len(c.Keys) == 0 {
		return ChannelKey{}
	}

	var selectOpts ChannelKeySelectOptions
	if len(opts) > 0 {
		selectOpts = opts[0]
	}

	if selectOpts.PreferredKeyID > 0 {
		for _, k := range c.Keys {
			if k.ID != selectOpts.PreferredKeyID || !k.Enabled || k.ChannelKey == "" {
				continue
			}
			if _, excluded := selectOpts.ExcludeKeyIDs[k.ID]; excluded {
				break
			}
			return k
		}
	}

	best := ChannelKey{}
	bestCost := 0.0
	bestSet := false

	for _, k := range c.Keys {
		if !k.Enabled || k.ChannelKey == "" {
			continue
		}
		if _, excluded := selectOpts.ExcludeKeyIDs[k.ID]; excluded {
			continue
		}
		if !bestSet || k.TotalCost < bestCost {
			best = k
			bestCost = k.TotalCost
			bestSet = true
		}
	}

	if !bestSet {
		return ChannelKey{}
	}
	return best
}
