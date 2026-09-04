package model

type SiteChannelCard struct {
	SiteID       int                  `json:"site_id"`
	SiteName     string               `json:"site_name"`
	BaseURL      string               `json:"base_url"`
	Platform     SitePlatform         `json:"platform"`
	Enabled      bool                 `json:"enabled"`
	AccountCount int                  `json:"account_count"`
	Accounts     []SiteChannelAccount `json:"accounts"`
}

type SiteChannelAccount struct {
	SiteID         int                `json:"site_id"`
	AccountID      int                `json:"account_id"`
	AccountName    string             `json:"account_name"`
	Enabled        bool               `json:"enabled"`
	AutoSync       bool               `json:"auto_sync"`
	GroupCount     int                `json:"group_count"`
	ModelCount     int                `json:"model_count"`
	Groups         []SiteChannelGroup `json:"groups"`
	RouteSummaries []SiteRouteSummary `json:"route_summaries"`
}

type SiteRouteSummary struct {
	RouteType SiteModelRouteType `json:"route_type"`
	Count     int                `json:"count"`
}

type SiteChannelGroup struct {
	GroupKey                string                         `json:"group_key"`
	GroupName               string                         `json:"group_name"`
	ProjectionDisabled      bool                           `json:"projection_disabled"`
	ChannelDisabled         bool                           `json:"channel_disabled"`
	ProjectionSuspended     bool                           `json:"projection_suspended"`
	ProjectionSuspendReason string                         `json:"projection_suspend_reason,omitempty"`
	ProjectionSuspendedAt   *int64                         `json:"projection_suspended_at,omitempty"`
	ModelSyncStatus         SiteGroupModelSyncStatus       `json:"model_sync_status"`
	ModelSyncMessage        string                         `json:"model_sync_message,omitempty"`
	ModelSyncAuthoritative  bool                           `json:"model_sync_authoritative"`
	ModelSyncModelCount     int                            `json:"model_sync_model_count"`
	LastModelSyncAt         *int64                         `json:"last_model_sync_at,omitempty"`
	LastModelSyncSuccessAt  *int64                         `json:"last_model_sync_success_at,omitempty"`
	ModelSyncFailureCount   int                            `json:"model_sync_failure_count"`
	KeyCount                int                            `json:"key_count"`
	EnabledKeyCount         int                            `json:"enabled_key_count"`
	MaskedPendingKeyCount   int                            `json:"masked_pending_key_count"`
	HasKeys                 bool                           `json:"has_keys"`
	HasProjectedChannel     bool                           `json:"has_projected_channel"`
	ProjectedChannelIDs     []int                          `json:"projected_channel_ids"`
	ProjectedChannels       []SiteProjectedChannelSettings `json:"projected_channels,omitempty"`
	SourceKeys              []SiteSourceKey                `json:"source_keys,omitempty"`
	ProjectedKeys           []SiteProjectedKey             `json:"projected_keys,omitempty"`
	Models                  []SiteChannelModel             `json:"models"`
}

type SiteSourceKey struct {
	ID          int                  `json:"id"`
	Enabled     bool                 `json:"enabled"`
	Token       string               `json:"token"`
	TokenMasked string               `json:"token_masked"`
	Name        string               `json:"name"`
	GroupKey    string               `json:"group_key"`
	GroupName   string               `json:"group_name"`
	ValueStatus SiteTokenValueStatus `json:"value_status"`
	LastSyncAt  *int64               `json:"last_sync_at,omitempty"`
}

type SiteProjectedChannelSettings struct {
	ChannelID      int                `json:"channel_id"`
	ChannelName    string             `json:"channel_name"`
	RouteType      SiteModelRouteType `json:"route_type"`
	AutoGroup      AutoGroupType      `json:"auto_group"`
	EffectiveGroup AutoGroupType      `json:"effective_auto_group"`
	ParamOverride  string             `json:"param_override"`
	GlobalOverride bool               `json:"global_override"`
}

type SiteProjectedKey struct {
	ID               int     `json:"id"`
	ChannelID        int     `json:"channel_id"`
	ChannelName      string  `json:"channel_name"`
	Enabled          bool    `json:"enabled"`
	ChannelKey       string  `json:"channel_key"`
	ChannelKeyMasked string  `json:"channel_key_masked"`
	Remark           string  `json:"remark"`
	StatusCode       int     `json:"status_code"`
	LastUseTimeStamp int64   `json:"last_use_time_stamp"`
	TotalCost        float64 `json:"total_cost"`
}

type SiteChannelModel struct {
	ModelName          string                   `json:"model_name"`
	Source             string                   `json:"source"`
	RouteType          SiteModelRouteType       `json:"route_type"`
	RouteSource        SiteModelRouteSource     `json:"route_source"`
	ManualOverride     bool                     `json:"manual_override"`
	Disabled           bool                     `json:"disabled"`
	ProjectedChannelID *int                     `json:"projected_channel_id,omitempty"`
	RouteMetadata      *SiteModelRouteMetadata  `json:"route_metadata,omitempty"`
	History            *SiteModelHistorySummary `json:"history,omitempty"`
	// DisableProtocolFallback 回显操作者的协议勾选：false 表示两个 OpenAI
	// 文本协议都允许（真实能力交给运行时探测），true 表示只允许 RouteType
	// 指定的那一个。
	DisableProtocolFallback bool `json:"disable_protocol_fallback"`
	// OpenAIChatCapability / OpenAIResponsesCapability 是投影渠道当前学到的
	// 真实协议能力，供前端把勾选渲染成"已验证支持 / 已验证不支持 / 未知"。
	// 没有投影渠道时为空字符串。
	OpenAIChatCapability      OpenAIProtocolCapability `json:"openai_chat_capability,omitempty"`
	OpenAIResponsesCapability OpenAIProtocolCapability `json:"openai_responses_capability,omitempty"`
}

type SiteModelHistorySummary struct {
	SuccessCount  int                      `json:"success_count"`
	FailureCount  int                      `json:"failure_count"`
	LastRequestAt *int64                   `json:"last_request_at,omitempty"`
	BucketSpan    int                      `json:"bucket_span,omitempty"` // 桶宽（秒），0 表示无数据
	Buckets       []SiteModelHistoryBucket `json:"buckets,omitempty"`
}

type SiteModelHistoryBucket struct {
	Time    int64 `json:"time"`
	Success int   `json:"success"`
	Failure int   `json:"failure"`
}

type SiteModelRouteUpdateRequest struct {
	GroupKey        string             `json:"group_key" binding:"required"`
	ModelName       string             `json:"model_name" binding:"required"`
	RouteType       SiteModelRouteType `json:"route_type" binding:"required"`
	RouteRawPayload string             `json:"route_raw_payload,omitempty"`
	// DisableProtocolFallback 对应 UI 上的协议勾选：两个协议都勾选时为 nil
	// 或 false（允许在 Chat/Responses 之间降级，真实能力交给运行时探测），
	// 只勾一个时为 true（把投影渠道锁定到 RouteType 指定的单协议）。
	// 指针类型让"未携带该字段"的旧客户端保持原值而不是被静默重置。
	DisableProtocolFallback *bool `json:"disable_protocol_fallback,omitempty"`
}

type SiteModelDisableUpdateRequest struct {
	GroupKey  string `json:"group_key" binding:"required"`
	ModelName string `json:"model_name" binding:"required"`
	Disabled  bool   `json:"disabled"`
}

type SiteProjectedChannelSettingsUpdateRequest struct {
	ChannelID     int           `json:"channel_id" binding:"required"`
	AutoGroup     AutoGroupType `json:"auto_group"`
	ParamOverride string        `json:"param_override"`
}

type SiteManualModelAddRequest struct {
	GroupKey string                    `json:"group_key" binding:"required"`
	Models   []SiteManualModelAddEntry `json:"models" binding:"required"`
}

type SiteManualModelAddEntry struct {
	ModelName string             `json:"model_name" binding:"required"`
	RouteType SiteModelRouteType `json:"route_type" binding:"required"`
}

type SiteManualModelDeleteRequest struct {
	GroupKey  string `json:"group_key" binding:"required"`
	ModelName string `json:"model_name" binding:"required"`
}

type SiteChannelKeyCreateRequest struct {
	GroupKey string `json:"group_key" binding:"required"`
	Name     string `json:"name,omitempty"`
}

type SiteProjectedKeyAddRequest struct {
	Enabled    bool   `json:"enabled"`
	ChannelKey string `json:"channel_key" binding:"required"`
	Remark     string `json:"remark,omitempty"`
}

type SiteProjectedKeyUpdateItem struct {
	ID         int     `json:"id" binding:"required"`
	Enabled    *bool   `json:"enabled,omitempty"`
	ChannelKey *string `json:"channel_key,omitempty"`
	Remark     *string `json:"remark,omitempty"`
}

type SiteProjectedKeyUpdateRequest struct {
	GroupKey     string                       `json:"group_key" binding:"required"`
	KeysToAdd    []SiteProjectedKeyAddRequest `json:"keys_to_add,omitempty"`
	KeysToUpdate []SiteProjectedKeyUpdateItem `json:"keys_to_update,omitempty"`
	KeysToDelete []int                        `json:"keys_to_delete,omitempty"`
}

type SiteSourceKeyAddRequest struct {
	Enabled bool   `json:"enabled"`
	Token   string `json:"token" binding:"required"`
	Name    string `json:"name,omitempty"`
}

type SiteSourceKeyUpdateItem struct {
	ID      int     `json:"id" binding:"required"`
	Enabled *bool   `json:"enabled,omitempty"`
	Token   *string `json:"token,omitempty"`
	Name    *string `json:"name,omitempty"`
}

type SiteSourceKeyUpdateRequest struct {
	GroupKey     string                    `json:"group_key" binding:"required"`
	KeysToAdd    []SiteSourceKeyAddRequest `json:"keys_to_add,omitempty"`
	KeysToUpdate []SiteSourceKeyUpdateItem `json:"keys_to_update,omitempty"`
	KeysToDelete []int                     `json:"keys_to_delete,omitempty"`
}

type SiteGroupProjectionUpdateRequest struct {
	GroupKey           string `json:"group_key" binding:"required"`
	ProjectionDisabled bool   `json:"projection_disabled"`
}

type SiteGroupChannelUpdateRequest struct {
	GroupKey        string `json:"group_key" binding:"required"`
	ChannelDisabled bool   `json:"channel_disabled"`
}
