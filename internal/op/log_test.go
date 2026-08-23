package op

import (
	"errors"
	"slices"
	"testing"
	"time"

	dbpkg "github.com/bestruirui/octopus/internal/db"
	"github.com/bestruirui/octopus/internal/model"
)

func resetRelayLogStateForTest() {
	relayLogPendingLock.Lock()
	relayLogPending = make([]model.RelayLog, 0, relayLogBatchSize)
	relayLogPendingBytes = 0
	relayLogPendingLock.Unlock()

	relayLogRecentLock.Lock()
	relayLogRecent = make([]model.RelayLog, 0, relayLogRecentMaxSize)
	relayLogRecentLock.Unlock()

	relayLogDroppedTotal.Store(0)
	relayLogLastDropWarn.Store(0)
	for {
		select {
		case <-relayLogFlushSignal:
		default:
			return
		}
	}
}

func TestRelayLogAddQueuesWithoutDBWrite(t *testing.T) {
	ctx := setupSiteOpTestDB(t)
	if err := settingRefreshCache(ctx); err != nil {
		t.Fatalf("settingRefreshCache failed: %v", err)
	}
	resetRelayLogStateForTest()

	logCount := relayLogBatchSize + 5
	for i := 0; i < logCount; i++ {
		if err := RelayLogAdd(ctx, model.RelayLog{Time: time.Now().Unix(), RequestModelName: "gpt-4o-mini", Success: true}); err != nil {
			t.Fatalf("RelayLogAdd failed: %v", err)
		}
	}

	if got := RelayLogPendingLen(); got != logCount {
		t.Fatalf("expected pending logs to stay queued, got %d", got)
	}
	var dbCount int64
	if err := dbpkg.GetDB().WithContext(ctx).Model(&model.RelayLog{}).Count(&dbCount).Error; err != nil {
		t.Fatalf("count relay logs failed: %v", err)
	}
	if dbCount != 0 {
		t.Fatalf("RelayLogAdd wrote to DB synchronously, db rows=%d", dbCount)
	}
}

func TestRelayLogListDefaultsToLightFieldsAndNoContentKeyword(t *testing.T) {
	ctx := setupSiteOpTestDB(t)
	if err := settingRefreshCache(ctx); err != nil {
		t.Fatalf("settingRefreshCache failed: %v", err)
	}
	resetRelayLogStateForTest()

	rows := []model.RelayLog{
		{ID: 101, Time: 101, RequestModelName: "gpt-visible", RequestAPIKeyName: "key-a", ChannelId: 1, ChannelName: "primary", ActualModelName: "gpt-visible", RequestContent: "hidden-needle", ResponseContent: "hidden-response", Success: true},
		{ID: 102, Time: 102, RequestModelName: "claude", RequestAPIKeyName: "key-b", ChannelId: 1, ChannelName: "secondary", ActualModelName: "claude", Error: "visible failure", RequestContent: "plain", Success: false},
	}
	if err := dbpkg.GetDB().WithContext(ctx).Create(&rows).Error; err != nil {
		t.Fatalf("create relay logs failed: %v", err)
	}

	result, err := RelayLogListWithFilter(ctx, RelayLogListFilter{Page: 1, PageSize: 10, WithTotal: true})
	if err != nil {
		t.Fatalf("RelayLogListWithFilter failed: %v", err)
	}
	if result.Total != 2 || len(result.Logs) != 2 {
		t.Fatalf("unexpected list result: %+v", result)
	}
	for _, item := range result.Logs {
		if item.RequestContent != "" || item.ResponseContent != "" {
			t.Fatalf("expected list to omit content fields by default, got %+v", item)
		}
	}

	contentResult, err := RelayLogListWithFilter(ctx, RelayLogListFilter{Keyword: "hidden-needle", Page: 1, PageSize: 10, WithTotal: true})
	if err != nil {
		t.Fatalf("RelayLogListWithFilter keyword failed: %v", err)
	}
	if contentResult.Total != 0 || len(contentResult.Logs) != 0 {
		t.Fatalf("default keyword unexpectedly searched content: %+v", contentResult)
	}

	contentResult, err = RelayLogListWithFilter(ctx, RelayLogListFilter{Keyword: "hidden-needle", KeywordScope: RelayLogKeywordScopeContent, StartTime: intPtr(0), EndTime: intPtr(200), Page: 1, PageSize: 10, WithTotal: true})
	if err != nil {
		t.Fatalf("RelayLogListWithFilter content keyword failed: %v", err)
	}
	if contentResult.Total != 1 || len(contentResult.Logs) != 1 || contentResult.Logs[0].ID != 101 {
		t.Fatalf("content keyword did not find expected row: %+v", contentResult)
	}
}

func intPtr(v int) *int { return &v }

func TestRelayLogListContainsKeywordRequiresMinLength(t *testing.T) {
	ctx := setupSiteOpTestDB(t)
	if err := settingRefreshCache(ctx); err != nil {
		t.Fatalf("settingRefreshCache failed: %v", err)
	}
	resetRelayLogStateForTest()

	_, err := RelayLogListWithFilter(ctx, RelayLogListFilter{Keyword: "ab", KeywordMode: RelayLogKeywordModeContains, Page: 1, PageSize: 10})
	if !errors.Is(err, ErrRelayLogContainsKeywordTooShort) {
		t.Fatalf("expected ErrRelayLogContainsKeywordTooShort, got %v", err)
	}
}

func TestRelayLogListPrefixKeywordMatchesModelPrefix(t *testing.T) {
	ctx := setupSiteOpTestDB(t)
	if err := settingRefreshCache(ctx); err != nil {
		t.Fatalf("settingRefreshCache failed: %v", err)
	}
	resetRelayLogStateForTest()

	rows := []model.RelayLog{
		{ID: 401, Time: 401, RequestModelName: "gpt-4o-mini", ChannelName: "primary", Success: true},
		{ID: 402, Time: 402, RequestModelName: "claude-3", ChannelName: "secondary", Success: false},
	}
	if err := dbpkg.GetDB().WithContext(ctx).Create(&rows).Error; err != nil {
		t.Fatalf("create relay logs failed: %v", err)
	}

	result, err := RelayLogListWithFilter(ctx, RelayLogListFilter{Keyword: "gpt", Page: 1, PageSize: 10, WithTotal: true})
	if err != nil {
		t.Fatalf("prefix search failed: %v", err)
	}
	if result.Total != 1 || len(result.Logs) != 1 || result.Logs[0].ID != 401 {
		t.Fatalf("prefix search returned unexpected rows: %+v", result)
	}
	if result.SearchMode != "fast" {
		t.Fatalf("expected SearchMode=fast, got %q", result.SearchMode)
	}
}

func TestRelayLogListModelFilterMatchesRequestAndActualModels(t *testing.T) {
	ctx := setupSiteOpTestDB(t)
	if err := settingRefreshCache(ctx); err != nil {
		t.Fatalf("settingRefreshCache failed: %v", err)
	}
	resetRelayLogStateForTest()

	rows := []model.RelayLog{
		{ID: 501, Time: 501, RequestModelName: "request-alias", ActualModelName: "upstream-a", Success: true},
		{ID: 502, Time: 502, RequestModelName: "other-alias", ActualModelName: "Upstream-B", Success: true},
		{ID: 503, Time: 503, RequestModelName: "unrelated", ActualModelName: "upstream-c", Success: true},
	}
	if err := dbpkg.GetDB().WithContext(ctx).Create(&rows).Error; err != nil {
		t.Fatalf("create relay logs failed: %v", err)
	}

	result, err := RelayLogListWithFilter(ctx, RelayLogListFilter{
		ModelNames: []string{" REQUEST-ALIAS ", "upstream-b", "upstream-b"},
		Page:       1,
		PageSize:   10,
		WithTotal:  true,
	})
	if err != nil {
		t.Fatalf("model filter failed: %v", err)
	}
	if result.Total != 2 || len(result.Logs) != 2 || result.Logs[0].ID != 502 || result.Logs[1].ID != 501 {
		t.Fatalf("model filter returned unexpected rows: %+v", result)
	}
}

func TestRelayLogListSourceKeywordMatchesChannelAndModels(t *testing.T) {
	ctx := setupSiteOpTestDB(t)
	if err := settingRefreshCache(ctx); err != nil {
		t.Fatalf("settingRefreshCache failed: %v", err)
	}
	resetRelayLogStateForTest()

	rows := []model.RelayLog{
		{ID: 521, Time: 521, RequestModelName: "gpt-5.6-sol", ActualModelName: "upstream-a", ChannelName: "primary", Success: true},
		{ID: 522, Time: 522, RequestModelName: "alias", ActualModelName: "gpt-5.6-sol-reasoning", ChannelName: "secondary", Success: true},
		{ID: 523, Time: 523, RequestModelName: "unrelated", ActualModelName: "upstream-c", ChannelName: "gpt-5.6-sol route", Success: true},
		{ID: 524, Time: 524, RequestModelName: "unrelated", ActualModelName: "upstream-d", ChannelName: "other", Success: true},
	}
	if err := dbpkg.GetDB().WithContext(ctx).Create(&rows).Error; err != nil {
		t.Fatalf("create relay logs failed: %v", err)
	}

	result, err := RelayLogListWithFilter(ctx, RelayLogListFilter{
		SourceKeyword: " GPT-5.6-SOL ",
		Page:          1,
		PageSize:      10,
		WithTotal:     true,
	})
	if err != nil {
		t.Fatalf("source keyword filter failed: %v", err)
	}
	if result.Total != 3 || len(result.Logs) != 3 || result.Logs[0].ID != 523 || result.Logs[1].ID != 522 || result.Logs[2].ID != 521 {
		t.Fatalf("source keyword filter returned unexpected rows: %+v", result)
	}
}

func TestRelayLogListModelFilterMatchesPendingLogs(t *testing.T) {
	ctx := setupSiteOpTestDB(t)
	if err := settingRefreshCache(ctx); err != nil {
		t.Fatalf("settingRefreshCache failed: %v", err)
	}
	resetRelayLogStateForTest()

	for _, entry := range []model.RelayLog{
		{Time: 601, RequestModelName: "live-alias", ActualModelName: "opus-5", Success: true},
		{Time: 602, RequestModelName: "opus", ActualModelName: "other-upstream", Success: true},
	} {
		if err := RelayLogAdd(ctx, entry); err != nil {
			t.Fatalf("RelayLogAdd failed: %v", err)
		}
	}

	result, err := RelayLogListWithFilter(ctx, RelayLogListFilter{
		ModelNames: []string{" OPUS-5 "},
		Page:       1,
		PageSize:   10,
		WithTotal:  true,
	})
	if err != nil {
		t.Fatalf("pending model filter failed: %v", err)
	}
	if result.Total != 1 || len(result.Logs) != 1 || result.Logs[0].RequestModelName != "live-alias" {
		t.Fatalf("pending model filter returned unexpected rows: %+v", result)
	}
}

func TestRelayLogListSourceKeywordMatchesPendingLogs(t *testing.T) {
	ctx := setupSiteOpTestDB(t)
	if err := settingRefreshCache(ctx); err != nil {
		t.Fatalf("settingRefreshCache failed: %v", err)
	}
	resetRelayLogStateForTest()

	for _, entry := range []model.RelayLog{
		{Time: 611, RequestModelName: "live-alias", ActualModelName: "gpt-5.6-sol", ChannelName: "primary", Success: true},
		{Time: 612, RequestModelName: "other", ActualModelName: "other-upstream", ChannelName: "secondary", Success: true},
	} {
		if err := RelayLogAdd(ctx, entry); err != nil {
			t.Fatalf("RelayLogAdd failed: %v", err)
		}
	}

	result, err := RelayLogListWithFilter(ctx, RelayLogListFilter{
		SourceKeyword: "gpt-5.6-sol",
		Page:          1,
		PageSize:      10,
		WithTotal:     true,
	})
	if err != nil {
		t.Fatalf("pending source keyword filter failed: %v", err)
	}
	if result.Total != 1 || len(result.Logs) != 1 || result.Logs[0].RequestModelName != "live-alias" {
		t.Fatalf("pending source keyword filter returned unexpected rows: %+v", result)
	}
}

func TestRelayLogListCursorReturnsNextCursorWithoutTotal(t *testing.T) {
	ctx := setupSiteOpTestDB(t)
	if err := settingRefreshCache(ctx); err != nil {
		t.Fatalf("settingRefreshCache failed: %v", err)
	}
	resetRelayLogStateForTest()

	rows := []model.RelayLog{
		{ID: 201, Time: 201, RequestModelName: "a", Success: true},
		{ID: 202, Time: 202, RequestModelName: "b", Success: true},
		{ID: 203, Time: 203, RequestModelName: "c", Success: true},
	}
	if err := dbpkg.GetDB().WithContext(ctx).Create(&rows).Error; err != nil {
		t.Fatalf("create relay logs failed: %v", err)
	}

	first, err := RelayLogListWithFilter(ctx, RelayLogListFilter{Limit: 2})
	if err != nil {
		t.Fatalf("RelayLogListWithFilter cursor failed: %v", err)
	}
	if first.Total != 0 || !first.HasMore || first.NextCursor == nil || len(first.Logs) != 2 || first.Logs[0].ID != 203 || first.Logs[1].ID != 202 {
		t.Fatalf("unexpected first cursor page: %+v", first)
	}
	second, err := RelayLogListWithFilter(ctx, RelayLogListFilter{Limit: 2, BeforeTime: &first.NextCursor.Time, BeforeID: &first.NextCursor.ID})
	if err != nil {
		t.Fatalf("RelayLogListWithFilter second cursor failed: %v", err)
	}
	if second.HasMore || second.NextCursor != nil || len(second.Logs) != 1 || second.Logs[0].ID != 201 {
		t.Fatalf("unexpected second cursor page: %+v", second)
	}
}

func TestRelayLogGetReturnsFullContent(t *testing.T) {
	ctx := setupSiteOpTestDB(t)
	if err := settingRefreshCache(ctx); err != nil {
		t.Fatalf("settingRefreshCache failed: %v", err)
	}
	resetRelayLogStateForTest()

	row := model.RelayLog{ID: 301, Time: 301, RequestModelName: "gpt", RequestContent: "full request", ResponseContent: "full response", Success: true}
	if err := dbpkg.GetDB().WithContext(ctx).Create(&row).Error; err != nil {
		t.Fatalf("create relay log failed: %v", err)
	}

	got, err := RelayLogGet(ctx, row.ID)
	if err != nil {
		t.Fatalf("RelayLogGet failed: %v", err)
	}
	if got.RequestContent != row.RequestContent || got.ResponseContent != row.ResponseContent {
		t.Fatalf("expected full content, got %+v", got)
	}
}

func TestRelayLogChannelIDsUsesPendingAndDB(t *testing.T) {
	ctx := setupSiteOpTestDB(t)
	if err := settingRefreshCache(ctx); err != nil {
		t.Fatalf("settingRefreshCache failed: %v", err)
	}
	resetRelayLogStateForTest()

	rows := []model.RelayLog{
		{ID: 701, Time: 701, ChannelId: 2, Success: true},
		{ID: 702, Time: 702, ChannelId: 4, Success: false},
		{ID: 703, Time: 703, ChannelId: 0, Success: true},
	}
	if err := dbpkg.GetDB().WithContext(ctx).Create(&rows).Error; err != nil {
		t.Fatalf("create relay logs failed: %v", err)
	}
	for _, entry := range []model.RelayLog{
		{Time: 704, ChannelId: 1, Success: true},
		{Time: 705, ChannelId: 2, Success: true},
		{Time: 706, ChannelId: 0, Success: true},
	} {
		if err := RelayLogAdd(ctx, entry); err != nil {
			t.Fatalf("RelayLogAdd failed: %v", err)
		}
	}

	got, err := RelayLogChannelIDs(ctx)
	if err != nil {
		t.Fatalf("RelayLogChannelIDs failed: %v", err)
	}
	want := []int{1, 2, 4}
	if !slices.Equal(got, want) {
		t.Fatalf("expected channel IDs %v, got %v", want, got)
	}
}

func TestRelayLogChannelIDsUsesRecentWhenDisabledAndRefreshesAfterClear(t *testing.T) {
	ctx := setupSiteOpTestDB(t)
	if err := settingRefreshCache(ctx); err != nil {
		t.Fatalf("settingRefreshCache failed: %v", err)
	}
	if err := SettingSetString(model.SettingKeyRelayLogKeepEnabled, "false"); err != nil {
		t.Fatalf("disable relay log retention failed: %v", err)
	}
	resetRelayLogStateForTest()

	row := model.RelayLog{ID: 707, Time: 707, ChannelId: 9, Success: true}
	if err := dbpkg.GetDB().WithContext(ctx).Create(&row).Error; err != nil {
		t.Fatalf("create persisted relay log failed: %v", err)
	}
	if err := RelayLogAdd(ctx, model.RelayLog{Time: 708, ChannelId: 3, Success: true}); err != nil {
		t.Fatalf("RelayLogAdd failed: %v", err)
	}

	got, err := RelayLogChannelIDs(ctx)
	if err != nil {
		t.Fatalf("RelayLogChannelIDs failed: %v", err)
	}
	if !slices.Equal(got, []int{3}) {
		t.Fatalf("expected recent channel IDs [3], got %v", got)
	}

	if err := RelayLogClear(ctx); err != nil {
		t.Fatalf("RelayLogClear failed: %v", err)
	}
	got, err = RelayLogChannelIDs(ctx)
	if err != nil {
		t.Fatalf("RelayLogChannelIDs after clear failed: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("expected no channel IDs after clear, got %v", got)
	}
}

func TestRelayLogFlushPendingPersistsQueuedLogs(t *testing.T) {
	ctx := setupSiteOpTestDB(t)
	if err := settingRefreshCache(ctx); err != nil {
		t.Fatalf("settingRefreshCache failed: %v", err)
	}
	resetRelayLogStateForTest()

	for i := 0; i < 3; i++ {
		if err := RelayLogAdd(ctx, model.RelayLog{Time: int64(100 + i), RequestModelName: "model", Success: true}); err != nil {
			t.Fatalf("RelayLogAdd failed: %v", err)
		}
	}
	if err := RelayLogFlushPending(ctx); err != nil {
		t.Fatalf("RelayLogFlushPending failed: %v", err)
	}
	if got := RelayLogPendingLen(); got != 0 {
		t.Fatalf("expected pending queue to be empty, got %d", got)
	}
	var dbCount int64
	if err := dbpkg.GetDB().WithContext(ctx).Model(&model.RelayLog{}).Count(&dbCount).Error; err != nil {
		t.Fatalf("count relay logs failed: %v", err)
	}
	if dbCount != 3 {
		t.Fatalf("expected 3 persisted logs, got %d", dbCount)
	}
}
