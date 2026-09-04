package op

import (
	"context"
	"strings"
	"testing"

	dbpkg "github.com/bestruirui/octopus/internal/db"
	"github.com/bestruirui/octopus/internal/model"
	"gorm.io/gorm/clause"
)

func TestStatsSiteModelHourlyUpsertAssignmentsAreDialectCorrect(t *testing.T) {
	tests := []struct {
		dialect string
		want    []string
		reject  []string
	}{
		{dialect: "sqlite", want: []string{"excluded.input_token", "MAX("}, reject: []string{"VALUES(", "GREATEST("}},
		{dialect: "postgres", want: []string{"excluded.input_token", "GREATEST("}, reject: []string{"VALUES(", "MAX("}},
		{dialect: "mysql", want: []string{"VALUES(input_token)", "GREATEST("}, reject: []string{"excluded.", "MAX("}},
	}
	for _, tt := range tests {
		t.Run(tt.dialect, func(t *testing.T) {
			assignments := statsSiteModelHourlyUpsertAssignments(tt.dialect)
			inputExpr, ok := assignments["input_token"].(clause.Expr)
			if !ok {
				t.Fatalf("input_token assignment is %T", assignments["input_token"])
			}
			lastExpr, ok := assignments["last_request_at"].(clause.Expr)
			if !ok {
				t.Fatalf("last_request_at assignment is %T", assignments["last_request_at"])
			}
			sql := inputExpr.SQL + " " + lastExpr.SQL
			for _, want := range tt.want {
				if !strings.Contains(sql, want) {
					t.Fatalf("dialect %s SQL %q missing %q", tt.dialect, sql, want)
				}
			}
			for _, reject := range tt.reject {
				if strings.Contains(sql, reject) {
					t.Fatalf("dialect %s SQL %q unexpectedly contains %q", tt.dialect, sql, reject)
				}
			}
		})
	}
}

func TestRestoreSiteModelHourlyCacheMergesConcurrentSamples(t *testing.T) {
	siteModelHourlyCacheLock.Lock()
	previous := siteModelHourlyCache
	siteModelHourlyCache = make(map[siteModelHourlyKey]*model.StatsSiteModelHourly)
	siteModelHourlyCacheLock.Unlock()
	t.Cleanup(func() {
		siteModelHourlyCacheLock.Lock()
		siteModelHourlyCache = previous
		siteModelHourlyCacheLock.Unlock()
	})

	key := siteModelHourlyKey{Hour: 100, SiteAccountID: 7, GroupKey: "default", ModelName: "model-a"}
	siteModelHourlyCacheLock.Lock()
	siteModelHourlyCache[key] = &model.StatsSiteModelHourly{
		Hour: 100, SiteAccountID: 7, GroupKey: "default", ModelName: "model-a", Date: "20260829", LastRequestAt: 20,
		StatsMetrics: model.StatsMetrics{InputToken: 2, RequestSuccess: 1},
	}
	siteModelHourlyCacheLock.Unlock()

	restoreSiteModelHourlyCache([]model.StatsSiteModelHourly{{
		Hour: 100, SiteAccountID: 7, GroupKey: "default", ModelName: "model-a", Date: "20260828", LastRequestAt: 10,
		StatsMetrics: model.StatsMetrics{InputToken: 3, OutputToken: 4, RequestFailed: 1},
	}})

	siteModelHourlyCacheLock.Lock()
	got := *siteModelHourlyCache[key]
	siteModelHourlyCacheLock.Unlock()
	if got.InputToken != 5 || got.OutputToken != 4 || got.RequestSuccess != 1 || got.RequestFailed != 1 {
		t.Fatalf("restored metrics were not merged: %+v", got.StatsMetrics)
	}
	if got.LastRequestAt != 20 || got.Date != "20260829" {
		t.Fatalf("newer concurrent metadata was overwritten: date=%s last=%d", got.Date, got.LastRequestAt)
	}
}

func TestStatsSiteModelHourlySaveDBUpsertsSQLite(t *testing.T) {
	ctx := setupSiteOpTestDB(t)
	resetSiteModelHourlyCacheForTest(t)
	key := siteModelHourlyKey{Hour: 101, SiteAccountID: 8, GroupKey: "default", ModelName: "model-b"}

	siteModelHourlyCacheLock.Lock()
	siteModelHourlyCache[key] = &model.StatsSiteModelHourly{
		Hour: key.Hour, SiteAccountID: key.SiteAccountID, GroupKey: key.GroupKey, ModelName: key.ModelName,
		Date: "20260829", LastRequestAt: 10,
		StatsMetrics: model.StatsMetrics{InputToken: 2, OutputToken: 3, RequestSuccess: 1},
	}
	siteModelHourlyCacheLock.Unlock()
	if err := StatsSiteModelHourlySaveDB(ctx); err != nil {
		t.Fatalf("first save: %v", err)
	}

	siteModelHourlyCacheLock.Lock()
	siteModelHourlyCache[key] = &model.StatsSiteModelHourly{
		Hour: key.Hour, SiteAccountID: key.SiteAccountID, GroupKey: key.GroupKey, ModelName: key.ModelName,
		Date: "20260829", LastRequestAt: 20,
		StatsMetrics: model.StatsMetrics{InputToken: 5, OutputToken: 7, RequestFailed: 1},
	}
	siteModelHourlyCacheLock.Unlock()
	if err := StatsSiteModelHourlySaveDB(ctx); err != nil {
		t.Fatalf("second save: %v", err)
	}

	var got model.StatsSiteModelHourly
	if err := dbpkg.GetDB().WithContext(ctx).Where(
		"hour = ? AND site_account_id = ? AND group_key = ? AND model_name = ?",
		key.Hour, key.SiteAccountID, key.GroupKey, key.ModelName,
	).Take(&got).Error; err != nil {
		t.Fatalf("load saved row: %v", err)
	}
	if got.InputToken != 7 || got.OutputToken != 10 || got.RequestSuccess != 1 || got.RequestFailed != 1 || got.LastRequestAt != 20 {
		t.Fatalf("unexpected accumulated row: %+v", got)
	}
}

func TestStatsSiteModelHourlySaveDBRestoresCacheOnFailure(t *testing.T) {
	_ = setupSiteOpTestDB(t)
	resetSiteModelHourlyCacheForTest(t)
	key := siteModelHourlyKey{Hour: 102, SiteAccountID: 9, GroupKey: "default", ModelName: "model-c"}
	row := &model.StatsSiteModelHourly{
		Hour: key.Hour, SiteAccountID: key.SiteAccountID, GroupKey: key.GroupKey, ModelName: key.ModelName,
		Date: "20260829", LastRequestAt: 30,
		StatsMetrics: model.StatsMetrics{InputToken: 11, RequestFailed: 1},
	}
	siteModelHourlyCacheLock.Lock()
	siteModelHourlyCache[key] = row
	siteModelHourlyCacheLock.Unlock()

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := StatsSiteModelHourlySaveDB(canceled); err == nil {
		t.Fatalf("expected canceled write to fail")
	}
	siteModelHourlyCacheLock.Lock()
	got := siteModelHourlyCache[key]
	siteModelHourlyCacheLock.Unlock()
	if got == nil || got.InputToken != 11 || got.RequestFailed != 1 || got.LastRequestAt != 30 {
		t.Fatalf("failed write was not restored to cache: %+v", got)
	}
}

func resetSiteModelHourlyCacheForTest(t *testing.T) {
	t.Helper()
	siteModelHourlyCacheLock.Lock()
	previous := siteModelHourlyCache
	siteModelHourlyCache = make(map[siteModelHourlyKey]*model.StatsSiteModelHourly)
	// persisting 快照代表进行中的刷盘事务，测试环境不应有残留；
	// 这里直接清空，避免泄漏到后续测试的读取合并里。
	siteModelHourlyPersisting = nil
	siteModelHourlyPersistingVersion++
	siteModelHourlyCacheLock.Unlock()
	t.Cleanup(func() {
		siteModelHourlyCacheLock.Lock()
		siteModelHourlyCache = previous
		siteModelHourlyPersisting = nil
		siteModelHourlyCacheLock.Unlock()
	})
}
