package op

import (
	"context"
	"testing"
	"time"

	"github.com/bestruirui/octopus/internal/model"
)

// siteModelHourlySavePauseTestHook 安装测试钩子：SaveDB 在快照已登记、
// 刷盘事务尚未开始的位置阻塞，直到测试放行。返回 entered/release 两个
// channel，并在 t.Cleanup 中兜底清理全局钩子与阻塞状态。
func installSiteModelHourlySavePauseHook(t *testing.T) (entered <-chan struct{}, release chan<- struct{}) {
	t.Helper()
	enteredCh := make(chan struct{})
	releaseCh := make(chan struct{})
	siteModelHourlySavePauseHook = func() {
		close(enteredCh)
		<-releaseCh
	}
	t.Cleanup(func() {
		siteModelHourlySavePauseHook = nil
		select {
		case <-releaseCh:
		default:
			close(releaseCh)
		}
	})
	return enteredCh, releaseCh
}

// seedSiteModelHourlyCacheForTest 在锁下预置一个内存小时桶。
func seedSiteModelHourlyCacheForTest(t *testing.T, key siteModelHourlyKey, metrics model.StatsMetrics, lastRequestAt int64, date string) {
	t.Helper()
	siteModelHourlyCacheLock.Lock()
	defer siteModelHourlyCacheLock.Unlock()
	siteModelHourlyCache[key] = &model.StatsSiteModelHourly{
		Hour: key.Hour, SiteAccountID: key.SiteAccountID, GroupKey: key.GroupKey, ModelName: key.ModelName,
		Date: date, LastRequestAt: lastRequestAt, StatsMetrics: metrics,
	}
}

// addSiteModelHourlySampleForTest 模拟并发请求在锁下向内存桶累加样本，
// 与 StatsSiteModelHourlyUpdate 的合并语义一致。
func addSiteModelHourlySampleForTest(t *testing.T, key siteModelHourlyKey, metrics model.StatsMetrics, lastRequestAt int64) {
	t.Helper()
	siteModelHourlyCacheLock.Lock()
	defer siteModelHourlyCacheLock.Unlock()
	entry, ok := siteModelHourlyCache[key]
	if !ok {
		entry = &model.StatsSiteModelHourly{
			Hour: key.Hour, SiteAccountID: key.SiteAccountID, GroupKey: key.GroupKey, ModelName: key.ModelName,
		}
		siteModelHourlyCache[key] = entry
	}
	entry.StatsMetrics.Add(metrics)
	if lastRequestAt > entry.LastRequestAt {
		entry.LastRequestAt = lastRequestAt
	}
}

// siteModelSeriesSummaryForTest 读取指定账号的 (group, model) 序列摘要。
func siteModelSeriesSummaryForTest(t *testing.T, ctx context.Context, siteAccountID int, seriesKey string) *model.SiteModelHistorySummary {
	t.Helper()
	summaries, err := SiteChannelModelHourlyForAccounts(ctx, []int{siteAccountID})
	if err != nil {
		t.Fatalf("SiteChannelModelHourlyForAccounts failed: %v", err)
	}
	return summaries[siteAccountID][seriesKey]
}

// TestStatsSiteModelHourlyReadDuringUncommittedSaveSeesSnapshot 确定性复现
// "刷盘已取走快照但事务尚未提交时读取"的窗口：修复前旧快照已从内存移除、
// DB 尚未提交，读取会瞬时少计；修复后读取必须合并在途快照，提交成功后
// 不重复计数，写入期间的新样本不丢失。
func TestStatsSiteModelHourlyReadDuringUncommittedSaveSeesSnapshot(t *testing.T) {
	ctx := setupSiteOpTestDB(t)
	resetSiteModelHourlyCacheForTest(t)

	key := siteModelHourlyKey{Hour: int(time.Now().Unix() / 3600), SiteAccountID: 10, GroupKey: "default", ModelName: "model-d"}
	seedSiteModelHourlyCacheForTest(t, key, model.StatsMetrics{InputToken: 5, RequestSuccess: 5}, 10, "20260829")

	entered, release := installSiteModelHourlySavePauseHook(t)
	done := make(chan error, 1)
	go func() {
		done <- StatsSiteModelHourlySaveDB(ctx)
	}()

	select {
	case <-entered:
	case err := <-done:
		t.Fatalf("save returned before reaching the persisting window: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatalf("save did not reach the persisting window")
	}

	seriesKey := key.GroupKey + "\x00" + key.ModelName

	// 快照已出内存、事务未提交：读取必须仍看到快照数据。
	if summary := siteModelSeriesSummaryForTest(t, ctx, key.SiteAccountID, seriesKey); summary == nil || summary.SuccessCount != 5 {
		t.Fatalf("in-flight snapshot not visible during uncommitted save: %+v", summary)
	}

	// 并发请求在写入期间记录新样本：读取必须合并在途快照与新样本，不丢失。
	addSiteModelHourlySampleForTest(t, key, model.StatsMetrics{InputToken: 2, RequestSuccess: 2}, 20)
	if summary := siteModelSeriesSummaryForTest(t, ctx, key.SiteAccountID, seriesKey); summary == nil || summary.SuccessCount != 7 {
		t.Fatalf("concurrent sample lost during save: %+v, want success=7", summary)
	}

	close(release)
	if err := <-done; err != nil {
		t.Fatalf("save after release: %v", err)
	}

	// 提交成功后快照注销：读取来自 DB + 新样本，不重复计数。
	if summary := siteModelSeriesSummaryForTest(t, ctx, key.SiteAccountID, seriesKey); summary == nil || summary.SuccessCount != 7 {
		t.Fatalf("committed snapshot double counted or lost: %+v, want success=7", summary)
	}

	siteModelHourlyCacheLock.Lock()
	persisting := len(siteModelHourlyPersisting)
	siteModelHourlyCacheLock.Unlock()
	if persisting != 0 {
		t.Fatalf("persisting batches left after successful save: %d", persisting)
	}
}

// TestStatsSiteModelHourlySaveFailureRestoresAndClearsPersisting 验证事务失败时
// 快照 merge restore 回内存、persisting 不残留，失败前后读取均不少计。
func TestStatsSiteModelHourlySaveFailureRestoresAndClearsPersisting(t *testing.T) {
	ctx := setupSiteOpTestDB(t)
	resetSiteModelHourlyCacheForTest(t)

	key := siteModelHourlyKey{Hour: int(time.Now().Unix() / 3600), SiteAccountID: 11, GroupKey: "default", ModelName: "model-e"}
	seedSiteModelHourlyCacheForTest(t, key, model.StatsMetrics{InputToken: 3, RequestFailed: 3}, 30, "20260829")

	entered, release := installSiteModelHourlySavePauseHook(t)
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	done := make(chan error, 1)
	go func() {
		done <- StatsSiteModelHourlySaveDB(canceled)
	}()

	select {
	case <-entered:
	case err := <-done:
		t.Fatalf("save returned before reaching the persisting window: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatalf("save did not reach the persisting window")
	}

	seriesKey := key.GroupKey + "\x00" + key.ModelName

	// 事务尚未开始：数据已出内存但未落库，读取依赖 persisting 快照。
	if summary := siteModelSeriesSummaryForTest(t, ctx, key.SiteAccountID, seriesKey); summary == nil || summary.FailureCount != 3 {
		t.Fatalf("in-flight snapshot not visible before failed save: %+v", summary)
	}

	close(release)
	if err := <-done; err == nil {
		t.Fatalf("expected canceled save to fail")
	}

	// 失败后快照 restore 回内存，persisting 不残留，读取不少计。
	if summary := siteModelSeriesSummaryForTest(t, ctx, key.SiteAccountID, seriesKey); summary == nil || summary.FailureCount != 3 {
		t.Fatalf("restored snapshot not visible after failed save: %+v", summary)
	}
	siteModelHourlyCacheLock.Lock()
	persisting := len(siteModelHourlyPersisting)
	cacheEntries := len(siteModelHourlyCache)
	siteModelHourlyCacheLock.Unlock()
	if persisting != 0 || cacheEntries != 1 {
		t.Fatalf("after failed save: persisting=%d cacheEntries=%d, want 0 and 1", persisting, cacheEntries)
	}
}
