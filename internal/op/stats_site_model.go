package op

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/bestruirui/octopus/internal/db"
	"github.com/bestruirui/octopus/internal/model"
	"github.com/bestruirui/octopus/internal/utils/cache"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// channelSiteBinding 是 channelID → 站点账号绑定的简化形式。
type channelSiteBinding struct {
	SiteAccountID int
	BaseGroupKey  string
	Found         bool
}

// 站点渠道绑定缓存，懒加载，正向命中永久持有；负向命中也缓存，避免每次请求都查 DB。
// 由于 SiteChannelBinding 的 channel_id 是 uniqueIndex，迁移场景下绑定基本不会重映射，
// 在站点账号删除时会调用 invalidateSiteBindingCache 清理。
var siteBindingByChannelCache = cache.New[int, channelSiteBinding](16)

// 桶级缓存：以小时桶为粒度累加，由后台任务批量持久化。
type siteModelHourlyKey struct {
	Hour          int
	SiteAccountID int
	GroupKey      string
	ModelName     string
}

var siteModelHourlyCache = make(map[siteModelHourlyKey]*model.StatsSiteModelHourly)
var siteModelHourlyCacheLock sync.Mutex

// siteModelHourlyPersistingBatch 是一次刷盘事务尚未结束期间暂存的内存快照。
// 快照在锁下从 siteModelHourlyCache 摘除时登记，事务结束（提交成功注销或
// 失败 restore）后移除；读取路径据此在"已出内存、事务未提交"的窗口里
// 仍能合并这部分数据，避免瞬时少计。
type siteModelHourlyPersistingBatch struct {
	rows []model.StatsSiteModelHourly
}

var (
	siteModelHourlyPersisting []*siteModelHourlyPersistingBatch
	// siteModelHourlyPersistingVersion 在任一批次离开 persisting 列表时递增。
	// 读取端用它检测 DB 查询期间是否有刷盘事务结束：若查询恰好跨越提交
	// 时刻，DB 结果既不含该批次、快照也已注销，会造成少计；命中时重读。
	siteModelHourlyPersistingVersion int
)

// siteModelHourlySavePauseHook 仅测试使用：在快照已登记、刷盘事务尚未开始时
// 调用，用于确定性复现"读取落在刷盘窗口"的竞态。生产代码保持 nil。
var siteModelHourlySavePauseHook func()

// siteModelHourlyReadRetryLimit 限制读取端因刷盘结束事件重读 DB 的次数，
// 防御刷盘被改成高频循环时的读取端活锁；超限后接受亚毫秒级残余窗口，
// 该窗口只影响单次读取的展示值，不会持久化。
const siteModelHourlyReadRetryLimit = 4

// StatsSiteModelHourlyUpdate 记录一次站点渠道请求到对应小时桶。
// 非站点渠道（无绑定）会被静默忽略。
func StatsSiteModelHourlyUpdate(channelID int, actualModel string, metrics model.StatsMetrics) {
	actualModel = strings.TrimSpace(actualModel)
	if channelID == 0 || actualModel == "" {
		return
	}

	binding, err := lookupChannelSiteBinding(channelID)
	if err != nil || !binding.Found {
		return
	}

	now := time.Now()
	hour := int(now.Unix() / 3600)
	nowSec := now.Unix()
	date := now.Format("20060102")

	key := siteModelHourlyKey{
		Hour:          hour,
		SiteAccountID: binding.SiteAccountID,
		GroupKey:      binding.BaseGroupKey,
		ModelName:     actualModel,
	}

	siteModelHourlyCacheLock.Lock()
	defer siteModelHourlyCacheLock.Unlock()
	entry, ok := siteModelHourlyCache[key]
	if !ok {
		entry = &model.StatsSiteModelHourly{
			Hour:          hour,
			SiteAccountID: binding.SiteAccountID,
			GroupKey:      binding.BaseGroupKey,
			ModelName:     actualModel,
			Date:          date,
		}
		siteModelHourlyCache[key] = entry
	}
	entry.StatsMetrics.Add(metrics)
	if nowSec > entry.LastRequestAt {
		entry.LastRequestAt = nowSec
	}
}

// StatsSiteModelHourlyRecordAttempts 把一次 relay 中所有 success/failed attempts
// 按 (channel, attempt.modelName) 维度记录到小时桶。仅累加 request_success/request_failed，
// 与现有 site_channel 历史计数语义一致；token/cost 等不在此处累加（已由全局 stats 处理）。
func StatsSiteModelHourlyRecordAttempts(attempts []model.ChannelAttempt, fallbackModel string) {
	for _, attempt := range attempts {
		if attempt.ChannelID == 0 {
			continue
		}
		if attempt.Status != model.AttemptSuccess && attempt.Status != model.AttemptFailed {
			continue
		}
		modelName := strings.TrimSpace(attempt.ModelName)
		if modelName == "" {
			modelName = strings.TrimSpace(fallbackModel)
		}
		if modelName == "" {
			continue
		}
		var metrics model.StatsMetrics
		if attempt.Status == model.AttemptSuccess {
			metrics.RequestSuccess = 1
		} else {
			metrics.RequestFailed = 1
		}
		StatsSiteModelHourlyUpdate(attempt.ChannelID, modelName, metrics)
	}
}

const statsSiteModelHourlyPersistBatchSize = 200

// StatsSiteModelHourlySaveDB 把内存桶批量 upsert 入库。
// 由 stats 后台任务调用。
func StatsSiteModelHourlySaveDB(ctx context.Context) error {
	siteModelHourlyCacheLock.Lock()
	if len(siteModelHourlyCache) == 0 {
		siteModelHourlyCacheLock.Unlock()
		return nil
	}
	rows := make([]model.StatsSiteModelHourly, 0, len(siteModelHourlyCache))
	for _, entry := range siteModelHourlyCache {
		rows = append(rows, *entry)
	}
	siteModelHourlyCache = make(map[siteModelHourlyKey]*model.StatsSiteModelHourly)
	// 登记为 persisting：事务结束前读取路径仍能合并这份快照，
	// 请求路径与新样本继续写入上面换出的新 map，互不阻塞。
	batch := &siteModelHourlyPersistingBatch{rows: rows}
	siteModelHourlyPersisting = append(siteModelHourlyPersisting, batch)
	siteModelHourlyCacheLock.Unlock()

	if siteModelHourlySavePauseHook != nil {
		siteModelHourlySavePauseHook()
	}

	dbConn := db.GetDB().WithContext(ctx)
	assignments := statsSiteModelHourlyUpsertAssignments(dbConn.Dialector.Name())
	err := dbConn.Transaction(func(tx *gorm.DB) error {
		return tx.Clauses(clause.OnConflict{
			Columns: []clause.Column{
				{Name: "hour"}, {Name: "site_account_id"}, {Name: "group_key"}, {Name: "model_name"},
			},
			DoUpdates: clause.Assignments(assignments),
		}).CreateInBatches(&rows, statsSiteModelHourlyPersistBatchSize).Error
	})

	siteModelHourlyCacheLock.Lock()
	removeSiteModelHourlyPersistingLocked(batch)
	if err != nil {
		// The snapshot was removed before the write to keep request updates fast.
		// Restore it on failure, including samples recorded during the write.
		restoreSiteModelHourlyCacheLocked(rows)
	}
	// 版本递增必须与批次移除在同一个锁段内，读取端才能据此判断
	// DB 查询基线是否已过期。
	siteModelHourlyPersistingVersion++
	siteModelHourlyCacheLock.Unlock()
	return err
}

// statsSiteModelHourlyUpsertAssignments returns dialect-correct references to
// the incoming row. SQLite and PostgreSQL expose excluded.*, while MySQL uses
// VALUES(column) in the supported ON DUPLICATE KEY UPDATE syntax.
func statsSiteModelHourlyUpsertAssignments(dialect string) map[string]interface{} {
	incoming := func(column string) string {
		if dialect == "mysql" {
			return "VALUES(" + column + ")"
		}
		return "excluded." + column
	}
	maxFunction := "MAX"
	if dialect == "mysql" || dialect == "postgres" {
		maxFunction = "GREATEST"
	}
	const table = "stats_site_model_hourlies"
	add := func(column string) interface{} {
		return gorm.Expr(fmt.Sprintf("%s.%s + %s", table, column, incoming(column)))
	}
	return map[string]interface{}{
		"date":            gorm.Expr(incoming("date")),
		"input_token":     add("input_token"),
		"output_token":    add("output_token"),
		"input_cost":      add("input_cost"),
		"output_cost":     add("output_cost"),
		"wait_time":       add("wait_time"),
		"request_success": add("request_success"),
		"request_failed":  add("request_failed"),
		"last_request_at": gorm.Expr(fmt.Sprintf("%s(%s.last_request_at, %s)", maxFunction, table, incoming("last_request_at"))),
	}
}

func restoreSiteModelHourlyCache(rows []model.StatsSiteModelHourly) {
	siteModelHourlyCacheLock.Lock()
	defer siteModelHourlyCacheLock.Unlock()
	restoreSiteModelHourlyCacheLocked(rows)
}

// restoreSiteModelHourlyCacheLocked 把刷盘失败的快照合并回内存桶，
// 调用方必须持有 siteModelHourlyCacheLock。
func restoreSiteModelHourlyCacheLocked(rows []model.StatsSiteModelHourly) {
	if len(rows) == 0 {
		return
	}
	for _, row := range rows {
		key := siteModelHourlyKey{
			Hour:          row.Hour,
			SiteAccountID: row.SiteAccountID,
			GroupKey:      row.GroupKey,
			ModelName:     row.ModelName,
		}
		if existing, ok := siteModelHourlyCache[key]; ok {
			existing.StatsMetrics.Add(row.StatsMetrics)
			if row.LastRequestAt > existing.LastRequestAt {
				existing.LastRequestAt = row.LastRequestAt
			}
			if row.Date > existing.Date {
				existing.Date = row.Date
			}
			continue
		}
		copyRow := row
		siteModelHourlyCache[key] = &copyRow
	}
}

const siteChannelModelHistoryWindow = 90 * 24 * time.Hour

// SiteChannelModelHourlyForAccount 读取指定 site account 下最近一段时间的 (group, model) 小时聚合，
// 合并未刷盘的内存桶后，按自适应桶宽生成 SiteModelHistorySummary。
// key 与 site_channel.go 保持一致：baseGroupKey + "\x00" + modelName。
func SiteChannelModelHourlyForAccount(ctx context.Context, siteAccountID int) (map[string]*model.SiteModelHistorySummary, error) {
	result, err := SiteChannelModelHourlyForAccounts(ctx, []int{siteAccountID})
	if err != nil {
		return nil, err
	}
	if result[siteAccountID] == nil {
		return map[string]*model.SiteModelHistorySummary{}, nil
	}
	return result[siteAccountID], nil
}

func SiteChannelModelHourlyForAccounts(ctx context.Context, siteAccountIDs []int) (map[int]map[string]*model.SiteModelHistorySummary, error) {
	if len(siteAccountIDs) == 0 {
		return map[int]map[string]*model.SiteModelHistorySummary{}, nil
	}
	accountSet := make(map[int]struct{}, len(siteAccountIDs))
	ids := make([]int, 0, len(siteAccountIDs))
	for _, id := range siteAccountIDs {
		if id <= 0 {
			continue
		}
		if _, ok := accountSet[id]; ok {
			continue
		}
		accountSet[id] = struct{}{}
		ids = append(ids, id)
	}
	if len(ids) == 0 {
		return map[int]map[string]*model.SiteModelHistorySummary{}, nil
	}

	minHour := int(time.Now().Add(-siteChannelModelHistoryWindow).Unix() / 3600)
	var rows []model.StatsSiteModelHourly
	var pending []model.StatsSiteModelHourly
	for retry := 0; ; retry++ {
		siteModelHourlyCacheLock.Lock()
		versionBefore := siteModelHourlyPersistingVersion
		siteModelHourlyCacheLock.Unlock()

		rows = nil
		if err := db.GetDB().WithContext(ctx).
			Where("site_account_id IN ? AND hour >= ?", ids, minHour).
			Order("site_account_id ASC").
			Order("hour ASC").
			Find(&rows).Error; err != nil {
			return nil, err
		}

		// 合并尚未刷盘的内存桶与刷盘事务尚未结束的快照。
		siteModelHourlyCacheLock.Lock()
		pending = collectSiteModelHourlyPendingLocked(accountSet, minHour)
		stable := siteModelHourlyPersistingVersion == versionBefore
		siteModelHourlyCacheLock.Unlock()
		if stable || retry >= siteModelHourlyReadRetryLimit {
			break
		}
		// DB 查询期间有刷盘事务结束（提交成功或失败 restore）：上面的查询
		// 可能恰好跨越提交时刻，重读一次以对齐内存基线，避免少计或重复。
	}

	type compositeKey struct {
		SiteAccountID int
		Hour          int
		GroupKey      string
		ModelName     string
	}
	merged := make(map[compositeKey]*model.StatsSiteModelHourly, len(rows)+len(pending))
	add := func(r model.StatsSiteModelHourly) {
		k := compositeKey{SiteAccountID: r.SiteAccountID, Hour: r.Hour, GroupKey: r.GroupKey, ModelName: r.ModelName}
		if existing, ok := merged[k]; ok {
			existing.StatsMetrics.Add(r.StatsMetrics)
			if r.LastRequestAt > existing.LastRequestAt {
				existing.LastRequestAt = r.LastRequestAt
			}
			return
		}
		copyRow := r
		merged[k] = &copyRow
	}
	for _, r := range rows {
		add(r)
	}
	for _, r := range pending {
		add(r)
	}

	type groupedSeries struct {
		GroupKey  string
		ModelName string
		Hours     []model.StatsSiteModelHourly
	}
	groupedByAccount := make(map[int]map[string]*groupedSeries)
	for _, entry := range merged {
		key := entry.GroupKey + "\x00" + entry.ModelName
		grouped := groupedByAccount[entry.SiteAccountID]
		if grouped == nil {
			grouped = make(map[string]*groupedSeries)
			groupedByAccount[entry.SiteAccountID] = grouped
		}
		series, ok := grouped[key]
		if !ok {
			series = &groupedSeries{GroupKey: entry.GroupKey, ModelName: entry.ModelName}
			grouped[key] = series
		}
		series.Hours = append(series.Hours, *entry)
	}

	result := make(map[int]map[string]*model.SiteModelHistorySummary, len(ids))
	for _, id := range ids {
		result[id] = make(map[string]*model.SiteModelHistorySummary)
	}
	for accountID, grouped := range groupedByAccount {
		accountResult := result[accountID]
		if accountResult == nil {
			accountResult = make(map[string]*model.SiteModelHistorySummary, len(grouped))
			result[accountID] = accountResult
		}
		for key, series := range grouped {
			sort.Slice(series.Hours, func(i, j int) bool {
				return series.Hours[i].Hour < series.Hours[j].Hour
			})
			accountResult[key] = buildSiteModelSummary(series.Hours)
		}
	}
	return result, nil
}

// collectSiteModelHourlyPendingLocked 返回尚未落库的行：siteModelHourlyCache
// 中的内存桶，以及刷盘事务尚未结束的 persisting 快照。提交成功或失败 restore
// 后批次会被移除，因此不会与 DB 已提交数据重复合并。
// 调用方必须持有 siteModelHourlyCacheLock。
func collectSiteModelHourlyPendingLocked(accountSet map[int]struct{}, minHour int) []model.StatsSiteModelHourly {
	pending := make([]model.StatsSiteModelHourly, 0, len(siteModelHourlyCache)+len(siteModelHourlyPersisting))
	for k, entry := range siteModelHourlyCache {
		if _, ok := accountSet[k.SiteAccountID]; ok && k.Hour >= minHour {
			pending = append(pending, *entry)
		}
	}
	for _, batch := range siteModelHourlyPersisting {
		for _, row := range batch.rows {
			if _, ok := accountSet[row.SiteAccountID]; ok && row.Hour >= minHour {
				pending = append(pending, row)
			}
		}
	}
	return pending
}

// buildSiteModelSummary 把按时间排序的小时记录聚合为 SiteModelHistorySummary，
// 自适应选择桶宽。
func buildSiteModelSummary(hours []model.StatsSiteModelHourly) *model.SiteModelHistorySummary {
	summary := &model.SiteModelHistorySummary{}
	if len(hours) == 0 {
		return summary
	}

	var maxLast int64
	for i := range hours {
		summary.SuccessCount += int(hours[i].RequestSuccess)
		summary.FailureCount += int(hours[i].RequestFailed)
		if hours[i].LastRequestAt > maxLast {
			maxLast = hours[i].LastRequestAt
		}
	}

	earliestHour := hours[0].Hour
	latestHour := hours[len(hours)-1].Hour
	if maxLast > 0 {
		summary.LastRequestAt = &maxLast
	} else {
		// 兼容老数据：fallback 到该 hour 最后一秒
		latestSec := int64(latestHour+1)*3600 - 1
		summary.LastRequestAt = &latestSec
	}
	spanSeconds := int64((latestHour - earliestHour + 1) * 3600)

	bucketSpan := chooseBucketSpan(spanSeconds)
	summary.BucketSpan = bucketSpan

	bucketMap := make(map[int64]*model.SiteModelHistoryBucket)
	for _, h := range hours {
		hourStart := int64(h.Hour) * 3600
		bucketStart := hourStart - hourStart%int64(bucketSpan)
		bucket, ok := bucketMap[bucketStart]
		if !ok {
			bucket = &model.SiteModelHistoryBucket{Time: bucketStart}
			bucketMap[bucketStart] = bucket
		}
		bucket.Success += int(h.RequestSuccess)
		bucket.Failure += int(h.RequestFailed)
	}

	buckets := make([]model.SiteModelHistoryBucket, 0, len(bucketMap))
	for _, b := range bucketMap {
		buckets = append(buckets, *b)
	}
	sort.Slice(buckets, func(i, j int) bool {
		return buckets[i].Time < buckets[j].Time
	})
	summary.Buckets = buckets
	return summary
}

func chooseBucketSpan(spanSeconds int64) int {
	const (
		hour = int64(3600)
		day  = 24 * hour
		week = 7 * day
	)
	switch {
	case spanSeconds <= 24*hour:
		return int(hour)
	case spanSeconds <= 7*day:
		return int(6 * hour)
	case spanSeconds <= 30*day:
		return int(day)
	default:
		return int(week)
	}
}

// removeSiteModelHourlyPersistingLocked 按指针注销一个刷盘批次，
// 调用方必须持有 siteModelHourlyCacheLock。
func removeSiteModelHourlyPersistingLocked(batch *siteModelHourlyPersistingBatch) {
	for i, b := range siteModelHourlyPersisting {
		if b == batch {
			siteModelHourlyPersisting = append(siteModelHourlyPersisting[:i], siteModelHourlyPersisting[i+1:]...)
			return
		}
	}
}

// lookupChannelSiteBinding 查询并缓存 channelID → 站点绑定信息。
func lookupChannelSiteBinding(channelID int) (channelSiteBinding, error) {
	if cached, ok := siteBindingByChannelCache.Get(channelID); ok {
		return cached, nil
	}
	var binding model.SiteChannelBinding
	err := db.GetDB().Where("channel_id = ?", channelID).First(&binding).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			result := channelSiteBinding{Found: false}
			siteBindingByChannelCache.Set(channelID, result)
			return result, nil
		}
		return channelSiteBinding{}, err
	}
	baseGroupKey, _ := model.ParseSiteChannelBindingKey(binding.GroupKey)
	result := channelSiteBinding{
		SiteAccountID: binding.SiteAccountID,
		BaseGroupKey:  baseGroupKey,
		Found:         true,
	}
	siteBindingByChannelCache.Set(channelID, result)
	return result, nil
}

func deleteSiteModelHourlyCacheForAccounts(accountIDs []int) {
	if len(accountIDs) == 0 {
		return
	}
	accountSet := make(map[int]struct{}, len(accountIDs))
	for _, id := range accountIDs {
		accountSet[id] = struct{}{}
	}

	siteModelHourlyCacheLock.Lock()
	defer siteModelHourlyCacheLock.Unlock()
	for key := range siteModelHourlyCache {
		if _, ok := accountSet[key.SiteAccountID]; ok {
			delete(siteModelHourlyCache, key)
		}
	}
}

// invalidateSiteBindingCache 在站点账号变更时清理映射缓存。
func invalidateSiteBindingCache() {
	siteBindingByChannelCache.Clear()
}

// SiteChannelBindingCacheInvalidate clears the channel-to-site binding cache
// after an external projection component commits a binding change.
func SiteChannelBindingCacheInvalidate() {
	invalidateSiteBindingCache()
}
