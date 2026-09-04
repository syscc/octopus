package op

import (
	"context"
	"fmt"
	"sync"
	"testing"

	dbpkg "github.com/bestruirui/octopus/internal/db"
	"github.com/bestruirui/octopus/internal/model"
	"github.com/bestruirui/octopus/internal/transformer/outbound"
	"gorm.io/gorm"
)

// failRefreshMarker marks a context whose channel-row SELECT queries must
// fail, so tests can exercise the committed-but-unrefreshable path of
// ChannelUpdate without touching the transaction itself.
type failRefreshMarker struct{}

// failKeyUpdateMarker marks a context whose channel_keys UPDATE statements
// must fail, so tests can exercise the pending-restore path of
// ChannelKeySaveDB.
type failKeyUpdateMarker struct{}

// registerFailingQueryCallback injects an error into GORM query callbacks for
// statements on the given table, but only when the statement context carries
// the marker. It returns a func that unregisters the callback.
func registerFailingQueryCallback(t *testing.T, table string, marker any, failAt int) func() {
	t.Helper()
	if failAt <= 0 {
		failAt = 1
	}

	name := "test_fail_query_" + table + "_" + t.Name()
	var mu sync.Mutex
	matched := 0
	err := dbpkg.GetDB().Callback().Query().Before("gorm:query").Register(name, func(tx *gorm.DB) {
		if tx.Statement == nil || tx.Statement.Table != table {
			return
		}
		if tx.Statement.Context == nil || tx.Statement.Context.Value(marker) == nil {
			return
		}
		mu.Lock()
		matched++
		shouldFail := matched == failAt
		mu.Unlock()
		if shouldFail {
			_ = tx.AddError(context.DeadlineExceeded)
		}
	})
	if err != nil {
		t.Fatalf("register failing query callback failed: %v", err)
	}
	return func() {
		_ = dbpkg.GetDB().Callback().Query().Remove(name)
	}
}

// registerFailingUpdateCallback injects an error into GORM update callbacks
// for statements on the given table, but only when the statement context
// carries the marker.
func registerFailingUpdateCallback(t *testing.T, table string, marker any) func() {
	t.Helper()

	name := "test_fail_update_" + table + "_" + t.Name()
	err := dbpkg.GetDB().Callback().Update().Before("gorm:update").Register(name, func(tx *gorm.DB) {
		if tx.Statement == nil || tx.Statement.Table != table {
			return
		}
		if tx.Statement.Context == nil || tx.Statement.Context.Value(marker) == nil {
			return
		}
		_ = tx.AddError(context.DeadlineExceeded)
	})
	if err != nil {
		t.Fatalf("register failing update callback failed: %v", err)
	}
	return func() {
		_ = dbpkg.GetDB().Callback().Update().Remove(name)
	}
}

// loadChannelKeyRowFromDB reads one channel_keys row straight from the
// database so assertions do not depend on the runtime caches.
func loadChannelKeyRowFromDB(t *testing.T, ctx context.Context, keyID int) model.ChannelKey {
	t.Helper()

	var row model.ChannelKey
	if err := dbpkg.GetDB().WithContext(ctx).First(&row, keyID).Error; err != nil {
		t.Fatalf("query channel key %d from db failed: %v", keyID, err)
	}
	return row
}

// createChannelWithKeys creates one channel with the given key secrets and
// returns the cached channel with the persisted key rows (IDs backfilled).
func createChannelWithKeys(t *testing.T, ctx context.Context, name string, keySecrets []string) *model.Channel {
	t.Helper()

	channel := &model.Channel{
		Name:     name,
		Type:     outbound.OutboundTypeOpenAIChat,
		Enabled:  true,
		BaseUrls: []model.BaseUrl{{URL: "https://api.example.com/v1"}},
	}
	for i, secret := range keySecrets {
		channel.Keys = append(channel.Keys, model.ChannelKey{
			Enabled:    true,
			ChannelKey: secret,
			Remark:     fmt.Sprintf("key-%d", i),
		})
	}
	if err := ChannelCreate(channel, ctx); err != nil {
		t.Fatalf("ChannelCreate failed: %v", err)
	}
	cached, err := ChannelGet(channel.ID, ctx)
	if err != nil {
		t.Fatalf("ChannelGet after create failed: %v", err)
	}
	return cached
}

// cachedChannelKeyByID returns the key served by ChannelGet for the channel,
// failing the test when the key is (or is not) cached as expected.
func cachedChannelKeyByID(t *testing.T, ctx context.Context, channelID int, keyID int, wantPresent bool) model.ChannelKey {
	t.Helper()

	channel, err := ChannelGet(channelID, ctx)
	if err != nil {
		t.Fatalf("ChannelGet failed: %v", err)
	}
	for _, key := range channel.Keys {
		if key.ID == keyID {
			if !wantPresent {
				t.Fatalf("expected key %d to be absent from cached channel keys", keyID)
			}
			return key
		}
	}
	if wantPresent {
		t.Fatalf("expected key %d to be present in cached channel keys", keyID)
	}
	return model.ChannelKey{}
}

// TestChannelRecordNoopWithDBAlreadyTargetSyncsStaleCache covers the MySQL
// RowsAffected semantics: a guarded UPDATE that changes nothing (the DB column
// already holds the recorded capability while the cache is stale) reports zero
// affected rows. The record must re-read the authoritative columns and sync
// the stale cache instead of failing or faking a manual-mode takeover.
func TestChannelRecordNoopWithDBAlreadyTargetSyncsStaleCache(t *testing.T) {
	ctx := setupOpenAIProtocolTestDB(t)
	channel := createOpenAIProtocolTestChannel(t, ctx, "mysql-noop-channel", outbound.OutboundTypeOpenAIChat,
		[]model.BaseUrl{{URL: "https://api.example.com/v1"}})

	// A committed learning result the cache has not seen...
	if err := dbpkg.GetDB().Exec(
		"UPDATE channels SET openai_chat_capability = ? WHERE id = ?",
		model.OpenAIProtocolCapabilitySupported, channel.ID,
	).Error; err != nil {
		t.Fatalf("seed committed capability failed: %v", err)
	}

	// ... plus a trigger that mimics MySQL reporting zero affected rows for
	// value-identical guarded updates, so the record's UPDATE is a no-op.
	if err := dbpkg.GetDB().Exec(
		"CREATE TRIGGER noop_identical_chat_update BEFORE UPDATE OF openai_chat_capability ON channels " +
			"WHEN NEW.openai_chat_capability IS OLD.openai_chat_capability BEGIN SELECT RAISE(IGNORE); END",
	).Error; err != nil {
		t.Fatalf("create noop trigger failed: %v", err)
	}
	t.Cleanup(func() {
		_ = dbpkg.GetDB().Exec("DROP TRIGGER IF EXISTS noop_identical_chat_update").Error
	})

	// The cache still holds the fresh-channel state (chat unknown), so the
	// record does not take the early-return path and hits the no-op UPDATE.
	mustRecordOpenAIProtocolCapability(t, ctx, channel.ID, outbound.OutboundTypeOpenAIChat, model.OpenAIProtocolCapabilitySupported)

	want := channelProtocolState{Mode: model.OpenAIProtocolModeAuto, Chat: model.OpenAIProtocolCapabilitySupported, Responses: model.OpenAIProtocolCapabilityUnknown}
	if got := loadChannelProtocolStateFromDB(t, ctx, channel.ID); got != want {
		t.Fatalf("expected db to keep the committed capability, want %#v, got %#v", want, got)
	}
	if got := cachedChannelProtocolState(t, ctx, channel.ID); got != want {
		t.Fatalf("expected noop update to sync the stale cache to the db state, want %#v, got %#v", want, got)
	}
}

// TestChannelKeyPendingRuntimeSurvivesFullAndSingleRefresh verifies that both
// refresh paths keep the runtime fields (status_code / last_use_time_stamp /
// total_cost) and the pending-save marker of keys updated by
// ChannelKeyUpdate, instead of rolling them back to the stale database row,
// and that a later ChannelKeySaveDB persists the in-memory values.
func TestChannelKeyPendingRuntimeSurvivesFullAndSingleRefresh(t *testing.T) {
	ctx := setupOpenAIProtocolTestDB(t)
	channel := createChannelWithKeys(t, ctx, "pending-runtime-channel", []string{"sk-pending-key"})

	key := cachedChannelKeyByID(t, ctx, channel.ID, channel.Keys[0].ID, true)
	if key.TotalCost != 0 || key.StatusCode != 0 || key.LastUseTimeStamp != 0 {
		t.Fatalf("expected zeroed runtime fields on a fresh key, got %#v", key)
	}

	runtimeKey := key
	runtimeKey.StatusCode = 429
	runtimeKey.LastUseTimeStamp = 1234567890
	runtimeKey.TotalCost = 12.5
	if err := ChannelKeyUpdate(runtimeKey); err != nil {
		t.Fatalf("ChannelKeyUpdate failed: %v", err)
	}

	assertPendingRuntimeSurvives := func(stage string) {
		t.Helper()
		cachedKey := cachedChannelKeyByID(t, ctx, channel.ID, key.ID, true)
		if cachedKey.StatusCode != 429 || cachedKey.LastUseTimeStamp != 1234567890 || cachedKey.TotalCost != 12.5 {
			t.Fatalf("%s: expected pending runtime fields to survive, got %#v", stage, cachedKey)
		}
		if served, ok := channelKeyCache.Get(key.ID); !ok ||
			served.StatusCode != 429 || served.LastUseTimeStamp != 1234567890 || served.TotalCost != 12.5 {
			t.Fatalf("%s: expected key cache to keep pending runtime fields, got %#v ok=%v", stage, served, ok)
		}
		pending := channelKeyPendingSnapshot()
		if _, isPending := pending[key.ID]; !isPending {
			t.Fatalf("%s: expected key %d to stay pending after refresh", stage, key.ID)
		}
	}

	if err := channelRefreshCache(ctx); err != nil {
		t.Fatalf("channelRefreshCache failed: %v", err)
	}
	assertPendingRuntimeSurvives("full refresh")

	if err := channelRefreshCacheByID(channel.ID, ctx); err != nil {
		t.Fatalf("channelRefreshCacheByID failed: %v", err)
	}
	assertPendingRuntimeSurvives("single refresh")

	// The database row must still hold the stale values: only the save task
	// is allowed to persist the pending runtime fields.
	if row := loadChannelKeyRowFromDB(t, ctx, key.ID); row.StatusCode != 0 || row.TotalCost != 0 || row.LastUseTimeStamp != 0 {
		t.Fatalf("expected db row to keep stale runtime values before save, got %#v", row)
	}

	if err := ChannelKeySaveDB(ctx); err != nil {
		t.Fatalf("ChannelKeySaveDB failed: %v", err)
	}
	if row := loadChannelKeyRowFromDB(t, ctx, key.ID); row.StatusCode != 429 || row.LastUseTimeStamp != 1234567890 || row.TotalCost != 12.5 {
		t.Fatalf("expected save to persist runtime fields, got %#v", row)
	}
	if pending := channelKeyPendingSnapshot(); len(pending) != 0 {
		t.Fatalf("expected pending set to be empty after successful save, got %#v", pending)
	}
}

// TestDeletedChannelKeyNotResurrectedByLateUpdateAndSave covers the deletion
// path end to end: after a committed ChannelUpdate deletes a key, a late
// runtime ChannelKeyUpdate carrying the deleted key is a silent no-op and a
// following ChannelKeySaveDB must not INSERT the row back.
func TestDeletedChannelKeyNotResurrectedByLateUpdateAndSave(t *testing.T) {
	ctx := setupOpenAIProtocolTestDB(t)
	channel := createChannelWithKeys(t, ctx, "deleted-key-channel", []string{"sk-doomed-key", "sk-survivor-key"})

	doomed := channel.Keys[0]
	survivor := channel.Keys[1]

	if _, err := ChannelUpdate(&model.ChannelUpdateRequest{
		ID:           channel.ID,
		KeysToDelete: []int{doomed.ID},
	}, ctx); err != nil {
		t.Fatalf("ChannelUpdate deleting key failed: %v", err)
	}

	// A late runtime update carrying the deleted key must be a no-op that
	// does not resurrect the key in any cache or pending set.
	lateKey := doomed
	lateKey.StatusCode = 500
	lateKey.TotalCost = 99
	if err := ChannelKeyUpdate(lateKey); err != nil {
		t.Fatalf("late ChannelKeyUpdate for deleted key should return nil, got %v", err)
	}
	if _, stillCached := channelKeyCache.Get(doomed.ID); stillCached {
		t.Fatalf("expected deleted key %d to stay out of the key cache", doomed.ID)
	}
	if pending := channelKeyPendingSnapshot(); len(pending) != 0 {
		t.Fatalf("expected no pending keys after late update for deleted key, got %#v", pending)
	}
	cachedChannelKeyByID(t, ctx, channel.ID, doomed.ID, false)

	if err := ChannelKeySaveDB(ctx); err != nil {
		t.Fatalf("ChannelKeySaveDB failed: %v", err)
	}

	var doomedCount int64
	if err := dbpkg.GetDB().WithContext(ctx).Model(&model.ChannelKey{}).Where("id = ?", doomed.ID).Count(&doomedCount).Error; err != nil {
		t.Fatalf("count deleted key rows failed: %v", err)
	}
	if doomedCount != 0 {
		t.Fatalf("expected deleted key %d to stay deleted in db, got %d rows", doomed.ID, doomedCount)
	}

	// The survivor stays fully functional.
	survivorKey := cachedChannelKeyByID(t, ctx, channel.ID, survivor.ID, true)
	if survivorKey.ChannelKey != survivor.ChannelKey {
		t.Fatalf("expected survivor key material to survive, got %q", survivorKey.ChannelKey)
	}
	if got := loadChannelKeyRowFromDB(t, ctx, survivor.ID); got.ID != survivor.ID {
		t.Fatalf("expected survivor row in db, got %#v", got)
	}
}

// TestChannelKeySaveDBOnlyUpdatesRuntimeColumns verifies the save path never
// overwrites admin-managed columns (enabled / channel_key / remark): only
// status_code / last_use_time_stamp / total_cost are written, even when the
// in-memory key still carries the pre-admin-edit values.
func TestChannelKeySaveDBOnlyUpdatesRuntimeColumns(t *testing.T) {
	ctx := setupOpenAIProtocolTestDB(t)
	channel := createChannelWithKeys(t, ctx, "admin-fields-channel", []string{"sk-orig-key"})

	key := cachedChannelKeyByID(t, ctx, channel.ID, channel.Keys[0].ID, true)

	// An admin rotates the key material in the DB directly; the runtime key
	// snapshot still holds the pre-edit admin fields.
	if err := dbpkg.GetDB().Exec(
		"UPDATE channel_keys SET enabled = ?, channel_key = ?, remark = ? WHERE id = ?",
		false, "sk-admin-rotated", "admin remark", key.ID,
	).Error; err != nil {
		t.Fatalf("seed admin values failed: %v", err)
	}

	runtimeKey := key
	runtimeKey.Enabled = true
	runtimeKey.ChannelKey = "sk-orig-key"
	runtimeKey.Remark = key.Remark
	runtimeKey.StatusCode = 503
	runtimeKey.LastUseTimeStamp = 1700000000
	runtimeKey.TotalCost = 3.25
	if err := ChannelKeyUpdate(runtimeKey); err != nil {
		t.Fatalf("ChannelKeyUpdate failed: %v", err)
	}
	if err := ChannelKeySaveDB(ctx); err != nil {
		t.Fatalf("ChannelKeySaveDB failed: %v", err)
	}

	row := loadChannelKeyRowFromDB(t, ctx, key.ID)
	if row.StatusCode != 503 || row.LastUseTimeStamp != 1700000000 || row.TotalCost != 3.25 {
		t.Fatalf("expected runtime columns to be persisted, got %#v", row)
	}
	if row.Enabled {
		t.Fatalf("expected admin enabled=false to survive the save")
	}
	if row.ChannelKey != "sk-admin-rotated" {
		t.Fatalf("expected admin channel_key to survive the save, got %q", row.ChannelKey)
	}
	if row.Remark != "admin remark" {
		t.Fatalf("expected admin remark to survive the save, got %q", row.Remark)
	}
}

// TestChannelKeySaveDBFailureRestoresPending verifies that a failing runtime
// UPDATE puts the pending marker back so the next save retries the key.
func TestChannelKeySaveDBFailureRestoresPending(t *testing.T) {
	ctx := setupOpenAIProtocolTestDB(t)
	channel := createChannelWithKeys(t, ctx, "save-failure-channel", []string{"sk-failing-key"})

	key := cachedChannelKeyByID(t, ctx, channel.ID, channel.Keys[0].ID, true)
	runtimeKey := key
	runtimeKey.StatusCode = 502
	runtimeKey.TotalCost = 1.5
	if err := ChannelKeyUpdate(runtimeKey); err != nil {
		t.Fatalf("ChannelKeyUpdate failed: %v", err)
	}

	markedCtx := context.WithValue(ctx, failKeyUpdateMarker{}, true)
	unregister := registerFailingUpdateCallback(t, "channel_keys", failKeyUpdateMarker{})
	if err := ChannelKeySaveDB(markedCtx); err == nil {
		t.Fatalf("expected ChannelKeySaveDB to fail with injected update error")
	}
	unregister()

	pending := channelKeyPendingSnapshot()
	if _, isPending := pending[key.ID]; !isPending {
		t.Fatalf("expected failed key %d to be back in the pending set", key.ID)
	}

	if err := ChannelKeySaveDB(ctx); err != nil {
		t.Fatalf("ChannelKeySaveDB retry failed: %v", err)
	}
	row := loadChannelKeyRowFromDB(t, ctx, key.ID)
	if row.StatusCode != 502 || row.TotalCost != 1.5 {
		t.Fatalf("expected retried save to persist runtime fields, got %#v", row)
	}
	if pending := channelKeyPendingSnapshot(); len(pending) != 0 {
		t.Fatalf("expected pending set to be empty after retry, got %#v", pending)
	}
}

// TestChannelUpdateRefreshFailureFallbackRebuildsKeys covers requirement 4
// end to end: when the post-commit refresh of a ChannelUpdate fails, the
// fallback cache must reflect the committed key set - deleted keys gone from
// channel.Keys / channelKeyCache / GetChannelKey, updated fields applied and
// created keys (with their IDs) visible.
func TestChannelUpdateRefreshFailureFallbackRebuildsKeys(t *testing.T) {
	ctx := setupOpenAIProtocolTestDB(t)
	channel := createChannelWithKeys(t, ctx, "fallback-keys-channel", []string{"sk-fallback-doomed", "sk-fallback-update", "sk-fallback-keep"})

	doomed := channel.Keys[0]
	updateKey := channel.Keys[1]
	keep := channel.Keys[2]

	unregister := registerFailingQueryCallback(t, "channels", failRefreshMarker{}, 2)
	defer unregister()
	failCtx := context.WithValue(ctx, failRefreshMarker{}, true)

	newEnabled := false
	newSecret := "sk-fallback-rotated"
	if _, err := ChannelUpdate(&model.ChannelUpdateRequest{
		ID:                 channel.ID,
		BypassManagedCheck: true,
		KeysToDelete:       []int{doomed.ID},
		KeysToUpdate: []model.ChannelKeyUpdateRequest{{
			ID:         updateKey.ID,
			Enabled:    &newEnabled,
			ChannelKey: &newSecret,
		}},
		KeysToAdd: []model.ChannelKeyAddRequest{{
			Enabled:    true,
			ChannelKey: "sk-fallback-added",
			Remark:     "added",
		}},
	}, failCtx); err == nil {
		t.Fatalf("expected ChannelUpdate to surface the refresh failure")
	}

	// The committed database rows are the ground truth: doomed gone, the
	// updated row rotated, the created row present with an ID.
	var doomedCount int64
	if err := dbpkg.GetDB().WithContext(ctx).Model(&model.ChannelKey{}).Where("id = ?", doomed.ID).Count(&doomedCount).Error; err != nil {
		t.Fatalf("count doomed rows failed: %v", err)
	}
	if doomedCount != 0 {
		t.Fatalf("expected doomed key to be deleted in db, got %d rows", doomedCount)
	}

	// Fallback cache: deleted key is gone from the channel key list, from
	// the key cache and from GetChannelKey selections.
	cachedChannelKeyByID(t, ctx, channel.ID, doomed.ID, false)
	if _, stillCached := channelKeyCache.Get(doomed.ID); stillCached {
		t.Fatalf("expected deleted key %d to be dropped from the key cache by the fallback", doomed.ID)
	}

	// The updated key reflects the committed fields but keeps pending
	// runtime state (none here, so the committed values apply).
	gotUpdated := cachedChannelKeyByID(t, ctx, channel.ID, updateKey.ID, true)
	if gotUpdated.Enabled {
		t.Fatalf("expected committed enabled=false on updated key")
	}
	if gotUpdated.ChannelKey != newSecret {
		t.Fatalf("expected committed rotated secret on updated key, got %q", gotUpdated.ChannelKey)
	}
	if served, ok := channelKeyCache.Get(updateKey.ID); !ok || served.ChannelKey != newSecret {
		t.Fatalf("expected key cache to serve the updated key, got %#v ok=%v", served, ok)
	}

	// The created key is visible with its ID in both caches.
	cached, err := ChannelGet(channel.ID, ctx)
	if err != nil {
		t.Fatalf("ChannelGet failed: %v", err)
	}
	var added *model.ChannelKey
	for i := range cached.Keys {
		if cached.Keys[i].ChannelKey == "sk-fallback-added" {
			added = &cached.Keys[i]
			break
		}
	}
	if added == nil || added.ID == 0 {
		t.Fatalf("expected created key with backfilled ID in fallback cache, got %#v", cached.Keys)
	}
	if served, ok := channelKeyCache.Get(added.ID); !ok || served.ChannelKey != "sk-fallback-added" {
		t.Fatalf("expected key cache to serve the created key, got %#v ok=%v", served, ok)
	}

	// GetChannelKey must select neither the deleted nor the disabled key.
	selected := cached.GetChannelKey()
	if selected.ID == doomed.ID || selected.ID == updateKey.ID {
		t.Fatalf("expected GetChannelKey to skip deleted/disabled keys, got %#v", selected)
	}
	if selected.ID != keep.ID {
		t.Fatalf("expected GetChannelKey to select the surviving enabled key %d, got %#v", keep.ID, selected)
	}
}

// TestChannelUpdateFallbackKeepsPendingRuntimeFields verifies that the
// fallback of a committed ChannelUpdate keeps the runtime fields of a key
// with a pending runtime update: the admin update only touches admin
// columns, and the not-yet-persisted runtime state must survive.
func TestChannelUpdateFallbackKeepsPendingRuntimeFields(t *testing.T) {
	ctx := setupOpenAIProtocolTestDB(t)
	channel := createChannelWithKeys(t, ctx, "fallback-pending-channel", []string{"sk-fallback-pending"})

	key := cachedChannelKeyByID(t, ctx, channel.ID, channel.Keys[0].ID, true)
	runtimeKey := key
	runtimeKey.StatusCode = 401
	runtimeKey.LastUseTimeStamp = 1700000123
	runtimeKey.TotalCost = 7.75
	if err := ChannelKeyUpdate(runtimeKey); err != nil {
		t.Fatalf("ChannelKeyUpdate failed: %v", err)
	}

	unregister := registerFailingQueryCallback(t, "channels", failRefreshMarker{}, 2)
	defer unregister()
	failCtx := context.WithValue(ctx, failRefreshMarker{}, true)

	newRemark := "rotated by admin"
	if _, err := ChannelUpdate(&model.ChannelUpdateRequest{
		ID:                 channel.ID,
		BypassManagedCheck: true,
		KeysToUpdate: []model.ChannelKeyUpdateRequest{{
			ID:     key.ID,
			Remark: &newRemark,
		}},
	}, failCtx); err == nil {
		t.Fatalf("expected ChannelUpdate to surface the refresh failure")
	}

	got := cachedChannelKeyByID(t, ctx, channel.ID, key.ID, true)
	if got.Remark != newRemark {
		t.Fatalf("expected committed remark in fallback cache, got %q", got.Remark)
	}
	if got.StatusCode != 401 || got.LastUseTimeStamp != 1700000123 || got.TotalCost != 7.75 {
		t.Fatalf("expected pending runtime fields to survive the fallback, got %#v", got)
	}
	if served, ok := channelKeyCache.Get(key.ID); !ok || served.StatusCode != 401 || served.TotalCost != 7.75 {
		t.Fatalf("expected key cache to keep pending runtime fields, got %#v ok=%v", served, ok)
	}
	if _, isPending := channelKeyPendingSnapshot()[key.ID]; !isPending {
		t.Fatalf("expected key %d to stay pending after the fallback", key.ID)
	}
}

// TestChannelRefreshDropsDeletedChannelAndPhantomEntries verifies the eviction
// half of the full refresh: channels that vanished from the database are
// dropped from the channel cache together with their key-cache entries and
// pending markers, while existing channels survive.
func TestChannelRefreshDropsDeletedChannelAndPhantomEntries(t *testing.T) {
	ctx := setupOpenAIProtocolTestDB(t)
	channel := createChannelWithKeys(t, ctx, "refresh-eviction-channel", []string{"sk-eviction-key"})

	ghostID := 987654
	ghostKeyID := 876543
	channelCache.Set(ghostID, model.Channel{
		ID:   ghostID,
		Name: "phantom-cache-channel",
		Keys: []model.ChannelKey{{ID: ghostKeyID, ChannelID: ghostID, ChannelKey: "sk-phantom"}},
	})
	channelKeyCache.Set(ghostKeyID, model.ChannelKey{ID: ghostKeyID, ChannelID: ghostID, ChannelKey: "sk-phantom"})
	channelKeyCacheNeedUpdateLock.Lock()
	channelKeyCacheNeedUpdate[ghostKeyID] = struct{}{}
	channelKeyCacheNeedUpdateLock.Unlock()

	if err := channelRefreshCache(ctx); err != nil {
		t.Fatalf("channelRefreshCache failed: %v", err)
	}

	if _, stillCached := channelCache.Get(ghostID); stillCached {
		t.Fatalf("expected phantom channel to be evicted from the channel cache")
	}
	if _, stillCached := channelKeyCache.Get(ghostKeyID); stillCached {
		t.Fatalf("expected phantom key to be evicted from the key cache")
	}
	if _, isPending := channelKeyPendingSnapshot()[ghostKeyID]; isPending {
		t.Fatalf("expected phantom key pending marker to be dropped")
	}

	if _, err := ChannelGet(channel.ID, ctx); err != nil {
		t.Fatalf("expected existing channel to survive the refresh: %v", err)
	}
	cachedChannelKeyByID(t, ctx, channel.ID, channel.Keys[0].ID, true)
}

// TestChannelRefreshEvictionRechecksDBUnderLock covers the eviction race: a
// channel whose row disappears from the database between the refresh's ID
// snapshot and the eviction step, but is re-created (same ID) before the
// evictor acquires the channel lock, must not be evicted. The eviction only
// deletes after re-checking the database under the channel mutation lock.
func TestChannelRefreshEvictionRechecksDBUnderLock(t *testing.T) {
	ctx := setupOpenAIProtocolTestDB(t)
	channel := createChannelWithKeys(t, ctx, "eviction-race-channel", []string{"sk-race-key"})

	// Hold the channel mutation lock so the refresh's per-channel reload and
	// eviction both block on it.
	unlock := lockChannelMutation(channel.ID)

	// The row disappears from the database while the lock is held, so the
	// refresh's ID snapshot (taken without the lock) cannot include it.
	if err := dbpkg.GetDB().Exec("DELETE FROM channel_keys WHERE channel_id = ?", channel.ID).Error; err != nil {
		unlock()
		t.Fatalf("delete channel keys failed: %v", err)
	}
	if err := dbpkg.GetDB().Exec("DELETE FROM channels WHERE id = ?", channel.ID).Error; err != nil {
		unlock()
		t.Fatalf("delete channel failed: %v", err)
	}

	refreshDone := make(chan error, 1)
	go func() {
		refreshDone <- channelRefreshCache(ctx)
	}()

	// The row comes back (a concurrent re-creation with the same ID) before
	// the lock is released: the evictor must re-check the database after
	// acquiring the lock and keep the channel.
	if err := dbpkg.GetDB().Exec(
		"INSERT INTO channels (id, name, type, enabled, base_urls, proxy_mode, ws_mode, openai_protocol_mode, openai_chat_capability, openai_responses_capability) "+
			"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
		channel.ID, channel.Name, outbound.OutboundTypeOpenAIChat, true,
		`[{"url":"https://api.example.com/v1","delay":0}]`,
		"direct", "inherit", string(model.OpenAIProtocolModeAuto),
		string(model.OpenAIProtocolCapabilityUnknown), string(model.OpenAIProtocolCapabilityUnknown),
	).Error; err != nil {
		unlock()
		t.Fatalf("re-insert channel failed: %v", err)
	}
	unlock()

	if err := <-refreshDone; err != nil {
		t.Fatalf("channelRefreshCache failed: %v", err)
	}

	if _, stillCached := channelCache.Get(channel.ID); !stillCached {
		t.Fatalf("expected re-created channel %d to survive the eviction re-check", channel.ID)
	}
}

// TestChannelRefreshRacingWithProtocolLearningStaysConsistent drives the real
// concurrency window: protocol learning commits guarded updates to the
// database while a full refresh runs. After the storm, the database, the
// channel cache and the key cache must agree on the learned state.
func TestChannelRefreshRacingWithProtocolLearningStaysConsistent(t *testing.T) {
	ctx := setupOpenAIProtocolTestDB(t)
	channel := createOpenAIProtocolTestChannel(t, ctx, "refresh-learning-race", outbound.OutboundTypeOpenAIChat,
		[]model.BaseUrl{{URL: "https://api.example.com/v1"}})

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		protocol := outbound.OutboundTypeOpenAIChat
		capability := model.OpenAIProtocolCapabilitySupported
		if i == 1 {
			protocol = outbound.OutboundTypeOpenAIResponse
			capability = model.OpenAIProtocolCapabilityUnsupported
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				if err := ChannelRecordOpenAIProtocolCapability(channel.ID, protocol, capability, ctx); err != nil {
					t.Errorf("concurrent record failed: %v", err)
					return
				}
			}
		}()
	}

	for i := 0; i < 25; i++ {
		if err := channelRefreshCache(ctx); err != nil {
			t.Fatalf("channelRefreshCache failed: %v", err)
		}
	}
	close(stop)
	wg.Wait()

	// Converge to the final state deterministically, then verify one last
	// full refresh serves exactly the committed database state.
	mustRecordOpenAIProtocolCapability(t, ctx, channel.ID, outbound.OutboundTypeOpenAIChat, model.OpenAIProtocolCapabilitySupported)
	mustRecordOpenAIProtocolCapability(t, ctx, channel.ID, outbound.OutboundTypeOpenAIResponse, model.OpenAIProtocolCapabilityUnsupported)
	if err := channelRefreshCache(ctx); err != nil {
		t.Fatalf("final channelRefreshCache failed: %v", err)
	}

	want := channelProtocolState{Mode: model.OpenAIProtocolModeAuto, Chat: model.OpenAIProtocolCapabilitySupported, Responses: model.OpenAIProtocolCapabilityUnsupported}
	if got := loadChannelProtocolStateFromDB(t, ctx, channel.ID); got != want {
		t.Fatalf("expected db to hold the learned state, want %#v, got %#v", want, got)
	}
	if got := cachedChannelProtocolState(t, ctx, channel.ID); got != want {
		t.Fatalf("expected cache to hold the learned state after refresh, want %#v, got %#v", want, got)
	}
}

func TestChannelDeleteClearsPendingKeyState(t *testing.T) {
	ctx := setupOpenAIProtocolTestDB(t)
	channel := createChannelWithKeys(t, ctx, "delete-pending-channel", []string{"delete-pending-key"})
	key := channel.Keys[0]
	key.StatusCode = 500
	if err := ChannelKeyUpdate(key); err != nil {
		t.Fatalf("mark key runtime update: %v", err)
	}
	if _, pending := channelKeyPendingSnapshot()[key.ID]; !pending {
		t.Fatalf("expected key %d to be pending before channel deletion", key.ID)
	}

	if err := ChannelDel(channel.ID, ctx); err != nil {
		t.Fatalf("delete channel: %v", err)
	}
	if _, cached := channelKeyCache.Get(key.ID); cached {
		t.Fatalf("deleted channel key %d remained in key cache", key.ID)
	}
	if _, pending := channelKeyPendingSnapshot()[key.ID]; pending {
		t.Fatalf("deleted channel key %d remained pending", key.ID)
	}
}
