package migrate

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/bestruirui/octopus/internal/model"
	"github.com/bestruirui/octopus/internal/transformer/outbound"
	"gorm.io/gorm"
)

const (
	strictCloudflareBaseURL = "https://api.cloudflare.com/client/v4/accounts/acc123/ai/v1"
	lookalikeCloudflareURL  = "https://api.cloudflare.com.evil.example/client/v4/accounts/acc123/ai/v1"
)

// createLegacyChannelsTable creates the legacy channels table with raw SQL
// and without the three protocol columns, so the migration has to add them.
func createLegacyChannelsTable(t *testing.T, db *gorm.DB) {
	t.Helper()

	if err := db.Exec(
		"CREATE TABLE channels (id INTEGER PRIMARY KEY, type INTEGER NOT NULL DEFAULT 0, base_urls TEXT NOT NULL DEFAULT '[]')",
	).Error; err != nil {
		t.Fatalf("create legacy channels table failed: %v", err)
	}
}

// createLegacyChannelsTableWithProtocolColumns creates the legacy channels
// table with the three protocol columns already present as nullable columns
// without defaults, mirroring a partially migrated schema.
func createLegacyChannelsTableWithProtocolColumns(t *testing.T, db *gorm.DB) {
	t.Helper()

	if err := db.Exec(
		"CREATE TABLE channels (id INTEGER PRIMARY KEY, type INTEGER NOT NULL DEFAULT 0, base_urls TEXT NOT NULL DEFAULT '[]', " +
			"openai_protocol_mode TEXT, openai_chat_capability TEXT, openai_responses_capability TEXT)",
	).Error; err != nil {
		t.Fatalf("create legacy channels table with protocol columns failed: %v", err)
	}
}

func insertLegacyChannel(t *testing.T, db *gorm.DB, id int, channelType int, baseURLsJSON string) {
	t.Helper()

	if err := db.Exec(
		"INSERT INTO channels (id, type, base_urls) VALUES (?, ?, ?)",
		id, channelType, baseURLsJSON,
	).Error; err != nil {
		t.Fatalf("insert legacy channel %d failed: %v", id, err)
	}
}

// insertLegacyChannelWithProtocol inserts a row into a channels table that
// already has the three protocol columns; nil values persist as NULL.
func insertLegacyChannelWithProtocol(t *testing.T, db *gorm.DB, id int, channelType int, baseURLsJSON string, mode, chat, responses interface{}) {
	t.Helper()

	if err := db.Exec(
		"INSERT INTO channels (id, type, base_urls, openai_protocol_mode, openai_chat_capability, openai_responses_capability) VALUES (?, ?, ?, ?, ?, ?)",
		id, channelType, baseURLsJSON, mode, chat, responses,
	).Error; err != nil {
		t.Fatalf("insert legacy channel %d with protocol columns failed: %v", id, err)
	}
}

func legacyBaseURLsJSON(t *testing.T, urls ...string) string {
	t.Helper()

	baseURLs := make([]model.BaseUrl, 0, len(urls))
	for _, url := range urls {
		baseURLs = append(baseURLs, model.BaseUrl{URL: url})
	}
	encoded, err := json.Marshal(baseURLs)
	if err != nil {
		t.Fatalf("marshal base urls failed: %v", err)
	}
	return string(encoded)
}

type channelProtocolRow struct {
	Type      int
	Mode      string
	Chat      string
	Responses string
}

func loadChannelProtocolRow(t *testing.T, db *gorm.DB, id int) channelProtocolRow {
	t.Helper()

	var row channelProtocolRow
	if err := db.Raw(
		"SELECT type AS type, openai_protocol_mode AS mode, openai_chat_capability AS chat, openai_responses_capability AS responses FROM channels WHERE id = ?",
		id,
	).Scan(&row).Error; err != nil {
		t.Fatalf("query channel %d protocol columns failed: %v", id, err)
	}
	return row
}

// channelColumnDefaults returns a map of column name to declared default
// value from PRAGMA table_info, with absent defaults omitted.
func channelColumnDefaults(t *testing.T, db *gorm.DB) map[string]string {
	t.Helper()

	var columns []struct {
		Name      string
		DfltValue *string
	}
	if err := db.Raw("PRAGMA table_info(channels)").Scan(&columns).Error; err != nil {
		t.Fatalf("read channels table info failed: %v", err)
	}
	defaults := make(map[string]string, len(columns))
	for _, column := range columns {
		if column.DfltValue != nil {
			defaults[column.Name] = strings.Trim(strings.TrimSpace(*column.DfltValue), `"'`)
		}
	}
	return defaults
}

// createCloudflareBindingTables creates the minimal site binding tables used
// by the migration to detect channels bound to cloudflare sites.
func createCloudflareBindingTables(t *testing.T, db *gorm.DB) {
	t.Helper()

	statements := []string{
		"CREATE TABLE sites (id INTEGER PRIMARY KEY, platform TEXT NOT NULL)",
		"CREATE TABLE site_accounts (id INTEGER PRIMARY KEY, site_id INTEGER NOT NULL)",
		"CREATE TABLE site_channel_bindings (id INTEGER PRIMARY KEY, site_account_id INTEGER NOT NULL, channel_id INTEGER NOT NULL)",
	}
	for _, statement := range statements {
		if err := db.Exec(statement).Error; err != nil {
			t.Fatalf("create binding table failed: %v", err)
		}
	}
}

func insertSite(t *testing.T, db *gorm.DB, id int, platform string) {
	t.Helper()

	if err := db.Exec("INSERT INTO sites (id, platform) VALUES (?, ?)", id, platform).Error; err != nil {
		t.Fatalf("insert site %d failed: %v", id, err)
	}
}

func insertSiteAccount(t *testing.T, db *gorm.DB, id, siteID int) {
	t.Helper()

	if err := db.Exec("INSERT INTO site_accounts (id, site_id) VALUES (?, ?)", id, siteID).Error; err != nil {
		t.Fatalf("insert site account %d failed: %v", id, err)
	}
}

func insertSiteChannelBinding(t *testing.T, db *gorm.DB, id, siteAccountID, channelID int) {
	t.Helper()

	if err := db.Exec("INSERT INTO site_channel_bindings (id, site_account_id, channel_id) VALUES (?, ?, ?)", id, siteAccountID, channelID).Error; err != nil {
		t.Fatalf("insert site channel binding %d failed: %v", id, err)
	}
}

func mustMigrateChannelOpenAIProtocol(t *testing.T, db *gorm.DB) {
	t.Helper()

	if err := migrateChannelOpenAIProtocolCapabilities(db); err != nil {
		t.Fatalf("migrateChannelOpenAIProtocolCapabilities returned error: %v", err)
	}
}

func strPtr(value string) *string {
	return &value
}

// TestMigrateChannelOpenAIProtocolSkipsWhenChannelsTableMissing verifies the
// migration is a no-op returning nil when the channels table does not exist.
func TestMigrateChannelOpenAIProtocolSkipsWhenChannelsTableMissing(t *testing.T) {
	db := openMigrationTestDB(t)

	if err := migrateChannelOpenAIProtocolCapabilities(db); err != nil {
		t.Fatalf("expected nil error without channels table, got %v", err)
	}
	if db.Migrator().HasTable("channels") {
		t.Fatalf("expected migration not to create the channels table")
	}
}

// TestMigrateChannelOpenAIProtocolAddsColumnsWithExactNamesAndDefaults
// verifies that on a legacy channels table the migration adds exactly the
// three documented columns, backfills existing rows to auto/unknown and that
// new rows inserted afterwards pick up the column defaults auto/unknown.
func TestMigrateChannelOpenAIProtocolAddsColumnsWithExactNamesAndDefaults(t *testing.T) {
	db := openMigrationTestDB(t)
	createLegacyChannelsTable(t, db)
	insertLegacyChannel(t, db, 1, int(outbound.OutboundTypeOpenAIResponse), legacyBaseURLsJSON(t, "https://api.example.com/v1"))

	mustMigrateChannelOpenAIProtocol(t, db)

	defaults := channelColumnDefaults(t, db)
	for column, want := range map[string]string{
		"openai_protocol_mode":        "auto",
		"openai_chat_capability":      "unknown",
		"openai_responses_capability": "unknown",
	} {
		got, ok := defaults[column]
		if !ok {
			t.Fatalf("expected column %q to be added with a default, table columns: %#v", column, defaults)
		}
		if got != want {
			t.Fatalf("expected column %q default %s, got %s", column, want, got)
		}
	}

	if got := loadChannelProtocolRow(t, db, 1); got.Mode != string(model.OpenAIProtocolModeAuto) ||
		got.Chat != string(model.OpenAIProtocolCapabilityUnknown) ||
		got.Responses != string(model.OpenAIProtocolCapabilityUnknown) {
		t.Fatalf("expected legacy row backfilled to auto/unknown/unknown, got %#v", got)
	}

	// A row inserted after the migration must default to auto/unknown without
	// listing the columns explicitly.
	if err := db.Exec(
		"INSERT INTO channels (id, type, base_urls) VALUES (2, ?, ?)",
		int(outbound.OutboundTypeOpenAIChat), legacyBaseURLsJSON(t, "https://api.example.com/v1"),
	).Error; err != nil {
		t.Fatalf("insert post-migration channel failed: %v", err)
	}
	if got := loadChannelProtocolRow(t, db, 2); got.Mode != string(model.OpenAIProtocolModeAuto) ||
		got.Chat != string(model.OpenAIProtocolCapabilityUnknown) ||
		got.Responses != string(model.OpenAIProtocolCapabilityUnknown) {
		t.Fatalf("expected post-migration row to default to auto/unknown/unknown, got %#v", got)
	}
}

// TestMigrateChannelOpenAIProtocolBackfillsNullAndEmptyValues verifies that
// NULL and empty-string values are backfilled to auto/unknown while existing
// non-empty values are preserved.
func TestMigrateChannelOpenAIProtocolBackfillsNullAndEmptyValues(t *testing.T) {
	db := openMigrationTestDB(t)
	createLegacyChannelsTableWithProtocolColumns(t, db)

	insertLegacyChannelWithProtocol(t, db, 1, int(outbound.OutboundTypeOpenAIChat), legacyBaseURLsJSON(t, "https://a.example.com/v1"), nil, nil, nil)
	insertLegacyChannelWithProtocol(t, db, 2, int(outbound.OutboundTypeOpenAIChat), legacyBaseURLsJSON(t, "https://b.example.com/v1"), strPtr(""), strPtr(""), strPtr(""))
	insertLegacyChannelWithProtocol(t, db, 3, int(outbound.OutboundTypeOpenAIChat), legacyBaseURLsJSON(t, "https://c.example.com/v1"),
		strPtr(string(model.OpenAIProtocolModeChatOnly)),
		strPtr(string(model.OpenAIProtocolCapabilitySupported)),
		strPtr(string(model.OpenAIProtocolCapabilityUnsupported)))

	mustMigrateChannelOpenAIProtocol(t, db)

	for _, id := range []int{1, 2} {
		if got := loadChannelProtocolRow(t, db, id); got.Mode != string(model.OpenAIProtocolModeAuto) ||
			got.Chat != string(model.OpenAIProtocolCapabilityUnknown) ||
			got.Responses != string(model.OpenAIProtocolCapabilityUnknown) {
			t.Fatalf("expected NULL/empty row %d backfilled to auto/unknown/unknown, got %#v", id, got)
		}
	}
	if got := loadChannelProtocolRow(t, db, 3); got.Mode != string(model.OpenAIProtocolModeChatOnly) ||
		got.Chat != string(model.OpenAIProtocolCapabilitySupported) ||
		got.Responses != string(model.OpenAIProtocolCapabilityUnsupported) {
		t.Fatalf("expected non-empty values of row 3 preserved, got %#v", got)
	}
}

// TestMigrateChannelOpenAIProtocolForcesChatOnlyForStrictCloudflareURLs
// verifies that a channel whose base URLs include a strict Cloudflare
// Workers AI base URL is forced to openai_chat/chat_only/supported/
// unsupported.
func TestMigrateChannelOpenAIProtocolForcesChatOnlyForStrictCloudflareURLs(t *testing.T) {
	db := openMigrationTestDB(t)
	createLegacyChannelsTableWithProtocolColumns(t, db)

	// A strict URL mixed with a normal one still forces the channel.
	insertLegacyChannelWithProtocol(t, db, 1, int(outbound.OutboundTypeOpenAIResponse),
		legacyBaseURLsJSON(t, "https://api.example.com/v1", strictCloudflareBaseURL),
		strPtr(string(model.OpenAIProtocolModeResponsesOnly)),
		strPtr(string(model.OpenAIProtocolCapabilityUnsupported)),
		strPtr(string(model.OpenAIProtocolCapabilitySupported)))

	mustMigrateChannelOpenAIProtocol(t, db)

	if got := loadChannelProtocolRow(t, db, 1); got.Type != int(outbound.OutboundTypeOpenAIChat) ||
		got.Mode != string(model.OpenAIProtocolModeChatOnly) ||
		got.Chat != string(model.OpenAIProtocolCapabilitySupported) ||
		got.Responses != string(model.OpenAIProtocolCapabilityUnsupported) {
		t.Fatalf("expected cloudflare url channel forced to openai_chat/chat_only/supported/unsupported, got %#v", got)
	}
}

// TestMigrateChannelOpenAIProtocolForcesChatOnlyForCloudflareSiteBindings
// verifies that a channel bound to a cloudflare site through
// sites/site_accounts/site_channel_bindings is forced to
// openai_chat/chat_only even without any cloudflare base URL.
func TestMigrateChannelOpenAIProtocolForcesChatOnlyForCloudflareSiteBindings(t *testing.T) {
	db := openMigrationTestDB(t)
	createLegacyChannelsTableWithProtocolColumns(t, db)
	createCloudflareBindingTables(t, db)

	insertLegacyChannelWithProtocol(t, db, 1, int(outbound.OutboundTypeOpenAIResponse),
		legacyBaseURLsJSON(t, "https://api.example.com/v1"), nil, nil, nil)
	insertLegacyChannelWithProtocol(t, db, 2, int(outbound.OutboundTypeOpenAIResponse),
		legacyBaseURLsJSON(t, "https://api.example.com/v1"), nil, nil, nil)

	insertSite(t, db, 1, string(model.SitePlatformCloudflare))
	insertSite(t, db, 2, "openai")
	insertSiteAccount(t, db, 1, 1)
	insertSiteAccount(t, db, 2, 2)
	insertSiteChannelBinding(t, db, 1, 1, 1)
	insertSiteChannelBinding(t, db, 2, 2, 2)

	mustMigrateChannelOpenAIProtocol(t, db)

	if got := loadChannelProtocolRow(t, db, 1); got.Type != int(outbound.OutboundTypeOpenAIChat) ||
		got.Mode != string(model.OpenAIProtocolModeChatOnly) ||
		got.Chat != string(model.OpenAIProtocolCapabilitySupported) ||
		got.Responses != string(model.OpenAIProtocolCapabilityUnsupported) {
		t.Fatalf("expected cloudflare-bound channel forced to openai_chat/chat_only/supported/unsupported, got %#v", got)
	}
	if got := loadChannelProtocolRow(t, db, 2); got.Type != int(outbound.OutboundTypeOpenAIResponse) ||
		got.Mode != string(model.OpenAIProtocolModeAuto) ||
		got.Chat != string(model.OpenAIProtocolCapabilityUnknown) ||
		got.Responses != string(model.OpenAIProtocolCapabilityUnknown) {
		t.Fatalf("expected non-cloudflare-bound channel untouched apart from backfill, got %#v", got)
	}
}

// TestMigrateChannelOpenAIProtocolLeavesNonCloudflareChannelsUntouched
// verifies that normal channels, lookalike hosts such as
// api.cloudflare.com.evil.example and non-strict cloudflare URLs are not
// forced to chat_only.
func TestMigrateChannelOpenAIProtocolLeavesNonCloudflareChannelsUntouched(t *testing.T) {
	db := openMigrationTestDB(t)
	createLegacyChannelsTableWithProtocolColumns(t, db)

	insertLegacyChannelWithProtocol(t, db, 1, int(outbound.OutboundTypeOpenAIResponse),
		legacyBaseURLsJSON(t, "https://api.example.com/v1"), nil, nil, nil)
	insertLegacyChannelWithProtocol(t, db, 2, int(outbound.OutboundTypeOpenAIResponse),
		legacyBaseURLsJSON(t, lookalikeCloudflareURL), nil, nil, nil)
	insertLegacyChannelWithProtocol(t, db, 3, int(outbound.OutboundTypeOpenAIResponse),
		legacyBaseURLsJSON(t, "http://api.cloudflare.com/client/v4/accounts/acc123/ai/v1"), nil, nil, nil)
	insertLegacyChannelWithProtocol(t, db, 4, int(outbound.OutboundTypeOpenAIResponse),
		legacyBaseURLsJSON(t, "https://api.cloudflare.com/client/v4/accounts/acc123/ai/v1?x=1"), nil, nil, nil)
	insertLegacyChannelWithProtocol(t, db, 5, int(outbound.OutboundTypeOpenAIResponse),
		legacyBaseURLsJSON(t, "https://api.cloudflare.com/v1"), nil, nil, nil)

	mustMigrateChannelOpenAIProtocol(t, db)

	for _, id := range []int{1, 2, 3, 4, 5} {
		if got := loadChannelProtocolRow(t, db, id); got.Type != int(outbound.OutboundTypeOpenAIResponse) ||
			got.Mode != string(model.OpenAIProtocolModeAuto) ||
			got.Chat != string(model.OpenAIProtocolCapabilityUnknown) ||
			got.Responses != string(model.OpenAIProtocolCapabilityUnknown) {
			t.Fatalf("expected non-cloudflare channel %d untouched apart from backfill, got %#v", id, got)
		}
	}
}

// TestMigrateChannelOpenAIProtocolIsIdempotent verifies that running the
// migration twice converges to the same state for backfilled, forced and
// untouched channels.
func TestMigrateChannelOpenAIProtocolIsIdempotent(t *testing.T) {
	db := openMigrationTestDB(t)
	createLegacyChannelsTableWithProtocolColumns(t, db)
	createCloudflareBindingTables(t, db)

	insertLegacyChannelWithProtocol(t, db, 1, int(outbound.OutboundTypeOpenAIChat),
		legacyBaseURLsJSON(t, "https://api.example.com/v1"), nil, nil, nil)
	insertLegacyChannelWithProtocol(t, db, 2, int(outbound.OutboundTypeOpenAIResponse),
		legacyBaseURLsJSON(t, strictCloudflareBaseURL),
		strPtr(string(model.OpenAIProtocolModeResponsesOnly)), nil, nil)
	insertLegacyChannelWithProtocol(t, db, 3, int(outbound.OutboundTypeOpenAIResponse),
		legacyBaseURLsJSON(t, "https://api.example.com/v1"), nil, nil, nil)
	insertSite(t, db, 1, string(model.SitePlatformCloudflare))
	insertSiteAccount(t, db, 1, 1)
	insertSiteChannelBinding(t, db, 1, 1, 3)

	mustMigrateChannelOpenAIProtocol(t, db)
	firstRun := make([]channelProtocolRow, 0, 3)
	for _, id := range []int{1, 2, 3} {
		firstRun = append(firstRun, loadChannelProtocolRow(t, db, id))
	}

	mustMigrateChannelOpenAIProtocol(t, db)
	for index, id := range []int{1, 2, 3} {
		if got := loadChannelProtocolRow(t, db, id); got != firstRun[index] {
			t.Fatalf("expected second migration run to be idempotent for channel %d, first %#v, second %#v", id, firstRun[index], got)
		}
	}

	wantForced := channelProtocolRow{
		Type:      int(outbound.OutboundTypeOpenAIChat),
		Mode:      string(model.OpenAIProtocolModeChatOnly),
		Chat:      string(model.OpenAIProtocolCapabilitySupported),
		Responses: string(model.OpenAIProtocolCapabilityUnsupported),
	}
	if firstRun[1] != wantForced || firstRun[2] != wantForced {
		t.Fatalf("expected cloudflare channels forced on first run, got %#v and %#v", firstRun[1], firstRun[2])
	}
	wantNormal := channelProtocolRow{
		Type:      int(outbound.OutboundTypeOpenAIChat),
		Mode:      string(model.OpenAIProtocolModeAuto),
		Chat:      string(model.OpenAIProtocolCapabilityUnknown),
		Responses: string(model.OpenAIProtocolCapabilityUnknown),
	}
	if firstRun[0] != wantNormal {
		t.Fatalf("expected normal channel backfilled on first run, got %#v", firstRun[0])
	}
}

// TestMigrateChannelOpenAIProtocolRunsAtomically verifies that the migration
// runs in a single transaction: when the final forced Cloudflare update
// fails, the earlier backfill writes are rolled back instead of leaving a
// half-migrated state behind.
func TestMigrateChannelOpenAIProtocolRunsAtomically(t *testing.T) {
	db := openMigrationTestDB(t)
	createLegacyChannelsTableWithProtocolColumns(t, db)

	// One Cloudflare channel forces the final UPDATE; the trigger aborts it
	// so the migration must fail and roll the backfill writes back too.
	insertLegacyChannelWithProtocol(t, db, 1, int(outbound.OutboundTypeOpenAIResponse),
		legacyBaseURLsJSON(t, strictCloudflareBaseURL), nil, nil, nil)
	if err := db.Exec(
		"CREATE TRIGGER block_channel_type_update BEFORE UPDATE OF type ON channels " +
			"BEGIN SELECT RAISE(ABORT, 'type update blocked'); END",
	).Error; err != nil {
		t.Fatalf("create blocking trigger failed: %v", err)
	}

	if err := migrateChannelOpenAIProtocolCapabilities(db); err == nil {
		t.Fatalf("expected migration to fail when the forced cloudflare update is aborted")
	}

	if got := loadChannelProtocolRow(t, db, 1); got.Mode != "" || got.Chat != "" || got.Responses != "" {
		t.Fatalf("expected failed migration to roll back the backfill writes, got %#v", got)
	}
}

// TestMigrateChannelOpenAIProtocolLeavesNonOpenAITypesUnchanged verifies that
// a Cloudflare-bound channel with a non-OpenAI type (e.g. Anthropic or Gemini
// via a gateway) is left completely untouched: runtime normalization forces
// non-OpenAI channels back to auto mode anyway, and retyping or forcing
// chat-only on them would silently break a working setup.
func TestMigrateChannelOpenAIProtocolLeavesNonOpenAITypesUnchanged(t *testing.T) {
	db := openMigrationTestDB(t)
	createLegacyChannelsTableWithProtocolColumns(t, db)
	createCloudflareBindingTables(t, db)

	insertLegacyChannelWithProtocol(t, db, 1, int(outbound.OutboundTypeAnthropic),
		legacyBaseURLsJSON(t, "https://gateway.example.com/v1"), nil, nil, nil)
	insertSite(t, db, 1, string(model.SitePlatformCloudflare))
	insertSiteAccount(t, db, 1, 1)
	insertSiteChannelBinding(t, db, 1, 1, 1)

	mustMigrateChannelOpenAIProtocol(t, db)

	got := loadChannelProtocolRow(t, db, 1)
	if got.Type != int(outbound.OutboundTypeAnthropic) {
		t.Fatalf("expected non-openai channel type to be preserved, got type %d", got.Type)
	}
	if got.Mode != string(model.OpenAIProtocolModeAuto) ||
		got.Chat != string(model.OpenAIProtocolCapabilityUnknown) ||
		got.Responses != string(model.OpenAIProtocolCapabilityUnknown) {
		t.Fatalf("expected non-openai cloudflare-bound channel to stay auto/unknown after backfill, got %#v", got)
	}
}

// TestRunMigrationsWithRecordRetriesFailedMigration verifies that a failed
// migration is persisted with a distinct status and is retried on the next run.
func TestRunMigrationsWithRecordRetriesFailedMigration(t *testing.T) {
	db := openMigrationTestDB(t)
	const version = 19001

	if err := runMigrationsWithRecord(db, []Migration{{
		Version: version,
		Up: func(*gorm.DB) error {
			return gorm.ErrInvalidData
		},
	}}); err == nil {
		t.Fatal("expected first migration attempt to fail")
	}

	var record MigrationRecord
	if err := db.First(&record, version).Error; err != nil {
		t.Fatalf("load failed migration record: %v", err)
	}
	if record.Status != MigrationRecordStatusFailed {
		t.Fatalf("expected failed status %d, got %d", MigrationRecordStatusFailed, record.Status)
	}
	if record.Status == MigrationRecordStatusSuccess {
		t.Fatal("failed migration status must not equal success status")
	}

	attempts := 0
	if err := runMigrationsWithRecord(db, []Migration{{
		Version: version,
		Up: func(*gorm.DB) error {
			attempts++
			return nil
		},
	}}); err != nil {
		t.Fatalf("expected failed migration to be retried successfully: %v", err)
	}
	if attempts != 1 {
		t.Fatalf("expected one retry attempt, got %d", attempts)
	}
	if err := db.First(&record, version).Error; err != nil {
		t.Fatalf("reload successful migration record: %v", err)
	}
	if record.Status != MigrationRecordStatusSuccess {
		t.Fatalf("expected successful status %d after retry, got %d", MigrationRecordStatusSuccess, record.Status)
	}
}
