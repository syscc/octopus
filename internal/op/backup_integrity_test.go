package op

import (
	"context"
	"fmt"
	"strings"
	"testing"

	dbpkg "github.com/bestruirui/octopus/internal/db"
	"github.com/bestruirui/octopus/internal/model"
	"github.com/bestruirui/octopus/internal/transformer/outbound"
	logpkg "github.com/bestruirui/octopus/internal/utils/log"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func TestDBImportRejectsUnmappedForeignKeysInsteadOfUsingLocalIDs(t *testing.T) {
	t.Run("channel key", func(t *testing.T) {
		ctx := setupBackupTestDB(t)
		channel := seedBackupIntegrityChannel(t, "local-channel-key-parent")
		dump := &model.DBDump{Version: 1, ChannelKeys: []model.ChannelKey{{
			ID: 10, ChannelID: channel.ID, Enabled: true, ChannelKey: "unmapped-channel-key",
		}}}
		assertImportRejectsUnmappedParent(t, ctx, dump, "channel_keys", &model.ChannelKey{})
	})

	t.Run("site account", func(t *testing.T) {
		ctx := setupBackupTestDB(t)
		site := seedBackupIntegritySite(t, "local-site-account-parent")
		dump := &model.DBDump{Version: 1, SiteAccounts: []model.SiteAccount{{
			ID: 11, SiteID: site.ID, Name: "unmapped-account",
			CredentialType: model.SiteCredentialTypeAPIKey, APIKey: "unmapped-account-key", Enabled: true,
		}}}
		assertImportRejectsUnmappedParent(t, ctx, dump, "site_accounts", &model.SiteAccount{})
	})

	t.Run("site token", func(t *testing.T) {
		ctx := setupBackupTestDB(t)
		_, account := seedBackupIntegritySiteAccount(t, "local-token-parent")
		dump := &model.DBDump{Version: 1, SiteTokens: []model.SiteToken{{
			ID: 12, SiteAccountID: account.ID, Token: "unmapped-token", GroupKey: "default", Enabled: true,
		}}}
		assertImportRejectsUnmappedParent(t, ctx, dump, "site_tokens", &model.SiteToken{})
	})

	t.Run("site user group", func(t *testing.T) {
		ctx := setupBackupTestDB(t)
		_, account := seedBackupIntegritySiteAccount(t, "local-user-group-parent")
		dump := &model.DBDump{Version: 1, SiteUserGroups: []model.SiteUserGroup{{
			ID: 13, SiteAccountID: account.ID, GroupKey: "default", Name: "unmapped-user-group",
		}}}
		assertImportRejectsUnmappedParent(t, ctx, dump, "site_user_groups", &model.SiteUserGroup{})
	})

	t.Run("site model", func(t *testing.T) {
		ctx := setupBackupTestDB(t)
		_, account := seedBackupIntegritySiteAccount(t, "local-model-parent")
		dump := &model.DBDump{Version: 1, SiteModels: []model.SiteModel{{
			ID: 14, SiteAccountID: account.ID, GroupKey: "default", ModelName: "unmapped-model",
		}}}
		assertImportRejectsUnmappedParent(t, ctx, dump, "site_models", &model.SiteModel{})
	})

	t.Run("site channel binding", func(t *testing.T) {
		ctx := setupBackupTestDB(t)
		site, account := seedBackupIntegritySiteAccount(t, "local-binding-parent")
		channel := seedBackupIntegrityChannel(t, "local-binding-channel")
		dump := &model.DBDump{Version: 1, SiteChannelBindings: []model.SiteChannelBinding{{
			ID: 15, SiteID: site.ID, SiteAccountID: account.ID, GroupKey: "default", ChannelID: channel.ID,
		}}}
		assertImportRejectsUnmappedParent(t, ctx, dump, "site_channel_bindings", &model.SiteChannelBinding{})
	})

	t.Run("group item", func(t *testing.T) {
		ctx := setupBackupTestDB(t)
		channel := seedBackupIntegrityChannel(t, "local-group-item-channel")
		group := &model.Group{Name: "local-group-item-parent", Mode: model.GroupModeFailover}
		if err := dbpkg.GetDB().Create(group).Error; err != nil {
			t.Fatalf("create local group: %v", err)
		}
		dump := &model.DBDump{Version: 1, GroupItems: []model.GroupItem{{
			ID: 16, GroupID: group.ID, ChannelID: channel.ID, ModelName: "unmapped-model",
		}}}
		assertImportRejectsUnmappedParent(t, ctx, dump, "group_items", &model.GroupItem{})
	})

	t.Run("binding user group", func(t *testing.T) {
		ctx := setupBackupTestDB(t)
		site, account := seedBackupIntegritySiteAccount(t, "local-binding-user-group-parent")
		channel := seedBackupIntegrityChannel(t, "local-binding-user-group-channel")
		localUserGroup := &model.SiteUserGroup{SiteAccountID: account.ID, GroupKey: "local", Name: "local-user-group"}
		if err := dbpkg.GetDB().Create(localUserGroup).Error; err != nil {
			t.Fatalf("create local site user group: %v", err)
		}
		const dumpSiteID = 201
		const dumpAccountID = 202
		const dumpChannelID = 203
		dump := &model.DBDump{
			Version: 1,
			Sites: []model.Site{{
				ID: dumpSiteID, Name: site.Name, Platform: site.Platform, BaseURL: site.BaseURL, Enabled: true,
			}},
			SiteAccounts: []model.SiteAccount{{
				ID: dumpAccountID, SiteID: dumpSiteID, Name: account.Name,
				CredentialType: model.SiteCredentialTypeAPIKey, APIKey: "mapped-account-key", Enabled: true,
			}},
			Channels: []model.Channel{{
				ID: dumpChannelID, Name: channel.Name, Type: channel.Type, Enabled: true,
			}},
			SiteChannelBindings: []model.SiteChannelBinding{{
				ID: 17, SiteID: dumpSiteID, SiteAccountID: dumpAccountID,
				SiteUserGroupID: &localUserGroup.ID, GroupKey: "local", ChannelID: dumpChannelID,
			}},
		}
		assertImportRejectsUnmappedParent(t, ctx, dump, "site_channel_bindings", &model.SiteChannelBinding{})
	})
}

func TestDBImportDuplicateCloudflareBindingDoesNotRewriteDiscardedChannel(t *testing.T) {
	for _, duplicateBy := range []string{"account_group", "channel"} {
		t.Run(duplicateBy, func(t *testing.T) {
			ctx := setupOpenAIProtocolTestDB(t)
			site := &model.Site{
				Name: "duplicate-binding-cloudflare-" + duplicateBy, Platform: model.SitePlatformCloudflare,
				BaseURL: "https://api.cloudflare.com/client/v4/accounts/duplicate" + duplicateBy + "/ai", Enabled: true,
			}
			if err := SiteCreate(site, ctx); err != nil {
				t.Fatalf("create cloudflare site: %v", err)
			}
			account := &model.SiteAccount{
				SiteID: site.ID, Name: "duplicate-binding-account-" + duplicateBy,
				CredentialType: model.SiteCredentialTypeAPIKey, APIKey: "duplicate-binding-key", Enabled: true,
			}
			if err := SiteAccountCreate(account, ctx); err != nil {
				t.Fatalf("create cloudflare account: %v", err)
			}

			target := createOpenAIProtocolTestChannel(t, ctx, "discarded-binding-target-"+duplicateBy,
				outbound.OutboundTypeOpenAIChat, []model.BaseUrl{{URL: "https://api.example.com/v1"}})

			switch duplicateBy {
			case "account_group":
				bound := createOpenAIProtocolTestChannel(t, ctx, "existing-account-binding-"+duplicateBy,
					outbound.OutboundTypeOpenAIChat, []model.BaseUrl{{URL: "https://bound.example.com/v1"}})
				if err := dbpkg.GetDB().Create(&model.SiteChannelBinding{
					SiteID: site.ID, SiteAccountID: account.ID, GroupKey: "default", ChannelID: bound.ID,
				}).Error; err != nil {
					t.Fatalf("create existing account/group binding: %v", err)
				}
			case "channel":
				otherSite, otherAccount := seedBackupIntegritySiteAccount(t, "other-channel-binding-parent")
				if err := dbpkg.GetDB().Create(&model.SiteChannelBinding{
					SiteID: otherSite.ID, SiteAccountID: otherAccount.ID, GroupKey: "other", ChannelID: target.ID,
				}).Error; err != nil {
					t.Fatalf("create existing channel binding: %v", err)
				}
			}

			const dumpChannelID = 301
			const dumpSiteID = 302
			const dumpAccountID = 303
			dump := &model.DBDump{
				Version: 1,
				Channels: []model.Channel{{
					ID: dumpChannelID, Name: target.Name, Type: target.Type, Enabled: true,
				}},
				Sites: []model.Site{{
					ID: dumpSiteID, Name: site.Name, Platform: site.Platform, BaseURL: site.BaseURL, Enabled: true,
				}},
				SiteAccounts: []model.SiteAccount{{
					ID: dumpAccountID, SiteID: dumpSiteID, Name: account.Name,
					CredentialType: model.SiteCredentialTypeAPIKey, APIKey: "mapped-duplicate-key", Enabled: true,
				}},
				SiteChannelBindings: []model.SiteChannelBinding{{
					ID: 304, SiteID: dumpSiteID, SiteAccountID: dumpAccountID,
					GroupKey: "default::openai-response", ChannelID: dumpChannelID,
				}},
			}
			if _, err := DBImportIncremental(ctx, dump); err != nil {
				t.Fatalf("duplicate binding import failed: %v", err)
			}

			want := channelProtocolState{
				Mode: model.OpenAIProtocolModeAuto, Chat: model.OpenAIProtocolCapabilityUnknown,
				Responses: model.OpenAIProtocolCapabilityUnknown,
			}
			if got := loadChannelProtocolStateFromDB(t, ctx, target.ID); got != want {
				t.Fatalf("discarded duplicate binding rewrote target in db: want %#v got %#v", want, got)
			}
			if got := cachedChannelProtocolState(t, ctx, target.ID); got != want {
				t.Fatalf("discarded duplicate binding rewrote target in cache: want %#v got %#v", want, got)
			}
		})
	}
}

func TestDBImportProxyNameConflictLogDoesNotExposeURLCredentials(t *testing.T) {
	ctx := setupBackupTestDB(t)
	core, observed := observer.New(zap.WarnLevel)
	originalLogger := logpkg.Logger
	logpkg.Logger = zap.New(core).Sugar()
	t.Cleanup(func() { logpkg.Logger = originalLogger })

	dump := &model.DBDump{Version: 1, ProxyConfigurations: []model.ProxyConfiguration{
		{ID: 1, Name: "duplicate-proxy", URL: "http://user-one:proxy-secret-one@one.example:8080", Enabled: true},
		{ID: 2, Name: "duplicate-proxy", URL: "http://user-two:proxy-secret-two@two.example:8080", Enabled: true},
	}}
	if _, err := DBImportIncremental(ctx, dump); err != nil {
		t.Fatalf("proxy conflict import failed: %v", err)
	}

	entries := observed.FilterMessage("proxy configuration name conflict during import").All()
	if len(entries) != 1 {
		t.Fatalf("expected one proxy conflict warning, got %d", len(entries))
	}
	logged := entries[0].Message + fmt.Sprint(entries[0].ContextMap())
	for _, forbidden := range []string{"proxy-secret-one", "proxy-secret-two", "user-one", "user-two", "one.example", "two.example"} {
		if strings.Contains(logged, forbidden) {
			t.Fatalf("proxy conflict log exposed URL credential material %q: %s", forbidden, logged)
		}
	}
}

func assertImportRejectsUnmappedParent(t *testing.T, ctx context.Context, dump *model.DBDump, table string, row any) {
	t.Helper()
	_, err := DBImportIncremental(ctx, dump)
	if err == nil || !strings.Contains(err.Error(), "unmapped") {
		t.Fatalf("expected explicit unmapped foreign key error for %s, got %v", table, err)
	}
	var count int64
	if err := dbpkg.GetDB().Model(row).Count(&count).Error; err != nil {
		t.Fatalf("count %s after rejected import: %v", table, err)
	}
	if count != 0 {
		t.Fatalf("rejected import polluted %s through a local ID collision: %d rows", table, count)
	}
}

func seedBackupIntegrityChannel(t *testing.T, name string) *model.Channel {
	t.Helper()
	channel := &model.Channel{Name: name, Type: outbound.OutboundTypeOpenAIChat, Enabled: true}
	if err := dbpkg.GetDB().Create(channel).Error; err != nil {
		t.Fatalf("create local channel: %v", err)
	}
	return channel
}

func seedBackupIntegritySite(t *testing.T, name string) *model.Site {
	t.Helper()
	site := &model.Site{Name: name, Platform: model.SitePlatformNewAPI, BaseURL: "https://" + name + ".example.com", Enabled: true}
	if err := dbpkg.GetDB().Create(site).Error; err != nil {
		t.Fatalf("create local site: %v", err)
	}
	return site
}

func seedBackupIntegritySiteAccount(t *testing.T, name string) (*model.Site, *model.SiteAccount) {
	t.Helper()
	site := seedBackupIntegritySite(t, name+"-site")
	account := &model.SiteAccount{
		SiteID: site.ID, Name: name + "-account",
		CredentialType: model.SiteCredentialTypeAPIKey, APIKey: name + "-key", Enabled: true,
	}
	if err := dbpkg.GetDB().Create(account).Error; err != nil {
		t.Fatalf("create local site account: %v", err)
	}
	return site, account
}
