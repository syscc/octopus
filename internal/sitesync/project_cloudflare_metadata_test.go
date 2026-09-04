package sitesync

import (
	"testing"

	"github.com/bestruirui/octopus/internal/model"
)

func TestCloudflareProjectionIgnoresLegacyRouteGroupMetadata(t *testing.T) {
	item := model.SiteModel{
		GroupKey:  model.SiteDefaultGroupKey,
		ModelName: "@cf/test/model",
		RouteRawPayload: model.SiteModelRouteMetadata{
			RouteSupported: true,
			RouteType:      model.SiteModelRouteTypeOpenAIChat,
			EnableGroups:   []string{"other-group"},
		}.Marshal(),
	}
	if siteModelBelongsToProjectedGroup(item, model.SiteDefaultGroupKey, model.SitePlatformNewAPI) {
		t.Fatal("expected non-Cloudflare projection to honor explicit group metadata")
	}
	if !siteModelBelongsToProjectedGroup(item, model.SiteDefaultGroupKey, model.SitePlatformCloudflare) {
		t.Fatal("expected Cloudflare projection to ignore legacy route group metadata")
	}
}
