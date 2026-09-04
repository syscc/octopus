package sitesync

import (
	"strings"
	"testing"
)

func TestCollectCloudflareCatalogModelsRejectsMalformedArrayItems(t *testing.T) {
	tests := []struct {
		name       string
		payload    string
		errMessage string
	}{
		{
			name:       "scalar item",
			payload:    `{"success":true,"result":["@cf/test/model"]}`,
			errMessage: "is not an object",
		},
		{
			name:       "mixed items",
			payload:    `{"success":true,"result":[{"name":"@cf/test/valid"},7]}`,
			errMessage: "is not an object",
		},
		{
			name:       "missing name",
			payload:    `{"success":true,"result":[{}]}`,
			errMessage: "missing a non-empty string name",
		},
		{
			name:       "empty name",
			payload:    `{"success":true,"result":[{"name":"  "}]}`,
			errMessage: "missing a non-empty string name",
		},
		{
			name:       "non-string name",
			payload:    `{"success":true,"result":[{"name":123}]}`,
			errMessage: "missing a non-empty string name",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			models, err := collectCloudflareCatalogModels(cloudflareModelCatalogMaxPages, func(page int) (map[string]any, error) {
				return decodeCloudflareCatalogPayload(t, tt.payload), nil
			})
			if err == nil || !strings.Contains(err.Error(), tt.errMessage) {
				t.Fatalf("expected %q error, got models=%v err=%v", tt.errMessage, models, err)
			}
			if models != nil {
				t.Fatalf("expected malformed catalog not to return partial models, got %v", models)
			}
		})
	}
}

func TestCollectCloudflareCatalogModelsIgnoresMalformedNonTextItem(t *testing.T) {
	models, err := collectCloudflareCatalogModels(cloudflareModelCatalogMaxPages, func(page int) (map[string]any, error) {
		if page > 1 {
			return map[string]any{"success": true, "result": []any{}}, nil
		}
		return map[string]any{
			"success": true,
			"result": []any{
				map[string]any{"task": map[string]any{"name": "Text-to-Image"}},
				map[string]any{"name": "@cf/test/chat", "task": map[string]any{"name": "Text Generation"}},
			},
		}, nil
	})
	if err != nil {
		t.Fatalf("expected malformed non-text item to be ignored, got %v", err)
	}
	if len(models) != 1 || models[0] != "@cf/test/chat" {
		t.Fatalf("expected only the text generation model, got %v", models)
	}
}
