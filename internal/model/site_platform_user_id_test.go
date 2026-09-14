package model

import (
	"encoding/json"
	"testing"
)

func TestSiteAccountPlatformUserIDJSONCompatibility(t *testing.T) {
	tests := []struct {
		name string
		body string
		want *string
	}{
		{name: "omitted", body: `{}`, want: nil},
		{name: "null", body: `{"platform_user_id":null}`, want: nil},
		{name: "empty string", body: `{"platform_user_id":"  "}`, want: nil},
		{name: "text", body: `{"platform_user_id":" X5MVNT "}`, want: stringPointer("X5MVNT")},
		{name: "legacy integer", body: `{"platform_user_id":9007199254740993}`, want: stringPointer("9007199254740993")},
		{name: "zero", body: `{"platform_user_id":0}`, want: nil},
		{name: "negative", body: `{"platform_user_id":-1}`, want: nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var account SiteAccount
			if err := json.Unmarshal([]byte(tt.body), &account); err != nil {
				t.Fatalf("json.Unmarshal failed: %v", err)
			}
			if !sameStringPointer(account.PlatformUserID, tt.want) {
				t.Fatalf("platform user id = %#v, want %#v", account.PlatformUserID, tt.want)
			}

			encoded, err := json.Marshal(account)
			if err != nil {
				t.Fatalf("json.Marshal failed: %v", err)
			}
			var response struct {
				PlatformUserID *string `json:"platform_user_id"`
			}
			if err := json.Unmarshal(encoded, &response); err != nil {
				t.Fatalf("decode response failed: %v", err)
			}
			if !sameStringPointer(response.PlatformUserID, tt.want) {
				t.Fatalf("response platform user id = %#v, want %#v", response.PlatformUserID, tt.want)
			}
		})
	}
}

func TestSiteAccountPlatformUserIDRejectsWrongJSONTypes(t *testing.T) {
	for _, body := range []string{
		`{"platform_user_id":true}`,
		`{"platform_user_id":[]}`,
		`{"platform_user_id":{}}`,
		`{"platform_user_id":1.5}`,
	} {
		var account SiteAccount
		if err := json.Unmarshal([]byte(body), &account); err == nil {
			t.Fatalf("json.Unmarshal(%s) unexpectedly succeeded", body)
		}

		var request SiteAccountUpdateRequest
		if err := json.Unmarshal([]byte(`{"id":1,"platform_user_id":`+extractJSONValue(body)+`}`), &request); err == nil {
			t.Fatalf("update json.Unmarshal(%s) unexpectedly succeeded", body)
		}
	}
}

func TestSiteAccountUpdatePlatformUserIDPresence(t *testing.T) {
	tests := []struct {
		name string
		body string
		set  bool
		want *string
	}{
		{name: "omitted", body: `{"id":1}`, set: false, want: nil},
		{name: "null", body: `{"id":1,"platform_user_id":null}`, set: true, want: nil},
		{name: "legacy integer", body: `{"id":1,"platform_user_id":42}`, set: true, want: stringPointer("42")},
		{name: "text", body: `{"id":1,"platform_user_id":" X5MVNT "}`, set: true, want: stringPointer("X5MVNT")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var request SiteAccountUpdateRequest
			if err := json.Unmarshal([]byte(tt.body), &request); err != nil {
				t.Fatalf("json.Unmarshal failed: %v", err)
			}
			if request.PlatformUserIDSet != tt.set {
				t.Fatalf("PlatformUserIDSet = %v, want %v", request.PlatformUserIDSet, tt.set)
			}
			if !sameStringPointer(request.PlatformUserID, tt.want) {
				t.Fatalf("platform user id = %#v, want %#v", request.PlatformUserID, tt.want)
			}
		})
	}
}

func TestSiteAccountNormalizePlatformUserID(t *testing.T) {
	value := "  X5MVNT  "
	account := SiteAccount{PlatformUserID: &value}
	account.Normalize()
	if account.PlatformUserID == nil || *account.PlatformUserID != "X5MVNT" {
		t.Fatalf("normalized platform user id = %#v, want X5MVNT", account.PlatformUserID)
	}
}

func stringPointer(value string) *string { return &value }

func sameStringPointer(left, right *string) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}

func extractJSONValue(body string) string {
	const prefix = `{"platform_user_id":`
	value := body[len(prefix) : len(body)-1]
	return value
}
