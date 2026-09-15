package model

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

func TestGlobalAutoGroupModelFilter(t *testing.T) {
	for _, tt := range []struct {
		name, value, channel, model string
		want                        bool
	}{
		{"off", `{"mode":"off","keywords":["gpt"]}`, "source", "gpt", true},
		{"blacklist channel", `{"mode":"blacklist","keywords":["只会喵喵叫"]}`, "只会喵喵叫/default", "gpt", false},
		{"blacklist model", `{"mode":"blacklist","keywords":["mini"]}`, "source", "GPT-mini", false},
		{"blacklist unrelated", `{"mode":"blacklist","keywords":["mini"]}`, "source", "gpt", true},
		{"whitelist OR case trim", `{"mode":"whitelist","keywords":["other"," GPT ","gpt"]}`, "source", "gPt-mini", true},
		{"whitelist channel", `{"mode":"whitelist","keywords":["source"]}`, "SOURCE/default", "gpt", true},
		{"whitelist unrelated", `{"mode":"whitelist","keywords":["mini"]}`, "source", "gpt", false},
		{"blacklist empty", `{"mode":"blacklist","keywords":[" ","\t"]}`, "source", "gpt", true},
		{"whitelist empty", `{"mode":"whitelist","keywords":[" ","\t"]}`, "source", "gpt", false},
		{"independent fields", `{"mode":"blacklist","keywords":["sourcegpt"]}`, "source", "gpt", true},
		{"no wildcard", `{"mode":"blacklist","keywords":["source/*"]}`, "source/default", "gpt", true},
		{"no regex", `{"mode":"blacklist","keywords":["g.t"]}`, "source", "gpt", true},
		{"Greek case is literal", `{"mode":"blacklist","keywords":["ΟΣ"]}`, "source", "οσ", true},
		{"Turkish I is literal", `{"mode":"blacklist","keywords":["İ"]}`, "source", "i", true},
		{"ASCII case folds", `{"mode":"blacklist","keywords":["GPT"]}`, "source", "gpt", false},
		{"FEFF trims", `{"mode":"blacklist","keywords":["\ufeffblocked\ufeff"]}`, "source", "blocked", false},
		{"0085 trims", `{"mode":"blacklist","keywords":["\u0085blocked\u0085"]}`, "source", "blocked", false},
		{"FEFF and 0085 are empty", `{"mode":"whitelist","keywords":["\ufeff\u0085"]}`, "source", "blocked", false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			filter, err := ParseGlobalAutoGroupModelFilter(tt.value)
			if err != nil {
				t.Fatal(err)
			}
			if got := filter.Allows(tt.channel, tt.model); got != tt.want {
				t.Fatalf("Allows = %v, want %v", got, tt.want)
			}
		})
	}
	filter, err := ParseGlobalAutoGroupModelFilter(`{"mode":"whitelist","keywords":[" GPT ","gpt",""," 中文 ","中文"]}`)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(filter.Keywords, []string{"GPT", "中文"}) {
		t.Fatalf("keywords = %#v", filter.Keywords)
	}
	filter, err = ParseGlobalAutoGroupModelFilter(`{"mode":"blacklist","keywords":["\ufeff GPT\u0085","gpt","\ufeff","\u0085"," \ufeff\u0085 "]}`)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(filter.Keywords, []string{"GPT"}) {
		t.Fatalf("trimmed keywords = %#v", filter.Keywords)
	}
	filter, err = ParseGlobalAutoGroupModelFilter(`{"mode":"blacklist","keywords":["ΟΣ","οσ","İ","i"]}`)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(filter.Keywords, []string{"ΟΣ", "οσ", "İ", "i"}) {
		t.Fatalf("non-ASCII keywords were folded: %#v", filter.Keywords)
	}
}

func TestGlobalAutoGroupModelFilterValidation(t *testing.T) {
	for _, value := range []string{"", " ", "{", "null", "[]", `{}`, `{"mode":"invalid","keywords":[]}`, `{"mode":null,"keywords":[]}`, `{"mode":1,"keywords":[]}`, `{"mode":"off"}`, `{"mode":"off","keywords":null}`, `{"mode":"off","keywords":"gpt"}`, `{"mode":"off","keywords":[1]}`, `{"mode":"off","keywords":[null]}`, `{"mode":"off","keywords":["alpha,beta"]}`, `{"mode":"off","keywords":["alpha，beta"]}`, `{"mode":"off","keywords":["alpha\nbeta"]}`, `{"mode":"off","keywords":["\ngpt\n"]}`, `{"mode":"off","keywords":[],"extra":true}`} {
		t.Run(value, func(t *testing.T) {
			setting := Setting{Key: SettingKeyGlobalAutoGroupModelFilter, Value: value}
			if setting.Validate() == nil {
				t.Fatalf("accepted invalid value %q", value)
			}
		})
	}
	for _, tt := range []struct {
		name     string
		keywords []string
		valid    bool
	}{
		{"200 Unicode", []string{strings.Repeat("喵", 200)}, true},
		{"201 Unicode", []string{strings.Repeat("喵", 201)}, false},
		{"trim before length", []string{" " + strings.Repeat("喵", 200) + " "}, true},
		{"FEFF and 0085 trim before length", []string{"\ufeff\u0085" + strings.Repeat("喵", 200) + "\u0085\ufeff"}, true},
		{"FEFF and 0085 leave 201 characters invalid", []string{"\ufeff" + strings.Repeat("喵", 201) + "\u0085"}, false},
		{"100 words", filterTestKeywords(100), true},
		{"101 words", filterTestKeywords(101), false},
		{"dedup before count", append(filterTestKeywords(100), " WORD0 "), true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			raw, err := json.Marshal(GlobalAutoGroupModelFilter{Mode: GlobalAutoGroupModelFilterModeBlacklist, Keywords: tt.keywords})
			if err != nil {
				t.Fatal(err)
			}
			setting := Setting{Key: SettingKeyGlobalAutoGroupModelFilter, Value: string(raw)}
			if err := setting.Validate(); (err == nil) != tt.valid {
				t.Fatalf("Validate error = %v, valid = %v", err, tt.valid)
			}
		})
	}
	for _, setting := range DefaultSettings() {
		if setting.Key == SettingKeyGlobalAutoGroupModelFilter {
			filter, err := ParseGlobalAutoGroupModelFilter(setting.Value)
			if err != nil || filter.Mode != GlobalAutoGroupModelFilterModeOff || len(filter.Keywords) != 0 {
				t.Fatalf("invalid default: %+v, %v", filter, err)
			}
			return
		}
	}
	t.Fatal("missing default setting")
}

func filterTestKeywords(n int) []string {
	keywords := make([]string, n)
	for i := range keywords {
		keywords[i] = fmt.Sprintf("word%d", i)
	}
	return keywords
}
