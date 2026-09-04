package relay

import (
	"math"
	"testing"

	"github.com/bestruirui/octopus/internal/utils/httpbody"
)

// TestUpstreamWSReadLimitCap 验证上游 WS 读上限的钳制逻辑：正常值保持原状，
// 超过硬上限或极端值（MaxInt）被钳制到与 httpbody.MaxLLMResponseBodyBytes
// 一致的 64 MiB，非正值回落到安全默认，避免 coder/websocket 内部 n++ 溢出成
// 负数后按“取消限制”语义处理。
func TestUpstreamWSReadLimitCap(t *testing.T) {
	if wsUpstreamMaxReadLimitBytes != httpbody.MaxLLMResponseBodyBytes {
		t.Fatalf("wsUpstreamMaxReadLimitBytes = %d, want %d", wsUpstreamMaxReadLimitBytes, httpbody.MaxLLMResponseBodyBytes)
	}

	cases := []struct {
		name       string
		configured int
		want       int64
	}{
		{name: "small value unchanged", configured: 4096, want: 4096},
		{name: "default 32MiB unchanged", configured: 32 * 1024 * 1024, want: 32 * 1024 * 1024},
		{name: "exactly at cap unchanged", configured: 64 * 1024 * 1024, want: wsUpstreamMaxReadLimitBytes},
		{name: "one byte over cap is clamped", configured: 64*1024*1024 + 1, want: wsUpstreamMaxReadLimitBytes},
		{name: "max int is clamped", configured: math.MaxInt, want: wsUpstreamMaxReadLimitBytes},
		{name: "zero falls back to default", configured: 0, want: wsUpstreamDefaultReadLimitBytes},
		{name: "negative falls back to default", configured: -1, want: wsUpstreamDefaultReadLimitBytes},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := upstreamWSReadLimit(tc.configured)
			if got != tc.want {
				t.Fatalf("upstreamWSReadLimit(%d) = %d, want %d", tc.configured, got, tc.want)
			}
			if got <= 0 {
				t.Fatalf("upstreamWSReadLimit(%d) = %d, want a positive limit", tc.configured, got)
			}
			// coder/websocket 对上限做 n++，钳制后必须仍为正数。
			if got+1 <= 0 {
				t.Fatalf("upstreamWSReadLimit(%d)+1 overflowed to %d", tc.configured, got+1)
			}
		})
	}
}
