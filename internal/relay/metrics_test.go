package relay

import (
	"testing"

	"github.com/bestruirui/octopus/internal/model"

	transformerModel "github.com/bestruirui/octopus/internal/transformer/model"
)

// usage 完全缺失时，应使用 TransportInputTokens 兜底填充 input，output 保持 0。
func TestSetInternalResponseFallbackWhenUsageMissing(t *testing.T) {
	m := &RelayMetrics{TransportInputTokens: intPtr(123)}
	m.SetInternalResponse(&transformerModel.InternalLLMResponse{}, "test-model")

	if m.Stats.InputToken != 123 {
		t.Fatalf("input token: got %d want 123 (fallback)", m.Stats.InputToken)
	}
	if m.BillInputTokens == nil || *m.BillInputTokens != 123 {
		t.Fatalf("bill input tokens: got %v want 123", m.BillInputTokens)
	}
	if m.Stats.OutputToken != 0 {
		t.Fatalf("output token: got %d want 0", m.Stats.OutputToken)
	}
}

// usage 存在但输入侧全为 0（仅上报 output）时，input 兜底、output 保留。
func TestSetInternalResponseFallbackWhenInputZero(t *testing.T) {
	m := &RelayMetrics{TransportInputTokens: intPtr(50)}
	m.SetInternalResponse(&transformerModel.InternalLLMResponse{
		Usage: &transformerModel.Usage{PromptTokens: 0, CompletionTokens: 30},
	}, "test-model")

	if m.Stats.InputToken != 50 {
		t.Fatalf("input token: got %d want 50 (fallback)", m.Stats.InputToken)
	}
	if m.Stats.OutputToken != 30 {
		t.Fatalf("output token: got %d want 30 (preserved)", m.Stats.OutputToken)
	}
}

// 上游正常上报 input 时不触发兜底（保留真实值，而非估算值）。
func TestSetInternalResponseNoFallbackWhenInputReported(t *testing.T) {
	m := &RelayMetrics{TransportInputTokens: intPtr(999)}
	m.SetInternalResponse(&transformerModel.InternalLLMResponse{
		Usage: &transformerModel.Usage{PromptTokens: 12, CompletionTokens: 7},
	}, "test-model")

	if m.Stats.InputToken != 12 {
		t.Fatalf("input token: got %d want 12 (reported, not fallback)", m.Stats.InputToken)
	}
	if m.Stats.OutputToken != 7 {
		t.Fatalf("output token: got %d want 7", m.Stats.OutputToken)
	}
}

// 仅缓存命中（input_tokens=0 但 cache_read>0）属于已上报输入，不应被估算覆盖。
func TestSetInternalResponseNoFallbackWhenCacheOnly(t *testing.T) {
	m := &RelayMetrics{TransportInputTokens: intPtr(999)}
	m.SetInternalResponse(&transformerModel.InternalLLMResponse{
		Usage: &transformerModel.Usage{PromptTokens: 0, CacheReadInputTokens: 40, CompletionTokens: 5},
	}, "test-model")

	if m.Stats.InputToken != 0 {
		t.Fatalf("input token: got %d want 0 (cache-only is reported input)", m.Stats.InputToken)
	}
}

func TestSetInternalResponseClearsPreviousAttemptUsage(t *testing.T) {
	m := &RelayMetrics{
		BillInputTokens:  intPtr(17),
		CacheReadTokens:  intPtr(11),
		CacheWriteTokens: intPtr(7),
	}
	m.Stats.InputToken = 17
	m.Stats.OutputToken = 13
	m.Stats.InputCost = 1.25
	m.Stats.OutputCost = 2.5

	m.SetInternalResponse(&transformerModel.InternalLLMResponse{}, "test-model")

	if m.Stats.InputToken != 0 || m.Stats.OutputToken != 0 || m.Stats.InputCost != 0 || m.Stats.OutputCost != 0 {
		t.Fatalf("previous attempt usage leaked into the replacement response: %#v", m.Stats)
	}
	if m.BillInputTokens != nil || m.CacheReadTokens != nil || m.CacheWriteTokens != nil {
		t.Fatalf("previous attempt billing details leaked: bill=%v read=%v write=%v", m.BillInputTokens, m.CacheReadTokens, m.CacheWriteTokens)
	}
}

func TestSyncWSTransportMetricsClearsExecModeAfterHTTPDowngrade(t *testing.T) {
	metrics := &RelayMetrics{UsedWS: true}
	metrics.SetWSExecMode(model.RelayLogWSExecModePassthrough)
	metrics.SetWSRecovery(model.RelayLogWSRecoveryDowngrade)
	ra := &relayAttempt{
		relayRequest:  &relayRequest{metrics: metrics},
		attemptUsedWS: false,
	}

	ra.syncWSTransportMetrics()

	if metrics.UsedWS || metrics.WSExecMode != nil {
		t.Fatalf("HTTP downgrade must clear final WS transport markers: used=%t exec=%v", metrics.UsedWS, metrics.WSExecMode)
	}
	if metrics.WSRecovery == nil || *metrics.WSRecovery != model.RelayLogWSRecoveryDowngrade {
		t.Fatalf("HTTP downgrade must preserve recovery reason, got %v", metrics.WSRecovery)
	}
}
