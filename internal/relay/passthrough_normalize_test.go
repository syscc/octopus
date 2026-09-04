package relay

import (
	"testing"

	transformerModel "github.com/bestruirui/octopus/internal/transformer/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSplitConcatenatedJSONDataDetectsMalformed(t *testing.T) {
	// 两个完整的 JSON 对象拼在一起（中间有换行符或空格）
	concatenated := `{"id":"1","type":"start"}
{"id":"2","type":"progress"}`

	first, rest, ok := splitConcatenatedJSONData(concatenated)
	require.True(t, ok, "应该检测到拼接的 JSON")
	assert.JSONEq(t, `{"id":"1","type":"start"}`, first)
	assert.JSONEq(t, `{"id":"2","type":"progress"}`, rest)
}

func TestSplitConcatenatedJSONDataKeepsValidMultiline(t *testing.T) {
	// 合法的多行 JSON（整体是一个有效对象）
	valid := `{
  "id": "1",
  "type": "test"
}`
	first, _, ok := splitConcatenatedJSONData(valid)
	require.False(t, ok, "合法的单个 JSON 不应触发拆分")
	assert.Empty(t, first)
}

func TestSplitConcatenatedJSONDataArrays(t *testing.T) {
	concatenated := `["item1"]["item2"]`
	first, rest, ok := splitConcatenatedJSONData(concatenated)
	require.True(t, ok)
	assert.JSONEq(t, `["item1"]`, first)
	assert.JSONEq(t, `["item2"]`, rest)
}

func TestSplitConcatenatedJSONDataInvalidPrefix(t *testing.T) {
	invalid := `not-json`
	_, _, ok := splitConcatenatedJSONData(invalid)
	require.False(t, ok, "非 JSON 前缀不应触发拆分")
}

func TestSplitConcatenatedJSONDataSingleInvalid(t *testing.T) {
	// 单个不完整的 JSON（没有拼接，只是残缺）
	incomplete := `{"id":"1","type":`
	_, _, ok := splitConcatenatedJSONData(incomplete)
	require.False(t, ok, "单个残缺 JSON 不应触发拆分")
}

// passthroughSSEEventType 只负责解析，不再截断拼接的 data：截断与重写是
// rewriteConcatenatedBlock 的职责。拼接的 JSON 整体不可解析，所以推断不出
// eventType，正好让 consumeBlock 先走重写再分类。
func TestPassthroughSSEEventTypeReturnsRawConcatenatedData(t *testing.T) {
	malformed := []byte(
		"data: {\"id\":\"evt1\",\"type\":\"response.created\"}\n" +
			"data: {\"id\":\"evt2\",\"type\":\"response.in_progress\"}\n\n",
	)

	eventType, data := passthroughSSEEventType(malformed)

	assert.Empty(t, eventType, "拼接的 JSON 解析不出 type")
	assert.Contains(t, data, "evt1")
	assert.Contains(t, data, "evt2", "解析阶段保留完整 data，交给重写阶段拆分")
}

func TestPassthroughSSEEventTypeKeepsValidMultiLineData(t *testing.T) {
	// 合法的多行 data：SSE 规范允许用多个 data 行发送一个 JSON
	valid := []byte(
		"data: {\"id\":\"1\",\n" +
			"data: \"value\":\"test\"}\n\n",
	)

	_, data := passthroughSSEEventType(valid)

	// 拼接后是一个完整 JSON（虽然语法错误，但这是原样保留）
	assert.Contains(t, data, "\"id\":\"1\"")
	assert.Contains(t, data, "\"value\":\"test\"")
}

func TestPassthroughSSEEventTypeWithEventField(t *testing.T) {
	// 显式指定 event 字段的 SSE
	block := []byte(
		"event: custom_event\n" +
			"data: {\"payload\":\"test\"}\n\n",
	)

	eventType, data := passthroughSSEEventType(block)

	assert.Equal(t, "custom_event", eventType)
	assert.Contains(t, data, "payload")
}

func TestPassthroughSSEEventTypeDoneMarker(t *testing.T) {
	done := []byte("data: [DONE]\n\n")
	eventType, data := passthroughSSEEventType(done)

	assert.Equal(t, "[DONE]", eventType)
	assert.Equal(t, "[DONE]", data)
}

func TestSplitConcatenatedJSONDataTrimming(t *testing.T) {
	// 前后有空白符
	concatenated := `  {"a":1}   {"b":2}  `
	first, rest, ok := splitConcatenatedJSONData(concatenated)
	require.True(t, ok)
	assert.JSONEq(t, `{"a":1}`, first)
	assert.JSONEq(t, `{"b":2}`, rest)
}

func TestSplitConcatenatedJSONDataNestedObjects(t *testing.T) {
	// 嵌套对象（仍是单个有效 JSON）
	nested := `{"outer":{"inner":{"deep":"value"}}}`
	_, _, ok := splitConcatenatedJSONData(nested)
	require.False(t, ok, "单个嵌套 JSON 不应触发拆分")
}

func TestConsumeBlockRewritesConcatenatedSSE(t *testing.T) {
	// 真实场景：上游返回畸形 SSE，两个 data 拼在一起
	malformed := []byte(
		"data: {\"id\":\"evt1\",\"type\":\"response.created\"}\n" +
			"data: {\"id\":\"evt2\",\"type\":\"response.in_progress\"}\n\n",
	)

	cfg := transformerModel.PassthroughConfig{
		TerminalEvents: map[string]struct{}{"response.done": {}},
	}
	transform := newPassthroughSSETransform(cfg, false)

	output, outcome, err, hasPayload := transform.consumeBlock(malformed, false)

	// 应该成功处理
	require.NoError(t, err)
	assert.Equal(t, transformerModel.PassthroughTerminalOutcomeNone, outcome)
	assert.True(t, hasPayload)

	// 输出应该是规范的 SSE（只包含第一个 JSON）
	outputStr := string(output)
	assert.Contains(t, outputStr, "data: {\"id\":\"evt1\"")
	assert.NotContains(t, outputStr, "evt2", "第二个 JSON 不应出现在输出里")
	assert.Contains(t, outputStr, "\n\n", "应该有空行分隔符")

	// pending 队列应该包含第二个 JSON（重新入队）
	assert.Greater(t, len(transform.pending), 0, "剩余 JSON 应该重新入队")
	assert.Contains(t, string(transform.pending), "evt2")
}

func TestConsumeBlockPreservesEventMetadata(t *testing.T) {
	// 畸形 SSE 带 event 字段
	malformed := []byte(
		"event: response.created\n" +
			"data: {\"id\":\"evt1\"}\n" +
			"data: {\"id\":\"evt2\"}\n\n",
	)

	cfg := transformerModel.PassthroughConfig{}
	transform := newPassthroughSSETransform(cfg, false)

	output, _, _, _ := transform.consumeBlock(malformed, false)
	outputStr := string(output)

	// 应该保留 event 字段
	assert.Contains(t, outputStr, "event: response.created")
	assert.Contains(t, outputStr, "data: {\"id\":\"evt1\"")
	assert.NotContains(t, outputStr, "evt2")
}

func TestConsumeBlockHandlesNormalSSE(t *testing.T) {
	// 正常的 SSE（没有拼接）
	normal := []byte("data: {\"id\":\"evt1\",\"type\":\"test\"}\n\n")

	cfg := transformerModel.PassthroughConfig{}
	transform := newPassthroughSSETransform(cfg, false)

	output, _, err, _ := transform.consumeBlock(normal, false)

	require.NoError(t, err)
	// 应该原样输出
	assert.Equal(t, normal, output)
	// pending 应该是空的
	assert.Empty(t, transform.pending)
}

func TestPassthroughSSEIntegrationRewritesMalformedStream(t *testing.T) {
	// 端到端测试：输入畸形流，验证输出是规范的多个独立帧
	malformed := []byte(
		"data: {\"id\":\"evt1\",\"type\":\"start\"}\n" +
			"data: {\"id\":\"evt2\",\"type\":\"progress\"}\n\n" +
			"data: {\"id\":\"evt3\",\"type\":\"done\"}\n\n",
	)

	cfg := transformerModel.PassthroughConfig{
		TerminalEvents: map[string]struct{}{"done": {}},
	}
	transform := newPassthroughSSETransform(cfg, false)

	// 第一次 transform：输入畸形帧
	result1 := transform.transform(nil, malformed, false)
	require.NoError(t, result1.Err)

	output1 := string(result1.Output)
	// 第一次输出应该包含 evt1 和 evt2（因为 evt2 被重新入队后立即消费）
	assert.Contains(t, output1, "evt1")

	// 验证 evt2 也被处理了（从 pending 重新消费）
	assert.Contains(t, output1, "evt2")

	// evt3 应该也被处理（最后一个独立帧）
	assert.Contains(t, output1, "evt3")

	// 验证 terminal 状态
	assert.Equal(t, transformerModel.PassthroughTerminalOutcomeCompleted, result1.Outcome)
}

// 三个及以上 JSON 串联时必须全部拆成独立帧。早先的实现只拆一层，剩下的
// 值留在同一个 data 行里没有前缀，下游按 SSE 规范会直接忽略该行 —— 数据
// 静默丢失，比解析报错更难查。
func TestConsumeBlockSplitsThreeConcatenatedJSON(t *testing.T) {
	block := []byte("data: {\"a\":1}\ndata: {\"b\":2}\ndata: {\"c\":3}\n\n")

	transform := newPassthroughSSETransform(transformerModel.PassthroughConfig{}, false)
	result := transform.transform(nil, block, false)
	require.NoError(t, result.Err)

	out := string(result.Output)
	assert.Equal(t, "data: {\"a\":1}\n\ndata: {\"b\":2}\n\ndata: {\"c\":3}\n\n", out,
		"三个 JSON 必须各自成为带 data: 前缀的独立帧")
	assert.Empty(t, transform.pending, "全部拆完后不应有残留")
}

// 终止事件落在第二个串联值里时，拆帧后仍要被正确识别为 terminal。
func TestConsumeBlockDetectsTerminalInSecondConcatenatedValue(t *testing.T) {
	block := []byte("data: {\"type\":\"delta\"}\ndata: {\"type\":\"response.completed\"}\n\n")

	cfg := transformerModel.PassthroughConfig{
		TerminalEvents: map[string]struct{}{"response.completed": {}},
	}
	transform := newPassthroughSSETransform(cfg, false)
	result := transform.transform(nil, block, false)

	assert.Equal(t, transformerModel.PassthroughTerminalOutcomeCompleted, result.Outcome)
	assert.Contains(t, string(result.Output), "response.completed")
}
