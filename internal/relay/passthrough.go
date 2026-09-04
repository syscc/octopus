package relay

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/bestruirui/octopus/internal/relay/stream"
	transformerModel "github.com/bestruirui/octopus/internal/transformer/model"
)

// passthroughSSETransform frames raw SSE bytes before writing them downstream.
// Framing is needed because a network read may split an event in the middle of
// its JSON payload; protocol failures must be classified before that event is
// committed to a client that has not seen any response payload yet.
type passthroughSSETransform struct {
	config      transformerModel.PassthroughConfig
	pending     []byte
	seenPayload bool
	terminal    transformerModel.PassthroughTerminalOutcome
	filterDone  bool
}

func newPassthroughSSETransform(config transformerModel.PassthroughConfig, filterDone bool) *passthroughSSETransform {
	return &passthroughSSETransform{config: config, filterDone: filterDone}
}

func (f *passthroughSSETransform) transform(_ context.Context, data []byte, payloadWritten bool) stream.StreamTransformResult {
	if f.terminal != transformerModel.PassthroughTerminalOutcomeNone {
		return stream.StreamTransformResult{}
	}
	f.pending = append(f.pending, data...)
	if len(f.pending) > maxSSEEventSize {
		return stream.StreamTransformResult{Err: fmt.Errorf("passthrough SSE event exceeds limit %d bytes", maxSSEEventSize)}
	}
	return f.consume(payloadWritten, false)
}

func (f *passthroughSSETransform) finalize(_ context.Context, payloadWritten bool) stream.StreamTransformResult {
	if f.terminal != transformerModel.PassthroughTerminalOutcomeNone {
		return stream.StreamTransformResult{}
	}
	return f.consume(payloadWritten, true)
}

func (f *passthroughSSETransform) consume(payloadWritten, final bool) stream.StreamTransformResult {
	var output []byte
	outputHasPayload := false
	for len(f.pending) > 0 {
		block, rest, found := nextPassthroughSSEBlock(f.pending)
		if !found {
			if !final {
				// Some upstreams end the final SSE event with a single newline
				// rather than the spec's blank-line dispatch delimiter. A complete
				// terminal event is still safe to classify immediately; ordinary
				// partial events remain buffered until the next read/EOF.
				pendingType, pendingData := passthroughSSEEventType(f.pending)
				if !f.isTerminalType(pendingType) || !passthroughSSEDataComplete(pendingData) {
					break
				}
				block = append([]byte(nil), f.pending...)
				rest = nil
				found = true
			} else {
				block = append([]byte(nil), f.pending...)
				rest = nil
			}
		}
		f.pending = rest
		blockOutput, outcome, terminalErr, blockHasPayload := f.consumeBlock(block, payloadWritten || f.seenPayload || outputHasPayload)
		output = append(output, blockOutput...)
		outputHasPayload = outputHasPayload || blockHasPayload
		if outcome != transformerModel.PassthroughTerminalOutcomeNone {
			f.terminal = outcome
		}
		if terminalErr != nil {
			return stream.StreamTransformResult{Output: output, Outcome: outcome, Err: terminalErr, NonPayload: len(output) > 0 && !outputHasPayload}
		}
		if f.terminal != transformerModel.PassthroughTerminalOutcomeNone {
			// A protocol terminal ends the passthrough response. Bytes after it
			// are not part of the client-visible response.
			f.pending = nil
			break
		}
		if !found {
			break
		}
	}
	return stream.StreamTransformResult{Output: output, Outcome: f.terminal, NonPayload: len(output) > 0 && !outputHasPayload}
}

func (f *passthroughSSETransform) isTerminalType(eventType string) bool {
	if _, ok := f.config.TerminalEvents[eventType]; ok {
		return true
	}
	if _, ok := f.config.FailureEvents[eventType]; ok {
		return true
	}
	_, ok := f.config.IncompleteEvents[eventType]
	if ok {
		return true
	}
	_, ok = f.config.CancelledEvents[eventType]
	return ok
}

func passthroughSSEDataComplete(data string) bool {
	data = strings.TrimSpace(data)
	if data == "" || data == "[DONE]" {
		return data == "[DONE]"
	}
	return json.Valid([]byte(data))
}

func (f *passthroughSSETransform) consumeBlock(block []byte, alreadyPayload bool) ([]byte, transformerModel.PassthroughTerminalOutcome, error, bool) {
	// 上游可能把多个 event 挤进同一个 SSE 帧（缺少空行分隔），下游按 SSE
	// 规范把连续的 data 行拼成一个值后 JSON.parse 会在第二个对象的起点炸掉。
	// 先把畸形帧重写成规范的单事件帧，剩余部分退回 pending 队列，后续所有
	// 分类逻辑就都作用在合法帧上，透传出去的字节也是合法的。
	block = f.rewriteConcatenatedBlock(block)

	eventType, data := passthroughSSEEventType(block)
	if f.filterDone && eventType == "[DONE]" {
		return nil, transformerModel.PassthroughTerminalOutcomeNone, nil, false
	}

	if _, failed := f.config.FailureEvents[eventType]; failed {
		outcome := transformerModel.PassthroughTerminalOutcomeFailed
		failure := passthroughStructuredFailure(eventType, data)
		if alreadyPayload {
			return block, outcome, stream.NewPassthroughTerminalError(outcome, failure), true
		}
		return nil, outcome, stream.NewPassthroughTerminalError(outcome, failure), false
	}
	if _, incomplete := f.config.IncompleteEvents[eventType]; incomplete {
		return block, transformerModel.PassthroughTerminalOutcomeIncomplete,
			stream.NewPassthroughTerminalError(transformerModel.PassthroughTerminalOutcomeIncomplete), true
	}
	if _, cancelled := f.config.CancelledEvents[eventType]; cancelled {
		return block, transformerModel.PassthroughTerminalOutcomeCancelled,
			stream.NewPassthroughTerminalError(transformerModel.PassthroughTerminalOutcomeCancelled), true
	}
	if _, terminal := f.config.TerminalEvents[eventType]; terminal {
		return block, transformerModel.PassthroughTerminalOutcomeCompleted, nil, true
	}

	blockHasPayload := passthroughBlockHasPayload(eventType, data)
	if blockHasPayload {
		f.seenPayload = true
	}
	return block, transformerModel.PassthroughTerminalOutcomeNone, nil, blockHasPayload
}

// rewriteConcatenatedBlock 检测一个 SSE 帧内是否串联了多个 JSON 值。命中时
// 返回只含第一个值的规范帧，并把剩余值作为独立帧压回 pending 队首；未命中
// 时原样返回，保证正常流量字节级不变。
func (f *passthroughSSETransform) rewriteConcatenatedBlock(block []byte) []byte {
	_, data := passthroughSSEEventType(block)
	first, remainder, ok := splitConcatenatedJSONData(data)
	if !ok {
		return block
	}

	// 保留原帧的非 data 字段（event / id / retry / 注释），它们描述的是
	// 第一个事件；剩余 JSON 作为裸 data 帧，类型由其自身 type 字段推断。
	normalized := bytes.ReplaceAll(block, []byte("\r\n"), []byte("\n"))
	normalized = bytes.ReplaceAll(normalized, []byte("\r"), []byte("\n"))
	var buf bytes.Buffer
	for _, line := range strings.Split(string(normalized), "\n") {
		if strings.TrimSpace(line) == "" || strings.HasPrefix(line, "data:") {
			continue
		}
		buf.WriteString(line)
		buf.WriteByte('\n')
	}
	buf.WriteString("data: ")
	buf.WriteString(first)
	buf.WriteString("\n\n")

	// 剩余部分可能还串着更多 JSON（上游一次挤进三个以上事件）。逐个拆开
	// 并各自补上 data: 前缀：否则第二个之后的值会留在同一 data 行里没有
	// 前缀，下游按 SSE 规范直接忽略该行，数据就丢了。
	var requeued bytes.Buffer
	for rest := remainder; ; {
		next, tail, more := splitConcatenatedJSONData(rest)
		if !more {
			requeued.WriteString("data: ")
			requeued.WriteString(rest)
			requeued.WriteString("\n\n")
			break
		}
		requeued.WriteString("data: ")
		requeued.WriteString(next)
		requeued.WriteString("\n\n")
		rest = tail
	}
	f.pending = append(requeued.Bytes(), f.pending...)
	return buf.Bytes()
}

func nextPassthroughSSEBlock(data []byte) (block, rest []byte, found bool) {
	best := -1
	endLen := 0
	markers := [][]byte{[]byte("\n\n"), []byte("\r\n\r\n"), []byte("\r\r")}
	for _, marker := range markers {
		if index := bytes.Index(data, marker); index >= 0 {
			end := index + len(marker)
			if best < 0 || end < best+endLen {
				best = index
				endLen = len(marker)
			}
		}
	}
	if best < 0 {
		return nil, data, false
	}
	end := best + endLen
	return data[:end], data[end:], true
}

func passthroughSSEEventType(block []byte) (string, string) {
	normalized := bytes.ReplaceAll(block, []byte("\r\n"), []byte("\n"))
	normalized = bytes.ReplaceAll(normalized, []byte("\r"), []byte("\n"))
	var eventType string
	var dataLines []string
	for _, line := range strings.Split(string(normalized), "\n") {
		switch {
		case strings.HasPrefix(line, "event:"):
			eventType = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
		case strings.HasPrefix(line, "data:"):
			value := strings.TrimPrefix(line, "data:")
			if strings.HasPrefix(value, " ") {
				value = value[1:]
			}
			dataLines = append(dataLines, value)
		}
	}
	data := strings.TrimSpace(strings.Join(dataLines, "\n"))
	if eventType == "" {
		switch {
		case data == "[DONE]":
			eventType = "[DONE]"
		case strings.HasPrefix(data, "{"):
			var envelope struct {
				Type string `json:"type"`
			}
			if err := json.Unmarshal([]byte(data), &envelope); err == nil {
				eventType = strings.TrimSpace(envelope.Type)
			}
		}
	}
	return eventType, data
}

// splitConcatenatedJSONData 检测由多个 JSON 值串联而成的 SSE data 值。
// 只有当整串不是合法 JSON、而它的前缀是一个完整 JSON 值且后面还跟着
// 非空内容时才算命中；返回第一个 JSON 值和剩余部分。
//
// 这是对上游 SSE 分帧错误的防御：合法的多行 data（比如把一个 JSON 拆成
// 几行发送）拼接后仍是单个合法 JSON，不会走进这里，原样保持不变。
func splitConcatenatedJSONData(data string) (first string, rest string, ok bool) {
	trimmed := strings.TrimSpace(data)
	if !strings.HasPrefix(trimmed, "{") && !strings.HasPrefix(trimmed, "[") {
		return "", "", false
	}
	if json.Valid([]byte(trimmed)) {
		return "", "", false
	}
	decoder := json.NewDecoder(strings.NewReader(trimmed))
	var head json.RawMessage
	if err := decoder.Decode(&head); err != nil {
		return "", "", false
	}
	remainder := strings.TrimSpace(trimmed[decoder.InputOffset():])
	if remainder == "" {
		return "", "", false
	}
	return strings.TrimSpace(string(head)), remainder, true
}

func passthroughBlockHasPayload(eventType, data string) bool {
	// SSE comments (including the processor heartbeat `:`) carry no protocol
	// event and must not commit an attempt. A named event or a data field is
	// client-visible protocol content, including a legal terminal.
	return strings.TrimSpace(eventType) != "" || strings.TrimSpace(data) != ""
}

func passthroughStructuredFailure(eventType, data string) *wsUpstreamEventError {
	failure := &wsUpstreamEventError{Type: limitPassthroughText(strings.TrimSpace(eventType), 256)}
	var envelope map[string]json.RawMessage
	if json.Unmarshal([]byte(data), &envelope) == nil {
		apply := func(object map[string]json.RawMessage, allowStatus bool) {
			if object == nil {
				return
			}
			if code := rawPassthroughString(object["code"]); code != "" {
				failure.Code = code
			}
			if message := rawPassthroughText(object["message"], 512); message != "" {
				failure.Message = message
			}
			if allowStatus {
				if status := rawPassthroughStatus(object["status"]); status > 0 {
					failure.Status = status
				}
			}
			if typ := rawPassthroughText(object["type"], 256); typ != "" {
				failure.Type = typ
			}
		}
		apply(envelope, true)
		if nested, ok := rawPassthroughObject(envelope["error"]); ok {
			apply(nested, true)
		}
		if response, ok := rawPassthroughObject(envelope["response"]); ok {
			if failure.Status == 0 {
				failure.Status = rawPassthroughStatus(response["status"])
			}
			if nested, nestedOK := rawPassthroughObject(response["error"]); nestedOK {
				apply(nested, false)
			}
		}
	}
	if failure.Message == "" {
		failure.Message = "upstream response failed"
	}
	failure.Code = limitPassthroughText(failure.Code, 256)
	failure.Type = limitPassthroughText(failure.Type, 256)
	failure.Message = limitPassthroughText(failure.Message, 512)
	return failure
}

// classifyPassthroughJSONFailure detects a structured failure in a non-stream
// passthrough response without exposing or forwarding its body. Responses may
// express failure through `status`, through a top-level/nested `error` object,
// or through a typed event envelope; all three forms use the same bounded,
// bounded structured classification representation.
func classifyPassthroughJSONFailure(body []byte, config transformerModel.PassthroughConfig) error {
	if len(body) == 0 {
		return nil
	}
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(body, &envelope); err != nil {
		return nil
	}

	typ := strings.TrimSpace(rawPassthroughText(envelope["type"], 256))
	status := strings.ToLower(strings.TrimSpace(rawPassthroughText(envelope["status"], 64)))
	response, _ := rawPassthroughObject(envelope["response"])
	responseStatus := strings.ToLower(strings.TrimSpace(rawPassthroughText(response["status"], 64)))

	topError := rawPassthroughPresent(envelope["error"])
	responseError := rawPassthroughPresent(response["error"])
	failureEvent := typ != "" && hasPassthroughEvent(config.FailureEvents, typ)
	if !failureEvent && (status == "failed" || responseStatus == "failed") &&
		(hasPassthroughEvent(config.FailureEvents, "error") || hasPassthroughEvent(config.FailureEvents, "response.failed")) {
		failureEvent = true
	}
	if !failureEvent && (topError || responseError) && hasPassthroughEvent(config.FailureEvents, "error") {
		failureEvent = true
	}
	if failureEvent {
		failure := passthroughStructuredFailure(typ, string(body))
		return stream.NewPassthroughTerminalError(transformerModel.PassthroughTerminalOutcomeFailed, failure)
	}

	incompleteEvent := hasPassthroughEvent(config.IncompleteEvents, typ) ||
		(status == "incomplete" || responseStatus == "incomplete")
	if incompleteEvent {
		return stream.NewPassthroughTerminalError(transformerModel.PassthroughTerminalOutcomeIncomplete)
	}

	cancelledEvent := hasPassthroughEvent(config.CancelledEvents, typ) ||
		status == "cancelled" || status == "canceled" ||
		responseStatus == "cancelled" || responseStatus == "canceled"
	if cancelledEvent {
		return stream.NewPassthroughTerminalError(transformerModel.PassthroughTerminalOutcomeCancelled)
	}
	return nil
}

func hasPassthroughEvent(events map[string]struct{}, eventType string) bool {
	if eventType == "" {
		return false
	}
	_, ok := events[eventType]
	return ok
}

func rawPassthroughObject(raw json.RawMessage) (map[string]json.RawMessage, bool) {
	if len(raw) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return nil, false
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(raw, &object); err != nil {
		return nil, false
	}
	return object, true
}

func rawPassthroughPresent(raw json.RawMessage) bool {
	trimmed := bytes.TrimSpace(raw)
	return len(trimmed) > 0 && !bytes.Equal(trimmed, []byte("null"))
}

func rawPassthroughText(raw json.RawMessage, max int) string {
	if len(raw) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return ""
	}
	var text string
	if json.Unmarshal(raw, &text) == nil {
		return limitPassthroughText(strings.TrimSpace(text), max)
	}
	return ""
}

func passthroughOutcomeFromError(err error) transformerModel.PassthroughTerminalOutcome {
	var terminalErr *stream.PassthroughTerminalError
	if errors.As(err, &terminalErr) && terminalErr != nil {
		return terminalErr.Outcome
	}
	return transformerModel.PassthroughTerminalOutcomeNone
}

func passthroughTerminalStatus(err error) int {
	var structured *wsUpstreamEventError
	if errors.As(err, &structured) && structured != nil {
		if structured.Status >= 100 {
			return structured.Status
		}
		return wsUpstreamErrorStatus(err)
	}
	var responseErr *transformerModel.ResponseError
	if errors.As(err, &responseErr) && responseErr != nil {
		if responseErr.StatusCode >= 100 {
			return responseErr.StatusCode
		}
		return wsUpstreamErrorStatus(err)
	}
	var terminalErr *stream.PassthroughTerminalError
	if errors.As(err, &terminalErr) {
		return wsUpstreamErrorStatus(err)
	}
	return 0
}

func rawPassthroughString(raw json.RawMessage) string {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return ""
	}
	var text string
	if json.Unmarshal(trimmed, &text) == nil {
		return limitPassthroughText(strings.TrimSpace(text), 256)
	}
	var number json.Number
	if len(trimmed) > 0 && (trimmed[0] == '-' || (trimmed[0] >= '0' && trimmed[0] <= '9')) && json.Unmarshal(trimmed, &number) == nil {
		return limitPassthroughText(number.String(), 256)
	}
	return ""
}

func rawPassthroughStatus(raw json.RawMessage) int {
	text := rawPassthroughString(raw)
	status, err := strconv.Atoi(text)
	if err != nil || status < 100 || status > 599 {
		return 0
	}
	return status
}

func limitPassthroughText(value string, max int) string {
	if max <= 0 {
		return ""
	}
	if len(value) <= max {
		return value
	}
	return strings.ToValidUTF8(value[:max], "")
}
