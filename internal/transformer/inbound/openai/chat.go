package openai

import (
	"context"
	"encoding/json"

	"github.com/bestruirui/octopus/internal/transformer/model"
)

type ChatInbound struct {
	streamAggregator model.StreamAggregator
	streamSeen       bool
	// storedResponse stores the non-stream response
	storedResponse  *model.InternalLLMResponse
	doneEmitted     bool
	terminalOutcome model.PassthroughTerminalOutcome
	terminalError   error
}

func (i *ChatInbound) TransformRequest(ctx context.Context, body []byte) (*model.InternalLLMRequest, error) {
	var request model.InternalLLMRequest
	if err := json.Unmarshal(body, &request); err != nil {
		return nil, err
	}
	// O-H2: tag the origin so outbound transformers (raw passthrough,
	// alternation enforcement, schema conversion) can tell a Chat request
	// apart from a Responses request.
	request.RawAPIFormat = model.APIFormatOpenAIChatCompletion
	return &request, nil
}

func (i *ChatInbound) TransformResponse(ctx context.Context, response *model.InternalLLMResponse) ([]byte, error) {
	// Store the response for later retrieval
	i.storedResponse = response

	body, err := json.Marshal(response)
	if err != nil {
		return nil, err
	}
	return body, nil
}

func (i *ChatInbound) TransformStream(ctx context.Context, stream *model.InternalLLMResponse) ([]byte, error) {
	if stream == nil {
		return nil, nil
	}
	i.streamSeen = true
	if i.doneEmitted && (stream.Object == "[DONE]" || stream.Error != nil || i.terminalOutcome != model.PassthroughTerminalOutcomeCompleted) {
		return nil, nil
	}
	wireStream := stream
	if stream.Error != nil {
		// Error is semantic terminal state. It must take precedence over a
		// malformed accompanying [DONE] marker and must never be overwritten by
		// the successful sentinel branch below.
		i.doneEmitted = true
		i.terminalOutcome = model.PassthroughTerminalOutcomeFailed
		i.terminalError = stream.Error
		if stream.Object == "[DONE]" {
			copy := *stream
			copy.Object = "chat.completion.chunk"
			wireStream = &copy
		}
	} else if stream.Object == "[DONE]" {
		i.doneEmitted = true
		i.terminalOutcome = model.PassthroughTerminalOutcomeCompleted
		i.terminalError = nil
		return []byte("data: [DONE]\n\n"), nil
	}

	// Store the chunk for aggregation.
	i.streamAggregator.Add(stream)

	var body []byte
	var err error

	// Handle the case where choices are empty but we need them to be present as an empty array
	// This is to satisfy some clients (like Cherry Studio) that require choices field to be present
	if len(wireStream.Choices) == 0 && wireStream.Object == "chat.completion.chunk" {
		type Alias model.InternalLLMResponse
		aux := &struct {
			*Alias
			Choices []model.Choice `json:"choices"`
		}{
			Alias:   (*Alias)(wireStream),
			Choices: []model.Choice{},
		}
		body, err = json.Marshal(aux)
	} else {
		body, err = json.Marshal(wireStream)
	}

	if err != nil {
		return nil, err
	}
	return []byte("data: " + string(body) + "\n\n"), nil
}

func (i *ChatInbound) TransformStreamEvents(ctx context.Context, events []model.StreamEvent) ([]byte, error) {
	if len(events) == 0 || i.doneEmitted {
		return nil, nil
	}

	// An error has precedence over Done even when both arrive in one provider
	// frame. Stop at the first wire terminal and attach a later error only as
	// the semantic result; content after Done must never be projected.
	prefix, errorEvent, hasDone := model.SplitStreamEventsAtTerminal(events)
	projected := make([]model.StreamEvent, 0, len(prefix))
	for _, event := range prefix {
		if event.Kind != model.StreamEventKindError {
			projected = append(projected, event)
		}
	}

	var output []byte
	if hasVisibleChatStreamEvent(projected) {
		// Some lifecycle/extension events carry no Chat fields and therefore
		// produce a nil aggregate. They still prove that the upstream stream
		// started, so clean EOF must be classified as incomplete rather than
		// empty. UsageDelta remains metadata-only and is intentionally ignored.
		i.streamSeen = true
	}
	if stream := model.InternalResponseFromStreamEvents(projected); stream != nil {
		chunk, err := i.TransformStream(ctx, stream)
		output = append(output, chunk...)
		if err != nil {
			return output, err
		}
	}
	if errorEvent != nil {
		err := errorEvent.Error
		errorResponse := model.InternalResponseFromStreamEvents([]model.StreamEvent{*errorEvent})
		if errorResponse != nil {
			i.streamAggregator.Add(errorResponse)
			body, marshalErr := json.Marshal(errorResponse)
			if marshalErr != nil {
				return output, marshalErr
			}
			output = append(output, []byte("data: "+string(body)+"\n\n")...)
		}
		i.doneEmitted = true
		i.terminalOutcome = model.PassthroughTerminalOutcomeFailed
		i.terminalError = err
		return output, err
	}
	if hasDone && !i.doneEmitted {
		i.doneEmitted = true
		i.terminalOutcome = model.PassthroughTerminalOutcomeCompleted
		i.terminalError = nil
		output = append(output, "data: [DONE]\n\n"...)
	}
	return output, nil
}

func hasVisibleChatStreamEvent(events []model.StreamEvent) bool {
	for _, event := range events {
		switch event.Kind {
		case model.StreamEventKindDone, model.StreamEventKindUsageDelta, model.StreamEventKindError:
			continue
		default:
			return true
		}
	}
	return false
}

// GetInternalResponse returns the complete internal response for logging, statistics, etc.
// For streaming: aggregates all stored stream chunks into a complete response
// For non-streaming: returns the stored response
func (i *ChatInbound) GetInternalResponse(ctx context.Context) (*model.InternalLLMResponse, error) {
	if i.storedResponse != nil {
		return i.storedResponse, nil
	}
	return i.streamAggregator.BuildAndReset(), nil
}

// FinalizeStream closes a Chat stream that reached clean transport EOF without
// the required [DONE] marker. The visible prefix remains valid, but the turn is
// incomplete; no finish reason or usage is synthesized.
func (i *ChatInbound) FinalizeStream(ctx context.Context) ([]byte, error) {
	if i.doneEmitted || !i.streamSeen {
		return nil, nil
	}
	return i.FinalizeInterruptedStream(ctx)
}

// FinalizeInterruptedStream 在上游传输中断且部分 payload 已写出后，用 Chat
// 协议唯一的流终止符 [DONE] 收尾客户端流；不伪造 finish_reason 或 usage。
// 返回的哨兵让 relay 将该次尝试记为硬失败（payload 已可见，禁止
// retry/failover），并计入熔断/outlier 健康。
func (i *ChatInbound) FinalizeInterruptedStream(_ context.Context) ([]byte, error) {
	if i.doneEmitted {
		return nil, nil
	}
	i.doneEmitted = true
	i.terminalOutcome = model.PassthroughTerminalOutcomeIncomplete
	i.terminalError = model.ErrIncompleteUpstreamStream
	return []byte("data: [DONE]\n\n"), model.ErrIncompleteUpstreamStream
}

func (i *ChatInbound) StreamTerminalOutcome() (model.PassthroughTerminalOutcome, error) {
	return i.terminalOutcome, i.terminalError
}
