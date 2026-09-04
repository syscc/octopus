package model

import (
	"context"
	"net/http"
	"net/url"
)

type Inbound interface {
	// 入站请求转为内部通用格式
	TransformRequest(ctx context.Context, body []byte) (*InternalLLMRequest, error)

	// 将出站内部通用响应转为入站对应的响应格式
	TransformResponse(ctx context.Context, response *InternalLLMResponse) ([]byte, error)

	// 将出站内部通用流式响应转为入站对应的流式响应格式
	TransformStream(ctx context.Context, stream *InternalLLMResponse) ([]byte, error)

	// 获取完整的内部响应，用于日志记录、数据统计等
	// 流式场景：将储存的流式响应聚合为完整的响应
	// 非流式场景：返回储存的完整响应
	GetInternalResponse(ctx context.Context) (*InternalLLMResponse, error)
}

type Outbound interface {
	// 将入站内部通用请求转为出站对应的请求格式
	TransformRequest(ctx context.Context, request *InternalLLMRequest, baseUrl, key string) (*http.Request, error)

	// 将出站响应转为内部通用响应格式
	TransformResponse(ctx context.Context, response *http.Response) (*InternalLLMResponse, error)

	// 将出站流式转为内部通用流式响应格式
	TransformStream(ctx context.Context, eventData []byte) (*InternalLLMResponse, error)
}

type OutboundStreamEventTransformer interface {
	// TransformStreamEvent converts provider stream bytes into explicit stream events.
	TransformStreamEvent(ctx context.Context, eventData []byte) ([]StreamEvent, error)
}

type InboundStreamEventTransformer interface {
	// TransformStreamEvents converts explicit stream events into the inbound wire format.
	TransformStreamEvents(ctx context.Context, events []StreamEvent) ([]byte, error)
}

// InboundStreamFinalizer optionally emits protocol terminal data when an
// upstream stream ends without an explicit wire-level done event.
type InboundStreamFinalizer interface {
	FinalizeStream(ctx context.Context) ([]byte, error)
}

// InboundInterruptedFinalizer emits a protocol incomplete terminal when an
// upstream transport fails after response payload has already been projected.
// It is distinct from InboundStreamFinalizer: normal EOF may legitimately
// complete a response after a finish signal, while an interrupted stream must
// never be upgraded to response.completed.
type InboundInterruptedFinalizer interface {
	FinalizeInterruptedStream(ctx context.Context) ([]byte, error)
}

// InboundResponseInitializer optionally prepares a fresh inbound adapter for
// a new relay attempt. Implementations must copy request-bound settings only
// (defensively, to avoid aliasing) and must not carry mutable per-response
// stream state across attempts.
type InboundResponseInitializer interface {
	InitializeResponse(request *InternalLLMRequest)
}

// InboundIncompleteFinalizer synthesizes protocol terminal bytes for a raw
// passthrough upstream stream that reached EOF without any terminal event.
// The returned bytes, when non-empty, are appended to the client stream; the
// relay then treats the attempt as failed with ErrIncompleteUpstreamStream so
// it is neither persisted for replay nor failed over to another channel.
type InboundIncompleteFinalizer interface {
	FinalizeIncompleteStream(ctx context.Context, rawStream []byte) ([]byte, error)
}

// InboundStreamTerminalObserver reports a terminal that the inbound adapter
// has projected into the downstream protocol. The processor samples this only
// after the corresponding bytes were written successfully, so generated data
// is never confused with client-visible data.
type InboundStreamTerminalObserver interface {
	StreamTerminalOutcome() (PassthroughTerminalOutcome, error)
}

/*
请求流程
非流式

client		-> inbound.TransformRequest(ctx, body)
			-> outbound.TransformRequest(ctx, request)
 			-> http.Do(request)
 			-> outbound.TransformResponse(ctx, response)
			-> inbound.TransformResponse(ctx, response)
															-> client

流式
client		-> inbound.TransformRequest(ctx, body)
        	-> outbound.TransformStream(ctx, chunk)
        	-> http.Do(request)
        	-> outbound.TransformStream(ctx, chunk)
        	-> inbound.TransformStream(ctx, chunk)
															-> client
*/

// PassthroughCapable is an optional interface for Outbound transformers that support
// same-format passthrough (bypassing Internal Model round-trip).
//
// When both inbound and outbound use the same protocol (e.g., Anthropic→Anthropic or
// OpenAI Responses→OpenAI Responses), passthrough preserves request byte-stability
// (critical for prompt caching) and avoids transformation overhead.
type PassthroughCapable interface {
	// CanPassthrough returns true if this outbound can accept raw bytes from the given inbound format.
	// Example: Anthropic MessageOutbound returns true when inboundFormat == APIFormatAnthropicMessage.
	CanPassthrough(inboundFormat APIFormat) bool

	// TransformRequestRaw builds an HTTP request from raw client bytes, rewriting only essential
	// fields (model name, authorization) while preserving request structure.
	//
	// This method maintains byte-level stability for features like Anthropic's prompt caching,
	// where field order and whitespace matter.
	TransformRequestRaw(ctx context.Context, rawBody []byte, model, baseUrl, key string, query url.Values) (*http.Request, error)

	// PassthroughConfig returns passthrough-specific settings for this protocol.
	PassthroughConfig() PassthroughConfig
}

// PassthroughTerminalOutcome describes the protocol result of a raw terminal event.
// It is deliberately separate from transport completion: a stream may stop on a
// terminal event and still represent an upstream failure or an incomplete turn.
type PassthroughTerminalOutcome string

const (
	PassthroughTerminalOutcomeNone       PassthroughTerminalOutcome = ""
	PassthroughTerminalOutcomeCompleted  PassthroughTerminalOutcome = "completed"
	PassthroughTerminalOutcomeFailed     PassthroughTerminalOutcome = "failed"
	PassthroughTerminalOutcomeIncomplete PassthroughTerminalOutcome = "incomplete"
	PassthroughTerminalOutcomeCancelled  PassthroughTerminalOutcome = "cancelled"
)

// PassthroughConfig provides protocol-specific settings for passthrough operation.
type PassthroughConfig struct {
	// TerminalEvents defines protocol-specific terminal event types for early completion detection.
	// When a stream contains a terminal event (e.g., "message_stop" for Anthropic, "response.completed"
	// for OpenAI Responses), the relay can treat client disconnection as success rather than failure.
	TerminalEvents map[string]struct{}

	// FailureEvents are terminal events that prove the upstream request failed.
	// They are not successful merely because they stop the wire stream. When a
	// failure is observed before any payload is committed, the relay suppresses
	// the raw event so normal retry/failover can produce the public error.
	FailureEvents map[string]struct{}

	// IncompleteEvents are legal protocol terminals whose result is incomplete,
	// but not an upstream transport failure (for example Responses' status
	// "incomplete"). They must not be confused with ErrIncompleteUpstreamStream.
	IncompleteEvents map[string]struct{}

	// CancelledEvents are legal provider terminals that stop a request without
	// completing it. They are kept distinct from both a completed response and
	// a transport-level incomplete stream.
	CancelledEvents map[string]struct{}

	// CollectMetrics defines whether to call collectResponse() after passthrough stream ends.
	// Set to true for protocols that require full response aggregation for cost/token tracking
	// (Anthropic), false for protocols with different metrics semantics (OpenAI Responses).
	CollectMetrics bool
}
