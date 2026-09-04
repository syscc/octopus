package relay

import (
	"errors"
	"testing"

	transformerModel "github.com/bestruirui/octopus/internal/transformer/model"
)

func TestPassthroughSSETransformClassifiesStructuredFailure(t *testing.T) {
	cfg := transformerModel.PassthroughConfig{
		TerminalEvents: map[string]struct{}{
			"response.completed": {},
			"response.failed":    {},
		},
		FailureEvents: map[string]struct{}{
			"response.failed": {},
		},
	}
	transform := newPassthroughSSETransform(cfg, false)
	first := transform.transform(nil, []byte("data: {\"type\":\"response.failed\",\"response\":{\"error\":{\"code\":\"server_error\",\"message\":\"upstream failed\"}}}\n\n"), false)
	if first.Output != nil {
		t.Fatalf("failure before payload must be suppressed, got %q", first.Output)
	}
	if first.Outcome != transformerModel.PassthroughTerminalOutcomeFailed {
		t.Fatalf("unexpected outcome %q", first.Outcome)
	}
	if first.Err == nil {
		t.Fatal("expected structured failure")
	}
	var structured *wsUpstreamEventError
	if !errors.As(first.Err, &structured) || structured.Code != "server_error" {
		t.Fatalf("expected typed structured failure, got %v", first.Err)
	}
}

func TestPassthroughSSETransformPreservesCommittedFailure(t *testing.T) {
	cfg := transformerModel.PassthroughConfig{
		TerminalEvents: map[string]struct{}{
			"response.completed": {},
			"response.failed":    {},
		},
		FailureEvents: map[string]struct{}{
			"response.failed": {},
		},
	}
	transform := newPassthroughSSETransform(cfg, false)
	partial := []byte("data: {\"type\":\"response.output_text.delta\",\"delta\":\"hello\"}\n\n")
	if result := transform.transform(nil, partial, false); string(result.Output) != string(partial) || result.Err != nil {
		t.Fatalf("expected partial event to pass through, got output=%q err=%v", result.Output, result.Err)
	}
	failure := []byte("data: {\"type\":\"response.failed\",\"response\":{\"error\":{\"message\":\"failed\"}}}\n\n")
	result := transform.transform(nil, failure, true)
	if string(result.Output) != string(failure) {
		t.Fatalf("committed failure must be preserved, got %q", result.Output)
	}
	if result.Err == nil || result.Outcome != transformerModel.PassthroughTerminalOutcomeFailed {
		t.Fatalf("expected committed failure outcome, got output=%q outcome=%q err=%v", result.Output, result.Outcome, result.Err)
	}
}

func TestPassthroughSSETransformHandlesSplitEvent(t *testing.T) {
	cfg := transformerModel.PassthroughConfig{
		TerminalEvents: map[string]struct{}{
			"response.completed": {},
		},
	}
	transform := newPassthroughSSETransform(cfg, false)
	first := transform.transform(nil, []byte("data: {\"type\":\"response.com"), false)
	if len(first.Output) != 0 || first.Err != nil {
		t.Fatalf("split event should remain buffered, got output=%q err=%v", first.Output, first.Err)
	}
	second := transform.transform(nil, []byte("pleted\"}\n\n"), false)
	if string(second.Output) != "data: {\"type\":\"response.completed\"}\n\n" || second.Outcome != transformerModel.PassthroughTerminalOutcomeCompleted {
		t.Fatalf("split terminal was not reconstructed, got output=%q outcome=%q", second.Output, second.Outcome)
	}
}

func TestPassthroughSSETransformFiltersDoneAcrossChunks(t *testing.T) {
	cfg := transformerModel.PassthroughConfig{
		TerminalEvents: map[string]struct{}{"response.completed": {}},
	}
	transform := newPassthroughSSETransform(cfg, true)
	var output []byte
	for _, chunk := range [][]byte{
		[]byte("data: one\n\ndata: [DO"),
		[]byte("NE]\r\n\r\ndata: two\r\n\r\n"),
	} {
		result := transform.transform(nil, chunk, false)
		if result.Err != nil {
			t.Fatalf("split [DONE] transform failed: %v", result.Err)
		}
		output = append(output, result.Output...)
	}
	final := transform.finalize(nil, true)
	if final.Err != nil {
		t.Fatalf("split [DONE] finalization failed: %v", final.Err)
	}
	output = append(output, final.Output...)

	want := "data: one\n\ndata: two\r\n\r\n"
	if got := string(output); got != want {
		t.Fatalf("frame-aware [DONE] filtering changed ordinary events: got %q want %q", got, want)
	}
}

func TestPassthroughSSETransformDoesNotFilterDoneTextInsideJSON(t *testing.T) {
	cfg := transformerModel.PassthroughConfig{
		TerminalEvents: map[string]struct{}{"response.completed": {}},
	}
	transform := newPassthroughSSETransform(cfg, true)
	block := []byte("data: {\"type\":\"response.output_text.delta\",\"delta\":\"literal data: [DONE] text\"}\n\n")
	result := transform.transform(nil, block, false)
	if result.Err != nil {
		t.Fatalf("ordinary JSON event failed: %v", result.Err)
	}
	if got := string(result.Output); got != string(block) {
		t.Fatalf("[DONE] text inside JSON must remain byte-for-byte unchanged: got %q want %q", got, block)
	}
}

func TestPassthroughSSETransformFinalFlushHandlesUndelimitedDone(t *testing.T) {
	cfg := transformerModel.PassthroughConfig{
		TerminalEvents: map[string]struct{}{"response.completed": {}},
	}

	t.Run("done sentinel", func(t *testing.T) {
		transform := newPassthroughSSETransform(cfg, true)
		if result := transform.transform(nil, []byte("data: [DO"), false); len(result.Output) != 0 || result.Err != nil {
			t.Fatalf("partial sentinel must remain buffered: output=%q err=%v", result.Output, result.Err)
		}
		if result := transform.transform(nil, []byte("NE]\r\n"), false); len(result.Output) != 0 || result.Err != nil {
			t.Fatalf("undelimited sentinel must remain buffered until EOF: output=%q err=%v", result.Output, result.Err)
		}
		result := transform.finalize(nil, false)
		if len(result.Output) != 0 || result.Err != nil {
			t.Fatalf("final sentinel must be filtered: output=%q err=%v", result.Output, result.Err)
		}
	})

	t.Run("ordinary event", func(t *testing.T) {
		transform := newPassthroughSSETransform(cfg, true)
		block := []byte("data: ordinary final event")
		if result := transform.transform(nil, block, false); len(result.Output) != 0 || result.Err != nil {
			t.Fatalf("undelimited event must remain buffered until EOF: output=%q err=%v", result.Output, result.Err)
		}
		result := transform.finalize(nil, false)
		if result.Err != nil || string(result.Output) != string(block) {
			t.Fatalf("final ordinary event changed: output=%q err=%v", result.Output, result.Err)
		}
	})
}

func TestPassthroughSSETransformTerminalCallbackCanCancel(t *testing.T) {
	cfg := transformerModel.PassthroughConfig{
		TerminalEvents: map[string]struct{}{"response.completed": {}},
	}
	transform := newPassthroughSSETransform(cfg, false)
	data := []byte("data: {\"type\":\"response.created\"}\n\ndata: {\"type\":\"response.completed\"}\n\n")
	result := transform.transform(nil, data, false)
	if string(result.Output) != string(data) {
		t.Fatalf("terminal data was not emitted, got %q", result.Output)
	}
	if result.Outcome != transformerModel.PassthroughTerminalOutcomeCompleted {
		t.Fatalf("unexpected outcome %q", result.Outcome)
	}
}

func TestPassthroughSSETransformCommentsAreNonPayload(t *testing.T) {
	cfg := transformerModel.PassthroughConfig{
		TerminalEvents: map[string]struct{}{"response.completed": {}},
	}
	transform := newPassthroughSSETransform(cfg, false)
	result := transform.transform(nil, []byte(": keep-alive\n\n"), false)
	if string(result.Output) != ": keep-alive\n\n" {
		t.Fatalf("comment should pass through unchanged, got %q", result.Output)
	}
	if result.Outcome != transformerModel.PassthroughTerminalOutcomeNone || !result.NonPayload {
		t.Fatalf("comment must not commit a terminal or payload: outcome=%q nonPayload=%t", result.Outcome, result.NonPayload)
	}
}

func TestPassthroughSSETransformLegalNonCompletedOutcomes(t *testing.T) {
	for _, tc := range []struct {
		name    string
		event   string
		outcome transformerModel.PassthroughTerminalOutcome
	}{
		{name: "incomplete", event: "response.incomplete", outcome: transformerModel.PassthroughTerminalOutcomeIncomplete},
		{name: "cancelled", event: "response.cancelled", outcome: transformerModel.PassthroughTerminalOutcomeCancelled},
		{name: "canceled", event: "response.canceled", outcome: transformerModel.PassthroughTerminalOutcomeCancelled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := transformerModel.PassthroughConfig{
				TerminalEvents:   map[string]struct{}{tc.event: {}},
				IncompleteEvents: map[string]struct{}{"response.incomplete": {}},
				CancelledEvents:  map[string]struct{}{"response.cancelled": {}, "response.canceled": {}},
			}
			result := newPassthroughSSETransform(cfg, false).transform(nil, []byte("data: {\"type\":\""+tc.event+"\"}\n\n"), false)
			if result.Outcome != tc.outcome || result.Err == nil || len(result.Output) == 0 {
				t.Fatalf("unexpected legal outcome: output=%q outcome=%q err=%v", result.Output, result.Outcome, result.Err)
			}
		})
	}
}
