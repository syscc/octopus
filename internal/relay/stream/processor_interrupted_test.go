package stream

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/bestruirui/octopus/internal/transformer/model"
)

const (
	interruptedDeltaEvent    = "data: {\"type\":\"response.output_text.delta\",\"delta\":\"partial\"}\n\n"
	interruptedTerminalEvent = "data: {\"type\":\"response.completed\"}\n\n"
	incompleteTerminalEvent  = "data: {\"type\":\"response.incomplete\"}\n\n"
)

// interruptedSource emits scripted events, then returns readErr on every
// subsequent read, simulating a transport break in the middle of a stream.
type interruptedSource struct {
	events  [][]byte
	readErr error
	index   int
	closed  bool
}

func (s *interruptedSource) ReadEvent(context.Context) ([]byte, error) {
	if s.index < len(s.events) {
		data := s.events[s.index]
		s.index++
		return data, nil
	}
	return nil, s.readErr
}

func (s *interruptedSource) Close() error {
	s.closed = true
	return nil
}

// writeResult scripts the outcome of one StreamWriter.Write call.
type writeResult struct {
	n   int
	err error
}

// scriptedWriter replays per-call write results and can force the Written()
// report, simulating writers such as WSStreamWriter that track frame delivery
// state themselves.
type scriptedWriter struct {
	results      []writeResult // consumed in call order; missing entries succeed with n=len(data)
	buf          bytes.Buffer  // accepted bytes (only for writes without error)
	calls        int
	written      bool
	forceWritten bool
}

func (w *scriptedWriter) Write(data []byte) (int, error) {
	var r writeResult
	if w.calls < len(w.results) {
		r = w.results[w.calls]
	} else {
		r = writeResult{n: len(data)}
	}
	w.calls++
	if r.err == nil {
		w.buf.Write(data)
		w.written = true
	}
	return r.n, r.err
}

func (w *scriptedWriter) Flush() {}

func (w *scriptedWriter) Written() bool { return w.written || w.forceWritten }

// cancelAfterWriteWriter cancels the client context immediately after a
// downstream write succeeds, exercising terminal delivery versus disconnect
// ordering in transform mode.
type cancelAfterWriteWriter struct {
	*scriptedWriter
	cancel context.CancelFunc
}

func (w *cancelAfterWriteWriter) Write(data []byte) (int, error) {
	n, err := w.scriptedWriter.Write(data)
	if w.cancel != nil {
		cancel := w.cancel
		w.cancel = nil
		cancel()
	}
	return n, err
}

func (w *scriptedWriter) Header() http.Header { return http.Header{} }

func (w *scriptedWriter) WriteHeader(int) {}

// writeOutput must treat a partial write (n>0 or writer-reported delivery) as
// committed payload: no retry, failover, or replay may follow.
func TestStreamProcessor_PayloadWrittenOnPartialWrite(t *testing.T) {
	writeErr := errors.New("downstream write failed")
	cases := []struct {
		name         string
		result       writeResult
		forceWritten bool
		wantPayload  bool
	}{
		{name: "partial n>0 counts as written", result: writeResult{n: 5, err: writeErr}, wantPayload: true},
		{name: "n=0 with writer-reported delivery counts", result: writeResult{n: 0, err: writeErr}, forceWritten: true, wantPayload: true},
		{name: "n=0 without delivery marks nothing", result: writeResult{n: 0, err: writeErr}, wantPayload: false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			source := &interruptedSource{events: [][]byte{[]byte("chunk")}, readErr: io.EOF}
			writer := &scriptedWriter{results: []writeResult{tc.result}, forceWritten: tc.forceWritten}

			processor := NewStreamProcessor(StreamConfig{
				Source:  source,
				Writer:  writer,
				Context: context.Background(),
			})

			err := processor.Run()
			if !errors.Is(err, writeErr) {
				t.Fatalf("expected write error, got %v", err)
			}
			if got := processor.PayloadWritten(); got != tc.wantPayload {
				t.Fatalf("PayloadWritten()=%t, want %t", got, tc.wantPayload)
			}
		})
	}
}

func TestStreamProcessorPartialTerminalWriteDoesNotFinalizeAgain(t *testing.T) {
	source := newMockStreamSource([][]byte{[]byte("terminal")})
	writer := &scriptedWriter{results: []writeResult{{n: 4}}}
	processor := NewStreamProcessor(StreamConfig{
		Source:  source,
		Writer:  writer,
		Context: context.Background(),
		TransformWithOutcome: func(_ context.Context, _ []byte, _ bool) StreamTransformResult {
			return StreamTransformResult{
				Output:  []byte("data: terminal\n\n"),
				Outcome: model.PassthroughTerminalOutcomeCompleted,
			}
		},
		FinalizeWithOutcome: func(context.Context) StreamTransformResult {
			t.Fatal("partial terminal write must not finalize again")
			return StreamTransformResult{}
		},
	})

	err := processor.Run()
	if !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("error = %v, want io.ErrShortWrite", err)
	}
	if !processor.PayloadWritten() {
		t.Fatal("accepted terminal prefix must mark payload visible")
	}
}

func TestStreamProcessorTerminalErrorSurvivesDownstreamWriteFailure(t *testing.T) {
	upstreamErr := errors.New("typed upstream failure")
	writeErr := errors.New("terminal write failed")
	const payload = "data: partial\n\n"
	const terminal = "data: failed\n\n"
	writer := &scriptedWriter{results: []writeResult{{n: len(payload)}, {n: 0, err: writeErr}}}
	processor := NewStreamProcessor(StreamConfig{
		Source:  newMockStreamSource([][]byte{[]byte("delta"), []byte("failed")}),
		Writer:  writer,
		Context: context.Background(),
		TransformWithOutcome: func(_ context.Context, data []byte, _ bool) StreamTransformResult {
			if string(data) == "failed" {
				return StreamTransformResult{
					Output:  []byte(terminal),
					Outcome: model.PassthroughTerminalOutcomeFailed,
					Err:     NewPassthroughTerminalError(model.PassthroughTerminalOutcomeFailed, upstreamErr),
				}
			}
			return StreamTransformResult{Output: []byte(payload)}
		},
	})

	err := processor.Run()
	if !errors.Is(err, upstreamErr) || !errors.Is(err, writeErr) || !errors.Is(err, ErrDownstreamWriteFailed) {
		t.Fatalf("terminal and downstream errors must share one chain, got %v", err)
	}
	var terminalErr *PassthroughTerminalError
	if !errors.As(err, &terminalErr) || terminalErr.Outcome != model.PassthroughTerminalOutcomeFailed {
		t.Fatalf("typed terminal outcome was lost: %v", err)
	}
	if got := processor.PassthroughOutcome(); got != model.PassthroughTerminalOutcomeNone {
		t.Fatalf("undelivered terminal outcome = %s, want none", got)
	}
	if !processor.PayloadWritten() || writer.calls != 2 {
		t.Fatalf("payload/writes = %t/%d, want true/2", processor.PayloadWritten(), writer.calls)
	}
}

func TestStreamProcessorUndeliveredIncompleteOutcomeSurvivesWriteFailure(t *testing.T) {
	writeErr := errors.New("incomplete terminal write failed")
	processor := NewStreamProcessor(StreamConfig{
		Source:  newMockStreamSource([][]byte{[]byte("incomplete")}),
		Writer:  &scriptedWriter{results: []writeResult{{n: 0, err: writeErr}}},
		Context: context.Background(),
		TransformWithOutcome: func(context.Context, []byte, bool) StreamTransformResult {
			return StreamTransformResult{
				Output:  []byte(incompleteTerminalEvent),
				Outcome: model.PassthroughTerminalOutcomeIncomplete,
			}
		},
	})

	err := processor.Run()
	if !errors.Is(err, writeErr) || !errors.Is(err, ErrDownstreamWriteFailed) {
		t.Fatalf("downstream write error classification was lost: %v", err)
	}
	var terminalErr *PassthroughTerminalError
	if !errors.As(err, &terminalErr) || terminalErr.Outcome != model.PassthroughTerminalOutcomeIncomplete {
		t.Fatalf("undelivered incomplete outcome was lost: %v", err)
	}
	if got := processor.PassthroughOutcome(); got != model.PassthroughTerminalOutcomeNone {
		t.Fatalf("undelivered terminal outcome = %s, want none", got)
	}
}

func TestStreamProcessorTransformOutputWriteErrorPreservesTransformError(t *testing.T) {
	transformErr := errors.New("semantic transform failure")
	writeErr := errors.New("payload write failed")
	processor := NewStreamProcessor(StreamConfig{
		Source:  newMockStreamSource([][]byte{[]byte("event")}),
		Writer:  &scriptedWriter{results: []writeResult{{n: 0, err: writeErr}}},
		Context: context.Background(),
		Transform: func(context.Context, []byte) ([]byte, error) {
			return []byte("data: projected\n\n"), transformErr
		},
	})

	err := processor.Run()
	if !errors.Is(err, transformErr) || !errors.Is(err, writeErr) || !errors.Is(err, ErrDownstreamWriteFailed) {
		t.Fatalf("transform and downstream errors must share one chain, got %v", err)
	}
}

// A transport break after partial payload with no terminal event yet must
// invoke OnInterrupted exactly once, write exactly one synthesized terminal,
// and return an error that preserves both the read error and the callback's
// typed error.
func TestStreamProcessor_OnInterruptedSynthesizesSingleTerminal(t *testing.T) {
	cases := []struct {
		name    string
		readErr error
	}{
		{name: "io.ErrUnexpectedEOF", readErr: io.ErrUnexpectedEOF},
		{name: "custom transport error", readErr: errors.New("upstream connection reset")},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			source := &interruptedSource{
				events:  [][]byte{[]byte(interruptedDeltaEvent)},
				readErr: tc.readErr,
			}
			writer := &scriptedWriter{}
			calls := 0
			processor := NewStreamProcessor(StreamConfig{
				Source:          source,
				Writer:          writer,
				Context:         context.Background(),
				BufferRawStream: true,
				TerminalEvents: map[string]struct{}{
					"response.completed": {},
				},
				OnInterrupted: func(ctx context.Context, rawStream []byte) ([]byte, error) {
					calls++
					if !bytes.Contains(rawStream, []byte("response.output_text.delta")) {
						t.Errorf("OnInterrupted must receive the buffered raw stream, got %q", rawStream)
					}
					return []byte(incompleteTerminalEvent), model.ErrIncompleteUpstreamStream
				},
			})

			err := processor.Run()
			if err == nil {
				t.Fatal("expected interrupted stream error")
			}
			if !errors.Is(err, tc.readErr) {
				t.Fatalf("original read error must be preserved, got %v", err)
			}
			if !errors.Is(err, model.ErrIncompleteUpstreamStream) {
				t.Fatalf("ErrIncompleteUpstreamStream must be preserved, got %v", err)
			}
			if calls != 1 {
				t.Fatalf("OnInterrupted called %d times, want exactly 1", calls)
			}
			output := writer.buf.String()
			if strings.Count(output, "response.incomplete") != 1 {
				t.Fatalf("expected exactly one synthesized response.incomplete, got %q", output)
			}
			if !strings.Contains(output, interruptedDeltaEvent) {
				t.Fatalf("original payload must be preserved, got %q", output)
			}
			if !processor.PayloadWritten() {
				t.Fatal("payload must be marked written")
			}
		})
	}
}

// When the client already received a protocol terminal before the transport
// broke, OnInterrupted must not run and no second terminal may be written.
func TestStreamProcessor_OnInterruptedSkippedWhenTerminalSeen(t *testing.T) {
	source := &interruptedSource{
		events: [][]byte{
			[]byte(interruptedDeltaEvent),
			[]byte(interruptedTerminalEvent),
		},
		readErr: io.ErrUnexpectedEOF,
	}
	writer := &scriptedWriter{}
	calls := 0
	processor := NewStreamProcessor(StreamConfig{
		Source:          source,
		Writer:          writer,
		Context:         context.Background(),
		BufferRawStream: true,
		TerminalEvents: map[string]struct{}{
			"response.completed": {},
		},
		OnInterrupted: func(ctx context.Context, rawStream []byte) ([]byte, error) {
			calls++
			return []byte(incompleteTerminalEvent), model.ErrIncompleteUpstreamStream
		},
	})

	err := processor.Run()
	if err == nil {
		t.Fatal("expected interrupted stream error")
	}
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("original read error must be preserved, got %v", err)
	}
	if errors.Is(err, model.ErrIncompleteUpstreamStream) {
		t.Fatalf("terminal already delivered, incomplete sentinel must not leak, got %v", err)
	}
	if calls != 0 {
		t.Fatalf("OnInterrupted called %d times, want 0", calls)
	}
	output := writer.buf.String()
	if strings.Count(output, "response.completed") != 1 {
		t.Fatalf("terminal must appear exactly once, got %q", output)
	}
	if strings.Contains(output, "response.incomplete") {
		t.Fatalf("no second terminal may be synthesized, got %q", output)
	}
	if !processor.PayloadWritten() {
		t.Fatal("payload must be marked written")
	}
}

// Without any payload delivered to the client the attempt is still retryable:
// OnInterrupted must not run and nothing may be written.
func TestStreamProcessor_OnInterruptedSkippedWithoutPayload(t *testing.T) {
	source := &interruptedSource{readErr: io.ErrUnexpectedEOF}
	writer := &scriptedWriter{}
	calls := 0
	processor := NewStreamProcessor(StreamConfig{
		Source:          source,
		Writer:          writer,
		Context:         context.Background(),
		BufferRawStream: true,
		TerminalEvents: map[string]struct{}{
			"response.completed": {},
		},
		OnInterrupted: func(ctx context.Context, rawStream []byte) ([]byte, error) {
			calls++
			return []byte(incompleteTerminalEvent), model.ErrIncompleteUpstreamStream
		},
	})

	err := processor.Run()
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("original read error must be preserved, got %v", err)
	}
	if errors.Is(err, model.ErrIncompleteUpstreamStream) {
		t.Fatalf("no payload written, incomplete sentinel must not leak, got %v", err)
	}
	if calls != 0 {
		t.Fatalf("OnInterrupted called %d times, want 0", calls)
	}
	if writer.buf.Len() != 0 {
		t.Fatalf("nothing may be written without payload, got %q", writer.buf.String())
	}
	if processor.PayloadWritten() {
		t.Fatal("payload must not be marked written")
	}
}

// A downstream disconnect is not an upstream interruption: OnInterrupted must
// not run and the disconnect error is returned unchanged.
func TestStreamProcessor_OnInterruptedSkippedOnClientDisconnect(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	source := &cancelTestSource{first: []byte(interruptedDeltaEvent)}
	writer := &scriptedWriter{}
	calls := 0
	firstTokenSeen := make(chan struct{})
	processor := NewStreamProcessor(StreamConfig{
		Source:  source,
		Writer:  writer,
		Context: ctx,
		OnFirstToken: func() {
			close(firstTokenSeen)
		},
		OnInterrupted: func(ctx context.Context, rawStream []byte) ([]byte, error) {
			calls++
			return []byte(incompleteTerminalEvent), model.ErrIncompleteUpstreamStream
		},
	})

	errChan := make(chan error, 1)
	go func() {
		errChan <- processor.Run()
	}()

	<-firstTokenSeen
	cancel()
	err := <-errChan

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	if calls != 0 {
		t.Fatalf("OnInterrupted called %d times on client disconnect, want 0", calls)
	}
	if !processor.PayloadWritten() {
		t.Fatal("payload written before disconnect must stay marked")
	}
	if strings.Contains(writer.buf.String(), "response.incomplete") {
		t.Fatalf("disconnect must not synthesize a terminal, got %q", writer.buf.String())
	}
}

func TestStreamProcessorFinalizeWriteErrorPreservesIncompleteSentinel(t *testing.T) {
	writeErr := errors.New("terminal write failed")
	source := &interruptedSource{readErr: io.EOF}
	writer := &scriptedWriter{results: []writeResult{{n: 0, err: writeErr}}}
	processor := NewStreamProcessor(StreamConfig{
		Source:  source,
		Writer:  writer,
		Context: context.Background(),
		Finalize: func(context.Context) ([]byte, error) {
			return []byte(incompleteTerminalEvent), model.ErrIncompleteUpstreamStream
		},
	})

	err := processor.Run()
	if !errors.Is(err, writeErr) {
		t.Fatalf("expected terminal write error, got %v", err)
	}
	if !errors.Is(err, model.ErrIncompleteUpstreamStream) {
		t.Fatalf("expected incomplete sentinel to survive terminal write error, got %v", err)
	}
	if !errors.Is(err, ErrDownstreamWriteFailed) {
		t.Fatalf("expected downstream write classification to survive finalization, got %v", err)
	}
}

func TestStreamProcessorIncompleteFinalizeWriteErrorPreservesSentinel(t *testing.T) {
	writeErr := errors.New("incomplete terminal write failed")
	source := &interruptedSource{events: [][]byte{[]byte(interruptedDeltaEvent)}, readErr: io.EOF}
	writer := &scriptedWriter{results: []writeResult{{n: len(interruptedDeltaEvent)}, {n: 0, err: writeErr}}}
	processor := NewStreamProcessor(StreamConfig{
		Source:          source,
		Writer:          writer,
		Context:         context.Background(),
		BufferRawStream: true,
		TerminalEvents:  map[string]struct{}{"response.completed": {}},
		IncompleteFinalize: func(context.Context, []byte) ([]byte, error) {
			return []byte(incompleteTerminalEvent), model.ErrIncompleteUpstreamStream
		},
	})

	err := processor.Run()
	if !errors.Is(err, writeErr) {
		t.Fatalf("expected incomplete terminal write error, got %v", err)
	}
	if !errors.Is(err, model.ErrIncompleteUpstreamStream) {
		t.Fatalf("expected incomplete sentinel to survive incomplete terminal write error, got %v", err)
	}
	if !errors.Is(err, ErrDownstreamWriteFailed) {
		t.Fatalf("expected downstream write classification to survive incomplete finalization, got %v", err)
	}
}

func TestStreamProcessorDownstreamWriteFailureIsClassified(t *testing.T) {
	writeErr := errors.New("broken pipe")
	processor := NewStreamProcessor(StreamConfig{
		Source:  &interruptedSource{events: [][]byte{[]byte("payload")}, readErr: io.EOF},
		Writer:  &scriptedWriter{results: []writeResult{{n: 0, err: writeErr}}},
		Context: context.Background(),
	})

	err := processor.Run()
	if !errors.Is(err, ErrDownstreamWriteFailed) {
		t.Fatalf("expected downstream write classification, got %v", err)
	}
	if errors.Is(err, model.ErrIncompleteUpstreamStream) {
		t.Fatalf("downstream write failure must not be classified as incomplete upstream stream: %v", err)
	}
}

func TestStreamProcessorTransformErrorAfterPayloadSynthesizesTerminal(t *testing.T) {
	transformErr := errors.New("malformed upstream event")
	source := &interruptedSource{
		events:  [][]byte{[]byte("first"), []byte("second")},
		readErr: io.EOF,
	}
	writer := &scriptedWriter{}
	calls := 0
	processor := NewStreamProcessor(StreamConfig{
		Source:          source,
		Writer:          writer,
		Context:         context.Background(),
		BufferRawStream: true,
		TerminalEvents:  map[string]struct{}{"response.completed": {}},
		Transform: func(_ context.Context, data []byte) ([]byte, error) {
			if string(data) == "second" {
				return nil, transformErr
			}
			return []byte("data: partial\n\n"), nil
		},
		OnInterrupted: func(context.Context, []byte) ([]byte, error) {
			calls++
			return []byte(incompleteTerminalEvent), model.ErrIncompleteUpstreamStream
		},
	})

	err := processor.Run()
	if !errors.Is(err, transformErr) {
		t.Fatalf("expected original transform error, got %v", err)
	}
	if !errors.Is(err, model.ErrIncompleteUpstreamStream) {
		t.Fatalf("expected incomplete sentinel after transform error, got %v", err)
	}
	if calls != 1 {
		t.Fatalf("OnInterrupted called %d times, want 1", calls)
	}
	output := writer.buf.String()
	if !strings.Contains(output, "partial") || strings.Count(output, "response.incomplete") != 1 {
		t.Fatalf("expected partial payload plus one incomplete terminal, got %q", output)
	}
}

func TestStreamProcessorCompletedTransformTerminalWinsClientCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	writer := &cancelAfterWriteWriter{scriptedWriter: &scriptedWriter{}, cancel: cancel}
	observerCalls := 0
	processor := NewStreamProcessor(StreamConfig{
		Source:  newMockStreamSource([][]byte{[]byte("terminal")}),
		Writer:  writer,
		Context: ctx,
		Transform: func(context.Context, []byte) ([]byte, error) {
			return []byte("data: {\"type\":\"response.completed\"}\n\n"), nil
		},
		TerminalObserver: func() (model.PassthroughTerminalOutcome, error) {
			observerCalls++
			return model.PassthroughTerminalOutcomeCompleted, nil
		},
	})

	if err := processor.Run(); err != nil {
		t.Fatalf("delivered completed terminal must remain successful after cancellation, got %v", err)
	}
	if got := processor.PassthroughOutcome(); got != model.PassthroughTerminalOutcomeCompleted {
		t.Fatalf("outcome = %q, want completed", got)
	}
	if observerCalls != 1 {
		t.Fatalf("terminal observer calls = %d, want 1", observerCalls)
	}
	if !processor.PayloadWritten() {
		t.Fatal("delivered terminal must mark payload visible")
	}
}
