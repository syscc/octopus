package stream

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/bestruirui/octopus/internal/transformer/model"
)

// mockStreamWriter implements StreamWriter for testing.
type mockStreamWriter struct {
	buffer  bytes.Buffer
	written bool
	headers http.Header
}

func newMockStreamWriter() *mockStreamWriter {
	return &mockStreamWriter{
		headers: make(http.Header),
	}
}

func (m *mockStreamWriter) Write(data []byte) (int, error) {
	m.written = true
	return m.buffer.Write(data)
}

func (m *mockStreamWriter) Flush() {}

func (m *mockStreamWriter) Written() bool {
	return m.written
}

func (m *mockStreamWriter) Header() http.Header {
	return m.headers
}

func (m *mockStreamWriter) WriteHeader(code int) {}

// mockStreamSource implements StreamSource for testing.
type mockStreamSource struct {
	events [][]byte
	index  int
	closed bool
}

func newMockStreamSource(events [][]byte) *mockStreamSource {
	return &mockStreamSource{events: events}
}

func (m *mockStreamSource) ReadEvent(ctx context.Context) ([]byte, error) {
	if m.index >= len(m.events) {
		return nil, io.EOF
	}
	data := m.events[m.index]
	m.index++
	return data, nil
}

func (m *mockStreamSource) Close() error {
	m.closed = true
	return nil
}

func TestStreamProcessor_BasicFlow(t *testing.T) {
	events := [][]byte{
		[]byte(`{"data":"chunk1"}`),
		[]byte(`{"data":"chunk2"}`),
		[]byte(`{"data":"chunk3"}`),
	}

	source := newMockStreamSource(events)
	writer := newMockStreamWriter()
	ctx := context.Background()

	processor := NewStreamProcessor(StreamConfig{
		Source:  source,
		Writer:  writer,
		Context: ctx,
	})

	err := processor.Run()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !source.closed {
		t.Error("source not closed")
	}

	output := writer.buffer.String()
	for _, event := range events {
		if !strings.Contains(output, string(event)) {
			t.Errorf("output missing event: %s", event)
		}
	}
}

func TestStreamProcessor_WithTransform(t *testing.T) {
	events := [][]byte{
		[]byte(`chunk1`),
		[]byte(`chunk2`),
	}

	source := newMockStreamSource(events)
	writer := newMockStreamWriter()
	ctx := context.Background()

	transform := func(ctx context.Context, data []byte) ([]byte, error) {
		// Wrap in SSE format
		return []byte("data: " + string(data) + "\n\n"), nil
	}

	processor := NewStreamProcessor(StreamConfig{
		Source:    source,
		Writer:    writer,
		Context:   ctx,
		Transform: transform,
	})

	err := processor.Run()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	output := writer.buffer.String()
	expected := "data: chunk1\n\ndata: chunk2\n\n"
	if output != expected {
		t.Errorf("unexpected output:\ngot:  %q\nwant: %q", output, expected)
	}
}

func TestStreamProcessor_FinalizeWritesTerminalPayload(t *testing.T) {
	source := newMockStreamSource([][]byte{[]byte(`finish`)})
	writer := newMockStreamWriter()
	firstTokenCalled := false
	processor := NewStreamProcessor(StreamConfig{
		Source:  source,
		Writer:  writer,
		Context: context.Background(),
		Transform: func(context.Context, []byte) ([]byte, error) {
			return nil, nil
		},
		Finalize: func(context.Context) ([]byte, error) {
			return []byte("data: terminal\n\n"), nil
		},
		OnFirstToken: func() {
			firstTokenCalled = true
		},
	})

	if err := processor.Run(); err != nil {
		t.Fatalf("unexpected finalize error: %v", err)
	}
	if got := writer.buffer.String(); got != "data: terminal\n\n" {
		t.Fatalf("unexpected finalized output %q", got)
	}
	if !processor.PayloadWritten() || !firstTokenCalled {
		t.Fatalf("expected finalized payload to count as the first token")
	}
}

func TestStreamProcessor_FinalizeErrorStillWritesOutput(t *testing.T) {
	source := newMockStreamSource(nil)
	writer := newMockStreamWriter()
	firstTokenCalled := false
	processor := NewStreamProcessor(StreamConfig{
		Source:  source,
		Writer:  writer,
		Context: context.Background(),
		Finalize: func(context.Context) ([]byte, error) {
			return []byte("data: {\"type\":\"response.incomplete\"}\n\n"), model.ErrIncompleteUpstreamStream
		},
		OnFirstToken: func() {
			firstTokenCalled = true
		},
	})

	err := processor.Run()
	if err == nil {
		t.Fatal("expected incomplete finalize error")
	}
	if !errors.Is(err, model.ErrIncompleteUpstreamStream) {
		t.Fatalf("expected ErrIncompleteUpstreamStream, got %v", err)
	}
	if got := writer.buffer.String(); !strings.Contains(got, "response.incomplete") {
		t.Fatalf("finalize output must be written before the error is propagated, got %q", got)
	}
	if !processor.PayloadWritten() || !firstTokenCalled {
		t.Fatal("written finalize payload must count as first token")
	}
}

func TestStreamProcessorTransformOutputAndErrorFinalizeOnce(t *testing.T) {
	semanticErr := errors.New("semantic terminal failure")
	source := newMockStreamSource([][]byte{[]byte("upstream")})
	writer := newMockStreamWriter()
	finishCalls := 0
	interruptedCalls := 0
	processor := NewStreamProcessor(StreamConfig{
		Source:  source,
		Writer:  writer,
		Context: context.Background(),
		Transform: func(context.Context, []byte) ([]byte, error) {
			return []byte("data: {\"type\":\"error\"}\n\n"), semanticErr
		},
		TerminalObserver: func() (model.PassthroughTerminalOutcome, error) {
			return model.PassthroughTerminalOutcomeFailed, semanticErr
		},
		OnInterrupted: func(context.Context, []byte) ([]byte, error) {
			interruptedCalls++
			return []byte("data: synthetic\n\n"), nil
		},
		OnFinish: func(context.Context, []byte) error {
			finishCalls++
			return nil
		},
	})

	err := processor.Run()
	if !errors.Is(err, semanticErr) {
		t.Fatalf("expected semantic error, got %v", err)
	}
	if got := writer.buffer.String(); strings.Count(got, "data:") != 1 || !strings.Contains(got, `"type":"error"`) {
		t.Fatalf("expected one delivered terminal frame, got %q", got)
	}
	if processor.PassthroughOutcome() != model.PassthroughTerminalOutcomeFailed {
		t.Fatalf("outcome = %q, want failed", processor.PassthroughOutcome())
	}
	if finishCalls != 1 || interruptedCalls != 0 {
		t.Fatalf("expected exactly one finish and no synthetic interruption, finish=%d interrupted=%d", finishCalls, interruptedCalls)
	}
}

func TestStreamProcessorCommentOnlyEOFIsEmpty(t *testing.T) {
	source := newMockStreamSource([][]byte{[]byte(": keep-alive\n\n")})
	writer := newMockStreamWriter()
	processor := NewStreamProcessor(StreamConfig{
		Source:          source,
		Writer:          writer,
		Context:         context.Background(),
		BufferRawStream: true,
		TransformWithOutcome: func(_ context.Context, data []byte, _ bool) StreamTransformResult {
			return StreamTransformResult{Output: data, NonPayload: true}
		},
	})

	err := processor.Run()
	if !errors.Is(err, ErrEmptyUpstreamStream) {
		t.Fatalf("comment-only EOF error = %v, want ErrEmptyUpstreamStream", err)
	}
	if processor.PayloadWritten() {
		t.Fatal("comment-only stream must not count as payload")
	}
	if got := writer.buffer.String(); got != ": keep-alive\n\n" {
		t.Fatalf("comment should still be forwarded, got %q", got)
	}
}

func TestStreamProcessor_PassthroughIncompleteTerminalAtEOF(t *testing.T) {
	partial := "data: {\"type\":\"response.output_text.delta\",\"delta\":\"partial\"}\n\n"
	source := newMockStreamSource([][]byte{[]byte(partial)})
	writer := newMockStreamWriter()
	onFinishCalled := false
	cleanupErr := errors.New("cleanup failed")
	processor := NewStreamProcessor(StreamConfig{
		Source:          source,
		Writer:          writer,
		Context:         context.Background(),
		BufferRawStream: true,
		TerminalEvents: map[string]struct{}{
			"response.completed": {},
		},
		IncompleteFinalize: func(ctx context.Context, rawStream []byte) ([]byte, error) {
			if !bytes.Contains(rawStream, []byte("response.output_text.delta")) {
				t.Fatalf("incomplete finalizer must receive the buffered raw stream, got %q", rawStream)
			}
			return []byte("data: {\"type\":\"response.incomplete\"}\n\n"), model.ErrIncompleteUpstreamStream
		},
		OnFinish: func(context.Context, []byte) error {
			onFinishCalled = true
			return cleanupErr
		},
	})

	err := processor.Run()
	if !errors.Is(err, model.ErrIncompleteUpstreamStream) {
		t.Fatalf("expected ErrIncompleteUpstreamStream, got %v", err)
	}
	if !errors.Is(err, cleanupErr) {
		t.Fatalf("expected OnFinish error to be joined without losing incomplete sentinel, got %v", err)
	}
	output := writer.buffer.String()
	if !strings.Contains(output, partial) {
		t.Fatalf("original passthrough bytes must be preserved, got %q", output)
	}
	if strings.Count(output, "response.incomplete") != 1 {
		t.Fatalf("expected exactly one synthesized response.incomplete, got %q", output)
	}
	if strings.Contains(output, "response.completed") {
		t.Fatalf("completed must not be synthesized, got %q", output)
	}
	if !processor.PayloadWritten() || !onFinishCalled {
		t.Fatal("incomplete terminal must leave payloadWritten true and run OnFinish")
	}
}

func TestStreamProcessor_PassthroughTerminalStreamSucceeds(t *testing.T) {
	source := newMockStreamSource([][]byte{[]byte("data: {\"type\":\"response.output_text.delta\"}\n\ndata: {\"type\":\"response.completed\"}\n\n")})
	writer := newMockStreamWriter()
	processor := NewStreamProcessor(StreamConfig{
		Source:          source,
		Writer:          writer,
		Context:         context.Background(),
		BufferRawStream: true,
		TerminalEvents: map[string]struct{}{
			"response.completed": {},
		},
		IncompleteFinalize: func(ctx context.Context, rawStream []byte) ([]byte, error) {
			t.Fatal("incomplete finalizer must not run for a terminal stream")
			return nil, nil
		},
	})

	if err := processor.Run(); err != nil {
		t.Fatalf("terminal passthrough stream must succeed, got %v", err)
	}
	output := writer.buffer.String()
	if strings.Count(output, "response.completed") != 1 || strings.Contains(output, "response.incomplete") {
		t.Fatalf("terminal stream bytes must be preserved verbatim, got %q", output)
	}
}

func TestStreamProcessor_PassthroughIncompleteDisabledWithoutTerminalEvents(t *testing.T) {
	source := newMockStreamSource([][]byte{[]byte("data: legacy\n\n")})
	writer := newMockStreamWriter()
	processor := NewStreamProcessor(StreamConfig{
		Source:          source,
		Writer:          writer,
		Context:         context.Background(),
		BufferRawStream: true,
		IncompleteFinalize: func(ctx context.Context, rawStream []byte) ([]byte, error) {
			t.Fatal("incomplete finalizer must not run without configured terminal events")
			return nil, nil
		},
	})

	if err := processor.Run(); err != nil {
		t.Fatalf("legacy non-terminal passthrough must stay successful, got %v", err)
	}
	if !processor.PayloadWritten() {
		t.Fatal("payload should be written")
	}
}

func TestStreamProcessor_FirstTokenCallback(t *testing.T) {
	events := [][]byte{
		[]byte(`chunk1`),
		[]byte(`chunk2`),
	}

	source := newMockStreamSource(events)
	writer := newMockStreamWriter()
	ctx := context.Background()

	firstTokenCalled := false
	processor := NewStreamProcessor(StreamConfig{
		Source:  source,
		Writer:  writer,
		Context: ctx,
		OnFirstToken: func() {
			firstTokenCalled = true
		},
	})

	err := processor.Run()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !firstTokenCalled {
		t.Error("OnFirstToken not called")
	}
}

func TestStreamProcessor_EmptyStream(t *testing.T) {
	source := newMockStreamSource([][]byte{})
	writer := newMockStreamWriter()
	ctx := context.Background()

	processor := NewStreamProcessor(StreamConfig{
		Source:  source,
		Writer:  writer,
		Context: ctx,
	})

	err := processor.Run()
	if err == nil {
		t.Fatal("expected error for empty stream")
	}

	if !errors.Is(err, ErrEmptyUpstreamStream) {
		t.Errorf("expected ErrEmptyUpstreamStream, got: %v", err)
	}
}

func TestStreamProcessor_ContextCancellation(t *testing.T) {
	// Source that emits one chunk then blocks until cancelled
	source := &cancelTestSource{first: []byte(`chunk1`)}
	writer := newMockStreamWriter()
	ctx, cancel := context.WithCancel(context.Background())

	firstTokenSeen := make(chan struct{})
	processor := NewStreamProcessor(StreamConfig{
		Source:  source,
		Writer:  writer,
		Context: ctx,
		OnFirstToken: func() {
			close(firstTokenSeen)
		},
	})

	errChan := make(chan error, 1)
	go func() {
		errChan <- processor.Run()
	}()

	// Wait until first token is processed, then cancel
	<-firstTokenSeen
	cancel()

	err := <-errChan
	if err == nil {
		t.Fatal("expected context cancellation error")
	}

	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled, got: %v", err)
	}
}

// cancelTestSource emits one chunk then blocks until context is cancelled.
type cancelTestSource struct {
	first  []byte
	sent   bool
	closed bool
}

func (s *cancelTestSource) ReadEvent(ctx context.Context) ([]byte, error) {
	if !s.sent {
		s.sent = true
		return s.first, nil
	}
	<-ctx.Done()
	return nil, ctx.Err()
}

func (s *cancelTestSource) Close() error {
	s.closed = true
	return nil
}

func TestStreamProcessor_BufferRawStream(t *testing.T) {
	events := [][]byte{
		[]byte(`chunk1`),
		[]byte(`chunk2`),
	}

	source := newMockStreamSource(events)
	writer := newMockStreamWriter()
	ctx := context.Background()

	var bufferedData []byte
	processor := NewStreamProcessor(StreamConfig{
		Source:          source,
		Writer:          writer,
		Context:         ctx,
		BufferRawStream: true,
		OnFinish: func(ctx context.Context, rawStream []byte) error {
			bufferedData = rawStream
			return nil
		},
	})

	err := processor.Run()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expected := "chunk1chunk2"
	if string(bufferedData) != expected {
		t.Errorf("unexpected buffered data:\ngot:  %q\nwant: %q", bufferedData, expected)
	}
}

func TestStreamProcessor_FirstTokenTimeout(t *testing.T) {
	// Source that blocks forever
	blockingSource := &blockingStreamSource{}
	writer := newMockStreamWriter()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	processor := NewStreamProcessor(StreamConfig{
		Source:            blockingSource,
		Writer:            writer,
		Context:           ctx,
		FirstTokenTimeout: 10 * time.Millisecond,
	})

	err := processor.Run()
	if err == nil {
		t.Fatal("expected timeout error")
	}

	if !strings.Contains(err.Error(), "first token timeout") {
		t.Errorf("unexpected error: %v", err)
	}
}

// blockingStreamSource blocks forever in ReadEvent.
type blockingStreamSource struct {
	closed bool
}

func (s *blockingStreamSource) ReadEvent(ctx context.Context) ([]byte, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func (s *blockingStreamSource) Close() error {
	s.closed = true
	return nil
}

func TestStreamProcessor_TerminalEventDetection(t *testing.T) {
	// SSE stream with terminal event
	sseData := `data: {"type":"message_start"}

data: {"type":"content_block_delta"}

data: {"type":"message_stop"}

`
	source := newMockStreamSource([][]byte{[]byte(sseData)})
	writer := newMockStreamWriter()
	ctx, cancel := context.WithCancel(context.Background())

	terminalEvents := map[string]struct{}{
		"message_stop": {},
	}

	firstTokenSeen := make(chan struct{})
	processor := NewStreamProcessor(StreamConfig{
		Source:          source,
		Writer:          writer,
		Context:         ctx,
		BufferRawStream: true,
		TerminalEvents:  terminalEvents,
		OnFirstToken: func() {
			close(firstTokenSeen)
		},
	})

	// Start processor in goroutine
	errChan := make(chan error, 1)
	go func() {
		errChan <- processor.Run()
	}()

	// Wait until first token is written, then cancel
	<-firstTokenSeen
	cancel()

	// Should treat as success because terminal event was reached
	err := <-errChan
	if err != nil {
		t.Errorf("expected nil error due to terminal event detection, got: %v", err)
	}
}

func TestRawSource(t *testing.T) {
	data := []byte("raw chunk data")
	reader := io.NopCloser(bytes.NewReader(data))

	source := NewRawSource(reader, 10)
	defer source.Close()

	var chunks [][]byte
	for {
		chunk, err := source.ReadEvent(context.Background())
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(chunk) > 0 {
			chunks = append(chunks, chunk)
		}
	}

	// Reconstruct data
	result := bytes.Join(chunks, nil)
	if !bytes.Equal(result, data) {
		t.Errorf("data mismatch:\ngot:  %q\nwant: %q", result, data)
	}
}

func TestSSESource(t *testing.T) {
	sseData := `data: event1

data: event2

data: event3

`
	reader := io.NopCloser(strings.NewReader(sseData))
	source := NewSSESource(reader, 0)
	defer source.Close()

	expected := []string{"event1", "event2", "event3"}
	var events []string

	for {
		data, err := source.ReadEvent(context.Background())
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		events = append(events, string(data))
	}

	if len(events) != len(expected) {
		t.Fatalf("event count mismatch: got %d, want %d", len(events), len(expected))
	}

	for i, ev := range events {
		if ev != expected[i] {
			t.Errorf("event[%d] mismatch: got %q, want %q", i, ev, expected[i])
		}
	}
}
