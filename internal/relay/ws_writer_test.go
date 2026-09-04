package relay

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/bestruirui/octopus/internal/relay/stream"
	"github.com/coder/websocket"
)

// stubWSFrameWriter implements wsFrameWriter with scripted per-frame results
// so partial multi-frame Write behavior can be exercised without a live
// WebSocket connection.
type stubWSFrameWriter struct {
	frames       []string // frames handed to Write, in order (including failed ones)
	results      []error  // per-frame result; indexes beyond the slice succeed
	closedCode   websocket.StatusCode
	closedReason string
}

func (s *stubWSFrameWriter) Write(_ context.Context, _ websocket.MessageType, data []byte) error {
	idx := len(s.frames)
	s.frames = append(s.frames, string(data))
	if idx < len(s.results) && s.results[idx] != nil {
		return s.results[idx]
	}
	return nil
}

func (s *stubWSFrameWriter) Close(code websocket.StatusCode, reason string) error {
	s.closedCode = code
	s.closedReason = reason
	return nil
}

func newTestWSStreamWriter(conn wsFrameWriter) *WSStreamWriter {
	return &WSStreamWriter{conn: conn, ctx: context.Background()}
}

// A single Write may carry multiple SSE data lines. When a later frame fails
// after an earlier one reached the client, the payload is already visible
// downstream: Written() must be true even though Write reports an error, so
// the relay Written defense line is not bypassed.
func TestWSStreamWriterPartialMultiFrameWriteMarksWritten(t *testing.T) {
	writeErr := errors.New("second frame failed")
	conn := &stubWSFrameWriter{results: []error{nil, writeErr}}
	w := newTestWSStreamWriter(conn)

	n, err := w.Write([]byte("data: {\"a\":1}\n\ndata: {\"b\":2}\n\n"))
	if !errors.Is(err, writeErr) {
		t.Fatalf("expected write error, got %v", err)
	}
	if n != 0 {
		t.Fatalf("failed write must report 0 bytes, got %d", n)
	}
	if !w.Written() {
		t.Fatal("first frame reached the client, Written() must be true despite the error")
	}
	want := []string{`{"a":1}`, `{"b":2}`}
	if len(conn.frames) != len(want) {
		t.Fatalf("expected %d frame attempts, got %v", len(want), conn.frames)
	}
	for i, frame := range want {
		if conn.frames[i] != frame {
			t.Fatalf("frame %d = %q, want %q", i, conn.frames[i], frame)
		}
	}
}

// When the very first frame fails nothing reached the client, so the write
// attempt is still retryable and Written() must stay false.
func TestWSStreamWriterFirstFrameFailureNotWritten(t *testing.T) {
	writeErr := errors.New("first frame failed")
	conn := &stubWSFrameWriter{results: []error{writeErr}}
	w := newTestWSStreamWriter(conn)

	n, err := w.Write([]byte("data: {\"a\":1}\n\n"))
	if !errors.Is(err, writeErr) {
		t.Fatalf("expected write error, got %v", err)
	}
	if n != 0 {
		t.Fatalf("failed write must report 0 bytes, got %d", n)
	}
	if w.Written() {
		t.Fatal("no frame reached the client, Written() must stay false")
	}
	if len(conn.frames) != 1 || conn.frames[0] != `{"a":1}` {
		t.Fatalf("unexpected frame attempts: %v", conn.frames)
	}
}

func TestWSStreamWriterSuccessMarksWritten(t *testing.T) {
	conn := &stubWSFrameWriter{}
	w := newTestWSStreamWriter(conn)

	input := []byte("data: {\"a\":1}\n\ndata: {\"b\":2}\n\n")
	n, err := w.Write(input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != len(input) {
		t.Fatalf("successful write must report len(data)=%d, got %d", len(input), n)
	}
	if !w.Written() {
		t.Fatal("successful frames must mark the writer as written")
	}
	if len(conn.frames) != 2 {
		t.Fatalf("expected 2 frames, got %v", conn.frames)
	}
}

// A write that only carries the [DONE] sentinel forwards no payload, so it
// must neither emit a frame nor mark the writer as written.
func TestWSStreamWriterDoneOnlyWriteNotWritten(t *testing.T) {
	conn := &stubWSFrameWriter{}
	w := newTestWSStreamWriter(conn)

	input := []byte("data: [DONE]\n\n")
	n, err := w.Write(input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if n != len(input) {
		t.Fatalf("successful write must report len(data)=%d, got %d", len(input), n)
	}
	if w.Written() {
		t.Fatal("[DONE]-only write must not count as payload")
	}
	if len(conn.frames) != 0 {
		t.Fatalf("no frame expected for [DONE], got %v", conn.frames)
	}
}

func TestWSStreamWriterCloseWithErrorClosesFrameSink(t *testing.T) {
	conn := &stubWSFrameWriter{}
	w := newTestWSStreamWriter(conn)
	w.CloseWithError()
	if conn.closedCode != websocket.StatusInternalError || conn.closedReason == "" {
		t.Fatalf("expected downstream error close, code=%d reason=%q", conn.closedCode, conn.closedReason)
	}
}

type wsDoneOnlyStreamSource struct {
	read bool
}

func (s *wsDoneOnlyStreamSource) ReadEvent(context.Context) ([]byte, error) {
	if s.read {
		return nil, io.EOF
	}
	s.read = true
	return []byte("done"), nil
}

func (s *wsDoneOnlyStreamSource) Close() error { return nil }

func TestWSStreamWriterDoneOnlyProcessorRemainsEmpty(t *testing.T) {
	conn := &stubWSFrameWriter{}
	writer := newTestWSStreamWriter(conn)
	processor := stream.NewStreamProcessor(stream.StreamConfig{
		Source:  &wsDoneOnlyStreamSource{},
		Writer:  writer,
		Context: context.Background(),
		Transform: func(context.Context, []byte) ([]byte, error) {
			return []byte("data: [DONE]\n\n"), nil
		},
	})

	if err := processor.Run(); !errors.Is(err, stream.ErrEmptyUpstreamStream) {
		t.Fatalf("done-only WS stream error = %v, want ErrEmptyUpstreamStream", err)
	}
	if processor.PayloadWritten() || writer.Written() {
		t.Fatal("suppressed [DONE] must not count as client-visible payload")
	}
	if len(conn.frames) != 0 {
		t.Fatalf("done-only stream emitted frames: %v", conn.frames)
	}
}
