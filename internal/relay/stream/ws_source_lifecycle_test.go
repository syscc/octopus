package stream

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"
)

// Compile-time proof that WSSource signals stream failures so the processor
// can remove its pooled connection instead of returning it for reuse.
var _ errorSignalingSource = (*WSSource)(nil)

// lifecycleSource is a StreamSource that records close ordering and captures
// the context handed to ReadEvent so tests can verify the processor cancels
// the read context before closing the source.
type lifecycleSource struct {
	events  [][]byte
	index   int
	readErr error
	// blockAfterEvents makes ReadEvent block after the events are consumed
	// (like a silent WS upstream) instead of returning io.EOF.
	blockAfterEvents bool

	mu              sync.Mutex
	activeCtx       context.Context
	closeOrder      []string
	closeCtxErr     error // ctx state observed by Close for the in-flight read
	closedWithError bool
}

func (s *lifecycleSource) ReadEvent(ctx context.Context) ([]byte, error) {
	s.mu.Lock()
	s.activeCtx = ctx
	s.mu.Unlock()

	if s.readErr != nil {
		return nil, s.readErr
	}
	if s.index >= len(s.events) {
		if !s.blockAfterEvents {
			return nil, io.EOF
		}
		// Block like a silent WS upstream until the read context is canceled.
		<-ctx.Done()
		return nil, ctx.Err()
	}
	data := s.events[s.index]
	s.index++
	return data, nil
}

func (s *lifecycleSource) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.activeCtx != nil {
		s.closeCtxErr = s.activeCtx.Err()
	} else {
		s.closeCtxErr = errNoActiveRead
	}
	s.closeOrder = append(s.closeOrder, "close")
	return nil
}

func (s *lifecycleSource) CloseWithError() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closedWithError = true
	s.closeOrder = append(s.closeOrder, "closeWithError")
}

var errNoActiveRead = errors.New("no active read")

func (s *lifecycleSource) order() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.closeOrder...)
}

// A failed stream must signal the error close before the plain close so pooled
// connections are removed rather than returned for reuse.
func TestStreamProcessor_ErrorStreamClosesSourceWithErrorFirst(t *testing.T) {
	source := &lifecycleSource{readErr: errors.New("upstream read failed")}
	processor := NewStreamProcessor(StreamConfig{
		Source:  source,
		Writer:  newMockStreamWriter(),
		Context: context.Background(),
	})

	err := processor.Run()
	if err == nil {
		t.Fatalf("expected processor error")
	}

	order := source.order()
	if len(order) != 2 || order[0] != "closeWithError" || order[1] != "close" {
		t.Fatalf("expected [closeWithError close] on error, got %v", order)
	}
	if !source.closedWithError {
		t.Fatalf("expected CloseWithError to be invoked")
	}
}

// A successful stream must only close the source normally (pool reuse path).
func TestStreamProcessor_SuccessfulStreamSkipsErrorClose(t *testing.T) {
	source := &lifecycleSource{events: [][]byte{[]byte(`{"data":"chunk"}`)}}
	processor := NewStreamProcessor(StreamConfig{
		Source:  source,
		Writer:  newMockStreamWriter(),
		Context: context.Background(),
	})

	if err := processor.Run(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	order := source.order()
	if len(order) != 1 || order[0] != "close" {
		t.Fatalf("expected single close on success, got %v", order)
	}
	if source.closedWithError {
		t.Fatalf("expected no CloseWithError on success")
	}
}

// The read context must already be canceled when the source is closed, so a
// pooled WS connection is never returned while its reader goroutine may still
// be blocked on the socket.
func TestStreamProcessor_CancelsReadContextBeforeClosingSource(t *testing.T) {
	source := &lifecycleSource{blockAfterEvents: true}
	ctx, cancel := context.WithCancel(context.Background())
	processor := NewStreamProcessor(StreamConfig{
		Source:  source,
		Writer:  newMockStreamWriter(),
		Context: ctx,
	})

	done := make(chan error, 1)
	go func() {
		done <- processor.Run()
	}()

	// Wait until the reader goroutine is parked inside ReadEvent.
	deadline := time.Now().Add(5 * time.Second)
	for {
		source.mu.Lock()
		active := source.activeCtx
		source.mu.Unlock()
		if active != nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("ReadEvent was never started")
		}
		time.Sleep(5 * time.Millisecond)
	}

	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("processor did not return after cancellation")
	}

	source.mu.Lock()
	observed := source.closeCtxErr
	source.mu.Unlock()
	if !errors.Is(observed, context.Canceled) {
		t.Fatalf("expected source close to observe a canceled read context, got %v", observed)
	}
}

// fakeWSUpstreamReader is a minimal WSUpstreamReader recording close calls.
type fakeWSUpstreamReader struct {
	events     [][]byte
	calls      int
	closed     bool
	closedErr  bool
	statusCode int
}

func (f *fakeWSUpstreamReader) ReadEvent(ctx context.Context) ([]byte, error) {
	if f.calls >= len(f.events) {
		return nil, io.EOF
	}
	data := f.events[f.calls]
	f.calls++
	return data, nil
}

func (f *fakeWSUpstreamReader) Close() error {
	f.closed = true
	return nil
}

func (f *fakeWSUpstreamReader) CloseWithError() {
	f.closedErr = true
}

func (f *fakeWSUpstreamReader) StatusCode() int { return f.statusCode }

// WSSource must delegate both close flavors to the underlying reader.
func TestWSSource_DelegatesCloseAndCloseWithError(t *testing.T) {
	fake := &fakeWSUpstreamReader{statusCode: 200}
	source := NewWSSource(fake)

	if err := source.Close(); err != nil {
		t.Fatalf("WSSource.Close failed: %v", err)
	}
	if !fake.closed {
		t.Fatalf("expected underlying reader Close to be called")
	}

	source.CloseWithError()
	if !fake.closedErr {
		t.Fatalf("expected underlying reader CloseWithError to be called")
	}
}

// End to end: a WSSource wrapping a failing upstream must end with the error
// close applied, while a successful stream only closes normally.
func TestWSSource_ProcessorErrorPathAppliesErrorClose(t *testing.T) {
	failing := &errorWSReader{err: errors.New("ws read error")}
	processor := NewStreamProcessor(StreamConfig{
		Source:  NewWSSource(failing),
		Writer:  newMockStreamWriter(),
		Context: context.Background(),
	})
	if err := processor.Run(); err == nil {
		t.Fatalf("expected error from failing ws reader")
	}
	if !failing.closedErr {
		t.Fatalf("expected CloseWithError on failing ws reader")
	}
	if !failing.closed {
		t.Fatalf("expected plain Close on failing ws reader")
	}

	okReader := &fakeWSUpstreamReader{events: [][]byte{[]byte(`{"data":"x"}`)}}
	processor = NewStreamProcessor(StreamConfig{
		Source:  NewWSSource(okReader),
		Writer:  newMockStreamWriter(),
		Context: context.Background(),
	})
	if err := processor.Run(); err != nil {
		t.Fatalf("unexpected error from eof ws reader: %v", err)
	}
	if okReader.closedErr {
		t.Fatalf("expected no CloseWithError on successful ws reader")
	}
	if !okReader.closed {
		t.Fatalf("expected Close on successful ws reader")
	}
}

// errorWSReader returns a single error then EOF.
type errorWSReader struct {
	err       error
	calls     int
	closed    bool
	closedErr bool
}

func (e *errorWSReader) ReadEvent(ctx context.Context) ([]byte, error) {
	e.calls++
	if e.calls == 1 {
		return nil, e.err
	}
	return nil, io.EOF
}

func (e *errorWSReader) Close() error {
	e.closed = true
	return nil
}

func (e *errorWSReader) CloseWithError() {
	e.closedErr = true
}

func (e *errorWSReader) StatusCode() int { return 502 }
