package stream

import (
	"context"
)

// WSUpstreamReader abstracts WebSocket upstream reader interface.
// This avoids circular dependency with internal/relay package.
type WSUpstreamReader interface {
	ReadEvent(ctx context.Context) ([]byte, error)
	Close() error
	CloseWithError()
	StatusCode() int
}

// PendingErrorReader exposes an error that was observed together with a
// structured terminal frame. The processor consumes it after the frame has
// been delivered, so accounting retains the transport/provider error without
// generating a duplicate terminal event.
type PendingErrorReader interface {
	PendingError() error
}

// WSSource wraps a WebSocket upstream reader.
type WSSource struct {
	reader WSUpstreamReader
}

// NewWSSource creates a source from a WebSocket reader.
func NewWSSource(reader WSUpstreamReader) *WSSource {
	return &WSSource{reader: reader}
}

// ReadEvent reads the next WebSocket event.
func (s *WSSource) ReadEvent(ctx context.Context) ([]byte, error) {
	return s.reader.ReadEvent(ctx)
}

// Close releases the WebSocket connection.
func (s *WSSource) Close() error {
	if s.reader != nil {
		s.reader.Close()
	}
	return nil
}

// CloseWithError closes the source after a failed stream so the underlying
// pooled connection is removed and closed instead of being reused.
func (s *WSSource) CloseWithError() {
	if s.reader != nil {
		s.reader.CloseWithError()
	}
}

func (s *WSSource) PendingError() error {
	if reader, ok := s.reader.(PendingErrorReader); ok {
		return reader.PendingError()
	}
	return nil
}
