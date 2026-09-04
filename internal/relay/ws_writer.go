package relay

import (
	"bytes"
	"context"
	"net/http"
	"sync"

	"github.com/coder/websocket"
)

// wsFrameWriter abstracts the WebSocket frame sink so partial multi-frame
// writes can be exercised deterministically in tests (*websocket.Conn satisfies it).
type wsFrameWriter interface {
	Write(ctx context.Context, typ websocket.MessageType, data []byte) error
}

type wsFrameCloser interface {
	Close(code websocket.StatusCode, reason string) error
}

// WSStreamWriter implements StreamWriter for WebSocket clients.
// It converts SSE "data: {...}\n\n" formatted bytes to bare JSON WebSocket text frames.
type WSStreamWriter struct {
	conn    wsFrameWriter
	ctx     context.Context
	written bool
	mu      sync.Mutex
}

func NewWSStreamWriter(ctx context.Context, conn *websocket.Conn) *WSStreamWriter {
	return &WSStreamWriter{
		conn: conn,
		ctx:  ctx,
	}
}

func (w *WSStreamWriter) Write(data []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Extract JSON data from SSE format "data: {...}\n\n"
	lines := extractSSEDataLines(data)
	wroteFrame := false
	for _, line := range lines {
		if len(line) == 0 {
			continue
		}
		writeCtx, cancel := context.WithTimeout(w.ctx, wsWriteTimeout)
		err := w.conn.Write(writeCtx, websocket.MessageText, line)
		cancel()
		if err != nil {
			// A single Write may carry multiple SSE data lines (one WS frame
			// each). Once any frame reached the client the payload is visible
			// downstream, so Written() must reflect it even though this Write
			// failed on a later frame; otherwise the Written defense line in
			// relay (no retry/failover/replay after payload) is bypassed.
			if wroteFrame {
				w.written = true
			}
			return 0, err
		}
		wroteFrame = true
	}
	if wroteFrame {
		w.written = true
	}
	return len(data), nil
}

func (w *WSStreamWriter) Flush() {
	// WebSocket sends are immediate, no buffering needed
}

func (w *WSStreamWriter) Written() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.written
}

func (w *WSStreamWriter) Header() http.Header {
	// WebSocket doesn't use HTTP headers for individual messages
	return http.Header{}
}

func (w *WSStreamWriter) WriteHeader(code int) {
	// WebSocket doesn't have per-message status codes
}

// CloseWithError terminates a downstream WebSocket after a visible write
// failed in a way that could not be classified as an ordinary disconnect.
// This prevents the client from waiting indefinitely for a terminal event.
func (w *WSStreamWriter) CloseWithError() {
	if closer, ok := w.conn.(wsFrameCloser); ok {
		_ = closer.Close(websocket.StatusInternalError, "stream write failed")
	}
}

// extractSSEDataLines extracts the data payload from SSE formatted bytes.
// Input format: "data: {json}\n\n" or multiple such lines concatenated.
// Returns the raw JSON data for each line (without "data: " prefix).
func extractSSEDataLines(data []byte) [][]byte {
	var results [][]byte
	prefix := []byte("data: ")

	for _, line := range bytes.Split(data, []byte("\n")) {
		line = bytes.TrimSpace(line)
		if len(line) == 0 {
			continue
		}
		if bytes.HasPrefix(line, prefix) {
			payload := line[len(prefix):]
			if len(payload) > 0 && !bytes.Equal(payload, []byte("[DONE]")) {
				results = append(results, payload)
			}
		}
	}

	return results
}
