package relay

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/big"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/bestruirui/octopus/internal/utils/log"
	"github.com/coder/websocket"
)

// wsUpstreamReader reads events from an upstream WebSocket connection.
type wsUpstreamReader struct {
	conn       *websocket.Conn
	pc         *pooledConn
	channelID  int
	keyID      int
	done       bool         // true after a terminal event has been returned
	statusCode atomic.Int32 // read by relay after the processor returned, written by the reader goroutine
	pendingErr error        // returned after the corresponding structured terminal frame

	// closeMu serializes Close/CloseWithError against ReadEvent so pool
	// lifecycle decisions are race-free and error closes take priority.
	closeMu sync.Mutex
	closed  bool // Close or CloseWithError already called
	errored bool // CloseWithError already called
	// readers tracks in-flight ReadEvent calls so Close never returns a
	// connection to the pool while a read is still running on the socket.
	readers sync.WaitGroup
}

func newWSUpstreamReader(pc *pooledConn, channelID, keyID int) *wsUpstreamReader {
	reader := &wsUpstreamReader{
		conn:      pc.conn,
		pc:        pc,
		channelID: channelID,
		keyID:     keyID,
	}
	reader.statusCode.Store(http.StatusOK)
	return reader
}

func wsEventString(value any) string {
	if typed, ok := value.(string); ok {
		return limitWSEventText(typed, wsEventFieldMaxBytes)
	}
	return ""
}

const wsEventFieldMaxBytes = 512

func limitWSEventText(value string, max int) string {
	if len(value) <= max {
		return value
	}
	return strings.ToValidUTF8(value[:max], "")
}

func wsEventStatusCode(value any) int {
	var raw string
	switch typed := value.(type) {
	case float64:
		if math.IsNaN(typed) || math.IsInf(typed, 0) || typed != math.Trunc(typed) {
			return 0
		}
		raw = strconv.FormatFloat(typed, 'f', -1, 64)
	case string:
		raw = strings.TrimSpace(typed)
	case json.Number:
		raw = typed.String()
	default:
		return 0
	}
	integer, ok := wsIntegralNumber(raw)
	if !ok || !integer.IsInt64() {
		return 0
	}
	status := integer.Int64()
	if status < 100 || status > 599 {
		return 0
	}
	return int(status)
}

func wsIntegralNumber(raw string) (*big.Int, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, false
	}
	rational, ok := new(big.Rat).SetString(raw)
	if !ok || !rational.IsInt() {
		return nil, false
	}
	return new(big.Int).Set(rational.Num()), true
}

func wsEventErrorFields(value any) (code, message, errorType string, present bool) {
	if value == nil {
		return "", "", "", false
	}
	if fields, ok := value.(map[string]any); ok {
		return normalizeWSUpstreamErrorCode(fields["code"]), wsEventString(fields["message"]), wsEventString(fields["type"]), true
	}
	return "", wsEventString(value), "", true
}

func (r *wsUpstreamReader) ReadEvent(ctx context.Context) ([]byte, error) {
	r.closeMu.Lock()
	if r.pendingErr != nil {
		err := r.pendingErr
		r.pendingErr = nil
		r.closeMu.Unlock()
		return nil, err
	}
	if r.closed || r.done {
		r.closeMu.Unlock()
		return nil, io.EOF
	}
	r.readers.Add(1)
	r.closeMu.Unlock()
	defer r.readers.Done()

	msgType, data, err := r.conn.Read(ctx)
	if err != nil {
		// Check if it's a normal close
		closeStatus := websocket.CloseStatus(err)
		if closeStatus == websocket.StatusNormalClosure || closeStatus == websocket.StatusGoingAway {
			return nil, io.EOF
		}
		switch closeStatus {
		case websocket.StatusPolicyViolation:
			r.statusCode.Store(http.StatusConflict)
		case websocket.StatusTryAgainLater:
			r.statusCode.Store(http.StatusServiceUnavailable)
		default:
			if r.statusCode.Load() < 400 {
				r.statusCode.Store(http.StatusBadGateway)
			}
		}
		return nil, newWSTransportError("ws read error", err)
	}

	if msgType != websocket.MessageText {
		return nil, fmt.Errorf("unexpected ws message type: %d", msgType)
	}
	if len(data) > maxSSEEventSize {
		r.statusCode.Store(http.StatusBadGateway)
		return nil, fmt.Errorf("ws event exceeds limit %d bytes", maxSSEEventSize)
	}

	// Check for error and terminal events.
	var event map[string]any
	if json.Unmarshal(data, &event) == nil {
		eventType := wsEventString(event["type"])
		eventStatus := wsEventStatusCode(event["status"])
		response, _ := event["response"].(map[string]any)
		responseStatus := wsEventString(response["status"])
		topErrorPresent := event["error"] != nil
		responseErrorPresent := response["error"] != nil

		terminal := isWSStreamTerminalEvent(eventType)
		switch responseStatus {
		case "failed", "incomplete", "cancelled", "canceled":
			terminal = true
		}
		if isWSStreamErrorEvent(eventType) || topErrorPresent || responseErrorPresent {
			if eventStatus > 0 {
				r.statusCode.Store(int32(eventStatus))
			} else if r.statusCode.Load() < 400 {
				r.statusCode.Store(http.StatusBadGateway)
			}
			eventErr := &wsUpstreamEventError{
				Status:  int(r.statusCode.Load()),
				Code:    normalizeWSUpstreamErrorCode(event["code"]),
				Message: wsEventString(event["message"]),
			}
			if code, message, errorType, present := wsEventErrorFields(event["error"]); present {
				eventErr.Code = code
				eventErr.Message = message
				eventErr.Type = errorType
			}
			if code, message, errorType, present := wsEventErrorFields(response["error"]); present {
				eventErr.Code = code
				eventErr.Message = message
				eventErr.Type = errorType
			}
			if eventErr.Message == "" {
				eventErr.Message = "upstream ws error"
			}
			r.closeMu.Lock()
			r.pendingErr = eventErr
			r.done = true
			r.closeMu.Unlock()
			return data, nil
		}
		if terminal {
			r.closeMu.Lock()
			r.done = true
			r.closeMu.Unlock()
		}
	}
	return data, nil
}

func (r *wsUpstreamReader) PendingError() error {
	r.closeMu.Lock()
	defer r.closeMu.Unlock()
	err := r.pendingErr
	r.pendingErr = nil
	return err
}

func (r *wsUpstreamReader) StatusCode() int {
	return int(r.statusCode.Load())
}

func (r *wsUpstreamReader) Headers() http.Header {
	return http.Header{
		"Content-Type": []string{"text/event-stream"},
	}
}

func (r *wsUpstreamReader) Body() io.ReadCloser {
	return nil // WS doesn't have a body
}

// Close returns the connection to the pool for reuse. It is idempotent: a
// connection already closed via Close or CloseWithError is not put back again.
func (r *wsUpstreamReader) Close() error {
	r.closeMu.Lock()
	if r.closed {
		r.closeMu.Unlock()
		return nil
	}
	r.closed = true
	r.closeMu.Unlock()

	// The processor cancels its read context before closing the source, so any
	// in-flight ReadEvent returns promptly. Waiting here guarantees the pooled
	// connection is never read by the previous consumer's goroutine and the
	// next request at the same time.
	r.readers.Wait()

	// Serialize the final pool decision with CloseWithError. Otherwise an error
	// close can remove the connection while this waiter is still able to put it
	// back into the pool.
	r.closeMu.Lock()
	defer r.closeMu.Unlock()
	if r.errored {
		return nil
	}

	// Return connection to pool (don't close it)
	wsUpstreamPool.Put(r.pc)
	log.Debugf("upstream WS connection returned to pool (channel=%d, key=%d)", r.channelID, r.keyID)
	return nil
}

// CloseWithError closes the reader and removes the connection from pool.
// It is idempotent and takes priority over Close: even after a normal Close
// already returned the connection to the pool (e.g. the stream processor's
// cleanup), a later CloseWithError still retracts it by removing and closing
// the underlying connection so a failed stream is never reused.
func (r *wsUpstreamReader) CloseWithError() {
	r.closeMu.Lock()
	if r.errored {
		r.closeMu.Unlock()
		return
	}
	r.errored = true
	r.closed = true
	r.closeMu.Unlock()

	// Closing the socket force-unblocks any residual reader, so unlike Close
	// there is no need to wait for in-flight ReadEvent calls here.
	wsUpstreamPool.RemoveConn(r.pc)
}
