package relay

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/coder/websocket"
)

func TestWSUpstreamReaderReturnsStructuredFrameBeforeError(t *testing.T) {
	clientConn, serverConn := newTestWSConnPair(t)
	t.Cleanup(func() {
		clientConn.CloseNow()
		serverConn.CloseNow()
	})
	pc := &pooledConn{conn: clientConn}
	reader := newWSUpstreamReader(pc, 1, 2)
	frame := `{"type":"response.failed","response":{"id":"resp_failed","status":"failed","error":{"code":500,"message":"boom","type":"server_error"}}}`
	writeDone := make(chan error, 1)
	go func() {
		writeDone <- serverConn.Write(context.Background(), websocket.MessageText, []byte(frame))
	}()

	data, err := reader.ReadEvent(context.Background())
	if err != nil {
		t.Fatalf("structured terminal frame was hidden by its error: %v", err)
	}
	if !strings.Contains(string(data), `"type":"response.failed"`) || !strings.Contains(string(data), `"message":"boom"`) {
		t.Fatalf("unexpected structured frame: %s", data)
	}
	if err := <-writeDone; err != nil {
		t.Fatalf("write structured frame: %v", err)
	}

	data, err = reader.ReadEvent(context.Background())
	if len(data) != 0 {
		t.Fatalf("pending structured error returned duplicate data: %s", data)
	}
	var upstreamErr *wsUpstreamEventError
	if !errors.As(err, &upstreamErr) {
		t.Fatalf("expected preserved wsUpstreamEventError, got %v", err)
	}
	if upstreamErr.Code != "500" || upstreamErr.Message != "boom" || upstreamErr.Type != "server_error" {
		t.Fatalf("structured error metadata was lost: %+v", upstreamErr)
	}
}

func TestWSUpstreamReaderAcceptsNumericAndTopLevelErrorCodes(t *testing.T) {
	clientConn, serverConn := newTestWSConnPair(t)
	t.Cleanup(func() {
		clientConn.CloseNow()
		serverConn.CloseNow()
	})
	reader := newWSUpstreamReader(&pooledConn{conn: clientConn}, 1, 2)
	frames := []string{
		`{"type":"response.failed","response":{"status":"failed","error":{"code":500,"message":"numeric boom","type":"server_error"}}}`,
		`{"type":"error","code":"rate_limit_exceeded","message":"top-level boom","status":429}`,
		`{"type":"error","code":500,"message":{"detail":"object boom"},"status":"failed"}`,
	}
	for _, frame := range frames {
		writeDone := make(chan error, 1)
		go func() { writeDone <- serverConn.Write(context.Background(), websocket.MessageText, []byte(frame)) }()
		data, err := reader.ReadEvent(context.Background())
		if err != nil || len(data) == 0 {
			t.Fatalf("structured frame was not forwarded: data=%q err=%v", data, err)
		}
		if err := <-writeDone; err != nil {
			t.Fatalf("write structured frame: %v", err)
		}
		_, err = reader.ReadEvent(context.Background())
		var upstreamErr *wsUpstreamEventError
		if !errors.As(err, &upstreamErr) {
			t.Fatalf("expected preserved structured error, got %v", err)
		}
		if strings.Contains(frame, "numeric") && (upstreamErr.Code != "500" || upstreamErr.Message != "numeric boom") {
			t.Fatalf("numeric error metadata lost: %+v", upstreamErr)
		}
		if strings.Contains(frame, "top-level") && (upstreamErr.Code != "rate_limit_exceeded" || upstreamErr.Message != "top-level boom" || upstreamErr.Status != 429) {
			t.Fatalf("top-level error metadata lost: %+v", upstreamErr)
			if strings.Contains(frame, "object boom") && (upstreamErr.Code != "500" || !strings.Contains(upstreamErr.Message, "object boom")) {
				t.Fatalf("type-mismatched error metadata lost: %+v", upstreamErr)
			}
		}
		reader.closeMu.Lock()
		reader.done = false
		reader.closeMu.Unlock()
	}
}
