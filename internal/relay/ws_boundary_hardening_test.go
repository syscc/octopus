package relay

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/coder/websocket"
)

// wsCloseReasonSecret 模拟 provider 可控的 close frame reason：真实场景里它可能
// 携带上游 URL、凭据或响应体片段，绝不能进入 Error()、日志或 relay log。
const wsCloseReasonSecret = "sk-provider-secret-9f8e7d https://internal.upstream.example/v1/responses"

func wsCloseCause(code websocket.StatusCode, reason string) error {
	return fmt.Errorf("failed to get reader: received close frame: %w",
		websocket.CloseError{Code: code, Reason: reason})
}

// TestWSTransportErrorHidesProviderReason 验证包装后的上游 WS 传输错误：
// 外层 Error() 固定且不含 provider 文本，同时 close status、errors.Is/As 链与
// 已知安全分类仍然可用。
func TestWSTransportErrorHidesProviderReason(t *testing.T) {
	cases := []struct {
		name         string
		op           string
		cause        error
		wantErrorMsg string
		wantStatus   int
		wantCode     string
		wantReset    bool
	}{
		{
			name: "continuation restart stays classifiable",
			op:   "ws read error",
			cause: wsCloseCause(websocket.StatusPolicyViolation,
				"upstream continuation connection is unavailable; please restart the conversation "+wsCloseReasonSecret),
			wantErrorMsg: "ws read error: close status 1008",
			wantStatus:   http.StatusConflict,
			wantCode:     "conversation_restart_required",
			wantReset:    true,
		},
		{
			name: "no available account stays classifiable",
			op:   "ws passthrough read error",
			cause: wsCloseCause(websocket.StatusTryAgainLater,
				"no available account "+wsCloseReasonSecret),
			wantErrorMsg: "ws passthrough read error: close status 1013",
			wantStatus:   http.StatusServiceUnavailable,
			wantCode:     "no_available_account",
		},
		{
			name: "rate limit stays classifiable",
			op:   "ws read error",
			cause: wsCloseCause(websocket.StatusPolicyViolation,
				"rate_limit_exceeded "+wsCloseReasonSecret),
			wantErrorMsg: "ws read error: close status 1008",
			wantStatus:   http.StatusTooManyRequests,
			wantCode:     "upstream_rate_limited",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := newWSTransportError(tc.op, tc.cause)

			if got := err.Error(); got != tc.wantErrorMsg {
				t.Fatalf("Error() = %q, want %q", got, tc.wantErrorMsg)
			}
			assertNoProviderReason(t, "Error()", err.Error())
			assertNoProviderReason(t, "relayErrorMessage()", relayErrorMessage(err))
			assertNoProviderReason(t, "upstreamClassificationMessage()", upstreamClassificationMessage(err))
			// 包装后的错误也会被再次 %w 包装进上层信息，同样必须保持干净。
			assertNoProviderReason(t, "wrapped Error()", fmt.Errorf("ws stream read error: %w", err).Error())

			if status := websocket.CloseStatus(err); status != websocket.CloseStatus(tc.cause) {
				t.Fatalf("CloseStatus() = %v, want %v", status, websocket.CloseStatus(tc.cause))
			}
			var closeErr websocket.CloseError
			if !errors.As(err, &closeErr) {
				t.Fatal("errors.As() lost the underlying websocket.CloseError")
			}

			publicErr, ok := classifyWSPublicError(err, 0)
			if !ok {
				t.Fatal("classifyWSPublicError() did not classify a sanitized transport error")
			}
			if publicErr.Status != tc.wantStatus || publicErr.Code != tc.wantCode {
				t.Fatalf("classifyWSPublicError() = {status:%d code:%s}, want {status:%d code:%s}",
					publicErr.Status, publicErr.Code, tc.wantStatus, tc.wantCode)
			}
			if publicErr.ResetConversation != tc.wantReset {
				t.Fatalf("classifyWSPublicError().ResetConversation = %t, want %t", publicErr.ResetConversation, tc.wantReset)
			}
			if got := wsUpstreamErrorStatus(err); got != tc.wantStatus {
				t.Fatalf("wsUpstreamErrorStatus() = %d, want %d", got, tc.wantStatus)
			}
		})
	}
}

// TestWSTransportErrorContinuationAndBrokenConnection 验证连接断裂识别在错误
// 文本被清洗后仍然基于类型工作，并且 continuation 重连判定不受影响。
func TestWSTransportErrorContinuationAndBrokenConnection(t *testing.T) {
	brokenCause := fmt.Errorf("failed to get reader: use of closed network connection %s", wsCloseReasonSecret)
	broken := newWSTransportError("ws read error", brokenCause)

	if got := broken.Error(); got != "ws read error: connection broken" {
		t.Fatalf("Error() = %q, want %q", got, "ws read error: connection broken")
	}
	assertNoProviderReason(t, "Error()", broken.Error())
	if !isUpstreamWSConnectionBroken(broken) {
		t.Fatal("isUpstreamWSConnectionBroken() = false for a sanitized broken-connection error")
	}
	if !isContinuationTransportFailure(broken) {
		t.Fatal("isContinuationTransportFailure() = false for a sanitized broken-connection error")
	}
	if !shouldReconnectUpstreamWSBeforeReplay(broken) {
		t.Fatal("shouldReconnectUpstreamWSBeforeReplay() = false for a sanitized broken-connection error")
	}

	eofCause := fmt.Errorf("failed to read frame header: %w (%s)", io.ErrUnexpectedEOF, wsCloseReasonSecret)
	eof := newWSTransportError("ws passthrough read error", eofCause)
	if !errors.Is(eof, io.ErrUnexpectedEOF) {
		t.Fatal("errors.Is(io.ErrUnexpectedEOF) = false, want the cause chain preserved")
	}
	assertNoProviderReason(t, "Error()", eof.Error())
	if !isUpstreamWSConnectionBroken(eof) {
		t.Fatal("isUpstreamWSConnectionBroken() = false for a sanitized EOF read error")
	}

	restart := newWSTransportError("ws read error", wsCloseCause(websocket.StatusPolicyViolation,
		"please restart the conversation "+wsCloseReasonSecret))
	if !isContinuationTransportFailure(restart) {
		t.Fatal("isContinuationTransportFailure() = false for a sanitized restart-required error")
	}

	if newWSTransportError("ws read error", nil) != nil {
		t.Fatal("newWSTransportError(nil) returned a non-nil error")
	}

	// 未知 reason 不产生任何分类标记，也不泄漏 provider 文本。
	unknown := newWSTransportError("ws read error", wsCloseCause(websocket.StatusInternalError, wsCloseReasonSecret))
	assertNoProviderReason(t, "Error()", unknown.Error())
	assertNoProviderReason(t, "upstreamClassificationMessage()", upstreamClassificationMessage(unknown))
	if got := wsUpstreamErrorStatus(unknown); got != http.StatusBadGateway {
		t.Fatalf("wsUpstreamErrorStatus() = %d, want %d", got, http.StatusBadGateway)
	}
}

func assertNoProviderReason(t *testing.T, label, text string) {
	t.Helper()
	lowered := strings.ToLower(text)
	for _, leak := range []string{"sk-provider-secret-9f8e7d", "internal.upstream.example", "received close frame", "reason ="} {
		if strings.Contains(lowered, strings.ToLower(leak)) {
			t.Fatalf("%s leaked provider-controlled text %q: %s", label, leak, text)
		}
	}
}
