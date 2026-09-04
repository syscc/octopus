package relay

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"

	"github.com/bestruirui/octopus/internal/relay/stream"
	transformerModel "github.com/bestruirui/octopus/internal/transformer/model"
	"github.com/coder/websocket"
)

func TestIsClientCancellationMatchesWrappedRequestErrors(t *testing.T) {
	ctx := context.Background()

	if !isClientCancellation(ctx, fmt.Errorf("failed to send request: %w", context.Canceled)) {
		t.Fatalf("expected wrapped context.Canceled to be treated as client cancellation")
	}
	if !isClientCancellation(ctx, fmt.Errorf("failed to send request: %w", context.DeadlineExceeded)) {
		t.Fatalf("expected wrapped context.DeadlineExceeded to be treated as client cancellation")
	}
}

func TestIsClientCancellationFallsBackToContextState(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if !isClientCancellation(ctx, fmt.Errorf("upstream request aborted")) {
		t.Fatalf("expected canceled request context to be treated as client cancellation")
	}
}

func TestIsClientCancellationPreservesDefinitiveUpstreamFailures(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	tests := []struct {
		name string
		err  error
	}{
		{
			name: "http status",
			err:  errors.Join(context.Canceled, newUpstreamHTTPError(http.StatusInternalServerError, []byte(`{"error":{"type":"server_error"}}`))),
		},
		{
			name: "websocket event",
			err:  errors.Join(context.Canceled, &wsUpstreamEventError{Status: http.StatusTooManyRequests, Type: "rate_limit_error"}),
		},
		{
			name: "structured response error",
			err:  errors.Join(context.Canceled, &transformerModel.ResponseError{StatusCode: http.StatusBadGateway}),
		},
		{
			name: "failed terminal",
			err:  stream.NewPassthroughTerminalError(transformerModel.PassthroughTerminalOutcomeFailed, context.Canceled),
		},
		{
			name: "incomplete stream",
			err:  errors.Join(context.Canceled, transformerModel.ErrIncompleteUpstreamStream),
		},

		{
			name: "websocket transport close",
			err:  errors.Join(context.Canceled, newWSTransportError("ws read error", websocket.CloseError{Code: websocket.StatusInternalError})),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if isClientCancellation(ctx, tt.err) {
				t.Fatal("definitive upstream failure must take precedence over concurrent client cancellation")
			}
		})
	}
}

func TestIsClientCancellationIgnoresOrdinaryErrors(t *testing.T) {
	if isClientCancellation(context.Background(), fmt.Errorf("dial tcp timeout")) {
		t.Fatalf("expected ordinary upstream error to not be treated as client cancellation")
	}
}

func TestIsClientCancellationIgnoresLocalRelayBudgetTimeout(t *testing.T) {
	ctx, cancel := context.WithTimeoutCause(context.Background(), 0, errLocalRelayBudgetExceeded)
	defer cancel()

	<-ctx.Done()
	if isClientCancellation(ctx, contextError(ctx)) {
		t.Fatalf("expected local relay budget timeout to not be treated as client cancellation")
	}
}
