package relay

import (
	"context"
	"errors"
)

var (
	errLocalRelayBudgetExceeded = errors.New("local relay budget exceeded")
	errFirstTokenTimeout        = errors.New("first token timeout")
)

func contextError(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	if cause := context.Cause(ctx); cause != nil {
		return cause
	}
	return ctx.Err()
}

func isLocalRelayBudgetExceeded(ctx context.Context, err error) bool {
	if errors.Is(err, errLocalRelayBudgetExceeded) {
		return true
	}
	if ctx == nil {
		return false
	}
	return errors.Is(context.Cause(ctx), errLocalRelayBudgetExceeded)
}

func isFirstTokenTimeout(ctx context.Context, err error) bool {
	if errors.Is(err, errFirstTokenTimeout) {
		return true
	}
	if ctx == nil {
		return false
	}
	return errors.Is(context.Cause(ctx), errFirstTokenTimeout)
}

func hasDefinitiveUpstreamFailure(err error) bool {
	if err == nil {
		return false
	}
	var httpErr *upstreamHTTPError
	if errors.As(err, &httpErr) && httpErr != nil {
		return true
	}
	return hasIndependentWSTransportFailure(err) ||
		hasIndependentUpstreamFailure(attemptResult{UpstreamErr: err})
}

func isClientCancellation(ctx context.Context, err error) bool {
	if isLocalRelayBudgetExceeded(ctx, err) || isLocalRelayBudgetExceeded(ctx, contextError(ctx)) ||
		isFirstTokenTimeout(ctx, err) || isFirstTokenTimeout(ctx, contextError(ctx)) {
		return false
	}
	// A provider response or typed protocol terminal is independent evidence of
	// upstream failure. It remains health-accountable when the client context is
	// cancelled concurrently after that evidence has already been obtained.
	if hasDefinitiveUpstreamFailure(err) {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	if ctx == nil {
		return false
	}
	return errors.Is(ctx.Err(), context.Canceled) || errors.Is(ctx.Err(), context.DeadlineExceeded)
}
