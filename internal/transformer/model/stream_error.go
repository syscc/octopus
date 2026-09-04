package model

import "errors"

// ErrIncompleteUpstreamStream marks an upstream stream that already projected
// partial response data to the client but ended (EOF) without any protocol
// terminal signal (finish reason / response.completed / message_stop / …).
//
// Relay semantics:
//   - The attempt must be treated as failed: HTTP/WS replay state must not be
//     persisted, because the response never completed.
//   - Because partial payload was already written, protocol fallback and
//     channel failover are forbidden — the client received a synthesized
//     terminal (e.g. response.incomplete) instead.
var ErrIncompleteUpstreamStream = errors.New("upstream stream ended without a terminal event after partial response output")
