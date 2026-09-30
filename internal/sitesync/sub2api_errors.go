package sitesync

import (
	"errors"
	"fmt"

	"github.com/bestruirui/octopus/internal/apperror"
)

// wrapSub2APIRefreshFailure keeps both the original request failure and the
// refresh failure visible while preserving the original app-error contract.
func wrapSub2APIRefreshFailure(original error, refreshErr error) error {
	if refreshErr == nil {
		return original
	}
	originalMessage := sanitizeSiteStatusMessage(original)
	refreshMessage := sanitizeSiteStatusMessage(refreshErr)
	message := "sub2api refresh failed: " + refreshMessage
	if originalMessage != "" {
		message += fmt.Sprintf("; request failed: %s", originalMessage)
	}
	message = sanitizeSiteStatusText(message)

	code := apperror.Code(original)
	if code == "" {
		code = apperror.Code(refreshErr)
	}
	if code == "" {
		code = apperror.CodeCommonInternalError
	}
	wrapped := apperror.Wrap(code, message, errors.Join(original, refreshErr))
	status := apperror.Status(original)
	if status == 0 {
		status = apperror.Status(refreshErr)
	}
	if status != 0 {
		wrapped.WithStatus(status)
	}
	originalParams := apperror.Params(original)
	if originalParams == nil {
		originalParams = apperror.Params(refreshErr)
	}
	params := make(map[string]any, len(originalParams)+1)
	for key, value := range originalParams {
		params[key] = value
	}
	params["sub2apiRefreshFailure"] = message
	wrapped.WithParams(params)
	return wrapped
}
