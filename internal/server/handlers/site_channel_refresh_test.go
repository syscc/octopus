package handlers

import (
	"errors"
	"net/http"
	"testing"

	"github.com/bestruirui/octopus/internal/apperror"
	"github.com/bestruirui/octopus/internal/op"
)

func TestCreateSiteChannelKeyErrorPreservesRefreshParams(t *testing.T) {
	original := apperror.New("site.upstream", "refresh failed").
		WithStatus(http.StatusBadGateway).
		WithParam("sub2apiRefreshFailure", "refresh token expired")
	wrapped := wrapSiteChannelKeyError(original, http.StatusBadGateway)

	if wrapped.Code != op.CodeSiteChannelKeyCreateFailed {
		t.Fatalf("wrapped code = %q, want %q", wrapped.Code, op.CodeSiteChannelKeyCreateFailed)
	}
	if wrapped.Status != http.StatusBadGateway {
		t.Fatalf("wrapped status = %d, want %d", wrapped.Status, http.StatusBadGateway)
	}
	if got := apperror.Params(wrapped)["sub2apiRefreshFailure"]; got != "refresh token expired" {
		t.Fatalf("refresh failure param = %#v, want preserved reason", got)
	}
	if errors.Is(wrapped, original) == false {
		t.Fatal("wrapped error lost original cause")
	}
}

func TestCreateSiteChannelKeyErrorWithoutParamsKeepsParamsEmpty(t *testing.T) {
	wrapped := wrapSiteChannelKeyError(errors.New("ordinary failure"), http.StatusInternalServerError)

	if wrapped.Code != op.CodeSiteChannelKeyCreateFailed || wrapped.Status != http.StatusInternalServerError {
		t.Fatalf("generic wrapper changed contract: code=%q status=%d", wrapped.Code, wrapped.Status)
	}
	if len(apperror.Params(wrapped)) != 0 {
		t.Fatalf("ordinary error unexpectedly gained params: %#v", apperror.Params(wrapped))
	}
}
