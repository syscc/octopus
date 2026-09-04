package relay

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/bestruirui/octopus/internal/transformer/inbound"
	"github.com/bestruirui/octopus/internal/utils/httpbody"
	"github.com/gin-gonic/gin"
)

func TestParseRequestRejectsDeclaredOversizeBody(t *testing.T) {
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	context, _ := gin.CreateTestContext(recorder)
	request := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"unused"}`))
	request.ContentLength = httpbody.MaxLLMRequestBodyBytes + 1
	context.Request = request

	_, _, _, err := parseRequest(inbound.InboundTypeOpenAIChat, context)
	if !errors.Is(err, httpbody.ErrRequestBodyTooLarge) {
		t.Fatalf("parseRequest() error = %v, want ErrRequestBodyTooLarge", err)
	}
	if recorder.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusRequestEntityTooLarge)
	}
}
