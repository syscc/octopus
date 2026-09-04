package relay

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/bestruirui/octopus/internal/utils/httpbody"
	"github.com/gin-gonic/gin"
)

func TestHandleResponsesCompactRejectsDeclaredOversizeBody(t *testing.T) {
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	context, _ := gin.CreateTestContext(recorder)
	request := httptest.NewRequest(http.MethodPost, "/v1/responses/compact", strings.NewReader(`{"model":"unused","input":"x"}`))
	request.ContentLength = httpbody.MaxLLMRequestBodyBytes + 1
	context.Request = request

	HandleResponsesCompact(context)

	if recorder.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want %d; body=%s", recorder.Code, http.StatusRequestEntityTooLarge, recorder.Body.String())
	}
}
