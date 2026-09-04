package handlers

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/bestruirui/octopus/internal/utils/httpbody"
	"github.com/gin-gonic/gin"
)

func oversizedDeclaredRequest(method, target string, maxBytes int64) *http.Request {
	request := httptest.NewRequest(method, target, strings.NewReader(`{"model":"unused"}`))
	request.ContentLength = maxBytes + 1
	return request
}

func TestReadImportPayloadRejectsDeclaredOversizeBody(t *testing.T) {
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	context, _ := gin.CreateTestContext(recorder)
	request := oversizedDeclaredRequest(http.MethodPost, "/api/v1/site/import/metapi", httpbody.MaxSiteImportBodyBytes)
	context.Request = request

	_, err := readImportPayload(context)
	if !errors.Is(err, httpbody.ErrRequestBodyTooLarge) {
		t.Fatalf("readImportPayload() error = %v, want ErrRequestBodyTooLarge", err)
	}
}

func TestImportDBRejectsDeclaredOversizeBody(t *testing.T) {
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	context, _ := gin.CreateTestContext(recorder)
	context.Request = oversizedDeclaredRequest(http.MethodPost, "/api/v1/setting/import", httpbody.MaxManagementImportBodyBytes)

	importDB(context)
	if recorder.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want %d; body=%s", recorder.Code, http.StatusRequestEntityTooLarge, recorder.Body.String())
	}
}
