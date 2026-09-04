package handlers

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	dbpkg "github.com/bestruirui/octopus/internal/db"
	"github.com/bestruirui/octopus/internal/model"
	"github.com/bestruirui/octopus/internal/op"
	"github.com/bestruirui/octopus/internal/server/middleware"
	"github.com/bestruirui/octopus/internal/transformer/outbound"
	"github.com/gin-gonic/gin"
)

func setupChannelProbeHandlerTestDB(t *testing.T) context.Context {
	t.Helper()
	if dbpkg.GetDB() != nil {
		_ = dbpkg.Close()
	}
	dbPath := filepath.Join(t.TempDir(), "channel-probe-handler.db")
	if err := dbpkg.InitDB("sqlite", dbPath, false); err != nil {
		t.Fatalf("InitDB failed: %v", err)
	}
	if err := op.InitCache(); err != nil {
		t.Fatalf("InitCache failed: %v", err)
	}
	t.Cleanup(func() { _ = dbpkg.Close() })
	return context.Background()
}

func TestProbeOpenAIProtocolHandlerReturnsSanitizedEnvelope(t *testing.T) {
	ctx := setupChannelProbeHandlerTestDB(t)
	const upstreamBody = `{"error":{"message":"private upstream detail","type":"authentication_error"}}`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = io.WriteString(w, upstreamBody)
	}))
	defer server.Close()

	channel := &model.Channel{
		Name:               "handler-probe-fixture",
		Type:               outbound.OutboundTypeOpenAIChat,
		Enabled:            true,
		BaseUrls:           []model.BaseUrl{{URL: server.URL}},
		Keys:               []model.ChannelKey{{Enabled: true, ChannelKey: "fixture-value"}},
		Model:              "configured-model",
		ProxyMode:          model.ProxyUsageModeDirect,
		OpenAIProtocolMode: model.OpenAIProtocolModeAuto,
	}
	if err := op.ChannelCreate(channel, ctx); err != nil {
		t.Fatalf("ChannelCreate failed: %v", err)
	}

	gin.SetMode(gin.TestMode)
	request := httptest.NewRequest(http.MethodPost, "/api/v1/channel/probe-openai-protocol", strings.NewReader(`{"id":1}`))
	request.Header.Set("Content-Type", "application/json")
	writer := httptest.NewRecorder()
	ginContext, _ := gin.CreateTestContext(writer)
	ginContext.Request = request
	probeOpenAIProtocol(ginContext)

	if writer.Code != http.StatusOK {
		t.Fatalf("handler status = %d, want 200", writer.Code)
	}
	var envelope struct {
		Code int             `json:"code"`
		Data json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(writer.Body.Bytes(), &envelope); err != nil {
		t.Fatalf("decode response envelope failed: %v", err)
	}
	if envelope.Code != http.StatusOK || len(envelope.Data) == 0 {
		t.Fatalf("unexpected response envelope code/data")
	}
	responseText := writer.Body.String()
	for _, forbidden := range []string{server.URL, upstreamBody, "private upstream detail", "fixture-value", "base_url"} {
		if strings.Contains(responseText, forbidden) {
			t.Fatalf("handler response contains forbidden probe detail")
		}
	}
	var data struct {
		ChannelID int `json:"channel_id"`
		Endpoints []struct {
			Endpoint string `json:"endpoint"`
			Outcome  string `json:"outcome"`
			Status   *int   `json:"status"`
		} `json:"endpoints"`
	}
	if err := json.Unmarshal(envelope.Data, &data); err != nil {
		t.Fatalf("decode probe data failed: %v", err)
	}
	if data.ChannelID != channel.ID || len(data.Endpoints) != 2 {
		t.Fatalf("unexpected probe data shape")
	}
	for _, endpoint := range data.Endpoints {
		if endpoint.Outcome != "failed" || endpoint.Status == nil || *endpoint.Status != http.StatusUnauthorized {
			t.Fatalf("unexpected endpoint summary for %s", endpoint.Endpoint)
		}
	}

	// Keep the response assertions above focused on the public contract.
}

func TestProbeOpenAIProtocolHandlerRejectsInvalidInput(t *testing.T) {
	gin.SetMode(gin.TestMode)
	cases := []struct {
		name       string
		body       string
		wantStatus int
	}{
		{name: "malformed json", body: `{"id":`, wantStatus: http.StatusBadRequest},
		{name: "non-positive id", body: `{"id":0}`, wantStatus: http.StatusBadRequest},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPost, "/api/v1/channel/probe-openai-protocol", strings.NewReader(tc.body))
			request.Header.Set("Content-Type", "application/json")
			writer := httptest.NewRecorder()
			ginContext, _ := gin.CreateTestContext(writer)
			ginContext.Request = request
			probeOpenAIProtocol(ginContext)
			if writer.Code != tc.wantStatus {
				t.Fatalf("handler status = %d, want %d", writer.Code, tc.wantStatus)
			}
		})
	}

	_ = setupChannelProbeHandlerTestDB(t)
	request := httptest.NewRequest(http.MethodPost, "/api/v1/channel/probe-openai-protocol", strings.NewReader(`{"id":999999}`))
	request.Header.Set("Content-Type", "application/json")
	writer := httptest.NewRecorder()
	ginContext, _ := gin.CreateTestContext(writer)
	ginContext.Request = request
	probeOpenAIProtocol(ginContext)
	if writer.Code != http.StatusNotFound {
		t.Fatalf("unknown channel status = %d, want %d", writer.Code, http.StatusNotFound)
	}
	var envelope struct {
		ErrorCode string `json:"error_code"`
		Message   string `json:"message"`
	}
	if err := json.Unmarshal(writer.Body.Bytes(), &envelope); err != nil {
		t.Fatalf("decode unknown-channel response failed: %v", err)
	}
	if envelope.ErrorCode != codeChannelNotFound || envelope.Message != "channel not found" {
		t.Fatalf("unexpected unknown-channel response: code=%q message=%q", envelope.ErrorCode, envelope.Message)
	}
}

func TestProbeOpenAIProtocolRouteRequiresAuthAndJSON(t *testing.T) {
	gin.SetMode(gin.TestMode)
	engine := gin.New()
	engine.POST("/api/v1/channel/probe-openai-protocol", middleware.Auth(), middleware.RequireJSON(), probeOpenAIProtocol)

	unauthenticated := httptest.NewRequest(http.MethodPost, "/api/v1/channel/probe-openai-protocol", strings.NewReader(`{"id":1}`))
	unauthenticated.Header.Set("Content-Type", "application/json")
	unauthenticatedWriter := httptest.NewRecorder()
	engine.ServeHTTP(unauthenticatedWriter, unauthenticated)
	if unauthenticatedWriter.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated route status = %d, want %d", unauthenticatedWriter.Code, http.StatusUnauthorized)
	}

	jsonEngine := gin.New()
	jsonEngine.POST("/probe", middleware.RequireJSON(), func(c *gin.Context) { c.Status(http.StatusNoContent) })
	nonJSON := httptest.NewRequest(http.MethodPost, "/probe", strings.NewReader("{}"))
	nonJSON.Header.Set("Content-Type", "text/plain")
	nonJSONWriter := httptest.NewRecorder()
	jsonEngine.ServeHTTP(nonJSONWriter, nonJSON)
	if nonJSONWriter.Code != http.StatusUnsupportedMediaType {
		t.Fatalf("non-JSON route status = %d, want %d", nonJSONWriter.Code, http.StatusUnsupportedMediaType)
	}
}

func TestShouldTriggerOpenAIProtocolProbeOnlyForRelevantChanges(t *testing.T) {
	auto := model.OpenAIProtocolModeAuto
	manual := model.OpenAIProtocolModeChatOnly
	channel := &model.Channel{ID: 1, Type: outbound.OutboundTypeOpenAIChat, OpenAIProtocolMode: auto}
	cases := []struct {
		name string
		req  *model.ChannelUpdateRequest
		want bool
	}{
		{name: "create", req: nil, want: true},
		{name: "name", req: &model.ChannelUpdateRequest{Name: stringPtr("renamed")}, want: false},
		{name: "enabled", req: &model.ChannelUpdateRequest{Enabled: boolPtr(false)}, want: false},
		{name: "auto group", req: &model.ChannelUpdateRequest{AutoGroup: autoGroupPtr(model.AutoGroupTypeNone)}, want: false},
		{name: "model", req: &model.ChannelUpdateRequest{Model: stringPtr("new-model")}, want: true},
		{name: "base urls", req: &model.ChannelUpdateRequest{BaseUrls: &[]model.BaseUrl{{URL: "https://example.invalid"}}}, want: true},
		{name: "key remark only", req: &model.ChannelUpdateRequest{KeysToUpdate: []model.ChannelKeyUpdateRequest{{Remark: stringPtr("note")}}}, want: false},
		{name: "key enabled", req: &model.ChannelUpdateRequest{KeysToUpdate: []model.ChannelKeyUpdateRequest{{Enabled: boolPtr(false)}}}, want: true},
		{name: "mode to manual", req: &model.ChannelUpdateRequest{OpenAIProtocolMode: &manual}, want: false},
		{name: "mode to auto", req: &model.ChannelUpdateRequest{OpenAIProtocolMode: &auto}, want: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := shouldTriggerOpenAIProtocolProbe(channel, tc.req); got != tc.want {
				t.Fatalf("shouldTriggerOpenAIProtocolProbe() = %v, want %v", got, tc.want)
			}
		})
	}

	manualChannel := *channel
	manualChannel.OpenAIProtocolMode = manual
	if shouldTriggerOpenAIProtocolProbe(&manualChannel, &model.ChannelUpdateRequest{Model: stringPtr("ignored")}) {
		t.Fatal("manual channel update scheduled an automatic probe")
	}
	nonOpenAI := *channel
	nonOpenAI.Type = outbound.OutboundTypeAnthropic
	if shouldTriggerOpenAIProtocolProbe(&nonOpenAI, &model.ChannelUpdateRequest{Model: stringPtr("ignored")}) {
		t.Fatal("non-OpenAI channel update scheduled an OpenAI probe")
	}
}

func stringPtr(value string) *string                              { return &value }
func boolPtr(value bool) *bool                                    { return &value }
func autoGroupPtr(value model.AutoGroupType) *model.AutoGroupType { return &value }
