package relay

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/bestruirui/octopus/internal/model"
	"github.com/bestruirui/octopus/internal/op"
	"github.com/bestruirui/octopus/internal/outlierwindow"
	"github.com/bestruirui/octopus/internal/relay/balancer"
	"github.com/bestruirui/octopus/internal/transformer/outbound"
	"github.com/gin-gonic/gin"
)

// 客户端取消不应被记为上游失败：不进熔断，也不进 outlier。
func TestHandleResponsesCompactCancellationNotUpstreamFailure(t *testing.T) {
	gin.SetMode(gin.TestMode)
	ctx := setupRelayTestDB(t)
	if err := op.SettingSetInt(model.SettingKeyCircuitBreakerThreshold, 1); err != nil {
		t.Fatalf("set circuit threshold: %v", err)
	}

	upstreamStop := make(chan struct{})
	upstreamHit := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// 读完请求体：触发服务端后台读，客户端断开后 r.Context() 才会取消
		_, _ = io.Copy(io.Discard, r.Body)
		close(upstreamHit)
		select {
		case <-r.Context().Done():
		case <-upstreamStop:
		}
	}))
	t.Cleanup(func() {
		close(upstreamStop)
		upstream.Close()
	})

	channel := &model.Channel{
		Name:     "compact-cancel-channel",
		Type:     outbound.OutboundTypeOpenAIResponse,
		Enabled:  true,
		BaseUrls: []model.BaseUrl{{URL: upstream.URL + "/v1"}},
		Model:    "compact-model",
		Keys:     []model.ChannelKey{{Enabled: true, ChannelKey: "cancel-key"}},
	}
	if err := op.ChannelCreate(channel, ctx); err != nil {
		t.Fatalf("ChannelCreate failed: %v", err)
	}
	group := &model.Group{Name: "compact-cancel-group", Mode: model.GroupModeFailover}
	if err := op.GroupCreate(group, ctx); err != nil {
		t.Fatalf("GroupCreate failed: %v", err)
	}
	if err := op.GroupItemAdd(&model.GroupItem{GroupID: group.ID, ChannelID: channel.ID, ModelName: "compact-model", Priority: 1, Weight: 1}, ctx); err != nil {
		t.Fatalf("GroupItemAdd failed: %v", err)
	}
	saved, err := op.ChannelGet(channel.ID, ctx)
	if err != nil || len(saved.Keys) == 0 {
		t.Fatalf("ChannelGet failed: %v", err)
	}

	reqCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Set("api_key_id", 43)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses/compact",
		strings.NewReader(`{"model":"compact-cancel-group","input":"hello"}`)).WithContext(reqCtx)
	c.Request.Header.Set("Content-Type", "application/json")

	handlerDone := make(chan struct{})
	go func() {
		defer close(handlerDone)
		HandleResponsesCompact(c)
	}()

	select {
	case <-upstreamHit:
	case <-time.After(5 * time.Second):
		t.Fatal("compact request never reached upstream")
	}
	cancel()
	select {
	case <-handlerDone:
	case <-time.After(5 * time.Second):
		t.Fatal("compact handler did not return after cancellation")
	}

	if tripped, _ := balancer.IsTripped(channel.ID, saved.Keys[0].ID, "compact-cancel-group"); tripped {
		t.Fatal("client cancellation must not trip upstream circuit health")
	}
	if stats := outlierwindow.Evaluate(channel.ID, time.Now()); stats.Samples != 0 {
		t.Fatalf("client cancellation must not enter outlier health: %+v", stats)
	}
}
