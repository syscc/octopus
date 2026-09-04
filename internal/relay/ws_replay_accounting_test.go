package relay

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bestruirui/octopus/internal/model"
	"github.com/bestruirui/octopus/internal/op"
	"github.com/bestruirui/octopus/internal/transformer/outbound"
	"github.com/coder/websocket"
)

func TestWSResponseCreateReplayPreservesInitialAttemptMetrics(t *testing.T) {
	ctx := setupRelayTestDB(t)
	resetWSUpstreamPool()
	t.Cleanup(resetWSUpstreamPool)
	if err := op.SettingSetString(model.SettingKeyResponsesWSEnabled, "true"); err != nil {
		t.Fatalf("enable upstream Responses WS: %v", err)
	}
	if err := op.SettingSetString(model.SettingKeyResponsesWSDefaultMode, "passthrough"); err != nil {
		t.Fatalf("set upstream Responses WS mode: %v", err)
	}

	var wsRequests atomic.Int32
	var httpRequests atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
			conn, err := websocket.Accept(w, r, nil)
			if err != nil {
				return
			}
			defer conn.CloseNow()
			readCtx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
			defer cancel()
			if _, _, err := conn.Read(readCtx); err != nil {
				return
			}
			wsRequests.Add(1)
			_ = conn.Write(readCtx, websocket.MessageText, []byte(`{"type":"error","status":409,"error":{"type":"invalid_request_error","message":"please restart the conversation"}}`))
			return
		}

		httpRequests.Add(1)
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = fmt.Fprint(w, strings.Join([]string{
			`data: {"type":"response.created","response":{"id":"resp_recovered","object":"response","model":"upstream-model","status":"in_progress"}}`,
			"",
			`data: {"type":"response.output_text.delta","delta":"recovered"}`,
			"",
			`data: {"type":"response.completed","response":{"id":"resp_recovered","object":"response","model":"upstream-model","status":"completed","usage":{"input_tokens":3,"output_tokens":1,"total_tokens":4}}}`,
			"",
		}, "\n"))
	}))
	t.Cleanup(upstream.Close)

	channel := &model.Channel{
		Name:     "ws-replay-accounting-channel",
		Type:     outbound.OutboundTypeOpenAIResponse,
		Enabled:  true,
		BaseUrls: []model.BaseUrl{{URL: upstream.URL + "/v1"}},
		Model:    "upstream-model",
		Keys:     []model.ChannelKey{{Enabled: true, ChannelKey: "ws-key"}},
		WSMode:   model.ChannelWSModePassthrough,
	}
	if err := op.ChannelCreate(channel, ctx); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	group := &model.Group{
		Name:            "ws-replay-accounting-group",
		Mode:            model.GroupModeFailover,
		SessionKeepTime: 60,
	}
	if err := op.GroupCreate(group, ctx); err != nil {
		t.Fatalf("create group: %v", err)
	}
	if err := op.GroupItemAdd(&model.GroupItem{GroupID: group.ID, ChannelID: channel.ID, ModelName: "upstream-model"}, ctx); err != nil {
		t.Fatalf("add group item: %v", err)
	}

	clientConn, serverConn := newTestWSConnPair(t)
	t.Cleanup(func() {
		_ = clientConn.Close(websocket.StatusNormalClosure, "")
		_ = serverConn.Close(websocket.StatusNormalClosure, "")
	})

	state := &wsConversationState{
		DownstreamSessionID: "ws-replay-accounting-session",
		RequestModel:        group.Name,
		ChannelID:           channel.ID,
		ChannelKeyID:        channel.Keys[0].ID,
		LastResponseID:      "resp_previous",
		ReplayWindowItems:   json.RawMessage(`[{"type":"message","role":"user","content":[{"type":"input_text","text":"previous"}]}]`),
		LastOutboundType:    outbound.OutboundTypeOpenAIResponse,
		LastOutboundTypeSet: true,
	}
	beforeTotal := op.StatsTotalGet()
	beforeChannel := op.StatsChannelGet(channel.ID)
	beforeLogs, err := op.RelayLogList(ctx, nil, nil, nil, 1, 100)
	if err != nil {
		t.Fatalf("list baseline relay logs: %v", err)
	}
	knownLogIDs := make(map[int64]struct{}, len(beforeLogs))
	for _, relayLog := range beforeLogs {
		knownLogIDs[relayLog.ID] = struct{}{}
	}

	resultingState := processWSResponseCreate(
		ctx,
		serverConn,
		[]byte(`{"type":"response.create","model":"ws-replay-accounting-group","previous_response_id":"resp_previous","input":"next"}`),
		1,
		"",
		state.DownstreamSessionID,
		state,
	)
	if resultingState == nil || resultingState.LastResponseID != "resp_recovered" {
		t.Fatalf("expected replay to recover the conversation, got %#v", resultingState)
	}
	if wsRequests.Load() != 1 || httpRequests.Load() != 1 {
		t.Fatalf("expected one failed WS continuation and one HTTP replay, got ws=%d http=%d", wsRequests.Load(), httpRequests.Load())
	}

	readCtx, cancelRead := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelRead()
	var eventTypes []string
	for {
		_, data, err := clientConn.Read(readCtx)
		if err != nil {
			t.Fatalf("read replayed downstream event: %v (events=%v)", err, eventTypes)
		}
		var event struct {
			Type string `json:"type"`
		}
		if err := json.Unmarshal(data, &event); err != nil {
			t.Fatalf("decode replayed downstream event %q: %v", data, err)
		}
		eventTypes = append(eventTypes, event.Type)
		if event.Type == "response.completed" {
			break
		}
	}
	if len(eventTypes) == 0 || eventTypes[len(eventTypes)-1] != "response.completed" {
		t.Fatalf("replay did not deliver a completed response: %v", eventTypes)
	}

	logs, err := op.RelayLogList(ctx, nil, nil, nil, 1, 100)
	if err != nil {
		t.Fatalf("list relay logs: %v", err)
	}
	var matching []model.RelayLog
	for _, relayLog := range logs {
		if relayLog.RequestModelName != group.Name {
			continue
		}
		if _, existed := knownLogIDs[relayLog.ID]; existed {
			continue
		}
		matching = append(matching, relayLog)
	}
	if len(matching) != 1 {
		t.Errorf("expected one new request-level relay log after replay, got %d logs: %#v", len(matching), matching)
	} else {
		relayLog := matching[0]
		if !relayLog.Success || relayLog.TotalAttempts != 2 || len(relayLog.Attempts) != 2 {
			t.Errorf("replay log lost the first attempt: success=%t total=%d attempts=%#v", relayLog.Success, relayLog.TotalAttempts, relayLog.Attempts)
		}
		if len(relayLog.Attempts) == 2 {
			if relayLog.Attempts[0].Status != model.AttemptFailed || relayLog.Attempts[1].Status != model.AttemptSuccess {
				t.Errorf("unexpected replay attempt sequence: %#v", relayLog.Attempts)
			}
			if relayLog.Attempts[0].AttemptNum != 1 || relayLog.Attempts[1].AttemptNum != 2 {
				t.Errorf("unexpected replay attempt numbering: %#v", relayLog.Attempts)
			}
			for _, attempt := range relayLog.Attempts {
				if attempt.ChannelID != channel.ID || attempt.ChannelKeyID != channel.Keys[0].ID {
					t.Errorf("replay attempt lost channel/key identity: %#v", attempt)
				}
			}
		}
		if relayLog.InputTokens != 3 || relayLog.OutputTokens != 1 {
			t.Errorf("replay log usage was lost or duplicated: input=%d output=%d", relayLog.InputTokens, relayLog.OutputTokens)
		}
	}

	totalStats := op.StatsTotalGet()
	if totalStats.RequestSuccess-beforeTotal.RequestSuccess != 1 || totalStats.RequestFailed-beforeTotal.RequestFailed != 0 ||
		totalStats.InputToken-beforeTotal.InputToken != 3 || totalStats.OutputToken-beforeTotal.OutputToken != 1 {
		t.Errorf("request-level stats were lost, duplicated, or counted as failed: before=%+v after=%+v", beforeTotal, totalStats)
	}
	channelStats := op.StatsChannelGet(channel.ID)
	if channelStats.RequestSuccess-beforeChannel.RequestSuccess != 1 || channelStats.RequestFailed-beforeChannel.RequestFailed != 1 ||
		channelStats.InputToken-beforeChannel.InputToken != 3 || channelStats.OutputToken-beforeChannel.OutputToken != 1 {
		t.Errorf("attempt-level channel stats were not preserved exactly once: before=%+v after=%+v", beforeChannel, channelStats)
	}
}
