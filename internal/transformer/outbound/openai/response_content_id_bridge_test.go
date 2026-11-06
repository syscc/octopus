package openai

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	inbound "github.com/bestruirui/octopus/internal/transformer/inbound/openai"
)

func TestResponsesRefusalItemIDWithoutTerminalOutput(t *testing.T) {
	ctx := context.Background()
	outbound := &ResponseOutbound{}
	responseInbound := &inbound.ResponseInbound{}
	var wire []byte
	for _, raw := range []string{
		`{"type":"response.refusal.delta","output_index":0,"item_id":"msg_refusal","delta":"cannot comply"}`,
		`{"type":"response.completed","response":{"id":"resp_refusal","model":"test","status":"completed","usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}}`,
	} {
		events, err := outbound.TransformStreamEvent(ctx, []byte(raw))
		if err != nil {
			t.Fatal(err)
		}
		body, err := responseInbound.TransformStreamEvents(ctx, events)
		if err != nil {
			t.Fatal(err)
		}
		wire = append(wire, body...)
	}
	found := false
	for _, frame := range strings.Split(string(wire), "\n\n") {
		if !strings.HasPrefix(frame, "data: ") {
			continue
		}
		var event struct {
			Type     string             `json:"type"`
			Response *ResponsesResponse `json:"response"`
		}
		if err := json.Unmarshal([]byte(strings.TrimPrefix(frame, "data: ")), &event); err != nil {
			t.Fatal(err)
		}
		if event.Type == "response.completed" {
			found = true
			if event.Response == nil || len(event.Response.Output) != 1 {
				t.Fatalf("missing refusal item: %s", wire)
			}
			item := event.Response.Output[0]
			if item.ID != "msg_refusal" || item.Content == nil || len(item.Content.Items) != 1 || item.Content.Items[0].Refusal == nil || *item.Content.Items[0].Refusal != "cannot comply" {
				t.Fatalf("terminal lost refusal ID or content: %s", wire)
			}
		}
	}
	if !found {
		t.Fatal("missing completed event")
	}
}
