package relay

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	dbmodel "github.com/bestruirui/octopus/internal/model"
	"github.com/coder/websocket"
)

// TestWSUpstreamDialReadLimit 验证 wsPool.Dial 之后的 conn.SetReadLimit(maxSSEEventSize)
// 真正在 socket 层生效：恰好等于上限的上游消息可以被完整读取，超过上限一个字节的消息
// 在读取阶段就返回 websocket.ErrMessageTooBig 并被库关闭连接，不会作为正常事件完整交付
// 给调用方，也就不需要依赖后续的 len(data) 检查。
//
// 注意：coder/websocket 的 limitReader 会多留一个字节的余量用于 fin 帧，因此超限时
// Read 返回的 data 可能已包含部分或全部字节，唯一可靠的信号是 Read 返回错误。
func TestWSUpstreamDialReadLimit(t *testing.T) {
	const limit = 4096

	previousLimit := maxSSEEventSize
	maxSSEEventSize = limit
	defer func() { maxSSEEventSize = previousLimit }()

	cases := []struct {
		name       string
		size       int
		wantTooBig bool
	}{
		{name: "at limit is received", size: limit, wantTooBig: false},
		{name: "over limit fails at read", size: limit + 1, wantTooBig: true},
	}

	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			payload := bytes.Repeat([]byte("a"), tc.size)
			writeErrCh := make(chan error, 1)

			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/v1/responses" {
					http.NotFound(w, r)
					return
				}
				conn, err := websocket.Accept(w, r, nil)
				if err != nil {
					writeErrCh <- err
					return
				}
				defer conn.CloseNow()

				writeCtx, cancelWrite := context.WithTimeout(r.Context(), 5*time.Second)
				defer cancelWrite()
				writeErrCh <- conn.Write(writeCtx, websocket.MessageText, payload)

				// 阻塞到客户端关闭连接为止，避免 CloseNow 截断尚未投递的数据；
				// 该 Read 同时会回应客户端的 close 帧，让客户端的关闭握手立即完成。
				readCtx, cancelRead := context.WithTimeout(r.Context(), 10*time.Second)
				defer cancelRead()
				_, _, _ = conn.Read(readCtx)
			}))
			defer server.Close()

			pool := newUnitWSPool()
			channel := &dbmodel.Channel{ID: 900 + i}
			headers := buildUpstreamWSHeaders(nil, channel, "read-limit-key")
			key := newWSPoolKey(channel.ID, 901, headers)
			if !pool.reserveDial(key) {
				t.Fatal("reserveDial refused the first dial for a fresh pool")
			}

			dialCtx, cancelDial := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancelDial()
			pc, _, err := pool.Dial(dialCtx, key, channel, server.URL+"/v1", headers)
			if err != nil {
				t.Fatalf("Dial() error = %v", err)
			}
			defer pool.Remove(key)

			readCtx, cancelRead := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancelRead()
			_, data, readErr := pc.conn.Read(readCtx)

			if tc.wantTooBig {
				if readErr == nil {
					t.Fatalf("Read() succeeded for a %d byte message with read limit %d, got %d bytes", tc.size, limit, len(data))
				}
				if !errors.Is(readErr, websocket.ErrMessageTooBig) {
					t.Fatalf("Read() error = %v, want error wrapping websocket.ErrMessageTooBig", readErr)
				}
				// 超限后连接已被库关闭，后续读取不能再返回正常事件。
				nextCtx, cancelNext := context.WithTimeout(context.Background(), 2*time.Second)
				defer cancelNext()
				if _, _, err := pc.conn.Read(nextCtx); err == nil {
					t.Fatal("connection stayed usable after an oversize message")
				}
				return
			}

			if readErr != nil {
				t.Fatalf("Read() error = %v for a message exactly at the %d byte limit", readErr, limit)
			}
			if len(data) != tc.size {
				t.Fatalf("Read() returned %d bytes, want %d", len(data), tc.size)
			}
			if writeErr := <-writeErrCh; writeErr != nil {
				t.Fatalf("upstream Write() error = %v", writeErr)
			}
		})
	}
}
