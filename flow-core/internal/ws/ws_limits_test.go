package ws

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// Phase 5c — the WS plane never called SetReadLimit, so gorilla buffered
// whatever a client announced: one authenticated session could stream a
// multi-GB frame and OOM the pod. These tests pin the bound (oversized frame
// is refused with 1009) and, just as importantly, that a big-but-legitimate
// frame still goes through — a real dashboard batch must not regress.

// dialRawWS opens an unauthenticated connection with no DB dependency.
func dialRawWS(t *testing.T) *websocket.Conn {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(HandleWebSocket))
	t.Cleanup(srv.Close)

	conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(srv.URL, "http"), nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}

func TestWebSocketRejectsOversizedFrame(t *testing.T) {
	conn := dialRawWS(t)

	// One byte over the cap. Sent as the very first message so nothing but the
	// read limit can be responsible for the close.
	oversized := make([]byte, maxWSMessageBytes+1)
	for i := range oversized {
		oversized[i] = 'a'
	}
	if err := conn.WriteMessage(websocket.TextMessage, oversized); err != nil {
		t.Fatalf("write oversized: %v", err)
	}

	_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	_, _, err := conn.ReadMessage()
	if err == nil {
		t.Fatal("oversized frame was accepted; expected the connection to be closed")
	}
	if !websocket.IsCloseError(err, websocket.CloseMessageTooBig) {
		t.Fatalf("oversized frame closed with %v, want close code 1009 (message too big)", err)
	}
}

func TestWebSocketAcceptsLargeLegitimateFrame(t *testing.T) {
	conn := dialRawWS(t)

	if err := conn.WriteJSON(map[string]interface{}{
		"authCmd": map[string]interface{}{"cmdId": 0, "token": wsToken(t, wsTenantA, "TENANT_ADMIN")},
	}); err != nil {
		t.Fatalf("authCmd: %v", err)
	}

	// Half the cap: bigger than any real subscription batch, still accepted.
	// NOTIFICATIONS_COUNT is answered without touching the database, so this
	// proves the frame was read and processed end to end.
	padding := strings.Repeat("x", maxWSMessageBytes/2)
	if err := conn.WriteJSON(map[string]interface{}{
		"cmds":    []map[string]interface{}{{"type": "NOTIFICATIONS_COUNT", "cmdId": 7}},
		"padding": padding,
	}); err != nil {
		t.Fatalf("write large frame: %v", err)
	}

	_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	_, raw, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("large legitimate frame was rejected: %v", err)
	}
	var msg map[string]interface{}
	if err := json.Unmarshal(raw, &msg); err != nil {
		t.Fatalf("decode reply: %v", err)
	}
	if msg["cmdUpdateType"] != "NOTIFICATIONS_COUNT" {
		t.Fatalf("unexpected reply to large frame: %#v", msg)
	}
}

// TestWebSocketPingKeepsIdleSessionAlive documents the keepalive contract: the
// read deadline is armed at wsPongWait, the server pings well before it, and a
// client that answers pings (every WS client does, at the protocol level) is
// never disconnected. Kept short by asserting on the constants' relationship
// rather than sleeping 90s in CI.
func TestWebSocketKeepaliveBudget(t *testing.T) {
	if wsPingInterval >= wsPongWait {
		t.Fatalf("ping interval %v must be below the read deadline %v", wsPingInterval, wsPongWait)
	}
	if wsPongWait < 2*wsPingInterval {
		t.Fatalf("read deadline %v must tolerate at least two lost pings at %v", wsPongWait, wsPingInterval)
	}
}
