package rpc

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

// resetState clears the package globals a test may have touched. The config is
// sync.Once-guarded in production; tests need to re-read env, so they set cfg
// directly through here.
func resetState(t *testing.T, c config) {
	t.Helper()
	cfgOnce.Do(func() {}) // burn the Once so loadConfig() returns what we set
	cfg = c
	pendingMu.Lock()
	pending = map[string]chan []byte{}
	pendingMu.Unlock()
	repliesMu.Lock()
	repliesReady = true
	repliesMu.Unlock()
	t.Cleanup(func() {
		pendingMu.Lock()
		pending = map[string]chan []byte{}
		pendingMu.Unlock()
	})
}

// fakeBroker stands in for rmqtt's HTTP API. online controls whether the device
// is reported as connected; published records what we asked the broker to send.
type fakeBroker struct {
	mu        sync.Mutex
	online    bool
	published []map[string]interface{}
	server    *httptest.Server
}

func newFakeBroker(t *testing.T, online bool) *fakeBroker {
	t.Helper()
	b := &fakeBroker{online: online}
	b.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasPrefix(r.URL.Path, "/api/v1/clients/"):
			b.mu.Lock()
			ok := b.online
			b.mu.Unlock()
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			w.WriteHeader(http.StatusOK)
		case r.URL.Path == "/api/v1/mqtt/publish":
			var body map[string]interface{}
			_ = json.NewDecoder(r.Body).Decode(&body)
			b.mu.Lock()
			b.published = append(b.published, body)
			b.mu.Unlock()
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ok"))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(b.server.Close)
	return b
}

func (b *fakeBroker) lastPublish() map[string]interface{} {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.published) == 0 {
		return nil
	}
	return b.published[len(b.published)-1]
}

func TestSendOneway_PublishesToTheDeviceOwnTopic(t *testing.T) {
	b := newFakeBroker(t, true)
	resetState(t, config{enabled: true, brokerAPI: b.server.URL, timeout: time.Second})

	if err := SendOneway("abc123", "req-1", []byte(`{"method":"reboot"}`)); err != nil {
		t.Fatalf("SendOneway: %v", err)
	}
	got := b.lastPublish()
	if got == nil {
		t.Fatal("nothing was published to the broker")
	}
	want := "thingsflow/devices/abc123/rpc/request/req-1"
	if got["topic"] != want {
		t.Fatalf("topic = %v, want %v", got["topic"], want)
	}
	// QoS 0 would let a command vanish silently when the broker is under load.
	if got["qos"] != float64(1) {
		t.Fatalf("qos = %v, want 1", got["qos"])
	}
	// A retained command would be re-delivered to the device on every reconnect —
	// for a breaker-control fleet that is a replay of the last command forever.
	if got["retain"] != false {
		t.Fatalf("retain = %v, want false", got["retain"])
	}
}

// An offline device must fail immediately. Without the presence check the
// publish succeeds into the void and the caller waits out the whole timeout.
func TestSendOneway_OfflineDeviceFailsFast(t *testing.T) {
	b := newFakeBroker(t, false)
	resetState(t, config{enabled: true, brokerAPI: b.server.URL, timeout: time.Second})

	if err := SendOneway("abc123", "req-1", []byte(`{}`)); err != ErrOffline {
		t.Fatalf("err = %v, want ErrOffline", err)
	}
	if b.lastPublish() != nil {
		t.Fatal("published to an offline device instead of failing")
	}
}

func TestSendTwoway_ReturnsDeviceReply(t *testing.T) {
	b := newFakeBroker(t, true)
	resetState(t, config{enabled: true, brokerAPI: b.server.URL, timeout: 2 * time.Second})

	go func() {
		// Wait for registration, then deliver the reply the way the NATS listener would.
		for i := 0; i < 100; i++ {
			pendingMu.Lock()
			ch, ok := pending["req-7"]
			pendingMu.Unlock()
			if ok {
				ch <- []byte(`{"ok":true}`)
				return
			}
			time.Sleep(2 * time.Millisecond)
		}
	}()

	reply, err := SendTwoway(context.Background(), "abc123", "req-7", []byte(`{"method":"get"}`), time.Second)
	if err != nil {
		t.Fatalf("SendTwoway: %v", err)
	}
	if string(reply) != `{"ok":true}` {
		t.Fatalf("reply = %s", reply)
	}
}

func TestSendTwoway_TimesOutWhenSilent(t *testing.T) {
	b := newFakeBroker(t, true)
	resetState(t, config{enabled: true, brokerAPI: b.server.URL, timeout: time.Second})

	_, err := SendTwoway(context.Background(), "abc123", "req-8", []byte(`{}`), 60*time.Millisecond)
	if err != ErrTimeout {
		t.Fatalf("err = %v, want ErrTimeout", err)
	}
	// The waiter must be unregistered, or a long-running server leaks a channel
	// for every command a device never answers.
	pendingMu.Lock()
	_, leaked := pending["req-8"]
	pendingMu.Unlock()
	if leaked {
		t.Fatal("pending entry leaked after timeout")
	}
}

// A reply that arrives between registration and the caller's select must not be
// lost; the buffered channel is what makes that safe.
func TestHandleResponse_DeliversToWaiter(t *testing.T) {
	resetState(t, config{enabled: true, timeout: time.Second})
	ch := make(chan []byte, 1)
	pendingMu.Lock()
	pending["req-9"] = ch
	pendingMu.Unlock()

	handleResponse(natsMsg("thingsflow/devices/dev9/rpc/response/req-9", "", []byte(`{"v":1}`)))

	select {
	case got := <-ch:
		if string(got) != `{"v":1}` {
			t.Fatalf("got %s", got)
		}
	default:
		t.Fatal("reply was not delivered to the waiter")
	}
}

// The broker ACL already binds the response topic to the connection's Client ID,
// but flow-core re-checks against the JWT claim. If the two ever disagree the
// reply is dropped rather than fulfilling another device's pending call.
func TestHandleResponse_RejectsIdentityMismatch(t *testing.T) {
	resetState(t, config{enabled: true, timeout: time.Second})
	ch := make(chan []byte, 1)
	pendingMu.Lock()
	pending["req-10"] = ch
	pendingMu.Unlock()

	jwt := fakeDeviceJWT(t, "someoneelse")
	handleResponse(natsMsg("thingsflow/devices/dev10/rpc/response/req-10", jwt, []byte(`{"stolen":true}`)))

	select {
	case got := <-ch:
		t.Fatalf("accepted a reply from a mismatched identity: %s", got)
	default:
	}
}

func TestHandleResponse_AcceptsMatchingIdentity(t *testing.T) {
	resetState(t, config{enabled: true, timeout: time.Second})
	ch := make(chan []byte, 1)
	pendingMu.Lock()
	pending["req-11"] = ch
	pendingMu.Unlock()

	handleResponse(natsMsg("thingsflow/devices/dev11/rpc/response/req-11",
		fakeDeviceJWT(t, "dev11"), []byte(`{"ok":1}`)))

	select {
	case <-ch:
	default:
		t.Fatal("rejected a reply whose JWT identity matches the topic")
	}
}

func TestParseResponseTopic(t *testing.T) {
	cases := []struct {
		topic  string
		mqttID string
		reqID  string
		ok     bool
	}{
		{"thingsflow/devices/abc/rpc/response/r1", "abc", "r1", true},
		{"thingsflow/devices/abc/rpc/request/r1", "", "", false}, // request, not response
		{"thingsflow/devices/abc/telemetry", "", "", false},      // telemetry must not be mistaken for a reply
		{"thingsflow/devices//rpc/response/r1", "", "", false},   // empty identity
		{"thingsflow/devices/abc/rpc/response/", "", "", false},  // empty request id
		{"evil/devices/abc/rpc/response/r1", "", "", false},      // foreign namespace
	}
	for _, c := range cases {
		mqttID, reqID, ok := ParseResponseTopic(c.topic)
		if ok != c.ok || mqttID != c.mqttID || reqID != c.reqID {
			t.Errorf("ParseResponseTopic(%q) = (%q,%q,%v), want (%q,%q,%v)",
				c.topic, mqttID, reqID, ok, c.mqttID, c.reqID, c.ok)
		}
	}
}

// JWT payloads are unpadded base64url. A padding-strict decoder here would yield
// empty claims and silently disable the identity check — the same failure mode
// that once dropped every MQTT message on the ingest path.
func TestDeviceIdentityFromUsername_UnpaddedJWT(t *testing.T) {
	tok := fakeDeviceJWT(t, "dev-xyz")
	if got := deviceIdentityFromUsername(tok); got != "dev-xyz" {
		t.Fatalf("got %q, want dev-xyz", got)
	}
	if got := deviceIdentityFromUsername("Bearer " + tok); got != "dev-xyz" {
		t.Fatalf("Bearer prefix not stripped: got %q", got)
	}
	if got := deviceIdentityFromUsername("not-a-jwt"); got != "" {
		t.Fatalf("got %q, want empty for a non-JWT", got)
	}
}

func TestClampTimeout(t *testing.T) {
	if got := clampTimeout(0); got != defaultTimeout {
		t.Errorf("clampTimeout(0) = %v, want the default", got)
	}
	if got := clampTimeout(5 * time.Minute); got != maxTimeout {
		t.Errorf("clampTimeout(5m) = %v, want capped at %v", got, maxTimeout)
	}
	if got := clampTimeout(3 * time.Second); got != 3*time.Second {
		t.Errorf("clampTimeout(3s) = %v", got)
	}
}

func natsMsg(topic, username string, data []byte) *nats.Msg {
	h := nats.Header{}
	h.Set("topic", topic)
	if username != "" {
		h.Set("from_username", username)
	}
	return &nats.Msg{Data: data, Header: h}
}

// fakeDeviceJWT builds an unsigned token whose payload carries a clientid claim.
// The signature is never checked here — rmqtt already verified it before the
// message reached NATS; this code only reads the claim.
func fakeDeviceJWT(t *testing.T, clientID string) string {
	t.Helper()
	enc := func(v interface{}) string {
		b, err := json.Marshal(v)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		return base64.RawURLEncoding.EncodeToString(b)
	}
	return enc(map[string]string{"alg": "ES256", "typ": "JWT"}) + "." +
		enc(map[string]string{"clientid": clientID}) + ".sig"
}
