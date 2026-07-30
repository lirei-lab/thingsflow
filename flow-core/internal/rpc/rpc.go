// Package rpc delivers server-to-device commands over MQTT and correlates the
// device's reply back to the waiting HTTP request.
//
// Direction matters here. Telemetry flows device→cloud and deliberately never
// touches flow-core: rmqtt bridges it straight to NATS and Bento writes it. RPC
// flows the other way — cloud→device — and has no data-plane path at all, which
// is why /api/rpc/* answered 501 until now.
//
// Delivery uses rmqtt's HTTP API (the `rmqtt-http-api` plugin, already enabled)
// rather than a new NATS→MQTT bridge: the broker is the only component that
// knows which devices are connected, and asking it to publish is one call with
// no extra moving parts. The reply comes back the way telemetry already does —
// device publishes, rmqtt bridges to NATS, flow-core reads it — so the response
// path reuses transport that is already load-bearing.
//
// Topics are platform-native (`thingsflow/devices/<mqttId>/rpc/...`), not
// ThingsBoard's `v1/devices/me/rpc/...`. That is a security choice: the broker
// ACL binds every device topic to `%c`, the connection's Client ID, which
// rmqtt-auth-jwt pins to the JWT's `clientid` claim. A literal "me" segment
// cannot be bound that way, so adopting TB's spelling would trade broker-level
// anti-spoofing for cosmetic familiarity on the device side.
package rpc

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
)

// Errors the HTTP layer maps to status codes.
var (
	ErrDisabled  = errors.New("rpc delivery is disabled")
	ErrOffline   = errors.New("device is not connected")
	ErrTimeout   = errors.New("device did not respond in time")
	ErrNoReplies = errors.New("rpc response transport is unavailable")
)

const (
	defaultTimeout = 10 * time.Second
	maxTimeout     = 60 * time.Second
)

type config struct {
	enabled         bool
	brokerAPI       string
	responseSubject string
	timeout         time.Duration
}

var (
	cfg     config
	cfgOnce sync.Once

	pendingMu sync.Mutex
	pending   = map[string]chan []byte{}

	// repliesReady is closed once the NATS response listener is subscribed.
	// Two-way calls made before that would hang until timeout for no good
	// reason, so they fail fast with ErrNoReplies instead.
	repliesMu    sync.RWMutex
	repliesReady bool
)

func loadConfig() config {
	cfgOnce.Do(func() {
		cfg = config{
			enabled:         strings.EqualFold(getEnv("RPC_ENABLED", "true"), "true"),
			brokerAPI:       strings.TrimRight(getEnv("MQTT_BROKER_API_URL", "http://thingsflow-rmqtt-edge:6060"), "/"),
			responseSubject: getEnv("RPC_RESPONSE_SUBJECT", "tf.rpc.response"),
			timeout:         defaultTimeout,
		}
		if ms, err := strconv.Atoi(getEnv("RPC_DEFAULT_TIMEOUT_MS", "")); err == nil && ms > 0 {
			cfg.timeout = clampTimeout(time.Duration(ms) * time.Millisecond)
		}
	})
	return cfg
}

func getEnv(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

func clampTimeout(d time.Duration) time.Duration {
	if d <= 0 {
		return defaultTimeout
	}
	if d > maxTimeout {
		return maxTimeout
	}
	return d
}

// Enabled reports whether RPC delivery is configured.
func Enabled() bool { return loadConfig().enabled }

var httpClient = &http.Client{Timeout: 5 * time.Second}

// IsOnline asks the broker whether the device currently holds a session.
// Without this a command to an offline device looks identical to a slow one:
// the publish succeeds, nobody is subscribed, and the caller waits out the full
// timeout before learning nothing happened.
func IsOnline(mqttID string) bool {
	c := loadConfig()
	req, err := http.NewRequest(http.MethodGet, c.brokerAPI+"/api/v1/clients/"+mqttID, nil)
	if err != nil {
		return false
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		// Broker unreachable — do not claim the device is offline, that would be a
		// misleading error. Let the publish attempt produce the real failure.
		slog.Warn("rpc_broker_unreachable", slog.String("err", err.Error()))
		return true
	}
	defer resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

// publish asks the broker to deliver payload on the device's request topic.
func publish(mqttID, requestID string, payload []byte) error {
	c := loadConfig()
	body, err := json.Marshal(map[string]interface{}{
		"topic":   RequestTopic(mqttID, requestID),
		"payload": string(payload),
		"qos":     1,
		"retain":  false,
		// Identifies the publisher in broker logs; it is not a device identity.
		"clientid": "flow-core-rpc",
	})
	if err != nil {
		return err
	}
	req, err := http.NewRequest(http.MethodPost, c.brokerAPI+"/api/v1/mqtt/publish", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("broker publish failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("broker publish returned %d", resp.StatusCode)
	}
	return nil
}

// RequestTopic is the topic a device must subscribe to for commands.
func RequestTopic(mqttID, requestID string) string {
	return fmt.Sprintf("thingsflow/devices/%s/rpc/request/%s", mqttID, requestID)
}

// ResponseTopic is the topic a device must publish its reply on.
func ResponseTopic(mqttID, requestID string) string {
	return fmt.Sprintf("thingsflow/devices/%s/rpc/response/%s", mqttID, requestID)
}

// SendOneway delivers a command and returns as soon as the broker accepts it.
// No reply is awaited, so this works even when the response transport is down.
func SendOneway(mqttID, requestID string, payload []byte) error {
	if !Enabled() {
		return ErrDisabled
	}
	if !IsOnline(mqttID) {
		return ErrOffline
	}
	return publish(mqttID, requestID, payload)
}

// SendTwoway delivers a command and blocks until the device replies or timeout
// elapses. The returned bytes are the device's raw response payload.
func SendTwoway(ctx context.Context, mqttID, requestID string, payload []byte, timeout time.Duration) ([]byte, error) {
	if !Enabled() {
		return nil, ErrDisabled
	}
	if !repliesListening() {
		return nil, ErrNoReplies
	}
	if !IsOnline(mqttID) {
		return nil, ErrOffline
	}

	// Register before publishing. A fast device can reply before publish() even
	// returns, and an unregistered reply is dropped — which would surface as a
	// timeout on a device that answered correctly.
	ch := make(chan []byte, 1)
	pendingMu.Lock()
	pending[requestID] = ch
	pendingMu.Unlock()
	defer func() {
		pendingMu.Lock()
		delete(pending, requestID)
		pendingMu.Unlock()
	}()

	if err := publish(mqttID, requestID, payload); err != nil {
		return nil, err
	}

	select {
	case reply := <-ch:
		return reply, nil
	case <-time.After(clampTimeout(timeout)):
		return nil, ErrTimeout
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func repliesListening() bool {
	repliesMu.RLock()
	defer repliesMu.RUnlock()
	return repliesReady
}

// StartResponseListener subscribes to the NATS subject that rmqtt bridges device
// RPC replies onto. Runs for the process lifetime; safe to call when RPC is off
// (it returns immediately).
func StartResponseListener(ctx context.Context, natsURL string) {
	c := loadConfig()
	if !c.enabled || natsURL == "" {
		return
	}
	go func() {
		for attempt := 1; ; attempt++ {
			nc, err := nats.Connect(natsURL,
				nats.Name("thingsflow-rpc-responses"),
				nats.Timeout(5*time.Second))
			if err == nil {
				sub, subErr := nc.Subscribe(c.responseSubject, handleResponse)
				if subErr == nil {
					repliesMu.Lock()
					repliesReady = true
					repliesMu.Unlock()
					slog.Info("rpc_response_listener_started", slog.String("subject", c.responseSubject))
					<-ctx.Done()
					_ = sub.Unsubscribe()
					nc.Close()
					return
				}
				nc.Close()
				err = subErr
			}
			repliesMu.Lock()
			repliesReady = false
			repliesMu.Unlock()
			slog.Warn("rpc_response_listener_retry",
				slog.Int("attempt", attempt), slog.String("err", err.Error()))
			select {
			case <-ctx.Done():
				return
			case <-time.After(5 * time.Second):
			}
		}
	}()
}

// handleResponse routes one bridged device reply to whoever is waiting for it.
func handleResponse(msg *nats.Msg) {
	topic := ""
	if msg.Header != nil {
		topic = msg.Header.Get("topic")
	}
	mqttID, requestID, ok := ParseResponseTopic(topic)
	if !ok {
		slog.Warn("rpc_response_bad_topic", slog.String("topic", topic))
		return
	}

	// Anti-spoofing, same rule as telemetry ingest: trust the JWT claim carried in
	// the MQTT username over anything in the topic or payload. The broker ACL
	// already binds the topic to the connection's Client ID, so this is the second
	// of two independent checks rather than the only one.
	if claimed := deviceIdentityFromUsername(msg.Header.Get("from_username")); claimed != "" && claimed != mqttID {
		slog.Warn("rpc_response_identity_mismatch",
			slog.String("topic_identity", mqttID), slog.String("claimed_identity", claimed))
		return
	}

	pendingMu.Lock()
	ch, waiting := pending[requestID]
	pendingMu.Unlock()
	if !waiting {
		// Late reply after the caller gave up, or a reply to a one-way command.
		// Neither is an error worth alarming on.
		return
	}
	select {
	case ch <- msg.Data:
	default:
	}
}

// ParseResponseTopic extracts (mqttId, requestId) from a device reply topic.
func ParseResponseTopic(topic string) (string, string, bool) {
	parts := strings.Split(strings.Trim(topic, "/"), "/")
	// thingsflow / devices / <mqttId> / rpc / response / <requestId>
	if len(parts) != 6 || parts[0] != "thingsflow" || parts[1] != "devices" ||
		parts[3] != "rpc" || parts[4] != "response" {
		return "", "", false
	}
	if parts[2] == "" || parts[5] == "" {
		return "", "", false
	}
	return parts[2], parts[5], true
}

// deviceIdentityFromUsername pulls the mqtt identity claim out of the device JWT
// that rmqtt forwards as `from_username`. Returns "" when absent or unparseable —
// callers treat that as "no second opinion", not as a mismatch.
func deviceIdentityFromUsername(username string) string {
	raw := strings.TrimPrefix(strings.TrimSpace(username), "Bearer ")
	parts := strings.Split(raw, ".")
	if len(parts) != 3 {
		return ""
	}
	payload, err := base64URLDecode(parts[1])
	if err != nil {
		return ""
	}
	var claims struct {
		ClientID string `json:"clientid"`
		MqttID   string `json:"mqttId"`
	}
	if err := json.Unmarshal(payload, &claims); err != nil {
		return ""
	}
	if claims.ClientID != "" {
		return claims.ClientID
	}
	return claims.MqttID
}
