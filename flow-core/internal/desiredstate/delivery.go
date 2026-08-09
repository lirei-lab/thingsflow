package desiredstate

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

// Errors the desired-state delivery surfaces to its (fire-and-forget) callers.
var (
	ErrDisabled = errors.New("desired-state delivery is disabled")
)

// DesiredTopic is the per-device topic a device subscribes to for its desired
// state (R5, spike 05-01). The dedicated .../desired segment deliberately avoids
// the NATS egress bridge (thingsflow/devices/+/attributes -> raw.events) so
// desired state never leaks into the ingest hot path.
func DesiredTopic(mqttID string) string {
	return fmt.Sprintf("thingsflow/devices/%s/desired", mqttID)
}

// DeviceLookup resolves a device id to (mqttIdentity, tenantId). Injected at
// startup (mirrors rpc.DeviceLookup) so this package does not import the device
// package. Returns an error when the device does not exist.
var DeviceLookup func(deviceID string) (mqttID string, tenantID string, err error)

type config struct {
	enabled   bool
	brokerAPI string
}

var (
	cfg     config
	cfgOnce sync.Once
)

func loadConfig() config {
	cfgOnce.Do(func() {
		cfg = config{
			enabled:   strings.EqualFold(getEnv("DESIRED_STATE_ENABLED", "true"), "true"),
			brokerAPI: strings.TrimRight(getEnv("MQTT_BROKER_API_URL", "http://thingsflow-rmqtt-edge:6060"), "/"),
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

// Enabled reports whether desired-state delivery is configured.
func Enabled() bool { return loadConfig().enabled }

var httpClient = &http.Client{Timeout: 5 * time.Second}

// publishDesiredFn is the injectable retained-publish sink (mirrors the rpc
// publish / broadcastTelemetry function-var pattern): production POSTs to the
// rmqtt HTTP API with retain:true; tests replace it with a spy that captures the
// publish without a broker.
var publishDesiredFn = publishRetained

// DeliverDesired fire-and-forgets the device's desired state onto its retained
// desired topic. A retained publish means the broker stores the payload and
// delivers it to the device on connect/reconnect (replay), so the desired state
// reaches the device even if it was offline when written. Never blocks a write;
// errors are logged, not returned to the control-plane write path.
func DeliverDesired(mqttID string, payload []byte) {
	if mqttID == "" || len(payload) == 0 {
		return
	}
	if !Enabled() {
		return
	}
	if err := publishDesiredFn(mqttID, payload); err != nil {
		slog.Warn("desired_delivery_failed", slog.String("mqtt_id", mqttID), slog.String("err", err.Error()))
	}
}

// DeliverDesiredForEntity resolves the device's mqttIdentity and delivers the
// payload. Convenience used by the twin write path when only the device UUID is
// known. An unresolvable device is skipped (WARN) — the HTTP poll still covers
// non-MQTT devices.
func DeliverDesiredForEntity(deviceID string, payload []byte) {
	if DeviceLookup == nil {
		return
	}
	mqttID, _, err := DeviceLookup(deviceID)
	if err != nil {
		slog.Warn("desired_delivery_skip", slog.String("device_id", deviceID), slog.String("err", err.Error()))
		return
	}
	DeliverDesired(mqttID, payload)
}

// publishRetained asks the broker to deliver the payload on the device's desired
// topic with retain:true (the rmqtt HTTP API — same endpoint RPC uses, retain
// flipped on). A dedicated clientid keeps broker logs unambiguous.
func publishRetained(mqttID string, payload []byte) error {
	c := loadConfig()
	body, err := json.Marshal(map[string]interface{}{
		"topic":    DesiredTopic(mqttID),
		"payload":  string(payload),
		"qos":      1,
		"retain":   true,
		"clientid": "flow-core-desired",
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
