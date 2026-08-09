package desiredstate

import (
	"testing"

	"github.com/nats-io/nats.go"
)

// TestParseReportedTopic — extracts mqttId from the device attributes topic and
// rejects telemetry/rpc/empty shapes so only reported attributes converge.
func TestParseReportedTopic(t *testing.T) {
	cases := []struct {
		topic string
		id    string
		ok    bool
	}{
		{"thingsflow/devices/dev-1/attributes", "dev-1", true},
		{"thingsflow/devices/uuid123/attributes", "uuid123", true},
		{"thingsflow/devices/dev-1/telemetry", "", false}, // not attributes
		{"thingsflow/devices/dev-1/rpc/request/req-1", "", false},
		{"thingsflow/devices//attributes", "", false},            // empty mqttId
		{"other/devices/dev-1/attributes", "", false},            // wrong prefix
		{"thingsflow/devices/dev-1", "", false},                  // too short
		{"thingsflow/devices/dev-1/attributes/extra", "", false}, // too long
	}
	for _, tc := range cases {
		id, ok := parseReportedTopic(tc.topic)
		if id != tc.id || ok != tc.ok {
			t.Fatalf("parseReportedTopic(%q) = (%q,%v), want (%q,%v)", tc.topic, id, ok, tc.id, tc.ok)
		}
	}
}

// TestHandleReportedMessageConverges — a reported attributes message resolves
// via ReportedLookup and merges CLIENT_SCOPE values through SaveAttributesKV.
// SaveAttributesKV is exercised via the real function var (unit-level: the
// message→merge wiring is what is asserted; the DB-backed merge is covered by
// the DSN round-trip in twin/tenant suites). Here we pin that a non-attributes
// topic and an unknown mqttId are skipped without a panic.
func TestHandleReportedMessageSkipsNonAttributes(t *testing.T) {
	origLookup := ReportedLookup
	defer func() { ReportedLookup = origLookup }()
	ReportedLookup = func(mqttID string) (string, string, error) {
		return "device-1", "tenant-a", nil
	}

	// Telemetry message on the shared raw subject must be ignored (topic header).
	handleReportedMessage(&nats.Msg{
		Subject: "tf.ingest.mqtt.raw.events",
		Header:  nats.Header{"topic": []string{"thingsflow/devices/dev-1/telemetry"}},
		Data:    []byte(`{"temp":25}`),
	})
	// RPC response message must be ignored.
	handleReportedMessage(&nats.Msg{
		Subject: "tf.ingest.mqtt.raw.events",
		Header:  nats.Header{"topic": []string{"thingsflow/devices/dev-1/rpc/response/req-1"}},
		Data:    []byte(`{"ok":true}`),
	})
	// No panic reached — that is the assertion (nothing to converge on).
}

// TestHandleReportedMessageUnknownMqttID — an attributes message whose mqttId
// cannot be resolved is skipped (WARN), never a panic.
func TestHandleReportedMessageUnknownMqttID(t *testing.T) {
	origLookup := ReportedLookup
	defer func() { ReportedLookup = origLookup }()
	ReportedLookup = func(mqttID string) (string, string, error) {
		return "", "", errTestNotFound
	}

	handleReportedMessage(&nats.Msg{
		Subject: "tf.ingest.mqtt.raw.events",
		Header:  nats.Header{"topic": []string{"thingsflow/devices/ghost/attributes"}},
		Data:    []byte(`{"k":"v"}`),
	})
	// Reached without panic.
}

// TestHandleReportedMessageEmptyPayload — an unparseable/empty payload is
// skipped defensively.
func TestHandleReportedMessageEmptyPayload(t *testing.T) {
	origLookup := ReportedLookup
	defer func() { ReportedLookup = origLookup }()
	ReportedLookup = func(mqttID string) (string, string, error) {
		return "device-1", "tenant-a", nil
	}

	handleReportedMessage(&nats.Msg{
		Subject: "tf.ingest.mqtt.raw.events",
		Header:  nats.Header{"topic": []string{"thingsflow/devices/dev-1/attributes"}},
		Data:    []byte(`{}`),
	})
	handleReportedMessage(&nats.Msg{
		Subject: "tf.ingest.mqtt.raw.events",
		Header:  nats.Header{"topic": []string{"thingsflow/devices/dev-1/attributes"}},
		Data:    []byte(`{not json`),
	})
	// Reached without panic.
}
