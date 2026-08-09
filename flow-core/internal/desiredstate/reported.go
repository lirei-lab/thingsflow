package desiredstate

import (
	"context"
	"encoding/json"
	"log"
	"strings"
	"time"

	"github.com/nats-io/nats.go"

	"flow-core/internal/tenant"
)

// ReportedLookup resolves an MQTT identity (the clientid in the reported topic,
// e.g. thingsflow/devices/<mqttId>/attributes) to the owning (deviceID,
// tenantID). Injected at startup so this package does not import the device
// package. Returns an error when no device owns that mqttId.
var ReportedLookup func(mqttID string) (deviceID string, tenantID string, err error)

// StartReportedConsumer subscribes to device-reported attributes and converges
// them into the twin record + KV (R5). It is a READ-ONLY subscription on the
// raw reported subject (the same subject the device-origin attributes bridge
// forwards to) that filters for the .../attributes topic via the `topic` header
// — it never modifies the bridge, Bento, or the ingest pipeline (verified by
// git status in the plan). Reported values are device-origin, so they merge as
// CLIENT_SCOPE into the shared write path (attribute_kv + twin-state KV),
// giving the twin a reported view alongside the 05-02 desired view.
//
// The consumer is resilient like the RPC response listener: it retries with
// backoff until the context is done and never crashes the process.
func StartReportedConsumer(ctx context.Context, natsURL, subject string) {
	if natsURL == "" || subject == "" || ReportedLookup == nil {
		return
	}
	go func() {
		backoff := time.Second
		for {
			nc, err := nats.Connect(natsURL,
				nats.Name("thingsflow-reported-converge"),
				nats.Timeout(5*time.Second))
			if err == nil {
				sub, subErr := nc.Subscribe(subject, handleReportedMessage)
				if subErr == nil {
					log.Printf("reported convergence active: subject=%s", subject)
					<-ctx.Done()
					_ = sub.Unsubscribe()
					nc.Close()
					return
				}
				nc.Close()
				err = subErr
			}
			log.Printf("WARN reported consumer retry: %v", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			if backoff < 30*time.Second {
				backoff *= 2
			}
		}
	}()
}

// handleReportedMessage converges one device-reported attribute message.
// Only messages whose `topic` header names the device attributes topic
// (thingsflow/devices/<mqttId>/attributes) are processed — telemetry messages
// on the same raw subject are ignored (they are not twin state). The payload
// is the flat {key: value} reported map; values merge as CLIENT_SCOPE.
func handleReportedMessage(msg *nats.Msg) {
	topic := ""
	if msg.Header != nil {
		topic = msg.Header.Get("topic")
	}
	mqttID, ok := parseReportedTopic(topic)
	if !ok {
		return
	}
	deviceID, tenantID, err := ReportedLookup(mqttID)
	if err != nil {
		log.Printf("DEBUG reported: unknown mqttId %q skipped: %v", mqttID, err)
		return
	}
	var values map[string]interface{}
	if err := json.Unmarshal(msg.Data, &values); err != nil || len(values) == 0 {
		log.Printf("DEBUG reported: unparseable/empty payload on %q skipped", topic)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// Reported = device-origin → CLIENT_SCOPE. Reuses the shared write path so
	// attribute_kv + the twin-state KV always agree. Never touches the ingest
	// hot path (this is a control-plane merge, not a Bento materializer).
	if err := tenant.SaveAttributesKV(ctx, tenantID, "DEVICE", deviceID, "CLIENT_SCOPE", values); err != nil {
		log.Printf("WARN reported merge failed device=%s mqttId=%s: %v", deviceID, mqttID, err)
	}
}

// parseReportedTopic extracts the mqttId from a device attributes topic
// (thingsflow/devices/<mqttId>/attributes). Any other shape → ok=false so
// telemetry and RPC messages on the shared raw subject are ignored.
func parseReportedTopic(topic string) (string, bool) {
	parts := strings.Split(topic, "/")
	// thingsflow / devices / <mqttId> / attributes
	if len(parts) != 4 || parts[0] != "thingsflow" || parts[1] != "devices" || parts[3] != "attributes" {
		return "", false
	}
	if parts[2] == "" {
		return "", false
	}
	return parts[2], true
}
