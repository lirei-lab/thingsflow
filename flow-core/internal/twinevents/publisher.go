package twinevents

import (
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
)

// Event-type constants are the stable, journal-wide event identities the
// control-plane writers emit (R4). The WS fan-out (Plan 04-02) and any
// auditing/search consumer keys off `type` to route an event.
const (
	EventAttributeSaved   = "twin.attribute.saved"
	EventFeatureSaved     = "twin.feature.saved"
	EventRelationSaved    = "twin.relation.saved"
	EventDeviceStateSaved = "twin.device_state.saved"
	EventAlarmSaved       = "twin.alarm.saved"
)

// twinEvent is the stable JSON envelope published onto TF_TWIN_EVENTS. Field
// names are TB camelCase to match the API contract; ts is epoch MILLIS.
type twinEvent struct {
	TenantID   string                 `json:"tenantId"`
	EntityType string                 `json:"entityType"`
	EntityID   string                 `json:"entityId"`
	Type       string                 `json:"type"`
	TS         int64                  `json:"ts"`
	Payload    map[string]interface{} `json:"payload"`
}

var (
	initOnce sync.Once
	nc       *nats.Conn
	subject  string
)

// publishFn is the injectable event sink, mirroring the broadcastTelemetry
// function-var pattern in twin_state.go: production defaults to the real NATS
// path (publishViaNATS) and tests replace it with a spy that captures calls
// without a NATS server. Publish delegates through it so injected spies
// observe the same guard behavior as production.
var publishFn = publishViaNATS

// InitPublisher connects the twin-event NATS publisher exactly once
// (sync.Once). If NATS is unreachable the publisher is disabled (nc stays
// nil) and every Publish becomes a no-op — the stores remain the source of
// truth and the journal is best-effort (R4). Boot is never blocked or
// panicked on a publisher failure, mirroring usage.InitPublisher.
func InitPublisher(url, baseSubject string) {
	initOnce.Do(func() {
		subject = basePrefix(baseSubject)
		c, err := nats.Connect(url, nats.Name("thingsflow-twinevents"), nats.Timeout(5*time.Second))
		if err != nil {
			log.Printf("WARN twin-events NATS publisher disabled: %v", err)
			return
		}
		nc = c
	})
}

// Publish fire-and-forgets a twin event onto
// tf.twin.events.<tenant>.<type>.<id> (entity type lowercased for a stable
// subject namespace). It is a no-op when the publisher is disabled (NATS
// unreachable at init) and skips empty tenant/entity identities (defensive).
// Errors are logged WARN and never returned — the stores are the source of
// truth and the journal feeds fan-out/search/audit, not correctness.
func Publish(tenantID, entityType, entityID, eventType string, payload map[string]interface{}) {
	if tenantID == "" || entityID == "" {
		return
	}
	if nc == nil {
		return
	}
	publishFn(tenantID, entityType, entityID, eventType, payload)
}

// publishViaNATS is the production event sink. It composes the per-event
// subject, marshals the stable envelope, and nc.Publish fire-and-forgets it
// (best-effort WARN on error, never retried, never blocks).
func publishViaNATS(tenantID, entityType, entityID, eventType string, payload map[string]interface{}) {
	eventSubject := composeSubject(subject, tenantID, entityType, entityID)
	ev := composeEvent(tenantID, entityType, entityID, eventType, payload, time.Now().UnixMilli())
	b, err := json.Marshal(ev)
	if err != nil {
		log.Printf("WARN twin-events marshal %s/%s/%s: %v", tenantID, entityType, entityID, err)
		return
	}
	if err := nc.Publish(eventSubject, b); err != nil {
		log.Printf("WARN twin-events publish %s/%s/%s: %v", tenantID, entityType, entityID, err)
	}
}

// basePrefix strips the trailing JetStream wildcard ("tf.twin.events.>" ->
// "tf.twin.events") so the per-event subject can append the entity triple.
// A blank/empty base falls back to the canonical prefix.
func basePrefix(baseSubject string) string {
	prefix := strings.TrimSuffix(baseSubject, ">")
	prefix = strings.TrimSuffix(prefix, ".")
	if strings.TrimSpace(prefix) == "" {
		return "tf.twin.events"
	}
	return prefix
}

// composeSubject builds tf.twin.events.<tenant>.<type>.<id> with the entity
// type lowercased for a stable subject namespace.
func composeSubject(base, tenantID, entityType, entityID string) string {
	return fmt.Sprintf("%s.%s.%s.%s", base, tenantID, strings.ToLower(entityType), entityID)
}

// composeEvent builds the stable JSON envelope. It is a pure helper so unit
// tests can pin the envelope fields and JSON shape without a NATS server.
func composeEvent(tenantID, entityType, entityID, eventType string, payload map[string]interface{}, ts int64) twinEvent {
	return twinEvent{
		TenantID:   tenantID,
		EntityType: entityType,
		EntityID:   entityID,
		Type:       eventType,
		TS:         ts,
		Payload:    payload,
	}
}
