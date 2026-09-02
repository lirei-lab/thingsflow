package usage

import (
	"encoding/json"
	"log"
	"sync"

	"github.com/nats-io/nats.go"

	"flow-core/internal/natsutil"
)

// entityPoint is the flat envelope flow-core publishes to the entity
// telemetry subject. The 03-01 Bento materializer consumes exactly these
// snake_case fields and turns each into one InfluxDB Line Protocol line.
// TS is epoch MILLIS — Bento appends "000000" to reach nanoseconds.
type entityPoint struct {
	TenantID     string `json:"tenant_id"`
	EntityType   string `json:"entity_type"`
	EntityID     string `json:"entity_id"`
	TelemetryKey string `json:"telemetry_key"`
	ValueString  string `json:"value_string"`
	ValueKind    string `json:"value_kind"`
	TS           int64  `json:"ts"` // epoch millis
}

var (
	pubOnce sync.Once
	nc      *nats.Conn
	subject string
)

// InitPublisher connects the usage NATS publisher exactly once (sync.Once),
// at StartReporter time. If NATS is unreachable the publisher is disabled
// (nc stays nil) and usage telemetry is best-effort dropped — the same
// posture as the old ignored `_, _ = Exec(...)` errors. Boot is never
// blocked or panicked on a publisher failure.
func InitPublisher(url, subj string) {
	pubOnce.Do(func() {
		subject = subj
		c, err := natsutil.Connect(url, "thingsflow-usage-reporter")
		if err != nil {
			log.Printf("WARN usage NATS publisher disabled: %v", err)
			return
		}
		nc = c
	})
}

// publishEntityPoint marshals the envelope and fire-and-forgets it onto the
// configured subject. It is a no-op when the publisher is disabled (nc nil)
// and never blocks or retries — usage is low-rate, forward-only, best-effort.
func publishEntityPoint(tenantID, entityType, entityID, key, valueString, kind string, tsMillis int64) {
	if nc == nil {
		return
	}
	b, err := json.Marshal(entityPoint{
		TenantID:     tenantID,
		EntityType:   entityType,
		EntityID:     entityID,
		TelemetryKey: key,
		ValueString:  valueString,
		ValueKind:    kind,
		TS:           tsMillis,
	})
	if err != nil {
		log.Printf("WARN usage marshal %s/%s: %v", entityID, key, err)
		return
	}
	if err := nc.Publish(subject, b); err != nil {
		log.Printf("WARN usage publish %s/%s: %v", entityID, key, err)
	}
}
