package ws

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"

	"github.com/nats-io/nats.go"

	"flow-core/internal/twinevents"
)

// StartJournalConsumer subscribes to TF_TWIN_EVENTS (tf.twin.events.>) and
// routes twin/attribute/relation events into the WS broadcasters so a change
// published by ANY replica reaches EVERY replica's subscribers (R4 multi-réplica
// fan-out). Resilient to channel close (resubscribe with backoff, never crash
// the WS plane); graceful no-op when NATS is unreachable.
//
// Fan-out decision (replace vs complement — evidence in the plan SUMMARY):
// COMPLEMENT. The three in-process broadcasters (BroadcastTelemetry,
// BroadcastAttributes, BroadcastAlarmEvent) iterate sessionManager.sessions —
// the LOCAL process session map — so an in-process broadcast on replica A only
// ever reaches A's subscribers. This consumer is the authoritative cross-replica
// channel for the control-plane event classes: every replica subscribes to the
// durable journal and routes each event into ITS OWN local broadcasters, which
// remain the delivery mechanism. The KV watch (flow-core/twin_state.go) is
// preserved as the single live-attribute publisher for the KV-derived path
// (data-plane telemetry re-broadcast + KV-bucket attribute diffs) — the journal
// is the ONLY cross-replica WS path for the event classes that never touch the
// twin_state KV bucket (device_state, alarm, relation), and the authoritative
// event-driven path for the rest.
//
// Queue-group choice: NONE. Sessions are local to a replica, so every replica
// must process every event to deliver it to ITS OWN subscribers; a queue group
// would hand each event to exactly one replica and silently starve the others
// (see journalQueueGroup).
func StartJournalConsumer(ctx context.Context, url, subject string) {
	nc, err := nats.Connect(url, nats.Name("thingsflow-flow-core-ws"), nats.Timeout(5*time.Second))
	if err != nil {
		// Mirror the 04-01 publisher posture: NATS unreachable at boot disables
		// the consumer and leaves the WS plane exactly as it was — in-process
		// broadcasts and the KV watch keep working. Never block or panic boot.
		log.Printf("WARN ws journal consumer disabled (NATS unreachable): %v", err)
		return
	}
	go func() {
		<-ctx.Done()
		nc.Close()
	}()
	log.Printf("ws journal consumer active: subject=%s", subject)
	go runJournalResubscribe(ctx, nc, subject)
}

// journalQueueGroup is deliberately empty: every replica consumes the full
// journal and fans each event out to its own local sessions (see the file
// comment on StartJournalConsumer). A non-empty queue group would deliver each
// event to exactly one replica — fine for once-per-cluster work, wrong for WS
// fan-out where subscribers are spread across replicas. Named constant so the
// choice is explicit and test-pinnable.
const journalQueueGroup = ""

// journalSubscribeFn is the subscription provider. Production subscribes to the
// journal subject on the real NATS connection (no queue group — every replica
// consumes the full journal) and returns a channel that closes when the
// subscription dies (connection close, server-side removal), mirroring the KV
// watch's `Watch` channel. Var so tests can inject a fake channel and drive
// the resubscribe loop without a NATS server (mirrors twinWatchRetryBase and
// the broadcastTelemetry function-var pattern).
var journalSubscribeFn = func(ctx context.Context, nc *nats.Conn, subject string) (<-chan struct{}, error) {
	done := make(chan struct{})
	var closeOnce sync.Once
	sub, err := nc.Subscribe(subject, func(m *nats.Msg) {
		routeJournalMessage(m)
	})
	if err != nil {
		return nil, err
	}
	// The closed handler fires when the subscription dies (connection close,
	// server-side removal); closing `done` wakes the resubscribe loop — the
	// same channel-close contract runTwinStateWatch relies on from Watch().
	sub.SetClosedHandler(func(subject string) {
		closeOnce.Do(func() { close(done) })
	})
	return done, nil
}

// journalRetryBase seeds the resubscribe backoff. Var so tests can shrink it
// to keep the resubscribe test fast (mirrors twinWatchRetryBase).
var journalRetryBase = time.Second

// Broadcasters as vars so journal_test.go can spy on them without real WS
// sessions — the same function-var pattern as broadcastTelemetry /
// broadcastAttributes in flow-core/twin_state.go. Routing goes through the
// vars, so a swapped spy observes the exact same dispatch as production.
var (
	journalBroadcastTelemetry  = BroadcastTelemetry
	journalBroadcastAttributes = BroadcastAttributes
	journalBroadcastAlarm      = BroadcastAlarmEvent
)

// journalEvent is the decoded TF_TWIN_EVENTS envelope. Field names mirror the
// 04-01 publisher's stable JSON envelope (twinevents/twinEvent): TB camelCase,
// ts in epoch MILLIS. The struct is local because twinevents' twinEvent is
// unexported; the JSON shape is the contract, so decoding into a local struct
// with identical tags is safe and keeps this package free of twinevents internals.
type journalEvent struct {
	TenantID   string                 `json:"tenantId"`
	EntityType string                 `json:"entityType"`
	EntityID   string                 `json:"entityId"`
	Type       string                 `json:"type"`
	TS         int64                  `json:"ts"`
	Payload    map[string]interface{} `json:"payload"`
}

// runJournalResubscribe keeps the journal subscription alive for the life of
// the process, mirroring runTwinStateWatch: a closed subscription channel
// (consumer death, server restart) must not permanently kill WS fan-out, so we
// resubscribe with exponential backoff that only grows while subscribes keep
// FAILING and resets on a successful subscribe. Context-aware and panic-free —
// a journal failure must never take down the WS plane.
func runJournalResubscribe(ctx context.Context, nc *nats.Conn, subject string) {
	maxBackoff := 30 * time.Second
	backoff := journalRetryBase
	for {
		if ctx.Err() != nil {
			return
		}
		done, err := journalSubscribeFn(ctx, nc, subject)
		if err != nil {
			log.Printf("ERROR ws journal subscribe failed (retrying in %s): %v", backoff, err)
		} else {
			select {
			case <-ctx.Done():
				return
			case <-done:
				log.Printf("ERROR ws journal subscription closed; resubscribing in %s", backoff)
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		if err != nil {
			if backoff *= 2; backoff > maxBackoff {
				backoff = maxBackoff
			}
		} else {
			backoff = journalRetryBase
		}
	}
}

// routeJournalMessage decodes a single TF_TWIN_EVENTS message and dispatches it
// by event type. Empty and malformed envelopes are logged and skipped — a bad
// event must never crash the WS plane (defensive: the publisher is best-effort
// and stores are the source of truth, so a dropped event is never a data-loss
// event). Event-type routing is forward compatible: unknown types are skipped.
func routeJournalMessage(m *nats.Msg) {
	if len(m.Data) == 0 {
		log.Printf("DEBUG ws journal: empty envelope on subject %q skipped", m.Subject)
		return
	}
	var ev journalEvent
	if err := json.Unmarshal(m.Data, &ev); err != nil {
		log.Printf("WARN ws journal: invalid envelope on subject %q: %v", m.Subject, err)
		return
	}
	routeJournalEvent(ev)
}

// routeJournalEvent dispatches a decoded event to the broadcasters. The routing
// table is pinned by the 04-01 publisher's payload shapes (evidence: internal/
// tenant/attributes.go SaveAttributesKV, flow-core/device_state.go
// SaveServerAttribute, internal/alarmmaterializer/postgres.go, internal/twin/
// write.go) and is documented in the plan SUMMARY.
func routeJournalEvent(ev journalEvent) {
	if ev.EntityID == "" {
		log.Printf("DEBUG ws journal: event with empty entityId skipped (type=%q)", ev.Type)
		return
	}
	switch ev.Type {
	case twinevents.EventAttributeSaved:
		// payload: {"scope": "SERVER_SCOPE", "values": {...}} from SaveAttributesKV.
		// The watch ALSO fans this out (SaveAttributesKV -> MergeAttributes -> KV
		// diff on every replica); the second push here is the accepted complement
		// cost — idempotent for TB widgets, bounded by control-plane write rate.
		scope, _ := ev.Payload["scope"].(string)
		values, _ := ev.Payload["values"].(map[string]interface{})
		if scope == "" || values == nil {
			log.Printf("WARN ws journal: %s missing scope/values in payload; skipped", ev.Type)
			return
		}
		journalBroadcastAttributes(ev.EntityID, scope, values)
	case twinevents.EventFeatureSaved:
		// payload is the raw features object {name: {properties: {...}}}.
		// Features PERSIST as flat SERVER_SCOPE attribute keys
		// (feature.<name>.<property>, twin/write.go flattens before
		// SaveAttributesKV), and WS feature subscribers register those flat
		// keys — so flatten here to the same key namespace the attribute event
		// (emitted by the same write) delivers. Broadcasting the raw object
		// with top-level feature-name keys would never match a feature.*
		// subscription and the fan-out would be a silent no-op. The journal
		// payload itself stays raw (future audit/search consumers want the
		// document); flattening happens at the WS fan-out boundary only.
		flat := flattenFeatures(ev.Payload)
		if len(flat) == 0 {
			log.Printf("DEBUG ws journal: %s had no flattenable feature properties; skipped", ev.Type)
			return
		}
		journalBroadcastAttributes(ev.EntityID, "SERVER_SCOPE", flat)
	case twinevents.EventDeviceStateSaved:
		// payload: {"key": ..., "value": ...} — a timestamped scalar state tick
		// (active/lastConnectTime/...) from the previously-silent device_state
		// writer. The telemetry broadcaster carries [ts, value] pairs, which is
		// exactly this shape; the event's own ts is the sample time. Alternative
		// (BroadcastAttributes SERVER_SCOPE) documented in the SUMMARY — chosen
		// BroadcastTelemetry so device-state ticks surface in live telemetry.
		key, _ := ev.Payload["key"].(string)
		if key == "" {
			log.Printf("WARN ws journal: %s missing key in payload; skipped", ev.Type)
			return
		}
		journalBroadcastTelemetry(ev.EntityID, map[string]interface{}{key: ev.Payload["value"]}, ev.TS)
	case twinevents.EventAlarmSaved:
		// entityId is the alarm's DEVICE originator, which is exactly how WS
		// AlarmSubs are keyed (ws.go handleCmd alarmSubCmds). Note: one alarm
		// create emits two events (insert + link, 04-01 handoff) — subscribers
		// tolerate the duplicate (same-value re-render), matching the existing
		// double-broadcast behaviour of BroadcastAlarmEvent itself.
		journalBroadcastAlarm(ev.EntityID, ev.Payload)
	case twinevents.EventRelationSaved:
		// No WS broadcaster exists for relations today — they surface via the
		// twin API and topology REST, not the WS plane (documented in the plan
		// SUMMARY; relations are not an R4 WS fan-out target). Log and skip so
		// the event remains observable for the future search/audit consumers.
		log.Printf("DEBUG ws journal: %s has no WS broadcaster; relation entity=%s skipped", ev.Type, ev.EntityID)
	default:
		// Forward compatible: an event type added by a newer publisher must not
		// crash or spam — log and skip.
		log.Printf("DEBUG ws journal: unknown event type %q skipped (forward compatible)", ev.Type)
	}
}

// flattenFeatures mirrors twin/write.go's feature persistence: a raw features
// object {name: {properties: {key: value}}} becomes the flat SERVER_SCOPE
// attribute namespace feature.<name>.<key> = value — the exact keys WS feature
// subscribers register and the attribute event (from the same write) delivers.
// Non-map or property-less entries are dropped defensively; a malformed feature
// never crashes the fan-out.
func flattenFeatures(features map[string]interface{}) map[string]interface{} {
	flat := map[string]interface{}{}
	for name, raw := range features {
		fm, _ := raw.(map[string]interface{})
		props, _ := fm["properties"].(map[string]interface{})
		for key, value := range props {
			flat["feature."+name+"."+key] = value
		}
	}
	return flat
}
