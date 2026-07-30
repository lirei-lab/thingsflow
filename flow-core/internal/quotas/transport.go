package quotas

import (
	"sync"
	"sync/atomic"
	"time"

	"flow-core/internal/metrics"
)

// Prometheus surface.
var (
	metricAllowed = metrics.Counter("flow_transport_allowed_total", "Transport messages accepted under per-tenant rate cap")
	metricDropped = metrics.Counter("flow_transport_dropped_total", "Transport messages rejected because tenant is over its per-minute cap")
)

// transport.go enforces a per-tenant per-minute ceiling on accepted
// transport messages. The TTLs in audit_log/ts_kv/device_telemetry
// bound retention; this bounds the ingest rate so a runaway device
// can't burn CPU + I/O proportionally even though the disk doesn't
// fill indefinitely.
//
// Algorithm: fixed-window counter per tenant. Window resets every
// minute on the wall clock so all tenants observe the same boundary.
// Memory: a sync.Map keyed by tenantId; inactive tenants stop ticking
// the counter but the map slot stays — negligible cost (one
// atomic.Int64 + one int64 timestamp per tenant).

type rateBucket struct {
	count       atomic.Int64 // messages accepted this window
	windowStart atomic.Int64 // unix-millis of the window's start
}

var (
	buckets   sync.Map // map[string]*rateBucket
	dropTotal atomic.Uint64
)

// AllowTransport returns true if the tenant is under its
// MaxTransportMessages-per-minute cap (or has no cap configured). It
// also charges 1 to the bucket as a side effect; callers must not
// invoke it twice per inbound message.
func AllowTransport(tenantID string) bool {
	if tenantID == "" {
		return true
	}
	limit := LimitsFor(tenantID).MaxTransportMessages
	if limit <= 0 {
		return true
	}

	now := time.Now().UnixMilli()
	windowMs := int64(60_000)
	currentWindow := (now / windowMs) * windowMs

	v, _ := buckets.LoadOrStore(tenantID, &rateBucket{})
	b := v.(*rateBucket)

	// Reset on window boundary; CAS so concurrent callers don't double-reset.
	for {
		oldStart := b.windowStart.Load()
		if oldStart == currentWindow {
			break
		}
		if b.windowStart.CompareAndSwap(oldStart, currentWindow) {
			b.count.Store(0)
			break
		}
	}

	new := b.count.Add(1)
	if new > limit {
		// Decrement to keep the counter at exactly $limit per window.
		b.count.Add(-1)
		dropTotal.Add(1)
		metricDropped.Inc()
		return false
	}
	metricAllowed.Inc()
	return true
}

// DropCount is exposed for tests + future /metrics surface.
func DropCount() uint64 {
	return dropTotal.Load()
}

// ResetForTest wipes a tenant's rate bucket. Test-only.
func ResetForTest(tenantID string) {
	buckets.Delete(tenantID)
}
