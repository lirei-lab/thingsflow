package main

import (
	"context"
	"log"
	"os"
	"reflect"
	"strconv"
	"time"

	"flow-core/internal/twinstore"
	"flow-core/internal/ws"
)

func initTwinStateStore(ctx context.Context) {
	mode := getEnv("TWIN_STATE_STORE", "")
	if mode == "" {
		return
	}
	switch mode {
	case "memory":
		twinstore.SetGlobal(twinstore.NewMemoryStore())
		log.Printf("twin state store initialized: memory")
		go startTwinStateWatch(ctx)
	case "nats":
		go connectNATSTwinStateStore(ctx)
	default:
		log.Printf("WARN unknown TWIN_STATE_STORE=%q; twin state store disabled", mode)
		return
	}
}

func connectNATSTwinStateStore(ctx context.Context) {
	url := getEnv("NATS_URL", "")
	bucket := getEnv("NATS_KV_BUCKET", "twin_state")
	for attempt := 1; ; attempt++ {
		nc, store, err := twinstore.ConnectNATS(url, bucket)
		if err == nil {
			twinstore.SetGlobal(store)
			log.Printf("NATS/twin-state mode active: Postgres latest telemetry is not authoritative")
			log.Printf("twin state store initialized: nats kv bucket=%s", bucket)
			go func() {
				<-ctx.Done()
				nc.Close()
			}()
			go startTwinStateWatch(ctx)
			return
		}
		log.Printf("WARN twin state store nats init attempt %d failed: %v", attempt, err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(5 * time.Second):
		}
	}
}

// broadcastTelemetry / broadcastAttributes are the watch loop's only outputs.
// Function variables so twin_state_test.go can count pushes without real WS
// sessions — the KV watch is the SINGLE publisher of attribute pushes (the
// direct REST-write broadcasts were removed in milestone 3 phase 1), so its
// emission behaviour needs to be pinnable in tests.
var (
	broadcastTelemetry  = ws.BroadcastTelemetry
	broadcastAttributes = ws.BroadcastAttributes
)

func startTwinStateWatch(ctx context.Context) {
	store := twinstore.Global()
	if store == nil {
		return
	}
	// TWIN_STATE_WATCH_ENABLED semantics: the watch is the SINGLE publisher
	// of live attribute pushes and re-broadcasts data-plane telemetry, so
	// setting it to "false" with a twin store configured means dashboards
	// stop receiving attribute updates entirely (REST writes still persist,
	// they just never reach WS subscribers). The flag exists only as an
	// operational kill switch for a broadcast storm — warn loudly so a
	// forgotten override cannot masquerade as "attributes are broken".
	if os.Getenv("TWIN_STATE_WATCH_ENABLED") == "false" {
		log.Printf("WARN twin state watch disabled (TWIN_STATE_WATCH_ENABLED=false) while a twin store is configured: live WS attribute pushes are disabled")
		return
	}
	runTwinStateWatch(ctx, store)
}

// twinWatchRetryBase seeds the resubscribe backoff. Var so tests can shrink it
// to keep the resubscribe test fast.
var twinWatchRetryBase = time.Second

// runTwinStateWatch keeps the KV watch alive for the life of the process.
// A NATS KV watcher's channel closes when its consumer dies (server restart,
// interest timeout); without this loop a single closure silently killed every
// future WS push until the next process restart. The `last` map survives
// resubscribes on purpose: it is only a diff base, so worst case a change
// missed during the gap re-broadcasts once — never a correctness issue.
func runTwinStateWatch(ctx context.Context, store twinstore.Store) {
	last := map[string]twinstore.State{}
	maxBackoff := 30 * time.Second
	backoff := twinWatchRetryBase
	for {
		if ctx.Err() != nil {
			return
		}
		changes, err := store.Watch(ctx, "")
		if err != nil {
			log.Printf("ERROR twin state watch subscribe failed (retrying in %s): %v", backoff, err)
		} else {
			watchTwinStateChanges(ctx, changes, last)
			if ctx.Err() != nil {
				return
			}
			log.Printf("ERROR twin state watch channel closed; resubscribing in %s", backoff)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		if err != nil {
			// Exponential backoff only while subscribes keep FAILING; a
			// successful subscription resets it so a healthy stream that
			// hiccups once resubscribes quickly.
			if backoff *= 2; backoff > maxBackoff {
				backoff = maxBackoff
			}
		} else {
			backoff = twinWatchRetryBase
		}
	}
}

// watchTwinStateChanges consumes KV change events and pushes telemetry and
// attribute diffs to the WS plane. Split from the retry loop so tests can
// establish the Watch subscription synchronously before driving writes.
// Returns when ctx is cancelled or the changes channel closes (the caller
// resubscribes); `last` is owned by the caller so it survives resubscribes.
func watchTwinStateChanges(ctx context.Context, changes <-chan twinstore.Change, last map[string]twinstore.State) {
	for {
		select {
		case <-ctx.Done():
			return
		case change, ok := <-changes:
			if !ok {
				return
			}
			changedTelem, ts, changedAttrs := applyTwinChange(last, change)
			if len(changedTelem) > 0 {
				broadcastTelemetry(change.New.EntityID, changedTelem, ts)
			}
			for scope, values := range changedAttrs {
				broadcastAttributes(change.New.EntityID, scope, values)
			}
		}
	}
}

// applyTwinChange computes the telemetry/attribute diffs one watch event
// produces and folds the event into the last-known-state map.
//
// The fold MERGES change.New into the stored state instead of replacing it.
// This is load-bearing: the data plane (Bento latest-kv, ~thousands of writes
// per second) produces per-key synthesized states — ONE telemetry key and
// EMPTY attributes (twinstore/nats.go Watch fallback). Replacing would let
// every such event wipe last.Attributes, so the next full-document write
// (e.g. any REST attribute save) would re-diff every attribute against
// nothing and re-broadcast the whole set to every subscriber — a broadcast
// storm proportional to data-plane write rate.
func applyTwinChange(last map[string]twinstore.State, change twinstore.Change) (map[string]interface{}, int64, map[string]map[string]interface{}) {
	key := twinstore.Key(change.New.EntityType, change.New.TenantID, change.New.EntityID)
	// Diff base: prefer what WE last observed over the event's own Old. On the
	// NATS path Old is always zero (twinstore/nats.go emits Change{New: …}), so
	// `last` is the only base there anyway; on the memory path preferring
	// `last` self-heals dropped Changes (MemoryStore.notify drops on a full
	// watch buffer) — a diff against a stale event-carried Old would silently
	// suppress keys whose real previous value we never saw. change.Old is only
	// trusted for the very first event of an entity, where we have nothing.
	old := last[key]
	if old.EntityID == "" {
		old = change.Old
	}
	changedTelem, ts := changedTelemetry(old, change.New)
	changedAttrs := changedAttributes(old, change.New)
	last[key] = mergeLastState(last[key], change.New)
	boundLastStates(last, key)
	return changedTelem, ts, changedAttrs
}

// twinWatchLastMax bounds the watch loop's `last` map ("no store grows
// unbounded" — the same rule that governs every telemetry store, see the
// QuestDB disk-fill incident). One State per live entity is cheap, but the map
// never forgets deleted entities, so a long-lived process on a churny tenant
// grows forever without a cap. Var (not const) so tests can shrink it.
var twinWatchLastMax = envIntDefault("TWIN_WATCH_LAST_MAX", 100000)

// boundLastStates evicts arbitrary entries (map iteration order) once the map
// exceeds twinWatchLastMax, sparing the just-updated key. Eviction is harmless
// by design: an evicted entity's next event simply re-broadcasts its full state
// once (no diff base), which subscribers already tolerate.
func boundLastStates(last map[string]twinstore.State, keep string) {
	if len(last) <= twinWatchLastMax {
		return
	}
	before := len(last)
	for key := range last {
		if key == keep {
			continue
		}
		delete(last, key)
		if len(last) <= twinWatchLastMax {
			break
		}
	}
	log.Printf("twin state watch: last-state map exceeded TWIN_WATCH_LAST_MAX=%d, evicted %d entries (now %d)",
		twinWatchLastMax, before-len(last), len(last))
}

func envIntDefault(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return def
}

// mergeLastState folds next into prev per key/scope (see applyTwinChange for
// why this must merge, not replace). In-place writes into prev's maps are safe
// ONLY because every map that enters `last` is cloned first (the first-event
// branch below) — next's maps may alias store-owned state (MemoryStore hands
// out clones today, but that is the store's implementation detail, not a
// contract), so adopting them uncloned would let later folds mutate state
// someone else still holds. Diffs are computed BEFORE the fold.
func mergeLastState(prev, next twinstore.State) twinstore.State {
	if prev.EntityID == "" {
		return cloneTwinState(next)
	}
	if next.UpdatedTS > prev.UpdatedTS {
		prev.UpdatedTS = next.UpdatedTS
	}
	if prev.Telemetry == nil && len(next.Telemetry) > 0 {
		prev.Telemetry = map[string]twinstore.Value{}
	}
	for key, value := range next.Telemetry {
		prev.Telemetry[key] = value
	}
	if prev.Attributes == nil && len(next.Attributes) > 0 {
		prev.Attributes = map[string]map[string]twinstore.Value{}
	}
	for scope, values := range next.Attributes {
		if prev.Attributes[scope] == nil {
			prev.Attributes[scope] = map[string]twinstore.Value{}
		}
		for key, value := range values {
			prev.Attributes[scope][key] = value
		}
	}
	return prev
}

// cloneTwinState deep-copies the maps the watch loop diffs against. Local
// (rather than exporting twinstore's cloneState) so the ownership guarantee of
// `last` is enforced at the point that needs it.
func cloneTwinState(state twinstore.State) twinstore.State {
	next := state
	next.Telemetry = make(map[string]twinstore.Value, len(state.Telemetry))
	for k, v := range state.Telemetry {
		next.Telemetry[k] = v
	}
	next.Attributes = make(map[string]map[string]twinstore.Value, len(state.Attributes))
	for scope, attrs := range state.Attributes {
		next.Attributes[scope] = make(map[string]twinstore.Value, len(attrs))
		for k, v := range attrs {
			next.Attributes[scope][k] = v
		}
	}
	next.Activity = make(map[string]interface{}, len(state.Activity))
	for k, v := range state.Activity {
		next.Activity[k] = v
	}
	return next
}

func changedTelemetry(old, next twinstore.State) (map[string]interface{}, int64) {
	changed := map[string]interface{}{}
	var maxTS int64
	for key, value := range next.Telemetry {
		oldValue, ok := old.Telemetry[key]
		if ok && oldValue.TS == value.TS && reflect.DeepEqual(oldValue.Value, value.Value) {
			continue
		}
		changed[key] = value.Value
		if value.TS > maxTS {
			maxTS = value.TS
		}
	}
	return changed, maxTS
}

// changedAttributes mirrors changedTelemetry per scope: a key counts as
// changed when it is new or when its (ts, value) pair moved. Returns
// scope → key → value, ready for one BroadcastAttributes call per scope.
func changedAttributes(old, next twinstore.State) map[string]map[string]interface{} {
	changed := map[string]map[string]interface{}{}
	for scope, values := range next.Attributes {
		for key, value := range values {
			oldValue, ok := old.Attributes[scope][key]
			if ok && oldValue.TS == value.TS && reflect.DeepEqual(oldValue.Value, value.Value) {
				continue
			}
			if changed[scope] == nil {
				changed[scope] = map[string]interface{}{}
			}
			changed[scope][key] = value.Value
		}
	}
	return changed
}
