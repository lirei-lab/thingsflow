package main

import (
	"context"
	"log"
	"os"
	"reflect"
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
	if store == nil || os.Getenv("TWIN_STATE_WATCH_ENABLED") == "false" {
		return
	}
	changes, err := store.Watch(ctx, "")
	if err != nil {
		log.Printf("WARN twin state watch failed: %v", err)
		return
	}
	watchTwinStateChanges(ctx, changes)
}

// watchTwinStateChanges consumes KV change events and pushes telemetry and
// attribute diffs to the WS plane. Split from startTwinStateWatch so tests can
// establish the Watch subscription synchronously before driving writes.
func watchTwinStateChanges(ctx context.Context, changes <-chan twinstore.Change) {
	last := map[string]twinstore.State{}
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
	old := change.Old
	if old.EntityID == "" {
		// NATS watch events carry no Old (twinstore/nats.go emits
		// Change{New: …} only) — reconstruct it from what we last saw.
		old = last[key]
	}
	changedTelem, ts := changedTelemetry(old, change.New)
	changedAttrs := changedAttributes(old, change.New)
	last[key] = mergeLastState(last[key], change.New)
	return changedTelem, ts, changedAttrs
}

// mergeLastState folds next into prev per key/scope (see applyTwinChange for
// why this must merge, not replace). prev's maps are owned exclusively by the
// watch loop's `last` map, so in-place writes are safe; diffs are computed
// BEFORE the fold.
func mergeLastState(prev, next twinstore.State) twinstore.State {
	if prev.EntityID == "" {
		return next
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
