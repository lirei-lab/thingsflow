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
	last := map[string]twinstore.State{}
	for {
		select {
		case <-ctx.Done():
			return
		case change, ok := <-changes:
			if !ok {
				return
			}
			old := change.Old
			if old.EntityID == "" {
				old = last[twinstore.Key(change.New.EntityType, change.New.TenantID, change.New.EntityID)]
			}
			last[twinstore.Key(change.New.EntityType, change.New.TenantID, change.New.EntityID)] = change.New
			changed, ts := changedTelemetry(old, change.New)
			if len(changed) > 0 {
				ws.BroadcastTelemetry(change.New.EntityID, changed, ts)
			}
		}
	}
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
