package main

import (
	"context"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"flow-core/internal/asset"
	"flow-core/internal/audit"
	authpkg "flow-core/internal/auth"
	"flow-core/internal/bootstrap"
	dbpkg "flow-core/internal/db"
	"flow-core/internal/device"
	"flow-core/internal/devicejwt"
	"flow-core/internal/metrics"
	"flow-core/internal/provisioning"
	"flow-core/internal/rpc"
	"flow-core/internal/system"
	"flow-core/internal/topology"
	"flow-core/internal/transport"
	"flow-core/internal/twin"
	"flow-core/internal/twinevents"
	"flow-core/internal/usage"
	"flow-core/internal/ws"
)

// initLogging configures slog as the default logger and routes the std
// `log` package through it. LOG_FORMAT=json (default in production)
// emits structured JSON; LOG_FORMAT=text keeps the human-readable format
// used during local development. LOG_LEVEL controls verbosity (debug,
// info, warn, error).
//
// Effect on existing log.Printf call sites: their output now flows
// through slog with level=INFO and the message preserved verbatim. New
// code should call slog.Info / slog.Warn / slog.Error directly with
// structured fields for grep-able production logs.
func initLogging() {
	level := slog.LevelInfo
	switch strings.ToLower(os.Getenv("LOG_LEVEL")) {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	}
	opts := &slog.HandlerOptions{Level: level}
	var handler slog.Handler
	if strings.ToLower(os.Getenv("LOG_FORMAT")) == "text" {
		handler = slog.NewTextHandler(os.Stderr, opts)
	} else {
		handler = slog.NewJSONHandler(os.Stderr, opts)
	}
	logger := slog.New(handler)
	slog.SetDefault(logger)
	// Route std log.Printf through slog at info level — every call site
	// in the codebase now emits structured logs without code churn.
	log.SetFlags(0)
	log.SetOutput(slogWriter{logger: logger})
}

// slogWriter is the io.Writer that std log writes into; it forwards each
// line as a slog.Info call. Naive level inference from "WARN"/"ERROR"
// prefixes preserves the existing log convention for severities.
type slogWriter struct{ logger *slog.Logger }

func (s slogWriter) Write(p []byte) (int, error) {
	msg := strings.TrimRight(string(p), "\n")
	switch {
	case strings.HasPrefix(msg, "ERROR") || strings.Contains(msg, "ERROR "):
		s.logger.Error(msg)
	case strings.HasPrefix(msg, "WARN") || strings.Contains(msg, "WARN "):
		s.logger.Warn(msg)
	case strings.HasPrefix(msg, "DEBUG") || strings.Contains(msg, "DEBUG: "):
		s.logger.Debug(msg)
	default:
		s.logger.Info(msg)
	}
	return len(p), nil
}

// preflightCheck validates production-critical environment variables
// before any subsystem boots. Operators get a clear error log + non-zero
// exit instead of a half-started bridge that fails opaquely 30s later.
//
// Production deploys MUST set: SPRING_DATASOURCE_URL,
// SPRING_DATASOURCE_PASSWORD, JWT_TOKEN_SIGNING_KEY, ALLOWED_ORIGIN, and a TSDB
// endpoint (TSDB_HOST, TSDB_PG_DSN, or the legacy QUESTDB_HOST). Helm values
// guard security before boot; this runtime preflight is the second line of
// defense.
// uses a hard-coded dev default; a wildcard ALLOWED_ORIGIN with browser
// credentials is a CORS footgun.
func preflightCheck() {
	prodMode := strings.EqualFold(os.Getenv("FLOW_ENV"), "production")
	if !prodMode {
		return
	}
	var missing []string
	required := []string{
		"SPRING_DATASOURCE_URL",
		"SPRING_DATASOURCE_PASSWORD",
		"JWT_TOKEN_SIGNING_KEY",
	}
	for _, key := range required {
		if os.Getenv(key) == "" {
			missing = append(missing, key)
		}
	}
	// The TSDB endpoint may be given under either the backend-neutral name or the
	// legacy QuestDB-era one; requiring the legacy name specifically would fail a
	// correctly-configured GreptimeDB deploy that already migrated.
	if os.Getenv("TSDB_HOST") == "" && os.Getenv("QUESTDB_HOST") == "" && os.Getenv("TSDB_PG_DSN") == "" {
		missing = append(missing, "TSDB_HOST (or TSDB_PG_DSN)")
	}
	if origin := os.Getenv("ALLOWED_ORIGIN"); origin == "" || origin == "*" {
		slog.Error("ALLOWED_ORIGIN is empty or '*' in production — CORS will not enforce origin checks; set to the UI's exact origin")
		missing = append(missing, "ALLOWED_ORIGIN (must be a specific origin in production)")
	}
	if len(missing) > 0 {
		slog.Error("preflight: missing required environment variables in FLOW_ENV=production",
			"missing", strings.Join(missing, ", "))
		os.Exit(2)
	}
	slog.Info("preflight: all required env vars present, FLOW_ENV=production")
}

func main() {
	initLogging()
	preflightCheck()

	InitPostgres()
	authpkg.InitConfig()
	if err := devicejwt.InitFromEnv(); err != nil {
		slog.Error("device JWT issuer initialization failed", "error", err)
		os.Exit(2)
	}
	InitQuestDBReader()
	initTwinStateStore(ctx)
	transport.FetchAttributes = fetchAttributes

	// Twin registry hooks: every device/asset create path upserts its registry
	// row and both delete paths reclaim it (twin_registry has no FK/cascade).
	// Sibling domains never import internal/twin — func-var injection at boot,
	// same pattern as transport.FetchAttributes above. Failures log and never
	// break the CRUD path itself: the boot backfill below re-converges.
	syncTwinRegistry := func(tenantID, entityType, entityID string) {
		syncCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := twin.SyncRegistryRow(syncCtx, dbpkg.Pool, tenantID, entityType, entityID); err != nil {
			log.Printf("WARN twin registry sync %s %s: %v", entityType, entityID, err)
		}
	}
	deleteTwinRegistry := func(tenantID, entityType, entityID string) {
		delCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := twin.DeleteRegistryRow(delCtx, dbpkg.Pool, tenantID, entityType, entityID); err != nil {
			log.Printf("WARN twin registry delete %s %s: %v", entityType, entityID, err)
		}
	}
	device.TwinRegistrySync = func(tenantID, deviceID string) { syncTwinRegistry(tenantID, "DEVICE", deviceID) }
	device.TwinRegistryDelete = func(tenantID, deviceID string) { deleteTwinRegistry(tenantID, "DEVICE", deviceID) }
	provisioning.TwinRegistrySync = device.TwinRegistrySync
	asset.TwinRegistrySync = func(tenantID, assetID string) { syncTwinRegistry(tenantID, "ASSET", assetID) }
	asset.TwinRegistryDelete = func(tenantID, assetID string) { deleteTwinRegistry(tenantID, "ASSET", assetID) }
	system.AssetTwinRegistrySync = asset.TwinRegistrySync
	bootstrap.TwinRegistrySync = syncTwinRegistry

	// Server-to-device RPC. The lookup hook keeps internal/rpc free of a device
	// package import; the listener owns its own NATS connection so RPC replies do
	// not depend on the twin-state connection staying up.
	rpc.DeviceLookup = lookupDeviceForRPC
	rpc.StartResponseListener(ctx, getEnv("NATS_URL", ""))

	// Postgres pool saturation — early warning for connection exhaustion.
	// `wait_count` rising means handlers are blocked waiting for a free
	// conn → bump PG_MAX_OPEN_CONNS or investigate a slow query.
	metrics.RegisterGauge("flow_postgres_pool_open",
		"Postgres connections currently open",
		func() float64 {
			if dbpkg.Pool == nil {
				return 0
			}
			return float64(dbpkg.Pool.Stats().OpenConnections)
		})
	metrics.RegisterGauge("flow_postgres_pool_in_use",
		"Postgres connections actively in use by a query",
		func() float64 {
			if dbpkg.Pool == nil {
				return 0
			}
			return float64(dbpkg.Pool.Stats().InUse)
		})
	metrics.RegisterGauge("flow_postgres_pool_wait_count",
		"Cumulative waits for a free postgres connection (saturation signal)",
		func() float64 {
			if dbpkg.Pool == nil {
				return 0
			}
			return float64(dbpkg.Pool.Stats().WaitCount)
		})
	var topologyMetricsMu sync.Mutex
	var topologyMetricsReport topology.ConsistencyReport
	var topologyMetricsAt time.Time
	var topologyMetricsOK bool
	sampleTopology := func() (topology.ConsistencyReport, bool) {
		topologyMetricsMu.Lock()
		defer topologyMetricsMu.Unlock()
		if topologyMetricsOK && time.Since(topologyMetricsAt) < 5*time.Second {
			return topologyMetricsReport, true
		}
		ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
		defer cancel()
		report, err := topology.CheckConsistencyContext(ctx, dbpkg.Pool)
		if err != nil {
			log.Printf("WARN topology metrics sample failed: %v", err)
			topologyMetricsOK = false
			return topology.ConsistencyReport{}, false
		}
		topologyMetricsReport = report
		topologyMetricsAt = time.Now()
		topologyMetricsOK = true
		return report, true
	}
	metrics.RegisterGauge("flow_topology_edges_total",
		"Modern topology edges stored in topology_edge",
		func() float64 {
			r, ok := sampleTopology()
			if !ok {
				return -1
			}
			return float64(r.TopologyEdges)
		})
	metrics.RegisterGauge("flow_topology_legacy_missing_total",
		"Governed legacy relations that have not been backfilled to topology_edge",
		func() float64 {
			r, ok := sampleTopology()
			if !ok {
				return -1
			}
			return float64(r.LegacyMissingTopology)
		})
	metrics.RegisterGauge("flow_topology_modern_missing_legacy_total",
		"Modern topology edges missing their ThingsBoard legacy relation mirror",
		func() float64 {
			r, ok := sampleTopology()
			if !ok {
				return -1
			}
			return float64(r.TopologyMissingLegacy)
		})
	metrics.RegisterGauge("flow_topology_cross_tenant_legacy_total",
		"Legacy relation rows whose resolvable endpoints belong to different tenants",
		func() float64 {
			r, ok := sampleTopology()
			if !ok {
				return -1
			}
			return float64(r.CrossTenantLegacyRelations)
		})
	audit.StartPartitionManager()

	log.Printf("flow-core NATS/twin-state mode active; telemetry hot path is external")
	go startHTTPServer()
	go func() {
		// Non-critical catalogue/demo/bootstrap maintenance must never block
		// the control-plane HTTP listener. On an already-seeded cluster these
		// calls are fast; if Postgres is slow or locked, readiness still exposes
		// the service state and the work can finish in the background.
		bootstrap.LoadSystem()
		bootstrap.LoadDemo()
		repairCtx, repairCancel := context.WithTimeout(ctx, 30*time.Second)
		defer repairCancel()
		if result, err := topology.RepairBackfillContext(repairCtx, dbpkg.Pool, false); err != nil {
			log.Printf("WARN topology backfill repair failed: %v", err)
		} else if result.Inserted > 0 {
			log.Printf("topology backfill repair inserted %d missing edge(s)", result.Inserted)
		}
		// Twin registry convergence: upsert any device/asset created while the
		// hooks were not live (older builds, direct SQL) and sweep orphans left
		// by deletes the hook missed. Log-not-fatal, same posture as the
		// topology repair above.
		registryCtx, registryCancel := context.WithTimeout(ctx, 30*time.Second)
		defer registryCancel()
		if converged, err := twin.BackfillRegistryContext(registryCtx, dbpkg.Pool); err != nil {
			log.Printf("WARN twin registry backfill failed: %v", err)
		} else if converged > 0 {
			log.Printf("twin registry backfill converged %d row(s) (upserts + orphan sweep)", converged)
		}
	}()
	go StartInactivityMonitor()
	usage.InitPublisher(getEnv("NATS_URL", ""), getEnv("ENTITY_TELEMETRY_SUBJECT", "tf.entity.telemetry.raw.events"))
	usage.StartReporter()

	// Twin event journal (R4): fire-and-forget publisher for control-plane
	// twin/attribute/relation writes. Subject base is the TF_TWIN_EVENTS
	// wildcard; the per-event subject is composed at publish time. An empty
	// NATS_URL disables the publisher gracefully (usage publisher posture).
	twinevents.InitPublisher(getEnv("NATS_URL", ""), "tf.twin.events.>")

	// WS multi-réplica fan-out (R4): every flow-core replica consumes the
	// TF_TWIN_EVENTS journal and fans control-plane twin/attribute/relation/
	// alarm events out to ITS OWN WS subscribers, so a change published by any
	// replica reaches every replica's subscribers. The consumer owns its NATS
	// connection, resubscribes with backoff, and is a graceful no-op when NATS
	// is unreachable — gated on NATS_URL so an intentionally-empty URL (local
	// non-NATS dev) logs no misleading WARN.
	if journalURL := getEnv("NATS_URL", ""); journalURL != "" {
		ws.StartJournalConsumer(ctx, journalURL, "tf.twin.events.>")
	}

	// Trap SIGTERM/SIGINT and cancel the root ctx so the HTTP server,
	// background monitors, and any goroutine derived from it shut down cleanly.
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		s := <-sig
		log.Printf("received %s, starting graceful shutdown", s)
		cancelCtx()
	}()

	log.Printf("flow-core running as control plane only")
	<-ctx.Done()
	log.Println("flow-core control plane stopped")
}
