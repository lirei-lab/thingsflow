// Package audit owns flow-core's audit trail: best-effort writer with
// async buffering + drop-on-overflow, plus the monthly RANGE-partition
// manager that keeps the postgres `audit_log` table bounded.
//
// Memory: never extend Flow audit-rate writes without TTL. The QuestDB
// `flow_decisions` disk-fill incident grew that table to 400 GB
// unbounded — postgres `audit_log` is a different table but the
// discipline is the same. Retention defaults to 90 days and is
// overridable via AUDIT_LOG_RETENTION_DAYS.
package audit

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	dbpkg "flow-core/internal/db"
	"flow-core/internal/metrics"
	"github.com/google/uuid"
)

// Prometheus surface — registered once on package init.
var (
	metricEvents  = metrics.Counter("flow_audit_events_total", "Audit events accepted by Write (queued or sync)")
	metricDropped = metrics.Counter("flow_audit_dropped_total", "Audit events dropped because the async queue was full")
)

const (
	// DefaultRetentionDays is exported for tests + ops scripts that
	// want the same default as the runtime when AUDIT_LOG_RETENTION_DAYS
	// isn't set.
	DefaultRetentionDays = 90
	partitionPrefix      = "audit_log_"
)

// Event is a tenant-scoped audit record. Empty IDs land as NULL.
type Event struct {
	TenantID   string
	UserID     string
	UserName   string
	EntityID   string
	EntityType string
	EntityName string
	ActionType string // LOGIN, LOGOUT, ADDED, UPDATED, DELETED, ...
	ActionData string // JSON or stringified context, optional
	Status     string // SUCCESS or FAILURE; defaults to SUCCESS
	Failure    string // optional failure detail
}

// EntityChange is the one-liner most CRUD handlers want: "user X did
// <action> on entity <id> of type <type> in tenant Y". Pulls
// tenantId/userId/userName out of the JWT claims, fills the entity
// fields, and writes a SUCCESS row. Reach for Write() when you need
// FAILURE detail or non-standard fields.
func EntityChange(claims map[string]interface{}, entityType, entityID, entityName, action string) {
	tenantID, _ := claims["tenantId"].(string)
	userID, _ := claims["userId"].(string)
	userName, _ := claims["sub"].(string)
	Write(Event{
		TenantID:   tenantID,
		UserID:     userID,
		UserName:   userName,
		EntityID:   entityID,
		EntityType: entityType,
		EntityName: entityName,
		ActionType: action,
		Status:     "SUCCESS",
	})
}

// queue is a bounded buffered channel feeding the async writer goroutine.
// Bounded → drop-on-overflow rather than block the request path: a
// missed audit row is a known quality issue, but blocking a login
// behind a slow Postgres is a production outage.
//
// Set AUDIT_LOG_QUEUE_SIZE=0 to disable async (writes go inline; useful
// in tests where you want determinism, or when explicitly debugging).
var (
	queue chan Event
	once  sync.Once
	drops atomic.Uint64
)

func queueSize() int {
	if v := os.Getenv("AUDIT_LOG_QUEUE_SIZE"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return 1000
}

// startWriter spawns the consumer goroutine on first use. Idempotent.
func startWriter() {
	once.Do(func() {
		size := queueSize()
		if size <= 0 {
			return // async disabled — Write falls back to sync
		}
		queue = make(chan Event, size)
		go func() {
			for e := range queue {
				doWrite(e)
			}
		}()
		go func() {
			t := time.NewTicker(60 * time.Second)
			defer t.Stop()
			var last uint64
			for range t.C {
				cur := drops.Load()
				if cur > last {
					log.Printf("WARN audit_log: dropped %d event(s) in last 60s (queue full)", cur-last)
					last = cur
				}
			}
		}()
		log.Printf("audit writer: async, buffer=%d events", size)
	})
}

// Write enqueues an audit row for async insert. Errors during insert
// are logged inside doWrite; the caller never sees them. If the queue
// is full, the event is dropped and counted; a periodic log line
// surfaces drop bursts.
//
// Set AUDIT_LOG_QUEUE_SIZE=0 to force synchronous writes.
func Write(e Event) {
	if dbpkg.Pool == nil || e.TenantID == "" {
		return
	}
	if e.Status == "" {
		e.Status = "SUCCESS"
	}
	metricEvents.Inc()
	startWriter()
	if queue == nil {
		doWrite(e) // async disabled
		return
	}
	select {
	case queue <- e:
	default:
		drops.Add(1)
		metricDropped.Inc()
	}
}

func doWrite(e Event) {
	now := time.Now().UnixMilli()
	_, err := dbpkg.Pool.Exec(`
		INSERT INTO audit_log (
			id, created_time, tenant_id, customer_id, entity_id, entity_type, entity_name,
			user_id, user_name, action_type, action_data, action_status, action_failure_details
		)
		VALUES ($1, $2, $3, NULL, NULLIF($4,'')::uuid, NULLIF($5,''), NULLIF($6,''),
		        NULLIF($7,'')::uuid, NULLIF($8,''), $9, NULLIF($10,''), $11, NULLIF($12,''))`,
		uuid.New().String(), now, e.TenantID,
		e.EntityID, e.EntityType, e.EntityName,
		e.UserID, e.UserName, e.ActionType, e.ActionData, e.Status, e.Failure,
	)
	if err != nil {
		log.Printf("WARN audit_log insert: %v (tenant=%s action=%s)", err, e.TenantID, e.ActionType)
	}
}

// StartPartitionManager runs the partition sweep on boot, then daily.
// Without it, audit_log INSERTs fail outright (PARTITION BY RANGE table
// rejects writes when no partition covers the row's created_time).
func StartPartitionManager() {
	if err := EnsurePartitions(); err != nil {
		log.Printf("WARN audit partitions (initial): %v", err)
	}
	go func() {
		t := time.NewTicker(24 * time.Hour)
		defer t.Stop()
		for range t.C {
			if err := EnsurePartitions(); err != nil {
				log.Printf("WARN audit partitions (daily): %v", err)
			}
		}
	}()
}

// EnsurePartitions creates monthly partitions covering current + next 2
// months (idempotent via CREATE TABLE IF NOT EXISTS), then drops any
// partition whose upper bound is older than the retention window.
func EnsurePartitions() error {
	if dbpkg.Pool == nil {
		return fmt.Errorf("dbpkg.Pool nil")
	}
	now := time.Now().UTC()
	for i := 0; i < 3; i++ {
		if err := createMonthPartition(now.AddDate(0, i, 0)); err != nil {
			return err
		}
	}
	return dropPartitionsOlderThan(time.Duration(RetentionDays()) * 24 * time.Hour)
}

// RetentionDays reads AUDIT_LOG_RETENTION_DAYS or falls back to
// DefaultRetentionDays. Exported so test helpers can assert on it
// without duplicating the parse logic.
func RetentionDays() int {
	if v := os.Getenv("AUDIT_LOG_RETENTION_DAYS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return DefaultRetentionDays
}

func createMonthPartition(t time.Time) error {
	start := time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, time.UTC)
	end := start.AddDate(0, 1, 0)
	name := fmt.Sprintf("%s%04d_%02d", partitionPrefix, start.Year(), start.Month())
	stmt := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s
		PARTITION OF audit_log
		FOR VALUES FROM (%d) TO (%d)`,
		name, start.UnixMilli(), end.UnixMilli())
	if _, err := dbpkg.Pool.Exec(stmt); err != nil {
		return fmt.Errorf("create %s: %w", name, err)
	}
	return nil
}

func dropPartitionsOlderThan(age time.Duration) error {
	rows, err := dbpkg.Pool.Query(`
		SELECT inhrelid::regclass::text
		FROM pg_inherits
		WHERE inhparent = 'audit_log'::regclass`)
	if err != nil {
		return fmt.Errorf("list partitions: %w", err)
	}
	defer rows.Close()

	cutoff := time.Now().Add(-age).UTC()
	var dropped []string
	for rows.Next() {
		var qname string
		if err := rows.Scan(&qname); err != nil {
			continue
		}
		bare := qname
		if i := strings.LastIndexByte(bare, '.'); i >= 0 {
			bare = bare[i+1:]
		}
		bare = strings.Trim(bare, `"`)
		var year, month int
		if _, scanErr := fmt.Sscanf(bare, partitionPrefix+"%04d_%02d", &year, &month); scanErr != nil {
			continue
		}
		partEnd := time.Date(year, time.Month(month)+1, 1, 0, 0, 0, 0, time.UTC)
		if partEnd.Before(cutoff) {
			if _, err := dbpkg.Pool.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", qname)); err != nil {
				log.Printf("WARN drop %s: %v", qname, err)
				continue
			}
			dropped = append(dropped, bare)
		}
	}
	if len(dropped) > 0 {
		log.Printf("audit_log: dropped %d expired partition(s): %v", len(dropped), dropped)
	}
	return nil
}
