package audit

import (
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	dbpkg "flow-core/internal/db"
	_ "github.com/lib/pq"
)

// newTestDB opens FLOW_TEST_PG_DSN and points dbpkg.Pool at it. Tests
// skip cleanly when the env var is unset so the whole suite is harmless
// without docker available.
func newTestDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("FLOW_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("FLOW_TEST_PG_DSN not set")
	}
	pool, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	if err := pool.Ping(); err != nil {
		t.Fatalf("ping: %v", err)
	}
	dbpkg.SetPoolForTest(t, pool)
	t.Cleanup(func() { dbpkg.SetPoolForTest(t, nil); pool.Close() })
	return pool
}

func setupAuditLog(t *testing.T, db *sql.DB) {
	t.Helper()
	_, err := db.Exec(`
		DROP TABLE IF EXISTS audit_log CASCADE;
		CREATE TABLE audit_log (
			id            uuid NOT NULL,
			created_time  bigint NOT NULL,
			tenant_id     uuid,
			customer_id   uuid,
			entity_id     uuid,
			entity_type   varchar(255),
			entity_name   varchar(255),
			user_id       uuid,
			user_name     varchar(255),
			action_type   varchar(255),
			action_data   varchar,
			action_status varchar(255),
			action_failure_details varchar
		) PARTITION BY RANGE (created_time);
	`)
	if err != nil {
		t.Fatalf("create audit_log: %v", err)
	}
}

func TestEnsurePartitions_CreatesCurrentAndNextMonths(t *testing.T) {
	db := newTestDB(t)
	setupAuditLog(t, db)

	if err := EnsurePartitions(); err != nil {
		t.Fatalf("EnsurePartitions: %v", err)
	}

	var n int
	if err := db.QueryRow(`
		SELECT count(*) FROM pg_inherits WHERE inhparent = 'audit_log'::regclass
	`).Scan(&n); err != nil {
		t.Fatalf("count partitions: %v", err)
	}
	if n != 3 {
		t.Errorf("partition count = %d, want 3 (current + next 2)", n)
	}

	now := time.Now().UTC()
	for i := 0; i < 3; i++ {
		ts := now.AddDate(0, i, 0)
		want := fmt.Sprintf("audit_log_%04d_%02d", ts.Year(), ts.Month())
		var found int
		err := db.QueryRow(`
			SELECT count(*) FROM pg_inherits
			WHERE inhparent = 'audit_log'::regclass AND inhrelid::regclass::text = $1
		`, want).Scan(&found)
		if err != nil || found != 1 {
			t.Errorf("partition %s: found=%d err=%v", want, found, err)
		}
	}
}

func TestEnsurePartitions_DropsExpired(t *testing.T) {
	db := newTestDB(t)
	setupAuditLog(t, db)
	t.Setenv("AUDIT_LOG_RETENTION_DAYS", "30")

	year, month := time.Now().UTC().AddDate(-1, 0, 0).Year(), int(time.Now().UTC().AddDate(-1, 0, 0).Month())
	start := time.Date(year, time.Month(month), 1, 0, 0, 0, 0, time.UTC)
	end := start.AddDate(0, 1, 0)
	stmt := fmt.Sprintf(`CREATE TABLE audit_log_%04d_%02d PARTITION OF audit_log
		FOR VALUES FROM (%d) TO (%d)`, year, month, start.UnixMilli(), end.UnixMilli())
	if _, err := db.Exec(stmt); err != nil {
		t.Fatalf("create old partition: %v", err)
	}

	var before int
	db.QueryRow(`SELECT count(*) FROM pg_inherits WHERE inhparent = 'audit_log'::regclass`).Scan(&before)
	if before == 0 {
		t.Fatalf("setup failed — old partition not visible")
	}

	if err := EnsurePartitions(); err != nil {
		t.Fatalf("EnsurePartitions: %v", err)
	}

	var after int
	db.QueryRow(`SELECT count(*) FROM pg_inherits WHERE inhparent = 'audit_log'::regclass`).Scan(&after)
	if after != 3 {
		t.Errorf("after sweep: %d partitions, want 3 (current + next 2; old dropped)", after)
	}
}

func TestRetentionDays_Env(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		t.Setenv("AUDIT_LOG_RETENTION_DAYS", "")
		if got := RetentionDays(); got != DefaultRetentionDays {
			t.Errorf("default = %d, want %d", got, DefaultRetentionDays)
		}
	})
	t.Run("override", func(t *testing.T) {
		t.Setenv("AUDIT_LOG_RETENTION_DAYS", "180")
		if got := RetentionDays(); got != 180 {
			t.Errorf("override = %d", got)
		}
	})
}

func TestWrite_NilPoolIsNoOp(t *testing.T) {
	dbpkg.SetPoolForTest(t, nil)
	t.Setenv("AUDIT_LOG_QUEUE_SIZE", "0")

	Write(Event{TenantID: "11111111-1111-1111-1111-111111111111", ActionType: "LOGIN"})
	Write(Event{TenantID: ""}) // empty tenant → also a no-op
	// no panic = pass
}
