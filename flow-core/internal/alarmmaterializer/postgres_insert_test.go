package alarmmaterializer

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"testing"
	"time"

	_ "github.com/lib/pq"
)

// These run InsertAlarm's SQL against a real Postgres.
//
// The existing tests drive the processor through a mocked repository, so the
// statement text itself is never executed — a malformed query passes every one
// of them and fails only in production. That is exactly what happened: an
// INSERT that kept its VALUES clause *and* gained a SELECT built, vetted and
// unit-tested cleanly, then failed on the first real intent with
// `syntax error at or near "SELECT"`.

func insertTestDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("FLOW_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("FLOW_TEST_PG_DSN not set")
	}
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := db.Ping(); err != nil {
		t.Fatalf("ping: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	for _, stmt := range []string{
		`DROP TABLE IF EXISTS alarm CASCADE`,
		`DROP TABLE IF EXISTS device CASCADE`,
		`CREATE TABLE device (id uuid PRIMARY KEY, tenant_id uuid, name text)`,
		`CREATE TABLE alarm (
			id uuid PRIMARY KEY, created_time bigint, start_ts bigint, end_ts bigint,
			type text, severity text, originator_id uuid, originator_type int,
			tenant_id uuid, customer_id uuid,
			acknowledged boolean, cleared boolean,
			propagate boolean, propagate_to_owner boolean, propagate_to_tenant boolean,
			additional_info text,
			ack_ts bigint, clear_ts bigint, assign_ts bigint)`,
	} {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("schema: %v", err)
		}
	}
	return db
}

const (
	liveTenant = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
	liveDevice = "dddddddd-dddd-dddd-dddd-dddddddddddd"
	goneDevice = "eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee"
)

func record(deviceID string) AlarmRecord {
	return AlarmRecord{
		ID:             "11111111-1111-1111-1111-111111111111",
		TenantID:       liveTenant,
		DeviceID:       deviceID,
		AlarmType:      "DeviceSilent",
		Severity:       "MAJOR",
		CreatedTime:    time.Now().UnixMilli(),
		StartTS:        time.Now().UnixMilli(),
		EndTS:          time.Now().UnixMilli(),
		AdditionalInfo: "{}",
	}
}

func TestInsertAlarm_WritesRowForLiveDevice(t *testing.T) {
	db := insertTestDB(t)
	if _, err := db.Exec(
		`INSERT INTO device (id, tenant_id, name) VALUES ($1,$2,'meter')`,
		liveDevice, liveTenant); err != nil {
		t.Fatalf("seed: %v", err)
	}
	repo := PostgresRepository{DB: db}
	if err := repo.InsertAlarm(context.Background(), record(liveDevice)); err != nil {
		t.Fatalf("InsertAlarm: %v", err)
	}
	var n int
	if err := db.QueryRow(`SELECT count(*) FROM alarm WHERE originator_id = $1`, liveDevice).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 1 {
		t.Fatalf("rows = %d, want 1", n)
	}
}

// Telemetry outlives the device row, so a timeseries-derived intent can name a
// device Postgres no longer has. Writing that alarm is noise, and alarm noise is
// what teaches operators to stop reading alarms.
func TestInsertAlarm_SkipsDeletedDevice(t *testing.T) {
	db := insertTestDB(t)
	repo := PostgresRepository{DB: db}
	err := repo.InsertAlarm(context.Background(), record(goneDevice))
	if !errors.Is(err, ErrOriginatorMissing) {
		t.Fatalf("err = %v, want ErrOriginatorMissing", err)
	}
	var n int
	_ = db.QueryRow(`SELECT count(*) FROM alarm`).Scan(&n)
	if n != 0 {
		t.Fatalf("rows = %d, want 0 — an alarm was written for a device that does not exist", n)
	}
}

// A device belonging to another tenant must not be alarmable either: the guard
// checks ownership, not just existence.
func TestInsertAlarm_SkipsDeviceOfAnotherTenant(t *testing.T) {
	db := insertTestDB(t)
	if _, err := db.Exec(
		`INSERT INTO device (id, tenant_id, name) VALUES ($1,'bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb','meter')`,
		liveDevice); err != nil {
		t.Fatalf("seed: %v", err)
	}
	repo := PostgresRepository{DB: db}
	if err := repo.InsertAlarm(context.Background(), record(liveDevice)); !errors.Is(err, ErrOriginatorMissing) {
		t.Fatalf("err = %v, want ErrOriginatorMissing", err)
	}
}
