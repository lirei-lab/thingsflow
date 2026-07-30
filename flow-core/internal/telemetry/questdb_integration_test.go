package telemetry

import (
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	_ "github.com/lib/pq"
)

// The QuestDB read path is the escape hatch from GreptimeDB: TELEMETRY_HISTORY_STORE
// selects it and four branches in this file diverge on it. Until these tests existed
// nothing exercised those branches, so the option looked available while being
// entirely unverified — the worst state to discover during an incident, which is
// exactly when you would reach for it.
//
// These run against a real QuestDB (CI service container, or locally via
// FLOW_TEST_QUESTDB_DSN) because the divergence is dialect-level — SAMPLE BY vs
// date_bin(), a differently-named timestamp column — and a mock would assert our
// own assumptions rather than QuestDB's behaviour.

const questTable = `CREATE TABLE IF NOT EXISTS device_telemetry_kv (
	tenant_id SYMBOL, device_id SYMBOL, telemetry_key SYMBOL,
	value_string STRING, value_kind SYMBOL, timestamp TIMESTAMP
) TIMESTAMP(timestamp) PARTITION BY DAY`

func newQuestDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("FLOW_TEST_QUESTDB_DSN")
	if dsn == "" {
		t.Skip("FLOW_TEST_QUESTDB_DSN not set")
	}
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := db.Ping(); err != nil {
		t.Fatalf("ping: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	if _, err := db.Exec(`DROP TABLE IF EXISTS device_telemetry_kv`); err != nil {
		t.Fatalf("drop: %v", err)
	}
	if _, err := db.Exec(questTable); err != nil {
		t.Fatalf("create: %v", err)
	}
	rows := []struct {
		ts  string
		val string
	}{
		{"2026-07-28T00:00:00.000000Z", "100.0"},
		{"2026-07-28T00:30:00.000000Z", "200.0"},
		{"2026-07-28T01:00:00.000000Z", "300.0"},
	}
	for _, r := range rows {
		// Literals, not placeholders: QuestDB's pg-wire rejects bind parameters on
		// this INSERT form ("got 2 parameters but the statement requires 0"). The
		// values are test-local constants, so there is nothing to inject.
		if _, err := db.Exec(fmt.Sprintf(
			`INSERT INTO device_telemetry_kv VALUES ('t1','dev-1','power','%s','number','%s')`,
			r.val, r.ts)); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
	// QuestDB commits through the WAL asynchronously, so a read immediately after
	// the insert legitimately sees zero rows. Poll rather than sleep a fixed time.
	deadline := time.Now().Add(15 * time.Second)
	for {
		var n int
		if err := db.QueryRow(`SELECT count(*) FROM device_telemetry_kv`).Scan(&n); err == nil && n == len(rows) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("seeded rows never became visible (WAL commit timed out)")
		}
		time.Sleep(200 * time.Millisecond)
	}
	return db
}

// useQuestDB points the package-level reader at the container for one test.
func useQuestDB(t *testing.T, db *sql.DB) {
	t.Helper()
	t.Setenv("TELEMETRY_HISTORY_STORE", "questdb")
	prev := PG
	PG = db
	t.Cleanup(func() { PG = prev })
}

func TestQuestDB_RawQueryRunsAndReturnsPoints(t *testing.T) {
	db := newQuestDB(t)
	useQuestDB(t, db)

	if col := telemetryKVTimestampColumn(); col != "timestamp" {
		t.Fatalf("timestamp column = %q, want %q for the QuestDB profile", col, "timestamp")
	}

	q := questDBKVRawQuery("dev-1", "power",
		"2026-07-27T00:00:00.000000Z", "2026-07-29T00:00:00.000000Z", "ASC", 100)
	rows, err := db.Query(q)
	if err != nil {
		t.Fatalf("raw query rejected by QuestDB: %v\nquery: %s", err, q)
	}
	defer rows.Close()

	n := 0
	for rows.Next() {
		var ts time.Time
		var raw, kind string
		if err := rows.Scan(&ts, &raw, &kind); err != nil {
			t.Fatalf("scan: %v (projection drifted from what the reader expects)", err)
		}
		n++
	}
	if n != 3 {
		t.Fatalf("got %d points, want 3", n)
	}
}

// SAMPLE BY is the QuestDB-only branch: GreptimeDB has no such clause, so this
// query shape is never exercised by the default profile.
func TestQuestDB_SampleByAggregationBuckets(t *testing.T) {
	db := newQuestDB(t)
	useQuestDB(t, db)

	q := questDBKVAggQuery("dev-1", "power",
		"2026-07-27T00:00:00.000000Z", "2026-07-29T00:00:00.000000Z", "ASC", 100,
		mapAggFunction("AVG"), msToSampleBy(3600000))
	rows, err := db.Query(q)
	if err != nil {
		t.Fatalf("SAMPLE BY query rejected by QuestDB: %v\nquery: %s", err, q)
	}
	defer rows.Close()

	got := map[int64]string{}
	for rows.Next() {
		var ts time.Time
		var raw, kind string
		if err := rows.Scan(&ts, &raw, &kind); err != nil {
			t.Fatalf("scan: %v", err)
		}
		got[ts.UnixMilli()] = raw
	}
	// 00:00 and 00:30 average to 150 in the first hourly bucket; 01:00 stands alone.
	if len(got) != 2 {
		t.Fatalf("got %d buckets, want 2 hourly buckets: %v", len(got), got)
	}
	var first string
	for _, ts := range sortedKeys(got) {
		first = got[ts]
		break
	}
	if first != "150.0" {
		t.Fatalf("first bucket = %q, want the average 150.0 of the two samples in it", first)
	}
}

func sortedKeys(m map[int64]string) []int64 {
	out := make([]int64, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j] < out[j-1]; j-- {
			out[j], out[j-1] = out[j-1], out[j]
		}
	}
	return out
}

// msToSampleBy translates the TB `interval` param into QuestDB's SAMPLE BY unit.
// A wrong unit silently changes the bucket width — the dashboard still renders,
// just with the wrong numbers — so pin it.
func TestMsToSampleBy_Units(t *testing.T) {
	cases := map[int64]string{
		1000:     "1s",
		60000:    "1m",
		900000:   "15m",
		3600000:  "1h",
		86400000: "1d",
	}
	for ms, want := range cases {
		if got := msToSampleBy(ms); got != want {
			t.Errorf("msToSampleBy(%d) = %q, want %q", ms, got, want)
		}
	}
}
