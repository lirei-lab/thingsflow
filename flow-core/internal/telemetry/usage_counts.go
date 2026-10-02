package telemetry

import (
	"errors"
	"fmt"
)

// ErrStoreUnavailable means the telemetry store is not configured or not
// reachable, so the caller has no count — as distinct from a count of zero.
var ErrStoreUnavailable = errors.New("telemetry store unavailable")

// StoredRowsThisHour counts the telemetry rows written since the top of the
// current hour.
//
// It lives here rather than in internal/usage because the table name and the
// timestamp column are this package's knowledge: the column differs between
// GreptimeDB (greptime_timestamp) and the QuestDB path (timestamp), and a
// caller that hardcoded either would silently count nothing on the other.
//
// The hour boundary comes from the database rather than the process clock, so
// the window matches the rows' own timestamps even if the two disagree.
func StoredRowsThisHour() (int64, error) {
	if PG == nil {
		return 0, ErrStoreUnavailable
	}
	query := fmt.Sprintf(
		"SELECT count(*) FROM device_telemetry_kv WHERE %s >= date_trunc('hour', now())",
		telemetryKVTimestampColumn(),
	)
	var n int64
	if err := PG.QueryRow(query).Scan(&n); err != nil {
		return 0, err
	}
	return n, nil
}
