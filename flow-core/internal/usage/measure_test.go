package usage

import (
	"errors"
	"testing"
	"time"
)

// stub replaces the measurement sources and restores them afterwards, so each
// test states exactly what the data plane is pretending to report.
func stub(t *testing.T, rows func() (int64, error), seq func(string) (uint64, error), cons func(string, string) (uint64, error)) {
	t.Helper()
	oldRows, oldSeq, oldCons := storedRowsThisHour, streamLastSeq, consumerDelivered
	storedRowsThisHour, streamLastSeq, consumerDelivered = rows, seq, cons
	baseline = hourBaseline{}
	t.Cleanup(func() {
		storedRowsThisHour, streamLastSeq, consumerDelivered = oldRows, oldSeq, oldCons
		baseline = hourBaseline{}
	})
}

func unwiredRows() (int64, error)                { return 0, errNotWired }
func unwiredSeq(string) (uint64, error)          { return 0, errNotWired }
func unwiredCons(string, string) (uint64, error) { return 0, errNotWired }

// The whole point of the change: a family that cannot be measured must be
// ABSENT, not zero. A zero on this dashboard reads as "measured, and nothing
// happened", which is what made the panel lie for as long as it existed.
func TestDataPlanePointsOmitsWhatItCannotMeasure(t *testing.T) {
	stub(t, unwiredRows, unwiredSeq, unwiredCons)
	got := dataPlanePoints(time.Now())
	if len(got) != 0 {
		t.Fatalf("dataPlanePoints() = %v, want no keys at all when nothing can be measured", got)
	}
}

// A store that errors is different from a store that is not wired, but the
// conclusion is the same: omit, never invent a zero.
func TestDataPlanePointsOmitsOnStoreError(t *testing.T) {
	stub(t, func() (int64, error) { return 0, errors.New("greptimedb unreachable") }, unwiredSeq, unwiredCons)
	if _, ok := dataPlanePoints(time.Now())["storageDataPointsCount"]; ok {
		t.Fatal("storageDataPointsCount present despite the store query failing")
	}
}

func TestDataPlanePointsReportsStoredRows(t *testing.T) {
	stub(t, func() (int64, error) { return 4242, nil }, unwiredSeq, unwiredCons)
	got := dataPlanePoints(time.Now())
	for _, k := range []string{
		"storageDataPointsCount", "storageDataPointsCountHourly",
		"transportDataPointsCount", "transportDataPointsCountHourly",
	} {
		if got[k] != 4242 {
			t.Errorf("%s = %d, want 4242", k, got[k])
		}
	}
}

// A JetStream sequence counts from the stream's creation, so the first reading
// of an hour establishes the baseline and must report nothing yet — publishing
// the absolute sequence would show the stream's entire history as this hour's
// traffic.
func TestSequenceCountersBaselineThenMeasureDelta(t *testing.T) {
	seq := uint64(1000)
	cons := uint64(500)
	stub(t, unwiredRows,
		func(string) (uint64, error) { return seq, nil },
		func(string, string) (uint64, error) { return cons, nil })

	now := time.Date(2026, 10, 2, 14, 30, 0, 0, time.UTC)
	first := dataPlanePoints(now)
	if first["transportMsgCount"] != 0 || first["ruleEngineExecutionCount"] != 0 {
		t.Fatalf("first reading of the hour = %v, want zeros while the baseline is taken", first)
	}

	seq, cons = 1150, 530
	second := dataPlanePoints(now.Add(time.Minute))
	if second["transportMsgCount"] != 150 {
		t.Errorf("transportMsgCount = %d, want 150", second["transportMsgCount"])
	}
	if second["ruleEngineExecutionCount"] != 30 {
		t.Errorf("ruleEngineExecutionCount = %d, want 30", second["ruleEngineExecutionCount"])
	}
}

func TestSequenceCountersRebaselineOnTheHour(t *testing.T) {
	seq, cons := uint64(1000), uint64(500)
	stub(t, unwiredRows,
		func(string) (uint64, error) { return seq, nil },
		func(string, string) (uint64, error) { return cons, nil })

	now := time.Date(2026, 10, 2, 14, 30, 0, 0, time.UTC)
	dataPlanePoints(now)
	seq, cons = 1900, 800
	if got := dataPlanePoints(now.Add(20 * time.Minute))["transportMsgCount"]; got != 900 {
		t.Fatalf("within the hour = %d, want 900", got)
	}

	// Crossing into the next hour restarts the count from the new baseline.
	if got := dataPlanePoints(now.Add(time.Hour))["transportMsgCount"]; got != 0 {
		t.Fatalf("first reading of the next hour = %d, want 0", got)
	}
	seq = 1975
	if got := dataPlanePoints(now.Add(time.Hour + time.Minute))["transportMsgCount"]; got != 75 {
		t.Fatalf("after the hour turned over = %d, want 75", got)
	}
}

// A purged or recreated stream moves the sequence backwards. Unsigned
// subtraction would wrap that into roughly eighteen quintillion.
func TestSequenceCountersSurviveAStreamGoingBackwards(t *testing.T) {
	seq, cons := uint64(5000), uint64(900)
	stub(t, unwiredRows,
		func(string) (uint64, error) { return seq, nil },
		func(string, string) (uint64, error) { return cons, nil })

	now := time.Date(2026, 10, 2, 9, 0, 0, 0, time.UTC)
	dataPlanePoints(now)
	seq, cons = 12, 3 // stream recreated
	got := dataPlanePoints(now.Add(time.Minute))
	if got["transportMsgCount"] != 0 {
		t.Fatalf("transportMsgCount = %d after the stream was recreated, want 0", got["transportMsgCount"])
	}
	seq = 60
	if g := dataPlanePoints(now.Add(2 * time.Minute))["transportMsgCount"]; g != 48 {
		t.Fatalf("after re-baselining = %d, want 48", g)
	}
}
