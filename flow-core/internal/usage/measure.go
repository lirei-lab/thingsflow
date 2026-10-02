package usage

import (
	"log"
	"os"
	"strings"
	"sync"
	"time"
)

// StoredRowsThisHour counts rows written to the telemetry store since the top of
// the hour. Injected at startup (main.go) from internal/telemetry, which owns
// the table and timestamp-column knowledge — a direct import would couple this
// package to the read path it only needs one number from.
var StoredRowsThisHour func() (int64, error)

// The four families below cannot be counted inside flow-core: telemetry never
// passes through it. The edge posts to Envoy/Bento http-ingest, which publishes
// to NATS, and Bento consumers write to GreptimeDB — the control plane never
// sees a transport message. RecordTransportMessage and RecordRuleEngineExecution
// stay for the flow-core-hosted transport endpoints, which no deployment
// currently uses; they contribute nothing here.
//
// So the numbers are read back from where the traffic actually is: the store for
// rows, the stream for messages, the alarm consumer for evaluations.
//
// Indirection through vars so the measurement can be exercised without a live
// NATS or a live store.
var (
	streamLastSeq      = jetStreamLastSeq
	consumerDelivered  = jetStreamConsumerDelivered
	storedRowsThisHour = func() (int64, error) {
		if StoredRowsThisHour == nil {
			return 0, errNotWired
		}
		return StoredRowsThisHour()
	}
)

type notWiredError struct{}

func (notWiredError) Error() string { return "measurement source not wired" }

var errNotWired = notWiredError{}

// hourBaseline remembers where the monotonic sequences stood at the top of the
// current hour, because a JetStream sequence counts since the stream was
// created, not since the hour began.
//
// It lives in memory, next to the counters resetHourly already owns. A restart
// re-baselines, so the hour in progress reads low until the next boundary. That
// is a deliberate trade against persisting state for a dashboard counter, and it
// is visible rather than hidden: the alternative, carrying the absolute sequence
// forward, would show a number that is wrong by the stream's whole history.
type hourBaseline struct {
	mu      sync.Mutex
	hour    time.Time
	msgSeq  uint64
	ruleSeq uint64
	primed  bool
}

var baseline hourBaseline

// since returns how far the sequences have advanced inside the current hour,
// re-baselining when the hour turns over.
func (b *hourBaseline) since(nowHour time.Time, msgSeq, ruleSeq uint64) (uint64, uint64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.primed || !b.hour.Equal(nowHour) {
		b.hour, b.msgSeq, b.ruleSeq, b.primed = nowHour, msgSeq, ruleSeq, true
		return 0, 0
	}
	// A stream that was purged or recreated can move backwards. Re-baseline
	// rather than report a negative count wrapped into a huge unsigned one.
	if msgSeq < b.msgSeq || ruleSeq < b.ruleSeq {
		b.msgSeq, b.ruleSeq = msgSeq, ruleSeq
		return 0, 0
	}
	return msgSeq - b.msgSeq, ruleSeq - b.ruleSeq
}

func env(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

func jetStreamLastSeq(stream string) (uint64, error) {
	if nc == nil {
		return 0, errNotWired
	}
	js, err := nc.JetStream()
	if err != nil {
		return 0, err
	}
	info, err := js.StreamInfo(stream)
	if err != nil {
		return 0, err
	}
	return info.State.LastSeq, nil
}

func jetStreamConsumerDelivered(stream, durable string) (uint64, error) {
	if nc == nil {
		return 0, errNotWired
	}
	js, err := nc.JetStream()
	if err != nil {
		return 0, err
	}
	info, err := js.ConsumerInfo(stream, durable)
	if err != nil {
		return 0, err
	}
	return info.Delivered.Consumer, nil
}

// dataPlanePoints returns the families measured outside flow-core, omitting any
// it could not measure.
//
// Omission is the point. These keys used to be published as hardcoded zeros, and
// a zero on that dashboard reads as "measured, and nothing happened" — which was
// false every minute since the panel existed. An absent key reads as "not
// measured", which is true when the store or the stream cannot be reached.
func dataPlanePoints(now time.Time) map[string]int64 {
	points := map[string]int64{}

	if rows, err := storedRowsThisHour(); err == nil {
		// One ingested data point becomes one stored row, so the transport and
		// storage families share the count. They are separate keys in the
		// ThingsBoard contract, not separate measurements here.
		points["storageDataPointsCount"] = rows
		points["storageDataPointsCountHourly"] = rows
		points["transportDataPointsCount"] = rows
		points["transportDataPointsCountHourly"] = rows
	} else if err != errNotWired {
		log.Printf("WARN usage: telemetry store count unavailable, omitting data-point counters: %v", err)
	}

	rawStream := env("NATS_RAW_STREAM", "TF_RAW")
	alarmDurable := env("ALARM_MATERIALIZER_DURABLE", "thingsflow-alarms-durable")

	msgSeq, msgErr := streamLastSeq(rawStream)
	ruleSeq, ruleErr := consumerDelivered(rawStream, alarmDurable)
	if msgErr != nil || ruleErr != nil {
		if msgErr != errNotWired && ruleErr != errNotWired {
			log.Printf("WARN usage: NATS counters unavailable, omitting message/rule counters: msg=%v rule=%v", msgErr, ruleErr)
		}
		return points
	}

	msgs, rules := baseline.since(now.Truncate(time.Hour), msgSeq, ruleSeq)
	points["transportMsgCount"] = int64(msgs)
	points["transportMsgCountHourly"] = int64(msgs)
	// One delivery to the alarm consumer is one threshold evaluation.
	points["ruleEngineExecutionCount"] = int64(rules)
	points["ruleEngineExecutionCountHourly"] = int64(rules)
	return points
}
