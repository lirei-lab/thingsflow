package deviceactivity

import (
	"context"
	"os"
	"strconv"
	"time"

	"flow-core/internal/twinstore"
)

const defaultTimeout = 60 * time.Second

func Timeout() time.Duration {
	raw := os.Getenv("DEVICE_INACTIVITY_TIMEOUT_SECONDS")
	if raw == "" {
		return defaultTimeout
	}
	seconds, err := strconv.Atoi(raw)
	if err != nil || seconds <= 0 {
		return defaultTimeout
	}
	return time.Duration(seconds) * time.Second
}

// ActiveFromTwin derives the UI-facing device activity flag from the NATS KV
// hot twin state. This keeps Flow Core out of the telemetry write path while
// still letting the UI reflect live device state.
func ActiveFromTwin(ctx context.Context, tenantID, deviceID string, now time.Time) (bool, bool) {
	store := twinstore.Global()
	if store == nil || tenantID == "" || deviceID == "" {
		return false, false
	}
	values, err := store.GetLatestTelemetry(ctx, tenantID, "DEVICE", deviceID, nil)
	if err != nil || len(values) == 0 {
		return false, false
	}
	var latest int64
	for _, value := range values {
		if value.TS > latest {
			latest = value.TS
		}
	}
	if latest <= 0 {
		return false, false
	}
	return now.Sub(time.UnixMilli(latest)) <= Timeout(), true
}
