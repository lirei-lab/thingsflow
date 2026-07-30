package main

import (
	"context"
	"os"
	"sync"
	"time"
)

var (
	// ctx is cancelled by main() on SIGTERM/SIGINT. Long-lived goroutines
	// should derive from it so graceful shutdown propagates.
	ctx, cancelCtx = context.WithCancel(context.Background())

	// TB Auth cache
	tbJwtToken  string
	tbJwtExpire time.Time
	tbAuthMutex sync.Mutex
)

func getEnv(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return fallback
}
