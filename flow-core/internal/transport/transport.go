// Package transport hosts the read-only ThingsBoard-compatible HTTP device
// surface that still belongs in flow-core. Device-origin telemetry/attribute
// writes are handled by the dedicated HTTP ingest gateway: Envoy verifies the
// device JWT and Bento publishes directly to NATS.
package transport

import "os"

// FetchAttributes is wired by main.go to the package-main fetchAttributes
// helper that reads attribute_kv. The HTTP GET-attributes path needs it;
// kept as a function variable so this package doesn't pull in postgres
// directly.
var FetchAttributes func(deviceID string, attrType int, keys []string, dest map[string]interface{})

// getEnvDefault — local replacement for the package-main getEnv helper.
// Doesn't belong in httputil because it's a generic env lookup.
func getEnvDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
