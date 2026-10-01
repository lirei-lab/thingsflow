package telemetry

import "testing"

// The default decides where the "Utilisation de l'API" dashboard reads from, and
// the wrong default is not a cosmetic preference: "postgres" points every
// non-device read at ts_kv, a table this codebase never writes (internal/usage
// publishes to NATS despite its persist* naming, and no INSERT INTO ts_kv exists
// anywhere). That default shipped to production and the dashboard rendered empty
// charts until it was found. Pin it so it cannot drift back.
func TestUsageReadBackendDefaultsToGreptime(t *testing.T) {
	t.Setenv("USAGE_READ_BACKEND", "")
	if got := usageReadBackend(); got != "greptime" {
		t.Fatalf("usageReadBackend() = %q with the variable unset, want %q — "+
			"a postgres default reads ts_kv, which nothing writes", got, "greptime")
	}
}

// The old branch stays reachable for a deployment that does have its own ts_kv
// writer, so the flip is a choice rather than a removal.
func TestUsageReadBackendHonoursExplicitPostgres(t *testing.T) {
	t.Setenv("USAGE_READ_BACKEND", "postgres")
	if got := usageReadBackend(); got != "postgres" {
		t.Fatalf("usageReadBackend() = %q, want %q", got, "postgres")
	}
}

// Operators type what they type; the accessor already lowercases and trims, and
// the exported sibling must agree with it or entityquery and ws would gate
// differently from the reader.
func TestUsageReadBackendNormalisesAndMatchesExportedAccessor(t *testing.T) {
	t.Setenv("USAGE_READ_BACKEND", "  GrepTime \t")
	if got := usageReadBackend(); got != "greptime" {
		t.Fatalf("usageReadBackend() = %q, want %q", got, "greptime")
	}
	if UsageReadBackend() != usageReadBackend() {
		t.Fatalf("UsageReadBackend() = %q disagrees with usageReadBackend() = %q",
			UsageReadBackend(), usageReadBackend())
	}
}
