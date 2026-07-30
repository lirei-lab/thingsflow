package telemetry

import "testing"

// The TSDB connection env was historically named QUESTDB_PG_*, which now reads as
// "QuestDB is in use" on a GreptimeDB-only deployment where QuestDB is not even
// deployed. These tests pin the rename's contract: new name wins, old name still
// works, and neither one silently loses to the default.
func TestTsdbEnv_PrefersNeutralName(t *testing.T) {
	t.Setenv("TSDB_PG_HOST", "greptime.internal")
	t.Setenv("QUESTDB_PG_HOST", "stale.legacy")
	if got := tsdbEnv("HOST", "fallback"); got != "greptime.internal" {
		t.Fatalf("tsdbEnv() = %q, want the TSDB_PG_ value to win", got)
	}
}

// An existing deployment carries only the legacy names in its ConfigMap. A Helm
// upgrade and a pod roll are not atomic, so the fallback is what keeps that
// deployment reading telemetry across the rename.
func TestTsdbEnv_FallsBackToLegacyName(t *testing.T) {
	t.Setenv("QUESTDB_PG_PORT", "8812")
	if got := tsdbEnv("PORT", "4003"); got != "8812" {
		t.Fatalf("tsdbEnv() = %q, want the legacy QUESTDB_PG_ value", got)
	}
}

func TestTsdbEnv_DefaultWhenUnset(t *testing.T) {
	if got := tsdbEnv("DB", "public"); got != "public" {
		t.Fatalf("tsdbEnv() = %q, want the default", got)
	}
}

// An empty value must not shadow the legacy name: the chart emits
// TSDB_PG_USER: "" for GreptimeDB (which needs no user), and treating that as
// "set" would break a deployment whose credentials live under the old names.
func TestTsdbEnv_EmptyNeutralValueDoesNotShadowLegacy(t *testing.T) {
	t.Setenv("TSDB_PG_USER", "")
	t.Setenv("QUESTDB_PG_USER", "admin")
	if got := tsdbEnv("USER", ""); got != "admin" {
		t.Fatalf("tsdbEnv() = %q, want the legacy value to be used when the new one is empty", got)
	}
}
