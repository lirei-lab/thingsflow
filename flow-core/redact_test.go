package main

import (
	"net/http/httptest"
	"testing"
)

// TestRedactDeviceToken — regression guard for H-2 in
// docs/SECURITY_AUDIT.md. /api/v1/{token}/* embeds the device access
// token in the URL path. Logging it raw means anyone with read access
// to bridge logs harvests every device token. The redactDeviceToken
// helper swaps the token segment for "<redacted>" while preserving
// the action segment (so lines stay grep-able by route).
func TestRedactDeviceToken(t *testing.T) {
	cases := []struct{ in, want string }{
		{"/api/v1/AbCdEf12345xYz/telemetry", "/api/v1/<redacted>/telemetry"},
		{"/api/v1/AbCdEf12345xYz/attributes", "/api/v1/<redacted>/attributes"},
		{"/api/v1/AbCdEf12345xYz/rpc/request/42", "/api/v1/<redacted>/rpc/request/42"},
		{"/api/v1/AbCdEf12345xYz", "/api/v1/<redacted>"},
		{"/api/v1/", "/api/v1/<redacted>"},
		{"/api/v1/provision", "/api/v1/provision"},
		// Non-/api/v1/ paths pass through unchanged
		{"/api/auth/login", "/api/auth/login"},
		{"/api/dashboard/abc", "/api/dashboard/abc"},
		{"/health", "/health"},
		{"", ""},
	}
	for _, c := range cases {
		if got := redactDeviceToken(c.in); got != c.want {
			t.Errorf("redactDeviceToken(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestIsExpectedNotFoundForDeviceLookup(t *testing.T) {
	req := httptest.NewRequest("GET", "/api/tenant/devices?deviceName=pilot-01", nil)
	if !isExpectedNotFound(req) {
		t.Fatal("device lookup miss should be treated as expected 404")
	}

	req = httptest.NewRequest("GET", "/api/device/missing", nil)
	if isExpectedNotFound(req) {
		t.Fatal("generic device 404 should remain a warning")
	}
}
