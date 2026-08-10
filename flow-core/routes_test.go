package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// Go's ServeMux panics at registration when two patterns overlap without one
// being strictly more specific — for example a literal `/api/x/types` alongside
// `GET /api/x/{id}`. That panic happens while the process is starting, so the
// symptom is a pod that never becomes ready rather than a failing request: a
// total outage caused by adding one route.
//
// Building the mux in a test is what makes that a failing build instead. It is
// the safety net for migrating the prefix routers to method-aware patterns,
// where every overlapping registration in a subtree has to change together.
func TestRegisterRoutes_NoPatternConflicts(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("route registration panicked — the service would not start: %v", r)
		}
	}()
	registerRoutes(http.NewServeMux(), "*")
}

// Routing is asserted by where a request lands, not by reading the table. These
// cover the shapes that have actually gone wrong here: a subtree router that
// handled one verb and let every other method fall through to an empty 200,
// which is how 34 endpoints were silently unimplemented.
func TestRegisterRoutes_DispatchesKnownPaths(t *testing.T) {
	mux := http.NewServeMux()
	registerRoutes(mux, "*")

	cases := []struct {
		method, path string
		// notFound means the mux has no handler at all for this path. Anything
		// else — 401, 405, 500 — proves a handler was reached, which is what is
		// being asserted here; the handlers' own behaviour is tested elsewhere.
		wantRouted bool
	}{
		{"GET", "/api/auth/user", true},
		{"POST", "/api/auth/login", true},
		{"GET", "/api/tenant/devices", true},
		{"GET", "/api/entityView/00000000-0000-4000-8000-000000000000", true},
		{"DELETE", "/api/entityView/00000000-0000-4000-8000-000000000000", true},
		{"GET", "/api/entityView/info/00000000-0000-4000-8000-000000000000", true},
		{"GET", "/api/entityView/types", true},
		{"GET", "/api/customer/00000000-0000-4000-8000-000000000000/deviceInfos", true},
		{"GET", "/api/dashboard/info/00000000-0000-4000-8000-000000000000", true},
		// Shapes the contract gate caught missing after the subtree→pattern
		// migration: bare dimensions, one- and three-segment events, and the
		// 405-not-404 endpoints. Each of these shipped broken once.
		{"GET", "/api/audit/logs/customer", true},
		{"GET", "/api/audit/logs/user", true},
		{"GET", "/api/audit/logs/entity/00000000-0000-4000-8000-000000000000", true},
		{"GET", "/api/calculatedField/00000000-0000-4000-8000-000000000000/debug", true},
		{"POST", "/api/events/00000000-0000-4000-8000-000000000000", true},
		{"POST", "/api/events/00000000-0000-4000-8000-000000000000/00000000-0000-4000-8000-000000000000/clear", true},
		{"POST", "/api/widgetsBundle", true},
		// R6 policy catalog + twin enforcement routes dispatch to their own
		// patterns, never the /api/ catch-all.
		{"GET", "/api/policies", true},
		{"POST", "/api/policies", true},
		{"GET", "/api/policies/owner/1.0.0", true},
		{"DELETE", "/api/policies/owner/1.0.0", true},
		{"GET", "/api/twins", true},
		{"GET", "/api/twins/DEVICE/00000000-0000-4000-8000-000000000000", true},
		{"PUT", "/api/twins/DEVICE/00000000-0000-4000-8000-000000000000/features", true},
		{"GET", "/health", true},
		{"GET", "/metrics", true},
		{"GET", "/definitely/not/a/route", false},
	}

	for _, tc := range cases {
		_, pattern := mux.Handler(httptest.NewRequest(tc.method, tc.path, nil))
		// The /api/ catch-all matches every API path, so "some pattern
		// matched" is not evidence of routing — landing in the catch-all is
		// exactly the miss this test exists to detect.
		routed := pattern != "" && pattern != "/api/"
		if routed != tc.wantRouted {
			t.Errorf("%s %s: routed=%v (pattern %q), want routed=%v",
				tc.method, tc.path, routed, pattern, tc.wantRouted)
		}
	}
}
