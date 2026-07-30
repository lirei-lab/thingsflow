package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

// TestDeclaredNoGoals_InSyncWithContract fails when nogoals_gen.go drifts from
// the contract JSON. The table is what the server *enforces*; the JSON is what
// the gate *verifies* — if they disagree, one of them is lying about scope.
//
// To regenerate the table after a contract change:
//
//	python3 - <<'EOF'
//	import json
//	entries = json.load(open('internal/uicontract/testdata/tb-ui-4.3.1.1.json'))['entries']
//	for m, p in sorted((e['method'], e['path'].replace('00000000-0000-4000-8000-000000000000','{id}'))
//	                   for e in entries if e['class'] == 'no-goal'):
//	    print(f'\t{{"{m}", "{p}"}},')
//	EOF
func TestDeclaredNoGoals_InSyncWithContract(t *testing.T) {
	raw, err := os.ReadFile("internal/uicontract/testdata/tb-ui-4.3.1.1.json")
	if err != nil {
		t.Fatalf("read contract: %v", err)
	}
	var contract struct {
		Entries []struct {
			Class  string `json:"class"`
			Method string `json:"method"`
			Path   string `json:"path"`
		} `json:"entries"`
	}
	if err := json.Unmarshal(raw, &contract); err != nil {
		t.Fatalf("parse contract: %v", err)
	}

	want := map[string]bool{}
	for _, e := range contract.Entries {
		if e.Class != "no-goal" {
			continue
		}
		pat := strings.ReplaceAll(e.Path, "00000000-0000-4000-8000-000000000000", "{id}")
		want[e.Method+" "+pat] = true
	}
	got := map[string]bool{}
	for _, ng := range declaredNoGoals {
		got[ng.method+" "+ng.pattern] = true
	}

	for k := range want {
		if !got[k] {
			t.Errorf("contract declares no-goal %q but nogoals_gen.go lacks it — regenerate", k)
		}
	}
	for k := range got {
		if !want[k] {
			t.Errorf("nogoals_gen.go lists %q but the contract does not declare it no-goal — regenerate", k)
		}
	}
}

func TestIsDeclaredNoGoal(t *testing.T) {
	cases := []struct {
		method, path string
		want         bool
	}{
		// Literal and {id} matching against real contract entries.
		{"DELETE", "/api/admin/autoCommitSettings", true},
		{"DELETE", "/api/edge/3f2a1b00-0000-4000-8000-aabbccddeeff", true},
		{"DELETE", "/api/edge/3f2a1b00-0000-4000-8000-aabbccddeeff/device/00000000-0000-4000-8000-000000000001", true},
		// Wrong method on a declared path is NOT declared — it must fail loudly.
		{"PATCH", "/api/admin/autoCommitSettings", false},
		// {id} matches exactly one segment, never zero, never two.
		{"DELETE", "/api/edge/", false},
		{"DELETE", "/api/edge/a/b", false},
		// Undeclared endpoints.
		{"GET", "/api/definitely/not/declared", false},
		{"POST", "/api/device", false},
	}
	for _, tc := range cases {
		if got := isDeclaredNoGoal(tc.method, tc.path); got != tc.want {
			t.Errorf("isDeclaredNoGoal(%s %s) = %v, want %v", tc.method, tc.path, got, tc.want)
		}
	}
}

// The catch-all's two failure modes, asserted end to end through the mux.
// A declared no-goal GET keeps the empty-PageData 200 so its screen renders;
// an undeclared GET must 404 — the empty 200 on undeclared paths is exactly
// how 34 endpoints once went silently unimplemented.
func TestCatchAll_DeclaredVsUndeclared(t *testing.T) {
	mux := http.NewServeMux()
	registerRoutes(mux, "*")

	get := func(path string) *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest("GET", path, nil))
		return rec
	}

	// GET /api/ai/model — declared no-goal in the contract.
	if rec := get("/api/ai/model"); rec.Code != http.StatusOK {
		t.Errorf("declared no-goal GET = %d, want 200 empty page", rec.Code)
	}
	// Undeclared path: loud 404 with the TB error envelope.
	rec := get("/api/definitely/not/declared")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("undeclared GET = %d, want 404", rec.Code)
	}
	var body struct {
		Status    int    `json:"status"`
		ErrorCode int    `json:"errorCode"`
		Message   string `json:"message"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("undeclared 404 body is not the JSON envelope: %v", err)
	}
	if body.Status != 404 || body.ErrorCode != 32 {
		t.Errorf("envelope = %+v, want status=404 errorCode=32", body)
	}
}
