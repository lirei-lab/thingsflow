package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// Regression coverage for the ui-contract data-fidelity audit
// (docs/adr/0002): DELETE /api/ruleChain/{id} was registered
// method-agnostic and system.HandleRuleChainByID has no method branch of
// its own — DELETE fell through to the same SELECT and returned 200 with
// the chain's data, reporting success for a delete that never happened (no
// DELETE FROM rule_chain exists anywhere in the repo). The registration
// closure now rejects non-GET before calling the handler, so this test
// needs no Postgres connection — it never reaches the query.

func TestRuleChainByID_DeleteIsMethodNotAllowed(t *testing.T) {
	mux := http.NewServeMux()
	registerRoutes(mux, "*")

	req := httptest.NewRequest(http.MethodDelete, "/api/ruleChain/00000000-0000-4000-8000-000000000000", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("DELETE /api/ruleChain/{id}: got %d, want %d (a prior version returned 200 with the chain's data without deleting anything)", rec.Code, http.StatusMethodNotAllowed)
	}
}
