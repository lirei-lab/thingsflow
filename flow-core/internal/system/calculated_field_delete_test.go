package system

import (
	"net/http"
	"net/http/httptest"
	"testing"

	authpkg "flow-core/internal/auth"
)

// Regression coverage for the ui-contract data-fidelity audit
// (docs/adr/0002): DELETE /api/calculatedField/{id} previously returned 200
// unconditionally, reporting success for a delete on a field GET already
// says can never be found (calculated fields don't persist anywhere on
// this platform — see docs/API_REFERENCE.md's documented disabled state).
// DELETE now matches GET's honest 404. No DB is touched by either branch,
// so this test needs no Postgres connection.

func TestHandleCalculatedFieldByID_DeleteMatchesGetNotFound(t *testing.T) {
	authpkg.InitConfig()
	tok, err := authpkg.GenerateAccess(authpkg.Subject{
		UserID:    "00000000-0000-0000-0000-000000000001",
		Email:     "x@x.org",
		Authority: "TENANT_ADMIN",
		TenantID:  "11111111-1111-1111-1111-111111111111",
	}, "test")
	if err != nil {
		t.Fatalf("jwt: %v", err)
	}

	for _, method := range []string{http.MethodGet, http.MethodDelete} {
		req := httptest.NewRequest(method, "/api/calculatedField/00000000-0000-4000-8000-000000000000", nil)
		req.Header.Set("X-Authorization", "Bearer "+tok)
		rec := httptest.NewRecorder()

		HandleCalculatedFieldByID(rec, req, "00000000-0000-4000-8000-000000000000")

		if rec.Code != http.StatusNotFound {
			t.Fatalf("%s /api/calculatedField/{id}: got %d, want %d (DELETE used to return 200 without deleting anything)", method, rec.Code, http.StatusNotFound)
		}
	}
}
