package widget

import (
	"net/http"
	"net/http/httptest"
	"testing"

	authpkg "flow-core/internal/auth"
)

// Regression coverage for the ui-contract data-fidelity audit
// (docs/adr/0002): /api/widgetType/{id} and /api/widgetsBundle/{id} were
// registered method-agnostic ("read-only" per api.go's own comment) but
// nothing enforced that — DELETE fell through to the same SELECT and
// returned 200 with the row's data, reporting success for a delete that
// never happened (no DELETE FROM widget_type / widgets_bundle exists
// anywhere in the repo). Both handlers now reject non-GET before touching
// the database, so these tests need no Postgres connection.

func fakeJWT(t *testing.T) string {
	t.Helper()
	t.Setenv("JWT_TOKEN_SIGNING_KEY", "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY=")
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
	return tok
}

func TestTypeByID_DeleteIsMethodNotAllowed(t *testing.T) {
	tok := fakeJWT(t)
	req := httptest.NewRequest(http.MethodDelete, "/api/widgetType/00000000-0000-4000-8000-000000000000", nil)
	req.Header.Set("X-Authorization", "Bearer "+tok)
	rec := httptest.NewRecorder()

	TypeByID(rec, req, "00000000-0000-4000-8000-000000000000")

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("DELETE /api/widgetType/{id}: got %d, want %d (a prior version returned 200 without deleting anything)", rec.Code, http.StatusMethodNotAllowed)
	}
}

func TestBundleByID_DeleteIsMethodNotAllowed(t *testing.T) {
	tok := fakeJWT(t)
	req := httptest.NewRequest(http.MethodDelete, "/api/widgetsBundle/00000000-0000-4000-8000-000000000000", nil)
	req.Header.Set("X-Authorization", "Bearer "+tok)
	rec := httptest.NewRecorder()

	BundleByID(rec, req, "00000000-0000-4000-8000-000000000000")

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("DELETE /api/widgetsBundle/{id}: got %d, want %d (a prior version returned 200 without deleting anything)", rec.Code, http.StatusMethodNotAllowed)
	}
}
