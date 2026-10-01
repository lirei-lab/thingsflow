package system

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	authpkg "flow-core/internal/auth"
)

// Regression coverage for the ui-contract data-fidelity audit
// (docs/adr/0002, docs/UI_CONTRACT_DATA_FIDELITY.md): /api/queues was
// registered method-agnostic, so a POST fell through to the SELECT and
// answered 200 with the queue LIST. The UI's "save queue" therefore reported
// success for a queue that was never created — the same shape as the
// widgetType DELETE that returned the row it had not deleted.
//
// The audit left this one open as a product decision rather than a missed
// wire-up, and the decision is not to implement the write: on this platform
// queues and their consumers are Helm/k8s-owned (the durables are created by
// the nats-bootstrap hook from chart values) and nothing in the data plane
// reads the `queue` table. A row written through the API would configure
// nothing, which is a more expensive lie than a refusal — so the endpoint
// answers honestly, like the other capabilities the platform deliberately
// does not have.
//
// Both cases reject before touching the database, so no Postgres is needed.

func queuesJWT(t *testing.T) string {
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

func TestQueuesWriteIsAnsweredHonestly(t *testing.T) {
	tok := queuesJWT(t)

	for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodDelete} {
		t.Run(method, func(t *testing.T) {
			body := bytes.NewReader([]byte(`{"name":"Main","topic":"tb_rule_engine.main"}`))
			req := httptest.NewRequest(method, "/api/queues", body)
			req.Header.Set("X-Authorization", "Bearer "+tok)
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()

			HandleQueues(rec, req)

			if rec.Code == http.StatusOK {
				t.Fatalf("%s /api/queues returned 200 — a prior version answered "+
					"with the queue list, reporting success for a write that never "+
					"happened; body=%s", method, rec.Body.String())
			}
			if rec.Code != http.StatusNotImplemented {
				t.Errorf("got %d, want %d", rec.Code, http.StatusNotImplemented)
			}
			// The message has to say WHERE queues actually come from, or the
			// next operator reads 501 as "unfinished" and files a bug.
			if !bytes.Contains(rec.Body.Bytes(), []byte("Helm")) {
				t.Errorf("the refusal does not point at the real source of queue "+
					"configuration: %s", rec.Body.String())
			}
		})
	}
}

func TestQueuesReadStillRequiresAuth(t *testing.T) {
	// The honest-refusal branch must not run before authentication — an
	// unauthenticated caller should still get 401, not a 501 that leaks that
	// the endpoint exists in this shape.
	req := httptest.NewRequest(http.MethodPost, "/api/queues", nil)
	rec := httptest.NewRecorder()

	HandleQueues(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Errorf("got %d, want 401", rec.Code)
	}
}
