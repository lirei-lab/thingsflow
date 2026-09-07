package resource

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	_ "github.com/lib/pq"

	authpkg "flow-core/internal/auth"
	dbpkg "flow-core/internal/db"
	"flow-core/internal/testdb"
)

// Regression coverage for the ui-contract data-fidelity audit (docs/adr/0002,
// docs/UI_CONTRACT_DATA_FIDELITY.md P2): PUT /api/image/import computed a full
// descriptor/etag/createdTime/tenantId/subType and wrote them to the row, but
// the response returned only id/title/fileName/resourceKey — while the sibling
// ImageUpload returned the complete shape for the same table.
func TestImageImport_ReturnsFullFieldSet(t *testing.T) {
	dsn := os.Getenv("FLOW_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("FLOW_TEST_PG_DSN not set")
	}
	db, err := sql.Open("postgres", testdb.Scoped(t, dsn))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := db.Ping(); err != nil {
		t.Fatalf("ping: %v", err)
	}

	const tenantID = "88888888-8888-8888-8888-888888888888"
	stmts := []string{
		`DROP TABLE IF EXISTS resource CASCADE`,
		`CREATE TABLE resource (
			id uuid PRIMARY KEY, created_time bigint, tenant_id uuid, title text,
			resource_type text, resource_sub_type text, resource_key text,
			file_name text, data bytea, etag text, descriptor text,
			is_public boolean, public_resource_key text,
			UNIQUE (tenant_id, resource_type, resource_key))`,
	}
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("schema: %v", err)
		}
	}
	dbpkg.SetPoolForTest(t, db)
	t.Cleanup(func() {
		dbpkg.SetPoolForTest(t, nil)
		db.Close()
	})

	authpkg.InitConfig()
	tok, err := authpkg.GenerateAccess(authpkg.Subject{
		UserID: "00000000-0000-0000-0000-000000000001", Email: "x@x.org",
		Authority: "TENANT_ADMIN", TenantID: tenantID,
	}, "test")
	if err != nil {
		t.Fatalf("jwt: %v", err)
	}

	body, _ := json.Marshal(map[string]interface{}{
		"title": "Logo", "fileName": "logo.png", "resourceKey": "logo.png",
		"mediaType": "image/png", "data": "aGVsbG8=", "public": true,
	})
	req := httptest.NewRequest(http.MethodPut, "/api/image/import", bytes.NewReader(body))
	req.Header.Set("X-Authorization", "Bearer "+tok)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	ImageImport(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status: got %d, body=%s", rec.Code, rec.Body.String())
	}
	var got map[string]interface{}
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode: %v", err)
	}

	// The four fields the old response already had.
	if got["title"] != "Logo" || got["fileName"] != "logo.png" || got["resourceKey"] != "logo.png" {
		t.Errorf("base fields wrong: %v", got)
	}
	// The fields it computed, persisted, and then dropped on the floor.
	for _, key := range []string{"createdTime", "tenantId", "resourceType", "resourceSubType", "etag", "descriptor", "public", "link"} {
		if _, ok := got[key]; !ok {
			t.Errorf("%s missing from response (a prior version omitted it despite writing it to the row)", key)
		}
	}
	if got["resourceType"] != "IMAGE" {
		t.Errorf("resourceType: got %v, want IMAGE", got["resourceType"])
	}
	if got["public"] != true {
		t.Errorf("public: got %v, want true (must reflect the posted value, not a constant)", got["public"])
	}
}
