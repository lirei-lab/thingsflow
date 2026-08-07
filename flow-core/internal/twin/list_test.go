package twin

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	authpkg "flow-core/internal/auth"
)

// testDeviceC is a third device in tenant A used by the listing/expansion
// tests for multi-row pagination, legacy-relation filters, and depth-2 chains.
const testDeviceC = "55555555-5555-5555-5555-555555555555"

// setupListTestDB builds the twin + registry schema, backfills twin_registry,
// and seeds the minimal legacy relation table plus the deviceC chain used by
// the listing filters and cursor tests. It returns the pool so tests that need
// direct SQL access can use it; tests that only exercise HandleList ignore the
// return (the pool is set on dbpkg.Global via newTwinTestDB).
func setupListTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db := newTwinTestDB(t)
	setupTwinTables(t, db)
	setupTwinRegistryTables(t, db)
	now := time.Now().UnixMilli()

	// Minimal legacy relation table (the TB schema; the twin test helpers do
	// not create it, but the listing's legacy EXISTS branch and the expand
	// traversal union both reference it). Dropped and recreated per test so
	// seeded rows are fresh and never collide with a prior run's leftovers in
	// the shared scratch database.
	if _, err := db.Exec(`DROP TABLE IF EXISTS relation CASCADE`); err != nil {
		t.Fatalf("drop relation table: %v", err)
	}
	if _, err := db.Exec(`CREATE TABLE relation (
		from_id uuid NOT NULL, from_type text NOT NULL,
		to_id uuid NOT NULL, to_type text NOT NULL,
		relation_type_group text, relation_type text,
		additional_info text, version bigint default 0,
		PRIMARY KEY (from_id, from_type, relation_type_group, relation_type, to_id, to_type))`); err != nil {
		t.Fatalf("create relation table: %v", err)
	}

	if _, err := db.Exec(`INSERT INTO device (id, created_time, tenant_id, name, type, label, additional_info)
		VALUES ($1, $2, $3, 'Meter C', 'meter', '', '{}')`, testDeviceC, now, testTenantA); err != nil {
		t.Fatalf("seed device C: %v", err)
	}
	for _, ent := range []struct{ tenant, etype, id string }{
		{testTenantA, "ASSET", testAssetA},
		{testTenantA, "DEVICE", testDeviceA},
		{testTenantA, "DEVICE", testDeviceC},
		{testTenantB, "DEVICE", testDeviceB},
	} {
		if err := SyncRegistryRow(context.Background(), db, ent.tenant, ent.etype, ent.id); err != nil {
			t.Fatalf("sync registry %s/%s: %v", ent.etype, ent.id, err)
		}
	}

	// Legacy-only edge (never mirrored into topology_edge): deviceA -> deviceC.
	if _, err := db.Exec(`INSERT INTO relation (from_id, from_type, to_id, to_type, relation_type_group, relation_type)
		VALUES ($1, 'DEVICE', $2, 'DEVICE', 'COMMON', 'Feeds')`, testDeviceA, testDeviceC); err != nil {
		t.Fatalf("seed legacy relation: %v", err)
	}
	// Cross-tenant legacy edge deviceB (tenant B) -> deviceA (tenant A). Its
	// far endpoint belongs to a different tenant, so it must never match any
	// tenant's listing (the legacy branch resolves and pins the far tenant).
	if _, err := db.Exec(`INSERT INTO relation (from_id, from_type, to_id, to_type, relation_type_group, relation_type)
		VALUES ($1, 'DEVICE', $2, 'DEVICE', 'COMMON', 'DependsOn')`, testDeviceB, testDeviceA); err != nil {
		t.Fatalf("seed cross-tenant legacy relation: %v", err)
	}
	return db
}

func sysAdminJWT(t *testing.T) string {
	t.Helper()
	authpkg.InitConfig()
	tok, err := authpkg.GenerateAccess(authpkg.Subject{
		UserID:    "00000000-0000-0000-0000-000000000002",
		Email:     "sysadmin@test.org",
		Authority: "SYS_ADMIN",
		TenantID:  "",
	}, "test")
	if err != nil {
		t.Fatalf("sysadmin jwt: %v", err)
	}
	return tok
}

func doListRequest(t *testing.T, query, token string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/api/twins"+query, nil)
	if token != "" {
		req.Header.Set("X-Authorization", "Bearer "+token)
	}
	w := httptest.NewRecorder()
	HandleList(w, req)
	return w
}

type listPage struct {
	Data []struct {
		Entity struct {
			ID string `json:"id"`
		} `json:"entity"`
	} `json:"data"`
	NextPageLink  interface{} `json:"nextPageLink"`
	HasNext       bool        `json:"hasNext"`
	TotalElements int         `json:"totalElements"`
}

func decodeListPage(t *testing.T, body []byte) listPage {
	t.Helper()
	var page listPage
	if err := json.Unmarshal(body, &page); err != nil {
		t.Fatalf("decode list page: %v", err)
	}
	return page
}

func TestListCursorRoundTrip(t *testing.T) {
	c := listCursor{TenantID: testTenantA, EntityType: "DEVICE", EntityID: testDeviceA}
	encoded := c.encode()
	decoded, err := decodeListCursor(encoded)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if decoded != c {
		t.Fatalf("round trip: got %+v want %+v", decoded, c)
	}
	if _, err := decodeListCursor("not-base64!!!"); err == nil {
		t.Fatal("expected error decoding non-base64 cursor")
	}
	if _, err := decodeListCursor(""); err == nil {
		t.Fatal("expected error decoding empty cursor")
	}
	if _, err := decodeListCursor("e30="); err == nil { // "{}" — incomplete
		t.Fatal("expected error decoding incomplete cursor")
	}
}

func TestListRequiresAuth(t *testing.T) {
	setupListTestDB(t)
	w := doListRequest(t, "", "")
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
}

func TestListEmptyTenantFailsClosed(t *testing.T) {
	setupListTestDB(t)
	// A TENANT_ADMIN token with an empty tenant claim must fail closed (401).
	authpkg.InitConfig()
	tok, err := authpkg.GenerateAccess(authpkg.Subject{
		UserID:    "00000000-0000-0000-0000-000000000003",
		Email:     "notenant@test.org",
		Authority: "TENANT_ADMIN",
		TenantID:  "",
	}, "test")
	if err != nil {
		t.Fatalf("jwt: %v", err)
	}
	w := doListRequest(t, "", tok)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
}

func TestListEmptyResult(t *testing.T) {
	setupListTestDB(t)
	// Tenant B holds only a device; kind=ASSET matches nothing — an empty
	// page is a 200 with hasNext=false, never an error.
	w := doListRequest(t, "?kind=ASSET", twinJWT(t, testTenantB))
	if w.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	page := decodeListPage(t, w.Body.Bytes())
	if len(page.Data) != 0 || page.HasNext || page.NextPageLink != nil || page.TotalElements != 0 {
		t.Fatalf("empty listing = %+v", page)
	}
}

func TestListKindValidation(t *testing.T) {
	setupListTestDB(t)
	w := doListRequest(t, "?kind=GATEWAY", twinJWT(t, testTenantA))
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
}

func TestListIsTenantScoped(t *testing.T) {
	setupListTestDB(t)
	w := doListRequest(t, "", twinJWT(t, testTenantA))
	if w.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	page := decodeListPage(t, w.Body.Bytes())
	if page.TotalElements != 3 {
		t.Fatalf("tenant A totalElements=%d want 3 (assetA, deviceA, deviceC)", page.TotalElements)
	}
	for _, item := range page.Data {
		if item.Entity.ID == testDeviceB {
			t.Fatalf("tenant A listing leaked tenant B device")
		}
	}
}

func TestListFilters(t *testing.T) {
	setupListTestDB(t)
	token := twinJWT(t, testTenantA)
	cases := []struct {
		name, query string
		wantIDs     []string
	}{
		{"kind device", "?kind=DEVICE", []string{testDeviceA, testDeviceC}},
		{"kind asset", "?kind=ASSET", []string{testAssetA}},
		{"definition substring", "?definition=thingsflow:device", []string{testDeviceA, testDeviceC}},
		{"text name", "?text=Meter", []string{testDeviceA, testDeviceC}},
		{"text label", "?text=Main%20meter", []string{testDeviceA}},
		{"relation FROM Contains", "?relationType=Contains&relationDirection=FROM", []string{testAssetA}},
		{"relation TO Contains", "?relationType=Contains&relationDirection=TO", []string{testDeviceA}},
		{"legacy relation FROM Feeds", "?relationType=Feeds&relationDirection=FROM&kind=DEVICE", []string{testDeviceA}},
		{"cross-tenant legacy FROM excluded", "?relationType=DependsOn", []string{}},
		{"cross-tenant legacy TO excluded", "?relationType=DependsOn&relationDirection=TO", []string{}},
		{"combined kind+definition+text", "?kind=DEVICE&definition=thingsflow:device&text=Main%20meter", []string{testDeviceA}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := doListRequest(t, tc.query, token)
			if w.Code != http.StatusOK {
				t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
			}
			page := decodeListPage(t, w.Body.Bytes())
			ids := []string{}
			for _, item := range page.Data {
				ids = append(ids, item.Entity.ID)
			}
			if len(ids) != len(tc.wantIDs) || page.TotalElements != len(tc.wantIDs) {
				t.Fatalf("query %s: ids=%v totalElements=%d want ids=%v", tc.query, ids, page.TotalElements, tc.wantIDs)
			}
			for _, want := range tc.wantIDs {
				found := false
				for _, id := range ids {
					if id == want {
						found = true
					}
				}
				if !found {
					t.Fatalf("query %s: missing id %s in %v", tc.query, want, ids)
				}
			}
		})
	}
}

func TestListPaginationAcrossTwoPages(t *testing.T) {
	setupListTestDB(t)
	token := twinJWT(t, testTenantA)
	// Tenant A holds 3 rows (assetA, deviceA, deviceC) ordered by
	// (tenant_id, entity_type, entity_id); pageSize=2 → [assetA, deviceA],
	// then page 2 = [deviceC].
	w := doListRequest(t, "?pageSize=2", token)
	if w.Code != http.StatusOK {
		t.Fatalf("page1 status=%d body=%s", w.Code, w.Body.String())
	}
	page1 := decodeListPage(t, w.Body.Bytes())
	if len(page1.Data) != 2 || !page1.HasNext || page1.NextPageLink == nil || page1.TotalElements != 3 {
		t.Fatalf("page1 = %+v", page1)
	}
	if page1.Data[0].Entity.ID != testAssetA || page1.Data[1].Entity.ID != testDeviceA {
		t.Fatalf("page1 order = %+v, want [assetA deviceA]", page1.Data)
	}

	w2 := doListRequest(t, "?pageSize=2&cursor="+page1.NextPageLink.(string), token)
	if w2.Code != http.StatusOK {
		t.Fatalf("page2 status=%d body=%s", w2.Code, w2.Body.String())
	}
	page2 := decodeListPage(t, w2.Body.Bytes())
	if len(page2.Data) != 1 || page2.HasNext || page2.NextPageLink != nil || page2.TotalElements != 3 {
		t.Fatalf("page2 = %+v", page2)
	}
	if page2.Data[0].Entity.ID != testDeviceC {
		t.Fatalf("page2 first = %v, want deviceC", page2.Data[0].Entity.ID)
	}
}

func TestListCrossTenantCursorRejected(t *testing.T) {
	setupListTestDB(t)
	// A cursor bound to tenant A must be rejected with 400 when presented by
	// tenant B — a cursor can never silently re-scope a request.
	c := (listCursor{TenantID: testTenantA, EntityType: "DEVICE", EntityID: testDeviceA}).encode()
	w := doListRequest(t, "?cursor="+c, twinJWT(t, testTenantB))
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status=%d body=%s want 400", w.Code, w.Body.String())
	}
}

func TestListInvalidCursorRejected(t *testing.T) {
	setupListTestDB(t)
	w := doListRequest(t, "?cursor=%21%21notbase64%21%21", twinJWT(t, testTenantA))
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status=%d body=%s want 400", w.Code, w.Body.String())
	}
}

func TestListSysAdminCrossTenant(t *testing.T) {
	setupListTestDB(t)
	w := doListRequest(t, "?pageSize=100", sysAdminJWT(t))
	if w.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	page := decodeListPage(t, w.Body.Bytes())
	if page.TotalElements != 4 {
		t.Fatalf("sysadmin totalElements=%d want 4 (assetA, deviceA, deviceC, deviceB)", page.TotalElements)
	}
	// A SYS_ADMIN may also resume a tenant-bound page (cross-tenant read).
	w2 := doListRequest(t, "?cursor="+(listCursor{TenantID: testTenantA, EntityType: "ASSET", EntityID: testAssetA}).encode(), sysAdminJWT(t))
	if w2.Code != http.StatusOK {
		t.Fatalf("sysadmin cursor status=%d body=%s", w2.Code, w2.Body.String())
	}
}
