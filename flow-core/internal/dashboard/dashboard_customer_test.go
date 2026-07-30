package dashboard

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

const (
	custX = "cccccccc-cccc-cccc-cccc-cccccccccc11"
	custY = "cccccccc-cccc-cccc-cccc-cccccccccc22"
	dashX = "eeeeeeee-eeee-eeee-eeee-eeeeeeeeee11"
)

func setupCustomerTables(t *testing.T, db *sql.DB) {
	t.Helper()
	setupTables(t, db)
	if _, err := db.Exec(`DROP TABLE IF EXISTS customer CASCADE`); err != nil {
		t.Fatalf("schema: %v", err)
	}
	if _, err := db.Exec(`CREATE TABLE customer (
		id uuid PRIMARY KEY, created_time bigint, tenant_id uuid,
		title text, is_public boolean)`); err != nil {
		t.Fatalf("schema: %v", err)
	}
	for _, s := range []struct {
		q    string
		args []interface{}
	}{
		{`INSERT INTO customer (id, created_time, tenant_id, title, is_public) VALUES ($1,0,$2,'Sitio Norte',false)`, []interface{}{custX, tenantA}},
		{`INSERT INTO customer (id, created_time, tenant_id, title, is_public) VALUES ($1,0,$2,'Sitio Sur',false)`, []interface{}{custY, tenantA}},
	} {
		if _, err := db.Exec(s.q, s.args...); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
}

func insertDashboard(t *testing.T, db *sql.DB, id, tenant, title, assigned string) {
	t.Helper()
	var a interface{}
	if assigned != "" {
		a = assigned
	}
	if _, err := db.Exec(
		`INSERT INTO dashboard (id, created_time, tenant_id, title, assigned_customers) VALUES ($1,0,$2,$3,$4)`,
		id, tenant, title, a); err != nil {
		t.Fatalf("insert dashboard: %v", err)
	}
}

func assignedJSON(customerIDs ...string) string {
	out := "["
	for i, c := range customerIDs {
		if i > 0 {
			out += ","
		}
		out += fmt.Sprintf(`{"customerId":{"entityType":"CUSTOMER","id":"%s"},"title":"t","public":false}`, c)
	}
	return out + "]"
}

func getJSON(t *testing.T, fn func(http.ResponseWriter, *http.Request), tenant, url string) (int, map[string]interface{}) {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, url, nil)
	req.Header.Set("X-Authorization", "Bearer "+fakeJWT(t, tenant))
	rec := httptest.NewRecorder()
	fn(rec, req)
	var body map[string]interface{}
	_ = json.Unmarshal(rec.Body.Bytes(), &body)
	return rec.Code, body
}

func TestListByCustomer_OnlyAssignedDashboards(t *testing.T) {
	db := newTestDB(t)
	setupCustomerTables(t, db)
	insertDashboard(t, db, dashX, tenantA, "Consumo", assignedJSON(custX))
	insertDashboard(t, db, "eeeeeeee-eeee-eeee-eeee-eeeeeeeeee12", tenantA, "Otro", assignedJSON(custY))
	insertDashboard(t, db, "eeeeeeee-eeee-eeee-eeee-eeeeeeeeee13", tenantA, "Sin asignar", "")

	code, body := getJSON(t, func(w http.ResponseWriter, r *http.Request) {
		ListByCustomer(w, r, custX)
	}, tenantA, "/api/customer/"+custX+"/dashboards?pageSize=10&page=0")

	if code != http.StatusOK {
		t.Fatalf("status = %d, want 200", code)
	}
	if got := body["totalElements"]; got != float64(1) {
		t.Fatalf("totalElements = %v, want 1", got)
	}
	data := body["data"].([]interface{})
	if got := data[0].(map[string]interface{})["title"]; got != "Consumo" {
		t.Fatalf("title = %v, want Consumo", got)
	}
}

// A dashboard with unparseable assigned_customers must be skipped, never crash the
// listing — the column is a plain varchar and legacy rows can hold anything.
func TestListByCustomer_IgnoresMalformedAssignment(t *testing.T) {
	db := newTestDB(t)
	setupCustomerTables(t, db)
	insertDashboard(t, db, dashX, tenantA, "Roto", "not json at all")
	insertDashboard(t, db, "eeeeeeee-eeee-eeee-eeee-eeeeeeeeee12", tenantA, "Bueno", assignedJSON(custX))

	code, body := getJSON(t, func(w http.ResponseWriter, r *http.Request) {
		ListByCustomer(w, r, custX)
	}, tenantA, "/api/customer/"+custX+"/dashboards")

	if code != http.StatusOK {
		t.Fatalf("status = %d, want 200", code)
	}
	if got := body["totalElements"]; got != float64(1) {
		t.Fatalf("totalElements = %v, want 1 (malformed row must be skipped, not fatal)", got)
	}
}

func TestListCustomers_ResolvesTitlesLive(t *testing.T) {
	db := newTestDB(t)
	setupCustomerTables(t, db)
	insertDashboard(t, db, dashX, tenantA, "Consumo", assignedJSON(custX, custY))

	// Rename after assignment: the response must show the current title, not the
	// stale copy denormalised into assigned_customers.
	if _, err := db.Exec(`UPDATE customer SET title = 'Sitio Norte (renombrado)' WHERE id = $1`, custX); err != nil {
		t.Fatalf("rename: %v", err)
	}

	code, body := getJSON(t, func(w http.ResponseWriter, r *http.Request) {
		ListCustomers(w, r, dashX)
	}, tenantA, "/api/dashboard/"+dashX+"/customers")

	if code != http.StatusOK {
		t.Fatalf("status = %d, want 200", code)
	}
	if got := body["totalElements"]; got != float64(2) {
		t.Fatalf("totalElements = %v, want 2", got)
	}
	titles := map[string]bool{}
	for _, e := range body["data"].([]interface{}) {
		titles[e.(map[string]interface{})["title"].(string)] = true
	}
	if !titles["Sitio Norte (renombrado)"] {
		t.Fatalf("titles = %v, want the renamed customer title", titles)
	}
}

// A customer deleted without cleaning the dashboard's array must be skipped
// rather than rendered as a blank row.
func TestListCustomers_SkipsDanglingCustomer(t *testing.T) {
	db := newTestDB(t)
	setupCustomerTables(t, db)
	insertDashboard(t, db, dashX, tenantA, "Consumo", assignedJSON(custX, custY))
	if _, err := db.Exec(`DELETE FROM customer WHERE id = $1`, custY); err != nil {
		t.Fatalf("delete: %v", err)
	}

	code, body := getJSON(t, func(w http.ResponseWriter, r *http.Request) {
		ListCustomers(w, r, dashX)
	}, tenantA, "/api/dashboard/"+dashX+"/customers")

	if code != http.StatusOK {
		t.Fatalf("status = %d, want 200", code)
	}
	if got := body["totalElements"]; got != float64(1) {
		t.Fatalf("totalElements = %v, want 1", got)
	}
}

func TestListCustomers_CrossTenantIs404(t *testing.T) {
	db := newTestDB(t)
	setupCustomerTables(t, db)
	insertDashboard(t, db, dashX, tenantA, "Consumo", assignedJSON(custX))

	code, _ := getJSON(t, func(w http.ResponseWriter, r *http.Request) {
		ListCustomers(w, r, dashX)
	}, tenantB, "/api/dashboard/"+dashX+"/customers")

	if code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404 (must not leak existence)", code)
	}
}
