package customer

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"

	"flow-core/internal/audit"
	dbpkg "flow-core/internal/db"
	"flow-core/internal/httputil"
)

// Assigning entities to a customer is how a tenant partitions a fleet by site,
// building or client: the customer owns a slice of the devices/assets and the
// dashboards that render them. The read side of this already worked (listing a
// customer's devices, dashboards, users), but every assignment mutation 404'd,
// so the feature was half-built and unusable from the UI.
//
// device, asset and entity_view carry a `customer_id` column, so assignment is a
// column update. Dashboards are many-to-many and use TB's `assigned_customers`
// JSON array, handled separately below.

// assignableEntity maps the URL segment used by the ThingsBoard UI to its table.
var assignableEntity = map[string]struct {
	table      string
	entityType string
}{
	"device":     {"device", "DEVICE"},
	"asset":      {"asset", "ASSET"},
	"entityView": {"entity_view", "ENTITY_VIEW"},
}

// HandleAssignToCustomer serves POST /api/customer/{customerId}/{entity}/{entityId}
// and DELETE /api/customer/{entity}/{entityId} (unassign, customer implied by the row).
// Passing an empty customerID unassigns.
func HandleAssignToCustomer(w http.ResponseWriter, r *http.Request, customerID, entityKind, entityID string) {
	claims, ok := httputil.RequireAuth(w, r)
	if !ok {
		return
	}
	tenantID, _ := claims["tenantId"].(string)

	spec, known := assignableEntity[entityKind]
	if !known {
		httputil.WriteError(w, http.StatusBadRequest, "Unsupported entity type: "+entityKind)
		return
	}

	// Tenant isolation: never let a caller touch a row from another tenant, and
	// never assign to a customer that is not theirs.
	var rowTenant, name string
	err := dbpkg.Pool.QueryRow(
		fmt.Sprintf("SELECT tenant_id::text, COALESCE(name,'') FROM %s WHERE id = $1", spec.table),
		entityID).Scan(&rowTenant, &name)
	if err != nil {
		httputil.WriteError(w, http.StatusNotFound, spec.entityType+" not found")
		return
	}
	if rowTenant != tenantID {
		httputil.WriteError(w, http.StatusForbidden, "Cross-tenant assignment denied")
		return
	}
	// Deliberately after the entity lookup above: a request naming an entity
	// that does not exist must still 404 on that, which is what the UI
	// contract pins for these paths.
	if isPublicCustomerSegment(customerID) {
		httputil.WriteError(w, http.StatusNotImplemented,
			"Public sharing is not enabled on this platform")
		return
	}
	if customerID != "" && !customerBelongsToTenant(customerID, tenantID) {
		httputil.WriteError(w, http.StatusForbidden, "Customer belongs to another tenant")
		return
	}

	var customerArg interface{}
	action := "UNASSIGNED_FROM_CUSTOMER"
	if customerID != "" {
		customerArg = customerID
		action = "ASSIGNED_TO_CUSTOMER"
	}
	if _, err := dbpkg.Pool.Exec(
		fmt.Sprintf("UPDATE %s SET customer_id = $1 WHERE id = $2", spec.table),
		customerArg, entityID); err != nil {
		log.Printf("ERROR assigning %s %s to customer %q: %v", spec.entityType, entityID, customerID, err)
		httputil.WriteError(w, http.StatusInternalServerError, "Failed to update assignment")
		return
	}
	audit.EntityChange(claims, spec.entityType, entityID, name, action)

	// TB returns the updated entity so the UI can refresh its row in place.
	entity, err := queryAssignedEntity(spec.table, spec.entityType, entityID, tenantID)
	if err != nil {
		w.WriteHeader(http.StatusOK)
		return
	}
	httputil.WriteJSON(w, http.StatusOK, entity)
}

// HandleAssignDashboardToCustomer serves
// POST|DELETE /api/customer/{customerId}/dashboard/{dashboardId}. Dashboards are
// many-to-many, so this edits TB's `assigned_customers` JSON array rather than a column.
func HandleAssignDashboardToCustomer(w http.ResponseWriter, r *http.Request, customerID, dashboardID string, assign bool) {
	claims, ok := httputil.RequireAuth(w, r)
	if !ok {
		return
	}
	tenantID, _ := claims["tenantId"].(string)

	var rowTenant, title string
	var assignedRaw sql.NullString
	err := dbpkg.Pool.QueryRow(
		`SELECT tenant_id::text, COALESCE(title,''), assigned_customers FROM dashboard WHERE id = $1`,
		dashboardID).Scan(&rowTenant, &title, &assignedRaw)
	if err != nil {
		httputil.WriteError(w, http.StatusNotFound, "Dashboard not found")
		return
	}
	if rowTenant != tenantID {
		httputil.WriteError(w, http.StatusForbidden, "Cross-tenant assignment denied")
		return
	}
	// After the dashboard lookup, for the same reason as in
	// HandleAssignToCustomer: a missing dashboard must still 404.
	if isPublicCustomerSegment(customerID) {
		httputil.WriteError(w, http.StatusNotImplemented,
			"Public sharing is not enabled on this platform")
		return
	}
	if !customerBelongsToTenant(customerID, tenantID) {
		httputil.WriteError(w, http.StatusForbidden, "Customer belongs to another tenant")
		return
	}

	assigned := parseAssignedCustomers(assignedRaw)
	custTitle := customerTitle(customerID)
	next := make([]map[string]interface{}, 0, len(assigned)+1)
	for _, a := range assigned {
		if assignedCustomerID(a) != customerID {
			next = append(next, a)
		}
	}
	if assign {
		next = append(next, map[string]interface{}{
			"customerId": map[string]interface{}{"entityType": "CUSTOMER", "id": customerID},
			"title":      custTitle,
			"public":     false,
		})
	}

	encoded, err := json.Marshal(next)
	if err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, "Failed to encode assignment")
		return
	}
	var arg interface{}
	if len(next) > 0 {
		arg = string(encoded)
	}
	if _, err := dbpkg.Pool.Exec(
		`UPDATE dashboard SET assigned_customers = $1 WHERE id = $2`, arg, dashboardID); err != nil {
		log.Printf("ERROR updating dashboard %s assignment: %v", dashboardID, err)
		httputil.WriteError(w, http.StatusInternalServerError, "Failed to update assignment")
		return
	}
	action := "UNASSIGNED_FROM_CUSTOMER"
	if assign {
		action = "ASSIGNED_TO_CUSTOMER"
	}
	audit.EntityChange(claims, "DASHBOARD", dashboardID, title, action)

	httputil.WriteJSON(w, http.StatusOK, map[string]interface{}{
		"id":                map[string]interface{}{"entityType": "DASHBOARD", "id": dashboardID},
		"tenantId":          map[string]interface{}{"entityType": "TENANT", "id": tenantID},
		"title":             title,
		"assignedCustomers": next,
	})
}

// isPublicCustomerSegment reports whether the path named TB's public-sharing
// pseudo-customer (POST /api/customer/public/{kind}/{id}).
//
// TB gives every tenant a real `customer` row titled "Public" with
// is_public = true, and "sharing publicly" means assigning the entity to it;
// a viewer then exchanges that customer's id for an anonymous CUSTOMER_USER
// token at /api/auth/login/public. Neither half exists here: no code ever
// creates such a row, and /api/auth/login/public is a documented 501 because
// this platform has no customer-scoped authorization to bound such a token
// with (see internal/system/onboarding_handlers.go and
// docs/UI_CONTRACT_DATA_FIDELITY.md).
//
// So this is answered 501 rather than left to fail as a 403 about a customer
// belonging to another tenant, which is what it did before: that message is
// untrue — "public" names no customer at all — and it read as a permissions
// problem the operator could fix, rather than a capability that isn't here.
func isPublicCustomerSegment(customerID string) bool {
	return customerID == "public"
}

func customerBelongsToTenant(customerID, tenantID string) bool {
	// An empty id is an unassign, and TB's nil-UUID sentinel is its "no
	// owner" value — the read paths substitute it for a null customer_id
	// (internal/system/info_handlers.go), so both mean "not owned by a
	// customer" rather than naming one.
	if customerID == "" || strings.HasPrefix(customerID, "13814000-1dd2-11b2") {
		return true
	}
	var owner string
	if err := dbpkg.Pool.QueryRow(
		`SELECT tenant_id::text FROM customer WHERE id = $1`, customerID).Scan(&owner); err != nil {
		return false
	}
	return owner == tenantID
}

func customerTitle(customerID string) string {
	var title string
	if err := dbpkg.Pool.QueryRow(
		`SELECT COALESCE(title,'') FROM customer WHERE id = $1`, customerID).Scan(&title); err != nil {
		return ""
	}
	return title
}

func parseAssignedCustomers(raw sql.NullString) []map[string]interface{} {
	if !raw.Valid || strings.TrimSpace(raw.String) == "" {
		return nil
	}
	var out []map[string]interface{}
	if err := json.Unmarshal([]byte(raw.String), &out); err != nil {
		return nil
	}
	return out
}

func assignedCustomerID(entry map[string]interface{}) string {
	ref, ok := entry["customerId"].(map[string]interface{})
	if !ok {
		return ""
	}
	id, _ := ref["id"].(string)
	return id
}

// queryAssignedEntity returns the TB-shaped entity after an assignment change.
func queryAssignedEntity(table, entityType, id, tenantID string) (map[string]interface{}, error) {
	var name string
	var createdTime int64
	var customerID sql.NullString
	err := dbpkg.Pool.QueryRow(
		fmt.Sprintf(`SELECT COALESCE(name,''), created_time, customer_id::text FROM %s WHERE id = $1`, table),
		id).Scan(&name, &createdTime, &customerID)
	if err != nil {
		return nil, err
	}
	out := map[string]interface{}{
		"id":          map[string]interface{}{"entityType": entityType, "id": id},
		"tenantId":    map[string]interface{}{"entityType": "TENANT", "id": tenantID},
		"createdTime": createdTime,
		"name":        name,
	}
	if customerID.Valid && customerID.String != "" {
		out["customerId"] = map[string]interface{}{"entityType": "CUSTOMER", "id": customerID.String}
	}
	return out, nil
}
