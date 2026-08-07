package twin

import (
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"strings"

	dbpkg "flow-core/internal/db"
	"flow-core/internal/httputil"
	"flow-core/internal/topology"
)

// expandSpecRE matches ?expand=relations(<depth>) where depth is a positive
// integer. Depth is bounded against topology.DefaultExpandMaxDepth below.
var expandSpecRE = regexp.MustCompile(`^relations\((\d+)\)$`)

// parseExpandSpec parses and bounds an ?expand=relations(depth) value. A
// depth below 1 or above the fixed API ceiling (topology.DefaultExpandMaxDepth,
// set by the 03-01 synthetic CTE benchmark) is rejected with an error rather
// than best-effort traversal: exceeding the ceiling must be an explicit
// client error (400), never a silently truncated expansion.
func parseExpandSpec(raw string) (int, error) {
	m := expandSpecRE.FindStringSubmatch(strings.TrimSpace(raw))
	if m == nil {
		return 0, fmt.Errorf("malformed expand parameter %q: expected relations(<depth>)", raw)
	}
	depth, err := strconv.Atoi(m[1])
	if err != nil {
		return 0, fmt.Errorf("malformed expand depth: %v", err)
	}
	if depth < 1 {
		return 0, fmt.Errorf("expand depth must be >= 1")
	}
	if depth > topology.DefaultExpandMaxDepth {
		return 0, fmt.Errorf("expand depth %d exceeds the maximum allowed depth %d", depth, topology.DefaultExpandMaxDepth)
	}
	return depth, nil
}

// expandErrorStatus maps an ExpandWithCTE failure to its HTTP status. A
// traversal-budget violation is a 422 (well-formed request that cannot be
// served within the node budget); anything else is a 500.
func expandErrorStatus(err error) int {
	if errors.Is(err, topology.ErrTraversalBudget) {
		return http.StatusUnprocessableEntity
	}
	return http.StatusInternalServerError
}

func expandErrorMessage(err error) string {
	if errors.Is(err, topology.ErrTraversalBudget) {
		return fmt.Sprintf("Twin traversal exceeded the node budget (%d nodes)", topology.DefaultExpandMaxNodes)
	}
	return "Twin traversal failed"
}

func nodeKey(ref topology.EntityRef) string {
	return strings.ToUpper(ref.Type) + ":" + ref.ID
}

// expandRelations implements the ?expand=relations(depth) option on the twin
// read. When the expand parameter is absent it returns the input relations
// unchanged, so the non-expand response stays byte-identical. When present it:
//
//  1. parses and bounds the depth (malformed or over-ceiling -> 400);
//  2. runs the Wave 1 tenant-scoped ExpandWithCTE for the authoritative
//     reachable set and node-budget enforcement (ErrTraversalBudget -> 422);
//  3. annotates the immediate relations with depth 1 + embedded neighbor state;
//  4. appends transitive nodes beyond depth 1 as depth-annotated entries with
//     embedded state (name/type/label/attributes).
//
// The traversal is scoped to the ROOT entity's own tenant — the caller's
// tenant for a non-SYS_ADMIN (GetByEntity enforces equality), and the root's
// tenant when a SYS_ADMIN (whose claim carries no tenant) crosses. Depth labels
// come from a bounded BFS over the same tenant-scoped NeighborsTenant union so
// they can never disagree with the edges ExpandWithCTE traversed.
func expandRelations(w http.ResponseWriter, r *http.Request, tenantID string, row entityRow, relations []relationProjection) ([]relationProjection, bool) {
	raw := r.URL.Query().Get("expand")
	if raw == "" {
		return relations, true
	}
	depth, err := parseExpandSpec(raw)
	if err != nil {
		httputil.WriteError(w, http.StatusBadRequest, err.Error())
		return nil, false
	}
	if dbpkg.Pool == nil {
		httputil.WriteError(w, http.StatusServiceUnavailable, "Database not ready")
		return nil, false
	}

	// traversalTenant: the root's tenant is authoritative for the root's graph.
	// A non-SYS_ADMIN caller claim already equals it; a SYS_ADMIN claim is
	// empty and falls back to the root's tenant.
	traversalTenant := tenantID
	if traversalTenant == "" {
		traversalTenant = row.TenantID
	}
	root := topology.EntityRef{Type: row.EntityType, ID: row.ID}
	direction := "FROM"
	var relationTypes []string // empty -> ExpandWithCTE defaults to ["Contains"]

	nodes, err := topology.ExpandWithCTE(dbpkg.Pool, traversalTenant, root, direction, relationTypes, depth, topology.DefaultExpandMaxNodes)
	if err != nil {
		httputil.WriteError(w, expandErrorStatus(err), expandErrorMessage(err))
		return nil, false
	}

	// ExpandWithCTE returns a flat, depth-sorted set; per-node depth is not
	// part of its EntityRef result, so a bounded BFS over the same tenant-scoped
	// union labels each node with its hop level for annotation.
	depths := expandDepths(dbpkg.Pool, traversalTenant, root, direction, relationTypes, depth)

	immediateKeys := map[string]bool{}
	for i := range relations {
		relations[i].Depth = 1
		if neighbor, ok := relationNeighbor(relations[i], row); ok {
			immediateKeys[nodeKey(neighbor)] = true
			relations[i].State = embedEntityState(traversalTenant, neighbor)
		}
	}

	rootKey := nodeKey(root)
	out := relations
	for _, n := range nodes {
		key := nodeKey(n)
		if key == rootKey || immediateKeys[key] {
			continue
		}
		d := depths[key]
		if d == 0 {
			d = depth
		}
		out = append(out, relationProjection{
			Group:     "COMMON",
			Direction: "OUT",
			Source:    thingID(traversalTenant, row.EntityType, row.ID),
			Target:    thingID(traversalTenant, n.Type, n.ID),
			SourceEntity: map[string]interface{}{
				"entityType": row.EntityType,
				"id":         row.ID,
			},
			TargetEntity: map[string]interface{}{
				"entityType": n.Type,
				"id":         n.ID,
			},
			Depth: d,
			State: embedEntityState(traversalTenant, n),
		})
	}
	return out, true
}

// relationNeighbor returns the far endpoint of an immediate relation relative
// to the root row — the entity on the other side of the edge. ok=false when
// the relation does not reference the root (defensive; loadRelations always
// emits edges incident to the root).
func relationNeighbor(rel relationProjection, row entityRow) (topology.EntityRef, bool) {
	other := func(m map[string]interface{}) (string, string, bool) {
		t, _ := m["entityType"].(string)
		id, _ := m["id"].(string)
		if t == "" || id == "" {
			return "", "", false
		}
		if t == row.EntityType && id == row.ID {
			return "", "", false
		}
		return t, id, true
	}
	if rel.SourceEntity != nil {
		if t, id, ok := other(rel.SourceEntity); ok {
			return topology.EntityRef{Type: t, ID: id}, true
		}
	}
	if rel.TargetEntity != nil {
		if t, id, ok := other(rel.TargetEntity); ok {
			return topology.EntityRef{Type: t, ID: id}, true
		}
	}
	return topology.EntityRef{}, false
}

// embedEntityState loads the entity's governed identity (name/type/label plus
// registry attributes) for embedding on an expanded relation. It degrades to
// the base entity attributes when the registry has no pin, and to a bare ref
// when the entity has vanished since the traversal — the caller still sees
// where the relation pointed.
//
// SECURITY: the neighbor is reached through an edge; if a cross-tenant edge
// were ever present (defense in depth — SaveEdge already refuses to create
// one), the entity row's tenant is verified against the traversal tenant
// BEFORE any governed state is embedded. A foreign neighbor degrades to a
// bare ref exactly like a vanished entity, so the expand path can never leak
// another tenant's name/type/label/attributes. This mirrors the tenant-scoped
// hydration of the REST relationsQuery walk (resolveEntity + nil-drop).
func embedEntityState(tenantID string, ref topology.EntityRef) map[string]interface{} {
	bare := map[string]interface{}{
		"entityType": ref.Type,
		"id":         ref.ID,
	}
	row, err := loadEntity(ref.Type, ref.ID)
	if err != nil {
		return bare
	}
	// Tenant isolation gate: the neighbor must belong to the traversal tenant.
	// A SYS_ADMIN expands within the root's resolved tenant (traversalTenant),
	// so an entity outside that tenant is out of scope for the graph walk too.
	if row.TenantID != tenantID {
		return bare
	}
	identity, err := loadIdentity(row)
	if err != nil {
		return buildAttributes(row)
	}
	if len(identity.Attributes) == 0 {
		return buildAttributes(row)
	}
	return identity.Attributes
}

// expandDepths labels every node reachable from root within maxDepth hops with
// its BFS hop level, reusing the same tenant-scoped NeighborsTenant union that
// backs ExpandWithCTE so edge visibility cannot diverge between the two reads.
// Membership is NOT authoritative here — ExpandWithCTE owns the set and the
// budget; this map only annotates depths. Nodes the BFS cannot reach (e.g. an
// entity deleted between the two reads) stay absent and callers fall back to
// the requested max depth.
func expandDepths(db *sql.DB, tenantID string, root topology.EntityRef, direction string, relationTypes []string, maxDepth int) map[string]int {
	depthByKey := map[string]int{nodeKey(root): 0}
	frontier := []topology.EntityRef{root}
	for level := 1; level <= maxDepth; level++ {
		next := []topology.EntityRef{}
		for _, cur := range frontier {
			neighbors, err := topology.NeighborsTenant(db, tenantID, cur, direction, relationTypes)
			if err != nil {
				continue
			}
			for _, n := range neighbors {
				key := nodeKey(n)
				if _, seen := depthByKey[key]; seen {
					continue
				}
				depthByKey[key] = level
				next = append(next, n)
			}
		}
		frontier = next
	}
	return depthByKey
}
