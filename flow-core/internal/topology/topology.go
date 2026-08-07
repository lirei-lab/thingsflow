// Package topology owns the SQL/PGQ-ready topology model.
//
// Postgres remains the source of truth. The package writes modern
// tenant-scoped edges to topology_edge and mirrors them into TB's
// legacy relation table so ThingsBoard UI compatibility stays intact.
package topology

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"time"
	"unicode/utf8"

	"flow-core/internal/metrics"
	"flow-core/internal/twinevents"
	"flow-core/internal/twinmodel"

	"github.com/lib/pq"
)

var (
	ErrCrossTenant               = errors.New("topology relation crosses tenants")
	ErrInvalidRelationType       = errors.New("invalid topology relation type")
	ErrUnknownEntity             = errors.New("unknown topology entity")
	ErrRelationNotAllowedByModel = errors.New("relation is not allowed by twin model")
	// ErrTraversalBudget is returned by ExpandWithCTE when the number of
	// distinct nodes reachable within the depth ceiling exceeds the node
	// budget. Callers surface it as an API-level rejection (e.g. HTTP 422)
	// rather than attempting a best-effort unbounded traversal.
	ErrTraversalBudget = errors.New("topology traversal budget exceeded")

	modelRelationViolationMetric = metrics.Counter(
		"flow_twin_model_relation_violations_total",
		"Relation saves that violate pinned twin model declarations in warn mode",
	)
)

type EntityRef struct {
	Type string
	ID   string
}

type Edge struct {
	TenantID          string
	From              EntityRef
	To                EntityRef
	RelationType      string
	RelationTypeGroup string
	Direction         string
	Metadata          json.RawMessage
	Version           int64
}

type EdgeFilter struct {
	TenantID          string
	From              EntityRef
	To                EntityRef
	RelationType      string
	RelationTypeGroup string
}

var defaultRelationTypes = []struct {
	Name             string
	Description      string
	AllowedFromTypes []string
	AllowedToTypes   []string
}{
	{"Contains", "Physical or logical containment", []string{"ASSET"}, []string{"ASSET", "DEVICE"}},
	{"Manages", "Operational management relationship", []string{"ASSET", "CUSTOMER", "TENANT"}, []string{"ASSET", "DEVICE", "CUSTOMER"}},
	{"LocatedIn", "Location relationship", []string{"DEVICE", "ASSET"}, []string{"ASSET"}},
	{"ConnectedTo", "Network or process connectivity", []string{"DEVICE", "ASSET"}, []string{"DEVICE", "ASSET"}},
	{"Feeds", "Upstream feed relationship", []string{"DEVICE", "ASSET"}, []string{"DEVICE", "ASSET"}},
	{"DependsOn", "Operational dependency relationship", []string{"DEVICE", "ASSET"}, []string{"DEVICE", "ASSET"}},
}

func SeedRelationTypes(db *sql.DB) error {
	now := time.Now().UnixMilli()
	for _, rt := range defaultRelationTypes {
		if _, err := db.Exec(`
			INSERT INTO topology_relation_type
				(name, description, allowed_from_types, allowed_to_types, is_directed, created_time)
			VALUES ($1, $2, $3, $4, true, $5)
			ON CONFLICT (name) DO UPDATE SET
				description = EXCLUDED.description,
				allowed_from_types = EXCLUDED.allowed_from_types,
				allowed_to_types = EXCLUDED.allowed_to_types`,
			rt.Name, rt.Description, pq.Array(rt.AllowedFromTypes), pq.Array(rt.AllowedToTypes), now); err != nil {
			return err
		}
	}
	return nil
}

func SaveEdge(db *sql.DB, edge Edge) error {
	edge = normalizeEdge(edge)
	if edge.RelationType == "" || edge.From.ID == "" || edge.To.ID == "" {
		return fmt.Errorf("%w: missing from/to/type", ErrUnknownEntity)
	}
	if edge.Direction != "DIRECTED" && edge.Direction != "BIDIRECTIONAL" {
		return fmt.Errorf("%w: unsupported direction %s", ErrInvalidRelationType, edge.Direction)
	}
	if edge.Metadata == nil || len(edge.Metadata) == 0 {
		edge.Metadata = json.RawMessage(`{}`)
	}
	if !json.Valid(edge.Metadata) {
		return fmt.Errorf("invalid edge metadata json")
	}
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if edge.Direction == "BIDIRECTIONAL" {
		if _, err := tx.Exec(`SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, bidirectionalLockKey(edge)); err != nil {
			return fmt.Errorf("lock bidirectional edge: %w", err)
		}
	}
	if err := validateRelationType(tx, edge); err != nil {
		return err
	}
	if edge.Direction == "BIDIRECTIONAL" {
		reverse := edge
		reverse.From, reverse.To = edge.To, edge.From
		if err := validateRelationType(tx, reverse); err != nil {
			return err
		}
	}
	fromTenant, err := resolveEntityTenant(tx, edge.From)
	if err != nil {
		return err
	}
	toTenant, err := resolveEntityTenant(tx, edge.To)
	if err != nil {
		return err
	}
	if fromTenant != edge.TenantID || toTenant != edge.TenantID {
		return fmt.Errorf("%w: from=%s to=%s expected=%s", ErrCrossTenant, fromTenant, toTenant, edge.TenantID)
	}
	if err := enforceModelRelation(tx, edge); err != nil {
		return err
	}

	now := time.Now().UnixMilli()
	if err := saveTopologyEdge(tx, edge, now); err != nil {
		return err
	}
	if err := saveLegacyMirror(tx, edge.From, edge.To, edge, string(edge.Metadata)); err != nil {
		return err
	}
	if edge.Direction == "BIDIRECTIONAL" {
		if err := saveLegacyMirror(tx, edge.To, edge.From, edge, string(edge.Metadata)); err != nil {
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		return err
	}
	// Twin event journal (R4): the edge committed — emit the relation-saved
	// event. Keyed on the edge origin (from); both endpoints, the relation
	// type, and the direction ride in the payload.
	twinevents.Publish(edge.TenantID, edge.From.Type, edge.From.ID, twinevents.EventRelationSaved, map[string]interface{}{
		"fromType":     edge.From.Type,
		"fromId":       edge.From.ID,
		"toType":       edge.To.Type,
		"toId":         edge.To.ID,
		"relationType": edge.RelationType,
		"direction":    edge.Direction,
	})
	return nil
}

func saveTopologyEdge(tx *sql.Tx, edge Edge, now int64) error {
	if edge.Direction == "BIDIRECTIONAL" {
		var fromID, fromType, toID, toType string
		err := tx.QueryRow(`
			SELECT from_id::text, from_type, to_id::text, to_type
			  FROM topology_edge
			 WHERE tenant_id=$1 AND relation_type_group=$2 AND relation_type=$3
			   AND direction='BIDIRECTIONAL'
			   AND (((from_id=$4 AND from_type=$5) AND (to_id=$6 AND to_type=$7))
			     OR ((from_id=$6 AND from_type=$7) AND (to_id=$4 AND to_type=$5)))
			 FOR UPDATE`, edge.TenantID, edge.RelationTypeGroup, edge.RelationType,
			edge.From.ID, edge.From.Type, edge.To.ID, edge.To.Type).
			Scan(&fromID, &fromType, &toID, &toType)
		if err == nil {
			_, err = tx.Exec(`UPDATE topology_edge
				SET metadata=$8::jsonb, updated_time=$9, version=version+1
				WHERE tenant_id=$1 AND from_id=$2 AND from_type=$3 AND to_id=$4 AND to_type=$5
				  AND relation_type_group=$6 AND relation_type=$7 AND direction='BIDIRECTIONAL'`,
				edge.TenantID, fromID, fromType, toID, toType, edge.RelationTypeGroup,
				edge.RelationType, string(edge.Metadata), now)
			return err
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
	}
	_, err := tx.Exec(`
		INSERT INTO topology_edge
			(tenant_id, from_id, from_type, to_id, to_type, relation_type_group,
			 relation_type, direction, metadata, created_time, updated_time, version)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb,$10,$10,1)
		ON CONFLICT (tenant_id, from_id, from_type, relation_type_group, relation_type, to_id, to_type, direction)
		DO UPDATE SET metadata=EXCLUDED.metadata, updated_time=EXCLUDED.updated_time,
		              version=topology_edge.version+1`,
		edge.TenantID, edge.From.ID, edge.From.Type, edge.To.ID, edge.To.Type,
		edge.RelationTypeGroup, edge.RelationType, edge.Direction, string(edge.Metadata), now)
	return err
}

func saveLegacyMirror(tx *sql.Tx, from, to EntityRef, edge Edge, metadata string) error {
	_, err := tx.Exec(`
		INSERT INTO relation
			(from_id, from_type, to_id, to_type, relation_type_group, relation_type, additional_info, version)
		VALUES ($1,$2,$3,$4,$5,$6,$7,1)
		ON CONFLICT (from_id, from_type, relation_type_group, relation_type, to_id, to_type)
		DO UPDATE SET additional_info=EXCLUDED.additional_info, version=relation.version+1`,
		from.ID, from.Type, to.ID, to.Type, edge.RelationTypeGroup, edge.RelationType, metadata)
	return err
}

func DeleteEdge(db *sql.DB, filter EdgeFilter) error {
	filter = normalizeFilter(filter)
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	rows, err := tx.Query(`
		DELETE FROM topology_edge
		 WHERE tenant_id=$1 AND relation_type_group=$6 AND relation_type=$7
		   AND ((from_id=$2 AND from_type=$3 AND to_id=$4 AND to_type=$5)
		     OR (direction='BIDIRECTIONAL' AND from_id=$4 AND from_type=$5 AND to_id=$2 AND to_type=$3))
		 RETURNING direction`,
		filter.TenantID, filter.From.ID, filter.From.Type, filter.To.ID, filter.To.Type,
		filter.RelationTypeGroup, filter.RelationType)
	if err != nil {
		return err
	}
	deletedBidirectional := false
	for rows.Next() {
		var direction string
		if err := rows.Scan(&direction); err != nil {
			rows.Close()
			return err
		}
		deletedBidirectional = deletedBidirectional || direction == "BIDIRECTIONAL"
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if err := removeLegacyMirrorIfUnused(tx, filter.TenantID, filter.From, filter.To, filter.RelationTypeGroup, filter.RelationType); err != nil {
		return err
	}
	if deletedBidirectional {
		if err := removeLegacyMirrorIfUnused(tx, filter.TenantID, filter.To, filter.From, filter.RelationTypeGroup, filter.RelationType); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func removeLegacyMirrorIfUnused(tx *sql.Tx, tenantID string, from, to EntityRef, group, relationType string) error {
	_, err := tx.Exec(`
		DELETE FROM relation r
		 WHERE r.from_id=$1 AND r.from_type=$2 AND r.to_id=$3 AND r.to_type=$4
		   AND r.relation_type_group=$5 AND r.relation_type=$6
		   AND NOT EXISTS (
				SELECT 1 FROM topology_edge te
				 WHERE te.tenant_id=$7 AND te.from_id=$1 AND te.from_type=$2
				   AND te.to_id=$3 AND te.to_type=$4
				   AND te.relation_type_group=$5 AND te.relation_type=$6
			)`, from.ID, from.Type, to.ID, to.Type, group, relationType, tenantID)
	return err
}

func ListEdges(db *sql.DB, filter EdgeFilter) ([]Edge, error) {
	filter = normalizeFilter(filter)
	conds := []string{"tenant_id = $1"}
	args := []interface{}{filter.TenantID}
	add := func(col string, val string) {
		if val == "" {
			return
		}
		args = append(args, val)
		conds = append(conds, fmt.Sprintf("%s = $%d", col, len(args)))
	}
	add("from_id", filter.From.ID)
	add("from_type", filter.From.Type)
	add("to_id", filter.To.ID)
	add("to_type", filter.To.Type)
	add("relation_type", filter.RelationType)
	add("relation_type_group", filter.RelationTypeGroup)

	rows, err := db.Query(`
		SELECT tenant_id::text, from_id::text, from_type, to_id::text, to_type,
		       relation_type_group, relation_type, direction, metadata, version
		  FROM topology_edge
		 WHERE `+strings.Join(conds, " AND ")+`
		 ORDER BY relation_type, from_type, from_id, to_type, to_id`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Edge
	for rows.Next() {
		var e Edge
		var metadata []byte
		if err := rows.Scan(&e.TenantID, &e.From.ID, &e.From.Type, &e.To.ID, &e.To.Type,
			&e.RelationTypeGroup, &e.RelationType, &e.Direction, &metadata, &e.Version); err != nil {
			return nil, err
		}
		e.Metadata = append(json.RawMessage(nil), metadata...)
		out = append(out, e)
	}
	return out, rows.Err()
}

type modelPin struct {
	Present bool
	ModelID string
	Version string
	Schema  twinmodel.DerivedSchema
}

func enforceModelRelation(tx *sql.Tx, edge Edge) error {
	fromPin, err := loadModelPin(tx, edge.TenantID, edge.From)
	if err != nil {
		return fmt.Errorf("load source twin model pin: %w", err)
	}
	toPin, err := loadModelPin(tx, edge.TenantID, edge.To)
	if err != nil {
		return fmt.Errorf("load target twin model pin: %w", err)
	}
	if !fromPin.Present || !toPin.Present {
		return nil
	}

	violations := relationshipViolations(fromPin, edge.RelationType, toPin, edge.To.Type, edge.Direction == "BIDIRECTIONAL")
	if edge.Direction == "BIDIRECTIONAL" {
		violations = append(violations,
			relationshipViolations(toPin, edge.RelationType, fromPin, edge.From.Type, true)...)
	}
	if len(violations) == 0 {
		return nil
	}
	mode := relationEnforcementMode()
	if edge.Direction == "BIDIRECTIONAL" || mode == "reject" {
		return fmt.Errorf("%w: %s", ErrRelationNotAllowedByModel, strings.Join(violations, "; "))
	}

	modelRelationViolationMetric.Inc()
	log.Printf("WARN twin model relation violation tenant_id=%s relation_type=%s from_type=%s from_id=%s to_type=%s to_id=%s from_model_id=%s from_model_version=%s to_model_id=%s to_model_version=%s violation_count=%d enforcement_mode=warn violations=%q",
		edge.TenantID, edge.RelationType, edge.From.Type, edge.From.ID, edge.To.Type, edge.To.ID,
		fromPin.ModelID, fromPin.Version, toPin.ModelID, toPin.Version, len(violations), strings.Join(violations, "; "))
	return nil
}

func loadModelPin(tx *sql.Tx, tenantID string, ref EntityRef) (modelPin, error) {
	var modelID, version sql.NullString
	var schema []byte
	err := tx.QueryRow(`
		SELECT tr.model_id, tr.model_version, tm.schema
		  FROM twin_registry tr
		  LEFT JOIN twin_model tm
		    ON tm.tenant_id=tr.tenant_id
		   AND tm.model_id=tr.model_id
		   AND tm.version=tr.model_version
		 WHERE tr.tenant_id=$1 AND tr.entity_type=$2 AND tr.entity_id=$3`,
		tenantID, ref.Type, ref.ID).Scan(&modelID, &version, &schema)
	if errors.Is(err, sql.ErrNoRows) {
		return modelPin{}, nil
	}
	if err != nil {
		return modelPin{}, err
	}
	if !modelID.Valid && !version.Valid {
		return modelPin{}, nil
	}
	if !modelID.Valid || !version.Valid {
		return modelPin{}, fmt.Errorf("incomplete model pin for %s/%s", ref.Type, ref.ID)
	}
	if len(schema) == 0 {
		return modelPin{}, fmt.Errorf("pinned model %s/%s is missing from catalog", modelID.String, version.String)
	}
	var derived twinmodel.DerivedSchema
	if err := json.Unmarshal(schema, &derived); err != nil {
		return modelPin{}, fmt.Errorf("decode model %s/%s schema: %w", modelID.String, version.String, err)
	}
	if derived.ModelID != modelID.String || derived.Version != version.String || derived.Kind != ref.Type {
		return modelPin{}, fmt.Errorf("pinned model %s/%s schema identity does not match %s", modelID.String, version.String, ref.Type)
	}
	return modelPin{Present: true, ModelID: modelID.String, Version: version.String, Schema: derived}, nil
}

func relationshipViolations(source modelPin, relationType string, target modelPin, targetType string, requireBidirectional bool) []string {
	relation, declared := source.Schema.Relationships[relationType]
	if !declared {
		return []string{fmt.Sprintf("model %s/%s does not declare %s", source.ModelID, source.Version, relationType)}
	}
	violations := make([]string, 0, 3)
	if !contains(relation.Target, target.ModelID) {
		violations = append(violations, fmt.Sprintf("model %s/%s does not authorize target model %s", source.ModelID, source.Version, target.ModelID))
	}
	if !contains(relation.TargetEntityTypes, targetType) {
		violations = append(violations, fmt.Sprintf("model %s/%s does not authorize target entity type %s", source.ModelID, source.Version, targetType))
	}
	if requireBidirectional && !relation.Bidirectional {
		violations = append(violations, fmt.Sprintf("model %s/%s does not declare %s bidirectional", source.ModelID, source.Version, relationType))
	}
	return violations
}

func relationEnforcementMode() string {
	if strings.EqualFold(strings.TrimSpace(os.Getenv("TWIN_MODEL_RELATION_ENFORCE")), "reject") {
		return "reject"
	}
	return "warn"
}

func bidirectionalLockKey(edge Edge) string {
	endpoint := func(ref EntityRef) string {
		return fmt.Sprintf("%d:%s%d:%s", utf8.RuneCountInString(ref.Type), ref.Type, utf8.RuneCountInString(ref.ID), ref.ID)
	}
	fromEndpoint, toEndpoint := endpoint(edge.From), endpoint(edge.To)
	if fromEndpoint > toEndpoint {
		fromEndpoint, toEndpoint = toEndpoint, fromEndpoint
	}
	part := func(value string) string { return fmt.Sprintf("%d:%s", utf8.RuneCountInString(value), value) }
	return part(edge.TenantID) + part(edge.RelationTypeGroup) + part(edge.RelationType) +
		part(fromEndpoint) + part(toEndpoint)
}

func BackfillFromLegacyRelations(db *sql.DB) (int64, error) {
	now := time.Now().UnixMilli()
	res, err := db.Exec(`
		WITH resolved AS (
			SELECT COALESCE(fa.tenant_id, fd.tenant_id)::uuid AS tenant_id,
			       r.from_id, r.from_type, r.to_id, r.to_type,
			       COALESCE(NULLIF(r.relation_type_group, ''), 'COMMON') AS relation_type_group,
			       r.relation_type,
			       COALESCE(NULLIF(r.additional_info, ''), '{}')::jsonb AS metadata
			  FROM relation r
			  LEFT JOIN asset fa ON r.from_type = 'ASSET' AND fa.id = r.from_id
			  LEFT JOIN device fd ON r.from_type = 'DEVICE' AND fd.id = r.from_id
			  LEFT JOIN asset ta ON r.to_type = 'ASSET' AND ta.id = r.to_id
			  LEFT JOIN device td ON r.to_type = 'DEVICE' AND td.id = r.to_id
			 WHERE COALESCE(fa.tenant_id, fd.tenant_id) IS NOT NULL
			   AND COALESCE(ta.tenant_id, td.tenant_id) = COALESCE(fa.tenant_id, fd.tenant_id)
		)
		INSERT INTO topology_edge
			(tenant_id, from_id, from_type, to_id, to_type, relation_type_group,
			 relation_type, direction, metadata, created_time, updated_time, version)
		SELECT tenant_id, from_id, from_type, to_id, to_type, relation_type_group,
		       relation_type, 'DIRECTED', metadata, $1, $1, 1
		  FROM resolved
		ON CONFLICT (tenant_id, from_id, from_type, relation_type_group, relation_type, to_id, to_type, direction)
		DO NOTHING`, now)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func Neighbors(db *sql.DB, root EntityRef, direction string, relationTypes []string) ([]EntityRef, error) {
	root.Type = normalizeEntityType(root.Type)
	direction = strings.ToUpper(strings.TrimSpace(direction))
	if direction == "" {
		direction = "FROM"
	}
	if len(relationTypes) == 0 {
		relationTypes = []string{"Contains"}
	}

	neighbors, err := neighborsFromTopology(db, root, direction, relationTypes)
	if err != nil {
		return nil, err
	}
	if len(neighbors) > 0 {
		return neighbors, nil
	}
	return neighborsFromLegacyRelation(db, root, direction, relationTypes)
}

func neighborsFromTopology(db *sql.DB, root EntityRef, direction string, relationTypes []string) ([]EntityRef, error) {
	var rows *sql.Rows
	var err error
	if direction == "FROM" {
		rows, err = db.Query(`
			SELECT DISTINCT
			       CASE WHEN direction='BIDIRECTIONAL' AND to_id=$1 AND to_type=$2 THEN from_id ELSE to_id END::text,
			       CASE WHEN direction='BIDIRECTIONAL' AND to_id=$1 AND to_type=$2 THEN from_type ELSE to_type END
			  FROM topology_edge
			 WHERE ((direction='DIRECTED' AND from_id=$1 AND from_type=$2)
			     OR (direction='BIDIRECTIONAL' AND ((from_id=$1 AND from_type=$2) OR (to_id=$1 AND to_type=$2))))
			   AND relation_type_group = 'COMMON'
			   AND relation_type = ANY($3)`,
			root.ID, root.Type, pq.Array(relationTypes))
	} else {
		rows, err = db.Query(`
			SELECT DISTINCT
			       CASE WHEN direction='BIDIRECTIONAL' AND from_id=$1 AND from_type=$2 THEN to_id ELSE from_id END::text,
			       CASE WHEN direction='BIDIRECTIONAL' AND from_id=$1 AND from_type=$2 THEN to_type ELSE from_type END
			  FROM topology_edge
			 WHERE ((direction='DIRECTED' AND to_id=$1 AND to_type=$2)
			     OR (direction='BIDIRECTIONAL' AND ((from_id=$1 AND from_type=$2) OR (to_id=$1 AND to_type=$2))))
			   AND relation_type_group = 'COMMON'
			   AND relation_type = ANY($3)`,
			root.ID, root.Type, pq.Array(relationTypes))
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanNeighbors(rows)
}

func neighborsFromLegacyRelation(db *sql.DB, root EntityRef, direction string, relationTypes []string) ([]EntityRef, error) {
	var rows *sql.Rows
	var err error
	if direction == "FROM" {
		rows, err = db.Query(`
			SELECT to_id::text, to_type
			  FROM relation
			 WHERE from_id = $1 AND from_type = $2
			   AND relation_type_group = 'COMMON'
			   AND relation_type = ANY($3)`,
			root.ID, root.Type, pq.Array(relationTypes))
	} else {
		rows, err = db.Query(`
			SELECT from_id::text, from_type
			  FROM relation
			 WHERE to_id = $1 AND to_type = $2
			   AND relation_type_group = 'COMMON'
			   AND relation_type = ANY($3)`,
			root.ID, root.Type, pq.Array(relationTypes))
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanNeighbors(rows)
}

func scanNeighbors(rows *sql.Rows) ([]EntityRef, error) {
	out := []EntityRef{}
	for rows.Next() {
		var ref EntityRef
		if err := rows.Scan(&ref.ID, &ref.Type); err != nil {
			return nil, err
		}
		ref.Type = normalizeEntityType(ref.Type)
		out = append(out, ref)
	}
	return out, rows.Err()
}

// Default limits for the Phase 3 twin expand API, fixed by the synthetic
// recursive-CTE benchmark (BenchmarkExpandCTEDepth5 / BenchmarkExpandCTEDepth10
// over a 30,000-edge synthetic graph, 2026-08-07). Do not raise either value
// without re-running the benchmark and recording the new baseline in
// docs/DIGITAL_TWIN.md.
const (
	DefaultExpandMaxDepth = 10
	DefaultExpandMaxNodes = 5000
)

// NeighborsTenant is the tenant-scoped traversal entry point for the Phase 3
// twin API. Unlike Neighbors it carries the tenant predicate in every SQL
// branch and reads a single deduplicating union of topology_edge plus legacy
// relation rows that are not present in topology_edge, so a legacy-only child
// is never masked by the presence of any topology child (the all-or-nothing
// fallback of Neighbors is not used here). BIDIRECTIONAL/DIRECTED semantics
// match Neighbors. root is expected to belong to tenantID; callers resolve the
// root tenant before calling.
func NeighborsTenant(db *sql.DB, tenantID string, root EntityRef, direction string, relationTypes []string) ([]EntityRef, error) {
	root.Type = normalizeEntityType(root.Type)
	direction = strings.ToUpper(strings.TrimSpace(direction))
	if direction == "" {
		direction = "FROM"
	}
	if len(relationTypes) == 0 {
		relationTypes = []string{"Contains"}
	}
	query := "SELECT DISTINCT nb_id::text AS nb_id, nb_type FROM (\n" +
		neighborUnionSQL(direction, "$1", "$4") + "\n) nbrs\n" +
		" WHERE nbrs.node_id = $2 AND nbrs.node_type = $3"
	rows, err := db.Query(query, tenantID, root.ID, root.Type, pq.Array(relationTypes))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanNeighbors(rows)
}

// ExpandWithCTE returns every node reachable from root within maxDepth hops
// using a tenant-scoped WITH RECURSIVE CTE. The recursive step re-uses the
// same deduplicating, tenant-predicated edge-source union as NeighborsTenant,
// so the recursive traversal and the one-hop read can never disagree on which
// edges are visible. maxDepth and maxNodes must be >= 1; maxNodes caps the
// number of distinct nodes returned (including root), and exceeding it returns
// ErrTraversalBudget instead of an unbounded result. The result is
// deterministic (ordered by depth, then normalized type, then id) for stable
// pagination, and the recursion is cycle-safe (each row carries its own
// visited path) so bidirectional rings terminate at the depth ceiling.
func ExpandWithCTE(db *sql.DB, tenantID string, root EntityRef, direction string, relationTypes []string, maxDepth, maxNodes int) ([]EntityRef, error) {
	if maxDepth < 1 {
		return nil, fmt.Errorf("topology expand maxDepth must be >= 1: %d", maxDepth)
	}
	if maxNodes < 1 {
		return nil, fmt.Errorf("topology expand maxNodes must be >= 1: %d", maxNodes)
	}
	root.Type = normalizeEntityType(root.Type)
	direction = strings.ToUpper(strings.TrimSpace(direction))
	if direction == "" {
		direction = "FROM"
	}
	if len(relationTypes) == 0 {
		relationTypes = []string{"Contains"}
	}
	query := `
WITH RECURSIVE expand(node_type, node_id, depth, path) AS (
    SELECT $2::text, $3::uuid, 0, ARRAY[$2::text || ':' || $3::text]
  UNION ALL
    SELECT e2.nb_type, e2.nb_id, e.depth + 1, e.path || (e2.nb_type || ':' || e2.nb_id::text)
      FROM expand e
      JOIN (
` + neighborUnionSQL(direction, "$4", "$5") + `
      ) e2 ON e2.node_id = e.node_id AND e2.node_type = e.node_type
     WHERE e.depth < $1
       AND NOT (e2.nb_type || ':' || e2.nb_id::text = ANY(e.path))
)
SELECT node_type, node_id::text AS node_id, depth
  FROM (
    SELECT DISTINCT ON (node_type, node_id) node_type, node_id, depth
      FROM expand
     ORDER BY node_type, node_id, depth
  ) deduped
 ORDER BY depth, node_type, node_id
 LIMIT $6`
	rows, err := db.Query(query, maxDepth, root.Type, root.ID, tenantID, pq.Array(relationTypes), maxNodes+1)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]EntityRef, 0, maxNodes+1)
	for rows.Next() {
		var ref EntityRef
		var depth int
		if err := rows.Scan(&ref.Type, &ref.ID, &depth); err != nil {
			return nil, err
		}
		ref.Type = normalizeEntityType(ref.Type)
		out = append(out, ref)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(out) > maxNodes {
		return nil, ErrTraversalBudget
	}
	return out, nil
}

// neighborUnionSQL builds the tenant-scoped, deduplicating edge-source UNION
// that backs both NeighborsTenant (one hop) and the recursive step of
// ExpandWithCTE. It returns every in-tenant edge in the requested direction as
// (node_id, node_type, nb_id, nb_type): node is the endpoint the traversal is
// standing on and nb is the neighbor on the far side. Every SQL branch carries
// the tenant predicate: topology_edge is filtered on its tenant_id column, and
// the legacy relation branch resolves the neighbor entity's tenant through the
// entity tables because relation has no tenant_id column. A legacy row whose
// endpoint already exists in topology_edge is masked by NOT EXISTS, so a child
// present in both stores is reported once and a legacy-only child is never
// hidden by an all-or-nothing fallback. The edge source is uncorrelated so the
// recursive term joins it once per level (hash join) instead of probing it per
// frontier row; callers filter it to a single node (NeighborsTenant) or join
// it to the working table (ExpandWithCTE). Keep the two call sites and this
// helper in sync.
//
// The two tokens are SQL fragments chosen by the caller so parameter numbering
// stays consistent with the arguments it passes: for NeighborsTenant they are
// $1 (tenant) and $4 (relation types); for ExpandWithCTE they are $4 and $5.
func neighborUnionSQL(direction, tenant, relTypes string) string {
	if direction == "TO" {
		return fmt.Sprintf(neighborUnionTO, tenant, relTypes)
	}
	return fmt.Sprintf(neighborUnionFROM, tenant, relTypes)
}

// neighborUnionFROM lists the FROM-direction edges (edges leaving the node:
// node=from side, neighbor=to side). %[1]s=tenant, %[2]s=relation types.
const neighborUnionFROM = `
SELECT te.from_id AS node_id, te.from_type AS node_type, te.to_id AS nb_id, te.to_type AS nb_type
  FROM topology_edge te
 WHERE te.tenant_id=%[1]s
   AND te.relation_type_group='COMMON'
   AND te.relation_type = ANY(%[2]s)
   AND te.direction='DIRECTED'
UNION ALL
SELECT te.from_id, te.from_type, te.to_id, te.to_type
  FROM topology_edge te
 WHERE te.tenant_id=%[1]s
   AND te.relation_type_group='COMMON'
   AND te.relation_type = ANY(%[2]s)
   AND te.direction='BIDIRECTIONAL'
UNION ALL
SELECT te.to_id, te.to_type, te.from_id, te.from_type
  FROM topology_edge te
 WHERE te.tenant_id=%[1]s
   AND te.relation_type_group='COMMON'
   AND te.relation_type = ANY(%[2]s)
   AND te.direction='BIDIRECTIONAL'
UNION ALL
SELECT r.from_id AS node_id, r.from_type AS node_type, r.to_id AS nb_id, r.to_type AS nb_type
  FROM relation r
  LEFT JOIN asset ta ON r.to_type='ASSET' AND ta.id=r.to_id
  LEFT JOIN device td ON r.to_type='DEVICE' AND td.id=r.to_id
  LEFT JOIN customer tc ON r.to_type='CUSTOMER' AND tc.id=r.to_id
  LEFT JOIN entity_view tev ON r.to_type='ENTITY_VIEW' AND tev.id=r.to_id
  LEFT JOIN dashboard tda ON r.to_type='DASHBOARD' AND tda.id=r.to_id
  LEFT JOIN device_profile tdp ON r.to_type='DEVICE_PROFILE' AND tdp.id=r.to_id
  LEFT JOIN asset_profile tap ON r.to_type='ASSET_PROFILE' AND tap.id=r.to_id
 WHERE r.relation_type_group='COMMON'
   AND r.relation_type = ANY(%[2]s)
   AND COALESCE(CASE WHEN r.to_type='TENANT' THEN r.to_id END,
                ta.tenant_id, td.tenant_id, tc.tenant_id, tev.tenant_id,
                tda.tenant_id, tdp.tenant_id, tap.tenant_id) = %[1]s
   AND NOT EXISTS (
        SELECT 1 FROM topology_edge te
         WHERE te.tenant_id=%[1]s
           AND te.from_id=r.from_id AND te.from_type=r.from_type
           AND te.to_id=r.to_id AND te.to_type=r.to_type
           AND te.relation_type_group=r.relation_type_group
           AND te.relation_type=r.relation_type)`

// neighborUnionTO lists the TO-direction edges (edges entering the node:
// node=to side, neighbor=from side).
const neighborUnionTO = `
SELECT te.to_id AS node_id, te.to_type AS node_type, te.from_id AS nb_id, te.from_type AS nb_type
  FROM topology_edge te
 WHERE te.tenant_id=%[1]s
   AND te.relation_type_group='COMMON'
   AND te.relation_type = ANY(%[2]s)
   AND te.direction='DIRECTED'
UNION ALL
SELECT te.from_id, te.from_type, te.to_id, te.to_type
  FROM topology_edge te
 WHERE te.tenant_id=%[1]s
   AND te.relation_type_group='COMMON'
   AND te.relation_type = ANY(%[2]s)
   AND te.direction='BIDIRECTIONAL'
UNION ALL
SELECT te.to_id, te.to_type, te.from_id, te.from_type
  FROM topology_edge te
 WHERE te.tenant_id=%[1]s
   AND te.relation_type_group='COMMON'
   AND te.relation_type = ANY(%[2]s)
   AND te.direction='BIDIRECTIONAL'
UNION ALL
SELECT r.to_id AS node_id, r.to_type AS node_type, r.from_id AS nb_id, r.from_type AS nb_type
  FROM relation r
  LEFT JOIN asset fa ON r.from_type='ASSET' AND fa.id=r.from_id
  LEFT JOIN device fd ON r.from_type='DEVICE' AND fd.id=r.from_id
  LEFT JOIN customer fc ON r.from_type='CUSTOMER' AND fc.id=r.from_id
  LEFT JOIN entity_view fev ON r.from_type='ENTITY_VIEW' AND fev.id=r.from_id
  LEFT JOIN dashboard fda ON r.from_type='DASHBOARD' AND fda.id=r.from_id
  LEFT JOIN device_profile fdp ON r.from_type='DEVICE_PROFILE' AND fdp.id=r.from_id
  LEFT JOIN asset_profile fap ON r.from_type='ASSET_PROFILE' AND fap.id=r.from_id
 WHERE r.relation_type_group='COMMON'
   AND r.relation_type = ANY(%[2]s)
   AND COALESCE(CASE WHEN r.from_type='TENANT' THEN r.from_id END,
                fa.tenant_id, fd.tenant_id, fc.tenant_id, fev.tenant_id,
                fda.tenant_id, fdp.tenant_id, fap.tenant_id) = %[1]s
   AND NOT EXISTS (
        SELECT 1 FROM topology_edge te
         WHERE te.tenant_id=%[1]s
           AND te.from_id=r.from_id AND te.from_type=r.from_type
           AND te.to_id=r.to_id AND te.to_type=r.to_type
           AND te.relation_type_group=r.relation_type_group
           AND te.relation_type=r.relation_type)`

func ResolveEntityTenant(db *sql.DB, ref EntityRef) (string, error) {
	return resolveEntityTenant(db, ref)
}

type rowQuerier interface {
	QueryRow(query string, args ...interface{}) *sql.Row
}

func resolveEntityTenant(db rowQuerier, ref EntityRef) (string, error) {
	ref.Type = normalizeEntityType(ref.Type)
	if ref.Type == "TENANT" {
		return ref.ID, nil
	}
	var table string
	switch ref.Type {
	case "ASSET":
		table = "asset"
	case "DEVICE":
		table = "device"
	case "CUSTOMER":
		table = "customer"
	case "ENTITY_VIEW":
		table = "entity_view"
	case "DASHBOARD":
		table = "dashboard"
	case "DEVICE_PROFILE":
		table = "device_profile"
	case "ASSET_PROFILE":
		table = "asset_profile"
	default:
		return "", fmt.Errorf("%w: unsupported type %s", ErrUnknownEntity, ref.Type)
	}
	var tenantID string
	err := db.QueryRow(`SELECT tenant_id::text FROM `+table+` WHERE id = $1`, ref.ID).Scan(&tenantID)
	if errors.Is(err, sql.ErrNoRows) {
		return "", fmt.Errorf("%w: %s/%s", ErrUnknownEntity, ref.Type, ref.ID)
	}
	return tenantID, err
}

func validateRelationType(db rowQuerier, edge Edge) error {
	var fromAllowed, toAllowed []string
	err := db.QueryRow(`
		SELECT COALESCE(allowed_from_types, ARRAY[]::text[]),
		       COALESCE(allowed_to_types, ARRAY[]::text[])
		  FROM topology_relation_type WHERE name = $1`,
		edge.RelationType).Scan(pq.Array(&fromAllowed), pq.Array(&toAllowed))
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("%w: %s", ErrInvalidRelationType, edge.RelationType)
	}
	if err != nil {
		return err
	}
	if !contains(fromAllowed, edge.From.Type) || !contains(toAllowed, edge.To.Type) {
		return fmt.Errorf("%w: %s %s -> %s", ErrInvalidRelationType, edge.RelationType, edge.From.Type, edge.To.Type)
	}
	return nil
}

func normalizeEdge(edge Edge) Edge {
	edge.From.Type = normalizeEntityType(edge.From.Type)
	edge.To.Type = normalizeEntityType(edge.To.Type)
	edge.RelationType = strings.TrimSpace(edge.RelationType)
	edge.RelationTypeGroup = strings.ToUpper(strings.TrimSpace(edge.RelationTypeGroup))
	if edge.RelationTypeGroup == "" {
		edge.RelationTypeGroup = "COMMON"
	}
	edge.Direction = strings.ToUpper(strings.TrimSpace(edge.Direction))
	if edge.Direction == "" {
		edge.Direction = "DIRECTED"
	}
	return edge
}

func normalizeFilter(filter EdgeFilter) EdgeFilter {
	filter.From.Type = normalizeEntityType(filter.From.Type)
	filter.To.Type = normalizeEntityType(filter.To.Type)
	filter.RelationType = strings.TrimSpace(filter.RelationType)
	filter.RelationTypeGroup = strings.ToUpper(strings.TrimSpace(filter.RelationTypeGroup))
	if filter.RelationTypeGroup == "" {
		filter.RelationTypeGroup = "COMMON"
	}
	return filter
}

func normalizeEntityType(v string) string {
	return strings.ToUpper(strings.TrimSpace(v))
}

func contains(values []string, want string) bool {
	for _, v := range values {
		if v == want {
			return true
		}
	}
	return false
}
