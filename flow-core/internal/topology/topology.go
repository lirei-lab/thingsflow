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
	"strings"
	"time"

	"github.com/lib/pq"
)

var (
	ErrCrossTenant         = errors.New("topology relation crosses tenants")
	ErrInvalidRelationType = errors.New("invalid topology relation type")
	ErrUnknownEntity       = errors.New("unknown topology entity")
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
	if edge.Metadata == nil || len(edge.Metadata) == 0 {
		edge.Metadata = json.RawMessage(`{}`)
	}
	if !json.Valid(edge.Metadata) {
		return fmt.Errorf("invalid edge metadata json")
	}
	if err := validateRelationType(db, edge); err != nil {
		return err
	}
	fromTenant, err := ResolveEntityTenant(db, edge.From)
	if err != nil {
		return err
	}
	toTenant, err := ResolveEntityTenant(db, edge.To)
	if err != nil {
		return err
	}
	if fromTenant != edge.TenantID || toTenant != edge.TenantID {
		return fmt.Errorf("%w: from=%s to=%s expected=%s", ErrCrossTenant, fromTenant, toTenant, edge.TenantID)
	}

	now := time.Now().UnixMilli()
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.Exec(`
		INSERT INTO topology_edge
			(tenant_id, from_id, from_type, to_id, to_type, relation_type_group,
			 relation_type, direction, metadata, created_time, updated_time, version)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb, $10, $10, 1)
		ON CONFLICT (tenant_id, from_id, from_type, relation_type_group, relation_type, to_id, to_type)
		DO UPDATE SET
			direction = EXCLUDED.direction,
			metadata = EXCLUDED.metadata,
			updated_time = EXCLUDED.updated_time,
			version = topology_edge.version + 1`,
		edge.TenantID, edge.From.ID, edge.From.Type, edge.To.ID, edge.To.Type,
		edge.RelationTypeGroup, edge.RelationType, edge.Direction, string(edge.Metadata), now); err != nil {
		return err
	}

	if _, err := tx.Exec(`
		INSERT INTO relation
			(from_id, from_type, to_id, to_type, relation_type_group, relation_type, additional_info, version)
		VALUES ($1, $2, $3, $4, $5, $6, $7, 1)
		ON CONFLICT (from_id, from_type, relation_type_group, relation_type, to_id, to_type)
		DO UPDATE SET additional_info = EXCLUDED.additional_info, version = relation.version + 1`,
		edge.From.ID, edge.From.Type, edge.To.ID, edge.To.Type,
		edge.RelationTypeGroup, edge.RelationType, string(edge.Metadata)); err != nil {
		return err
	}

	return tx.Commit()
}

func DeleteEdge(db *sql.DB, filter EdgeFilter) error {
	filter = normalizeFilter(filter)
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`
		DELETE FROM topology_edge
		 WHERE tenant_id = $1 AND from_id = $2 AND from_type = $3
		   AND to_id = $4 AND to_type = $5
		   AND relation_type_group = $6 AND relation_type = $7`,
		filter.TenantID, filter.From.ID, filter.From.Type, filter.To.ID, filter.To.Type,
		filter.RelationTypeGroup, filter.RelationType); err != nil {
		return err
	}
	if _, err := tx.Exec(`
		DELETE FROM relation
		 WHERE from_id = $1 AND from_type = $2 AND to_id = $3 AND to_type = $4
		   AND relation_type_group = $5 AND relation_type = $6`,
		filter.From.ID, filter.From.Type, filter.To.ID, filter.To.Type,
		filter.RelationTypeGroup, filter.RelationType); err != nil {
		return err
	}
	return tx.Commit()
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
		ON CONFLICT (tenant_id, from_id, from_type, relation_type_group, relation_type, to_id, to_type)
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
			SELECT to_id::text, to_type
			  FROM topology_edge
			 WHERE from_id = $1 AND from_type = $2
			   AND relation_type_group = 'COMMON'
			   AND relation_type = ANY($3)`,
			root.ID, root.Type, pq.Array(relationTypes))
	} else {
		rows, err = db.Query(`
			SELECT from_id::text, from_type
			  FROM topology_edge
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

func ResolveEntityTenant(db *sql.DB, ref EntityRef) (string, error) {
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

func validateRelationType(db *sql.DB, edge Edge) error {
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
	if edge.RelationTypeGroup == "" {
		edge.RelationTypeGroup = "COMMON"
	}
	if edge.Direction == "" {
		edge.Direction = "DIRECTED"
	}
	return edge
}

func normalizeFilter(filter EdgeFilter) EdgeFilter {
	filter.From.Type = normalizeEntityType(filter.From.Type)
	filter.To.Type = normalizeEntityType(filter.To.Type)
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
