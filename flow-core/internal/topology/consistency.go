package topology

import (
	"context"
	"database/sql"
	"time"
)

type ConsistencyReport struct {
	Status                      string `json:"status"`
	CheckedAt                   int64  `json:"checkedAt"`
	TopologyEdges               int64  `json:"topologyEdges"`
	LegacyRelations             int64  `json:"legacyRelations"`
	GovernedLegacyRelations     int64  `json:"governedLegacyRelations"`
	LegacyMissingTopology       int64  `json:"legacyMissingTopology"`
	TopologyMissingLegacy       int64  `json:"topologyMissingLegacy"`
	CrossTenantLegacyRelations  int64  `json:"crossTenantLegacyRelations"`
	InvalidTopologyRelationType int64  `json:"invalidTopologyRelationType"`
}

type RepairBackfillResult struct {
	DryRun      bool              `json:"dryRun"`
	WouldInsert int64             `json:"wouldInsert"`
	Inserted    int64             `json:"inserted"`
	Before      ConsistencyReport `json:"before"`
	After       ConsistencyReport `json:"after"`
}

func CheckConsistency(db *sql.DB) (ConsistencyReport, error) {
	return CheckConsistencyContext(context.Background(), db)
}

func CheckConsistencyContext(ctx context.Context, db *sql.DB) (ConsistencyReport, error) {
	var report ConsistencyReport
	report.CheckedAt = time.Now().UnixMilli()

	queries := []struct {
		dst *int64
		sql string
	}{
		{&report.TopologyEdges, `SELECT count(*) FROM topology_edge`},
		{&report.LegacyRelations, `SELECT count(*) FROM relation`},
		{&report.GovernedLegacyRelations, governedLegacyCountSQL},
		{&report.LegacyMissingTopology, legacyMissingTopologySQL},
		{&report.TopologyMissingLegacy, topologyMissingLegacySQL},
		{&report.CrossTenantLegacyRelations, crossTenantLegacySQL},
		{&report.InvalidTopologyRelationType, invalidTopologyRelationTypeSQL},
	}
	for _, q := range queries {
		if err := db.QueryRowContext(ctx, q.sql).Scan(q.dst); err != nil {
			return report, err
		}
	}

	report.Status = "OK"
	if report.LegacyMissingTopology > 0 ||
		report.TopologyMissingLegacy > 0 ||
		report.CrossTenantLegacyRelations > 0 ||
		report.InvalidTopologyRelationType > 0 {
		report.Status = "WARN"
	}
	return report, nil
}

func RepairBackfill(db *sql.DB, dryRun bool) (RepairBackfillResult, error) {
	return RepairBackfillContext(context.Background(), db, dryRun)
}

func RepairBackfillContext(ctx context.Context, db *sql.DB, dryRun bool) (RepairBackfillResult, error) {
	before, err := CheckConsistencyContext(ctx, db)
	if err != nil {
		return RepairBackfillResult{}, err
	}
	result := RepairBackfillResult{
		DryRun:      dryRun,
		WouldInsert: before.LegacyMissingTopology,
		Before:      before,
		After:       before,
	}
	if dryRun || before.LegacyMissingTopology == 0 {
		return result, nil
	}

	inserted, err := backfillMissingTopologyEdgesContext(ctx, db)
	if err != nil {
		return result, err
	}
	after, err := CheckConsistencyContext(ctx, db)
	if err != nil {
		return result, err
	}
	result.Inserted = inserted
	result.After = after
	return result, nil
}

func backfillMissingTopologyEdges(db *sql.DB) (int64, error) {
	return backfillMissingTopologyEdgesContext(context.Background(), db)
}

func backfillMissingTopologyEdgesContext(ctx context.Context, db *sql.DB) (int64, error) {
	now := time.Now().UnixMilli()
	res, err := db.ExecContext(ctx, `
		`+governedLegacyBaseSQL+`
		INSERT INTO topology_edge
			(tenant_id, from_id, from_type, to_id, to_type, relation_type_group,
			 relation_type, direction, metadata, created_time, updated_time, version)
		SELECT r.from_tenant_id, r.from_id, r.from_type, r.to_id, r.to_type,
		       r.relation_type_group, r.relation_type, 'DIRECTED',
		       COALESCE(NULLIF(r.additional_info, ''), '{}')::jsonb, $1, $1, 1
		  FROM resolved r
		  JOIN topology_relation_type rt ON rt.name = r.relation_type
		  LEFT JOIN topology_edge te
		    ON te.tenant_id = r.from_tenant_id
		   AND te.from_id = r.from_id
		   AND te.from_type = r.from_type
		   AND te.to_id = r.to_id
		   AND te.to_type = r.to_type
		   AND te.relation_type_group = r.relation_type_group
		   AND te.relation_type = r.relation_type
		 WHERE r.from_tenant_id IS NOT NULL
		   AND r.to_tenant_id = r.from_tenant_id
		   AND r.from_type = ANY(rt.allowed_from_types)
		   AND r.to_type = ANY(rt.allowed_to_types)
		   AND te.tenant_id IS NULL
		ON CONFLICT (tenant_id, from_id, from_type, relation_type_group, relation_type, to_id, to_type)
		DO NOTHING`, now)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

const governedLegacyBaseSQL = `
	WITH resolved AS (
		SELECT COALESCE(CASE WHEN r.from_type = 'TENANT' THEN r.from_id END, fa.tenant_id, fd.tenant_id, fc.tenant_id, fev.tenant_id, fda.tenant_id, fdp.tenant_id, fap.tenant_id)::uuid AS from_tenant_id,
		       COALESCE(ta.tenant_id, td.tenant_id, tc.tenant_id, tev.tenant_id, tda.tenant_id, tdp.tenant_id, tap.tenant_id)::uuid AS to_tenant_id,
		       r.from_id, r.from_type, r.to_id, r.to_type,
		       COALESCE(NULLIF(r.relation_type_group, ''), 'COMMON') AS relation_type_group,
		       r.relation_type,
		       r.additional_info
		  FROM relation r
		  LEFT JOIN asset fa ON r.from_type = 'ASSET' AND fa.id = r.from_id
		  LEFT JOIN device fd ON r.from_type = 'DEVICE' AND fd.id = r.from_id
		  LEFT JOIN customer fc ON r.from_type = 'CUSTOMER' AND fc.id = r.from_id
		  LEFT JOIN entity_view fev ON r.from_type = 'ENTITY_VIEW' AND fev.id = r.from_id
		  LEFT JOIN dashboard fda ON r.from_type = 'DASHBOARD' AND fda.id = r.from_id
		  LEFT JOIN device_profile fdp ON r.from_type = 'DEVICE_PROFILE' AND fdp.id = r.from_id
		  LEFT JOIN asset_profile fap ON r.from_type = 'ASSET_PROFILE' AND fap.id = r.from_id
		  LEFT JOIN asset ta ON r.to_type = 'ASSET' AND ta.id = r.to_id
		  LEFT JOIN device td ON r.to_type = 'DEVICE' AND td.id = r.to_id
		  LEFT JOIN customer tc ON r.to_type = 'CUSTOMER' AND tc.id = r.to_id
		  LEFT JOIN entity_view tev ON r.to_type = 'ENTITY_VIEW' AND tev.id = r.to_id
		  LEFT JOIN dashboard tda ON r.to_type = 'DASHBOARD' AND tda.id = r.to_id
		  LEFT JOIN device_profile tdp ON r.to_type = 'DEVICE_PROFILE' AND tdp.id = r.to_id
		  LEFT JOIN asset_profile tap ON r.to_type = 'ASSET_PROFILE' AND tap.id = r.to_id
	)
`

const governedLegacyCountSQL = governedLegacyBaseSQL + `
	SELECT count(*)
	  FROM resolved r
	  JOIN topology_relation_type rt ON rt.name = r.relation_type
	 WHERE r.from_tenant_id IS NOT NULL
	   AND r.to_tenant_id = r.from_tenant_id
	   AND r.from_type = ANY(rt.allowed_from_types)
	   AND r.to_type = ANY(rt.allowed_to_types)`

const legacyMissingTopologySQL = governedLegacyBaseSQL + `
	SELECT count(*)
	  FROM resolved r
	  JOIN topology_relation_type rt ON rt.name = r.relation_type
	  LEFT JOIN topology_edge te
	    ON te.tenant_id = r.from_tenant_id
	   AND te.from_id = r.from_id
	   AND te.from_type = r.from_type
	   AND te.to_id = r.to_id
	   AND te.to_type = r.to_type
	   AND te.relation_type_group = r.relation_type_group
	   AND te.relation_type = r.relation_type
	 WHERE r.from_tenant_id IS NOT NULL
	   AND r.to_tenant_id = r.from_tenant_id
	   AND r.from_type = ANY(rt.allowed_from_types)
	   AND r.to_type = ANY(rt.allowed_to_types)
	   AND te.tenant_id IS NULL`

const topologyMissingLegacySQL = `
	SELECT count(*)
	  FROM topology_edge te
	  LEFT JOIN relation r
	    ON r.from_id = te.from_id
	   AND r.from_type = te.from_type
	   AND r.to_id = te.to_id
	   AND r.to_type = te.to_type
	   AND COALESCE(NULLIF(r.relation_type_group, ''), 'COMMON') = te.relation_type_group
	   AND r.relation_type = te.relation_type
	 WHERE r.from_id IS NULL`

const crossTenantLegacySQL = governedLegacyBaseSQL + `
	SELECT count(*)
	  FROM resolved
	 WHERE from_tenant_id IS NOT NULL
	   AND to_tenant_id IS NOT NULL
	   AND from_tenant_id <> to_tenant_id`

const invalidTopologyRelationTypeSQL = `
	SELECT count(*)
	  FROM topology_edge te
	  LEFT JOIN topology_relation_type rt ON rt.name = te.relation_type
	 WHERE rt.name IS NULL
	    OR NOT (te.from_type = ANY(rt.allowed_from_types))
	    OR NOT (te.to_type = ANY(rt.allowed_to_types))`
