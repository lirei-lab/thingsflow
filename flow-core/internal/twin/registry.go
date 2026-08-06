package twin

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
)

// RegistryRow is the governed identity and model metadata for a projected twin.
type RegistryRow struct {
	TenantID     string
	ThingID      string
	EntityType   string
	EntityID     string
	PolicyID     string
	Definition   string
	Attributes   map[string]interface{}
	ModelID      string
	ModelVersion string
	Version      int64
}

// The device/asset upsert SQL is shared between the whole-table backfill and
// the single-entity sync: the %s slot receives either nothing (backfill) or a
// `AND tenant_id = $1 AND id = $2` filter (sync), so both paths project rows
// with exactly the same shape and can never drift apart.
const registryDeviceUpsertSQLTemplate = `
	WITH src AS (
		SELECT d.tenant_id, d.id, d.created_time,
		       d.tenant_id::text || ':device:' || d.id::text AS thing_id,
		       'tenant:' || d.tenant_id::text || ':default' AS policy_id,
		       COALESCE(tr.model_id, tm.model_id) AS model_id,
		       CASE WHEN tr.model_id IS NOT NULL THEN tr.model_version ELSE tm.version END AS model_version,
		       CASE
		         WHEN tr.model_id IS NOT NULL THEN tr.definition
		         WHEN tm.model_id IS NOT NULL THEN
		           'thingsflow:device:' || tm.model_id || ':' || tm.version
		         ELSE 'thingsflow:device:' ||
		           COALESCE(NULLIF(trim(both '_' from regexp_replace(lower(COALESCE(d.type, 'default')), '[^a-z0-9_]+', '_', 'g')), ''), 'default') ||
		           ':1.0.0'
		       END AS definition,
		       -- additional_info is only USUALLY an object: legacy rows and API
		       -- writers can hold a JSON scalar/array, and concatenating a
		       -- scalar with an object makes the whole statement throw --
		       -- killing the backfill for EVERY entity because of one row.
		       -- Non-objects degrade to '{}'.
		       COALESCE(CASE WHEN jsonb_typeof(NULLIF(trim(COALESCE(d.additional_info, '')), '')::jsonb) = 'object'
		            THEN NULLIF(trim(COALESCE(d.additional_info, '')), '')::jsonb
		            ELSE '{}'::jsonb END, '{}'::jsonb) ||
		       jsonb_build_object(
		           'id', d.id::text,
		           'entityType', 'DEVICE',
		           'tenantId', d.tenant_id::text,
		           'createdTime', d.created_time,
		           'name', d.name,
		           'type', COALESCE(d.type, ''),
		           'label', COALESCE(d.label, '')
		       ) AS attributes
		  FROM device d
		  LEFT JOIN twin_registry tr
		    ON tr.tenant_id = d.tenant_id
		   AND tr.entity_type = 'DEVICE'
		   AND tr.entity_id = d.id
		  LEFT JOIN LATERAL (
		       SELECT candidate.model_id, candidate.version
		         FROM twin_model candidate
		        WHERE candidate.tenant_id = d.tenant_id
		          AND candidate.kind = 'DEVICE'
		          AND ((tr.model_id IS NOT NULL
		                AND candidate.model_id = tr.model_id
		                AND candidate.version = tr.model_version)
		            OR (tr.model_id IS NULL
		                AND candidate.model_id = COALESCE(NULLIF(trim(both '_' from regexp_replace(lower(COALESCE(d.type, 'default')), '[^a-z0-9_]+', '_', 'g')), ''), 'default')))
		        ORDER BY string_to_array(candidate.version,'.')::int[] DESC
		        LIMIT 1
		  ) tm ON true
		 WHERE d.tenant_id IS NOT NULL%s
	)
	INSERT INTO twin_registry
		(tenant_id, thing_id, entity_type, entity_id, policy_id, definition, attributes,
		 model_id, model_version, created_time, updated_time)
	SELECT tenant_id, thing_id, 'DEVICE', id, policy_id, definition, attributes,
	       model_id, model_version, created_time, created_time
	  FROM src
	ON CONFLICT (tenant_id, entity_type, entity_id) DO UPDATE
	   SET thing_id = EXCLUDED.thing_id,
	       policy_id = EXCLUDED.policy_id,
	       definition = EXCLUDED.definition,
	       attributes = EXCLUDED.attributes,
	       model_id = EXCLUDED.model_id,
	       model_version = EXCLUDED.model_version,
	       updated_time = EXCLUDED.updated_time,
	       version = twin_registry.version + 1
	 WHERE twin_registry.thing_id IS DISTINCT FROM EXCLUDED.thing_id
	    OR twin_registry.policy_id IS DISTINCT FROM EXCLUDED.policy_id
	    OR twin_registry.definition IS DISTINCT FROM EXCLUDED.definition
	    OR twin_registry.attributes IS DISTINCT FROM EXCLUDED.attributes
	    OR twin_registry.model_id IS DISTINCT FROM EXCLUDED.model_id
	    OR twin_registry.model_version IS DISTINCT FROM EXCLUDED.model_version`

const registryAssetUpsertSQLTemplate = `
	WITH src AS (
		SELECT a.tenant_id, a.id, a.created_time,
		       a.tenant_id::text || ':asset:' || a.id::text AS thing_id,
		       'tenant:' || a.tenant_id::text || ':default' AS policy_id,
		       COALESCE(tr.model_id, tm.model_id) AS model_id,
		       CASE WHEN tr.model_id IS NOT NULL THEN tr.model_version ELSE tm.version END AS model_version,
		       CASE
		         WHEN tr.model_id IS NOT NULL THEN tr.definition
		         WHEN tm.model_id IS NOT NULL THEN
		           'thingsflow:asset:' || tm.model_id || ':' || tm.version
		         ELSE 'thingsflow:asset:' ||
		           COALESCE(NULLIF(trim(both '_' from regexp_replace(lower(COALESCE(a.type, 'default')), '[^a-z0-9_]+', '_', 'g')), ''), 'default') ||
		           ':1.0.0'
		       END AS definition,
		       -- Non-object additional_info degrades to '{}' — see the device
		       -- template for the rationale.
		       COALESCE(CASE WHEN jsonb_typeof(NULLIF(trim(COALESCE(a.additional_info, '')), '')::jsonb) = 'object'
		            THEN NULLIF(trim(COALESCE(a.additional_info, '')), '')::jsonb
		            ELSE '{}'::jsonb END, '{}'::jsonb) ||
		       jsonb_build_object(
		           'id', a.id::text,
		           'entityType', 'ASSET',
		           'tenantId', a.tenant_id::text,
		           'createdTime', a.created_time,
		           'name', a.name,
		           'type', COALESCE(a.type, ''),
		           'label', COALESCE(a.label, '')
		       ) AS attributes
		  FROM asset a
		  LEFT JOIN twin_registry tr
		    ON tr.tenant_id = a.tenant_id
		   AND tr.entity_type = 'ASSET'
		   AND tr.entity_id = a.id
		  LEFT JOIN LATERAL (
		       SELECT candidate.model_id, candidate.version
		         FROM twin_model candidate
		        WHERE candidate.tenant_id = a.tenant_id
		          AND candidate.kind = 'ASSET'
		          AND ((tr.model_id IS NOT NULL
		                AND candidate.model_id = tr.model_id
		                AND candidate.version = tr.model_version)
		            OR (tr.model_id IS NULL
		                AND candidate.model_id = COALESCE(NULLIF(trim(both '_' from regexp_replace(lower(COALESCE(a.type, 'default')), '[^a-z0-9_]+', '_', 'g')), ''), 'default')))
		        ORDER BY string_to_array(candidate.version,'.')::int[] DESC
		        LIMIT 1
		  ) tm ON true
		 WHERE a.tenant_id IS NOT NULL%s
	)
	INSERT INTO twin_registry
		(tenant_id, thing_id, entity_type, entity_id, policy_id, definition, attributes,
		 model_id, model_version, created_time, updated_time)
	SELECT tenant_id, thing_id, 'ASSET', id, policy_id, definition, attributes,
	       model_id, model_version, created_time, created_time
	  FROM src
	ON CONFLICT (tenant_id, entity_type, entity_id) DO UPDATE
	   SET thing_id = EXCLUDED.thing_id,
	       policy_id = EXCLUDED.policy_id,
	       definition = EXCLUDED.definition,
	       attributes = EXCLUDED.attributes,
	       model_id = EXCLUDED.model_id,
	       model_version = EXCLUDED.model_version,
	       updated_time = EXCLUDED.updated_time,
	       version = twin_registry.version + 1
	 WHERE twin_registry.thing_id IS DISTINCT FROM EXCLUDED.thing_id
	    OR twin_registry.policy_id IS DISTINCT FROM EXCLUDED.policy_id
	    OR twin_registry.definition IS DISTINCT FROM EXCLUDED.definition
	    OR twin_registry.attributes IS DISTINCT FROM EXCLUDED.attributes
	    OR twin_registry.model_id IS DISTINCT FROM EXCLUDED.model_id
	    OR twin_registry.model_version IS DISTINCT FROM EXCLUDED.model_version`

var (
	backfillRegistryDeviceSQL = fmt.Sprintf(registryDeviceUpsertSQLTemplate, "")
	backfillRegistryAssetSQL  = fmt.Sprintf(registryAssetUpsertSQLTemplate, "")
	syncRegistryDeviceSQL     = fmt.Sprintf(registryDeviceUpsertSQLTemplate, " AND d.tenant_id = $1 AND d.id = $2")
	syncRegistryAssetSQL      = fmt.Sprintf(registryAssetUpsertSQLTemplate, " AND a.tenant_id = $1 AND a.id = $2")
)

// twin_registry has no FK to device/asset (migration 0011), so nothing cascades
// on entity delete: without this sweep an orphaned registry row would live
// forever because the upserts above only ever add or update. Load-bearing —
// the runtime delete hook can be missed (crash between commit and hook), and
// this boot-time pass is what re-converges the registry either way.
//
// The predicate is TENANT-AWARE (`tenant_id` must match too): a registry row
// whose entity_id was somehow re-seated under another tenant is an orphan of
// ITS tenant and must go — matching by bare id would let such a row survive
// and keep serving stale identity across a tenant boundary.
const registryOrphanPredicate = `
	    (tr.entity_type = 'DEVICE' AND NOT EXISTS (SELECT 1 FROM device d WHERE d.id = tr.entity_id AND d.tenant_id = tr.tenant_id))
	 OR (tr.entity_type = 'ASSET' AND NOT EXISTS (SELECT 1 FROM asset a WHERE a.id = tr.entity_id AND a.tenant_id = tr.tenant_id))`

// DefaultSweepMax caps how many registry rows one sweep pass may delete unless
// TWIN_REGISTRY_SWEEP_MAX overrides it.
const DefaultSweepMax = 1000

func sweepMax() int64 {
	if v := os.Getenv("TWIN_REGISTRY_SWEEP_MAX"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n > 0 {
			return n
		}
	}
	return DefaultSweepMax
}

// sweepRegistryOrphans deletes registry rows whose entity no longer exists —
// GUARDED, because this is the only code path that mass-deletes governed twin
// identity. Two independent brakes, both tuned for the failure mode "the
// entity tables look empty/depleted because of a mis-wired DSN, a botched
// restore, or a schema mishap", where a naive sweep would erase the whole
// registry and every twin identity with it:
//   - if device AND asset are BOTH empty, the sweep aborts outright;
//   - if the candidate count exceeds TWIN_REGISTRY_SWEEP_MAX (default 1000),
//     the sweep aborts — a human raises the ceiling deliberately after
//     confirming a genuine mass deletion.
//
// The swept count is always logged so operators can trend it.
func sweepRegistryOrphans(ctx context.Context, db *sql.DB) (int64, error) {
	var deviceRows, assetRows, candidates int64
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM device`).Scan(&deviceRows); err != nil {
		return 0, fmt.Errorf("sweep guard count device: %w", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM asset`).Scan(&assetRows); err != nil {
		return 0, fmt.Errorf("sweep guard count asset: %w", err)
	}
	if err := db.QueryRowContext(ctx,
		`SELECT count(*) FROM twin_registry tr WHERE `+registryOrphanPredicate).Scan(&candidates); err != nil {
		return 0, fmt.Errorf("sweep guard count candidates: %w", err)
	}
	if candidates == 0 {
		log.Printf("twin registry orphan sweep: swept 0 row(s)")
		return 0, nil
	}
	if deviceRows == 0 && assetRows == 0 {
		log.Printf("ERROR twin registry orphan sweep ABORTED: device and asset tables are BOTH empty but %d registry row(s) exist — this looks like a mis-wired database, not %d real deletions; deleting nothing", candidates, candidates)
		return 0, nil
	}
	if max := sweepMax(); candidates > max {
		log.Printf("ERROR twin registry orphan sweep ABORTED: %d candidate(s) exceed TWIN_REGISTRY_SWEEP_MAX=%d — raise the ceiling deliberately if this mass deletion is real; deleting nothing", candidates, max)
		return 0, nil
	}
	swept, err := execCountContext(ctx, db, `DELETE FROM twin_registry tr WHERE `+registryOrphanPredicate)
	if err != nil {
		return 0, fmt.Errorf("sweep orphan twins: %w", err)
	}
	log.Printf("twin registry orphan sweep: swept %d row(s)", swept)
	return swept, nil
}

// BackfillRegistryContext projects existing TB-compatible devices/assets into
// the governed twin registry and sweeps rows whose entity no longer exists.
// Safe to run repeatedly; runs at boot from main.go's maintenance goroutine.
// Returns the total rows converged (upserts + orphans removed).
//
// The three passes are independent on purpose: a failure in the device pass
// must not stop assets from converging or orphans from being reclaimed (the
// old early-return meant one bad device row silently froze the whole
// registry). Errors are aggregated and returned together.
func BackfillRegistryContext(ctx context.Context, db *sql.DB) (int64, error) {
	var errs []error
	deviceCount, err := execCountContext(ctx, db, backfillRegistryDeviceSQL)
	if err != nil {
		errs = append(errs, fmt.Errorf("backfill device twins: %w", err))
	}
	assetCount, err := execCountContext(ctx, db, backfillRegistryAssetSQL)
	if err != nil {
		errs = append(errs, fmt.Errorf("backfill asset twins: %w", err))
	}
	orphanCount, err := sweepRegistryOrphans(ctx, db)
	if err != nil {
		errs = append(errs, err)
	}
	return deviceCount + assetCount + orphanCount, errors.Join(errs...)
}

// BackfillRegistry is the context-free wrapper kept for existing callers/tests.
func BackfillRegistry(db *sql.DB) (int64, error) {
	return BackfillRegistryContext(context.Background(), db)
}

// SyncRegistryRow upserts the registry row for a single device/asset. Wired at
// boot (main.go) into the create paths of every sibling domain via func-var
// hooks — sibling domains never import this package directly.
func SyncRegistryRow(ctx context.Context, db *sql.DB, tenantID, entityType, entityID string) error {
	var query string
	switch strings.ToUpper(strings.TrimSpace(entityType)) {
	case "DEVICE":
		query = syncRegistryDeviceSQL
	case "ASSET":
		query = syncRegistryAssetSQL
	default:
		return fmt.Errorf("unsupported twin entity type: %s", entityType)
	}
	if _, err := db.ExecContext(ctx, query, tenantID, entityID); err != nil {
		return fmt.Errorf("sync twin registry row %s/%s: %w", entityType, entityID, err)
	}
	return nil
}

// DeleteRegistryRow removes the registry row for a deleted device/asset —
// required because twin_registry has no FK/cascade to reclaim it.
func DeleteRegistryRow(ctx context.Context, db *sql.DB, tenantID, entityType, entityID string) error {
	_, err := db.ExecContext(ctx, `
		DELETE FROM twin_registry
		 WHERE tenant_id = $1 AND entity_type = $2 AND entity_id = $3`,
		tenantID, strings.ToUpper(strings.TrimSpace(entityType)), entityID)
	if err != nil {
		return fmt.Errorf("delete twin registry row %s/%s: %w", entityType, entityID, err)
	}
	return nil
}

func execCountContext(ctx context.Context, db *sql.DB, query string) (int64, error) {
	result, err := db.ExecContext(ctx, query)
	if err != nil {
		return 0, err
	}
	count, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}
	return count, nil
}

// GetRegistryByEntity loads the governed twin identity for an existing entity.
func GetRegistryByEntity(db *sql.DB, tenantID string, entityType string, entityID string) (RegistryRow, error) {
	var row RegistryRow
	var attrs []byte
	var modelID, modelVersion sql.NullString
	err := db.QueryRow(`
		SELECT tenant_id::text, thing_id, entity_type, entity_id::text,
		       policy_id, definition, attributes, model_id, model_version, version
		  FROM twin_registry
		 WHERE tenant_id = $1
		   AND entity_type = $2
		   AND entity_id = $3`,
		tenantID, strings.ToUpper(entityType), entityID,
	).Scan(&row.TenantID, &row.ThingID, &row.EntityType, &row.EntityID, &row.PolicyID,
		&row.Definition, &attrs, &modelID, &modelVersion, &row.Version)
	if err != nil {
		return RegistryRow{}, err
	}
	row.Attributes = map[string]interface{}{}
	if len(attrs) > 0 {
		if err := json.Unmarshal(attrs, &row.Attributes); err != nil {
			return RegistryRow{}, fmt.Errorf("decode twin registry attributes: %w", err)
		}
	}
	if modelID.Valid {
		row.ModelID = modelID.String
	}
	if modelVersion.Valid {
		row.ModelVersion = modelVersion.String
	}
	return row, nil
}
