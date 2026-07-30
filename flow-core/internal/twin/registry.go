package twin

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
)

// RegistryRow is the governed identity and model metadata for a projected twin.
type RegistryRow struct {
	TenantID   string
	ThingID    string
	EntityType string
	EntityID   string
	PolicyID   string
	Definition string
	Attributes map[string]interface{}
	Version    int64
}

const backfillRegistryDeviceSQL = `
	WITH src AS (
		SELECT tenant_id, id, created_time,
		       tenant_id::text || ':device:' || id::text AS thing_id,
		       'tenant:' || tenant_id::text || ':default' AS policy_id,
		       'thingsflow:device:' ||
		       COALESCE(NULLIF(trim(both '_' from regexp_replace(lower(COALESCE(type, 'default')), '[^a-z0-9_]+', '_', 'g')), ''), 'default') ||
		       ':1.0.0' AS definition,
		       COALESCE(NULLIF(trim(COALESCE(additional_info, '')), '')::jsonb, '{}'::jsonb) ||
		       jsonb_build_object(
		           'id', id::text,
		           'entityType', 'DEVICE',
		           'tenantId', tenant_id::text,
		           'createdTime', created_time,
		           'name', name,
		           'type', COALESCE(type, ''),
		           'label', COALESCE(label, '')
		       ) AS attributes
		  FROM device
		 WHERE tenant_id IS NOT NULL
	)
	INSERT INTO twin_registry
		(tenant_id, thing_id, entity_type, entity_id, policy_id, definition, attributes, created_time, updated_time)
	SELECT tenant_id, thing_id, 'DEVICE', id, policy_id, definition, attributes, created_time, created_time
	  FROM src
	ON CONFLICT (tenant_id, entity_type, entity_id) DO UPDATE
	   SET thing_id = EXCLUDED.thing_id,
	       policy_id = EXCLUDED.policy_id,
	       definition = EXCLUDED.definition,
	       attributes = EXCLUDED.attributes,
	       updated_time = EXCLUDED.updated_time,
	       version = twin_registry.version + 1
	 WHERE twin_registry.thing_id IS DISTINCT FROM EXCLUDED.thing_id
	    OR twin_registry.policy_id IS DISTINCT FROM EXCLUDED.policy_id
	    OR twin_registry.definition IS DISTINCT FROM EXCLUDED.definition
	    OR twin_registry.attributes IS DISTINCT FROM EXCLUDED.attributes`

const backfillRegistryAssetSQL = `
	WITH src AS (
		SELECT tenant_id, id, created_time,
		       tenant_id::text || ':asset:' || id::text AS thing_id,
		       'tenant:' || tenant_id::text || ':default' AS policy_id,
		       'thingsflow:asset:' ||
		       COALESCE(NULLIF(trim(both '_' from regexp_replace(lower(COALESCE(type, 'default')), '[^a-z0-9_]+', '_', 'g')), ''), 'default') ||
		       ':1.0.0' AS definition,
		       COALESCE(NULLIF(trim(COALESCE(additional_info, '')), '')::jsonb, '{}'::jsonb) ||
		       jsonb_build_object(
		           'id', id::text,
		           'entityType', 'ASSET',
		           'tenantId', tenant_id::text,
		           'createdTime', created_time,
		           'name', name,
		           'type', COALESCE(type, ''),
		           'label', COALESCE(label, '')
		       ) AS attributes
		  FROM asset
		 WHERE tenant_id IS NOT NULL
	)
	INSERT INTO twin_registry
		(tenant_id, thing_id, entity_type, entity_id, policy_id, definition, attributes, created_time, updated_time)
	SELECT tenant_id, thing_id, 'ASSET', id, policy_id, definition, attributes, created_time, created_time
	  FROM src
	ON CONFLICT (tenant_id, entity_type, entity_id) DO UPDATE
	   SET thing_id = EXCLUDED.thing_id,
	       policy_id = EXCLUDED.policy_id,
	       definition = EXCLUDED.definition,
	       attributes = EXCLUDED.attributes,
	       updated_time = EXCLUDED.updated_time,
	       version = twin_registry.version + 1
	 WHERE twin_registry.thing_id IS DISTINCT FROM EXCLUDED.thing_id
	    OR twin_registry.policy_id IS DISTINCT FROM EXCLUDED.policy_id
	    OR twin_registry.definition IS DISTINCT FROM EXCLUDED.definition
	    OR twin_registry.attributes IS DISTINCT FROM EXCLUDED.attributes`

// BackfillRegistry projects existing TB-compatible devices/assets into the
// governed twin registry. It is safe to run repeatedly.
func BackfillRegistry(db *sql.DB) (int64, error) {
	deviceCount, err := execCount(db, backfillRegistryDeviceSQL)
	if err != nil {
		return 0, fmt.Errorf("backfill device twins: %w", err)
	}
	assetCount, err := execCount(db, backfillRegistryAssetSQL)
	if err != nil {
		return 0, fmt.Errorf("backfill asset twins: %w", err)
	}
	return deviceCount + assetCount, nil
}

func execCount(db *sql.DB, query string) (int64, error) {
	result, err := db.Exec(query)
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
	err := db.QueryRow(`
		SELECT tenant_id::text, thing_id, entity_type, entity_id::text,
		       policy_id, definition, attributes, version
		  FROM twin_registry
		 WHERE tenant_id = $1
		   AND entity_type = $2
		   AND entity_id = $3`,
		tenantID, strings.ToUpper(entityType), entityID,
	).Scan(&row.TenantID, &row.ThingID, &row.EntityType, &row.EntityID, &row.PolicyID, &row.Definition, &attrs, &row.Version)
	if err != nil {
		return RegistryRow{}, err
	}
	row.Attributes = map[string]interface{}{}
	if len(attrs) > 0 {
		if err := json.Unmarshal(attrs, &row.Attributes); err != nil {
			return RegistryRow{}, fmt.Errorf("decode twin registry attributes: %w", err)
		}
	}
	return row, nil
}
