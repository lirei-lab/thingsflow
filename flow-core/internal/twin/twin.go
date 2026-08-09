// Package twin exposes a Ditto-inspired digital twin projection over the
// ThingsBoard-compatible entity model. It is read-only in this first cut:
// devices/assets remain the source entities and topology_edge remains the
// relationship source of truth.
package twin

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"regexp"
	"strings"

	dbpkg "flow-core/internal/db"
	"flow-core/internal/httputil"
	"flow-core/internal/twinmodel"
	"flow-core/internal/twinstore"
)

type entityRow struct {
	EntityType     string
	ID             string
	TenantID       string
	CreatedTime    int64
	Name           string
	Type           string
	Label          string
	AdditionalInfo map[string]interface{}
}

type telemetryProperty struct {
	Value  interface{} `json:"value"`
	TS     int64       `json:"ts"`
	Source string      `json:"source"`
}

type relationProjection struct {
	Type         string                 `json:"type"`
	Group        string                 `json:"group"`
	Direction    string                 `json:"direction"`
	Source       string                 `json:"source"`
	Target       string                 `json:"target"`
	SourceEntity map[string]interface{} `json:"sourceEntity"`
	TargetEntity map[string]interface{} `json:"targetEntity"`
	// Depth and State are expansion-only annotations (?expand=relations(depth)).
	// Both are omitempty so the plain (non-expand) twin read stays byte-identical.
	Depth int                    `json:"depth,omitempty"`
	State map[string]interface{} `json:"state,omitempty"`
}

type identityProjection struct {
	ThingID    string
	PolicyID   string
	Definition string
	Attributes map[string]interface{}
}

var definitionPartRE = regexp.MustCompile(`[^a-zA-Z0-9_]+`)

// GetByEntity serves GET /api/twins/{entityType}/{entityId}.
func GetByEntity(w http.ResponseWriter, r *http.Request, entityType string, entityID string) {
	claims, ok := httputil.RequireAuth(w, r)
	if !ok {
		return
	}
	entityType = strings.ToUpper(strings.TrimSpace(entityType))
	if entityType != "DEVICE" && entityType != "ASSET" {
		httputil.WriteError(w, http.StatusBadRequest, "Unsupported twin entity type")
		return
	}
	if !httputil.LooksLikeUUID(entityID) {
		httputil.WriteError(w, http.StatusBadRequest, "Invalid entity id")
		return
	}
	if dbpkg.Pool == nil {
		httputil.WriteError(w, http.StatusServiceUnavailable, "Database not ready")
		return
	}

	row, err := loadEntity(entityType, entityID)
	if err == sql.ErrNoRows {
		httputil.WriteError(w, http.StatusNotFound, "Twin entity not found")
		return
	}
	if err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, "Twin query failed")
		return
	}

	// The twin projection reads ts_kv_latest by entity_id only (no tenant column),
	// so tenant isolation must be enforced here by entity ownership before that
	// read. Fail closed on an empty tenant claim (401) rather than fall through to
	// an unscoped latest read; a SYS_ADMIN (no tenant binding) may cross tenants.
	tenantID, _ := claims["tenantId"].(string)
	if !callerIsSysAdmin(claims) {
		if tenantID == "" {
			httputil.WriteError(w, http.StatusUnauthorized, "Authentication required")
			return
		}
		if row.TenantID != tenantID {
			httputil.WriteError(w, http.StatusForbidden, "Cross-tenant access denied")
			return
		}
	}

	identity, err := loadIdentity(row)
	if err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, "Twin registry query failed")
		return
	}
	features, err := loadFeatures(row.TenantID, row.EntityType, row.ID)
	if err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, "Twin feature query failed")
		return
	}
	relations, err := loadRelations(row.TenantID, row.EntityType, row.ID)
	if err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, "Twin relation query failed")
		return
	}

	// ?expand=relations(depth) replaces the plain relation set with the
	// tenant-scoped expanded traversal (immediate relations annotated with
	// depth + embedded state, transitive nodes appended). Without the param
	// expandRelations is a no-op and the response stays byte-identical.
	if expanded, ok := expandRelations(w, r, tenantID, row, relations); ok {
		relations = expanded
	}

	// R5: a queryable desired-vs-reported delta. desiredProperties live in the
	// features map (SERVER_SCOPE, 05-02); reported values are the CLIENT_SCOPE
	// attributes the device reported (written by the desiredstate reported
	// convergence). The delta block reuses the 05-02 read surface (features)
	// and is additive — it never changes the existing keys.
	reported, err := loadReportedAttributes(row.ID)
	if err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, "Twin reported query failed")
		return
	}

	httputil.WriteJSON(w, http.StatusOK, map[string]interface{}{
		"thingId":    identity.ThingID,
		"policyId":   identity.PolicyID,
		"definition": identity.Definition,
		"entity": map[string]interface{}{
			"entityType": row.EntityType,
			"id":         row.ID,
		},
		"attributes": identity.Attributes,
		"features":   features,
		"relations":  relations,
		"delta":      computeDelta(features, reported),
	})
}

// loadReportedAttributes reads the CLIENT_SCOPE (attribute_type 0) attributes
// for an entity — the values a device reported (persisted by the desiredstate
// reported convergence through the shared SaveAttributesKV path). Flat
// key→value, mirroring fetchAttributes. Reported is device-origin, so it lives
// under CLIENT_SCOPE, distinct from the SERVER_SCOPE desired state.
func loadReportedAttributes(entityID string) (map[string]interface{}, error) {
	if dbpkg.Pool == nil {
		return map[string]interface{}{}, nil
	}
	rows, err := dbpkg.Pool.Query(`
		SELECT k.key, a.bool_v, a.str_v, a.long_v, a.dbl_v, a.json_v
		  FROM attribute_kv a
		  JOIN key_dictionary k ON k.key_id = a.attribute_key
		 WHERE a.entity_id = $1 AND a.attribute_type = 0
		 ORDER BY k.key`, entityID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	reported := map[string]interface{}{}
	for rows.Next() {
		var key string
		var boolV sql.NullBool
		var strV sql.NullString
		var longV sql.NullInt64
		var dblV sql.NullFloat64
		var jsonV []byte
		if err := rows.Scan(&key, &boolV, &strV, &longV, &dblV, &jsonV); err != nil {
			return nil, err
		}
		reported[key] = typedValue(boolV, strV, longV, dblV, jsonV)
	}
	return reported, rows.Err()
}

// computeDelta builds the desired-vs-reported delta per modeled feature. For
// each feature carrying desiredProperties, the delta block reports the desired
// value and the reported value for every desired key (a missing reported key is
// nil — the device has not converged yet). Features with no desired properties
// are omitted; the block is additive and deterministic.
//
// Reported values are matched by the BARE property name first (a device reports
// flat keys such as "sample_interval" on the attributes topic — the shape
// internal/desiredstate merges as CLIENT_SCOPE), falling back to the
// feature.<name>.<property> form so feature-prefixed reports also match.
func computeDelta(features, reported map[string]interface{}) map[string]interface{} {
	delta := map[string]interface{}{}
	for name, raw := range features {
		feat, _ := raw.(map[string]interface{})
		desired, _ := feat["desiredProperties"].(map[string]interface{})
		if len(desired) == 0 {
			continue
		}
		pair := map[string]interface{}{"desired": map[string]interface{}{}, "reported": map[string]interface{}{}}
		dMap := pair["desired"].(map[string]interface{})
		rMap := pair["reported"].(map[string]interface{})
		for key, dval := range desired {
			dMap[key] = dval
			// Reported keys persist as the device-reported flat CLIENT keys
			// (e.g. "sample_interval"); fall back to the full
			// feature.<name>.<property> form for feature-prefixed reports.
			// nil when not yet reported.
			rMap[key] = reported[key]
			if rMap[key] == nil {
				rMap[key] = reported["feature."+name+"."+key]
			}
		}
		delta[name] = pair
	}
	return delta
}

func loadIdentity(row entityRow) (identityProjection, error) {
	fallback := identityProjection{
		ThingID:    thingID(row.TenantID, row.EntityType, row.ID),
		PolicyID:   fmt.Sprintf("tenant:%s:default", row.TenantID),
		Definition: definition(row.EntityType, row.Type),
		Attributes: buildAttributes(row),
	}
	registry, err := GetRegistryByEntity(dbpkg.Pool, row.TenantID, row.EntityType, row.ID)
	if err == sql.ErrNoRows || isUndefinedTable(err) {
		return fallback, nil
	}
	if err != nil {
		return identityProjection{}, err
	}
	if len(registry.Attributes) == 0 {
		registry.Attributes = fallback.Attributes
	}
	return identityProjection{
		ThingID:    registry.ThingID,
		PolicyID:   registry.PolicyID,
		Definition: registry.Definition,
		Attributes: registry.Attributes,
	}, nil
}

func isUndefinedTable(err error) bool {
	if err == nil {
		return false
	}
	type sqlStateCarrier interface {
		SQLState() string
	}
	if state, ok := err.(sqlStateCarrier); ok {
		return state.SQLState() == "42P01"
	}
	return strings.Contains(err.Error(), "does not exist")
}

func loadEntity(entityType string, entityID string) (entityRow, error) {
	table := "device"
	if entityType == "ASSET" {
		table = "asset"
	}
	var row entityRow
	row.EntityType = entityType
	var additionalInfo sql.NullString
	var label sql.NullString
	err := dbpkg.Pool.QueryRow(`
		SELECT id::text, tenant_id::text, created_time, name,
		       COALESCE(type, ''), label, additional_info
		  FROM `+table+`
		 WHERE id = $1`, entityID,
	).Scan(&row.ID, &row.TenantID, &row.CreatedTime, &row.Name, &row.Type, &label, &additionalInfo)
	if err != nil {
		return entityRow{}, err
	}
	if label.Valid {
		row.Label = label.String
	}
	row.AdditionalInfo = map[string]interface{}{}
	if additionalInfo.Valid && strings.TrimSpace(additionalInfo.String) != "" {
		_ = json.Unmarshal([]byte(additionalInfo.String), &row.AdditionalInfo)
	}
	return row, nil
}

func buildAttributes(row entityRow) map[string]interface{} {
	attrs := map[string]interface{}{
		"id":          row.ID,
		"entityType":  row.EntityType,
		"tenantId":    row.TenantID,
		"createdTime": row.CreatedTime,
		"name":        row.Name,
		"type":        row.Type,
		"label":       row.Label,
	}
	for k, v := range row.AdditionalInfo {
		if _, exists := attrs[k]; !exists {
			attrs[k] = v
		}
	}
	return attrs
}

func loadFeatures(tenantID, entityType, entityID string) (map[string]interface{}, error) {
	features, err := loadObservedFeatures(tenantID, entityType, entityID)
	if err != nil {
		return nil, err
	}
	skeletons, err := loadModelFeatureSkeletons(tenantID, entityType, entityID)
	if err != nil {
		return nil, err
	}
	for name, skeleton := range skeletons {
		if _, observed := features[name]; !observed {
			features[name] = skeleton
		}
	}
	// R5: hydrate the persisted desiredProperties (feature.<name>.desired.<property>
	// SERVER_SCOPE keys) into every modeled feature so the twin GET returns real
	// desired values, not just the empty skeleton placeholder.
	if err := hydrateFeatureDesiredState(tenantID, entityID, features); err != nil {
		return nil, err
	}
	return features, nil
}

// hydrateFeatureDesiredState reads the persisted desired properties for a
// twin's modeled features. Desired properties are stored as SERVER_SCOPE
// attribute_kv keys feature.<name>.desired.<property> (see HandleSaveFeatures),
// distinct from reported feature.<name>.<property> keys. Values are injected
// into each feature's desiredProperties map; features with no persisted desired
// state keep their (possibly empty) declaration map. Reads are entity-scoped by
// entity_id only (attribute_kv has no tenant column), matching the ts_kv_latest
// read posture — tenant isolation is enforced by the caller before this read.
func hydrateFeatureDesiredState(tenantID, entityID string, features map[string]interface{}) error {
	if dbpkg.Pool == nil {
		return nil
	}
	rows, err := dbpkg.Pool.Query(`
		SELECT k.key, a.bool_v, a.str_v, a.long_v, a.dbl_v, a.json_v
		  FROM attribute_kv a
		  JOIN key_dictionary k ON k.key_id = a.attribute_key
		 WHERE a.entity_id = $1 AND a.attribute_type = 2
		   AND k.key LIKE 'feature.%.desired.%'
		 ORDER BY k.key`, entityID)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var key string
		var boolV sql.NullBool
		var strV sql.NullString
		var longV sql.NullInt64
		var dblV sql.NullFloat64
		var jsonV []byte
		if err := rows.Scan(&key, &boolV, &strV, &longV, &dblV, &jsonV); err != nil {
			return err
		}
		name, prop, ok := splitDesiredFeatureKey(key)
		if !ok {
			continue
		}
		feat, _ := features[name].(map[string]interface{})
		if feat == nil {
			// Persisted desired state for a feature the model no longer declares:
			// surface it under a minimal map rather than dropping it (defensive).
			feat = map[string]interface{}{}
			features[name] = feat
		}
		desired, _ := feat["desiredProperties"].(map[string]interface{})
		if desired == nil {
			desired = map[string]interface{}{}
			feat["desiredProperties"] = desired
		}
		desired[prop] = typedValue(boolV, strV, longV, dblV, jsonV)
	}
	return rows.Err()
}

// splitDesiredFeatureKey parses a persisted desired key of the shape
// feature.<name>.desired.<property> into (name, property, true); any other
// key shape returns ok=false so non-desired keys are skipped.
func splitDesiredFeatureKey(key string) (name, prop string, ok bool) {
	const prefix = "feature."
	if !strings.HasPrefix(key, prefix) {
		return "", "", false
	}
	rest := strings.TrimPrefix(key, prefix)
	// name is everything up to the first ".desired."; property follows it.
	idx := strings.Index(rest, ".desired.")
	if idx <= 0 {
		return "", "", false
	}
	name = rest[:idx]
	prop = rest[idx+len(".desired."):]
	if name == "" || prop == "" {
		return "", "", false
	}
	return name, prop, true
}

func loadObservedFeatures(tenantID, entityType, entityID string) (map[string]interface{}, error) {
	if store := twinstore.Global(); store != nil {
		if latest, err := store.GetLatestTelemetry(context.Background(), tenantID, entityType, entityID, nil); err == nil {
			props := map[string]interface{}{}
			for key, value := range latest {
				props[key] = telemetryProperty{
					Value:  value.Value,
					TS:     value.TS,
					Source: "nats_kv",
				}
			}
			return map[string]interface{}{
				"telemetry": map[string]interface{}{
					"definition": "thingsflow:feature:telemetry:1.0.0",
					"properties": props,
				},
			}, nil
		}
	}
	if natsTwinStateAuthoritative(entityType) {
		return map[string]interface{}{
			"telemetry": map[string]interface{}{
				"definition": "thingsflow:feature:telemetry:1.0.0",
				"properties": map[string]interface{}{},
			},
		}, nil
	}
	rows, err := dbpkg.Pool.Query(`
		SELECT k.key, t.ts, t.bool_v, t.str_v, t.long_v, t.dbl_v, t.json_v
		  FROM ts_kv_latest t
		  JOIN key_dictionary k ON k.key_id = t.key
		 WHERE t.entity_id = $1
		 ORDER BY k.key`, entityID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	props := map[string]interface{}{}
	for rows.Next() {
		var key string
		var ts int64
		var boolV sql.NullBool
		var strV sql.NullString
		var longV sql.NullInt64
		var dblV sql.NullFloat64
		var jsonV []byte
		if err := rows.Scan(&key, &ts, &boolV, &strV, &longV, &dblV, &jsonV); err != nil {
			return nil, err
		}
		props[key] = telemetryProperty{
			Value:  typedValue(boolV, strV, longV, dblV, jsonV),
			TS:     ts,
			Source: "ts_kv_latest",
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return map[string]interface{}{
		"telemetry": map[string]interface{}{
			"definition": "thingsflow:feature:telemetry:1.0.0",
			"properties": props,
		},
	}, nil
}

func loadModelFeatureSkeletons(tenantID, entityType, entityID string) (map[string]interface{}, error) {
	if dbpkg.Pool == nil {
		return map[string]interface{}{}, nil
	}
	var resolvedModel sql.NullString
	var rawSchema []byte
	err := dbpkg.Pool.QueryRow(`
		SELECT tm.model_id, COALESCE(tm.schema, '{}'::jsonb)
		  FROM twin_registry tr
		  LEFT JOIN twin_model tm
		    ON tm.tenant_id = tr.tenant_id
		   AND tm.model_id = tr.model_id
		   AND tm.version = tr.model_version
		 WHERE tr.tenant_id = $1
		   AND tr.entity_type = $2
		   AND tr.entity_id = $3
		   AND tr.model_id IS NOT NULL
		   AND tr.model_version IS NOT NULL`, tenantID, entityType, entityID).
		Scan(&resolvedModel, &rawSchema)
	if errors.Is(err, sql.ErrNoRows) || isUndefinedTable(err) {
		return map[string]interface{}{}, nil
	}
	if err != nil {
		return nil, err
	}
	if !resolvedModel.Valid {
		return nil, fmt.Errorf("pinned twin model is missing from catalog")
	}
	var schema twinmodel.DerivedSchema
	if err := json.Unmarshal(rawSchema, &schema); err != nil {
		return nil, fmt.Errorf("decode pinned twin model schema: %w", err)
	}
	skeletons := make(map[string]interface{}, len(schema.Features))
	for name, feature := range schema.Features {
		skeleton := map[string]interface{}{
			"definition": feature.Definition,
			"properties": map[string]interface{}{},
		}
		if len(feature.DesiredProperties) > 0 {
			skeleton["desiredProperties"] = map[string]interface{}{}
		}
		skeletons[name] = skeleton
	}
	return skeletons, nil
}

func natsTwinStateAuthoritative(entityType string) bool {
	return strings.EqualFold(entityType, "DEVICE") && strings.EqualFold(strings.TrimSpace(os.Getenv("TWIN_STATE_STORE")), "nats")
}

// callerIsSysAdmin reports whether the verified JWT carries the SYS_ADMIN scope.
// A SYS_ADMIN reads twins across tenants; every other authority is confined to
// its own tenant. Mirrors the identical check in internal/system.
func callerIsSysAdmin(claims map[string]interface{}) bool {
	scopes, _ := claims["scopes"].([]interface{})
	for _, s := range scopes {
		if str, ok := s.(string); ok && str == "SYS_ADMIN" {
			return true
		}
	}
	return false
}

func typedValue(boolV sql.NullBool, strV sql.NullString, longV sql.NullInt64, dblV sql.NullFloat64, jsonV []byte) interface{} {
	switch {
	case boolV.Valid:
		return boolV.Bool
	case strV.Valid:
		return strV.String
	case longV.Valid:
		return longV.Int64
	case dblV.Valid:
		return dblV.Float64
	case len(jsonV) > 0:
		var out interface{}
		if err := json.Unmarshal(jsonV, &out); err == nil {
			return out
		}
		return string(jsonV)
	default:
		return nil
	}
}

func loadRelations(tenantID string, entityType string, entityID string) ([]relationProjection, error) {
	rows, err := dbpkg.Pool.Query(`
		SELECT relation_type, relation_type_group, direction,
		       from_type, from_id::text, to_type, to_id::text
		  FROM topology_edge
		 WHERE tenant_id = $1
		   AND ((from_type = $2 AND from_id = $3) OR (to_type = $2 AND to_id = $3))
		 ORDER BY relation_type, from_type, from_id, to_type, to_id`,
		tenantID, entityType, entityID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []relationProjection
	for rows.Next() {
		var relType, group, storedDirection, fromType, fromID, toType, toID string
		if err := rows.Scan(&relType, &group, &storedDirection, &fromType, &fromID, &toType, &toID); err != nil {
			return nil, err
		}
		direction := "OUT"
		if storedDirection == "BIDIRECTIONAL" {
			direction = "BOTH"
		} else if toType == entityType && toID == entityID {
			direction = "IN"
		}
		out = append(out, relationProjection{
			Type:      relType,
			Group:     group,
			Direction: direction,
			Source:    thingID(tenantID, fromType, fromID),
			Target:    thingID(tenantID, toType, toID),
			SourceEntity: map[string]interface{}{
				"entityType": fromType,
				"id":         fromID,
			},
			TargetEntity: map[string]interface{}{
				"entityType": toType,
				"id":         toID,
			},
		})
	}
	return out, rows.Err()
}

func thingID(tenantID string, entityType string, entityID string) string {
	return tenantID + ":" + strings.ToLower(entityType) + ":" + entityID
}

func definition(entityType string, typ string) string {
	part := strings.Trim(definitionPartRE.ReplaceAllString(typ, "_"), "_")
	if part == "" {
		part = "default"
	}
	return "thingsflow:" + strings.ToLower(entityType) + ":" + strings.ToLower(part) + ":1.0.0"
}
