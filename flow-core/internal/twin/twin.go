// Package twin exposes a Ditto-inspired digital twin projection over the
// ThingsBoard-compatible entity model. It is read-only in this first cut:
// devices/assets remain the source entities and topology_edge remains the
// relationship source of truth.
package twin

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"regexp"
	"strings"

	dbpkg "flow-core/internal/db"
	"flow-core/internal/httputil"
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

	tenantID, _ := claims["tenantId"].(string)
	if tenantID != "" && row.TenantID != tenantID {
		httputil.WriteError(w, http.StatusForbidden, "Cross-tenant access denied")
		return
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
	})
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

func natsTwinStateAuthoritative(entityType string) bool {
	return strings.EqualFold(entityType, "DEVICE") && strings.EqualFold(strings.TrimSpace(os.Getenv("TWIN_STATE_STORE")), "nats")
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
		SELECT relation_type, relation_type_group,
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
		var relType, group, fromType, fromID, toType, toID string
		if err := rows.Scan(&relType, &group, &fromType, &fromID, &toType, &toID); err != nil {
			return nil, err
		}
		direction := "OUT"
		if toType == entityType && toID == entityID {
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
