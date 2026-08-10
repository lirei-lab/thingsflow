// Package twin exposes a Ditto-inspired digital twin projection over the
// ThingsBoard-compatible entity model. This file adds the first-class,
// model-validated twin state writes (R3): attributes and features, both
// gated by the Phase 2 pinned-model enforcement boundary.
package twin

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"strings"

	dbpkg "flow-core/internal/db"
	"flow-core/internal/desiredstate"
	"flow-core/internal/httputil"
	"flow-core/internal/tenant"
	"flow-core/internal/twinevents"
	"flow-core/internal/twinmodel"
)

// HandleSaveAttributes serves PUT/PATCH /api/twins/{entityType}/{entityId}/attributes.
//
// The write is validated against the entity's pinned twin model BEFORE any
// persistence: in reject mode a violation returns 400 and persists nothing; in
// warn mode the write persists and the violation is logged. Values are
// mirrored to attribute_kv (SERVER_SCOPE) and the twin-state KV through the
// single shared write path (tenant.SaveAttributesKV). Cross-tenant writes are
// denied with 403 unless the caller is a SYS_ADMIN.
//
// R6: this handler is additionally wrapped by the policy enforcement
// middleware in api.go (policy.EnforceWrite), which authorizes each written
// attribute path against the entity's resolved policy document before the
// handler runs. That layer is additive — the RequireAuth + cross-tenant + model
// checks below are unchanged and remain the outer guard. The explicit PUT/PATCH
// method gate ensures the methodless fallback route (which is not wrapped, to
// preserve the canonical 405 envelope) can never execute an unenforced write.
func HandleSaveAttributes(w http.ResponseWriter, r *http.Request, entityType, entityID string) {
	if r.Method != http.MethodPut && r.Method != http.MethodPatch {
		httputil.WriteError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}
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
	if errors.Is(err, sql.ErrNoRows) {
		httputil.WriteError(w, http.StatusNotFound, "Twin entity not found")
		return
	}
	if err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, "Twin query failed")
		return
	}

	tenantID, _ := claims["tenantId"].(string)
	if callerIsSysAdmin(claims) {
		// SYS_ADMIN writes resolve to the entity's ACTUAL tenant for model
		// lookup and persistence — never the empty/system JWT tenant.
		tenantID = row.TenantID
	} else {
		if tenantID == "" {
			httputil.WriteError(w, http.StatusUnauthorized, "Authentication required")
			return
		}
		if row.TenantID != tenantID {
			httputil.WriteError(w, http.StatusForbidden, "Cross-tenant access denied")
			return
		}
	}

	var body struct {
		Attributes map[string]interface{} `json:"attributes"`
	}
	if !decodeTwinWriteBody(w, r, &body) || body.Attributes == nil {
		httputil.WriteError(w, http.StatusBadRequest, "Invalid JSON")
		return
	}

	if err := twinmodel.ValidateAttributes(r.Context(), dbpkg.Pool, tenantID, entityType, entityID, body.Attributes); err != nil {
		if errors.Is(err, twinmodel.ErrAttributesRejected) {
			httputil.WriteError(w, http.StatusBadRequest, "Attributes violate pinned twin model")
			return
		}
		log.Printf("ERROR: Twin model attribute enforcement failed tenant_id=%s entity_type=%s entity_id=%s: %v",
			tenantID, entityType, entityID, err)
		httputil.WriteError(w, http.StatusInternalServerError, "Twin model attribute enforcement failed")
		return
	}

	if err := tenant.SaveAttributesKV(r.Context(), tenantID, entityType, entityID, "SERVER_SCOPE", body.Attributes); err != nil {
		log.Printf("ERROR: Failed to save twin attributes tenant_id=%s entity_type=%s entity_id=%s: %v",
			tenantID, entityType, entityID, err)
		httputil.WriteError(w, http.StatusInternalServerError, "Failed to save attributes")
		return
	}

	httputil.WriteJSON(w, http.StatusOK, map[string]interface{}{"persisted": true})
}

// HandleSaveFeatures serves PUT/PATCH /api/twins/{entityType}/{entityId}/features.
//
// Feature property maps are validated against the pinned model's feature
// declarations (schema.Features[name].Properties) before any persistence.
// Each validated property is persisted as feature.<name>.<property> through the
// single shared write path. Undeclared features are rejected (400) when the
// model's unknownKeys policy is "reject"; otherwise they pass through as
// unmodeled state. Reject-mode violations persist nothing.
//
// R6: this handler is additionally wrapped by the policy enforcement
// middleware in api.go (policy.EnforceWrite), which authorizes each written
// feature path against the entity's resolved policy document before the
// handler runs. That layer is additive — the RequireAuth + cross-tenant + model
// checks below are unchanged and remain the outer guard. The explicit PUT/PATCH
// method gate ensures the methodless fallback route (which is not wrapped, to
// preserve the canonical 405 envelope) can never execute an unenforced write.
func HandleSaveFeatures(w http.ResponseWriter, r *http.Request, entityType, entityID string) {
	if r.Method != http.MethodPut && r.Method != http.MethodPatch {
		httputil.WriteError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}
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
	if errors.Is(err, sql.ErrNoRows) {
		httputil.WriteError(w, http.StatusNotFound, "Twin entity not found")
		return
	}
	if err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, "Twin query failed")
		return
	}

	tenantID, _ := claims["tenantId"].(string)
	if callerIsSysAdmin(claims) {
		tenantID = row.TenantID
	} else {
		if tenantID == "" {
			httputil.WriteError(w, http.StatusUnauthorized, "Authentication required")
			return
		}
		if row.TenantID != tenantID {
			httputil.WriteError(w, http.StatusForbidden, "Cross-tenant access denied")
			return
		}
	}

	var body struct {
		Features map[string]interface{} `json:"features"`
	}
	if !decodeTwinWriteBody(w, r, &body) || body.Features == nil {
		httputil.WriteError(w, http.StatusBadRequest, "Invalid JSON")
		return
	}

	// Resolve the pinned model schema (nil when the entity has no pin → the
	// write is unmodeled pass-through, mirroring ValidateAttributes).
	schema, err := resolvePinnedSchema(r.Context(), tenantID, entityType, entityID)
	if err != nil {
		log.Printf("ERROR: Resolve pinned twin model schema tenant_id=%s entity_type=%s entity_id=%s: %v",
			tenantID, entityType, entityID, err)
		httputil.WriteError(w, http.StatusInternalServerError, "Twin model schema resolution failed")
		return
	}

	if schema != nil {
		mode := strings.TrimSpace(schema.EnforcementMode)
		if mode == "" {
			mode = "warn"
		}
		if mode != "warn" && mode != "reject" {
			log.Printf("ERROR: Invalid enforcementMode %q for pinned twin model tenant_id=%s entity_type=%s entity_id=%s",
				mode, tenantID, entityType, entityID)
			httputil.WriteError(w, http.StatusInternalServerError, "Invalid twin model enforcement mode")
			return
		}

		// Undeclared feature check: reject when the model's unknownKeys policy
		// is "reject"; otherwise pass through as unmodeled state.
		if schema.UnknownKeys == "reject" {
			for name := range body.Features {
				if name == "telemetry" {
					continue
				}
				if _, declared := schema.Features[name]; !declared {
					httputil.WriteError(w, http.StatusBadRequest, "Feature not declared by pinned twin model")
					return
				}
			}
		}

		violations := twinmodel.Validate(*schema, "", map[string]interface{}{"features": body.Features})
		if len(violations) > 0 {
			if mode == "reject" {
				httputil.WriteError(w, http.StatusBadRequest, "Features violate pinned twin model")
				return
			}
			log.Printf("WARN twin model feature violations tenant_id=%s entity_type=%s entity_id=%s model_id=%s version=%s violation_count=%d enforcement_mode=%s",
				tenantID, entityType, entityID, schema.ModelID, schema.Version, len(violations), mode)
		}
	}

	// Persist each validated feature property as feature.<name>.<property>
	// and each desired property as feature.<name>.desired.<property>, both
	// through the single shared write path (SERVER_SCOPE mirror). The
	// .desired. infix keeps desired distinct from reported so the read
	// surface can separate them (see hydrateFeatureState in twin.go). The
	// twinmodel.Validate call above already validates desiredProperties
	// against the pinned model (validate.go), so a violating desired value
	// never reaches persistence in reject mode.
	flat := map[string]interface{}{}
	for name, raw := range body.Features {
		fm, _ := raw.(map[string]interface{})
		props, _ := fm["properties"].(map[string]interface{})
		for key, value := range props {
			flat["feature."+name+"."+key] = value
		}
		desired, _ := fm["desiredProperties"].(map[string]interface{})
		for key, value := range desired {
			flat["feature."+name+".desired."+key] = value
		}
	}
	if len(flat) > 0 {
		if err := tenant.SaveAttributesKV(r.Context(), tenantID, entityType, entityID, "SERVER_SCOPE", flat); err != nil {
			log.Printf("ERROR: Failed to save twin features tenant_id=%s entity_type=%s entity_id=%s: %v",
				tenantID, entityType, entityID, err)
			httputil.WriteError(w, http.StatusInternalServerError, "Failed to save features")
			return
		}
		// Twin event journal (R4): feature properties persisted through the
		// shared path — emit the feature-saved event (a distinct semantic
		// event from the attribute event SaveAttributesKV emits for the
		// underlying write).
		twinevents.Publish(tenantID, entityType, entityID, twinevents.EventFeatureSaved, body.Features)
	}
	// R5: if the write carried desiredProperties, deliver the desired state to
	// the device via retained MQTT (replay on reconnect). Fire-and-forget and
	// ASYNC: delivery makes an HTTP call to the rmqtt broker (up to its 5s
	// timeout), so it must never sit on the control-plane write path — a slow or
	// down broker must not add latency to a twin feature write. The poll surface
	// still covers non-MQTT devices.
	if desiredPayload := extractDesiredPayload(body.Features); len(desiredPayload) > 0 && entityType == "DEVICE" {
		b, err := json.Marshal(desiredPayload)
		if err == nil {
			go desiredstate.DeliverDesiredForEntity(entityID, b)
		}
	}

	httputil.WriteJSON(w, http.StatusOK, map[string]interface{}{"persisted": true})
}

// extractDesiredPayload builds the Ditto-style desired-state payload the device
// receives: {"features": {<name>: {"desiredProperties": {...}}}} for every
// feature that carried desiredProperties in this write. Features with no desired
// properties are omitted so an empty write never publishes an empty retained doc.
func extractDesiredPayload(features map[string]interface{}) map[string]interface{} {
	out := map[string]interface{}{"features": map[string]interface{}{}}
	feats := out["features"].(map[string]interface{})
	for name, raw := range features {
		fm, _ := raw.(map[string]interface{})
		desired, _ := fm["desiredProperties"].(map[string]interface{})
		if len(desired) == 0 {
			continue
		}
		feats[name] = map[string]interface{}{"desiredProperties": desired}
	}
	if len(feats) == 0 {
		return nil
	}
	return out
}

// decodeTwinWriteBody strictly decodes a JSON object body, rejecting trailing
// content so a malformed payload cannot be silently truncated (the same strict
// decode the classic attribute REST handler enforces).
func decodeTwinWriteBody(w http.ResponseWriter, r *http.Request, dst interface{}) bool {
	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(dst); err != nil {
		return false
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return false
	}
	return true
}

// resolvePinnedSchema reads the entity's exact tenant-scoped registry pin and
// its persisted derived schema. It returns nil when the entity has no registry
// row or no pin (unmodeled pass-through, mirroring ValidateAttributes); any
// partial/dangling/broken pin or decode failure is an error so a broken pin
// never silently disables enforcement.
func resolvePinnedSchema(ctx context.Context, tenantID, entityType, entityID string) (*twinmodel.DerivedSchema, error) {
	var modelID, modelVersion, schemaJSON sql.NullString
	err := dbpkg.Pool.QueryRowContext(ctx, `
		SELECT tr.model_id, tr.model_version, tm.schema::text
		  FROM twin_registry tr
		  LEFT JOIN twin_model tm
		    ON tm.tenant_id=tr.tenant_id
		   AND tm.model_id=tr.model_id
		   AND tm.version=tr.model_version
		 WHERE tr.tenant_id=$1 AND tr.entity_type=$2 AND tr.entity_id=$3`,
		tenantID, entityType, entityID,
	).Scan(&modelID, &modelVersion, &schemaJSON)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if !modelID.Valid && !modelVersion.Valid {
		return nil, nil
	}
	if !modelID.Valid || !modelVersion.Valid || strings.TrimSpace(modelID.String) == "" || strings.TrimSpace(modelVersion.String) == "" {
		return nil, errors.New("invalid twin registry model pin: model_id and model_version must both be present")
	}
	if !schemaJSON.Valid {
		return nil, errors.New("dangling twin model pin " + modelID.String + "@" + modelVersion.String)
	}
	var schema twinmodel.DerivedSchema
	if err := json.Unmarshal([]byte(schemaJSON.String), &schema); err != nil {
		return nil, errors.New("decode schema for pinned twin model " + modelID.String + "@" + modelVersion.String + ": " + err.Error())
	}
	if schema.ModelID != modelID.String || schema.Version != modelVersion.String || schema.Kind != entityType {
		return nil, errors.New("invalid pinned twin model schema identity")
	}
	return &schema, nil
}
