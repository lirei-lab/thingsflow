package twinmodel

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"

	"flow-core/internal/metrics"
)

var (
	// ErrAttributesRejected is the stable classification used by HTTP write
	// boundaries to map a model validation rejection to status 400.
	ErrAttributesRejected = errors.New("attributes rejected by pinned twin model")
	// ErrInvalidEntityCategory rejects values outside the model language's
	// DEVICE/ASSET kinds before any catalog query or state mutation.
	ErrInvalidEntityCategory = errors.New("invalid model entity category")
)

var attributeViolationCounter = metrics.Counter(
	"flow_twin_model_attribute_violations_total",
	"Attribute writes with one or more violations accepted in warn mode.",
)

// AttributeValidationError describes a reject-mode model violation. It is a
// typed error so transport layers can return a client error without treating
// database, pin, or schema failures as user input failures.
type AttributeValidationError struct {
	TenantID   string
	EntityType string
	EntityID   string
	ModelID    string
	Version    string
	Violations []Violation
}

func (e *AttributeValidationError) Error() string {
	return fmt.Sprintf("%s: model %s@%s produced %d violation(s)",
		ErrAttributesRejected, e.ModelID, e.Version, len(e.Violations))
}

func (e *AttributeValidationError) Unwrap() error {
	return ErrAttributesRejected
}

// ValidateAttributes resolves the entity's exact tenant-scoped registry pin
// and persisted model schema before validating a partial classic-attribute
// write. A missing registry row or a row with no pin is deliberately
// compatible pass-through. Once a pin exists, every lookup/decode/configuration
// failure is an error; a broken pin must never silently disable enforcement.
//
// This function is the reusable modeled-state boundary for Phase 3 feature
// writes. Callers must invoke it before every persistence layer they mutate.
func ValidateAttributes(
	ctx context.Context,
	db *sql.DB,
	tenantID, entityType, entityID string,
	values map[string]interface{},
) error {
	entityType = strings.ToUpper(strings.TrimSpace(entityType))
	if entityType != "DEVICE" && entityType != "ASSET" {
		return fmt.Errorf("%w: %q", ErrInvalidEntityCategory, entityType)
	}
	if db == nil {
		return errors.New("twin model enforcement database unavailable")
	}

	var modelID, modelVersion, schemaJSON sql.NullString
	err := db.QueryRowContext(ctx, `
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
		return nil
	}
	if err != nil {
		return fmt.Errorf("resolve twin model pin: %w", err)
	}
	if !modelID.Valid && !modelVersion.Valid {
		return nil
	}
	if !modelID.Valid || !modelVersion.Valid || strings.TrimSpace(modelID.String) == "" || strings.TrimSpace(modelVersion.String) == "" {
		return errors.New("invalid twin registry model pin: model_id and model_version must both be present")
	}
	if !schemaJSON.Valid {
		return fmt.Errorf("dangling twin model pin %s@%s", modelID.String, modelVersion.String)
	}

	var schema DerivedSchema
	if err := json.Unmarshal([]byte(schemaJSON.String), &schema); err != nil {
		return fmt.Errorf("decode schema for pinned twin model %s@%s: %w", modelID.String, modelVersion.String, err)
	}
	if schema.ModelID != modelID.String || schema.Version != modelVersion.String || schema.Kind != entityType {
		return fmt.Errorf(
			"invalid pinned twin model schema identity: pin=%s@%s/%s schema=%s@%s/%s",
			modelID.String, modelVersion.String, entityType,
			schema.ModelID, schema.Version, schema.Kind,
		)
	}
	mode := strings.TrimSpace(schema.EnforcementMode)
	if mode == "" {
		mode = "warn"
	}
	if mode != "warn" && mode != "reject" {
		return fmt.Errorf("invalid enforcementMode %q for pinned twin model %s@%s", mode, modelID.String, modelVersion.String)
	}

	violations := Validate(schema, "", map[string]interface{}{"attributes": values})
	if len(violations) == 0 {
		return nil
	}
	if mode == "reject" {
		return &AttributeValidationError{
			TenantID: tenantID, EntityType: entityType, EntityID: entityID,
			ModelID: modelID.String, Version: modelVersion.String, Violations: violations,
		}
	}

	attributeViolationCounter.Inc()
	log.Printf(
		"WARN twin model attribute violations tenant_id=%s entity_type=%s entity_id=%s model_id=%s model_version=%s violation_count=%d enforcement_mode=%s",
		tenantID, entityType, entityID, modelID.String, modelVersion.String, len(violations), mode,
	)
	return nil
}
