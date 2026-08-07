package tenant

import (
	"context"
	"fmt"
	"log"

	"flow-core/internal/twinevents"
	"flow-core/internal/twinstore"
)

// SaveAttributesKV persists attribute values through the SINGLE write path for
// modeled attribute state: attribute_kv (typed columns via saveAttributesKV)
// then the twin-state KV merge (MergeAttributes). It is shared by the classic
// attribute REST handler (handleSaveAttributeRest) and the Phase 3 twin API
// (internal/twin/write.go) so the two stores always agree and no call site
// duplicates the write SQL.
//
// Callers MUST run twinmodel.ValidateAttributes BEFORE invoking this helper;
// this function performs persistence only and never makes a model decision.
// The scope string is validated through normalizeAttributeScope (the single
// CLIENT/SHARED/SERVER ordinal mapping) and an unknown scope is an error, so
// no caller can accidentally widen a write to a scope it did not name.
func SaveAttributesKV(ctx context.Context, tenantID, entityType, entityID, scope string, values map[string]interface{}) error {
	normalizedScope, attrType, ok := normalizeAttributeScope(scope)
	if !ok {
		return fmt.Errorf("unknown attribute scope %q", scope)
	}
	if err := saveAttributesKV(entityID, attrType, values); err != nil {
		return err
	}
	// The merge is keyed by the verified caller tenant (or the entity's
	// resolved actual tenant for SYS_ADMIN) — never request data. A merge
	// failure is a warning, not a write failure: attribute_kv is durable and
	// the twin KV watch re-converges from it.
	if store := twinstore.Global(); store != nil && tenantID != "" {
		if err := store.MergeAttributes(ctx, tenantID, entityType, entityID, normalizedScope, currentTimeMillis(), values); err != nil {
			log.Printf("WARN: Failed to save attributes to twin state for entity %s: %v", entityID, err)
		}
	}
	// Twin event journal (R4): the durable attribute_kv write succeeded — emit
	// the attribute-saved event. This is the SINGLE emit point for
	// EventAttributeSaved: both the classic REST handler and the Phase 3 twin
	// API funnel through SaveAttributesKV, so firing here covers both without
	// duplicating events at each call site.
	twinevents.Publish(tenantID, entityType, entityID, twinevents.EventAttributeSaved, map[string]interface{}{
		"scope":  normalizedScope,
		"values": values,
	})
	return nil
}
