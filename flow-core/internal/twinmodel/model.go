package twinmodel

import (
	"bytes"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

const maxModelIDBytes = 255

var (
	modelIDInvalidRE = regexp.MustCompile(`[^a-z0-9_]+`)
	versionRE        = regexp.MustCompile(`^(0|[1-9][0-9]{0,9})\.(0|[1-9][0-9]{0,9})\.(0|[1-9][0-9]{0,9})$`)
	featureURNRE     = regexp.MustCompile(`^thingsflow:feature:[a-z0-9_]+:([^.]+\.[^.]+\.[^.]+)$`)
)

// Model is the normalized authored model. Extra retains non-validation
// metadata so the authored definition can round-trip without polluting the
// derived validator schema.
type Model struct {
	ModelID         string                     `json:"modelId"`
	Version         string                     `json:"version"`
	Kind            string                     `json:"kind"`
	DisplayName     string                     `json:"displayName,omitempty"`
	UnknownKeys     string                     `json:"unknownKeys"`
	EnforcementMode string                     `json:"enforcementMode"`
	Attributes      map[string]PropertySpec    `json:"attributes"`
	Features        map[string]Feature         `json:"features"`
	Relationships   map[string]Relationship    `json:"relationships"`
	Extra           map[string]json.RawMessage `json:"-"`
}

// PropertySpec is the supported JSON-Schema subset for one property.
type PropertySpec struct {
	Type      string                     `json:"type"`
	Enum      []interface{}              `json:"enum,omitempty"`
	Minimum   *float64                   `json:"minimum,omitempty"`
	Maximum   *float64                   `json:"maximum,omitempty"`
	MinLength *int                       `json:"minLength,omitempty"`
	MaxLength *int                       `json:"maxLength,omitempty"`
	Pattern   string                     `json:"pattern,omitempty"`
	Required  bool                       `json:"required,omitempty"`
	Unit      string                     `json:"unit,omitempty"`
	Writable  bool                       `json:"writable,omitempty"`
	Extra     map[string]json.RawMessage `json:"-"`
}

// Feature groups reported and desired properties under a stable definition.
type Feature struct {
	Definition        string                     `json:"definition"`
	Properties        map[string]PropertySpec    `json:"properties"`
	DesiredProperties map[string]PropertySpec    `json:"desiredProperties"`
	Extra             map[string]json.RawMessage `json:"-"`
}

// Relationship narrows a global topology relation for this source model.
type Relationship struct {
	Target            []string                   `json:"target"`
	TargetEntityTypes []string                   `json:"targetEntityTypes"`
	MaxCardinality    int                        `json:"maxCardinality"`
	Bidirectional     bool                       `json:"bidirectional"`
	Extra             map[string]json.RawMessage `json:"-"`
}

// DerivedSchema is the deterministic, metadata-free schema persisted beside
// the authored model.
type DerivedSchema struct {
	ModelID         string                        `json:"modelId"`
	Version         string                        `json:"version"`
	Kind            string                        `json:"kind"`
	UnknownKeys     string                        `json:"unknownKeys"`
	EnforcementMode string                        `json:"enforcementMode"`
	Attributes      map[string]PropertySpec       `json:"attributes"`
	Features        map[string]FeatureSchema      `json:"features"`
	Relationships   map[string]RelationshipSchema `json:"relationships"`
}

// FeatureSchema is the metadata-free derived feature shape.
type FeatureSchema struct {
	Definition        string                  `json:"definition"`
	Properties        map[string]PropertySpec `json:"properties"`
	DesiredProperties map[string]PropertySpec `json:"desiredProperties"`
}

// RelationshipSchema is the metadata-free derived relationship shape.
type RelationshipSchema struct {
	Target            []string `json:"target"`
	TargetEntityTypes []string `json:"targetEntityTypes"`
	MaxCardinality    int      `json:"maxCardinality"`
	Bidirectional     bool     `json:"bidirectional"`
}

var modelKnownKeys = map[string]struct{}{
	"modelId": {}, "version": {}, "kind": {}, "displayName": {},
	"unknownKeys": {}, "enforcementMode": {}, "attributes": {},
	"features": {}, "relationships": {},
}

var propertyKnownKeys = map[string]struct{}{
	"type": {}, "enum": {}, "minimum": {}, "maximum": {},
	"minLength": {}, "maxLength": {}, "pattern": {}, "required": {},
	"unit": {}, "writable": {},
}

var featureKnownKeys = map[string]struct{}{
	"definition": {}, "properties": {}, "desiredProperties": {},
}

var relationshipKnownKeys = map[string]struct{}{
	"target": {}, "targetEntityTypes": {}, "maxCardinality": {}, "bidirectional": {},
}

// These are validation keywords with semantics outside the supported subset.
// They are rejected rather than mistaken for harmless authored metadata.
var unsupportedValidationKeywords = map[string]struct{}{
	"$ref": {}, "$dynamicRef": {}, "$defs": {}, "allOf": {}, "anyOf": {}, "oneOf": {}, "not": {},
	"type": {}, "enum": {}, "minimum": {}, "maximum": {}, "minLength": {}, "maxLength": {}, "pattern": {}, "required": {},
	"const": {}, "multipleOf": {}, "exclusiveMinimum": {}, "exclusiveMaximum": {},
	"minItems": {}, "maxItems": {}, "uniqueItems": {}, "contains": {}, "minContains": {}, "maxContains": {},
	"minProperties": {}, "maxProperties": {}, "properties": {}, "patternProperties": {}, "propertyNames": {},
	"prefixItems": {}, "items": {}, "additionalProperties": {}, "unevaluatedItems": {}, "unevaluatedProperties": {},
	"dependentRequired": {}, "dependentSchemas": {}, "if": {}, "then": {}, "else": {}, "format": {},
}

// Normalize parses and validates an authored model and returns both its
// normalized authored representation and the metadata-free derived schema.
func Normalize(raw json.RawMessage) (Model, []byte, error) {
	if len(bytes.TrimSpace(raw)) == 0 {
		return Model{}, nil, fmt.Errorf("model: empty JSON")
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(raw, &object); err != nil {
		return Model{}, nil, fmt.Errorf("model: invalid JSON object: %w", err)
	}
	if object == nil || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return Model{}, nil, fmt.Errorf("model: expected JSON object")
	}
	if err := validateAuthoredShape(object); err != nil {
		return Model{}, nil, err
	}

	var model Model
	if err := json.Unmarshal(raw, &model); err != nil {
		return Model{}, nil, fmt.Errorf("model: decode: %w", err)
	}
	var err error
	model.ModelID, err = NormalizeModelID(model.ModelID)
	if err != nil {
		return Model{}, nil, fmt.Errorf("modelId: %w", err)
	}
	if err := validateVersion(model.Version); err != nil {
		return Model{}, nil, fmt.Errorf("version: %w", err)
	}
	model.Kind = strings.ToUpper(strings.TrimSpace(model.Kind))
	if model.Kind != "DEVICE" && model.Kind != "ASSET" {
		return Model{}, nil, fmt.Errorf("kind: must be DEVICE or ASSET")
	}
	if model.UnknownKeys == "" {
		model.UnknownKeys = "allow"
	}
	if model.UnknownKeys != "allow" && model.UnknownKeys != "reject" {
		return Model{}, nil, fmt.Errorf("unknownKeys: must be allow or reject")
	}
	if model.EnforcementMode == "" {
		model.EnforcementMode = "warn"
	}
	if model.EnforcementMode != "warn" && model.EnforcementMode != "reject" {
		return Model{}, nil, fmt.Errorf("enforcementMode: must be warn or reject")
	}
	materializeCollections(&model)
	if err := normalizeRelationships(model.Relationships); err != nil {
		return Model{}, nil, err
	}

	derived := derive(model)
	derivedJSON, err := json.Marshal(derived)
	if err != nil {
		return Model{}, nil, fmt.Errorf("derived schema: encode: %w", err)
	}
	return model, derivedJSON, nil
}

// NormalizeModelID applies the exact lower-then-replace lexical contract used
// by the registry SQL, then enforces the catalog column bounds.
func NormalizeModelID(raw string) (string, error) {
	canonical := canonicalModelID(raw)
	if canonical == "" || strings.Trim(canonical, "_") == "" {
		return "", fmt.Errorf("normalizes to an empty identifier")
	}
	if len([]byte(canonical)) > maxModelIDBytes {
		return "", fmt.Errorf("normalized identifier exceeds %d bytes", maxModelIDBytes)
	}
	return canonical, nil
}

func canonicalModelID(raw string) string {
	lower := strings.ToLower(raw)
	return strings.Trim(modelIDInvalidRE.ReplaceAllString(lower, "_"), "_")
}

func validateVersion(version string) error {
	matches := versionRE.FindStringSubmatch(version)
	if matches == nil {
		return fmt.Errorf("must contain exactly three canonical decimal components")
	}
	for _, component := range matches[1:] {
		if _, err := strconv.ParseInt(component, 10, 32); err != nil {
			return fmt.Errorf("component %q exceeds PostgreSQL int32", component)
		}
	}
	return nil
}

// ValidateVersion applies the catalog's canonical three-component, int32-safe
// version contract. Persistence and HTTP path validation share this function
// with Normalize so semantic ordering in PostgreSQL is always safe.
func ValidateVersion(version string) error {
	return validateVersion(version)
}

func validateAuthoredShape(object map[string]json.RawMessage) error {
	for _, key := range []string{"modelId", "version", "kind"} {
		value, ok := object[key]
		if !ok {
			return fmt.Errorf("%s: required", key)
		}
		var decoded string
		if err := json.Unmarshal(value, &decoded); err != nil || strings.TrimSpace(decoded) == "" {
			return fmt.Errorf("%s: must be a non-empty string", key)
		}
	}
	for key := range object {
		if _, known := modelKnownKeys[key]; known {
			continue
		}
		if _, validation := unsupportedValidationKeywords[key]; validation {
			return fmt.Errorf("%s: unsupported validation keyword", key)
		}
	}
	if err := validatePropertyCollectionRaw(object, "attributes"); err != nil {
		return err
	}
	if err := validateFeaturesRaw(object); err != nil {
		return err
	}
	return validateRelationshipsRaw(object)
}

func validatePropertyCollectionRaw(parent map[string]json.RawMessage, key string) error {
	raw, ok := parent[key]
	if !ok {
		return nil
	}
	properties, err := decodeObject(raw, key)
	if err != nil {
		return err
	}
	return validatePropertiesRaw(properties, key)
}

func validatePropertiesRaw(properties map[string]json.RawMessage, path string) error {
	for name, raw := range properties {
		propertyPath := joinPath(path, name)
		object, err := decodeObject(raw, propertyPath)
		if err != nil {
			return err
		}
		if _, ok := object["type"]; !ok {
			return fmt.Errorf("%s/type: required", propertyPath)
		}
		for key := range object {
			if _, known := propertyKnownKeys[key]; known {
				continue
			}
			if _, validation := unsupportedValidationKeywords[key]; validation {
				return fmt.Errorf("%s/%s: unsupported validation keyword", propertyPath, key)
			}
		}
		var spec PropertySpec
		if err := json.Unmarshal(raw, &spec); err != nil {
			return fmt.Errorf("%s: decode: %w", propertyPath, err)
		}
		if err := validatePropertySpec(spec, propertyPath); err != nil {
			return err
		}
	}
	return nil
}

func validatePropertySpec(spec PropertySpec, path string) error {
	switch spec.Type {
	case "string", "number", "integer", "boolean", "object", "array":
	default:
		return fmt.Errorf("%s/type: unsupported type %q", path, spec.Type)
	}
	if spec.MinLength != nil && *spec.MinLength < 0 {
		return fmt.Errorf("%s/minLength: must be non-negative", path)
	}
	if spec.MaxLength != nil && *spec.MaxLength < 0 {
		return fmt.Errorf("%s/maxLength: must be non-negative", path)
	}
	if spec.Minimum != nil && spec.Maximum != nil && *spec.Minimum > *spec.Maximum {
		return fmt.Errorf("%s/maximum: must be greater than or equal to minimum", path)
	}
	if spec.MinLength != nil && spec.MaxLength != nil && *spec.MinLength > *spec.MaxLength {
		return fmt.Errorf("%s/maxLength: must be greater than or equal to minLength", path)
	}
	if spec.Pattern != "" {
		if _, err := regexp.Compile(spec.Pattern); err != nil {
			return fmt.Errorf("%s/pattern: invalid regular expression: %w", path, err)
		}
	}
	return nil
}

func validateFeaturesRaw(parent map[string]json.RawMessage) error {
	raw, ok := parent["features"]
	if !ok {
		return nil
	}
	features, err := decodeObject(raw, "features")
	if err != nil {
		return err
	}
	for name, rawFeature := range features {
		path := joinPath("features", name)
		object, err := decodeObject(rawFeature, path)
		if err != nil {
			return err
		}
		definitionRaw, ok := object["definition"]
		if !ok {
			return fmt.Errorf("%s/definition: required", path)
		}
		var definition string
		if err := json.Unmarshal(definitionRaw, &definition); err != nil || !featureURNRE.MatchString(definition) {
			return fmt.Errorf("%s/definition: must match thingsflow:feature:<name>:<version>", path)
		}
		match := featureURNRE.FindStringSubmatch(definition)
		if err := validateVersion(match[1]); err != nil {
			return fmt.Errorf("%s/definition: %w", path, err)
		}
		for key := range object {
			if _, known := featureKnownKeys[key]; known {
				continue
			}
			if _, validation := unsupportedValidationKeywords[key]; validation {
				return fmt.Errorf("%s/%s: unsupported validation keyword", path, key)
			}
		}
		for _, collection := range []string{"properties", "desiredProperties"} {
			propertyRaw, exists := object[collection]
			if !exists {
				continue
			}
			properties, err := decodeObject(propertyRaw, joinPath(path, collection))
			if err != nil {
				return err
			}
			if err := validatePropertiesRaw(properties, joinPath(path, collection)); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateRelationshipsRaw(parent map[string]json.RawMessage) error {
	raw, ok := parent["relationships"]
	if !ok {
		return nil
	}
	relationships, err := decodeObject(raw, "relationships")
	if err != nil {
		return err
	}
	for name, rawRelationship := range relationships {
		path := joinPath("relationships", name)
		object, err := decodeObject(rawRelationship, path)
		if err != nil {
			return err
		}
		for _, field := range []string{"target", "targetEntityTypes", "maxCardinality", "bidirectional"} {
			if _, exists := object[field]; !exists {
				return fmt.Errorf("%s/%s: required", path, field)
			}
		}
		for key := range object {
			if _, known := relationshipKnownKeys[key]; known {
				continue
			}
			if _, validation := unsupportedValidationKeywords[key]; validation {
				return fmt.Errorf("%s/%s: unsupported validation keyword", path, key)
			}
		}
		var relationship Relationship
		if err := json.Unmarshal(rawRelationship, &relationship); err != nil {
			return fmt.Errorf("%s: decode: %w", path, err)
		}
		if relationship.MaxCardinality <= 0 {
			return fmt.Errorf("%s/maxCardinality: must be a positive integer", path)
		}
		if len(relationship.Target) == 0 {
			return fmt.Errorf("%s/target: must contain at least one model ID", path)
		}
		if len(relationship.TargetEntityTypes) == 0 {
			return fmt.Errorf("%s/targetEntityTypes: must contain at least one entity type", path)
		}
	}
	return nil
}

func normalizeRelationships(relationships map[string]Relationship) error {
	for name, relationship := range relationships {
		seenTargets := map[string]struct{}{}
		for i, target := range relationship.Target {
			normalized, err := NormalizeModelID(target)
			if err != nil {
				return fmt.Errorf("relationships/%s/target/%d: %w", name, i, err)
			}
			if _, exists := seenTargets[normalized]; exists {
				return fmt.Errorf("relationships/%s/target/%d: duplicate normalized model ID %q", name, i, normalized)
			}
			seenTargets[normalized] = struct{}{}
			relationship.Target[i] = normalized
		}
		seenTypes := map[string]struct{}{}
		for i, entityType := range relationship.TargetEntityTypes {
			entityType = strings.ToUpper(strings.TrimSpace(entityType))
			if entityType != "DEVICE" && entityType != "ASSET" {
				return fmt.Errorf("relationships/%s/targetEntityTypes/%d: must be DEVICE or ASSET", name, i)
			}
			if _, exists := seenTypes[entityType]; exists {
				return fmt.Errorf("relationships/%s/targetEntityTypes/%d: duplicate entity type %q", name, i, entityType)
			}
			seenTypes[entityType] = struct{}{}
			relationship.TargetEntityTypes[i] = entityType
		}
		relationships[name] = relationship
	}
	return nil
}

func materializeCollections(model *Model) {
	if model.Attributes == nil {
		model.Attributes = map[string]PropertySpec{}
	}
	if model.Features == nil {
		model.Features = map[string]Feature{}
	}
	if model.Relationships == nil {
		model.Relationships = map[string]Relationship{}
	}
	for name, feature := range model.Features {
		if feature.Properties == nil {
			feature.Properties = map[string]PropertySpec{}
		}
		if feature.DesiredProperties == nil {
			feature.DesiredProperties = map[string]PropertySpec{}
		}
		model.Features[name] = feature
	}
}

func derive(model Model) DerivedSchema {
	derived := DerivedSchema{
		ModelID: model.ModelID, Version: model.Version, Kind: model.Kind,
		UnknownKeys: model.UnknownKeys, EnforcementMode: model.EnforcementMode,
		Attributes: map[string]PropertySpec{}, Features: map[string]FeatureSchema{},
		Relationships: map[string]RelationshipSchema{},
	}
	for name, spec := range model.Attributes {
		derived.Attributes[name] = stripPropertyMetadata(spec)
	}
	for name, feature := range model.Features {
		out := FeatureSchema{Definition: feature.Definition, Properties: map[string]PropertySpec{}, DesiredProperties: map[string]PropertySpec{}}
		for property, spec := range feature.Properties {
			out.Properties[property] = stripPropertyMetadata(spec)
		}
		for property, spec := range feature.DesiredProperties {
			out.DesiredProperties[property] = stripPropertyMetadata(spec)
		}
		derived.Features[name] = out
	}
	for name, relationship := range model.Relationships {
		derived.Relationships[name] = RelationshipSchema{
			Target: append([]string(nil), relationship.Target...), TargetEntityTypes: append([]string(nil), relationship.TargetEntityTypes...),
			MaxCardinality: relationship.MaxCardinality, Bidirectional: relationship.Bidirectional,
		}
	}
	return derived
}

func stripPropertyMetadata(spec PropertySpec) PropertySpec {
	spec.Extra = nil
	return spec
}

func decodeObject(raw json.RawMessage, path string) (map[string]json.RawMessage, error) {
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return nil, fmt.Errorf("%s: must be a JSON object", path)
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(raw, &object); err != nil || object == nil {
		return nil, fmt.Errorf("%s: must be a JSON object", path)
	}
	return object, nil
}

func joinPath(parts ...string) string {
	if len(parts) == 0 {
		return ""
	}
	out := make([]string, 0, len(parts))
	if base := strings.Trim(parts[0], "/"); base != "" {
		out = append(out, base)
	}
	for _, part := range parts[1:] {
		if part != "" {
			part = strings.ReplaceAll(part, "~", "~0")
			part = strings.ReplaceAll(part, "/", "~1")
			out = append(out, part)
		}
	}
	return strings.Join(out, "/")
}

func (model *Model) UnmarshalJSON(data []byte) error {
	type plain Model
	var value plain
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	value.Extra = unknownFields(raw, modelKnownKeys)
	*model = Model(value)
	return nil
}

func (model Model) MarshalJSON() ([]byte, error) {
	type plain Model
	value := plain(model)
	value.Extra = nil
	return marshalWithExtra(value, model.Extra)
}

func (spec *PropertySpec) UnmarshalJSON(data []byte) error {
	type plain PropertySpec
	var value plain
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	value.Extra = unknownFields(raw, propertyKnownKeys)
	*spec = PropertySpec(value)
	return nil
}

func (spec PropertySpec) MarshalJSON() ([]byte, error) {
	type plain PropertySpec
	value := plain(spec)
	value.Extra = nil
	return marshalWithExtra(value, spec.Extra)
}

func (feature *Feature) UnmarshalJSON(data []byte) error {
	type plain Feature
	var value plain
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	value.Extra = unknownFields(raw, featureKnownKeys)
	*feature = Feature(value)
	return nil
}

func (feature Feature) MarshalJSON() ([]byte, error) {
	type plain Feature
	value := plain(feature)
	value.Extra = nil
	return marshalWithExtra(value, feature.Extra)
}

func (relationship *Relationship) UnmarshalJSON(data []byte) error {
	type plain Relationship
	var value plain
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	value.Extra = unknownFields(raw, relationshipKnownKeys)
	*relationship = Relationship(value)
	return nil
}

func (relationship Relationship) MarshalJSON() ([]byte, error) {
	type plain Relationship
	value := plain(relationship)
	value.Extra = nil
	return marshalWithExtra(value, relationship.Extra)
}

func unknownFields(raw map[string]json.RawMessage, known map[string]struct{}) map[string]json.RawMessage {
	extra := map[string]json.RawMessage{}
	for key, value := range raw {
		if _, exists := known[key]; !exists {
			extra[key] = append(json.RawMessage(nil), value...)
		}
	}
	if len(extra) == 0 {
		return nil
	}
	return extra
}

func marshalWithExtra(value interface{}, extra map[string]json.RawMessage) ([]byte, error) {
	base, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	if len(extra) == 0 {
		return base, nil
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(base, &object); err != nil {
		return nil, err
	}
	for key, raw := range extra {
		if _, reserved := object[key]; !reserved {
			object[key] = raw
		}
	}
	return json.Marshal(object)
}

func enumContainsNumber(values []interface{}, want float64) bool {
	for _, value := range values {
		if number, ok := numericValue(value); ok && number == want {
			return true
		}
	}
	return false
}
