package twinmodel

import (
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"unicode/utf8"
)

// Violation identifies one failed model constraint at a JSON-Pointer-like
// path. Got is the observed value, or nil when a required value is absent.
type Violation struct {
	Pointer string      `json:"pointer"`
	Keyword string      `json:"keyword"`
	Message string      `json:"message"`
	Got     interface{} `json:"got"`
}

// Validate checks attributes and modeled feature property maps. Validation is
// partial by default; pass true as complete[0] to enforce required properties.
// Null values are accepted defensively. The unmodeled telemetry catch-all is
// deliberately outside unknown-key and property validation.
func Validate(schema DerivedSchema, basePath string, values map[string]interface{}, complete ...bool) []Violation {
	requireComplete := len(complete) > 0 && complete[0]
	violations := make([]Violation, 0)

	attributes, _ := values["attributes"].(map[string]interface{})
	violations = append(violations, validatePropertyMap(
		schema.Attributes, attributes, pointer(basePath, "attributes"), requireComplete, false,
	)...)

	features, _ := values["features"].(map[string]interface{})
	featureNames := sortedFeatureNames(schema.Features)
	for _, featureName := range featureNames {
		if featureName == "telemetry" {
			continue
		}
		featureSchema := schema.Features[featureName]
		featureValue, _ := features[featureName].(map[string]interface{})
		properties, _ := featureValue["properties"].(map[string]interface{})
		desired, _ := featureValue["desiredProperties"].(map[string]interface{})

		violations = append(violations, validatePropertyMap(
			featureSchema.Properties,
			properties,
			pointer(basePath, "features", featureName, "properties"),
			requireComplete,
			schema.UnknownKeys == "reject",
		)...)
		violations = append(violations, validatePropertyMap(
			featureSchema.DesiredProperties,
			desired,
			pointer(basePath, "features", featureName, "desiredProperties"),
			requireComplete,
			schema.UnknownKeys == "reject",
		)...)
	}

	sort.SliceStable(violations, func(i, j int) bool {
		if violations[i].Pointer == violations[j].Pointer {
			return violations[i].Keyword < violations[j].Keyword
		}
		return violations[i].Pointer < violations[j].Pointer
	})
	return violations
}

func validatePropertyMap(schema map[string]PropertySpec, values map[string]interface{}, path string, complete, rejectUnknown bool) []Violation {
	violations := make([]Violation, 0)
	if values == nil {
		values = map[string]interface{}{}
	}
	if rejectUnknown {
		keys := make([]string, 0, len(values))
		for key := range values {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			if _, modeled := schema[key]; !modeled {
				violations = append(violations, Violation{
					Pointer: pointer(path, key), Keyword: "unknown",
					Message: "property is not declared by the model", Got: values[key],
				})
			}
		}
	}

	keys := make([]string, 0, len(schema))
	for key := range schema {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		spec := schema[key]
		value, present := values[key]
		propertyPath := pointer(path, key)
		if !present {
			if complete && spec.Required {
				violations = append(violations, Violation{
					Pointer: propertyPath, Keyword: "required",
					Message: "required value is missing", Got: nil,
				})
			}
			continue
		}
		if value == nil {
			continue
		}
		violations = append(violations, validateValue(spec, propertyPath, value)...)
	}
	return violations
}

func validateValue(spec PropertySpec, path string, value interface{}) []Violation {
	violations := make([]Violation, 0, 4)
	if !matchesType(spec.Type, value) {
		violations = append(violations, Violation{
			Pointer: path, Keyword: "type",
			Message: fmt.Sprintf("expected %s", spec.Type), Got: value,
		})
	}
	if len(spec.Enum) > 0 && !enumContains(spec.Enum, value) {
		violations = append(violations, Violation{
			Pointer: path, Keyword: "enum",
			Message: "value is not in the allowed set", Got: value,
		})
	}
	if number, ok := numericValue(value); ok {
		if spec.Minimum != nil && number < *spec.Minimum {
			violations = append(violations, Violation{
				Pointer: path, Keyword: "minimum",
				Message: fmt.Sprintf("must be greater than or equal to %v", *spec.Minimum), Got: value,
			})
		}
		if spec.Maximum != nil && number > *spec.Maximum {
			violations = append(violations, Violation{
				Pointer: path, Keyword: "maximum",
				Message: fmt.Sprintf("must be less than or equal to %v", *spec.Maximum), Got: value,
			})
		}
	}
	if text, ok := value.(string); ok {
		length := utf8.RuneCountInString(text)
		if spec.MinLength != nil && length < *spec.MinLength {
			violations = append(violations, Violation{
				Pointer: path, Keyword: "minLength",
				Message: fmt.Sprintf("length must be at least %d", *spec.MinLength), Got: value,
			})
		}
		if spec.MaxLength != nil && length > *spec.MaxLength {
			violations = append(violations, Violation{
				Pointer: path, Keyword: "maxLength",
				Message: fmt.Sprintf("length must be at most %d", *spec.MaxLength), Got: value,
			})
		}
		if spec.Pattern != "" && !regexp.MustCompile(spec.Pattern).MatchString(text) {
			violations = append(violations, Violation{
				Pointer: path, Keyword: "pattern",
				Message: "string does not match the required pattern", Got: value,
			})
		}
	}
	return violations
}

func matchesType(kind string, value interface{}) bool {
	switch kind {
	case "string":
		_, ok := value.(string)
		return ok
	case "boolean":
		_, ok := value.(bool)
		return ok
	case "object":
		_, ok := value.(map[string]interface{})
		return ok
	case "array":
		if _, ok := value.([]interface{}); ok {
			return true
		}
		kind := reflect.TypeOf(value)
		return kind != nil && (kind.Kind() == reflect.Array || kind.Kind() == reflect.Slice)
	case "number":
		_, ok := numericValue(value)
		return ok
	case "integer":
		number, ok := numericValue(value)
		return ok && math.Trunc(number) == number
	default:
		return false
	}
}

func numericValue(value interface{}) (float64, bool) {
	var number float64
	switch value := value.(type) {
	case json.Number:
		parsed, err := value.Float64()
		if err != nil {
			return 0, false
		}
		number = parsed
	case float64:
		number = value
	case float32:
		number = float64(value)
	case int:
		number = float64(value)
	case int8:
		number = float64(value)
	case int16:
		number = float64(value)
	case int32:
		number = float64(value)
	case int64:
		number = float64(value)
	case uint:
		number = float64(value)
	case uint8:
		number = float64(value)
	case uint16:
		number = float64(value)
	case uint32:
		number = float64(value)
	case uint64:
		number = float64(value)
	default:
		return 0, false
	}
	if math.IsNaN(number) || math.IsInf(number, 0) {
		return 0, false
	}
	return number, true
}

func enumContains(allowed []interface{}, got interface{}) bool {
	gotNumber, gotIsNumber := numericValue(got)
	for _, candidate := range allowed {
		if candidateNumber, ok := numericValue(candidate); ok && gotIsNumber {
			if candidateNumber == gotNumber {
				return true
			}
			continue
		}
		if reflect.DeepEqual(candidate, got) {
			return true
		}
	}
	return false
}

func sortedFeatureNames(features map[string]FeatureSchema) []string {
	names := make([]string, 0, len(features))
	for name := range features {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func pointer(base string, parts ...string) string {
	base = strings.Trim(base, "/")
	out := make([]string, 0, len(parts)+1)
	if base != "" {
		out = append(out, base)
	}
	for _, part := range parts {
		if part == "" {
			continue
		}
		part = strings.ReplaceAll(part, "~", "~0")
		part = strings.ReplaceAll(part, "/", "~1")
		out = append(out, part)
	}
	return strings.Join(out, "/")
}
