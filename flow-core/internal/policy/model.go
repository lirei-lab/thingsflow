// Package policy implements a tenant-scoped, versioned policy catalog and the
// resolution of a twin registry policyId (a string like tenant:<tid>:default)
// into a real policy document. The document (subjects, thing:/... resources,
// Ditto-style grant/revoke entries) is what the Phase 6 twin API enforcement
// middleware evaluates; this package owns the model + store + CRUD only.
package policy

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

const (
	maxPolicyIDBytes = 255
	// DefaultPolicyID is the canonical catalog id that backs the registry's
	// tenant:<tid>:default seed. When no explicit 'default' row exists for a
	// tenant, Resolve falls back to the built-in owner-full-access policy so
	// legacy twins keep working.
	DefaultPolicyID = "default"
)

var (
	policyIDInvalidRE = regexp.MustCompile(`[^a-z0-9_]+`)
	versionRE         = regexp.MustCompile(`^(0|[1-9][0-9]{0,9})\.(0|[1-9][0-9]{0,9})\.(0|[1-9][0-9]{0,9})$`)
	// resourceSegmentRE is the permissive segment charset for a thing:/... path:
	// lower/upper alnum, underscore, hyphen, dot, '*' single-segment wildcard and
	// '#' recursive wildcard. Tenant UUIDs, DEVICE/ASSET kinds, feature names and
	// attribute keys all fit.
	resourceSegmentRE = regexp.MustCompile(`^[A-Za-z0-9_*#.-]+$`)
	// subjectRE is the policy subject shape: tenant:<id> | user:<id> | role:<name>
	// or the '*' all-subjects wildcard.
	subjectRE = regexp.MustCompile(`^(tenant|user|role):[A-Za-z0-9_:@.-]+$|^\*$`)
)

var knownActions = map[string]struct{}{
	"READ": {}, "WRITE": {}, "DELETE": {}, "*": {},
}

// Policy is the authored policy document round-tripped by the CRUD API. The
// derived, normalized form (DerivedPolicy) is what the enforcer reads.
type Policy struct {
	PolicyID  string   `json:"policyId"`
	Version   string   `json:"version"`
	Kind      string   `json:"kind,omitempty"`
	Subjects  []string `json:"subjects"`
	Resources []string `json:"resources"`
	Grants    []Entry  `json:"grants"`
	Revokes   []Entry  `json:"revokes"`
}

// Entry is one grant/revoke rule: a resource path (thing:/...) and the actions
// it affects. A revoke at a deeper path overrides a grant at any shallower
// path (Ditto-style deep revoke wins).
type Entry struct {
	Resource string   `json:"resource"`
	Actions  []string `json:"actions"`
}

// DerivedPolicy is the normalized schema persisted beside the authored
// document. Grants and Revokes are collapsed into deterministic resource ->
// sorted action maps so the authorizer can compute effective allow/deny from
// the deepest matching path without re-parsing authored JSON.
type DerivedPolicy struct {
	PolicyID  string              `json:"policyId"`
	Version   string              `json:"version"`
	Kind      string              `json:"kind"`
	Subjects  []string            `json:"subjects"`
	Resources []string            `json:"resources"`
	Grants    map[string][]string `json:"grants"`
	Revokes   map[string][]string `json:"revokes"`
	// Builtin marks the implicit owner-full-access fallback (no catalog row).
	Builtin bool `json:"builtin,omitempty"`
}

// Record is one immutable authored policy version plus catalog lifecycle data.
type Record struct {
	Policy      Policy          `json:"-"`
	Definition  json.RawMessage `json:"-"`
	Schema      json.RawMessage `json:"-"`
	Deprecated  bool            `json:"deprecated"`
	CreatedTime int64           `json:"createdTime"`
	UpdatedTime int64           `json:"updatedTime"`
}

// Normalize parses and validates an authored policy and returns its normalized
// form and the deterministic derived schema. Validation rejects non-canonical
// policyIds/versions, malformed thing:/... resource paths, unknown subjects
// and unknown actions — nothing invalid is ever persisted.
func Normalize(raw json.RawMessage) (Policy, DerivedPolicy, error) {
	if len(bytes.TrimSpace(raw)) == 0 {
		return Policy{}, DerivedPolicy{}, fmt.Errorf("policy: empty JSON")
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(raw, &object); err != nil {
		return Policy{}, DerivedPolicy{}, fmt.Errorf("policy: invalid JSON object: %w", err)
	}
	if object == nil || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return Policy{}, DerivedPolicy{}, fmt.Errorf("policy: expected JSON object")
	}
	for _, required := range []string{"policyId", "version"} {
		if _, ok := object[required]; !ok {
			return Policy{}, DerivedPolicy{}, fmt.Errorf("%s: required", required)
		}
	}

	var p Policy
	if err := json.Unmarshal(raw, &p); err != nil {
		return Policy{}, DerivedPolicy{}, fmt.Errorf("policy: decode: %w", err)
	}
	canonical, err := NormalizePolicyID(p.PolicyID)
	if err != nil {
		return Policy{}, DerivedPolicy{}, fmt.Errorf("policyId: %w", err)
	}
	p.PolicyID = canonical
	if err := validateVersion(p.Version); err != nil {
		return Policy{}, DerivedPolicy{}, fmt.Errorf("version: %w", err)
	}
	if p.Kind == "" {
		p.Kind = "TWIN"
	}
	if p.Kind != "TWIN" {
		return Policy{}, DerivedPolicy{}, fmt.Errorf("kind: must be TWIN")
	}

	// Subjects: normalize (trim, drop empties), validate shape, de-dupe, sort.
	seenSubjects := map[string]struct{}{}
	subjects := []string{}
	for _, rawSubject := range p.Subjects {
		subject := strings.TrimSpace(rawSubject)
		if subject == "" {
			continue
		}
		if !subjectRE.MatchString(subject) {
			return Policy{}, DerivedPolicy{}, fmt.Errorf("subject %q: invalid (want tenant:|user:|role: or *)", subject)
		}
		if _, dup := seenSubjects[subject]; dup {
			continue
		}
		seenSubjects[subject] = struct{}{}
		subjects = append(subjects, subject)
	}
	sort.Strings(subjects)
	p.Subjects = subjects

	// Resources: validate each starts with thing:/ and has well-formed segments.
	resources := []string{}
	seenResources := map[string]struct{}{}
	for _, rawResource := range p.Resources {
		resource := strings.TrimSpace(rawResource)
		if err := validateResourcePath(resource); err != nil {
			return Policy{}, DerivedPolicy{}, fmt.Errorf("resource %q: %w", resource, err)
		}
		if _, dup := seenResources[resource]; dup {
			continue
		}
		seenResources[resource] = struct{}{}
		resources = append(resources, resource)
	}
	sort.Strings(resources)
	p.Resources = resources

	grants, err := normalizeEntries(p.Grants, "grant")
	if err != nil {
		return Policy{}, DerivedPolicy{}, err
	}
	revokes, err := normalizeEntries(p.Revokes, "revoke")
	if err != nil {
		return Policy{}, DerivedPolicy{}, err
	}
	p.Grants = grants
	p.Revokes = revokes

	derived := DerivedPolicy{
		PolicyID:  p.PolicyID,
		Version:   p.Version,
		Kind:      p.Kind,
		Subjects:  p.Subjects,
		Resources: p.Resources,
		Grants:    entriesToMap(grants),
		Revokes:   entriesToMap(revokes),
	}
	return p, derived, nil
}

func normalizeEntries(entries []Entry, label string) ([]Entry, error) {
	normalized := []Entry{}
	seen := map[string]struct{}{}
	for _, entry := range entries {
		resource := strings.TrimSpace(entry.Resource)
		if err := validateResourcePath(resource); err != nil {
			return nil, fmt.Errorf("%s resource %q: %w", label, resource, err)
		}
		actions, err := normalizeActions(entry.Actions, label, resource)
		if err != nil {
			return nil, err
		}
		if _, dup := seen[resource]; dup {
			return nil, fmt.Errorf("%s resource %q: duplicate entry", label, resource)
		}
		seen[resource] = struct{}{}
		normalized = append(normalized, Entry{Resource: resource, Actions: actions})
	}
	sort.Slice(normalized, func(i, j int) bool { return normalized[i].Resource < normalized[j].Resource })
	return normalized, nil
}

func normalizeActions(actions []string, label, resource string) ([]string, error) {
	if len(actions) == 0 {
		return nil, fmt.Errorf("%s resource %q: actions required", label, resource)
	}
	seen := map[string]struct{}{}
	out := []string{}
	for _, rawAction := range actions {
		action := strings.ToUpper(strings.TrimSpace(rawAction))
		if _, ok := knownActions[action]; !ok {
			return nil, fmt.Errorf("%s resource %q: unknown action %q", label, resource, rawAction)
		}
		if _, dup := seen[action]; dup {
			continue
		}
		seen[action] = struct{}{}
		out = append(out, action)
	}
	sort.Strings(out)
	return out, nil
}

func entriesToMap(entries []Entry) map[string][]string {
	if len(entries) == 0 {
		return map[string][]string{}
	}
	out := make(map[string][]string, len(entries))
	for _, entry := range entries {
		out[entry.Resource] = entry.Actions
	}
	return out
}

func validateResourcePath(resource string) error {
	if !strings.HasPrefix(resource, "thing:/") {
		return fmt.Errorf("must start with thing:/")
	}
	rest := strings.TrimPrefix(resource, "thing:/")
	if rest == "" {
		return fmt.Errorf("empty path after thing:/")
	}
	segments := strings.Split(rest, "/")
	for _, segment := range segments {
		if segment == "" {
			return fmt.Errorf("empty path segment")
		}
		if !resourceSegmentRE.MatchString(segment) {
			return fmt.Errorf("invalid segment %q", segment)
		}
	}
	return nil
}

// NormalizePolicyID applies the canonical lower-then-replace contract for a
// policyId, matching the registry's lexical style and column bounds.
func NormalizePolicyID(raw string) (string, error) {
	canonical := canonicalPolicyID(raw)
	if canonical == "" || strings.Trim(canonical, "_") == "" {
		return "", fmt.Errorf("normalizes to an empty identifier")
	}
	if len([]byte(canonical)) > maxPolicyIDBytes {
		return "", fmt.Errorf("normalized identifier exceeds %d bytes", maxPolicyIDBytes)
	}
	return canonical, nil
}

func canonicalPolicyID(raw string) string {
	lower := strings.ToLower(raw)
	return strings.Trim(policyIDInvalidRE.ReplaceAllString(lower, "_"), "_")
}

// ValidateVersion applies the catalog's canonical three-component, int32-safe
// version contract shared by Normalize and HTTP path validation.
func ValidateVersion(version string) error {
	return validateVersion(version)
}

func validateVersion(version string) error {
	matches := versionRE.FindStringSubmatch(version)
	if matches == nil {
		return fmt.Errorf("must contain exactly three canonical decimal components")
	}
	for _, component := range matches[1:] {
		value, err := strconv.ParseInt(component, 10, 32)
		if err != nil || value > math.MaxInt32 {
			return fmt.Errorf("component %q exceeds PostgreSQL int32", component)
		}
	}
	return nil
}
