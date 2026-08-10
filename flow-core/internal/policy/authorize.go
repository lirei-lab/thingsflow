package policy

import (
	"strings"
)

// ResourcePath builds the canonical thing:/... resource path for a twin
// resource. suffix extends the base path with feature/attribute/model
// granularity, e.g. ResourcePath(tid, "DEVICE", id, "features", "temp")
// → "thing:/<tid>/DEVICE/<id>/features/temp".
func ResourcePath(tenantID, entityType, entityID string, suffix ...string) string {
	parts := append([]string{"thing:", tenantID, strings.ToUpper(strings.TrimSpace(entityType)), entityID}, suffix...)
	return strings.Join(parts, "/")
}

// SubjectForClaims derives the caller's policy subject from verified JWT
// claims. SYS_ADMIN is a special subject that always passes (the middleware
// short-circuits it); a tenant-bound caller becomes tenant:<id>, and a
// user-bound caller becomes user:<id>.
func SubjectForClaims(claims map[string]interface{}) string {
	scopes, _ := claims["scopes"].([]interface{})
	for _, scope := range scopes {
		if value, ok := scope.(string); ok && value == "SYS_ADMIN" {
			return "SYS_ADMIN"
		}
	}
	if tenantID, _ := claims["tenantId"].(string); tenantID != "" {
		return "tenant:" + tenantID
	}
	if userID, _ := claims["userId"].(string); userID != "" {
		return "user:" + userID
	}
	return ""
}

// Authorize evaluates a caller subject against a resolved policy document with
// Ditto-style precedence:
//
//   - The DEEPEST matching grant/revoke resource path decides. A resource
//     pattern matches a concrete path segment-by-segment; '*' matches exactly
//     one segment, '#' matches the remainder (recursive), and a plain prefix
//     pattern covers all descendant paths (so a root grant authorizes the tree
//     below it).
//   - At that depth a revoke overrides a grant (revoke profundo gana).
//   - A deeper entry that does not cover the requested action does NOT fall
//     back to a shallower grant (deny at the deepest matched level).
//   - An unmatched path fails closed (deny).
//
// Pure function — unit-testable without a database.
func Authorize(subject string, doc DerivedPolicy, resourcePath, action string) bool {
	if !subjectMatches(subject, doc.Subjects) {
		return false
	}
	action = strings.ToUpper(strings.TrimSpace(action))

	deepest := -1
	for path := range doc.Grants {
		if depth, ok := matchResource(path, resourcePath); ok && depth > deepest {
			deepest = depth
		}
	}
	for path := range doc.Revokes {
		if depth, ok := matchResource(path, resourcePath); ok && depth > deepest {
			deepest = depth
		}
	}
	if deepest < 0 {
		return false // unmatched → fail closed
	}

	// At the deepest depth, a revoke covering the action wins over any grant.
	for path, actions := range doc.Revokes {
		if depth, ok := matchResource(path, resourcePath); ok && depth == deepest && actionCovered(actions, action) {
			return false
		}
	}
	for path, actions := range doc.Grants {
		if depth, ok := matchResource(path, resourcePath); ok && depth == deepest && actionCovered(actions, action) {
			return true
		}
	}
	// The deepest entry exists but does not cover the action → fail closed.
	return false
}

func actionCovered(actions []string, action string) bool {
	for _, candidate := range actions {
		if candidate == "*" || candidate == action {
			return true
		}
	}
	return false
}

// subjectMatches reports whether the caller's subject is bound by the policy's
// subject list. "*" binds everyone; tenant:<id> binds the tenant's own subject;
// user:<id> binds a specific user. A SYS_ADMIN subject matches only "*" here —
// the middleware short-circuits SYS_ADMIN before reaching Authorize.
func subjectMatches(subject string, subjects []string) bool {
	if subject == "" {
		return false
	}
	for _, bound := range subjects {
		if bound == "*" || bound == subject {
			return true
		}
	}
	return false
}

// matchResource reports whether a policy resource pattern matches a concrete
// thing:/... path, returning the pattern's segment depth (deeper wins). '*' is
// a single-segment wildcard; '#' is a recursive wildcard that matches the rest
// of the path; a plain prefix pattern covers all descendant paths.
func matchResource(pattern, resource string) (int, bool) {
	patternSegments := strings.Split(strings.TrimPrefix(pattern, "thing:/"), "/")
	resourceSegments := strings.Split(strings.TrimPrefix(resource, "thing:/"), "/")
	for i, segment := range patternSegments {
		if segment == "#" {
			// Recursive: matches the remainder (including nothing more).
			return i + 1, true
		}
		if i >= len(resourceSegments) {
			return 0, false // pattern is deeper than the resource path
		}
		if segment == "*" {
			continue // matches exactly one segment
		}
		if segment != resourceSegments[i] {
			return 0, false
		}
	}
	// Pattern consumed: it is a prefix (covers descendants) or exact match.
	return len(patternSegments), true
}
