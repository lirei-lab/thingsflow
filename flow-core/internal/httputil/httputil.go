// Package httputil holds the small set of HTTP helpers shared by every
// handler in flow-core: a TB-style JSON writer, a TB-style error envelope,
// query-string coercion, the {entityType,id} unwrapper used by every CRUD
// endpoint, and the JWT auth gate.
//
// These were defined six different times in package main before the
// extraction. Centralizing them here means handlers can be moved out of
// package main without dragging string-matching utility functions along.
package httputil

import (
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/golang-jwt/jwt/v5"

	authpkg "flow-core/internal/auth"
)

// WriteJSON serializes payload with status. Suppresses the encode error —
// at this point the response writer is already committed, the connection
// will surface the error, and there's nothing useful we can return to
// the handler.
func WriteJSON(w http.ResponseWriter, status int, payload interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

// WriteError writes a TB-classic error envelope:
//
//	{ "status": 401, "message": "...", "errorCode": 10, "timestamp": ... }
//
// errorCode 10 is what TB uses for generic auth/permission failures; we
// reuse it for every error path so the UI's interceptor handles them
// uniformly. Replaces the misleadingly named sendAuthError.
func WriteError(w http.ResponseWriter, status int, message string) {
	WriteJSON(w, status, map[string]interface{}{
		"status":    status,
		"message":   message,
		"errorCode": 10,
		"timestamp": time.Now().UnixMilli(),
	})
}

// IntParam reads ?key= from the URL and parses it as int. Returns
// defaultVal on missing or unparseable input — handlers don't 400 on a
// bad pageSize, they fall back, matching TB classic behavior.
func IntParam(r *http.Request, key string, defaultVal int) int {
	val := r.URL.Query().Get(key)
	if val == "" {
		return defaultVal
	}
	n, err := strconv.Atoi(val)
	if err != nil {
		return defaultVal
	}
	return n
}

// MaxPageSize is the hard upper bound for every paginated endpoint.
//
// Phase 5c: IntParam returned whatever the client sent, so
// `?pageSize=100000000` made Postgres sort and stream a whole tenant table
// into Go maps — a handful of concurrent requests OOMs a 1Gi pod. 1000 is
// generous for real use (the TB UI's largest page is 100, and a bulk export
// script paging at 1000 still works unchanged) while capping one request's
// footprint at a few MB. It mirrors the bound the WS plane already applies to
// entity-data pageLinks (1024).
const MaxPageSize = 1000

// PageSize reads ?pageSize= and returns it clamped into [1, MaxPageSize].
// Missing, unparseable, or non-positive input falls back to defaultVal —
// handlers don't 400 on a bad pageSize, matching TB classic behavior.
// Use this instead of IntParam(r, "pageSize", …) on every paginated endpoint.
func PageSize(r *http.Request, defaultVal int) int {
	return ClampPageSize(IntParam(r, "pageSize", defaultVal), defaultVal)
}

// ClampPageSize applies the same bound to a page size that did not come from
// the query string — e.g. an entity-query pageLink carried in the JSON body.
func ClampPageSize(n, defaultVal int) int {
	if n < 1 {
		n = defaultVal
	}
	if n < 1 {
		return 1
	}
	if n > MaxPageSize {
		return MaxPageSize
	}
	return n
}

// ExtractEntityID pulls the inner "id" out of a TB-style {entityType, id}
// JSON object, or returns the string verbatim if the field is already a
// bare string. Returns "" if the field is missing or in an unknown shape.
func ExtractEntityID(body map[string]interface{}, key string) string {
	v, ok := body[key]
	if !ok || v == nil {
		return ""
	}
	if s, ok := v.(string); ok {
		return s
	}
	if m, ok := v.(map[string]interface{}); ok {
		if id, ok := m["id"].(string); ok {
			return id
		}
	}
	return ""
}

// RequireAuth enforces a valid JWT on the request. On failure it writes
// a 401 with the canonical TB error envelope and returns ok=false so the
// handler can `if !ok { return }` and bail out cleanly.
func RequireAuth(w http.ResponseWriter, r *http.Request) (jwt.MapClaims, bool) {
	claims, err := authpkg.Extract(r)
	if err != nil {
		WriteError(w, http.StatusUnauthorized, "Authentication required")
		return nil, false
	}
	return claims, true
}

// ExtractToken returns the validated JWT claims without writing anything
// to the response. Used by handlers that want to customize the error
// payload (e.g. /api/auth/user uses "Invalid or expired token" instead
// of the canonical "Authentication required" message).
func ExtractToken(r *http.Request) (jwt.MapClaims, error) {
	return authpkg.Extract(r)
}

// CoalesceStr returns *p if non-nil and non-empty, else fallback.
// Used by handlers that project optional varchar columns into the response.
func CoalesceStr(p *string, fallback string) string {
	if p == nil || *p == "" {
		return fallback
	}
	return *p
}

// SetOptional sets m[key]=*val if val is non-nil, otherwise m[key]=nil.
// Keeps response shape consistent — TB clients expect every documented
// field to be present, even when null.
func SetOptional(m map[string]interface{}, key string, val *string) {
	if val != nil {
		m[key] = *val
	} else {
		m[key] = nil
	}
}

// LooksLikeUUID is true for a 36-char canonical UUID string with dashes
// at positions 8/13/18/23. Used by routers that share a path prefix
// between an ID-bearing route and an alphabetic alias route.
func LooksLikeUUID(s string) bool {
	if len(s) != 36 {
		return false
	}
	return s[8] == '-' && s[13] == '-' && s[18] == '-' && s[23] == '-'
}

// RequireSysAdmin gates a handler on a JWT carrying the SYS_ADMIN scope.
// Writes 401 if no JWT, 403 if the scope is missing.
func RequireSysAdmin(w http.ResponseWriter, r *http.Request) (jwt.MapClaims, bool) {
	claims, err := authpkg.Extract(r)
	if err != nil {
		WriteError(w, http.StatusUnauthorized, "Authentication required")
		return nil, false
	}
	scopes, _ := claims["scopes"].([]interface{})
	for _, s := range scopes {
		if str, ok := s.(string); ok && str == "SYS_ADMIN" {
			return claims, true
		}
	}
	WriteError(w, http.StatusForbidden, "Forbidden: SYS_ADMIN scope required")
	return nil, false
}
