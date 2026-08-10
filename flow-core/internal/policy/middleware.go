package policy

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"strings"

	dbpkg "flow-core/internal/db"
	"flow-core/internal/httputil"
)

// Resolver resolves the policy context for a twin request: the entity's owning
// tenant and its registry policyId. api.go wires it to twin.PolicyContext.
type Resolver func(ctx context.Context, entityType, entityID string) (tenantID, policyID string, err error)

// EnforcementEnabled reports whether the twin API policy enforcement gate is
// active. Defaults to true; set POLICY_ENFORCEMENT_ENABLED=false to disable
// enforcement for debugging without removing the code.
func EnforcementEnabled() bool {
	return strings.TrimSpace(os.Getenv("POLICY_ENFORCEMENT_ENABLED")) != "false"
}

// EnforceRead wraps a twin read route (GET /api/twins/{entityType}/{entityId})
// with policy enforcement for the READ action on the entity's thing:/... path.
// The existing tenant isolation inside the handler stays — the policy layer is
// additive on top of it.
func EnforceRead(resolve Resolver, next func(w http.ResponseWriter, r *http.Request, entityType, entityID string)) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		entityType := r.PathValue("entityType")
		entityID := r.PathValue("entityId")
		if enforceEntityRequest(w, r, resolve, entityType, entityID, "READ", func(_ *http.Request, tenantID string) ([]string, error) {
			return []string{ResourcePath(tenantID, entityType, entityID)}, nil
		}) {
			next(w, r, entityType, entityID)
		}
	}
}

// EnforceWrite wraps a twin write route (PUT/PATCH .../attributes|features)
// with policy enforcement for the WRITE action. The body is parsed once to
// derive feature/attribute-granular resource paths, then restored so the
// handler reads it normally.
//
// Only the key the target route persists is enforced (an /attributes route
// derives only attribute paths, /features only feature paths): a well-formed
// or malformed extraneous key for the other route is ignored, exactly as the
// handler's single-key struct ignores it — so enforcement never rejects a body
// the handler accepts. A body that is not valid JSON, or whose own route key is
// present but malformed, fails closed (400); whatever the handler persists is
// always covered by an authorized resource path.
func EnforceWrite(resolve Resolver, next func(w http.ResponseWriter, r *http.Request, entityType, entityID string)) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		entityType := r.PathValue("entityType")
		entityID := r.PathValue("entityId")
		paths := func(r *http.Request, tenantID string) ([]string, error) {
			body, err := io.ReadAll(r.Body)
			if err != nil {
				return nil, err
			}
			// Restore the body so the handler parses it as usual.
			r.Body = io.NopCloser(bytes.NewReader(body))
			var payload map[string]json.RawMessage
			if err := json.Unmarshal(body, &payload); err != nil {
				return nil, err
			}
			routeKey := "features"
			if strings.HasSuffix(r.URL.Path, "/attributes") {
				routeKey = "attributes"
			}
			raw, ok := payload[routeKey]
			if !ok {
				// No keys for this route: authorize the base path; the
				// handler's own validation still applies.
				return []string{ResourcePath(tenantID, entityType, entityID)}, nil
			}
			var names map[string]json.RawMessage
			if err := json.Unmarshal(raw, &names); err != nil {
				// The route's own key is present but not an object: the handler
				// would 400 it too — fail closed here.
				return nil, err
			}
			var paths []string
			for name := range names {
				paths = append(paths, ResourcePath(tenantID, entityType, entityID, routeKey, name))
			}
			if len(paths) == 0 {
				paths = append(paths, ResourcePath(tenantID, entityType, entityID))
			}
			return paths, nil
		}
		if enforceEntityRequest(w, r, resolve, entityType, entityID, "WRITE", paths) {
			next(w, r, entityType, entityID)
		}
	}
}

// EnforceModelWrite wraps PUT /api/twins/{entityType}/{entityId}/model with
// policy enforcement for the WRITE action on the .../model resource path.
func EnforceModelWrite(resolve Resolver, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		entityType := r.PathValue("entityType")
		entityID := r.PathValue("entityId")
		if enforceEntityRequest(w, r, resolve, entityType, entityID, "WRITE", func(_ *http.Request, tenantID string) ([]string, error) {
			return []string{ResourcePath(tenantID, entityType, entityID, "model")}, nil
		}) {
			next(w, r)
		}
	}
}

// EnforceList wraps GET /api/twins with policy enforcement at the caller's
// tenant root (thing:/<tenant>) against the tenant's default policy. The list
// has no single entity, so the owning-tenant default is the document checked.
func EnforceList(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		claims, ok := httputil.RequireAuth(w, r)
		if !ok {
			return
		}
		if !EnforcementEnabled() || SubjectForClaims(claims) == "SYS_ADMIN" {
			next(w, r)
			return
		}
		tenantID, _ := claims["tenantId"].(string)
		if tenantID == "" {
			next(w, r) // the handler applies its own 401 on an empty tenant claim
			return
		}
		if dbpkg.Pool == nil {
			httputil.WriteError(w, http.StatusServiceUnavailable, "Policy database unavailable")
			return
		}
		doc, err := NewStore(dbpkg.Pool).Resolve(r.Context(), tenantID, "tenant:"+tenantID+":"+DefaultPolicyID)
		if err != nil {
			httputil.WriteError(w, http.StatusForbidden, "Policy not resolvable")
			return
		}
		if !Authorize(SubjectForClaims(claims), doc, "thing:/"+tenantID, "READ") {
			httputil.WriteError(w, http.StatusForbidden, "Policy denies access")
			return
		}
		next(w, r)
	}
}

// enforceEntityRequest is the shared enforcement core for entity-scoped twin
// routes. It returns true when the request may proceed (enforcement disabled,
// SYS_ADMIN, or every derived resource path authorized); on denial it writes
// the 403 envelope and returns false.
//
// A SYS_ADMIN short-circuits before any policy lookup: the existing tenant
// checks already let SYS_ADMIN cross tenants, so the policy layer must not
// newly block them (additive posture). A missing entity (ErrNoRows) passes
// through so the handler owns the 404; any other resolver error fails closed
// (500) — a transient lookup failure must not silently disable enforcement for
// an existing protected twin.
func enforceEntityRequest(w http.ResponseWriter, r *http.Request, resolve Resolver, entityType, entityID, action string, paths func(r *http.Request, tenantID string) ([]string, error)) bool {
	claims, ok := httputil.RequireAuth(w, r)
	if !ok {
		return false
	}
	if !EnforcementEnabled() || SubjectForClaims(claims) == "SYS_ADMIN" {
		return true
	}
	if dbpkg.Pool == nil {
		httputil.WriteError(w, http.StatusServiceUnavailable, "Policy database unavailable")
		return false
	}
	tenantID, policyID, err := resolve(r.Context(), entityType, entityID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			// Missing entity: the handler owns the 404 — no protected resource
			// exists to fail closed on.
			return true
		}
		httputil.WriteError(w, http.StatusInternalServerError, "Policy resolution failed")
		return false
	}
	doc, err := NewStore(dbpkg.Pool).Resolve(r.Context(), tenantID, policyID)
	if err != nil {
		httputil.WriteError(w, http.StatusForbidden, "Policy not resolvable")
		return false
	}
	subject := SubjectForClaims(claims)
	resourcePaths, err := paths(r, tenantID)
	if err != nil {
		// A body that cannot be parsed fails closed: the write is rejected
		// (the handler would 400 it too) rather than passing through unenforced.
		httputil.WriteError(w, http.StatusBadRequest, "Invalid twin write body")
		return false
	}
	for _, path := range resourcePaths {
		if !Authorize(subject, doc, path, action) {
			httputil.WriteError(w, http.StatusForbidden, "Policy denies access")
			return false
		}
	}
	return true
}
