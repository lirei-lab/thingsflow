package main

import (
	"net/http"
	"path"
	"strings"

	authpkg "flow-core/internal/auth"
	"flow-core/internal/httputil"
)

// Deny-by-default authentication gate.
//
// Before this gate existed, `startHTTPServer` added only request-id, body-cap
// and access-logging, and each of ~295 routes decided its own auth — several
// did not, so e.g. GET /api/plugins/telemetry/DEVICE/{id}/keys/timeseries
// returned data with no token at all. Fixing that per-handler is whack-a-mole.
//
// authGate wraps the whole mux: every request must carry a valid PLATFORM JWT
// (validated through the same authpkg.Extract / ParseAndValidate path
// httputil uses) EXCEPT for an explicit allowlist. On a missing/invalid token
// for a non-allowlisted path it writes a 401 via the canonical TB error
// envelope and never calls the handler.
//
// The allowlist is DATA, never a regex: matching is done on
// path.Clean(r.URL.Path) so a request-encoded `..` / `%2e%2e` traversal
// (which Go decodes into r.URL.Path before we see it) collapses to its real
// target and is judged against the allowlist as that target — you cannot
// dress a protected path up as a public one.

// authGateExactPaths are matched in full against the cleaned request path.
var authGateExactPaths = map[string]struct{}{
	"/health":  {},
	"/ready":   {},
	"/metrics": {},
	// Auth entry points. /api/auth/login mints the first token; /api/auth/token
	// (refresh) reads its refresh token from the JSON body and validates it
	// itself (see internal/user.HandleTokenRefresh) — the header-based gate
	// has nothing to validate for it, so it must be public.
	"/api/auth/login": {},
	"/api/auth/token": {},
	// WebSocket: the TB UI sends its token in the FIRST WS message (authCmd),
	// not a header, so a header-only gate would break the legitimate UI WS.
	// The upgrade is instead gated inside internal/ws (deny-by-default on the
	// authCmd). Let the upgrade reach that handler.
	"/api/ws": {},
}

// authGatePrefixes are matched as path prefixes (slash-boundary aware) against
// the cleaned request path.
var authGatePrefixes = []string{
	"/api/noauth/",        // provisioning, activate, reset, oauth2Clients, device-jwks…
	"/.well-known/",       // device JWKS / public PEM discovery
	"/login/oauth2/code/", // OAuth2 authorization-code callback
	"/api/v1/",            // device transport — authenticated by its OWN device-JWT path
	"/api/ws/",            // /api/ws/plugins/telemetry — gated inside internal/ws
}

// isPublicPath reports whether (method, cleanPath) is exempt from the platform
// JWT requirement. cleanPath MUST already be path.Clean'd by the caller.
func isPublicPath(method, cleanPath string) bool {
	// CORS preflight carries no credentials and the handlers already answer
	// OPTIONS with 200 — let it through on any path.
	if method == http.MethodOptions {
		return true
	}
	if _, ok := authGateExactPaths[cleanPath]; ok {
		return true
	}
	for _, p := range authGatePrefixes {
		// HasPrefix("/api/v1/…") matches subpaths; the TrimSuffix arm lets the
		// bare prefix (e.g. exactly "/api/noauth") through too, while the slash
		// keeps "/api/v1x" from masquerading as "/api/v1/".
		if cleanPath == strings.TrimSuffix(p, "/") || strings.HasPrefix(cleanPath, p) {
			return true
		}
	}
	return false
}

// authGate wraps next with the deny-by-default platform-JWT check described
// above. allowedOrigin is used only to attach CORS headers to the 401 so the
// browser UI can actually read the rejection instead of seeing an opaque CORS
// error.
func authGate(next http.Handler, allowedOrigin string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// The gate and the mux MUST agree on what path a request addresses.
		// They do not agree on traversal: net/http decodes %2e%2e into ".."
		// inside r.URL.Path (so path.Clean collapses it and the gate sees a
		// public path), while ServeMux routes the *encoded* form verbatim to
		// the protected pattern instead of redirecting it. That divergence is
		// a gate bypass, so any path carrying a traversal segment — encoded or
		// literal — is refused outright rather than normalised.
		clean := path.Clean(r.URL.Path)
		if clean != r.URL.Path && hasTraversal(r.URL.Path) {
			setCORSHeaders(w, allowedOrigin)
			httputil.WriteError(w, http.StatusBadRequest, "Malformed path")
			return
		}
		if isPublicPath(r.Method, clean) {
			next.ServeHTTP(w, r)
			return
		}
		if _, err := authpkg.Extract(r); err != nil {
			setCORSHeaders(w, allowedOrigin)
			httputil.WriteError(w, http.StatusUnauthorized, "Authentication required")
			return
		}
		next.ServeHTTP(w, r)
	})
}

// hasTraversal reports whether the path contains a ".." segment in either the
// decoded path or the raw (still percent-encoded) form. Both are checked
// because net/http decodes %2e%2e in r.URL.Path while ServeMux routes on the
// escaped form — a request can look clean in one view and traverse in the
// other.
func hasTraversal(decoded string) bool {
	for _, seg := range strings.Split(decoded, "/") {
		if seg == ".." {
			return true
		}
	}
	return false
}
