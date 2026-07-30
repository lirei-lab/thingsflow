package main

import (
	"net/http"
	"net/http/httptest"
	"testing"

	authpkg "flow-core/internal/auth"
)

// gateTestToken installs a deterministic HS512 key on the shared auth
// package and returns a freshly signed platform access token. Mirrors the
// key-setup pattern in internal/auth/jwt_test.go so the gate validates the
// token through the exact same ParseAndValidate path production uses.
func gateTestToken(t *testing.T) string {
	t.Helper()
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i + 1)
	}
	authpkg.SigningKey = key
	authpkg.Config.TokenIssuer = "flow.iot"
	authpkg.Config.TokenExpirationTime = 9000
	authpkg.Config.RefreshTokenExpTime = 604800
	t.Cleanup(func() { authpkg.SigningKey = nil })

	tok, err := authpkg.GenerateAccess(authpkg.Subject{
		UserID:    "00000000-0000-0000-0000-000000000001",
		Email:     "alice@example.com",
		Authority: "TENANT_ADMIN",
		TenantID:  "11111111-1111-1111-1111-111111111111",
		Enabled:   true,
	}, "session-1")
	if err != nil {
		t.Fatalf("GenerateAccess: %v", err)
	}
	return tok
}

func TestAuthGate_DenyByDefault(t *testing.T) {
	token := gateTestToken(t)

	cases := []struct {
		name     string
		method   string
		path     string
		token    bool
		want401  bool
		wantPass bool // handler behind the gate must be reached
	}{
		// The confirmed exploit: anonymous telemetry-key read → now 401.
		{"telemetry keys no token", "GET", "/api/plugins/telemetry/DEVICE/x/keys/timeseries", false, true, false},
		// Same route WITH a valid platform token → passes to handler.
		{"telemetry keys with token", "GET", "/api/plugins/telemetry/DEVICE/x/keys/timeseries", true, false, true},
		// Ordinary protected route without a token → 401.
		{"tenant devices no token", "GET", "/api/tenant/devices", false, true, false},
		{"tenant devices with token", "GET", "/api/tenant/devices", true, false, true},

		// Allowlist: auth entry points.
		{"login", "POST", "/api/auth/login", false, false, true},
		{"token refresh", "POST", "/api/auth/token", false, false, true},

		// Allowlist: intentionally public prefixes / exacts.
		{"noauth oauth2Clients", "GET", "/api/noauth/oauth2Clients", false, false, true},
		{"noauth activate", "POST", "/api/noauth/activate", false, false, true},
		{"well-known jwks", "GET", "/.well-known/thingsflow-device-jwks.json", false, false, true},
		{"oauth2 code callback", "GET", "/login/oauth2/code/generic", false, false, true},
		{"health", "GET", "/health", false, false, true},
		{"ready", "GET", "/ready", false, false, true},
		{"metrics", "GET", "/metrics", false, false, true},

		// Device transport — authenticated by its OWN device-JWT path.
		// The platform gate must let it through without validating a token.
		{"device telemetry no platform token", "POST", "/api/v1/telemetry", false, false, true},
		{"device provision", "POST", "/api/v1/provision", false, false, true},

		// WebSocket — NOT gated here (ws.go does its own authCmd check).
		{"ws upgrade", "GET", "/api/ws", false, false, true},
		{"ws telemetry upgrade", "GET", "/api/ws/plugins/telemetry", false, false, true},

		// OPTIONS preflight passes through on any path.
		{"options preflight", "OPTIONS", "/api/tenant/devices", false, false, true},

		// Path traversal must NOT slip past the allowlist: after path.Clean
		// this resolves to /api/tenant/devices, which is protected → 401.
		{"traversal escape from noauth", "GET", "/api/noauth/../tenant/devices", false, true, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			reached := false
			next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				reached = true
				w.WriteHeader(http.StatusOK)
			})
			gate := authGate(next, "*")

			r := httptest.NewRequest(tc.method, tc.path, nil)
			if tc.token {
				r.Header.Set("X-Authorization", "Bearer "+token)
			}
			rec := httptest.NewRecorder()
			gate.ServeHTTP(rec, r)

			if tc.want401 && rec.Code != http.StatusUnauthorized {
				t.Fatalf("%s %s: got status %d, want 401", tc.method, tc.path, rec.Code)
			}
			if !tc.want401 && rec.Code == http.StatusUnauthorized {
				t.Fatalf("%s %s: got 401, want pass-through", tc.method, tc.path)
			}
			if tc.wantPass && !reached {
				t.Fatalf("%s %s: handler not reached", tc.method, tc.path)
			}
			if tc.want401 && reached {
				t.Fatalf("%s %s: handler was reached despite 401", tc.method, tc.path)
			}
		})
	}
}

func TestIsPublicPath(t *testing.T) {
	public := []struct{ method, path string }{
		{"GET", "/health"},
		{"GET", "/ready"},
		{"GET", "/metrics"},
		{"POST", "/api/auth/login"},
		{"POST", "/api/auth/token"},
		{"GET", "/api/noauth/oauth2Clients"},
		{"GET", "/.well-known/thingsflow-device-public.pem"},
		{"GET", "/login/oauth2/code/keycloak"},
		{"POST", "/api/v1/telemetry"},
		{"GET", "/api/ws"},
		{"GET", "/api/ws/plugins/telemetry"},
		{"OPTIONS", "/api/tenant/devices"}, // preflight, any path
	}
	for _, p := range public {
		if !isPublicPath(p.method, p.path) {
			t.Errorf("isPublicPath(%s, %s) = false, want true", p.method, p.path)
		}
	}

	protected := []struct{ method, path string }{
		{"GET", "/api/tenant/devices"},
		{"GET", "/api/plugins/telemetry/DEVICE/x/keys/timeseries"},
		{"GET", "/api/auth/user"},
		{"POST", "/api/device"},
		{"GET", "/api/noauthx/thing"}, // no trailing-slash boundary confusion
		{"GET", "/api/v1x/telemetry"}, // prefix must respect the slash boundary
	}
	for _, p := range protected {
		if isPublicPath(p.method, p.path) {
			t.Errorf("isPublicPath(%s, %s) = true, want false", p.method, p.path)
		}
	}
}
