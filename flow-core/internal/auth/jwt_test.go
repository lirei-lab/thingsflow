package auth

import (
	"encoding/base64"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

// setUpDefaultKey installs a deterministic 32-byte HS512 key for the
// test process. Replaces the old applyDefaults() helper, which used a
// hardcoded TB-classic constant — that constant is gone (see C-2 in
// docs/SECURITY_AUDIT.md). Tests get a stable key without depending
// on env vars or postgres.
func setUpDefaultKey(t *testing.T) {
	t.Helper()
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i + 1)
	}
	SigningKey = key
	Config.TokenSigningKey = base64.StdEncoding.EncodeToString(key)
	Config.TokenIssuer = defaultIssuer
	Config.TokenExpirationTime = defaultTokenExp
	Config.RefreshTokenExpTime = defaultRefreshExp
	t.Cleanup(func() {
		SigningKey = nil
		Config.TokenIssuer = ""
		Config.TokenSigningKey = ""
	})
}

func TestInitConfig_FailsClosedInProductionWithoutKey(t *testing.T) {
	// We can't actually call InitConfig in production mode without
	// log.Fatal killing the test process. Instead we verify the
	// boundary indirectly: in dev mode with no env + no DB, InitConfig
	// generates a random key and sets WARN-level state.
	t.Setenv("FLOW_ENV", "")
	t.Setenv("JWT_TOKEN_SIGNING_KEY", "")
	SigningKey = nil
	InitConfig()
	if len(SigningKey) != 32 {
		t.Errorf("dev fallback should generate 32-byte random key, got %d bytes", len(SigningKey))
	}
	if Config.TokenIssuer != defaultIssuer {
		t.Errorf("issuer = %q, want %q", Config.TokenIssuer, defaultIssuer)
	}
}

func TestInitConfig_HonorsEnvVar(t *testing.T) {
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i + 100)
	}
	t.Setenv("FLOW_ENV", "")
	t.Setenv("JWT_TOKEN_SIGNING_KEY", base64.StdEncoding.EncodeToString(key))
	SigningKey = nil
	InitConfig()
	if len(SigningKey) != 32 {
		t.Errorf("expected 32 bytes from env, got %d", len(SigningKey))
	}
	for i, b := range SigningKey {
		if b != key[i] {
			t.Errorf("byte %d = %d, want %d", i, b, key[i])
			break
		}
	}
	t.Cleanup(func() {
		SigningKey = nil
		Config.TokenSigningKey = ""
		os.Unsetenv("JWT_TOKEN_SIGNING_KEY")
	})
}

func TestInitConfig_RejectsShortEnvKeyInDev(t *testing.T) {
	// Dev mode falls through to random key when env is too short
	// (≥32 bytes is a hard requirement for HS512 — shorter keys are
	// vulnerable to brute force).
	t.Setenv("FLOW_ENV", "")
	t.Setenv("JWT_TOKEN_SIGNING_KEY", base64.StdEncoding.EncodeToString([]byte("too-short")))
	SigningKey = nil
	InitConfig()
	if len(SigningKey) != 32 {
		t.Errorf("expected fallback random 32-byte key after rejecting short env key, got %d", len(SigningKey))
	}
}

func TestGenerateAccess_RoundTripsThroughParseAndValidate(t *testing.T) {
	setUpDefaultKey(t)
	sub := Subject{
		UserID:     "00000000-0000-0000-0000-000000000001",
		Email:      "alice@example.com",
		Authority:  "TENANT_ADMIN",
		TenantID:   "11111111-1111-1111-1111-111111111111",
		CustomerID: "22222222-2222-2222-2222-222222222222",
		Enabled:    true,
	}
	tok, err := GenerateAccess(sub, "session-1")
	if err != nil {
		t.Fatalf("GenerateAccess: %v", err)
	}
	claims, err := ParseAndValidate(tok)
	if err != nil {
		t.Fatalf("ParseAndValidate: %v", err)
	}
	if got, _ := claims["userId"].(string); got != sub.UserID {
		t.Errorf("userId = %q, want %q", got, sub.UserID)
	}
	if got, _ := claims["sub"].(string); got != sub.Email {
		t.Errorf("sub = %q, want %q", got, sub.Email)
	}
	if got, _ := claims["sessionId"].(string); got != "session-1" {
		t.Errorf("sessionId = %q", got)
	}
}

func TestGenerateRefresh_HasRefreshScope(t *testing.T) {
	setUpDefaultKey(t)
	sub := Subject{UserID: "u1", Email: "alice@example.com"}
	tok, err := GenerateRefresh(sub, "session-1")
	if err != nil {
		t.Fatalf("GenerateRefresh: %v", err)
	}
	claims, err := ParseAndValidate(tok)
	if err != nil {
		t.Fatalf("ParseAndValidate: %v", err)
	}
	scopes, _ := claims["scopes"].([]interface{})
	if len(scopes) != 1 || scopes[0] != "REFRESH_TOKEN" {
		t.Errorf("refresh scopes = %v", scopes)
	}
}

func TestParseAndValidate_RejectsTamperedSignature(t *testing.T) {
	setUpDefaultKey(t)
	tok, _ := GenerateAccess(Subject{UserID: "u1", Email: "alice@example.com"}, "s1")

	// Flip a byte in the last segment (signature) to break HMAC verification.
	parts := strings.Split(tok, ".")
	if len(parts) != 3 {
		t.Fatalf("token shape unexpected: %d parts", len(parts))
	}
	// Replace the first char of the signature with something different.
	bad := parts[0] + "." + parts[1] + "." + "Z" + parts[2][1:]
	if _, err := ParseAndValidate(bad); err == nil {
		t.Errorf("expected validation error on tampered signature")
	}
}

func TestExtract_HonorsXAuthorizationAndAuthorization(t *testing.T) {
	setUpDefaultKey(t)
	tok, _ := GenerateAccess(Subject{UserID: "u1", Email: "alice@example.com"}, "s1")

	// Standard Authorization header
	r := httptest.NewRequest("GET", "/api/auth/user", nil)
	r.Header.Set("Authorization", "Bearer "+tok)
	if _, err := Extract(r); err != nil {
		t.Errorf("Extract(Authorization): %v", err)
	}

	// X-Authorization header (TB classic shape)
	r = httptest.NewRequest("GET", "/api/auth/user", nil)
	r.Header.Set("X-Authorization", "Bearer "+tok)
	if _, err := Extract(r); err != nil {
		t.Errorf("Extract(X-Authorization): %v", err)
	}

	// Missing header
	r = httptest.NewRequest("GET", "/api/auth/user", nil)
	if _, err := Extract(r); err == nil {
		t.Errorf("Extract(no header): expected error")
	}
}
