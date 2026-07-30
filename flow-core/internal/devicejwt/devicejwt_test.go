package devicejwt

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

func TestIssueCreatesEdgeCompatibleDeviceTokenAndJWKS(t *testing.T) {
	signer, err := NewSigner(Config{
		Enabled:  true,
		Issuer:   "thingsflow-device",
		Audience: "thingsflow-mqtt",
		KeyID:    "test-key-1",
		TTL:      time.Hour,
	})
	if err != nil {
		t.Fatalf("NewSigner: %v", err)
	}

	issued, err := signer.Issue("device-123", "tenant-abc", "Boiler Pump")
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}
	if issued.Token == "" {
		t.Fatal("expected signed token")
	}
	if issued.TokenType != "Bearer" {
		t.Fatalf("token type = %q, want Bearer", issued.TokenType)
	}
	if issued.Subject != "device-123" {
		t.Fatalf("subject = %q, want device id", issued.Subject)
	}
	if issued.MQTTIdentity != "device123" {
		t.Fatalf("mqtt identity = %q, want topic-safe identity", issued.MQTTIdentity)
	}
	if issued.MQTTUsername != "Bearer "+issued.Token {
		t.Fatalf("mqtt username does not embed bearer token")
	}

	keyfunc := func(token *jwt.Token) (interface{}, error) {
		if token.Method != jwt.SigningMethodES256 {
			t.Fatalf("signing method = %v, want ES256", token.Method.Alg())
		}
		return signer.PublicKey(), nil
	}
	token, err := jwt.Parse(issued.Token, keyfunc, jwt.WithIssuer("thingsflow-device"), jwt.WithAudience("thingsflow-mqtt"))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	claims := token.Claims.(jwt.MapClaims)
	if got := claims["sub"]; got != "device-123" {
		t.Fatalf("sub = %v, want device id", got)
	}
	if got := claims["tenantId"]; got != "tenant-abc" {
		t.Fatalf("tenantId = %v, want tenant id", got)
	}
	if got := claims["mqttId"]; got != "device123" {
		t.Fatalf("mqttId = %v, want topic-safe device id", got)
	}
	roles, ok := claims["roles"].([]interface{})
	if !ok || len(roles) != 1 || roles[0] != "mqtt:stream" {
		t.Fatalf("roles = %#v, want [mqtt:stream]", claims["roles"])
	}

	req := httptest.NewRequest(http.MethodGet, "/api/noauth/device-jwks", nil)
	rec := httptest.NewRecorder()
	signer.HandleJWKS(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("JWKS status = %d, want 200", rec.Code)
	}
	var jwks struct {
		Keys []struct {
			Kty string `json:"kty"`
			Crv string `json:"crv"`
			Kid string `json:"kid"`
			Use string `json:"use"`
			Alg string `json:"alg"`
			X   string `json:"x"`
			Y   string `json:"y"`
		} `json:"keys"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &jwks); err != nil {
		t.Fatalf("decode JWKS: %v", err)
	}
	if len(jwks.Keys) != 1 {
		t.Fatalf("JWKS keys = %d, want 1", len(jwks.Keys))
	}
	key := jwks.Keys[0]
	if key.Kty != "EC" || key.Crv != "P-256" || key.Kid != "test-key-1" || key.Use != "sig" || key.Alg != "ES256" {
		t.Fatalf("unexpected JWK metadata: %+v", key)
	}
	if len(mustBase64URLDecode(t, key.X)) != 32 || len(mustBase64URLDecode(t, key.Y)) != 32 {
		t.Fatalf("expected 32-byte P-256 coordinates")
	}

	req = httptest.NewRequest(http.MethodGet, "/api/noauth/device-jwt-public.pem", nil)
	rec = httptest.NewRecorder()
	signer.HandlePublicPEM(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("public PEM status = %d, want 200", rec.Code)
	}
	if got := rec.Header().Get("Content-Type"); got != "application/x-pem-file" {
		t.Fatalf("public PEM content type = %q, want application/x-pem-file", got)
	}
	if !strings.HasPrefix(rec.Body.String(), "-----BEGIN PUBLIC KEY-----") {
		t.Fatalf("public PEM body does not start with public key header: %q", rec.Body.String())
	}
	block, _ := pem.Decode(rec.Body.Bytes())
	if block == nil {
		t.Fatalf("decode public PEM failed")
	}
	parsed, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		t.Fatalf("parse public PEM: %v", err)
	}
	publicKey, ok := parsed.(*ecdsa.PublicKey)
	if !ok {
		t.Fatalf("public PEM type = %T, want ECDSA public key", parsed)
	}
	if _, err := jwt.Parse(issued.Token, func(token *jwt.Token) (interface{}, error) {
		return publicKey, nil
	}, jwt.WithIssuer("thingsflow-device"), jwt.WithAudience("thingsflow-mqtt")); err != nil {
		t.Fatalf("parse token with public PEM: %v", err)
	}
}

func TestJWKSIncludesAdditionalPublicKeysForRotation(t *testing.T) {
	previous, err := NewSigner(Config{
		Enabled:  true,
		Issuer:   "thingsflow-device",
		Audience: "thingsflow-mqtt",
		KeyID:    "previous-key",
		TTL:      time.Hour,
	})
	if err != nil {
		t.Fatalf("previous signer: %v", err)
	}
	req := httptest.NewRequest(http.MethodGet, "/api/noauth/device-jwks", nil)
	rec := httptest.NewRecorder()
	previous.HandleJWKS(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("previous JWKS status = %d, want 200", rec.Code)
	}

	current, err := NewSigner(Config{
		Enabled:              true,
		Issuer:               "thingsflow-device",
		Audience:             "thingsflow-mqtt",
		KeyID:                "current-key",
		TTL:                  time.Hour,
		AdditionalPublicJWKS: rec.Body.String(),
	})
	if err != nil {
		t.Fatalf("current signer: %v", err)
	}

	rec = httptest.NewRecorder()
	current.HandleJWKS(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("current JWKS status = %d, want 200", rec.Code)
	}
	var jwks struct {
		Keys []struct {
			Kid string `json:"kid"`
		} `json:"keys"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &jwks); err != nil {
		t.Fatalf("decode JWKS: %v", err)
	}
	if len(jwks.Keys) != 2 {
		t.Fatalf("JWKS keys = %d, want current + previous", len(jwks.Keys))
	}
	if jwks.Keys[0].Kid != "current-key" || jwks.Keys[1].Kid != "previous-key" {
		t.Fatalf("unexpected key order: %+v", jwks.Keys)
	}
}

func mustBase64URLDecode(t *testing.T, raw string) []byte {
	t.Helper()
	decoded, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		t.Fatalf("base64url decode %q: %v", raw, err)
	}
	return decoded
}

// An unpinned device JWT key is regenerated on every boot. Existing MQTT sessions keep
// working because the broker does not re-verify them, so the only visible
// symptom is that new devices cannot connect — which is why production must
// refuse to start rather than degrade quietly.
func TestNewSigner_ProductionRequiresPinnedKey(t *testing.T) {
	t.Setenv("FLOW_ENV", "production")
	_, err := NewSigner(Config{Enabled: true, Issuer: "i", Audience: "a", KeyID: "k"})
	if err == nil {
		t.Fatal("production accepted an ephemeral device JWT key")
	}
}

func TestNewSigner_DevGeneratesEphemeralKey(t *testing.T) {
	t.Setenv("FLOW_ENV", "")
	s, err := NewSigner(Config{Enabled: true, Issuer: "i", Audience: "a", KeyID: "k"})
	if err != nil {
		t.Fatalf("dev should still boot with a generated key: %v", err)
	}
	if s == nil {
		t.Fatal("no signer returned")
	}
}

// signerWithKey builds a signer around a specific key/kid so a rotation can be
// simulated: mint with the old signer, verify with the new one.
func signerWithKey(t *testing.T, kid string, key *ecdsa.PrivateKey, additional string) *Signer {
	t.Helper()
	pemBytes, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	block := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: pemBytes})
	s, err := NewSigner(Config{
		Enabled: true, Issuer: defaultIssuer, Audience: defaultAudience,
		KeyID: kid, TTL: time.Hour,
		PrivateKeyPEM: string(block), AdditionalPublicJWKS: additional,
	})
	if err != nil {
		t.Fatalf("NewSigner: %v", err)
	}
	return s
}

func jwksFor(t *testing.T, kid string, key *ecdsa.PrivateKey) string {
	t.Helper()
	j := publicJWK{
		Kty: "EC", Crv: "P-256", Kid: kid, Use: "sig", Alg: "ES256",
		X: base64.RawURLEncoding.EncodeToString(padP256(key.PublicKey.X.Bytes())),
		Y: base64.RawURLEncoding.EncodeToString(padP256(key.PublicKey.Y.Bytes())),
	}
	b, err := json.Marshal(map[string]interface{}{"keys": []publicJWK{j}})
	if err != nil {
		t.Fatalf("marshal jwks: %v", err)
	}
	return string(b)
}

func newKey(t *testing.T) *ecdsa.PrivateKey {
	t.Helper()
	k, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("genkey: %v", err)
	}
	return k
}

// The point of the additional key set: after the signing key rotates, a device
// still holding a token signed by the retired key must keep verifying, or it can
// neither connect nor refresh — refresh authenticates with the very token being
// rejected — and only an out-of-band re-issue recovers it.
func TestValidate_AcceptsTokenFromRotatedOutKey(t *testing.T) {
	oldKey, newKey2 := newKey(t), newKey(t)
	oldSigner := signerWithKey(t, "kid-old", oldKey, "")
	tok, err := oldSigner.Issue("dev-1", "tenant-1", "Device 1")
	if err != nil {
		t.Fatalf("issue: %v", err)
	}

	// Post-rotation signer: signs with the new key, still trusts the old one.
	rotated := signerWithKey(t, "kid-new", newKey2, jwksFor(t, "kid-old", oldKey))
	claims, err := rotated.Validate(tok.Token)
	if err != nil {
		t.Fatalf("token from the retired key was rejected: %v", err)
	}
	if claims["deviceId"] != "dev-1" {
		t.Fatalf("claims = %v", claims)
	}
}

// Once the old key is dropped from the set, its tokens must stop working —
// otherwise "rotation" never actually retires anything.
func TestValidate_RejectsRetiredKeyOnceRemoved(t *testing.T) {
	oldKey, newKey2 := newKey(t), newKey(t)
	tok, err := signerWithKey(t, "kid-old", oldKey, "").Issue("dev-1", "tenant-1", "d")
	if err != nil {
		t.Fatalf("issue: %v", err)
	}
	if _, err := signerWithKey(t, "kid-new", newKey2, "").Validate(tok.Token); err == nil {
		t.Fatal("a token signed by a key no longer trusted was accepted")
	}
}

// An unknown kid must not fall back to the active key: that would make a stale
// or attacker-chosen kid indistinguishable from a legitimate token.
func TestValidate_RejectsUnknownKeyID(t *testing.T) {
	strangerKey := newKey(t)
	tok, err := signerWithKey(t, "kid-stranger", strangerKey, "").Issue("dev-1", "t", "d")
	if err != nil {
		t.Fatalf("issue: %v", err)
	}
	live := newKey(t)
	// The stranger's public key is trusted, but under a different kid.
	s := signerWithKey(t, "kid-live", live, jwksFor(t, "kid-other", strangerKey))
	if _, err := s.Validate(tok.Token); err == nil {
		t.Fatal("token naming an unknown kid was accepted")
	}
}

// A JWK whose coordinates are not a point on P-256 must be refused rather than
// reaching ecdsa.Verify.
func TestPublicJWK_ToECDSA_RejectsOffCurvePoint(t *testing.T) {
	j := publicJWK{
		X: base64.RawURLEncoding.EncodeToString(make([]byte, 32)),
		Y: base64.RawURLEncoding.EncodeToString(make([]byte, 32)),
	}
	if _, err := j.toECDSA(); err == nil {
		t.Fatal("an off-curve point was accepted as a public key")
	}
}
