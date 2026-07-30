package oidc

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

func TestProvidersFromEnvSingleProvider(t *testing.T) {
	t.Setenv("OIDC_ENABLED", "true")
	t.Setenv("OIDC_PROVIDER_ID", "external")
	t.Setenv("OIDC_PROVIDER_TITLE", "External OIDC")
	t.Setenv("OIDC_CLIENT_ID", "thingsflow")
	t.Setenv("OIDC_CLIENT_SECRET", "secret")
	t.Setenv("OIDC_ISSUER", "https://idp.example/realms/demo")
	t.Setenv("OIDC_AUTHORIZATION_URL", "https://idp.example/auth")
	t.Setenv("OIDC_TOKEN_URL", "https://idp.example/token")
	t.Setenv("OIDC_JWKS_URL", "https://idp.example/certs")
	t.Setenv("OIDC_DEFAULT_TENANT_ID", "aaaaaaaa-1dd2-11b2-8080-808080808080")
	t.Setenv("OIDC_ALLOW_USER_CREATION", "true")

	providers, err := ProvidersFromEnv()
	if err != nil {
		t.Fatalf("ProvidersFromEnv: %v", err)
	}
	if len(providers) != 1 {
		t.Fatalf("providers len = %d, want 1", len(providers))
	}
	p := providers[0]
	if p.ID != "external" || p.Title != "External OIDC" || !p.AllowUserCreation {
		t.Fatalf("unexpected provider: %+v", p)
	}
}

func TestProvidersFromEnvDiscoversEndpointsFromIssuer(t *testing.T) {
	var serverURL string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/.well-known/openid-configuration" {
			http.NotFound(w, r)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]string{
			"issuer":                 serverURL,
			"authorization_endpoint": serverURL + "/oauth/v2/authorize",
			"token_endpoint":         serverURL + "/oauth/v2/token",
			"jwks_uri":               serverURL + "/oauth/v2/keys",
			"userinfo_endpoint":      serverURL + "/oidc/v1/userinfo",
		})
	}))
	defer server.Close()
	serverURL = server.URL

	t.Setenv("OIDC_ENABLED", "true")
	t.Setenv("OIDC_PROVIDER_ID", "zitadel")
	t.Setenv("OIDC_PROVIDER_TITLE", "ZITADEL")
	t.Setenv("OIDC_CLIENT_ID", "thingsflow-ui")
	t.Setenv("OIDC_CLIENT_SECRET", "secret")
	t.Setenv("OIDC_ISSUER", server.URL)

	providers, err := ProvidersFromEnv()
	if err != nil {
		t.Fatalf("ProvidersFromEnv: %v", err)
	}
	if len(providers) != 1 {
		t.Fatalf("providers len = %d, want 1", len(providers))
	}
	p := providers[0]
	if p.ID != "zitadel" {
		t.Fatalf("provider ID = %q, want zitadel", p.ID)
	}
	if p.AuthorizationURL != server.URL+"/oauth/v2/authorize" {
		t.Fatalf("AuthorizationURL = %q", p.AuthorizationURL)
	}
	if p.TokenURL != server.URL+"/oauth/v2/token" {
		t.Fatalf("TokenURL = %q", p.TokenURL)
	}
	if p.JWKSURL != server.URL+"/oauth/v2/keys" {
		t.Fatalf("JWKSURL = %q", p.JWKSURL)
	}
	if p.UserInfoURL != server.URL+"/oidc/v1/userinfo" {
		t.Fatalf("UserInfoURL = %q", p.UserInfoURL)
	}
}

func TestSignedStateRoundTripRejectsTamper(t *testing.T) {
	key := []byte("01234567890123456789012345678901")
	codec := StateCodec{Key: key}
	state, err := codec.Sign(State{ProviderID: "external", ReturnURL: "https://app.example/", Nonce: "n1", IssuedAt: time.Now().Unix()})
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}
	parsed, err := codec.Verify(state, "external", time.Hour)
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if parsed.ProviderID != "external" || parsed.ReturnURL != "https://app.example/" {
		t.Fatalf("unexpected parsed state: %+v", parsed)
	}
	if _, err := codec.Verify(state+"tamper", "external", time.Hour); err == nil {
		t.Fatal("tampered state verified")
	}
}

func TestValidateIDTokenWithJWKS(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	kid := "test-key"
	issuer := "https://issuer.example"
	audience := "thingsflow"
	jwks := map[string]interface{}{
		"keys": []map[string]interface{}{{
			"kty": "RSA",
			"use": "sig",
			"kid": kid,
			"alg": "RS256",
			"n":   base64.RawURLEncoding.EncodeToString(key.PublicKey.N.Bytes()),
			"e":   base64.RawURLEncoding.EncodeToString(big.NewInt(int64(key.PublicKey.E)).Bytes()),
		}},
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(jwks)
	}))
	defer server.Close()

	token := jwt.NewWithClaims(jwt.SigningMethodRS256, jwt.MapClaims{
		"iss":         issuer,
		"aud":         audience,
		"sub":         "subject-1",
		"email":       "oidc-user@example.com",
		"given_name":  "Oidc",
		"family_name": "User",
		"exp":         time.Now().Add(time.Hour).Unix(),
		"iat":         time.Now().Unix(),
	})
	token.Header["kid"] = kid
	signed, err := token.SignedString(key)
	if err != nil {
		t.Fatalf("SignedString: %v", err)
	}

	claims, err := ValidateIDToken(Provider{Issuer: issuer, ClientID: audience, JWKSURL: server.URL}, signed)
	if err != nil {
		t.Fatalf("ValidateIDToken: %v", err)
	}
	if claims.Subject != "subject-1" || claims.Email != "oidc-user@example.com" {
		t.Fatalf("unexpected claims: %+v", claims)
	}
}

func TestEnrichClaimsFromUserInfo(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer access-1" {
			t.Fatalf("Authorization = %q", got)
		}
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"sub":         "subject-1",
			"email":       "oidc-user@example.com",
			"given_name":  "Oidc",
			"family_name": "User",
		})
	}))
	defer server.Close()

	claims, err := EnrichClaimsFromUserInfo(
		context.Background(),
		Provider{UserInfoURL: server.URL},
		IDClaims{Subject: "subject-1", Claims: map[string]interface{}{"sub": "subject-1"}},
		"access-1",
	)
	if err != nil {
		t.Fatalf("EnrichClaimsFromUserInfo: %v", err)
	}
	if claims.Email != "oidc-user@example.com" || claims.FirstName != "Oidc" || claims.LastName != "User" {
		t.Fatalf("unexpected claims: %+v", claims)
	}
}

func TestAppendTokensUsesThingsBoardQueryShape(t *testing.T) {
	redirect, err := appendTokens("https://thingsflow.example/login", "access-1", "refresh-1")
	if err != nil {
		t.Fatalf("appendTokens: %v", err)
	}
	if redirect != "https://thingsflow.example/login?accessToken=access-1&refreshToken=refresh-1" {
		t.Fatalf("unexpected redirect: %s", redirect)
	}
}
