package devicejwt

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
)

const (
	defaultIssuer   = "thingsflow-device"
	defaultAudience = "thingsflow-mqtt"
	defaultKeyID    = "thingsflow-device-es256-1"
	defaultTTL      = 24 * time.Hour
	deviceRole      = "mqtt:stream"
)

// Config describes the ThingsFlow device JWT issuer used by provisioning and edge
// JWT guards. ES256 keeps tokens compact for constrained devices while exposing
// only the public key through JWKS.
type Config struct {
	Enabled              bool
	Issuer               string
	Audience             string
	KeyID                string
	TTL                  time.Duration
	PrivateKeyPEM        string
	AdditionalPublicJWKS string
}

type publicJWK struct {
	Kty string `json:"kty"`
	Crv string `json:"crv"`
	Kid string `json:"kid"`
	Use string `json:"use,omitempty"`
	Alg string `json:"alg,omitempty"`
	X   string `json:"x"`
	Y   string `json:"y"`
}

type TokenResponse struct {
	Token        string `json:"token"`
	TokenType    string `json:"tokenType"`
	ExpiresAt    int64  `json:"expiresAt"`
	Issuer       string `json:"issuer"`
	Audience     string `json:"audience"`
	Subject      string `json:"subject"`
	MQTTIdentity string `json:"mqttIdentity"`
	MQTTUsername string `json:"mqttUsername"`
}

type Signer struct {
	cfg                  Config
	key                  *ecdsa.PrivateKey
	additionalPublicJWKs []publicJWK
}

var (
	currentMu     sync.RWMutex
	currentSigner *Signer
	currentOn     bool
)

func InitFromEnv() error {
	cfg, err := configFromEnv()
	if err != nil {
		return err
	}
	if !cfg.Enabled {
		currentMu.Lock()
		currentSigner = nil
		currentOn = false
		currentMu.Unlock()
		slog.Info("device JWT issuer disabled")
		return nil
	}
	signer, err := NewSigner(cfg)
	if err != nil {
		return err
	}
	currentMu.Lock()
	currentSigner = signer
	currentOn = true
	currentMu.Unlock()
	slog.Info("device JWT issuer ready",
		"issuer", signer.cfg.Issuer,
		"audience", signer.cfg.Audience,
		"kid", signer.cfg.KeyID,
		"ttl_seconds", int64(signer.cfg.TTL.Seconds()))
	return nil
}

func Enabled() bool {
	currentMu.RLock()
	defer currentMu.RUnlock()
	return currentOn && currentSigner != nil
}

func Issue(deviceID, tenantID, deviceName string) (TokenResponse, error) {
	currentMu.RLock()
	signer := currentSigner
	currentMu.RUnlock()
	if signer == nil {
		return TokenResponse{}, errors.New("device JWT issuer disabled")
	}
	return signer.Issue(deviceID, tenantID, deviceName)
}

func Validate(tokenString string) (jwt.MapClaims, error) {
	currentMu.RLock()
	signer := currentSigner
	currentMu.RUnlock()
	if signer == nil {
		return nil, errors.New("device JWT issuer disabled")
	}
	return signer.Validate(tokenString)
}

func HandleJWKS(w http.ResponseWriter, r *http.Request) {
	currentMu.RLock()
	signer := currentSigner
	currentMu.RUnlock()
	if signer == nil {
		http.Error(w, "device JWT issuer disabled", http.StatusNotFound)
		return
	}
	signer.HandleJWKS(w, r)
}

func HandlePublicPEM(w http.ResponseWriter, r *http.Request) {
	currentMu.RLock()
	signer := currentSigner
	currentMu.RUnlock()
	if signer == nil {
		http.Error(w, "device JWT issuer disabled", http.StatusNotFound)
		return
	}
	signer.HandlePublicPEM(w, r)
}

func NewSigner(cfg Config) (*Signer, error) {
	if !cfg.Enabled {
		return nil, errors.New("device JWT signer disabled")
	}
	cfg.Issuer = firstNonEmpty(cfg.Issuer, defaultIssuer)
	cfg.Audience = firstNonEmpty(cfg.Audience, defaultAudience)
	cfg.KeyID = firstNonEmpty(cfg.KeyID, defaultKeyID)
	if cfg.TTL <= 0 {
		cfg.TTL = defaultTTL
	}
	var key *ecdsa.PrivateKey
	var err error
	if strings.TrimSpace(cfg.PrivateKeyPEM) != "" {
		key, err = parseECPrivateKey(cfg.PrivateKeyPEM)
	} else {
		// An unpinned key is regenerated on every boot, and the consequence is
		// easy to misread: the MQTT broker fetched the *previous* public key at
		// its own startup, so after a flow-core restart every newly issued device
		// JWT fails authentication — while already-connected sessions keep
		// publishing, because the broker does not re-verify them. The platform
		// looks healthy and only new or reconnecting devices are locked out.
		//
		// Same posture as the user JWT signing key: refuse in production, where
		// the Helm values already require a pinned key, and warn loudly elsewhere.
		if strings.EqualFold(os.Getenv("FLOW_ENV"), "production") {
			return nil, errors.New(
				"DEVICE_JWT_ES256_PRIVATE_KEY_PEM_B64 is required in FLOW_ENV=production: " +
					"an ephemeral key breaks every device's MQTT authentication on restart")
		}
		slog.Warn("device JWT key not configured — generated an ephemeral P-256 key for this process",
			"consequence", "device JWTs issued before a restart stop authenticating; restart the MQTT broker so it re-fetches the public key, or set DEVICE_JWT_ES256_PRIVATE_KEY_PEM_B64")
		key, err = ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	}
	if err != nil {
		return nil, fmt.Errorf("device JWT key: %w", err)
	}
	if key.Curve != elliptic.P256() {
		return nil, errors.New("device JWT key must use P-256 for ES256")
	}
	additional, err := parseAdditionalPublicJWKs(cfg.AdditionalPublicJWKS)
	if err != nil {
		return nil, err
	}
	return &Signer{cfg: cfg, key: key, additionalPublicJWKs: additional}, nil
}

func (s *Signer) Validate(tokenString string) (jwt.MapClaims, error) {
	tokenString = strings.TrimSpace(strings.TrimPrefix(tokenString, "Bearer "))
	if tokenString == "" {
		return nil, errors.New("device JWT is required")
	}
	claims := jwt.MapClaims{}
	token, err := jwt.ParseWithClaims(
		tokenString,
		claims,
		func(token *jwt.Token) (interface{}, error) {
			if token.Method != jwt.SigningMethodES256 {
				return nil, fmt.Errorf("unexpected device JWT alg %q", token.Header["alg"])
			}
			return s.verificationKey(token)
		},
		jwt.WithIssuer(s.cfg.Issuer),
		jwt.WithAudience(s.cfg.Audience),
		jwt.WithValidMethods([]string{jwt.SigningMethodES256.Alg()}),
	)
	if err != nil {
		return nil, err
	}
	if token == nil || !token.Valid {
		return nil, errors.New("invalid device JWT")
	}
	return claims, nil
}

func (s *Signer) Issue(deviceID, tenantID, deviceName string) (TokenResponse, error) {
	deviceID = strings.TrimSpace(deviceID)
	tenantID = strings.TrimSpace(tenantID)
	deviceName = strings.TrimSpace(deviceName)
	if deviceID == "" {
		return TokenResponse{}, errors.New("deviceID is required")
	}
	if tenantID == "" {
		return TokenResponse{}, errors.New("tenantID is required")
	}
	mqttIdentity := topicSafeIdentity(deviceID)
	now := time.Now().UTC()
	exp := now.Add(s.cfg.TTL)
	claims := jwt.MapClaims{
		"iss":        s.cfg.Issuer,
		"sub":        deviceID,
		"aud":        jwt.ClaimStrings{s.cfg.Audience},
		"iat":        now.Unix(),
		"nbf":        now.Unix(),
		"exp":        exp.Unix(),
		"jti":        uuid.NewString(),
		"tenantId":   tenantID,
		"deviceId":   deviceID,
		"deviceName": deviceName,
		"mqttId":     mqttIdentity,
		// clientid pins the MQTT Client ID: rmqtt-auth-jwt validates the connection's
		// Client ID equals this claim (validate_claims.clientid), and the ACL binds the
		// publish topic to %c (the Client ID). Together a device may only publish to
		// thingsflow/devices/<mqttId>/... — it cannot use another device's topic.
		"clientid": mqttIdentity,
		"scope":    deviceRole,
		"roles":    []string{deviceRole},
	}
	token := jwt.NewWithClaims(jwt.SigningMethodES256, claims)
	token.Header["kid"] = s.cfg.KeyID
	signed, err := token.SignedString(s.key)
	if err != nil {
		return TokenResponse{}, err
	}
	return TokenResponse{
		Token:        signed,
		TokenType:    "Bearer",
		ExpiresAt:    exp.Unix(),
		Issuer:       s.cfg.Issuer,
		Audience:     s.cfg.Audience,
		Subject:      deviceID,
		MQTTIdentity: mqttIdentity,
		MQTTUsername: "Bearer " + signed,
	}, nil
}

func (s *Signer) PublicKey() interface{} {
	return &s.key.PublicKey
}

func (s *Signer) PublicKeyPEM() ([]byte, error) {
	der, err := x509.MarshalPKIXPublicKey(&s.key.PublicKey)
	if err != nil {
		return nil, err
	}
	return pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der}), nil
}

func (s *Signer) HandleJWKS(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	keys := append([]publicJWK{s.activePublicJWK()}, s.additionalPublicJWKs...)
	resp := map[string]interface{}{
		"keys": keys,
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func (s *Signer) HandlePublicPEM(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	pub, err := s.PublicKeyPEM()
	if err != nil {
		http.Error(w, "device JWT public key unavailable", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/x-pem-file")
	_, _ = w.Write(pub)
}

func configFromEnv() (Config, error) {
	enabled := envBool("DEVICE_JWT_ENABLED", true)
	ttl := defaultTTL
	if raw := strings.TrimSpace(os.Getenv("DEVICE_JWT_TTL_SECONDS")); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n <= 0 {
			return Config{}, fmt.Errorf("DEVICE_JWT_TTL_SECONDS must be a positive integer")
		}
		ttl = time.Duration(n) * time.Second
	}
	privateKey := strings.TrimSpace(os.Getenv("DEVICE_JWT_ES256_PRIVATE_KEY_PEM"))
	if privateKey == "" {
		if raw := strings.TrimSpace(os.Getenv("DEVICE_JWT_ES256_PRIVATE_KEY_PEM_B64")); raw != "" {
			decoded, err := base64.StdEncoding.DecodeString(raw)
			if err != nil {
				return Config{}, fmt.Errorf("DEVICE_JWT_ES256_PRIVATE_KEY_PEM_B64: %w", err)
			}
			privateKey = string(decoded)
		}
	}
	if enabled && privateKey == "" && strings.EqualFold(os.Getenv("FLOW_ENV"), "production") {
		return Config{}, errors.New("DEVICE_JWT_ES256_PRIVATE_KEY_PEM_B64 or DEVICE_JWT_ES256_PRIVATE_KEY_PEM is required in production")
	}
	additionalPublicJWKS := strings.TrimSpace(os.Getenv("DEVICE_JWT_ADDITIONAL_PUBLIC_JWKS"))
	if additionalPublicJWKS == "" {
		if raw := strings.TrimSpace(os.Getenv("DEVICE_JWT_ADDITIONAL_PUBLIC_JWKS_B64")); raw != "" {
			decoded, err := base64.StdEncoding.DecodeString(raw)
			if err != nil {
				return Config{}, fmt.Errorf("DEVICE_JWT_ADDITIONAL_PUBLIC_JWKS_B64: %w", err)
			}
			additionalPublicJWKS = string(decoded)
		}
	}
	return Config{
		Enabled:              enabled,
		Issuer:               firstNonEmpty(os.Getenv("DEVICE_JWT_ISSUER"), defaultIssuer),
		Audience:             firstNonEmpty(os.Getenv("DEVICE_JWT_AUDIENCE"), defaultAudience),
		KeyID:                firstNonEmpty(os.Getenv("DEVICE_JWT_KEY_ID"), defaultKeyID),
		TTL:                  ttl,
		PrivateKeyPEM:        privateKey,
		AdditionalPublicJWKS: additionalPublicJWKS,
	}, nil
}

// verificationKey picks the public key a device JWT should be checked against.
//
// Signing always uses the active key; verification also accepts the keys listed
// in DEVICE_JWT_ADDITIONAL_PUBLIC_JWKS. That asymmetry is what makes a signing
// key rotation survivable. Without it, a device holding a token signed by the
// retired key is stranded: the broker already has the new public key so it
// cannot connect, and it cannot refresh either, because
// POST /api/v1/devices/me/jwt/refresh authenticates with the very token being
// rejected. Recovering it needs an out-of-band re-issue — a site visit, for a
// meter in the field.
//
// The additional keys were already parsed and already published in
// /api/noauth/device-jwks; they were simply never consulted here, so the
// endpoint advertised keys this code would refuse.
//
// Selection is by `kid`, which is what a JWKS is for: each token names the key
// that signed it, so there is no "try every key until one works" — an unknown
// kid is rejected rather than silently falling back, and a token with no kid is
// checked against the active key, preserving the behaviour of tokens minted
// before key ids were set.
//
// Rotating without stranding anyone is therefore:
//
//  1. add the NEW public key to the additional set, keep signing with the old
//  2. switch signing to the new key (old tokens still verify, so devices can
//     refresh); move the OLD public key into the additional set
//  3. after one token TTL has elapsed, drop the old key
func (s *Signer) verificationKey(token *jwt.Token) (interface{}, error) {
	kid, _ := token.Header["kid"].(string)
	kid = strings.TrimSpace(kid)
	if kid == "" || kid == s.cfg.KeyID {
		return &s.key.PublicKey, nil
	}
	for _, jwk := range s.additionalPublicJWKs {
		if jwk.Kid != kid {
			continue
		}
		pub, err := jwk.toECDSA()
		if err != nil {
			return nil, fmt.Errorf("device JWT additional key %q: %w", kid, err)
		}
		return pub, nil
	}
	// Naming an unknown key is not a reason to fall back to the active one: that
	// would make a stale or attacker-chosen kid indistinguishable from a valid
	// token and defeat the point of pinning verification to a named key.
	return nil, fmt.Errorf("device JWT signed by unknown key id %q", kid)
}

// toECDSA rebuilds the P-256 public key from its JWK coordinates.
func (j publicJWK) toECDSA() (*ecdsa.PublicKey, error) {
	x, err := base64.RawURLEncoding.DecodeString(j.X)
	if err != nil {
		return nil, fmt.Errorf("x: %w", err)
	}
	y, err := base64.RawURLEncoding.DecodeString(j.Y)
	if err != nil {
		return nil, fmt.Errorf("y: %w", err)
	}
	pub := &ecdsa.PublicKey{
		Curve: elliptic.P256(),
		X:     new(big.Int).SetBytes(x),
		Y:     new(big.Int).SetBytes(y),
	}
	// A point off the curve is not a usable key, and ecdsa.Verify would panic on
	// some inputs rather than return false.
	if !pub.Curve.IsOnCurve(pub.X, pub.Y) {
		return nil, errors.New("point is not on the P-256 curve")
	}
	return pub, nil
}

func (s *Signer) activePublicJWK() publicJWK {
	pub := s.key.PublicKey
	return publicJWK{
		Kty: "EC",
		Crv: "P-256",
		Kid: s.cfg.KeyID,
		Use: "sig",
		Alg: "ES256",
		X:   base64.RawURLEncoding.EncodeToString(padP256(pub.X.Bytes())),
		Y:   base64.RawURLEncoding.EncodeToString(padP256(pub.Y.Bytes())),
	}
}

func parseAdditionalPublicJWKs(raw string) ([]publicJWK, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}
	var parsed struct {
		Keys []publicJWK `json:"keys"`
	}
	if err := json.Unmarshal([]byte(raw), &parsed); err != nil {
		return nil, fmt.Errorf("device JWT additional JWKS: %w", err)
	}
	keys := make([]publicJWK, 0, len(parsed.Keys))
	for _, key := range parsed.Keys {
		if key.Kty != "EC" || key.Crv != "P-256" || strings.TrimSpace(key.Kid) == "" || strings.TrimSpace(key.X) == "" || strings.TrimSpace(key.Y) == "" {
			return nil, fmt.Errorf("device JWT additional JWKS contains unsupported key %q; expected EC P-256 with kid/x/y", key.Kid)
		}
		if key.Use == "" {
			key.Use = "sig"
		}
		if key.Alg == "" {
			key.Alg = "ES256"
		}
		keys = append(keys, key)
	}
	return keys, nil
}

func parseECPrivateKey(raw string) (*ecdsa.PrivateKey, error) {
	block, _ := pem.Decode([]byte(raw))
	if block == nil {
		return nil, errors.New("invalid PEM block")
	}
	if key, err := x509.ParseECPrivateKey(block.Bytes); err == nil {
		return key, nil
	}
	parsed, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, err
	}
	key, ok := parsed.(*ecdsa.PrivateKey)
	if !ok {
		return nil, errors.New("private key is not ECDSA")
	}
	return key, nil
}

func topicSafeIdentity(deviceID string) string {
	replacer := strings.NewReplacer("-", "", "_", "", ":", "", "/", "")
	return replacer.Replace(strings.TrimSpace(deviceID))
}

func padP256(in []byte) []byte {
	if len(in) >= 32 {
		return in
	}
	out := make([]byte, 32)
	copy(out[32-len(in):], in)
	return out
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
	}
	return ""
}

func envBool(key string, def bool) bool {
	v := strings.TrimSpace(strings.ToLower(os.Getenv(key)))
	if v == "" {
		return def
	}
	switch v {
	case "1", "true", "yes", "y", "on":
		return true
	case "0", "false", "no", "n", "off":
		return false
	default:
		return def
	}
}
