// Package auth handles JWT signing/validation. Loads the signing key
// + token TTLs from the postgres `admin_settings.jwt` row at boot
// (matches what TB-Java reads from the same source).
//
// Token claims shape mirrors TB classic so the standard UI talks to
// us without changes:
//
//	access  token: sub, userId, scopes, sessionId, exp, iss, iat,
//	               enabled, isPublic, tenantId, customerId
//	refresh token: sub, userId, scopes=["REFRESH_TOKEN"], sessionId,
//	               exp, iss, iat, isPublic, jti
//
// Handlers (HandleLogin, HandleAuthUser, …) stay in package main for
// now; this package owns the cryptographic primitives + state.
package auth

import (
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"

	dbpkg "flow-core/internal/db"
)

// SigningKey is the HMAC-SHA-512 secret used by all token operations.
// Loaded by InitConfig().
var SigningKey []byte

// Config holds token TTLs + issuer. Loaded once at boot.
var Config struct {
	TokenExpirationTime int    `json:"tokenExpirationTime"` // seconds
	RefreshTokenExpTime int    `json:"refreshTokenExpTime"` // seconds
	TokenIssuer         string `json:"tokenIssuer"`
	TokenSigningKey     string `json:"tokenSigningKey"` // base64-encoded
}

const (
	defaultTokenExp   = 9000   // 2.5 hours
	defaultRefreshExp = 604800 // 7 days
	defaultIssuer     = "flow.iot"
)

// InitConfig wires the JWT signing key + token TTLs. Lookup order:
//
//  1. JWT_TOKEN_SIGNING_KEY env var (base64). Highest precedence so
//     production deploys can carry the key in a Kubernetes Secret.
//  2. admin_settings.jwt row (the TB-classic location). Allows admin
//     UI edits to take effect on the next boot.
//  3. Fallback: in dev (FLOW_ENV unset/non-production), generate a
//     random 32-byte key in-memory + log a WARN. In production
//     (FLOW_ENV=production), log.Fatal so the bridge refuses to start
//     without an explicit signing key.
//
// Token TTLs + issuer come from the DB row when present; otherwise
// the constants above. Idempotent — safe to call again after the
// admin edits the JWT settings.
//
// SECURITY: a previous version had a hardcoded "thingsboardDefault
// SigningKey" fallback that allowed admin token forgery if the row
// was missing. That constant is gone; if neither source provides a
// key, production refuses to boot.
func InitConfig() {
	prod := strings.EqualFold(os.Getenv("FLOW_ENV"), "production")
	Config.TokenExpirationTime = defaultTokenExp
	Config.RefreshTokenExpTime = defaultRefreshExp
	Config.TokenIssuer = defaultIssuer

	// 1. Env var
	if envKey := os.Getenv("JWT_TOKEN_SIGNING_KEY"); envKey != "" {
		key, err := base64.StdEncoding.DecodeString(envKey)
		if err != nil {
			if prod {
				log.Fatalf("ERROR: JWT_TOKEN_SIGNING_KEY must be base64-encoded: %v", err)
			}
			log.Printf("WARN: JWT_TOKEN_SIGNING_KEY decode failed: %v — falling through", err)
		} else if len(key) < 32 {
			if prod {
				log.Fatalf("ERROR: JWT_TOKEN_SIGNING_KEY too short (%d bytes); HS512 needs ≥32", len(key))
			}
			log.Printf("WARN: JWT_TOKEN_SIGNING_KEY too short (%d bytes) — falling through", len(key))
		} else {
			SigningKey = key
			Config.TokenSigningKey = envKey
			log.Printf("JWT config loaded from env: issuer=%s, tokenExp=%ds, refreshExp=%ds, keyLen=%d bytes",
				Config.TokenIssuer, Config.TokenExpirationTime, Config.RefreshTokenExpTime, len(SigningKey))
			return
		}
	}

	// 2. DB row (admin_settings.jwt)
	if dbpkg.Pool != nil {
		var jsonValue string
		err := dbpkg.Pool.QueryRow("SELECT json_value FROM admin_settings WHERE key = 'jwt'").Scan(&jsonValue)
		if err == nil {
			var dbCfg struct {
				TokenExpirationTime int    `json:"tokenExpirationTime"`
				RefreshTokenExpTime int    `json:"refreshTokenExpTime"`
				TokenIssuer         string `json:"tokenIssuer"`
				TokenSigningKey     string `json:"tokenSigningKey"`
			}
			if err := json.Unmarshal([]byte(jsonValue), &dbCfg); err != nil {
				log.Printf("WARN: parse admin_settings.jwt: %v", err)
			} else if dbCfg.TokenSigningKey == "" {
				log.Printf("WARN: admin_settings.jwt has empty tokenSigningKey — ignored")
			} else if key, derr := base64.StdEncoding.DecodeString(dbCfg.TokenSigningKey); derr != nil {
				log.Printf("WARN: admin_settings.jwt key not base64: %v", derr)
			} else if len(key) < 32 {
				log.Printf("WARN: admin_settings.jwt key too short (%d bytes); HS512 needs ≥32", len(key))
			} else {
				SigningKey = key
				if dbCfg.TokenExpirationTime > 0 {
					Config.TokenExpirationTime = dbCfg.TokenExpirationTime
				}
				if dbCfg.RefreshTokenExpTime > 0 {
					Config.RefreshTokenExpTime = dbCfg.RefreshTokenExpTime
				}
				if dbCfg.TokenIssuer != "" {
					Config.TokenIssuer = dbCfg.TokenIssuer
				}
				Config.TokenSigningKey = dbCfg.TokenSigningKey
				log.Printf("JWT config loaded from DB: issuer=%s, tokenExp=%ds, refreshExp=%ds, keyLen=%d bytes",
					Config.TokenIssuer, Config.TokenExpirationTime, Config.RefreshTokenExpTime, len(SigningKey))
				return
			}
		} else if err != sql.ErrNoRows {
			log.Printf("WARN: load admin_settings.jwt: %v", err)
		}
	}

	// 3. Fallback
	if prod {
		log.Fatal("FATAL: JWT signing key not configured — set JWT_TOKEN_SIGNING_KEY (base64, ≥32 bytes) or seed admin_settings.jwt before boot in FLOW_ENV=production")
	}
	rand32 := make([]byte, 32)
	if _, err := rand.Read(rand32); err != nil {
		log.Fatalf("FATAL: crypto/rand failed generating JWT key: %v", err)
	}
	SigningKey = rand32
	Config.TokenSigningKey = base64.StdEncoding.EncodeToString(rand32)
	log.Println("WARN: JWT signing key not configured — generated random 32-byte key for this process. " +
		"Tokens won't survive restart. Set JWT_TOKEN_SIGNING_KEY for stable signing (dev mode only).")
}

// Subject is the minimum user identity shape needed for token claims.
// Decouples internal/auth from the TBUserRow struct in package main.
type Subject struct {
	UserID     string
	Email      string
	Authority  string
	TenantID   string
	CustomerID string
	Enabled    bool
}

// GenerateAccess returns a freshly signed access token for sub.
func GenerateAccess(sub Subject, sessionID string) (string, error) {
	now := time.Now()
	claims := jwt.MapClaims{
		"sub":        sub.Email,
		"userId":     sub.UserID,
		"scopes":     []string{sub.Authority},
		"sessionId":  sessionID,
		"exp":        now.Add(time.Duration(Config.TokenExpirationTime) * time.Second).Unix(),
		"iss":        Config.TokenIssuer,
		"iat":        now.Unix(),
		"enabled":    sub.Enabled,
		"isPublic":   false,
		"tenantId":   sub.TenantID,
		"customerId": sub.CustomerID,
	}
	return jwt.NewWithClaims(jwt.SigningMethodHS512, claims).SignedString(SigningKey)
}

// GenerateRefresh returns a refresh token for sub. Carries scope
// ["REFRESH_TOKEN"] so it can't be used in lieu of an access token.
func GenerateRefresh(sub Subject, sessionID string) (string, error) {
	now := time.Now()
	claims := jwt.MapClaims{
		"sub":       sub.Email,
		"userId":    sub.UserID,
		"scopes":    []string{"REFRESH_TOKEN"},
		"sessionId": sessionID,
		"exp":       now.Add(time.Duration(Config.RefreshTokenExpTime) * time.Second).Unix(),
		"iss":       Config.TokenIssuer,
		"iat":       now.Unix(),
		"isPublic":  false,
		"jti":       uuid.New().String(),
	}
	return jwt.NewWithClaims(jwt.SigningMethodHS512, claims).SignedString(SigningKey)
}

// Extract parses the JWT from the X-Authorization (or Authorization)
// header and returns the validated claims. Returns an error when the
// header is missing/malformed, the signature doesn't match, or the
// token has expired.
func Extract(r *http.Request) (jwt.MapClaims, error) {
	authHeader := r.Header.Get("X-Authorization")
	if authHeader == "" {
		authHeader = r.Header.Get("Authorization")
	}
	if authHeader == "" || !strings.HasPrefix(authHeader, "Bearer ") {
		return nil, fmt.Errorf("missing or invalid authorization header")
	}
	return ParseAndValidate(strings.TrimPrefix(authHeader, "Bearer "))
}

// ParseAndValidate parses and validates a raw JWT token string. Used
// by both HTTP handlers and WebSocket auth (which doesn't have an
// http.Request).
func ParseAndValidate(tokenString string) (jwt.MapClaims, error) {
	token, err := jwt.Parse(tokenString, func(t *jwt.Token) (interface{}, error) {
		if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", t.Header["alg"])
		}
		return SigningKey, nil
	})
	if err != nil || !token.Valid {
		return nil, fmt.Errorf("invalid or expired token: %w", err)
	}
	claims, ok := token.Claims.(jwt.MapClaims)
	if !ok {
		return nil, fmt.Errorf("invalid token claims")
	}
	return claims, nil
}
