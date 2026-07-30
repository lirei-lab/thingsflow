package oidc

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"golang.org/x/crypto/bcrypt"

	"flow-core/internal/audit"
	authpkg "flow-core/internal/auth"
	dbpkg "flow-core/internal/db"
	"flow-core/internal/httputil"
	userpkg "flow-core/internal/user"
)

type Provider struct {
	ID                string   `json:"id"`
	Title             string   `json:"title"`
	ClientID          string   `json:"clientId"`
	ClientSecret      string   `json:"clientSecret"`
	Issuer            string   `json:"issuer"`
	AuthorizationURL  string   `json:"authorizationUrl"`
	TokenURL          string   `json:"tokenUrl"`
	JWKSURL           string   `json:"jwksUrl"`
	UserInfoURL       string   `json:"userInfoUrl"`
	RedirectURL       string   `json:"redirectUrl"`
	DefaultTenantID   string   `json:"defaultTenantId"`
	DefaultCustomerID string   `json:"defaultCustomerId"`
	DefaultAuthority  string   `json:"defaultAuthority"`
	AllowUserCreation bool     `json:"allowUserCreation"`
	Scopes            []string `json:"scopes"`
}

type State struct {
	ProviderID string `json:"providerId"`
	ReturnURL  string `json:"returnUrl"`
	Nonce      string `json:"nonce"`
	IssuedAt   int64  `json:"issuedAt"`
}

type StateCodec struct {
	Key []byte
}

type IDClaims struct {
	Issuer    string
	Subject   string
	Email     string
	FirstName string
	LastName  string
	Claims    map[string]interface{}
}

type discoveryDocument struct {
	Issuer                string `json:"issuer"`
	AuthorizationEndpoint string `json:"authorization_endpoint"`
	TokenEndpoint         string `json:"token_endpoint"`
	JWKSURI               string `json:"jwks_uri"`
	UserInfoEndpoint      string `json:"userinfo_endpoint"`
}

func ProvidersFromEnv() ([]Provider, error) {
	if raw := strings.TrimSpace(os.Getenv("OIDC_PROVIDERS_JSON")); raw != "" {
		var providers []Provider
		if err := json.Unmarshal([]byte(raw), &providers); err != nil {
			return nil, fmt.Errorf("parse OIDC_PROVIDERS_JSON: %w", err)
		}
		for i := range providers {
			normalizeProvider(&providers[i])
			if err := applyDiscovery(&providers[i]); err != nil {
				return nil, err
			}
			if err := validateProvider(providers[i]); err != nil {
				return nil, err
			}
		}
		return providers, nil
	}

	if !envBool("OIDC_ENABLED") {
		return nil, nil
	}
	p := Provider{
		ID:                envDefault("OIDC_PROVIDER_ID", "oidc"),
		Title:             envDefault("OIDC_PROVIDER_TITLE", "OIDC"),
		ClientID:          os.Getenv("OIDC_CLIENT_ID"),
		ClientSecret:      os.Getenv("OIDC_CLIENT_SECRET"),
		Issuer:            os.Getenv("OIDC_ISSUER"),
		AuthorizationURL:  os.Getenv("OIDC_AUTHORIZATION_URL"),
		TokenURL:          os.Getenv("OIDC_TOKEN_URL"),
		JWKSURL:           os.Getenv("OIDC_JWKS_URL"),
		UserInfoURL:       os.Getenv("OIDC_USERINFO_URL"),
		RedirectURL:       os.Getenv("OIDC_REDIRECT_URL"),
		DefaultTenantID:   os.Getenv("OIDC_DEFAULT_TENANT_ID"),
		DefaultCustomerID: os.Getenv("OIDC_DEFAULT_CUSTOMER_ID"),
		DefaultAuthority:  envDefault("OIDC_DEFAULT_AUTHORITY", "TENANT_ADMIN"),
		AllowUserCreation: envBool("OIDC_ALLOW_USER_CREATION"),
	}
	normalizeProvider(&p)
	if err := applyDiscovery(&p); err != nil {
		return nil, err
	}
	if err := validateProvider(p); err != nil {
		return nil, err
	}
	return []Provider{p}, nil
}

func (c StateCodec) Sign(state State) (string, error) {
	if len(c.Key) < 32 {
		return "", errors.New("OIDC state signing key must be at least 32 bytes")
	}
	b, err := json.Marshal(state)
	if err != nil {
		return "", err
	}
	payload := base64.RawURLEncoding.EncodeToString(b)
	mac := hmac.New(sha256.New, c.Key)
	_, _ = mac.Write([]byte(payload))
	sig := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	return payload + "." + sig, nil
}

func (c StateCodec) Verify(raw, expectedProvider string, maxAge time.Duration) (State, error) {
	var state State
	parts := strings.Split(raw, ".")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return state, errors.New("invalid state format")
	}
	mac := hmac.New(sha256.New, c.Key)
	_, _ = mac.Write([]byte(parts[0]))
	expectedSig := mac.Sum(nil)
	gotSig, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return state, errors.New("invalid state signature")
	}
	if !hmac.Equal(gotSig, expectedSig) {
		return state, errors.New("state signature mismatch")
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return state, err
	}
	if err := json.Unmarshal(payload, &state); err != nil {
		return state, err
	}
	if expectedProvider != "" && state.ProviderID != expectedProvider {
		return state, errors.New("state provider mismatch")
	}
	if maxAge > 0 && time.Since(time.Unix(state.IssuedAt, 0)) > maxAge {
		return state, errors.New("state expired")
	}
	return state, nil
}

func ValidateIDToken(p Provider, raw string) (IDClaims, error) {
	keys, err := fetchJWKS(p.JWKSURL)
	if err != nil {
		return IDClaims{}, err
	}
	claims := jwt.MapClaims{}
	token, err := jwt.ParseWithClaims(raw, claims, func(t *jwt.Token) (interface{}, error) {
		if _, ok := t.Method.(*jwt.SigningMethodRSA); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", t.Header["alg"])
		}
		kid, _ := t.Header["kid"].(string)
		if kid == "" {
			return nil, errors.New("missing token kid")
		}
		key, ok := keys[kid]
		if !ok {
			return nil, fmt.Errorf("unknown token kid: %s", kid)
		}
		return key, nil
	})
	if err != nil || !token.Valid {
		return IDClaims{}, fmt.Errorf("invalid id_token: %w", err)
	}
	issuer, _ := claims["iss"].(string)
	if issuer != p.Issuer {
		return IDClaims{}, errors.New("id_token issuer mismatch")
	}
	if !claimAudienceContains(claims["aud"], p.ClientID) {
		return IDClaims{}, errors.New("id_token audience mismatch")
	}
	exp, err := claims.GetExpirationTime()
	if err != nil || exp == nil || time.Now().After(exp.Time) {
		return IDClaims{}, errors.New("id_token expired or missing exp")
	}
	subject, _ := claims["sub"].(string)
	if subject == "" {
		return IDClaims{}, errors.New("id_token missing sub")
	}
	email, _ := claims["email"].(string)
	firstName, _ := claims["given_name"].(string)
	lastName, _ := claims["family_name"].(string)
	return IDClaims{
		Issuer:    issuer,
		Subject:   subject,
		Email:     email,
		FirstName: firstName,
		LastName:  lastName,
		Claims:    map[string]interface{}(claims),
	}, nil
}

func HandleOAuth2Clients(w http.ResponseWriter, r *http.Request) {
	if r.Method == "OPTIONS" {
		w.WriteHeader(http.StatusOK)
		return
	}
	providers, err := ProvidersFromEnv()
	if err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, "OIDC providers misconfigured")
		return
	}
	out := make([]map[string]interface{}, 0, len(providers))
	for _, p := range providers {
		out = append(out, map[string]interface{}{
			"id":               p.ID,
			"name":             p.ID,
			"title":            p.Title,
			"loginButtonLabel": p.Title,
			"url":              "/api/noauth/oidc/authorize/" + p.ID,
			"platforms":        []string{"WEB"},
		})
	}
	httputil.WriteJSON(w, http.StatusOK, out)
}

func HandleAuthorize(w http.ResponseWriter, r *http.Request, providerID string) {
	if r.Method == "OPTIONS" {
		w.WriteHeader(http.StatusOK)
		return
	}
	if r.Method != "GET" {
		httputil.WriteError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}
	p, err := providerByID(providerID)
	if err != nil {
		httputil.WriteError(w, http.StatusNotFound, "OIDC provider not found")
		return
	}
	nonce := randomString(24)
	state, err := stateCodec().Sign(State{
		ProviderID: p.ID,
		ReturnURL:  returnURL(r),
		Nonce:      nonce,
		IssuedAt:   time.Now().Unix(),
	})
	if err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, "Failed to create OIDC state")
		return
	}
	q := url.Values{}
	q.Set("response_type", "code")
	q.Set("client_id", p.ClientID)
	q.Set("redirect_uri", redirectURI(r, p))
	q.Set("scope", strings.Join(p.Scopes, " "))
	q.Set("state", state)
	q.Set("nonce", nonce)
	redirect := p.AuthorizationURL
	if strings.Contains(redirect, "?") {
		redirect += "&" + q.Encode()
	} else {
		redirect += "?" + q.Encode()
	}
	http.Redirect(w, r, redirect, http.StatusFound)
}

func HandleCallback(w http.ResponseWriter, r *http.Request, providerID string) {
	if r.Method == "OPTIONS" {
		w.WriteHeader(http.StatusOK)
		return
	}
	if r.Method != "GET" && r.Method != "POST" {
		httputil.WriteError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}
	if errText := r.URL.Query().Get("error"); errText != "" {
		httputil.WriteError(w, http.StatusUnauthorized, "OIDC login failed: "+errText)
		return
	}
	p, err := providerByID(providerID)
	if err != nil {
		httputil.WriteError(w, http.StatusNotFound, "OIDC provider not found")
		return
	}
	state, err := stateCodec().Verify(r.URL.Query().Get("state"), p.ID, 10*time.Minute)
	if err != nil {
		httputil.WriteError(w, http.StatusBadRequest, "Invalid OIDC state")
		return
	}
	tokens, err := exchangeCode(r.Context(), p, r.URL.Query().Get("code"), redirectURI(r, p))
	if err != nil {
		httputil.WriteError(w, http.StatusUnauthorized, "OIDC token exchange failed")
		return
	}
	claims, err := ValidateIDToken(p, tokens.IDToken)
	if err != nil {
		httputil.WriteError(w, http.StatusUnauthorized, "OIDC id_token validation failed")
		return
	}
	if claims.Email == "" && tokens.AccessToken != "" && p.UserInfoURL != "" {
		if enriched, err := EnrichClaimsFromUserInfo(r.Context(), p, claims, tokens.AccessToken); err == nil {
			claims = enriched
		}
	}
	localUser, err := FindOrCreateUser(p, claims)
	if err != nil {
		httputil.WriteError(w, http.StatusUnauthorized, err.Error())
		return
	}
	sessionID := uuid.New().String()
	access, err := authpkg.GenerateAccess(subjectFromUser(localUser), sessionID)
	if err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, "Failed to generate access token")
		return
	}
	refresh, err := authpkg.GenerateRefresh(subjectFromUser(localUser), sessionID)
	if err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, "Failed to generate refresh token")
		return
	}
	_, _ = dbpkg.Pool.Exec("UPDATE user_credentials SET last_login_ts = $1 WHERE user_id = $2", time.Now().UnixMilli(), localUser.ID)
	audit.Write(audit.Event{
		TenantID:   localUser.TenantID,
		UserID:     localUser.ID,
		UserName:   localUser.Email,
		EntityID:   localUser.ID,
		EntityType: "USER",
		EntityName: localUser.Email,
		ActionType: "LOGIN",
		ActionData: fmt.Sprintf(`{"providerId":%q,"externalSubject":%q}`, p.ID, claims.Subject),
		Status:     "SUCCESS",
	})
	if wantsJSON(r) {
		httputil.WriteJSON(w, http.StatusOK, map[string]string{"token": access, "refreshToken": refresh})
		return
	}
	redirectTarget, err := appendTokens(state.ReturnURL, access, refresh)
	if err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, "Failed to build OAuth redirect")
		return
	}
	http.Redirect(w, r, redirectTarget, http.StatusFound)
}

func FindOrCreateUser(p Provider, claims IDClaims) (*userpkg.TBUserRow, error) {
	if dbpkg.Pool == nil {
		return nil, errors.New("database not available")
	}
	if claims.Email == "" {
		return nil, errors.New("OIDC user email is required")
	}
	var userID string
	err := dbpkg.Pool.QueryRow(`
		SELECT user_id::text
		FROM external_identity
		WHERE provider_id = $1 AND issuer = $2 AND subject = $3`,
		p.ID, claims.Issuer, claims.Subject,
	).Scan(&userID)
	if err == nil {
		return userpkg.FindByID(userID)
	}
	if err != sql.ErrNoRows {
		return nil, fmt.Errorf("external identity lookup failed: %w", err)
	}

	localUser, err := userpkg.FindByEmail(claims.Email)
	if err != nil {
		if !p.AllowUserCreation {
			return nil, errors.New("OIDC user is not linked and auto-creation is disabled")
		}
		localUser, err = createUser(p, claims)
		if err != nil {
			return nil, err
		}
	}
	if err := linkIdentity(p, claims, localUser.ID); err != nil {
		return nil, err
	}
	return localUser, nil
}

func createUser(p Provider, claims IDClaims) (*userpkg.TBUserRow, error) {
	if p.DefaultTenantID == "" && p.DefaultAuthority != "SYS_ADMIN" {
		return nil, errors.New("OIDC default tenant is required for auto-created users")
	}
	now := time.Now().UnixMilli()
	userID := uuid.New().String()
	credID := uuid.New().String()
	additionalInfo := map[string]interface{}{"authProvider": p.ID}
	additionalJSON, _ := json.Marshal(additionalInfo)
	placeholderPassword, err := bcrypt.GenerateFromPassword([]byte(uuid.NewString()+uuid.NewString()), bcrypt.DefaultCost)
	if err != nil {
		return nil, err
	}
	_, err = dbpkg.Pool.Exec(`
		INSERT INTO tb_user (
			id, created_time, additional_info, authority, customer_id, email,
			first_name, last_name, phone, tenant_id, version
		)
		VALUES (
			$1, $2, $3, $4, NULLIF($5,'')::uuid, $6,
			NULLIF($7,''), NULLIF($8,''), NULL, NULLIF($9,'')::uuid, 1
		)`,
		userID, now, string(additionalJSON), p.DefaultAuthority, p.DefaultCustomerID,
		claims.Email, claims.FirstName, claims.LastName, p.DefaultTenantID,
	)
	if err != nil {
		return nil, fmt.Errorf("create OIDC user: %w", err)
	}
	_, err = dbpkg.Pool.Exec(`
		INSERT INTO user_credentials (
			id, created_time, enabled, password, user_id, additional_info,
			last_login_ts, failed_login_attempts
		)
		VALUES ($1, $2, true, $3, $4, '{"authProvider":"oidc"}', NULL, 0)`,
		credID, now, string(placeholderPassword), userID,
	)
	if err != nil {
		return nil, fmt.Errorf("create OIDC credentials: %w", err)
	}
	return userpkg.FindByID(userID)
}

func linkIdentity(p Provider, claims IDClaims, userID string) error {
	now := time.Now().UnixMilli()
	claimJSON, _ := json.Marshal(claims.Claims)
	_, err := dbpkg.Pool.Exec(`
		INSERT INTO external_identity (
			id, created_time, updated_time, provider_id, issuer, subject,
			user_id, email, claims
		)
		VALUES ($1, $2, $2, $3, $4, $5, $6, $7, $8)
		ON CONFLICT (provider_id, issuer, subject) DO UPDATE
		SET updated_time = EXCLUDED.updated_time,
		    user_id = EXCLUDED.user_id,
		    email = EXCLUDED.email,
		    claims = EXCLUDED.claims`,
		uuid.New().String(), now, p.ID, claims.Issuer, claims.Subject, userID, claims.Email, string(claimJSON),
	)
	if err != nil {
		return fmt.Errorf("link OIDC identity: %w", err)
	}
	return nil
}

type tokenResponse struct {
	AccessToken string `json:"access_token"`
	IDToken     string `json:"id_token"`
}

func EnrichClaimsFromUserInfo(ctx context.Context, p Provider, claims IDClaims, accessToken string) (IDClaims, error) {
	if strings.TrimSpace(p.UserInfoURL) == "" {
		return claims, errors.New("OIDC userinfo URL is empty")
	}
	if strings.TrimSpace(accessToken) == "" {
		return claims, errors.New("OIDC access token is empty")
	}
	req, err := http.NewRequestWithContext(ctx, "GET", p.UserInfoURL, nil)
	if err != nil {
		return claims, err
	}
	req.Header.Set("Authorization", "Bearer "+accessToken)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return claims, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return claims, fmt.Errorf("userinfo endpoint status %d: %s", resp.StatusCode, string(body))
	}
	var info map[string]interface{}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&info); err != nil {
		return claims, err
	}
	if sub, _ := info["sub"].(string); sub != "" && claims.Subject != "" && sub != claims.Subject {
		return claims, errors.New("userinfo subject mismatch")
	}
	if claims.Email == "" {
		claims.Email, _ = info["email"].(string)
	}
	if claims.FirstName == "" {
		claims.FirstName, _ = info["given_name"].(string)
	}
	if claims.LastName == "" {
		claims.LastName, _ = info["family_name"].(string)
	}
	if claims.Claims == nil {
		claims.Claims = map[string]interface{}{}
	}
	for k, v := range info {
		if _, exists := claims.Claims[k]; !exists {
			claims.Claims[k] = v
		}
	}
	return claims, nil
}

func exchangeCode(ctx context.Context, p Provider, code, redirect string) (tokenResponse, error) {
	if code == "" {
		return tokenResponse{}, errors.New("missing authorization code")
	}
	form := url.Values{}
	form.Set("grant_type", "authorization_code")
	form.Set("code", code)
	form.Set("redirect_uri", redirect)
	form.Set("client_id", p.ClientID)
	if p.ClientSecret != "" {
		form.Set("client_secret", p.ClientSecret)
	}
	req, err := http.NewRequest("POST", p.TokenURL, bytes.NewBufferString(form.Encode()))
	if err != nil {
		return tokenResponse{}, err
	}
	req = req.WithContext(ctx)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return tokenResponse{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return tokenResponse{}, fmt.Errorf("token endpoint status %d: %s", resp.StatusCode, string(body))
	}
	var out tokenResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return tokenResponse{}, err
	}
	if out.IDToken == "" {
		return tokenResponse{}, errors.New("token response missing id_token")
	}
	return out, nil
}

func fetchJWKS(jwksURL string) (map[string]*rsa.PublicKey, error) {
	resp, err := http.Get(jwksURL)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("jwks status %d", resp.StatusCode)
	}
	var set struct {
		Keys []struct {
			Kty string `json:"kty"`
			Kid string `json:"kid"`
			N   string `json:"n"`
			E   string `json:"e"`
		} `json:"keys"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&set); err != nil {
		return nil, err
	}
	keys := map[string]*rsa.PublicKey{}
	for _, jwk := range set.Keys {
		if jwk.Kty != "RSA" || jwk.Kid == "" {
			continue
		}
		nBytes, err := base64.RawURLEncoding.DecodeString(jwk.N)
		if err != nil {
			return nil, err
		}
		eBytes, err := base64.RawURLEncoding.DecodeString(jwk.E)
		if err != nil {
			return nil, err
		}
		e := 0
		for _, b := range eBytes {
			e = e<<8 + int(b)
		}
		if e == 0 {
			e = 65537
		}
		keys[jwk.Kid] = &rsa.PublicKey{N: new(big.Int).SetBytes(nBytes), E: e}
	}
	return keys, nil
}

func normalizeProvider(p *Provider) {
	if p.ID == "" {
		p.ID = "oidc"
	}
	if p.Title == "" {
		p.Title = p.ID
	}
	if p.DefaultAuthority == "" {
		p.DefaultAuthority = "TENANT_ADMIN"
	}
	if len(p.Scopes) == 0 {
		p.Scopes = []string{"openid", "email", "profile"}
	}
}

func applyDiscovery(p *Provider) error {
	if strings.TrimSpace(p.Issuer) == "" {
		return nil
	}
	if p.AuthorizationURL != "" && p.TokenURL != "" && p.JWKSURL != "" {
		return nil
	}
	doc, err := fetchDiscovery(p.Issuer)
	if err != nil {
		return fmt.Errorf("OIDC provider %q discovery: %w", p.ID, err)
	}
	if doc.Issuer != "" && strings.TrimRight(doc.Issuer, "/") != strings.TrimRight(p.Issuer, "/") {
		return fmt.Errorf("OIDC provider %q discovery issuer mismatch: %q", p.ID, doc.Issuer)
	}
	if p.AuthorizationURL == "" {
		p.AuthorizationURL = doc.AuthorizationEndpoint
	}
	if p.TokenURL == "" {
		p.TokenURL = doc.TokenEndpoint
	}
	if p.JWKSURL == "" {
		p.JWKSURL = doc.JWKSURI
	}
	if p.UserInfoURL == "" {
		p.UserInfoURL = doc.UserInfoEndpoint
	}
	return nil
}

func fetchDiscovery(issuer string) (discoveryDocument, error) {
	var doc discoveryDocument
	discoveryURL := strings.TrimRight(issuer, "/") + "/.well-known/openid-configuration"
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(discoveryURL)
	if err != nil {
		return doc, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return doc, fmt.Errorf("status %d", resp.StatusCode)
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&doc); err != nil {
		return doc, err
	}
	if doc.AuthorizationEndpoint == "" || doc.TokenEndpoint == "" || doc.JWKSURI == "" {
		return doc, errors.New("missing authorization_endpoint, token_endpoint, or jwks_uri")
	}
	return doc, nil
}

func validateProvider(p Provider) error {
	for name, value := range map[string]string{
		"id":               p.ID,
		"clientId":         p.ClientID,
		"issuer":           p.Issuer,
		"authorizationUrl": p.AuthorizationURL,
		"tokenUrl":         p.TokenURL,
		"jwksUrl":          p.JWKSURL,
	} {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("OIDC provider %q missing %s", p.ID, name)
		}
	}
	return nil
}

func providerByID(id string) (Provider, error) {
	providers, err := ProvidersFromEnv()
	if err != nil {
		return Provider{}, err
	}
	for _, p := range providers {
		if p.ID == id {
			return p, nil
		}
	}
	return Provider{}, sql.ErrNoRows
}

func stateCodec() StateCodec {
	key := os.Getenv("OIDC_STATE_SIGNING_KEY")
	if key == "" {
		key = authpkg.Config.TokenSigningKey
	}
	decoded, err := base64.StdEncoding.DecodeString(key)
	if err == nil && len(decoded) >= 32 {
		return StateCodec{Key: decoded}
	}
	return StateCodec{Key: []byte(key)}
}

func returnURL(r *http.Request) string {
	if v := r.URL.Query().Get("returnUrl"); v != "" {
		return v
	}
	return baseURL(r)
}

func baseURL(r *http.Request) string {
	scheme := r.Header.Get("X-Forwarded-Proto")
	if scheme == "" {
		if r.TLS != nil {
			scheme = "https"
		} else {
			scheme = "http"
		}
	}
	host := r.Header.Get("X-Forwarded-Host")
	if host == "" {
		host = r.Host
	}
	return scheme + "://" + host
}

func appendTokens(target, access, refresh string) (string, error) {
	if target == "" {
		target = "/"
	}
	u, err := url.Parse(target)
	if err != nil {
		return "", err
	}
	q := u.Query()
	q.Set("accessToken", access)
	q.Set("refreshToken", refresh)
	u.RawQuery = q.Encode()
	return u.String(), nil
}

func wantsJSON(r *http.Request) bool {
	return strings.EqualFold(r.URL.Query().Get("response"), "json") || strings.Contains(r.Header.Get("Accept"), "application/json")
}

func redirectURI(r *http.Request, p Provider) string {
	if p.RedirectURL != "" {
		return p.RedirectURL
	}
	scheme := r.Header.Get("X-Forwarded-Proto")
	if scheme == "" {
		if r.TLS != nil {
			scheme = "https"
		} else {
			scheme = "http"
		}
	}
	host := r.Header.Get("X-Forwarded-Host")
	if host == "" {
		host = r.Host
	}
	return scheme + "://" + host + "/login/oauth2/code/" + p.ID
}

func subjectFromUser(u *userpkg.TBUserRow) authpkg.Subject {
	return authpkg.Subject{
		UserID: u.ID, Email: u.Email, Authority: u.Authority,
		TenantID: u.TenantID, CustomerID: u.CustomerID, Enabled: u.Enabled,
	}
}

func claimAudienceContains(raw interface{}, want string) bool {
	switch aud := raw.(type) {
	case string:
		return aud == want
	case []string:
		for _, v := range aud {
			if v == want {
				return true
			}
		}
	case []interface{}:
		for _, v := range aud {
			if s, ok := v.(string); ok && s == want {
				return true
			}
		}
	}
	return false
}

func envBool(name string) bool {
	return strings.EqualFold(os.Getenv(name), "true") || os.Getenv(name) == "1"
}

func envDefault(name, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return fallback
}

func randomString(n int) string {
	buf := make([]byte, n)
	if _, err := rand.Read(buf); err != nil {
		return uuid.NewString()
	}
	return base64.RawURLEncoding.EncodeToString(buf)
}
