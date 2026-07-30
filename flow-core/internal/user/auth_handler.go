package user

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"golang.org/x/crypto/bcrypt"

	"flow-core/internal/audit"
	authpkg "flow-core/internal/auth"
	dbpkg "flow-core/internal/db"
	"flow-core/internal/httputil"
	"flow-core/internal/throttle"
)

// JWT primitives moved to internal/auth. Backwards-compat aliases below
// keep callers in package main compiling without import churn.
func InitJWTConfig() { authpkg.InitConfig() }

// TBUserRow represents a user record from the tb_user + user_credentials join.
type TBUserRow struct {
	ID             string
	CreatedTime    int64
	Email          string
	Authority      string
	TenantID       string
	CustomerID     string
	FirstName      *string
	LastName       *string
	Phone          *string
	AdditionalInfo *string
	PasswordHash   string
	Enabled        bool
}

// HandleLogin processes POST /api/auth/login
func HandleLogin(w http.ResponseWriter, r *http.Request) {
	if r.Method == "OPTIONS" {
		w.WriteHeader(http.StatusOK)
		return
	}

	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Brute-force throttle: gate the bcrypt cost (~100ms per attempt) by
	// counting recent failures per IP. No external dependency — in-memory
	// counter scoped to this process. Behind a load balancer set
	// LOGIN_THROTTLE_TRUST_PROXY=true so we read X-Forwarded-For instead.
	clientIP := throttle.ClientIP(r)
	if !throttle.Allow(clientIP) {
		w.Header().Set("Retry-After", "60")
		slog.Warn("login throttled", "client_ip", clientIP, "rid", r.Header.Get("X-Request-ID"))
		httputil.WriteError(w, http.StatusTooManyRequests, "Too many failed login attempts — try again in 60 seconds")
		return
	}

	var loginReq struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}
	if err := json.NewDecoder(r.Body).Decode(&loginReq); err != nil {
		httputil.WriteError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	if loginReq.Username == "" || loginReq.Password == "" {
		httputil.WriteError(w, http.StatusBadRequest, "Username and password are required")
		return
	}

	// Look up user by email
	user, err := findUserByEmail(loginReq.Username)
	if err != nil {
		throttle.RecordFail(clientIP)
		slog.Warn("login failed: user lookup",
			"username", loginReq.Username,
			"client_ip", clientIP,
			"rid", r.Header.Get("X-Request-ID"),
			"err", err.Error())
		httputil.WriteError(w, http.StatusUnauthorized, "Invalid email or password")
		return
	}

	if !user.Enabled {
		audit.Write(audit.Event{
			TenantID:   user.TenantID,
			UserID:     user.ID,
			UserName:   user.Email,
			EntityID:   user.ID,
			EntityType: "USER",
			EntityName: user.Email,
			ActionType: "LOGIN",
			Status:     "FAILURE",
			Failure:    "User account is disabled",
		})
		httputil.WriteError(w, http.StatusUnauthorized, "User account is disabled")
		return
	}

	// Verify password
	if err := bcrypt.CompareHashAndPassword([]byte(user.PasswordHash), []byte(loginReq.Password)); err != nil {
		throttle.RecordFail(clientIP)
		slog.Warn("login failed: bad password",
			"username", loginReq.Username,
			"client_ip", clientIP,
			"rid", r.Header.Get("X-Request-ID"))
		audit.Write(audit.Event{
			TenantID:   user.TenantID,
			UserID:     user.ID,
			UserName:   user.Email,
			EntityID:   user.ID,
			EntityType: "USER",
			EntityName: user.Email,
			ActionType: "LOGIN",
			Status:     "FAILURE",
			Failure:    "Invalid password",
		})
		httputil.WriteError(w, http.StatusUnauthorized, "Invalid email or password")
		return
	}
	throttle.RecordSuccess(clientIP)
	audit.Write(audit.Event{
		TenantID:   user.TenantID,
		UserID:     user.ID,
		UserName:   user.Email,
		EntityID:   user.ID,
		EntityType: "USER",
		EntityName: user.Email,
		ActionType: "LOGIN",
		Status:     "SUCCESS",
	})

	// Generate tokens
	sessionId := uuid.New().String()
	accessToken, err := generateAccessToken(user, sessionId)
	if err != nil {
		log.Printf("ERROR generating access token: %v", err)
		httputil.WriteError(w, http.StatusInternalServerError, "Internal server error")
		return
	}

	refreshToken, err := generateRefreshToken(user, sessionId)
	if err != nil {
		log.Printf("ERROR generating refresh token: %v", err)
		httputil.WriteError(w, http.StatusInternalServerError, "Internal server error")
		return
	}

	// Update last_login_ts
	_, _ = dbpkg.Pool.Exec("UPDATE user_credentials SET last_login_ts = $1 WHERE user_id = $2",
		time.Now().UnixMilli(), user.ID)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"token":        accessToken,
		"refreshToken": refreshToken,
	})

	log.Printf("Login successful: %s (authority: %s)", user.Email, user.Authority)
}

// HandleAuthUser processes GET /api/auth/user — returns current user info from JWT.
func HandleAuthUser(w http.ResponseWriter, r *http.Request) {
	claims, err := httputil.ExtractToken(r)
	if err != nil {
		httputil.WriteError(w, http.StatusUnauthorized, "Invalid or expired token")
		return
	}

	userId, _ := claims["userId"].(string)
	if userId == "" {
		httputil.WriteError(w, http.StatusUnauthorized, "Invalid token claims")
		return
	}

	// Fetch user from DB for fresh data
	user, err := FindByID(userId)
	if err != nil {
		httputil.WriteError(w, http.StatusNotFound, "User not found")
		return
	}

	// Build the response in exact TB format
	response := BuildResponse(user)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// HandleTokenRefresh processes POST /api/auth/token — refreshes the access token.
func HandleTokenRefresh(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var refreshReq struct {
		RefreshToken string `json:"refreshToken"`
	}
	if err := json.NewDecoder(r.Body).Decode(&refreshReq); err != nil {
		httputil.WriteError(w, http.StatusBadRequest, "Invalid request body")
		return
	}

	// Parse and validate the refresh token via the shared package — same
	// HS512 + signing key used elsewhere.
	claimsMap, err := authpkg.ParseAndValidate(refreshReq.RefreshToken)
	if err != nil {
		httputil.WriteError(w, http.StatusUnauthorized, "Invalid or expired refresh token")
		return
	}

	// Verify this is a refresh token
	scopes, _ := claimsMap["scopes"].([]interface{})
	isRefreshToken := false
	for _, s := range scopes {
		if s == "REFRESH_TOKEN" {
			isRefreshToken = true
			break
		}
	}
	if !isRefreshToken {
		httputil.WriteError(w, http.StatusUnauthorized, "Token is not a refresh token")
		return
	}

	userId, _ := claimsMap["userId"].(string)
	user, err := FindByID(userId)
	if err != nil || !user.Enabled {
		httputil.WriteError(w, http.StatusUnauthorized, "User not found or disabled")
		return
	}

	sessionId := uuid.New().String()
	accessToken, err := generateAccessToken(user, sessionId)
	if err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, "Failed to generate token")
		return
	}

	newRefreshToken, err := generateRefreshToken(user, sessionId)
	if err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, "Failed to generate refresh token")
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"token":        accessToken,
		"refreshToken": newRefreshToken,
	})
}

// ─── Internal helpers ───────────────────────────────────────────────────────

func findUserByEmail(email string) (*TBUserRow, error) {
	if dbpkg.Pool == nil {
		return nil, fmt.Errorf("database not available")
	}

	var user TBUserRow
	err := dbpkg.Pool.QueryRow(`
		SELECT u.id, u.created_time, u.email, u.authority,
		       COALESCE(u.tenant_id::text, ''),
		       COALESCE(u.customer_id::text, ''),
		       u.first_name, u.last_name, u.phone, u.additional_info,
		       c.password, c.enabled
		FROM tb_user u
		JOIN user_credentials c ON c.user_id = u.id
		WHERE u.email = $1`, email).Scan(
		&user.ID, &user.CreatedTime, &user.Email, &user.Authority,
		&user.TenantID, &user.CustomerID,
		&user.FirstName, &user.LastName, &user.Phone, &user.AdditionalInfo,
		&user.PasswordHash, &user.Enabled,
	)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("user not found: %s", email)
	}
	if err != nil {
		return nil, fmt.Errorf("query error: %w", err)
	}
	return &user, nil
}

func FindByEmail(email string) (*TBUserRow, error) {
	return findUserByEmail(email)
}

func FindByID(id string) (*TBUserRow, error) {
	if dbpkg.Pool == nil {
		return nil, fmt.Errorf("database not available")
	}

	var user TBUserRow
	err := dbpkg.Pool.QueryRow(`
		SELECT u.id, u.created_time, u.email, u.authority,
		       COALESCE(u.tenant_id::text, ''),
		       COALESCE(u.customer_id::text, ''),
		       u.first_name, u.last_name, u.phone, u.additional_info,
		       COALESCE(c.password, ''), COALESCE(c.enabled, false)
		FROM tb_user u
		JOIN user_credentials c ON c.user_id = u.id
		WHERE u.id = $1`, id).Scan(
		&user.ID, &user.CreatedTime, &user.Email, &user.Authority,
		&user.TenantID, &user.CustomerID,
		&user.FirstName, &user.LastName, &user.Phone, &user.AdditionalInfo,
		&user.PasswordHash, &user.Enabled,
	)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("user not found: %s", id)
	}
	if err != nil {
		return nil, fmt.Errorf("query error: %w", err)
	}
	return &user, nil
}

func generateAccessToken(user *TBUserRow, sessionId string) (string, error) {
	return authpkg.GenerateAccess(userToSubject(user), sessionId)
}

func generateRefreshToken(user *TBUserRow, sessionId string) (string, error) {
	return authpkg.GenerateRefresh(userToSubject(user), sessionId)
}

func userToSubject(u *TBUserRow) authpkg.Subject {
	return authpkg.Subject{
		UserID: u.ID, Email: u.Email, Authority: u.Authority,
		TenantID: u.TenantID, CustomerID: u.CustomerID, Enabled: u.Enabled,
	}
}

func validateAndParseClaims(tokenString string) (jwt.MapClaims, error) {
	return authpkg.ParseAndValidate(tokenString)
}

func BuildResponse(user *TBUserRow) map[string]interface{} {
	resp := map[string]interface{}{
		"id": map[string]interface{}{
			"entityType": "USER",
			"id":         user.ID,
		},
		"createdTime": user.CreatedTime,
		"tenantId": map[string]interface{}{
			"entityType": "TENANT",
			"id":         user.TenantID,
		},
		"customerId": map[string]interface{}{
			"entityType": "CUSTOMER",
			"id":         user.CustomerID,
		},
		"email":     user.Email,
		"authority": user.Authority,
		"name":      user.Email, // TB uses email as name when first/last are null
		"version":   1,
	}

	if user.FirstName != nil {
		resp["firstName"] = *user.FirstName
	} else {
		resp["firstName"] = nil
	}
	if user.LastName != nil {
		resp["lastName"] = *user.LastName
	} else {
		resp["lastName"] = nil
	}
	if user.Phone != nil {
		resp["phone"] = *user.Phone
	} else {
		resp["phone"] = nil
	}

	if user.AdditionalInfo != nil && *user.AdditionalInfo != "" {
		var info interface{}
		if err := json.Unmarshal([]byte(*user.AdditionalInfo), &info); err == nil {
			resp["additionalInfo"] = info
		} else {
			resp["additionalInfo"] = nil
		}
	} else {
		resp["additionalInfo"] = nil
	}

	return resp
}
