package user

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/google/uuid"
	"golang.org/x/crypto/bcrypt"

	"flow-core/internal/audit"
	dbpkg "flow-core/internal/db"
	"flow-core/internal/dbutil"
	"flow-core/internal/httputil"
	"flow-core/internal/quotas"
)

// HandleUserCreateOrUpdate processes POST /api/user
func HandleUserCreateOrUpdate(w http.ResponseWriter, r *http.Request) {
	claims, ok := httputil.RequireAuth(w, r)
	if !ok {
		return
	}
	tenantId, _ := claims["tenantId"].(string)
	authority, _ := claims["scopes"].([]interface{})
	isSysAdmin := false
	for _, s := range authority {
		if s == "SYS_ADMIN" {
			isSysAdmin = true
		}
	}

	var body map[string]interface{}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		httputil.WriteError(w, http.StatusBadRequest, "Invalid JSON")
		return
	}

	email, _ := body["email"].(string)
	if email == "" {
		httputil.WriteError(w, http.StatusBadRequest, "Missing email")
		return
	}

	userAuthority, _ := body["authority"].(string)
	if userAuthority == "" {
		userAuthority = "TENANT_ADMIN"
	}

	firstName, _ := body["firstName"].(string)
	lastName, _ := body["lastName"].(string)
	phone, _ := body["phone"].(string)
	additionalInfoJSON := dbutil.JSONOrNil(body["additionalInfo"])
	customerId := httputil.ExtractEntityID(body, "customerId")

	id := httputil.ExtractEntityID(body, "id")
	now := time.Now().UnixMilli()

	if id != "" {
		var existingTenant string
		if err := dbpkg.Pool.QueryRow("SELECT tenant_id FROM tb_user WHERE id = $1", id).Scan(&existingTenant); err != nil {
			httputil.WriteError(w, http.StatusNotFound, "User not found")
			return
		}
		if existingTenant != tenantId && !isSysAdmin {
			httputil.WriteError(w, http.StatusForbidden, "Cross-tenant update denied")
			return
		}
		_, err := dbpkg.Pool.Exec(`
			UPDATE tb_user
			SET email = $1, authority = $2, first_name = $3, last_name = $4,
			    phone = $5, additional_info = $6, customer_id = $7,
			    version = COALESCE(version, 1) + 1
			WHERE id = $8`,
			email, userAuthority, dbutil.NullStr(firstName), dbutil.NullStr(lastName),
			dbutil.NullStr(phone), additionalInfoJSON, dbutil.NullUUID(customerId), id)
		if err != nil {
			log.Printf("ERROR updating user %s: %v", id, err)
			httputil.WriteError(w, http.StatusInternalServerError, "Failed to update user")
			return
		}
		audit.EntityChange(claims, "USER", id, email, "UPDATED")
		user, _ := FindByID(id)
		httputil.WriteJSON(w, http.StatusOK, BuildResponse(user))
		return
	}

	// Create — generate activation token and credentials row
	if !quotas.Enforce(w, tenantId, "user") {
		return
	}
	id = uuid.New().String()
	_, err := dbpkg.Pool.Exec(`
		INSERT INTO tb_user (id, created_time, email, authority, tenant_id, customer_id,
		                    first_name, last_name, phone, additional_info, version)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 1)`,
		id, now, email, userAuthority, tenantId, dbutil.NullUUID(customerId),
		dbutil.NullStr(firstName), dbutil.NullStr(lastName), dbutil.NullStr(phone), additionalInfoJSON)
	if err != nil {
		log.Printf("ERROR creating user: %v", err)
		httputil.WriteError(w, http.StatusInternalServerError, "Failed to create user")
		return
	}

	activateToken := uuid.New().String()
	activateExp := time.Now().Add(24 * time.Hour).UnixMilli()
	_, err = dbpkg.Pool.Exec(`
		INSERT INTO user_credentials (id, created_time, user_id, enabled, activate_token, activate_token_exp_time)
		VALUES ($1, $2, $3, false, $4, $5)`,
		uuid.New().String(), now, id, activateToken, activateExp)
	if err != nil {
		log.Printf("WARN creating user_credentials for %s: %v", id, err)
	}

	audit.EntityChange(claims, "USER", id, email, "ADDED")
	user, _ := FindByID(id)
	httputil.WriteJSON(w, http.StatusOK, BuildResponse(user))
}

// HandleUserDelete processes DELETE /api/user/{id}
func HandleUserDelete(w http.ResponseWriter, r *http.Request, userId string) {
	claims, ok := httputil.RequireAuth(w, r)
	if !ok {
		return
	}
	tenantId, _ := claims["tenantId"].(string)

	var existingTenant, existingEmail string
	if err := dbpkg.Pool.QueryRow("SELECT tenant_id, email FROM tb_user WHERE id = $1", userId).Scan(&existingTenant, &existingEmail); err != nil {
		httputil.WriteError(w, http.StatusNotFound, "User not found")
		return
	}
	if existingTenant != tenantId {
		httputil.WriteError(w, http.StatusForbidden, "Cross-tenant delete denied")
		return
	}

	tx, err := dbpkg.Pool.Begin()
	if err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, "Failed to begin transaction")
		return
	}
	defer tx.Rollback()

	tx.Exec("DELETE FROM user_credentials WHERE user_id = $1", userId)
	tx.Exec("DELETE FROM user_settings WHERE user_id = $1", userId)
	if _, err := tx.Exec("DELETE FROM tb_user WHERE id = $1", userId); err != nil {
		log.Printf("ERROR deleting user %s: %v", userId, err)
		httputil.WriteError(w, http.StatusInternalServerError, "Failed to delete user")
		return
	}
	if err := tx.Commit(); err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, "Failed to commit deletion")
		return
	}
	audit.EntityChange(claims, "USER", userId, existingEmail, "DELETED")
	w.WriteHeader(http.StatusOK)
}

// HandleChangePassword processes POST /api/auth/changePassword
func HandleChangePassword(w http.ResponseWriter, r *http.Request) {
	claims, ok := httputil.RequireAuth(w, r)
	if !ok {
		return
	}
	userId, _ := claims["userId"].(string)
	if userId == "" {
		httputil.WriteError(w, http.StatusUnauthorized, "Invalid token")
		return
	}

	var body struct {
		CurrentPassword string `json:"currentPassword"`
		NewPassword     string `json:"newPassword"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		httputil.WriteError(w, http.StatusBadRequest, "Invalid JSON")
		return
	}
	if body.NewPassword == "" {
		httputil.WriteError(w, http.StatusBadRequest, "Missing newPassword")
		return
	}

	var currentHash string
	if err := dbpkg.Pool.QueryRow("SELECT password FROM user_credentials WHERE user_id = $1", userId).Scan(&currentHash); err != nil {
		httputil.WriteError(w, http.StatusNotFound, "User credentials not found")
		return
	}

	if currentHash != "" {
		if err := bcrypt.CompareHashAndPassword([]byte(currentHash), []byte(body.CurrentPassword)); err != nil {
			httputil.WriteError(w, http.StatusUnauthorized, "Invalid current password")
			return
		}
	}

	newHash, err := bcrypt.GenerateFromPassword([]byte(body.NewPassword), bcrypt.DefaultCost)
	if err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, "Failed to hash password")
		return
	}

	if _, err := dbpkg.Pool.Exec(
		"UPDATE user_credentials SET password = $1, reset_token = NULL, reset_token_exp_time = NULL WHERE user_id = $2",
		string(newHash), userId,
	); err != nil {
		log.Printf("ERROR updating password for user %s: %v", userId, err)
		httputil.WriteError(w, http.StatusInternalServerError, "Failed to update password")
		return
	}

	user, err := FindByID(userId)
	if err != nil {
		w.WriteHeader(http.StatusOK)
		return
	}
	sessionId := uuid.New().String()
	accessToken, _ := generateAccessToken(user, sessionId)
	refreshToken, _ := generateRefreshToken(user, sessionId)
	httputil.WriteJSON(w, http.StatusOK, map[string]string{
		"token":        accessToken,
		"refreshToken": refreshToken,
	})
}

// HandleLogout processes POST /api/auth/logout. ThingsBoard treats logout as
// a client-side token-discard operation; we just acknowledge.
func HandleLogout(w http.ResponseWriter, r *http.Request) {
	claims, ok := httputil.RequireAuth(w, r)
	if !ok {
		return
	}
	tenantId, _ := claims["tenantId"].(string)
	userId, _ := claims["userId"].(string)
	userName, _ := claims["sub"].(string)
	audit.Write(audit.Event{
		TenantID:   tenantId,
		UserID:     userId,
		UserName:   userName,
		EntityID:   userId,
		EntityType: "USER",
		EntityName: userName,
		ActionType: "LOGOUT",
		Status:     "SUCCESS",
	})
	w.WriteHeader(http.StatusOK)
}

// HandleUserActivationLink GET /api/user/{userId}/activationLink — returns activation URL
func HandleUserActivationLink(w http.ResponseWriter, r *http.Request, userId string) {
	if _, err := httputil.ExtractToken(r); err != nil {
		httputil.WriteError(w, http.StatusUnauthorized, "Authentication required")
		return
	}

	var token string
	if err := dbpkg.Pool.QueryRow("SELECT activate_token FROM user_credentials WHERE user_id = $1", userId).Scan(&token); err != nil {
		httputil.WriteError(w, http.StatusNotFound, "Activation token not found")
		return
	}
	host := r.Host
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	link := fmt.Sprintf("%s://%s/api/noauth/activate?activateToken=%s", scheme, host, token)
	w.Header().Set("Content-Type", "text/plain")
	w.Write([]byte(link))
}
