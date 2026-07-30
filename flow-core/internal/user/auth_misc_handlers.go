package user

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/google/uuid"
	"golang.org/x/crypto/bcrypt"

	dbpkg "flow-core/internal/db"
	"flow-core/internal/httputil"
)

// HandleResetPasswordByEmail POST /api/noauth/resetPasswordByEmail
//
// Body: {"email": "x@x"}
// Generates a reset_token in user_credentials and returns 200. In production
// this would email the user the token; we just log the link so dev/test can
// continue the flow.
func HandleResetPasswordByEmail(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Email string `json:"email"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		httputil.WriteError(w, http.StatusBadRequest, "Invalid JSON")
		return
	}
	if body.Email == "" {
		httputil.WriteError(w, http.StatusBadRequest, "Missing email")
		return
	}
	user, err := findUserByEmail(body.Email)
	if err != nil {
		// Don't leak whether the email exists. Always return 200 like TB does.
		w.WriteHeader(http.StatusOK)
		return
	}
	resetToken := uuid.New().String()
	resetExp := time.Now().Add(24 * time.Hour).UnixMilli()
	_, err = dbpkg.Pool.Exec(
		`UPDATE user_credentials SET reset_token = $1, reset_token_exp_time = $2 WHERE user_id = $3`,
		resetToken, resetExp, user.ID,
	)
	if err != nil {
		log.Printf("WARN reset token upsert: %v", err)
	}
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	link := fmt.Sprintf("%s://%s/login/resetPassword?resetToken=%s", scheme, r.Host, resetToken)
	log.Printf("Password reset requested for %s — link: %s", user.Email, link)
	w.WriteHeader(http.StatusOK)
}

// HandleResetPassword POST /api/noauth/resetPassword
//
// Body: {"resetToken": "...", "password": "newPwd"}
// Verifies the reset_token, updates the password hash, returns auth tokens.
func HandleResetPassword(w http.ResponseWriter, r *http.Request) {
	var body struct {
		ResetToken string `json:"resetToken"`
		Password   string `json:"password"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		httputil.WriteError(w, http.StatusBadRequest, "Invalid JSON")
		return
	}
	if body.ResetToken == "" || body.Password == "" {
		httputil.WriteError(w, http.StatusBadRequest, "Missing resetToken or password")
		return
	}
	var userId string
	var exp int64
	err := dbpkg.Pool.QueryRow(
		`SELECT user_id, COALESCE(reset_token_exp_time, 0) FROM user_credentials WHERE reset_token = $1`,
		body.ResetToken,
	).Scan(&userId, &exp)
	if err != nil {
		httputil.WriteError(w, http.StatusUnauthorized, "Invalid or expired reset token")
		return
	}
	if exp > 0 && exp < time.Now().UnixMilli() {
		httputil.WriteError(w, http.StatusUnauthorized, "Reset token expired")
		return
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(body.Password), bcrypt.DefaultCost)
	if err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, "Failed to hash password")
		return
	}
	_, err = dbpkg.Pool.Exec(
		`UPDATE user_credentials SET password = $1, reset_token = NULL, reset_token_exp_time = NULL,
		                            enabled = true, activate_token = NULL, activate_token_exp_time = NULL
		 WHERE user_id = $2`,
		string(hash), userId,
	)
	if err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, "Failed to update password")
		return
	}
	user, err := FindByID(userId)
	if err != nil {
		w.WriteHeader(http.StatusOK)
		return
	}
	sessionId := uuid.New().String()
	access, _ := generateAccessToken(user, sessionId)
	refresh, _ := generateRefreshToken(user, sessionId)
	httputil.WriteJSON(w, http.StatusOK, map[string]string{"token": access, "refreshToken": refresh})
}

// HandleActivate POST /api/noauth/activate
//
// Body: {"activateToken": "...", "password": "pwd"}
// Verifies the activate_token issued at user creation, sets the password, and
// flips enabled=true.
func HandleActivate(w http.ResponseWriter, r *http.Request) {
	var body struct {
		ActivateToken string `json:"activateToken"`
		Password      string `json:"password"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		httputil.WriteError(w, http.StatusBadRequest, "Invalid JSON")
		return
	}
	if body.ActivateToken == "" || body.Password == "" {
		httputil.WriteError(w, http.StatusBadRequest, "Missing activateToken or password")
		return
	}
	var userId string
	var exp int64
	err := dbpkg.Pool.QueryRow(
		`SELECT user_id, COALESCE(activate_token_exp_time, 0) FROM user_credentials WHERE activate_token = $1`,
		body.ActivateToken,
	).Scan(&userId, &exp)
	if err != nil {
		httputil.WriteError(w, http.StatusUnauthorized, "Invalid or expired activation token")
		return
	}
	if exp > 0 && exp < time.Now().UnixMilli() {
		httputil.WriteError(w, http.StatusUnauthorized, "Activation token expired")
		return
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(body.Password), bcrypt.DefaultCost)
	if err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, "Failed to hash password")
		return
	}
	_, err = dbpkg.Pool.Exec(
		`UPDATE user_credentials SET password = $1, enabled = true,
		                            activate_token = NULL, activate_token_exp_time = NULL
		 WHERE user_id = $2`,
		string(hash), userId,
	)
	if err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, "Failed to activate user")
		return
	}
	user, _ := FindByID(userId)
	sessionId := uuid.New().String()
	access, _ := generateAccessToken(user, sessionId)
	refresh, _ := generateRefreshToken(user, sessionId)
	httputil.WriteJSON(w, http.StatusOK, map[string]string{"token": access, "refreshToken": refresh})
}

// HandleResetPasswordCheck GET /api/noauth/resetPassword?resetToken=... — UI hits
// this to validate that the reset token in the URL is still valid before showing
// the password form.
func HandleResetPasswordCheck(w http.ResponseWriter, r *http.Request) {
	token := r.URL.Query().Get("resetToken")
	if token == "" {
		httputil.WriteError(w, http.StatusBadRequest, "Missing resetToken")
		return
	}
	var exp int64
	err := dbpkg.Pool.QueryRow(
		`SELECT COALESCE(reset_token_exp_time, 0) FROM user_credentials WHERE reset_token = $1`, token,
	).Scan(&exp)
	if err != nil {
		httputil.WriteError(w, http.StatusUnauthorized, "Invalid reset token")
		return
	}
	if exp > 0 && exp < time.Now().UnixMilli() {
		httputil.WriteError(w, http.StatusUnauthorized, "Reset token expired")
		return
	}
	w.WriteHeader(http.StatusOK)
}

// HandleActivateCheck GET /api/noauth/activate?activateToken=...
func HandleActivateCheck(w http.ResponseWriter, r *http.Request) {
	token := r.URL.Query().Get("activateToken")
	if token == "" {
		httputil.WriteError(w, http.StatusBadRequest, "Missing activateToken")
		return
	}
	var exp int64
	err := dbpkg.Pool.QueryRow(
		`SELECT COALESCE(activate_token_exp_time, 0) FROM user_credentials WHERE activate_token = $1`, token,
	).Scan(&exp)
	if err != nil {
		httputil.WriteError(w, http.StatusUnauthorized, "Invalid activation token")
		return
	}
	if exp > 0 && exp < time.Now().UnixMilli() {
		httputil.WriteError(w, http.StatusUnauthorized, "Activation token expired")
		return
	}
	w.WriteHeader(http.StatusOK)
}
