package system

import (
	"database/sql"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"

	dbpkg "flow-core/internal/db"
	"flow-core/internal/httputil"
)

// User onboarding and 2FA endpoints.
//
// These share a constraint: this platform has no outbound mail transport. TB
// classic emails an activation link and a 2FA code; there is nothing here to
// send them with. Rather than pretend, each endpoint does the half it can do
// honestly — mint and return the activation link so an operator can deliver it
// out of band — and says plainly when the other half is unavailable.
//
// The alternative, accepting the request and returning 200, is worse: the
// operator believes an invitation went out and the user never receives one.

// HandleUserActivationLinkInfo — GET /api/user/{userId}/activationLinkInfo.
// Returns the link itself so it can be copied and sent by whatever channel the
// operator actually has.
func HandleUserActivationLinkInfo(w http.ResponseWriter, r *http.Request, userID string) {
	claims, ok := httputil.RequireAuth(w, r)
	if !ok {
		return
	}
	tenantID, _ := claims["tenantId"].(string)

	token, email, err := ensureActivationToken(userID, tenantID)
	if err != nil {
		httputil.WriteError(w, http.StatusNotFound, "User not found")
		return
	}
	httputil.WriteJSON(w, http.StatusOK, map[string]interface{}{
		"value":      activationURL(r, token),
		"ttlMs":      int64(24 * time.Hour / time.Millisecond),
		"email":      email,
		"emailSent":  false,
		"emailError": "no outbound mail transport is configured; deliver this link out of band",
	})
}

// HandleUserToken — GET /api/user/{userId}/token. TB lets a sysadmin impersonate
// a tenant user. Impersonation is refused here rather than half-built: issuing a
// token for another identity is the kind of capability that has to be designed
// with an audit trail, not added because an endpoint was missing.
func HandleUserToken(w http.ResponseWriter, r *http.Request, userID string) {
	if _, ok := httputil.RequireAuth(w, r); !ok {
		return
	}
	_ = userID
	httputil.WriteError(w, http.StatusNotImplemented,
		"User impersonation is not enabled on this platform")
}

// HandleSendActivationMail — POST /api/user/sendActivationMail.
func HandleSendActivationMail(w http.ResponseWriter, r *http.Request) {
	claims, ok := httputil.RequireAuth(w, r)
	if !ok {
		return
	}
	tenantID, _ := claims["tenantId"].(string)
	email := strings.TrimSpace(readEmail(r))
	if email == "" {
		httputil.WriteError(w, http.StatusBadRequest, "email is required")
		return
	}
	var userID string
	if err := dbpkg.Pool.QueryRow(
		"SELECT id::text FROM tb_user WHERE email = $1 AND tenant_id = $2", email, tenantID).
		Scan(&userID); err != nil {
		httputil.WriteError(w, http.StatusNotFound, "User not found")
		return
	}
	token, _, err := ensureActivationToken(userID, tenantID)
	if err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, "Failed to prepare activation")
		return
	}
	// 202: the token is real and usable, but nothing was emailed.
	httputil.WriteJSON(w, http.StatusAccepted, map[string]interface{}{
		"activationLink": activationURL(r, token),
		"emailSent":      false,
		"message":        "no outbound mail transport is configured; deliver this link out of band",
	})
}

// HandleResendEmailActivation — POST /api/noauth/resendEmailActivation.
// Unauthenticated by design, so it must not reveal whether an address exists:
// the response is identical either way.
func HandleResendEmailActivation(w http.ResponseWriter, r *http.Request) {
	_ = readEmail(r)
	httputil.WriteJSON(w, http.StatusOK, map[string]interface{}{
		"emailSent": false,
		"message":   "if the address exists, an administrator can supply its activation link",
	})
}

// HandleActivateByEmailCode — POST /api/noauth/activateByEmailCode.
// Consumes an activation token and enables the account.
func HandleActivateByEmailCode(w http.ResponseWriter, r *http.Request) {
	body := readJSONBody(r)
	code := strings.TrimSpace(firstString(body, "emailCode", "activateToken", "code"))
	if code == "" {
		httputil.WriteError(w, http.StatusBadRequest, "emailCode is required")
		return
	}
	var userID string
	var expiry sql.NullInt64
	err := dbpkg.Pool.QueryRow(
		`SELECT user_id::text, activate_token_exp_time FROM user_credentials WHERE activate_token = $1`,
		code).Scan(&userID, &expiry)
	if err != nil {
		httputil.WriteError(w, http.StatusNotFound, "Invalid or expired activation code")
		return
	}
	if expiry.Valid && expiry.Int64 > 0 && time.Now().UnixMilli() > expiry.Int64 {
		httputil.WriteError(w, http.StatusNotFound, "Invalid or expired activation code")
		return
	}
	// Single-use: clearing the token is what prevents the same link activating
	// twice, so it happens in the same statement that enables the account.
	if _, err := dbpkg.Pool.Exec(
		`UPDATE user_credentials SET enabled = true, activate_token = NULL,
		        activate_token_exp_time = NULL WHERE user_id = $1`, userID); err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, "Failed to activate user")
		return
	}
	httputil.WriteJSON(w, http.StatusOK, map[string]interface{}{"activated": true})
}

// ensureActivationToken returns a usable activation token for the user, minting
// one if none is pending. Reusing a live token keeps a re-sent invitation and
// the original pointing at the same link.
func ensureActivationToken(userID, tenantID string) (string, string, error) {
	var email string
	if err := dbpkg.Pool.QueryRow(
		"SELECT email FROM tb_user WHERE id = $1 AND tenant_id = $2", userID, tenantID).
		Scan(&email); err != nil {
		return "", "", err
	}
	var existing sql.NullString
	var exp sql.NullInt64
	_ = dbpkg.Pool.QueryRow(
		"SELECT activate_token, activate_token_exp_time FROM user_credentials WHERE user_id = $1",
		userID).Scan(&existing, &exp)
	now := time.Now().UnixMilli()
	if existing.Valid && existing.String != "" && (!exp.Valid || exp.Int64 == 0 || exp.Int64 > now) {
		return existing.String, email, nil
	}
	token := uuid.NewString()
	if _, err := dbpkg.Pool.Exec(
		`UPDATE user_credentials SET activate_token = $1, activate_token_exp_time = $2
		  WHERE user_id = $3`, token, now+int64(24*time.Hour/time.Millisecond), userID); err != nil {
		return "", "", err
	}
	return token, email, nil
}

func activationURL(r *http.Request, token string) string {
	scheme := "https"
	if r.TLS == nil && !strings.EqualFold(r.Header.Get("X-Forwarded-Proto"), "https") {
		scheme = "http"
	}
	return scheme + "://" + r.Host + "/login/createPassword?activateToken=" + token
}

// ─── 2FA ─────────────────────────────────────────────────────────────────────

// HandleTwoFaVerificationSend — POST /api/auth/2fa/verification/send.
// Delivery-based providers (email, SMS) need a transport this platform does not
// have. Saying so is better than a 200 that leaves the user waiting for a code.
func HandleTwoFaVerificationSend(w http.ResponseWriter, r *http.Request) {
	httputil.WriteError(w, http.StatusNotImplemented,
		"Two-factor code delivery requires a mail or SMS transport, which is not configured")
}

// HandleTwoFaVerificationCheck — POST /api/auth/2fa/verification/check.
func HandleTwoFaVerificationCheck(w http.ResponseWriter, r *http.Request) {
	httputil.WriteError(w, http.StatusNotImplemented,
		"Two-factor verification is not enabled on this platform")
}

// HandleTwoFaAccountConfigGenerate — POST /api/2fa/account/config/generate.
func HandleTwoFaAccountConfigGenerate(w http.ResponseWriter, r *http.Request) {
	if _, ok := httputil.RequireAuth(w, r); !ok {
		return
	}
	httputil.WriteError(w, http.StatusNotImplemented,
		"Two-factor enrolment is not enabled on this platform")
}

// HandleTwoFaAccountConfigSubmit — POST /api/2fa/account/config/submit.
func HandleTwoFaAccountConfigSubmit(w http.ResponseWriter, r *http.Request) {
	if _, ok := httputil.RequireAuth(w, r); !ok {
		return
	}
	httputil.WriteError(w, http.StatusNotImplemented,
		"Two-factor enrolment is not enabled on this platform")
}

// ─── helpers ─────────────────────────────────────────────────────────────────

func readJSONBody(r *http.Request) map[string]interface{} {
	raw, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil || len(raw) == 0 {
		return map[string]interface{}{}
	}
	var body map[string]interface{}
	if err := json.Unmarshal(raw, &body); err != nil {
		return map[string]interface{}{}
	}
	return body
}

func readEmail(r *http.Request) string {
	if v := strings.TrimSpace(r.URL.Query().Get("email")); v != "" {
		return v
	}
	return firstString(readJSONBody(r), "email")
}

func firstString(body map[string]interface{}, keys ...string) string {
	for _, k := range keys {
		if v, ok := body[k].(string); ok && strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
	}
	return ""
}

// HandlePublicLogin — POST /api/auth/login/public.
//
// TB issues an anonymous token scoped to the "public" customer so a shared
// dashboard link works without an account. That customer does not exist here,
// so such a token would carry a tenant and customer that resolve to nothing —
// a credential with undefined scope, which is worse than no credential.
func HandlePublicLogin(w http.ResponseWriter, r *http.Request) {
	httputil.WriteError(w, http.StatusNotImplemented,
		"Public dashboard access is not enabled on this platform")
}
