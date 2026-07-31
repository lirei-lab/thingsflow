package alarmcomment

// HTTP handlers for /api/alarm/{alarmId}/comment(/[commentId]).
//
// Mirrors the surface of TB classic's AlarmCommentController: list (paged),
// create-or-update, and delete. Cross-tenant access is rejected by joining
// the lookup against alarm.tenant_id matching the JWT's tenantId.

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/google/uuid"

	dbpkg "flow-core/internal/db"
	"flow-core/internal/httputil"
)

type alarmCommentRow struct {
	ID          string                 `json:"id"`
	CreatedTime int64                  `json:"createdTime"`
	AlarmID     string                 `json:"alarmId"`
	UserID      *string                `json:"userId,omitempty"`
	Type        string                 `json:"type"`
	Comment     map[string]interface{} `json:"comment"`
	Name        *string                `json:"name,omitempty"`
}

// resolveAlarmTenantId returns the alarm's tenant_id, or "" if not found.
// Used for cross-tenant access checks.
func resolveAlarmTenantId(alarmId string) (string, bool) {
	var t string
	err := dbpkg.Pool.QueryRow("SELECT tenant_id::text FROM alarm WHERE id = $1", alarmId).Scan(&t)
	if err == sql.ErrNoRows {
		return "", false
	}
	if err != nil {
		log.Printf("WARN resolveAlarmTenantId(%s): %v", alarmId, err)
		return "", false
	}
	return t, true
}

// List — GET /api/alarm/{alarmId}/comment
func List(w http.ResponseWriter, r *http.Request, alarmId string) {
	claims, ok := httputil.RequireAuth(w, r)
	if !ok {
		return
	}
	jwtTenantId, _ := claims["tenantId"].(string)

	alarmTenantId, ok := resolveAlarmTenantId(alarmId)
	if !ok {
		httputil.WriteError(w, http.StatusNotFound, "Alarm not found")
		return
	}
	if alarmTenantId != jwtTenantId {
		httputil.WriteError(w, http.StatusForbidden, "Forbidden: alarm belongs to a different tenant")
		return
	}

	q := r.URL.Query()
	pageSize := httputil.PageSize(r, 10)
	page, _ := strconv.Atoi(q.Get("page"))
	if page < 0 {
		page = 0
	}
	offset := page * pageSize

	rows, err := dbpkg.Pool.Query(`
		SELECT ac.id::text, ac.created_time, ac.alarm_id::text,
		       COALESCE(ac.user_id::text, ''), ac.type, COALESCE(ac.comment, '{}'),
		       COALESCE(u.first_name || ' ' || u.last_name, u.email, '')
		  FROM alarm_comment ac
		  LEFT JOIN tb_user u ON u.id = ac.user_id
		 WHERE ac.alarm_id = $1
		 ORDER BY ac.created_time DESC
		 LIMIT $2 OFFSET $3`,
		alarmId, pageSize, offset,
	)
	if err != nil {
		log.Printf("ERROR alarm_comment list: %v", err)
		httputil.WriteError(w, http.StatusInternalServerError, "DB error")
		return
	}
	defer rows.Close()

	out := []alarmCommentRow{}
	for rows.Next() {
		var c alarmCommentRow
		var userID, name, commentStr string
		if err := rows.Scan(&c.ID, &c.CreatedTime, &c.AlarmID, &userID, &c.Type, &commentStr, &name); err != nil {
			log.Printf("WARN alarm_comment scan: %v", err)
			continue
		}
		if userID != "" {
			c.UserID = &userID
		}
		if name != "" {
			c.Name = &name
		}
		_ = json.Unmarshal([]byte(commentStr), &c.Comment)
		out = append(out, c)
	}

	// Count for pagination
	var total int
	_ = dbpkg.Pool.QueryRow("SELECT count(*) FROM alarm_comment WHERE alarm_id = $1", alarmId).Scan(&total)

	hasNext := (page+1)*pageSize < total
	totalPages := total / pageSize
	if total%pageSize != 0 {
		totalPages++
	}

	httputil.WriteJSON(w, http.StatusOK, map[string]interface{}{
		"data":          out,
		"totalPages":    totalPages,
		"totalElements": total,
		"hasNext":       hasNext,
	})
}

// Save — POST /api/alarm/{alarmId}/comment
// Body shape (TB-classic compatible):
//
//	{ "id": {"id": "<existing-uuid>"}, "comment": {"text": "..."}, "type": "OTHER" }
//
// `id` and `userId` in the body are ignored on create (TB convention) — we
// always set userId from the JWT.
func Save(w http.ResponseWriter, r *http.Request, alarmId string) {
	claims, ok := httputil.RequireAuth(w, r)
	if !ok {
		return
	}
	jwtTenantId, _ := claims["tenantId"].(string)
	jwtUserId, _ := claims["userId"].(string)

	alarmTenantId, ok := resolveAlarmTenantId(alarmId)
	if !ok {
		httputil.WriteError(w, http.StatusNotFound, "Alarm not found")
		return
	}
	if alarmTenantId != jwtTenantId {
		httputil.WriteError(w, http.StatusForbidden, "Forbidden: alarm belongs to a different tenant")
		return
	}

	var body map[string]interface{}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		httputil.WriteError(w, http.StatusBadRequest, "Invalid JSON body")
		return
	}

	commentObj, _ := body["comment"].(map[string]interface{})
	if commentObj == nil {
		commentObj = map[string]interface{}{}
	}
	commentJSON, _ := json.Marshal(commentObj)

	// Default to OTHER on create. SYSTEM is reserved for ack/clear/assign auto-comments.
	cType := "OTHER"
	if t, ok := body["type"].(string); ok && t != "" {
		cType = t
	}

	now := time.Now().UnixMilli()
	commentId := httputil.ExtractEntityID(body, "id")
	created := false

	var err error
	if commentId == "" {
		commentId = uuid.New().String()
		created = true
		_, err = dbpkg.Pool.Exec(`
			INSERT INTO alarm_comment (id, created_time, alarm_id, user_id, type, comment)
			VALUES ($1, $2, $3, $4, $5, $6)`,
			commentId, now, alarmId, jwtUserId, cType, string(commentJSON),
		)
	} else {
		// Update existing — only the comment body and (optionally) type.
		// TB classic also flips a `comment.edited` flag; we mirror that.
		var existing string
		if err := dbpkg.Pool.QueryRow(`SELECT comment FROM alarm_comment WHERE id = $1 AND alarm_id = $2`,
			commentId, alarmId).Scan(&existing); err != nil {
			httputil.WriteError(w, http.StatusNotFound, "Alarm comment not found")
			return
		}
		commentObj["edited"] = true
		commentJSON, _ = json.Marshal(commentObj)
		_, err = dbpkg.Pool.Exec(
			`UPDATE alarm_comment SET comment = $1, type = $2 WHERE id = $3 AND alarm_id = $4`,
			string(commentJSON), cType, commentId, alarmId,
		)
	}
	if err != nil {
		log.Printf("ERROR alarm_comment save: %v", err)
		httputil.WriteError(w, http.StatusInternalServerError, "DB error")
		return
	}

	if created {
		log.Printf("Alarm comment CREATED: id=%s alarm=%s tenant=%s", commentId, alarmId, jwtTenantId)
	} else {
		log.Printf("Alarm comment UPDATED: id=%s alarm=%s", commentId, alarmId)
	}

	httputil.WriteJSON(w, http.StatusOK, map[string]interface{}{
		"id":          map[string]string{"id": commentId, "entityType": "ALARM_COMMENT"},
		"createdTime": now,
		"alarmId":     map[string]string{"id": alarmId, "entityType": "ALARM"},
		"userId":      map[string]string{"id": jwtUserId, "entityType": "USER"},
		"type":        cType,
		"comment":     commentObj,
	})
}

// Delete — DELETE /api/alarm/{alarmId}/comment/{commentId}
func Delete(w http.ResponseWriter, r *http.Request, alarmId, commentId string) {
	claims, ok := httputil.RequireAuth(w, r)
	if !ok {
		return
	}
	jwtTenantId, _ := claims["tenantId"].(string)

	alarmTenantId, ok := resolveAlarmTenantId(alarmId)
	if !ok {
		httputil.WriteError(w, http.StatusNotFound, "Alarm not found")
		return
	}
	if alarmTenantId != jwtTenantId {
		httputil.WriteError(w, http.StatusForbidden, "Forbidden: alarm belongs to a different tenant")
		return
	}

	res, err := dbpkg.Pool.Exec(
		`DELETE FROM alarm_comment WHERE id = $1 AND alarm_id = $2`,
		commentId, alarmId,
	)
	if err != nil {
		log.Printf("ERROR alarm_comment delete: %v", err)
		httputil.WriteError(w, http.StatusInternalServerError, "DB error")
		return
	}
	rows, _ := res.RowsAffected()
	if rows == 0 {
		httputil.WriteError(w, http.StatusNotFound, "Alarm comment not found")
		return
	}
	log.Printf("Alarm comment DELETED: id=%s alarm=%s", commentId, alarmId)
	w.WriteHeader(http.StatusOK)
}

var _ = fmt.Sprintf
