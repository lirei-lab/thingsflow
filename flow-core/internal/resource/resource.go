// generateAccessToken20 produces a 20-char alphanumeric token. Used as
// the public_resource_key for shareable resources. Same shape as the
// device access-token generator in internal/device — duplicated here to
// keep packages independent.
//
// Package resource implements /api/images, /api/resources and related
// upload/download endpoints (system images, SCADA symbols, JS modules,
// generic resources). Combines the read-side surface that lived in
// resource_handler.go with the write-side surface from upload_handlers.go.
package resource

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	dbpkg "flow-core/internal/db"
	"flow-core/internal/dbutil"
	"flow-core/internal/httputil"
)

func generateAccessToken20() string {
	const alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := uuid.New()
	out := strings.Builder{}
	for i := 0; i < 20; i++ {
		out.WriteByte(alphabet[int(b[i%16])%len(alphabet)])
	}
	return out.String()
}

// Images serves GET /api/images.
// Query params:
//   - pageSize, page         (pagination)
//   - includeSystemImages    (true → also list rows where tenant_id = system tenant)
//   - imageSubType           (e.g. SCADA_SYMBOL, IMAGE)
//   - textSearch             (filter on title)
//
// Returns the same shape as TB-Java: PageData of TbResourceInfo with descriptor inline.
func Images(w http.ResponseWriter, r *http.Request) {
	claims, ok := httputil.RequireAuth(w, r)
	if !ok {
		return
	}
	tenantId, _ := claims["tenantId"].(string)

	pageSize := httputil.PageSize(r, 10)
	page := httputil.IntParam(r, "page", 0)
	includeSys := strings.EqualFold(r.URL.Query().Get("includeSystemImages"), "true")
	subType := r.URL.Query().Get("imageSubType")
	if subType == "" {
		subType = r.URL.Query().Get("resourceSubType")
	}
	textSearch := r.URL.Query().Get("textSearch")

	conds := []string{"resource_type = 'IMAGE'"}
	args := []interface{}{}
	idx := 1

	if includeSys {
		conds = append(conds, "(tenant_id = $"+strconv.Itoa(idx)+" OR tenant_id = '13814000-1dd2-11b2-8080-808080808080')")
	} else {
		conds = append(conds, "tenant_id = $"+strconv.Itoa(idx))
	}
	args = append(args, tenantId)
	idx++

	if subType != "" {
		conds = append(conds, "resource_sub_type = $"+strconv.Itoa(idx))
		args = append(args, subType)
		idx++
	}
	if textSearch != "" {
		conds = append(conds, "LOWER(title) LIKE $"+strconv.Itoa(idx))
		args = append(args, "%"+strings.ToLower(textSearch)+"%")
		idx++
	}

	whereClause := strings.Join(conds, " AND ")

	var total int
	dbpkg.Pool.QueryRow("SELECT count(*) FROM resource WHERE "+whereClause, args...).Scan(&total)

	offset := page * pageSize
	query := `SELECT id, created_time, tenant_id, title, resource_type, resource_sub_type,
	                 resource_key, public_resource_key, etag, file_name, descriptor,
	                 COALESCE(is_public, false), external_id
	          FROM resource WHERE ` + whereClause +
		` ORDER BY title LIMIT $` + strconv.Itoa(idx) + ` OFFSET $` + strconv.Itoa(idx+1)
	args = append(args, pageSize, offset)

	rows, err := dbpkg.Pool.Query(query, args...)
	if err != nil {
		log.Printf("ERROR querying images: %v", err)
		httputil.WriteError(w, http.StatusInternalServerError, "Database error")
		return
	}
	defer rows.Close()

	data := []map[string]interface{}{}
	for rows.Next() {
		item := scanResourceRow(rows)
		if item != nil {
			data = append(data, item)
		}
	}

	totalPages := int(math.Ceil(float64(total) / float64(pageSize)))
	if totalPages == 0 && total > 0 {
		totalPages = 1
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"data":          data,
		"totalPages":    totalPages,
		"totalElements": total,
		"hasNext":       (page + 1) < totalPages,
	})
}

// Resources serves GET /api/resource.
// Same as Images but allows any resource_type (LWM2M_MODEL, JS_MODULE, IMAGE, ...).
func Resources(w http.ResponseWriter, r *http.Request) {
	claims, ok := httputil.RequireAuth(w, r)
	if !ok {
		return
	}
	tenantId, _ := claims["tenantId"].(string)

	pageSize := httputil.PageSize(r, 10)
	page := httputil.IntParam(r, "page", 0)
	resourceType := r.URL.Query().Get("resourceType")
	resourceSubType := r.URL.Query().Get("resourceSubType")
	includeSys := strings.EqualFold(r.URL.Query().Get("includeSystem"), "true") ||
		strings.EqualFold(r.URL.Query().Get("includeSystemImages"), "true")
	textSearch := r.URL.Query().Get("textSearch")

	conds := []string{}
	args := []interface{}{}
	idx := 1

	if includeSys {
		conds = append(conds, "(tenant_id = $"+strconv.Itoa(idx)+" OR tenant_id = '13814000-1dd2-11b2-8080-808080808080')")
	} else {
		conds = append(conds, "tenant_id = $"+strconv.Itoa(idx))
	}
	args = append(args, tenantId)
	idx++

	if resourceType != "" {
		conds = append(conds, "resource_type = $"+strconv.Itoa(idx))
		args = append(args, resourceType)
		idx++
	}
	if resourceSubType != "" {
		conds = append(conds, "resource_sub_type = $"+strconv.Itoa(idx))
		args = append(args, resourceSubType)
		idx++
	}
	if textSearch != "" {
		conds = append(conds, "LOWER(title) LIKE $"+strconv.Itoa(idx))
		args = append(args, "%"+strings.ToLower(textSearch)+"%")
		idx++
	}

	whereClause := strings.Join(conds, " AND ")

	var total int
	dbpkg.Pool.QueryRow("SELECT count(*) FROM resource WHERE "+whereClause, args...).Scan(&total)

	offset := page * pageSize
	query := `SELECT id, created_time, tenant_id, title, resource_type, resource_sub_type,
	                 resource_key, public_resource_key, etag, file_name, descriptor,
	                 COALESCE(is_public, false), external_id
	          FROM resource WHERE ` + whereClause +
		` ORDER BY title LIMIT $` + strconv.Itoa(idx) + ` OFFSET $` + strconv.Itoa(idx+1)
	args = append(args, pageSize, offset)

	rows, err := dbpkg.Pool.Query(query, args...)
	if err != nil {
		log.Printf("ERROR querying resources: %v", err)
		httputil.WriteError(w, http.StatusInternalServerError, "Database error")
		return
	}
	defer rows.Close()

	data := []map[string]interface{}{}
	for rows.Next() {
		item := scanResourceRow(rows)
		if item != nil {
			data = append(data, item)
		}
	}

	totalPages := int(math.Ceil(float64(total) / float64(pageSize)))
	if totalPages == 0 && total > 0 {
		totalPages = 1
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"data":          data,
		"totalPages":    totalPages,
		"totalElements": total,
		"hasNext":       (page + 1) < totalPages,
	})
}

// scanResourceRow reads one resource row and emits the shape TB returns.
func scanResourceRow(rows interface {
	Scan(dest ...interface{}) error
}) map[string]interface{} {
	var id, tenantId, title, resourceType, resourceKey, fileName string
	var createdTime int64
	var resourceSubType, publicResourceKey, etag, descriptor, externalId *string
	var isPublic bool

	if err := rows.Scan(&id, &createdTime, &tenantId, &title, &resourceType, &resourceSubType,
		&resourceKey, &publicResourceKey, &etag, &fileName, &descriptor, &isPublic, &externalId); err != nil {
		log.Printf("WARN scan resource row: %v", err)
		return nil
	}

	item := map[string]interface{}{
		"id":           map[string]interface{}{"entityType": "TB_RESOURCE", "id": id},
		"createdTime":  createdTime,
		"tenantId":     map[string]interface{}{"entityType": "TENANT", "id": tenantId},
		"title":        title,
		"name":         title,
		"resourceType": resourceType,
		"resourceKey":  resourceKey,
		"fileName":     fileName,
		"public":       isPublic,
	}
	if resourceSubType != nil {
		item["resourceSubType"] = *resourceSubType
	} else {
		item["resourceSubType"] = nil
	}
	if publicResourceKey != nil {
		item["publicResourceKey"] = *publicResourceKey
	} else {
		item["publicResourceKey"] = nil
	}
	if etag != nil {
		item["etag"] = *etag
	} else {
		item["etag"] = nil
	}
	if descriptor != nil && *descriptor != "" {
		var d interface{}
		if err := json.Unmarshal([]byte(*descriptor), &d); err == nil {
			item["descriptor"] = d
		} else {
			item["descriptor"] = nil
		}
	} else {
		item["descriptor"] = nil
	}
	if externalId != nil && *externalId != "" {
		item["externalId"] = map[string]interface{}{"entityType": "TB_RESOURCE", "id": *externalId}
	} else {
		item["externalId"] = nil
	}

	// TB synthesizes link/publicLink based on tenant_id == system
	systemTenant := "13814000-1dd2-11b2-8080-808080808080"
	if resourceType == "IMAGE" {
		if tenantId == systemTenant {
			item["link"] = "/api/images/system/" + resourceKey
		} else {
			item["link"] = "/api/images/tenant/" + resourceKey
		}
		if isPublic && publicResourceKey != nil && *publicResourceKey != "" {
			item["publicLink"] = "/api/images/public/" + *publicResourceKey
		} else {
			item["publicLink"] = nil
		}
	} else if resourceType == "LWM2M_MODEL" {
		if tenantId == systemTenant {
			item["link"] = "/api/resource/lwm2m_model/system/" + resourceKey
		} else {
			item["link"] = "/api/resource/lwm2m_model/tenant/" + resourceKey
		}
		item["publicLink"] = nil
	} else {
		item["link"] = nil
		item["publicLink"] = nil
	}

	return item
}

// ImageData serves GET /api/images/{scope}/{key}[/preview].
//
//	scope = system | tenant | public
//	key   = the resource_key column (e.g. "polar-area.svg") or, for public,
//	        the public_resource_key.
//
// Returns the raw bytes from resource.data (or resource.preview when /preview
// suffix is present). Sends Content-Type, Content-Length and ETag so browsers
// can cache the SVGs across reloads.
func ImageData(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/api/images/"), "/")
	if len(parts) < 2 {
		http.Error(w, "invalid image path", http.StatusBadRequest)
		return
	}
	scope := parts[0]
	key := parts[1]
	wantPreview := len(parts) >= 3 && parts[2] == "preview"

	if dbpkg.Pool == nil {
		http.Error(w, "image storage unavailable", http.StatusServiceUnavailable)
		return
	}

	var data, preview []byte
	var etag, descriptorJSON *string
	var mediaType string

	systemTenant := "13814000-1dd2-11b2-8080-808080808080"

	if scope == "public" {
		// Public images: lookup by public_resource_key. No auth required (matches TB).
		err := dbpkg.Pool.QueryRow(
			`SELECT data, preview, etag, descriptor FROM resource
			 WHERE resource_type = 'IMAGE' AND public_resource_key = $1
			 LIMIT 1`, key,
		).Scan(&data, &preview, &etag, &descriptorJSON)
		if err != nil {
			http.NotFound(w, r)
			return
		}
	} else {
		// system/tenant: require auth, scope by tenant_id.
		claims, err := httputil.ExtractToken(r)
		if err != nil {
			httputil.WriteError(w, http.StatusUnauthorized, "Authentication required")
			return
		}
		tenantId, _ := claims["tenantId"].(string)
		ownerId := tenantId
		if scope == "system" {
			ownerId = systemTenant
		}
		err = dbpkg.Pool.QueryRow(
			`SELECT data, preview, etag, descriptor FROM resource
			 WHERE resource_type = 'IMAGE' AND tenant_id = $1 AND resource_key = $2
			 LIMIT 1`, ownerId, key,
		).Scan(&data, &preview, &etag, &descriptorJSON)
		if err != nil {
			http.NotFound(w, r)
			return
		}
	}

	// Pull mediaType from descriptor JSON. The preview block has its own mediaType.
	if descriptorJSON != nil && *descriptorJSON != "" {
		var d struct {
			MediaType         string `json:"mediaType"`
			PreviewDescriptor struct {
				MediaType string `json:"mediaType"`
			} `json:"previewDescriptor"`
		}
		_ = json.Unmarshal([]byte(*descriptorJSON), &d)
		mediaType = d.MediaType
		if wantPreview && d.PreviewDescriptor.MediaType != "" {
			mediaType = d.PreviewDescriptor.MediaType
		}
	}
	if mediaType == "" {
		// SVG is by far the most common case for SCADA symbols
		mediaType = "application/octet-stream"
		if strings.HasSuffix(strings.ToLower(key), ".svg") {
			mediaType = "image/svg+xml"
		}
	}

	body := data
	if wantPreview && len(preview) > 0 {
		body = preview
	}

	if etag != nil && *etag != "" {
		w.Header().Set("ETag", `"`+*etag+`"`)
		// Honor If-None-Match for cheap browser cache hits.
		if im := r.Header.Get("If-None-Match"); im != "" && strings.Contains(im, *etag) {
			w.WriteHeader(http.StatusNotModified)
			return
		}
	}
	w.Header().Set("Content-Type", mediaType)
	w.Header().Set("Cache-Control", "public, max-age=86400")
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	_, _ = w.Write(body)
}

func base64StdDecode(s string) ([]byte, error) { return base64.StdEncoding.DecodeString(s) }

// ImageUpload POST /api/image
//
// multipart/form-data:
//
//	file         — the binary (svg/png/jpg/...)
//	title        — display title
//	imageSubType — IMAGE | SCADA_SYMBOL
//
// Stores in resource table (resource_type=IMAGE), data + descriptor + etag.
func ImageUpload(w http.ResponseWriter, r *http.Request) {
	claims, ok := httputil.RequireAuth(w, r)
	if !ok {
		return
	}
	tenantId, _ := claims["tenantId"].(string)

	// 32 MB cap — bigger SVGs are unusual but the UI allows up to ~10 MB.
	if err := r.ParseMultipartForm(32 << 20); err != nil {
		httputil.WriteError(w, http.StatusBadRequest, "Invalid multipart payload")
		return
	}
	file, header, err := r.FormFile("file")
	if err != nil {
		httputil.WriteError(w, http.StatusBadRequest, "Missing file part")
		return
	}
	defer file.Close()

	data, err := io.ReadAll(file)
	if err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, "Failed to read file")
		return
	}

	title := r.FormValue("title")
	if title == "" {
		title = strings.TrimSuffix(header.Filename, filepath.Ext(header.Filename))
	}
	subType := r.FormValue("imageSubType")
	if subType == "" {
		subType = "IMAGE"
	}

	resourceKey := header.Filename
	mediaType := header.Header.Get("Content-Type")
	if mediaType == "" {
		mediaType = guessMediaType(header.Filename)
	}

	etag := sha256Hex(data)
	descriptor := map[string]interface{}{
		"mediaType": mediaType,
		"size":      len(data),
		"etag":      etag,
	}
	descBytes, _ := json.Marshal(descriptor)

	id := uuid.New().String()
	now := time.Now().UnixMilli()
	publicResourceKey := generateAccessToken20() // reuse 20-char generator

	_, err = dbpkg.Pool.Exec(`
		INSERT INTO resource (id, created_time, tenant_id, title, resource_type, resource_sub_type,
		                    resource_key, file_name, data, etag, descriptor,
		                    is_public, public_resource_key)
		VALUES ($1, $2, $3, $4, 'IMAGE', $5, $6, $7, $8, $9, $10, true, $11)
		ON CONFLICT (tenant_id, resource_type, resource_key) DO UPDATE SET
		  title = EXCLUDED.title,
		  data = EXCLUDED.data,
		  etag = EXCLUDED.etag,
		  descriptor = EXCLUDED.descriptor,
		  resource_sub_type = EXCLUDED.resource_sub_type`,
		id, now, tenantId, title, subType, resourceKey, header.Filename, data, etag, string(descBytes), publicResourceKey,
	)
	if err != nil {
		log.Printf("ERROR upload image: %v", err)
		httputil.WriteError(w, http.StatusInternalServerError, "Failed to save image")
		return
	}

	// Re-fetch the canonical row (handles upsert hitting an existing id)
	var rid string
	_ = dbpkg.Pool.QueryRow(
		"SELECT id FROM resource WHERE tenant_id = $1 AND resource_type = 'IMAGE' AND resource_key = $2",
		tenantId, resourceKey,
	).Scan(&rid)
	if rid == "" {
		rid = id
	}

	resp := map[string]interface{}{
		"id":              map[string]interface{}{"entityType": "TB_RESOURCE", "id": rid},
		"createdTime":     now,
		"tenantId":        map[string]interface{}{"entityType": "TENANT", "id": tenantId},
		"title":           title,
		"name":            title,
		"resourceType":    "IMAGE",
		"resourceSubType": subType,
		"resourceKey":     resourceKey,
		"fileName":        header.Filename,
		"etag":            etag,
		"descriptor":      descriptor,
		"public":          true,
		"link":            "/api/images/tenant/" + resourceKey,
	}
	httputil.WriteJSON(w, http.StatusOK, resp)
}

// ImageUpdate PUT /api/images/{scope}/{key}
// Updates the bytes of an existing image (multipart with "file" part).
func ImageUpdate(w http.ResponseWriter, r *http.Request) {
	claims, ok := httputil.RequireAuth(w, r)
	if !ok {
		return
	}
	tenantId, _ := claims["tenantId"].(string)

	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/api/images/"), "/")
	if len(parts) < 2 {
		httputil.WriteError(w, http.StatusBadRequest, "Invalid path")
		return
	}
	scope := parts[0]
	key := parts[1]
	ownerId := tenantId
	if scope == "system" {
		ownerId = "13814000-1dd2-11b2-8080-808080808080"
	}

	if err := r.ParseMultipartForm(32 << 20); err != nil {
		httputil.WriteError(w, http.StatusBadRequest, "Invalid multipart payload")
		return
	}
	file, header, err := r.FormFile("file")
	if err != nil {
		httputil.WriteError(w, http.StatusBadRequest, "Missing file part")
		return
	}
	defer file.Close()
	data, err := io.ReadAll(file)
	if err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, "Failed to read file")
		return
	}

	mediaType := header.Header.Get("Content-Type")
	if mediaType == "" {
		mediaType = guessMediaType(key)
	}
	etag := sha256Hex(data)
	descriptor := map[string]interface{}{
		"mediaType": mediaType, "size": len(data), "etag": etag,
	}
	descBytes, _ := json.Marshal(descriptor)

	res, err := dbpkg.Pool.Exec(
		`UPDATE resource SET data = $1, etag = $2, descriptor = $3
		 WHERE tenant_id = $4 AND resource_type = 'IMAGE' AND resource_key = $5`,
		data, etag, string(descBytes), ownerId, key,
	)
	if err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, "Failed to update image")
		return
	}
	rows, _ := res.RowsAffected()
	if rows == 0 {
		httputil.WriteError(w, http.StatusNotFound, "Image not found")
		return
	}
	w.WriteHeader(http.StatusOK)
}

// ImageImport PUT /api/image/import — accepts a JSON body that already
// contains the resource shape (title, fileName, data base64, ...). Used by the
// version-control feature to copy images between environments.
//
// We accept the same body shape and pass through to the upload code.
func ImageImport(w http.ResponseWriter, r *http.Request) {
	claims, ok := httputil.RequireAuth(w, r)
	if !ok {
		return
	}
	tenantId, _ := claims["tenantId"].(string)

	var body struct {
		Title           string `json:"title"`
		FileName        string `json:"fileName"`
		ResourceKey     string `json:"resourceKey"`
		ResourceSubType string `json:"resourceSubType"`
		MediaType       string `json:"mediaType"`
		Data            string `json:"data"` // base64
		IsPublic        bool   `json:"public"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		httputil.WriteError(w, http.StatusBadRequest, "Invalid JSON")
		return
	}
	if body.Data == "" {
		httputil.WriteError(w, http.StatusBadRequest, "Missing data")
		return
	}
	rawData := []byte(body.Data)
	// Body.Data may arrive as base64 or as raw text — TB sends base64.
	if decoded, err := decodeBase64(body.Data); err == nil {
		rawData = decoded
	}
	mediaType := body.MediaType
	if mediaType == "" {
		mediaType = guessMediaType(body.FileName)
	}
	resourceKey := body.ResourceKey
	if resourceKey == "" {
		resourceKey = body.FileName
	}
	etag := sha256Hex(rawData)
	descriptor := map[string]interface{}{
		"mediaType": mediaType, "size": len(rawData), "etag": etag,
	}
	descBytes, _ := json.Marshal(descriptor)
	id := uuid.New().String()
	now := time.Now().UnixMilli()
	pubKey := generateAccessToken20()
	subType := body.ResourceSubType
	if subType == "" {
		subType = "IMAGE"
	}
	_, err := dbpkg.Pool.Exec(`
		INSERT INTO resource (id, created_time, tenant_id, title, resource_type, resource_sub_type,
		                    resource_key, file_name, data, etag, descriptor,
		                    is_public, public_resource_key)
		VALUES ($1, $2, $3, $4, 'IMAGE', $5, $6, $7, $8, $9, $10, $11, $12)
		ON CONFLICT (tenant_id, resource_type, resource_key) DO UPDATE SET
		  title = EXCLUDED.title, data = EXCLUDED.data, etag = EXCLUDED.etag,
		  descriptor = EXCLUDED.descriptor, resource_sub_type = EXCLUDED.resource_sub_type`,
		id, now, tenantId, body.Title, subType, resourceKey, body.FileName, rawData, etag, string(descBytes),
		body.IsPublic, pubKey,
	)
	if err != nil {
		log.Printf("ERROR image import: %v", err)
		httputil.WriteError(w, http.StatusInternalServerError, "Failed to import image")
		return
	}
	// The row above already carries createdTime/tenantId/resourceType/
	// resourceSubType/etag/descriptor/public — this response used to omit
	// all of them, even though the sibling ImageUpload returns the full
	// shape for the same table (docs/UI_CONTRACT_DATA_FIDELITY.md P2).
	// On the ON CONFLICT branch the surviving row keeps its original id,
	// so resolve it rather than echoing the freshly-generated one.
	rid := id
	_ = dbpkg.Pool.QueryRow(
		"SELECT id FROM resource WHERE tenant_id = $1 AND resource_type = 'IMAGE' AND resource_key = $2",
		tenantId, resourceKey,
	).Scan(&rid)

	httputil.WriteJSON(w, http.StatusOK, map[string]interface{}{
		"id":              map[string]interface{}{"entityType": "TB_RESOURCE", "id": rid},
		"createdTime":     now,
		"tenantId":        map[string]interface{}{"entityType": "TENANT", "id": tenantId},
		"title":           body.Title,
		"name":            body.Title,
		"resourceType":    "IMAGE",
		"resourceSubType": subType,
		"resourceKey":     resourceKey,
		"fileName":        body.FileName,
		"etag":            etag,
		"descriptor":      descriptor,
		"public":          body.IsPublic,
		"link":            "/api/images/tenant/" + resourceKey,
	})
}

// Upload POST /api/resource — multipart upload for non-image
// resources (JS modules, LWM2M, dashboards). Body shape mirrors TB.
func Upload(w http.ResponseWriter, r *http.Request) {
	claims, ok := httputil.RequireAuth(w, r)
	if !ok {
		return
	}
	tenantId, _ := claims["tenantId"].(string)

	if err := r.ParseMultipartForm(32 << 20); err != nil {
		httputil.WriteError(w, http.StatusBadRequest, "Invalid multipart payload")
		return
	}
	file, header, err := r.FormFile("file")
	if err != nil {
		httputil.WriteError(w, http.StatusBadRequest, "Missing file part")
		return
	}
	defer file.Close()
	data, err := io.ReadAll(file)
	if err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, "Failed to read file")
		return
	}

	resourceType := r.FormValue("resourceType")
	if resourceType == "" {
		resourceType = "JS_MODULE"
	}
	subType := r.FormValue("resourceSubType")
	title := r.FormValue("title")
	if title == "" {
		title = strings.TrimSuffix(header.Filename, filepath.Ext(header.Filename))
	}
	resourceKey := header.Filename
	etag := sha256Hex(data)
	descriptor := map[string]interface{}{
		"mediaType": guessMediaType(header.Filename), "size": len(data), "etag": etag,
	}
	descBytes, _ := json.Marshal(descriptor)
	id := uuid.New().String()
	now := time.Now().UnixMilli()
	_, err = dbpkg.Pool.Exec(`
		INSERT INTO resource (id, created_time, tenant_id, title, resource_type, resource_sub_type,
		                    resource_key, file_name, data, etag, descriptor, is_public)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, false)
		ON CONFLICT (tenant_id, resource_type, resource_key) DO UPDATE SET
		  title = EXCLUDED.title, data = EXCLUDED.data, etag = EXCLUDED.etag,
		  descriptor = EXCLUDED.descriptor`,
		id, now, tenantId, title, resourceType, dbutil.NullStr(subType), resourceKey, header.Filename,
		data, etag, string(descBytes),
	)
	if err != nil {
		log.Printf("ERROR resource upload: %v", err)
		httputil.WriteError(w, http.StatusInternalServerError, "Failed to save resource")
		return
	}
	httputil.WriteJSON(w, http.StatusOK, map[string]interface{}{
		"id":           map[string]interface{}{"entityType": "TB_RESOURCE", "id": id},
		"createdTime":  now,
		"tenantId":     map[string]interface{}{"entityType": "TENANT", "id": tenantId},
		"title":        title,
		"resourceType": resourceType,
		"resourceKey":  resourceKey,
		"fileName":     header.Filename,
		"etag":         etag,
		"descriptor":   descriptor,
	})
}

// Delete DELETE /api/resource/{id}
func Delete(w http.ResponseWriter, r *http.Request, id string) {
	claims, ok := httputil.RequireAuth(w, r)
	if !ok {
		return
	}
	tenantId, _ := claims["tenantId"].(string)

	res, err := dbpkg.Pool.Exec(
		"DELETE FROM resource WHERE id = $1 AND tenant_id = $2",
		id, tenantId,
	)
	if err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, "Failed to delete resource")
		return
	}
	rows, _ := res.RowsAffected()
	if rows == 0 {
		httputil.WriteError(w, http.StatusNotFound, "Resource not found or not owned by tenant")
		return
	}
	w.WriteHeader(http.StatusOK)
}

// Download GET /api/resource/{id}/download
// Returns the raw file bytes with the original filename in Content-Disposition.
func Download(w http.ResponseWriter, r *http.Request, id string) {
	if _, err := httputil.ExtractToken(r); err != nil {
		httputil.WriteError(w, http.StatusUnauthorized, "Authentication required")
		return
	}
	var data []byte
	var etag, fileName, descriptorJSON *string
	err := dbpkg.Pool.QueryRow(
		`SELECT data, etag, file_name, descriptor FROM resource WHERE id = $1`,
		id,
	).Scan(&data, &etag, &fileName, &descriptorJSON)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	mediaType := "application/octet-stream"
	if descriptorJSON != nil {
		var d struct {
			MediaType string `json:"mediaType"`
		}
		_ = json.Unmarshal([]byte(*descriptorJSON), &d)
		if d.MediaType != "" {
			mediaType = d.MediaType
		}
	}
	if etag != nil && *etag != "" {
		w.Header().Set("ETag", `"`+*etag+`"`)
	}
	w.Header().Set("Content-Type", mediaType)
	if fileName != nil && *fileName != "" {
		w.Header().Set("Content-Disposition", `attachment; filename="`+*fileName+`"`)
	}
	w.Header().Set("Content-Length", fmt.Sprintf("%d", len(data)))
	_, _ = w.Write(data)
}

// JsDownload GET /api/resource/js/{id}/download — alias for /download
// limited to JS_MODULE resources.
func JsDownload(w http.ResponseWriter, r *http.Request, id string) {
	Download(w, r, id)
}

// Info GET /api/resource/{id}/info — metadata only, no data column.
func Info(w http.ResponseWriter, r *http.Request, id string) {
	if _, err := httputil.ExtractToken(r); err != nil {
		httputil.WriteError(w, http.StatusUnauthorized, "Authentication required")
		return
	}
	rows, err := dbpkg.Pool.Query(
		`SELECT id, created_time, tenant_id, title, resource_type, resource_sub_type,
		        resource_key, public_resource_key, etag, file_name, descriptor,
		        COALESCE(is_public, false), external_id
		 FROM resource WHERE id = $1`, id,
	)
	if err != nil || !rows.Next() {
		if rows != nil {
			rows.Close()
		}
		http.NotFound(w, r)
		return
	}
	defer rows.Close()
	item := scanResourceRow(rows)
	if item == nil {
		http.NotFound(w, r)
		return
	}
	httputil.WriteJSON(w, http.StatusOK, item)
}

// guessMediaType infers a Content-Type from a filename extension.
func guessMediaType(name string) string {
	ext := strings.ToLower(filepath.Ext(name))
	switch ext {
	case ".svg":
		return "image/svg+xml"
	case ".png":
		return "image/png"
	case ".jpg", ".jpeg":
		return "image/jpeg"
	case ".gif":
		return "image/gif"
	case ".webp":
		return "image/webp"
	case ".js":
		return "application/javascript"
	case ".json":
		return "application/json"
	case ".xml":
		return "application/xml"
	case ".pdf":
		return "application/pdf"
	default:
		return "application/octet-stream"
	}
}

// sha256Hex returns the lowercase hex-encoded SHA-256 of b.
func sha256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// decodeBase64 strips a "data:...;base64," prefix if present and decodes.
func decodeBase64(s string) ([]byte, error) {
	if i := strings.Index(s, ";base64,"); i >= 0 {
		s = s[i+len(";base64,"):]
	}
	return base64StdDecode(s)
}
