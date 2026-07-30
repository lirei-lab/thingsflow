package ota

// HTTP handlers for /api/otaPackage(s)/...
//
// Surface mirrored from TB classic's OtaPackageController:
//   GET    /api/otaPackages?pageSize=&page=&textSearch=
//   GET    /api/otaPackage/info/{id}     (no data)
//   GET    /api/otaPackage/{id}          (with data — sent as base64 in legacy)
//   GET    /api/otaPackage/{id}/download (raw bytes)
//   POST   /api/otaPackage               (create/update info)
//   POST   /api/otaPackage/{id}          (multipart upload of binary)
//   DELETE /api/otaPackage/{id}
//
// Storage: ota_package.data is bytea (see 10_ota-package-bytea.sql). The
// bridge owns the full lifecycle in postgres — no separate object store.
// Tenant scoping is enforced on every read and write.

import (
	"crypto/md5"
	"crypto/sha256"
	"crypto/sha512"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	dbpkg "flow-core/internal/db"
	"flow-core/internal/dbutil"
	"flow-core/internal/httputil"
)

// --- helpers ---------------------------------------------------------------

func otaInfoMap(id, title, version, ptype, tag, fileName, contentType, checksumAlg, checksum, url, deviceProfileId, additionalInfo, externalId string, createdTime, dataSize int64) map[string]interface{} {
	m := map[string]interface{}{
		"id":          map[string]string{"entityType": "OTA_PACKAGE", "id": id},
		"createdTime": createdTime,
		"title":       title,
		"version":     version,
		"type":        ptype,
		"hasData":     dataSize > 0,
		"dataSize":    dataSize,
	}
	if tag != "" {
		m["tag"] = tag
	}
	if fileName != "" {
		m["fileName"] = fileName
	}
	if contentType != "" {
		m["contentType"] = contentType
	}
	if checksumAlg != "" {
		m["checksumAlgorithm"] = checksumAlg
	}
	if checksum != "" {
		m["checksum"] = checksum
	}
	if url != "" {
		m["url"] = url
	}
	if deviceProfileId != "" {
		m["deviceProfileId"] = map[string]string{"entityType": "DEVICE_PROFILE", "id": deviceProfileId}
	}
	if externalId != "" {
		m["externalId"] = map[string]string{"entityType": "OTA_PACKAGE", "id": externalId}
	}
	if additionalInfo != "" {
		var parsed interface{}
		if json.Unmarshal([]byte(additionalInfo), &parsed) == nil {
			m["additionalInfo"] = parsed
		}
	} else {
		m["additionalInfo"] = nil
	}
	return m
}

// requireTenantAdmin returns claims if scope contains TENANT_ADMIN.
func requireTenantAdmin(w http.ResponseWriter, r *http.Request) (map[string]interface{}, bool) {
	claims, err := httputil.ExtractToken(r)
	if err != nil {
		httputil.WriteError(w, http.StatusUnauthorized, "Authentication required")
		return nil, false
	}
	scopes, _ := claims["scopes"].([]interface{})
	for _, s := range scopes {
		if str, ok := s.(string); ok && (str == "TENANT_ADMIN" || str == "SYS_ADMIN") {
			return claims, true
		}
	}
	httputil.WriteError(w, http.StatusForbidden, "Forbidden: TENANT_ADMIN scope required")
	return nil, false
}

// --- list ------------------------------------------------------------------

// List — GET /api/otaPackages
func List(w http.ResponseWriter, r *http.Request) {
	claims, ok := requireTenantAdmin(w, r)
	if !ok {
		return
	}
	tenantId, _ := claims["tenantId"].(string)

	q := r.URL.Query()
	pageSize, _ := strconv.Atoi(q.Get("pageSize"))
	if pageSize <= 0 {
		pageSize = 10
	}
	page, _ := strconv.Atoi(q.Get("page"))
	if page < 0 {
		page = 0
	}
	textSearch := strings.TrimSpace(q.Get("textSearch"))

	args := []interface{}{tenantId}
	where := " WHERE tenant_id = $1 "
	if textSearch != "" {
		args = append(args, "%"+textSearch+"%")
		where += fmt.Sprintf(" AND LOWER(title) LIKE LOWER($%d) ", len(args))
	}

	var total int
	if err := dbpkg.Pool.QueryRow("SELECT count(*) FROM ota_package"+where, args...).Scan(&total); err != nil {
		log.Printf("ERROR ota count: %v", err)
		httputil.WriteError(w, http.StatusInternalServerError, "DB error")
		return
	}
	args = append(args, pageSize, page*pageSize)
	limit := fmt.Sprintf(" ORDER BY created_time DESC LIMIT $%d OFFSET $%d", len(args)-1, len(args))

	rows, err := dbpkg.Pool.Query(`
		SELECT id::text, created_time, title, version, type,
		       COALESCE(tag,''), COALESCE(file_name,''), COALESCE(content_type,''),
		       COALESCE(checksum_algorithm,''), COALESCE(checksum,''),
		       COALESCE(url,''),
		       COALESCE(device_profile_id::text,''), COALESCE(additional_info,''),
		       COALESCE(external_id::text,''), COALESCE(data_size, 0)
		  FROM ota_package`+where+limit, args...)
	if err != nil {
		log.Printf("ERROR ota list: %v", err)
		httputil.WriteError(w, http.StatusInternalServerError, "DB error")
		return
	}
	defer rows.Close()

	data := []map[string]interface{}{}
	for rows.Next() {
		var id, title, ver, ptype, tag, fileName, contentType, checksumAlg, checksum, url, deviceProfileId, additionalInfo, externalId string
		var createdTime, dataSize int64
		if err := rows.Scan(&id, &createdTime, &title, &ver, &ptype, &tag, &fileName, &contentType, &checksumAlg, &checksum, &url, &deviceProfileId, &additionalInfo, &externalId, &dataSize); err != nil {
			log.Printf("WARN ota scan: %v", err)
			continue
		}
		data = append(data, otaInfoMap(id, title, ver, ptype, tag, fileName, contentType, checksumAlg, checksum, url, deviceProfileId, additionalInfo, externalId, createdTime, dataSize))
	}

	totalPages := total / pageSize
	if total%pageSize != 0 {
		totalPages++
	}
	httputil.WriteJSON(w, http.StatusOK, map[string]interface{}{
		"data":          data,
		"totalElements": total,
		"totalPages":    totalPages,
		"hasNext":       (page+1)*pageSize < total,
	})
}

// --- get-by-id -------------------------------------------------------------

func loadOtaInfo(tenantId, id string) (map[string]interface{}, error) {
	var title, ver, ptype, tag, fileName, contentType, checksumAlg, checksum, url, deviceProfileId, additionalInfo, externalId, ownerTenantId string
	var createdTime, dataSize int64
	err := dbpkg.Pool.QueryRow(`
		SELECT created_time, title, version, type,
		       COALESCE(tag,''), COALESCE(file_name,''), COALESCE(content_type,''),
		       COALESCE(checksum_algorithm,''), COALESCE(checksum,''),
		       COALESCE(url,''),
		       COALESCE(device_profile_id::text,''), COALESCE(additional_info,''),
		       COALESCE(external_id::text,''), COALESCE(data_size, 0),
		       tenant_id::text
		  FROM ota_package WHERE id = $1`,
		id,
	).Scan(&createdTime, &title, &ver, &ptype, &tag, &fileName, &contentType, &checksumAlg, &checksum, &url, &deviceProfileId, &additionalInfo, &externalId, &dataSize, &ownerTenantId)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if ownerTenantId != tenantId {
		return nil, errCrossTenant
	}
	return otaInfoMap(id, title, ver, ptype, tag, fileName, contentType, checksumAlg, checksum, url, deviceProfileId, additionalInfo, externalId, createdTime, dataSize), nil
}

var errCrossTenant = fmt.Errorf("ota: cross-tenant access denied")

// InfoByID — GET /api/otaPackage/info/{id}
// HandleOtaPackageById     — GET /api/otaPackage/{id}      (same response shape; we never embed binary data inline)
func InfoByID(w http.ResponseWriter, r *http.Request, id string) {
	claims, ok := requireTenantAdmin(w, r)
	if !ok {
		return
	}
	tenantId, _ := claims["tenantId"].(string)
	info, err := loadOtaInfo(tenantId, id)
	if err == errCrossTenant {
		httputil.WriteError(w, http.StatusForbidden, "Forbidden: cross-tenant access denied")
		return
	}
	if err != nil {
		log.Printf("ERROR ota info: %v", err)
		httputil.WriteError(w, http.StatusInternalServerError, "DB error")
		return
	}
	if info == nil {
		httputil.WriteError(w, http.StatusNotFound, "OTA package not found")
		return
	}
	httputil.WriteJSON(w, http.StatusOK, info)
}

// --- download --------------------------------------------------------------

// Download — GET /api/otaPackage/{id}/download
func Download(w http.ResponseWriter, r *http.Request, id string) {
	claims, ok := requireTenantAdmin(w, r)
	if !ok {
		return
	}
	tenantId, _ := claims["tenantId"].(string)

	var data []byte
	var fileName, contentType, ownerTenantId, urlField string
	err := dbpkg.Pool.QueryRow(`
		SELECT COALESCE(data, '\x'::bytea), COALESCE(file_name,''), COALESCE(content_type,''),
		       tenant_id::text, COALESCE(url,'')
		  FROM ota_package WHERE id = $1`, id,
	).Scan(&data, &fileName, &contentType, &ownerTenantId, &urlField)
	if err == sql.ErrNoRows {
		httputil.WriteError(w, http.StatusNotFound, "OTA package not found")
		return
	}
	if err != nil {
		log.Printf("ERROR ota download: %v", err)
		httputil.WriteError(w, http.StatusInternalServerError, "DB error")
		return
	}
	if ownerTenantId != tenantId {
		httputil.WriteError(w, http.StatusForbidden, "Forbidden: cross-tenant access denied")
		return
	}
	if urlField != "" {
		// TB classic returns 400 when the package was registered with an
		// external URL — there's nothing to stream, the device fetches it
		// directly.
		httputil.WriteError(w, http.StatusBadRequest, "OTA package was registered with an external URL — no inline data to download")
		return
	}
	if len(data) == 0 {
		httputil.WriteError(w, http.StatusNotFound, "OTA package has no data uploaded yet")
		return
	}

	if fileName == "" {
		fileName = "firmware.bin"
	}
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	w.Header().Set("Content-Type", contentType)
	w.Header().Set("Content-Length", strconv.Itoa(len(data)))
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", fileName))
	w.Header().Set("x-filename", fileName)
	_, _ = w.Write(data)
}

// --- save info / upload data ----------------------------------------------

// SaveInfo — POST /api/otaPackage  (JSON body, no file)
func SaveInfo(w http.ResponseWriter, r *http.Request) {
	claims, ok := requireTenantAdmin(w, r)
	if !ok {
		return
	}
	tenantId, _ := claims["tenantId"].(string)

	var body map[string]interface{}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		httputil.WriteError(w, http.StatusBadRequest, "Invalid JSON body")
		return
	}
	title, _ := body["title"].(string)
	version, _ := body["version"].(string)
	ptype, _ := body["type"].(string)
	if title == "" || version == "" || ptype == "" {
		httputil.WriteError(w, http.StatusBadRequest, "title, version and type are required")
		return
	}

	getStr := func(k string) interface{} {
		if v, ok := body[k].(string); ok && v != "" {
			return v
		}
		return nil
	}
	deviceProfileId := httputil.ExtractEntityID(body, "deviceProfileId")
	additionalInfoJSON := dbutil.JSONOrNil(body["additionalInfo"])
	now := time.Now().UnixMilli()
	id := httputil.ExtractEntityID(body, "id")

	if id == "" {
		id = uuid.New().String()
		_, err := dbpkg.Pool.Exec(`
			INSERT INTO ota_package (id, created_time, tenant_id, device_profile_id,
			                         type, title, version, tag, url,
			                         additional_info)
			VALUES ($1, $2, $3, NULLIF($4,'')::uuid, $5, $6, $7, $8, $9, $10)`,
			id, now, tenantId, deviceProfileId, ptype, title, version,
			getStr("tag"), getStr("url"), additionalInfoJSON,
		)
		if err != nil {
			log.Printf("ERROR ota insert: %v", err)
			httputil.WriteError(w, http.StatusInternalServerError, "Failed to insert OTA package")
			return
		}
		log.Printf("OTA package CREATED: id=%s tenant=%s title=%s v=%s", id, tenantId, title, version)
	} else {
		// Cross-tenant guard: only update if owned by the same tenant.
		res, err := dbpkg.Pool.Exec(`
			UPDATE ota_package SET title=$1, version=$2, type=$3,
			       tag=$4, url=$5, device_profile_id=NULLIF($6,'')::uuid,
			       additional_info=$7
			 WHERE id=$8 AND tenant_id=$9`,
			title, version, ptype, getStr("tag"), getStr("url"),
			deviceProfileId, additionalInfoJSON, id, tenantId,
		)
		if err != nil {
			log.Printf("ERROR ota update: %v", err)
			httputil.WriteError(w, http.StatusInternalServerError, "Failed to update OTA package")
			return
		}
		n, _ := res.RowsAffected()
		if n == 0 {
			httputil.WriteError(w, http.StatusNotFound, "OTA package not found")
			return
		}
		log.Printf("OTA package UPDATED: id=%s tenant=%s title=%s v=%s", id, tenantId, title, version)
	}

	info, _ := loadOtaInfo(tenantId, id)
	httputil.WriteJSON(w, http.StatusOK, info)
}

// Upload — POST /api/otaPackage/{id}  (multipart)
func Upload(w http.ResponseWriter, r *http.Request, id string) {
	claims, ok := requireTenantAdmin(w, r)
	if !ok {
		return
	}
	tenantId, _ := claims["tenantId"].(string)

	// Cross-tenant guard before reading the upload body.
	var ownerTenantId string
	if err := dbpkg.Pool.QueryRow("SELECT tenant_id::text FROM ota_package WHERE id = $1", id).Scan(&ownerTenantId); err != nil {
		if err == sql.ErrNoRows {
			httputil.WriteError(w, http.StatusNotFound, "OTA package not found")
			return
		}
		httputil.WriteError(w, http.StatusInternalServerError, "DB error")
		return
	}
	if ownerTenantId != tenantId {
		httputil.WriteError(w, http.StatusForbidden, "Forbidden: cross-tenant access denied")
		return
	}

	// Limit upload size to avoid OOM on malicious clients. 200 MiB is the
	// typical max OTA payload; bump via env if needed.
	const maxUpload = 200 << 20
	if err := r.ParseMultipartForm(maxUpload); err != nil {
		httputil.WriteError(w, http.StatusBadRequest, "Invalid multipart body: "+err.Error())
		return
	}

	q := r.URL.Query()
	checksumAlg := strings.ToUpper(q.Get("checksumAlgorithm"))
	if checksumAlg == "" {
		checksumAlg = "SHA256"
	}
	expectedChecksum := q.Get("checksum")

	file, header, err := r.FormFile("file")
	if err != nil {
		httputil.WriteError(w, http.StatusBadRequest, "Missing 'file' field in multipart")
		return
	}
	defer file.Close()
	data, err := io.ReadAll(file)
	if err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, "Failed to read upload")
		return
	}

	// Compute checksum to verify or to store.
	computed, err := computeChecksum(checksumAlg, data)
	if err != nil {
		httputil.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if expectedChecksum != "" && !strings.EqualFold(expectedChecksum, computed) {
		httputil.WriteError(w, http.StatusBadRequest,
			fmt.Sprintf("Checksum mismatch: expected %s, computed %s", expectedChecksum, computed))
		return
	}

	contentType := header.Header.Get("Content-Type")
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	if _, err := dbpkg.Pool.Exec(`
		UPDATE ota_package
		   SET data = $1, data_size = $2, file_name = $3, content_type = $4,
		       checksum_algorithm = $5, checksum = $6
		 WHERE id = $7 AND tenant_id = $8`,
		data, int64(len(data)), header.Filename, contentType,
		checksumAlg, computed, id, tenantId,
	); err != nil {
		log.Printf("ERROR ota upload: %v", err)
		httputil.WriteError(w, http.StatusInternalServerError, "Failed to persist OTA data")
		return
	}
	log.Printf("OTA package DATA: id=%s size=%d alg=%s checksum=%s file=%s",
		id, len(data), checksumAlg, computed, header.Filename)

	info, _ := loadOtaInfo(tenantId, id)
	httputil.WriteJSON(w, http.StatusOK, info)
}

func computeChecksum(alg string, data []byte) (string, error) {
	var h hash.Hash
	switch alg {
	case "SHA256":
		h = sha256.New()
	case "SHA384":
		h = sha512.New384()
	case "SHA512":
		h = sha512.New()
	case "MD5":
		h = md5.New()
	case "CRC32":
		v := crc32.ChecksumIEEE(data)
		return fmt.Sprintf("%08x", v), nil
	default:
		return "", fmt.Errorf("unsupported checksum algorithm %q (use SHA256/SHA384/SHA512/MD5/CRC32)", alg)
	}
	h.Write(data)
	return hex.EncodeToString(h.Sum(nil)), nil
}

// Delete — DELETE /api/otaPackage/{id}
func Delete(w http.ResponseWriter, r *http.Request, id string) {
	claims, ok := requireTenantAdmin(w, r)
	if !ok {
		return
	}
	tenantId, _ := claims["tenantId"].(string)
	res, err := dbpkg.Pool.Exec("DELETE FROM ota_package WHERE id = $1 AND tenant_id = $2", id, tenantId)
	if err != nil {
		log.Printf("ERROR ota delete: %v", err)
		httputil.WriteError(w, http.StatusInternalServerError, "DB error")
		return
	}
	rows, _ := res.RowsAffected()
	if rows == 0 {
		httputil.WriteError(w, http.StatusNotFound, "OTA package not found")
		return
	}
	log.Printf("OTA package DELETED: id=%s tenant=%s", id, tenantId)
	w.WriteHeader(http.StatusOK)
}
