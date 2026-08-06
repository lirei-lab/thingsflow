package twinmodel

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"

	dbpkg "flow-core/internal/db"
	"flow-core/internal/httputil"
)

const maxCatalogBodyBytes = 5 << 20

// HandleCollection serves GET/POST /api/twin-models.
func HandleCollection(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodPost {
		httputil.WriteError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}
	claims, ok := httputil.RequireAuth(w, r)
	if !ok {
		return
	}
	tenantID, ok := catalogTenant(claims)
	if !ok {
		httputil.WriteError(w, http.StatusForbidden, "A tenant-bound identity is required")
		return
	}
	if dbpkg.Pool == nil {
		httputil.WriteError(w, http.StatusInternalServerError, "Twin model database unavailable")
		return
	}
	store := NewStore(dbpkg.Pool)
	if r.Method == http.MethodGet {
		handleList(w, r, store, tenantID)
		return
	}
	handleCreate(w, r, store, tenantID)
}

func handleCreate(w http.ResponseWriter, r *http.Request, store *Store, tenantID string) {
	raw, err := readOneJSON(r)
	if err != nil {
		httputil.WriteError(w, http.StatusBadRequest, "Invalid model JSON")
		return
	}
	record, err := store.Create(r.Context(), tenantID, raw)
	if err != nil {
		writeStoreError(w, err, "Twin model create failed")
		return
	}
	payload, err := recordPayload(record)
	if err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, "Twin model response failed")
		return
	}
	httputil.WriteJSON(w, http.StatusCreated, payload)
}

func handleList(w http.ResponseWriter, r *http.Request, store *Store, tenantID string) {
	options, err := parseListOptions(r)
	if err != nil {
		httputil.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	page, err := store.List(r.Context(), tenantID, options)
	if err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, "Twin model list failed")
		return
	}
	data := make([]map[string]interface{}, 0, len(page.Data))
	for _, record := range page.Data {
		payload, err := recordPayload(record)
		if err != nil {
			httputil.WriteError(w, http.StatusInternalServerError, "Twin model response failed")
			return
		}
		data = append(data, payload)
	}
	httputil.WriteJSON(w, http.StatusOK, map[string]interface{}{
		"data": data, "totalElements": page.TotalElements, "totalPages": page.TotalPages,
		"hasNext": page.HasNext, "page": page.Page,
	})
}

// HandleVersion serves GET/DELETE /api/twin-models/{modelId}/{version}.
func HandleVersion(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodDelete {
		httputil.WriteError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}
	claims, ok := httputil.RequireAuth(w, r)
	if !ok {
		return
	}
	tenantID, ok := catalogTenant(claims)
	if !ok {
		httputil.WriteError(w, http.StatusForbidden, "A tenant-bound identity is required")
		return
	}
	modelID, version, err := validatePathIdentity(r.PathValue("modelId"), r.PathValue("version"))
	if err != nil {
		httputil.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if dbpkg.Pool == nil {
		httputil.WriteError(w, http.StatusInternalServerError, "Twin model database unavailable")
		return
	}
	store := NewStore(dbpkg.Pool)
	var record Record
	if r.Method == http.MethodDelete {
		record, err = store.Deprecate(r.Context(), tenantID, modelID, version)
	} else {
		record, err = store.Get(r.Context(), tenantID, modelID, version)
	}
	if err != nil {
		writeStoreError(w, err, "Twin model query failed")
		return
	}
	payload, err := recordPayload(record)
	if err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, "Twin model response failed")
		return
	}
	httputil.WriteJSON(w, http.StatusOK, payload)
}

// HandleRepoint serves PUT /api/twins/{entityType}/{entityId}/model.
func HandleRepoint(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut {
		httputil.WriteError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}
	claims, ok := httputil.RequireAuth(w, r)
	if !ok {
		return
	}
	entityType := strings.ToUpper(strings.TrimSpace(r.PathValue("entityType")))
	entityID := r.PathValue("entityId")
	if (entityType != "DEVICE" && entityType != "ASSET") || !httputil.LooksLikeUUID(entityID) {
		httputil.WriteError(w, http.StatusBadRequest, "Invalid twin entity identity")
		return
	}
	var request struct {
		ModelID string `json:"modelId"`
		Version string `json:"version"`
	}
	if err := decodeOneJSON(r, &request); err != nil {
		httputil.WriteError(w, http.StatusBadRequest, "Invalid model reference JSON")
		return
	}
	modelID, version, err := validatePathIdentity(request.ModelID, request.Version)
	if err != nil {
		httputil.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if dbpkg.Pool == nil {
		httputil.WriteError(w, http.StatusInternalServerError, "Twin model database unavailable")
		return
	}
	tenantID, _ := claims["tenantId"].(string)
	pin, err := NewStore(dbpkg.Pool).Repoint(r.Context(), tenantID, entityType, entityID, modelID, version, callerIsSysAdmin(claims))
	if err != nil {
		writeStoreError(w, err, "Twin model re-point failed")
		return
	}
	httputil.WriteJSON(w, http.StatusOK, pin)
}

func parseListOptions(r *http.Request) (ListOptions, error) {
	options := ListOptions{PageSize: 100, Latest: true}
	query := r.URL.Query()
	if raw := query.Get("page"); raw != "" {
		page, err := strconv.Atoi(raw)
		if err != nil || page < 0 {
			return ListOptions{}, errors.New("Invalid page")
		}
		options.Page = page
	}
	if raw := query.Get("pageSize"); raw != "" {
		pageSize, err := strconv.Atoi(raw)
		if err != nil || pageSize < 1 {
			return ListOptions{}, errors.New("Invalid pageSize")
		}
		options.PageSize = httputil.ClampPageSize(pageSize, 100)
	}
	if raw := query.Get("kind"); raw != "" {
		options.Kind = strings.ToUpper(strings.TrimSpace(raw))
		if options.Kind != "DEVICE" && options.Kind != "ASSET" {
			return ListOptions{}, errors.New("Invalid model kind")
		}
	}
	var err error
	if options.Latest, err = optionalBool(query.Get("latest"), true); err != nil {
		return ListOptions{}, errors.New("Invalid latest")
	}
	if options.IncludeDeprecated, err = optionalBool(query.Get("includeDeprecated"), false); err != nil {
		return ListOptions{}, errors.New("Invalid includeDeprecated")
	}
	return options, nil
}

func optionalBool(raw string, fallback bool) (bool, error) {
	if raw == "" {
		return fallback, nil
	}
	value, err := strconv.ParseBool(raw)
	if err != nil {
		return false, err
	}
	return value, nil
}

func validatePathIdentity(rawModelID, version string) (string, string, error) {
	modelID, err := NormalizeModelID(rawModelID)
	if err != nil || modelID != rawModelID {
		return "", "", errors.New("Invalid modelId")
	}
	if err := ValidateVersion(version); err != nil {
		return "", "", errors.New("Invalid model version")
	}
	return modelID, version, nil
}

func recordPayload(record Record) (map[string]interface{}, error) {
	var payload map[string]interface{}
	if err := json.Unmarshal(record.Definition, &payload); err != nil {
		return nil, err
	}
	payload["deprecated"] = record.Deprecated
	payload["createdTime"] = record.CreatedTime
	payload["updatedTime"] = record.UpdatedTime
	return payload, nil
}

func catalogTenant(claims map[string]interface{}) (string, bool) {
	tenantID, _ := claims["tenantId"].(string)
	return tenantID, tenantID != ""
}

func callerIsSysAdmin(claims map[string]interface{}) bool {
	scopes, _ := claims["scopes"].([]interface{})
	for _, scope := range scopes {
		if value, ok := scope.(string); ok && value == "SYS_ADMIN" {
			return true
		}
	}
	return false
}

func writeStoreError(w http.ResponseWriter, err error, fallback string) {
	switch {
	case errors.Is(err, ErrConflict), errors.Is(err, ErrDeprecated):
		httputil.WriteError(w, http.StatusConflict, err.Error())
	case errors.Is(err, ErrForbidden):
		httputil.WriteError(w, http.StatusForbidden, err.Error())
	case errors.Is(err, ErrNotFound):
		httputil.WriteError(w, http.StatusNotFound, err.Error())
	case errors.Is(err, ErrKindMismatch):
		httputil.WriteError(w, http.StatusBadRequest, err.Error())
	case errors.Is(err, ErrInvalidModel):
		httputil.WriteError(w, http.StatusBadRequest, err.Error())
	default:
		httputil.WriteError(w, http.StatusInternalServerError, fallback)
	}
}

func readOneJSON(r *http.Request) (json.RawMessage, error) {
	limited := io.LimitReader(r.Body, maxCatalogBodyBytes+1)
	body, err := io.ReadAll(limited)
	if err != nil || len(body) == 0 || len(body) > maxCatalogBodyBytes {
		return nil, errors.New("invalid JSON body")
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	var raw json.RawMessage
	if err := decoder.Decode(&raw); err != nil {
		return nil, err
	}
	if err := ensureJSONEOF(decoder); err != nil {
		return nil, err
	}
	return raw, nil
}

func decodeOneJSON(r *http.Request, target interface{}) error {
	decoder := json.NewDecoder(io.LimitReader(r.Body, maxCatalogBodyBytes+1))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	return ensureJSONEOF(decoder)
}

func ensureJSONEOF(decoder *json.Decoder) error {
	var extra interface{}
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("multiple JSON values")
		}
		return err
	}
	return nil
}
