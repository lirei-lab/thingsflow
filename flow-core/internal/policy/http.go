package policy

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"

	dbpkg "flow-core/internal/db"
	"flow-core/internal/httputil"
)

const maxPolicyBodyBytes = 1 << 20 // 1 MiB — deliberate cap: policy documents are small; the twin model catalog uses 5 MiB for authored model JSON

// HandleCollection serves GET (list) and POST (create) /api/policies.
func HandleCollection(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		handleList(w, r)
	case http.MethodPost:
		handleCreate(w, r)
	default:
		httputil.WriteError(w, http.StatusMethodNotAllowed, "Method not allowed")
	}
}

// HandleVersion serves GET (fetch one version) and DELETE (deprecate)
// /api/policies/{policyId}/{version}.
func HandleVersion(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet, http.MethodDelete:
		handleGetOrDeprecate(w, r)
	default:
		httputil.WriteError(w, http.StatusMethodNotAllowed, "Method not allowed")
	}
}

func handleCreate(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := catalogTenant(w, r)
	if !ok {
		return
	}
	if dbpkg.Pool == nil {
		httputil.WriteError(w, http.StatusInternalServerError, "Policy database unavailable")
		return
	}
	raw, err := readOneJSON(r)
	if err != nil {
		httputil.WriteError(w, http.StatusBadRequest, "Invalid policy JSON body")
		return
	}
	record, err := NewStore(dbpkg.Pool).Create(r.Context(), tenantID, raw)
	if err != nil {
		writeStoreError(w, err, "Policy create failed")
		return
	}
	payload, err := recordPayload(record)
	if err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, "Policy response failed")
		return
	}
	// 201 Created mirrors the twin-model catalog create contract.
	httputil.WriteJSON(w, http.StatusCreated, payload)
}

func handleList(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := catalogTenant(w, r)
	if !ok {
		return
	}
	if dbpkg.Pool == nil {
		httputil.WriteError(w, http.StatusInternalServerError, "Policy database unavailable")
		return
	}
	options, err := parseListOptions(r)
	if err != nil {
		httputil.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	page, err := NewStore(dbpkg.Pool).List(r.Context(), tenantID, options)
	if err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, "Policy list failed")
		return
	}
	payloads := make([]map[string]interface{}, 0, len(page.Data))
	for _, record := range page.Data {
		payload, err := recordPayload(record)
		if err != nil {
			httputil.WriteError(w, http.StatusInternalServerError, "Policy response failed")
			return
		}
		payloads = append(payloads, payload)
	}
	httputil.WriteJSON(w, http.StatusOK, map[string]interface{}{
		"data":          payloads,
		"totalElements": page.TotalElements,
		"totalPages":    page.TotalPages,
		"hasNext":       page.HasNext,
		"page":          page.Page,
	})
}

func handleGetOrDeprecate(w http.ResponseWriter, r *http.Request) {
	tenantID, ok := catalogTenant(w, r)
	if !ok {
		return
	}
	policyID, version, err := validatePathIdentity(r.PathValue("policyId"), r.PathValue("version"))
	if err != nil {
		httputil.WriteError(w, http.StatusBadRequest, err.Error())
		return
	}
	if dbpkg.Pool == nil {
		httputil.WriteError(w, http.StatusInternalServerError, "Policy database unavailable")
		return
	}
	store := NewStore(dbpkg.Pool)
	var record Record
	if r.Method == http.MethodDelete {
		record, err = store.Deprecate(r.Context(), tenantID, policyID, version)
	} else {
		record, err = store.Get(r.Context(), tenantID, policyID, version)
	}
	if err != nil {
		writeStoreError(w, err, "Policy query failed")
		return
	}
	payload, err := recordPayload(record)
	if err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, "Policy response failed")
		return
	}
	httputil.WriteJSON(w, http.StatusOK, payload)
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
	if raw := query.Get("latest"); raw != "" {
		value, err := strconv.ParseBool(raw)
		if err != nil {
			return ListOptions{}, errors.New("Invalid latest")
		}
		options.Latest = value
	}
	if raw := query.Get("includeDeprecated"); raw != "" {
		value, err := strconv.ParseBool(raw)
		if err != nil {
			return ListOptions{}, errors.New("Invalid includeDeprecated")
		}
		options.IncludeDeprecated = value
	}
	return options, nil
}

func validatePathIdentity(rawPolicyID, version string) (string, string, error) {
	policyID, err := NormalizePolicyID(rawPolicyID)
	if err != nil || policyID != rawPolicyID {
		return "", "", errors.New("Invalid policyId")
	}
	if err := ValidateVersion(version); err != nil {
		return "", "", errors.New("Invalid policy version")
	}
	return policyID, version, nil
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

func catalogTenant(w http.ResponseWriter, r *http.Request) (string, bool) {
	claims, ok := httputil.RequireAuth(w, r)
	if !ok {
		return "", false
	}
	tenantID, _ := claims["tenantId"].(string)
	if tenantID == "" {
		httputil.WriteError(w, http.StatusForbidden, "Missing tenant")
		return "", false
	}
	return tenantID, true
}

func writeStoreError(w http.ResponseWriter, err error, fallback string) {
	switch {
	case errors.Is(err, ErrConflict), errors.Is(err, ErrDeprecated):
		httputil.WriteError(w, http.StatusConflict, err.Error())
	case errors.Is(err, ErrForbidden):
		httputil.WriteError(w, http.StatusForbidden, err.Error())
	case errors.Is(err, ErrNotFound):
		httputil.WriteError(w, http.StatusNotFound, err.Error())
	case errors.Is(err, ErrInvalidPolicy):
		httputil.WriteError(w, http.StatusBadRequest, err.Error())
	default:
		httputil.WriteError(w, http.StatusInternalServerError, fallback)
	}
}

func readOneJSON(r *http.Request) (json.RawMessage, error) {
	limited := io.LimitReader(r.Body, maxPolicyBodyBytes+1)
	body, err := io.ReadAll(limited)
	if err != nil || len(body) == 0 || len(body) > maxPolicyBodyBytes {
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
