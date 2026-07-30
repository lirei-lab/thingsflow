package rpc

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/google/uuid"

	"flow-core/internal/httputil"
)

// Request is ThingsBoard's RPC body. `timeout` is optional and caps how long a
// two-way call waits; `persistent` is accepted and ignored — this platform has no
// store-and-forward queue, and silently dropping the field would be worse than
// saying so in the docs.
type Request struct {
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params"`
	Timeout int64           `json:"timeout"`
}

// DeviceLookup resolves a device id to (mqttIdentity, tenantId). Injected at
// startup so this package does not import the device package (which would create
// a cycle through the handler registry).
var DeviceLookup func(deviceID string) (mqttID string, tenantID string, err error)

// Handle serves POST /api/rpc/{oneway,twoway}/{deviceId} and the
// /api/plugins/rpc/* aliases.
func Handle(w http.ResponseWriter, r *http.Request, deviceID string, oneway bool) {
	claims, ok := httputil.RequireAuth(w, r)
	if !ok {
		return
	}
	if r.Method != http.MethodPost {
		httputil.WriteError(w, http.StatusMethodNotAllowed, "Method not allowed")
		return
	}
	if !Enabled() {
		httputil.WriteError(w, http.StatusNotImplemented, "RPC delivery is disabled (RPC_ENABLED=false)")
		return
	}
	if DeviceLookup == nil {
		httputil.WriteError(w, http.StatusInternalServerError, "RPC device lookup is not wired")
		return
	}

	tokenTenant, _ := claims["tenantId"].(string)
	mqttID, deviceTenant, err := DeviceLookup(deviceID)
	if err != nil {
		httputil.WriteError(w, http.StatusNotFound, "Device not found")
		return
	}
	if tokenTenant == "" || deviceTenant != tokenTenant {
		httputil.WriteError(w, http.StatusForbidden, "Cross-tenant RPC denied")
		return
	}

	raw, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		httputil.WriteError(w, http.StatusBadRequest, "Unreadable body")
		return
	}
	var req Request
	if err := json.Unmarshal(raw, &req); err != nil {
		httputil.WriteError(w, http.StatusBadRequest, "Invalid JSON")
		return
	}
	if req.Method == "" {
		httputil.WriteError(w, http.StatusBadRequest, "Missing RPC method")
		return
	}

	// The device sees the method/params it was sent, plus the id it must echo back
	// on the response topic. Params are forwarded verbatim rather than re-encoded
	// so a device contract that depends on key order or number formatting is not
	// quietly altered in transit.
	requestID := uuid.NewString()
	payload, err := json.Marshal(map[string]interface{}{
		"id":     requestID,
		"method": req.Method,
		"params": optionalJSON(req.Params),
	})
	if err != nil {
		httputil.WriteError(w, http.StatusInternalServerError, "Failed to encode RPC request")
		return
	}

	if oneway {
		if err := SendOneway(mqttID, requestID, payload); err != nil {
			writeRPCError(w, err)
			return
		}
		// TB answers a one-way RPC with an empty 200.
		w.WriteHeader(http.StatusOK)
		return
	}

	timeout := time.Duration(req.Timeout) * time.Millisecond
	reply, err := SendTwoway(r.Context(), mqttID, requestID, payload, timeout)
	if err != nil {
		writeRPCError(w, err)
		return
	}

	// Pass the device's reply through untouched when it is valid JSON, which is
	// what the TB widget contract expects. A non-JSON reply is wrapped rather than
	// dropped, so a misbehaving device is visible instead of looking like silence.
	w.Header().Set("Content-Type", "application/json")
	if json.Valid(reply) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(reply)
		return
	}
	httputil.WriteJSON(w, http.StatusOK, map[string]interface{}{"response": string(reply)})
}

func optionalJSON(raw json.RawMessage) interface{} {
	if len(raw) == 0 {
		return map[string]interface{}{}
	}
	return raw
}

func writeRPCError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrOffline):
		// TB uses 504 for "device unreachable"; the UI renders it as offline.
		httputil.WriteError(w, http.StatusGatewayTimeout, "Device is not connected")
	case errors.Is(err, ErrTimeout):
		httputil.WriteError(w, http.StatusGatewayTimeout, "Device did not respond in time")
	case errors.Is(err, ErrDisabled):
		httputil.WriteError(w, http.StatusNotImplemented, "RPC delivery is disabled")
	case errors.Is(err, ErrNoReplies):
		httputil.WriteError(w, http.StatusServiceUnavailable,
			"RPC response transport is unavailable; one-way RPC still works")
	case errors.Is(err, context.Canceled):
		// Caller hung up; nothing useful to say back to a closed connection.
		return
	default:
		httputil.WriteError(w, http.StatusBadGateway, "Failed to deliver RPC to device")
	}
}

// HandlePersistent — GET|DELETE /api/rpc/persistent/{rpcId}.
//
// There is no persistent RPC store. A command to an offline device fails with
// 504 rather than being queued, which is stated in the docs and reflected in the
// `persistent` request field being accepted and ignored.
//
// So there is never a persisted RPC to fetch or cancel, and 404 is the truthful
// answer for any id. The alternative — an empty 200 — would make the UI render
// an empty history as if commands had been recorded and then lost.
func HandlePersistent(w http.ResponseWriter, r *http.Request, rpcID string) {
	if _, ok := httputil.RequireAuth(w, r); !ok {
		return
	}
	switch r.Method {
	case http.MethodGet, http.MethodDelete:
		httputil.WriteError(w, http.StatusNotFound,
			"Persistent RPC is not available: commands are delivered live or fail")
	default:
		httputil.WriteError(w, http.StatusMethodNotAllowed, "Method not allowed")
	}
}
