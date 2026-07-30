package rpc

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	authpkg "flow-core/internal/auth"
)

const (
	tenantA = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
	tenantB = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
	devID   = "dddddddd-dddd-dddd-dddd-dddddddddddd"
)

func tenantToken(t *testing.T, tenantID string) string {
	t.Helper()
	authpkg.InitConfig()
	tok, err := authpkg.GenerateAccess(authpkg.Subject{
		UserID: "00000000-0000-0000-0000-000000000001",
		Email:  "x@x.org", Authority: "TENANT_ADMIN", TenantID: tenantID,
	}, "test")
	if err != nil {
		t.Fatalf("jwt: %v", err)
	}
	return tok
}

func postRPC(t *testing.T, tenantID, body string, oneway bool) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/api/rpc/x/"+devID, strings.NewReader(body))
	req.Header.Set("X-Authorization", "Bearer "+tenantToken(t, tenantID))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	Handle(rec, req, devID, oneway)
	return rec
}

// The device belongs to tenantA. A tenantB admin must not be able to command it —
// this is the check that keeps one customer from operating another's breakers.
func TestHandle_CrossTenantDenied(t *testing.T) {
	b := newFakeBroker(t, true)
	resetState(t, config{enabled: true, brokerAPI: b.server.URL, timeout: time.Second})
	DeviceLookup = func(string) (string, string, error) { return "devmqtt", tenantA, nil }
	t.Cleanup(func() { DeviceLookup = nil })

	rec := postRPC(t, tenantB, `{"method":"open_breaker"}`, true)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403 (body %s)", rec.Code, rec.Body.String())
	}
	if b.lastPublish() != nil {
		t.Fatal("a cross-tenant command reached the broker")
	}
}

func TestHandle_UnknownDeviceIs404(t *testing.T) {
	b := newFakeBroker(t, true)
	resetState(t, config{enabled: true, brokerAPI: b.server.URL, timeout: time.Second})
	DeviceLookup = func(string) (string, string, error) { return "", "", errors.New("no rows") }
	t.Cleanup(func() { DeviceLookup = nil })

	if rec := postRPC(t, tenantA, `{"method":"x"}`, true); rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}
}

func TestHandle_MissingMethodIs400(t *testing.T) {
	b := newFakeBroker(t, true)
	resetState(t, config{enabled: true, brokerAPI: b.server.URL, timeout: time.Second})
	DeviceLookup = func(string) (string, string, error) { return "devmqtt", tenantA, nil }
	t.Cleanup(func() { DeviceLookup = nil })

	if rec := postRPC(t, tenantA, `{"params":{}}`, true); rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
	if b.lastPublish() != nil {
		t.Fatal("published a command with no method")
	}
}

func TestHandle_OnewayReturns200AndPublishes(t *testing.T) {
	b := newFakeBroker(t, true)
	resetState(t, config{enabled: true, brokerAPI: b.server.URL, timeout: time.Second})
	DeviceLookup = func(string) (string, string, error) { return "devmqtt", tenantA, nil }
	t.Cleanup(func() { DeviceLookup = nil })

	rec := postRPC(t, tenantA, `{"method":"reboot","params":{"delay":5}}`, true)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body %s)", rec.Code, rec.Body.String())
	}
	pub := b.lastPublish()
	if pub == nil {
		t.Fatal("nothing published")
	}
	topic, _ := pub["topic"].(string)
	if !strings.HasPrefix(topic, "thingsflow/devices/devmqtt/rpc/request/") {
		t.Fatalf("topic = %q, want the device's own request topic", topic)
	}
	// The payload must carry method and params through unchanged; a device that
	// keys off params would misbehave if we re-shaped them.
	payload, _ := pub["payload"].(string)
	for _, want := range []string{`"method":"reboot"`, `"delay":5`, `"id":`} {
		if !strings.Contains(payload, want) {
			t.Fatalf("payload %s missing %s", payload, want)
		}
	}
}

// An offline device is a 504, not a 200. Reporting success for a command that
// reached nobody is the failure mode most likely to be mistaken for working.
func TestHandle_OfflineDeviceIs504(t *testing.T) {
	b := newFakeBroker(t, false)
	resetState(t, config{enabled: true, brokerAPI: b.server.URL, timeout: time.Second})
	DeviceLookup = func(string) (string, string, error) { return "devmqtt", tenantA, nil }
	t.Cleanup(func() { DeviceLookup = nil })

	if rec := postRPC(t, tenantA, `{"method":"x"}`, true); rec.Code != http.StatusGatewayTimeout {
		t.Fatalf("status = %d, want 504", rec.Code)
	}
}

func TestHandle_RequiresAuth(t *testing.T) {
	resetState(t, config{enabled: true, timeout: time.Second})
	DeviceLookup = func(string) (string, string, error) { return "devmqtt", tenantA, nil }
	t.Cleanup(func() { DeviceLookup = nil })

	req := httptest.NewRequest(http.MethodPost, "/api/rpc/oneway/"+devID, strings.NewReader(`{"method":"x"}`))
	rec := httptest.NewRecorder()
	Handle(rec, req, devID, true)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", rec.Code)
	}
}

func TestHandle_DisabledIs501(t *testing.T) {
	resetState(t, config{enabled: false, timeout: time.Second})
	DeviceLookup = func(string) (string, string, error) { return "devmqtt", tenantA, nil }
	t.Cleanup(func() { DeviceLookup = nil })

	if rec := postRPC(t, tenantA, `{"method":"x"}`, true); rec.Code != http.StatusNotImplemented {
		t.Fatalf("status = %d, want 501", rec.Code)
	}
}

// Two-way RPC with no response transport must say so rather than making the
// caller wait out the full timeout for a reply that can never arrive.
func TestHandle_TwowayWithoutResponseTransportIs503(t *testing.T) {
	b := newFakeBroker(t, true)
	resetState(t, config{enabled: true, brokerAPI: b.server.URL, timeout: time.Second})
	repliesMu.Lock()
	repliesReady = false
	repliesMu.Unlock()
	DeviceLookup = func(string) (string, string, error) { return "devmqtt", tenantA, nil }
	t.Cleanup(func() { DeviceLookup = nil })

	if rec := postRPC(t, tenantA, `{"method":"x"}`, false); rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}
}
