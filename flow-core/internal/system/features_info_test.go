package system

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

// Regression coverage for the ui-contract data-fidelity audit (docs/adr/0002,
// docs/UI_CONTRACT_DATA_FIDELITY.md P2): GET /api/admin/featuresInfo hardcoded
// oauthEnabled: false regardless of whether OIDC was actually configured.
func TestHandleFeaturesInfo_OauthEnabledReflectsConfig(t *testing.T) {
	tok := fakeSystemJWT(t, "11111111-1111-1111-1111-111111111111")

	get := func() bool {
		req := httptest.NewRequest(http.MethodGet, "/api/admin/featuresInfo", nil)
		req.Header.Set("X-Authorization", "Bearer "+tok)
		rec := httptest.NewRecorder()
		HandleFeaturesInfo(rec, req)
		var body map[string]interface{}
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatalf("decode: %v", err)
		}
		v, _ := body["oauthEnabled"].(bool)
		return v
	}

	if got := get(); got {
		t.Fatalf("with no OIDC env vars set: oauthEnabled = true, want false")
	}

	t.Setenv("OIDC_ENABLED", "true")
	if got := get(); !got {
		t.Errorf("with OIDC_ENABLED=true: oauthEnabled = false, want true (a prior version hardcoded false always)")
	}
}
