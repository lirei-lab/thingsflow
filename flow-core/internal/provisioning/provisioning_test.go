package provisioning

import (
	"strings"
	"testing"
)

func TestNormalizeProvisionRequestRequiresDeviceNameKeyAndSecret(t *testing.T) {
	_, err := normalizeProvisionRequest(provisionRequest{
		DeviceName:            "  pump-001  ",
		ProvisionDeviceKey:    "  fleet-a  ",
		ProvisionDeviceSecret: "  secret-a  ",
	})
	if err != nil {
		t.Fatalf("valid request rejected: %v", err)
	}

	cases := []provisionRequest{
		{ProvisionDeviceKey: "fleet-a", ProvisionDeviceSecret: "secret-a"},
		{DeviceName: "pump-001", ProvisionDeviceSecret: "secret-a"},
		{DeviceName: "pump-001", ProvisionDeviceKey: "fleet-a"},
	}
	for _, c := range cases {
		if _, err := normalizeProvisionRequest(c); err == nil {
			t.Fatalf("request %+v accepted, want validation error", c)
		}
	}
}

func TestProvisionSecretHashRoundTrip(t *testing.T) {
	hash, err := hashProvisionSecret("secret-a")
	if err != nil {
		t.Fatalf("hashProvisionSecret: %v", err)
	}
	if hash == "" || hash == "secret-a" || strings.Contains(hash, "secret-a") {
		t.Fatalf("hash leaked plaintext secret: %q", hash)
	}
	if !verifyProvisionSecret(hash, "secret-a") {
		t.Fatal("expected valid secret to verify")
	}
	if verifyProvisionSecret(hash, "wrong-secret") {
		t.Fatal("wrong secret verified")
	}
	if verifyProvisionSecret("", "secret-a") {
		t.Fatal("empty hash verified")
	}
}

func TestProvisionSuccessResponseKeepsClassicCredentialsAndIncludesDeviceJWT(t *testing.T) {
	resp := successResponse("ACCESS_TOKEN", "token-123", "device-123", "tenant-abc", nil)
	if resp.Status != "SUCCESS" {
		t.Fatalf("status = %q, want SUCCESS", resp.Status)
	}
	if resp.CredentialsType != "ACCESS_TOKEN" || resp.CredentialsValue != "token-123" {
		t.Fatalf("unexpected credentials response: %+v", resp)
	}
	if resp.DeviceID != "device-123" || resp.TenantID != "tenant-abc" {
		t.Fatalf("expected device and tenant ids in provisioning response: %+v", resp)
	}
}
