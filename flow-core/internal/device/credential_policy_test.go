package device

import "testing"

func TestNormalizeAccessTokenCredentialRejectsWeakTokens(t *testing.T) {
	t.Setenv("DEVICE_ACCESS_TOKEN_MIN_LENGTH", "20")

	if _, err := normalizeAccessTokenCredential("short-token"); err == nil {
		t.Fatal("expected weak token to be rejected")
	}
}

func TestNormalizeAccessTokenCredentialAcceptsStrongTokens(t *testing.T) {
	t.Setenv("DEVICE_ACCESS_TOKEN_MIN_LENGTH", "20")

	got, err := normalizeAccessTokenCredential("0123456789abcdefghij")
	if err != nil {
		t.Fatalf("expected strong token to be accepted: %v", err)
	}
	if got != "0123456789abcdefghij" {
		t.Fatalf("token = %q", got)
	}
}

func TestNormalizeAccessTokenCredentialRejectsWhitespace(t *testing.T) {
	t.Setenv("DEVICE_ACCESS_TOKEN_MIN_LENGTH", "20")

	if _, err := normalizeAccessTokenCredential("0123456789 abcdefghi"); err == nil {
		t.Fatal("expected token with whitespace to be rejected")
	}
}

func TestNormalizeAccessTokenCredentialAllowsEmptyForAutoGeneration(t *testing.T) {
	got, err := normalizeAccessTokenCredential("   ")
	if err != nil {
		t.Fatalf("empty token should allow auto-generation: %v", err)
	}
	if got != "" {
		t.Fatalf("token = %q, want empty", got)
	}
}
