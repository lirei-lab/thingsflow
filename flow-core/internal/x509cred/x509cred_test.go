package x509cred

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"strings"
	"testing"
	"time"
)

// makeTestPEM generates a self-signed cert and returns its PEM and
// the expected uppercase-hex SHA-256 fingerprint of its DER body.
func makeTestPEM(t *testing.T) (string, string) {
	t.Helper()
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("genkey: %v", err)
	}
	tpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "test-device"},
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
	}
	der, err := x509.CreateCertificate(rand.Reader, tpl, tpl, &priv.PublicKey, priv)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	pemBytes := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})

	// Compute the same fingerprint the package will compute.
	fp, _, err := ParseAndFingerprint(string(pemBytes))
	if err != nil {
		t.Fatalf("ParseAndFingerprint: %v", err)
	}
	return string(pemBytes), fp
}

func TestParseAndFingerprint_ValidCert(t *testing.T) {
	pem, fp := makeTestPEM(t)
	got, cert, err := ParseAndFingerprint(pem)
	if err != nil {
		t.Fatalf("ParseAndFingerprint: %v", err)
	}
	if got != fp {
		t.Errorf("fingerprint mismatch: got %q want %q", got, fp)
	}
	if cert == nil {
		t.Errorf("returned cert is nil")
	}
	if len(got) != 64 {
		t.Errorf("fingerprint len = %d, want 64 (sha-256 hex)", len(got))
	}
	if got != strings.ToUpper(got) {
		t.Errorf("fingerprint should be uppercase: %q", got)
	}
}

func TestParseAndFingerprint_RejectsGarbage(t *testing.T) {
	for _, input := range []string{
		"",
		"not a cert",
		"-----BEGIN PUBLIC KEY-----\nblah\n-----END PUBLIC KEY-----",
	} {
		if _, _, err := ParseAndFingerprint(input); err == nil {
			t.Errorf("expected error for input %q", input)
		}
	}
}

func TestResolveCredentialsID_AcceptsExplicitFingerprint(t *testing.T) {
	fp := strings.Repeat("AB", 32) // 64 hex chars
	got, err := ResolveCredentialsID(fp, "")
	if err != nil {
		t.Fatalf("ResolveCredentialsID: %v", err)
	}
	if got != fp {
		t.Errorf("got %q want %q", got, fp)
	}
}

func TestResolveCredentialsID_NormalizesColonsAndCase(t *testing.T) {
	// 32 bytes of 0xab encoded with colons + lowercase.
	in := strings.Join(strings.Split(strings.Repeat("ab", 32), ""), ":")
	want := strings.ToUpper(strings.ReplaceAll(in, ":", ""))
	got, err := ResolveCredentialsID(in, "")
	if err != nil {
		t.Fatalf("ResolveCredentialsID: %v", err)
	}
	if got != want {
		t.Errorf("got %q want %q", got, want)
	}
}

func TestResolveCredentialsID_RejectsBadHex(t *testing.T) {
	_, err := ResolveCredentialsID("not-a-hex-digest-but-just-looks-the-right-length-aaaaaaaaaaaaaaaa", "")
	if err == nil {
		t.Errorf("expected error for non-hex input")
	}
}

func TestResolveCredentialsID_FallsBackToHashingPEM(t *testing.T) {
	pem, fp := makeTestPEM(t)
	got, err := ResolveCredentialsID("", pem)
	if err != nil {
		t.Fatalf("ResolveCredentialsID: %v", err)
	}
	if got != fp {
		t.Errorf("got %q want %q", got, fp)
	}
}

func TestResolveCredentialsID_NeedsOneOrTheOther(t *testing.T) {
	if _, err := ResolveCredentialsID("", ""); err == nil {
		t.Errorf("expected error when both id and value empty")
	}
}
