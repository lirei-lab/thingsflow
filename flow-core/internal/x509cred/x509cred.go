// Package x509cred handles X.509-based device authentication: hashing
// PEM-encoded certificates into the SHA-256 fingerprint that
// device_credentials.credentials_id stores.
//
// Storage convention:
//
//	credentials_type  = 'X509_CERTIFICATE'
//	credentials_id    = uppercase hex SHA-256 of the DER form of the cert
//	                    (no colons, no spaces — matches the MQTT auth path)
//	credentials_value = the PEM the admin uploaded, kept for inspection
//	                    and future re-derivation
//
// Some MQTT gateways expose cert_pem in authn body templates when the TCP
// listener is TLS with a peer certificate; we hash the DER on our side
// rather than relying on a vendor-specific fingerprint placeholder.
package x509cred

import (
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"strings"
)

// ParseAndFingerprint accepts a PEM-encoded cert and returns the
// uppercase hex SHA-256 of the DER body plus the parsed certificate.
// Returns an error if the input is not a single CERTIFICATE block.
func ParseAndFingerprint(pemStr string) (fingerprint string, cert *x509.Certificate, err error) {
	pemStr = strings.TrimSpace(pemStr)
	if pemStr == "" {
		return "", nil, errors.New("empty certificate")
	}
	block, _ := pem.Decode([]byte(pemStr))
	if block == nil || block.Type != "CERTIFICATE" {
		return "", nil, errors.New("not a PEM CERTIFICATE block")
	}
	parsed, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return "", nil, err
	}
	sum := sha256.Sum256(block.Bytes)
	return strings.ToUpper(hex.EncodeToString(sum[:])), parsed, nil
}

// ResolveCredentialsID maps incoming credentials data to the storage
// value used as device_credentials.credentials_id.
//
// If credentialsID is non-empty we trust it (admins may want to
// pre-pin a cert before issuing it); we just normalize and validate
// the shape (uppercase, 64 hex chars). Otherwise we hash the PEM.
func ResolveCredentialsID(credentialsID, credentialsValue string) (string, error) {
	if strings.TrimSpace(credentialsID) != "" {
		s := strings.ToUpper(strings.NewReplacer(":", "", " ", "").Replace(credentialsID))
		if len(s) != 64 {
			return "", errors.New("X.509 credentialsId must be a SHA-256 hex digest (64 chars)")
		}
		if _, err := hex.DecodeString(s); err != nil {
			return "", errors.New("X.509 credentialsId must be a valid hex digest")
		}
		return s, nil
	}
	if strings.TrimSpace(credentialsValue) == "" {
		return "", errors.New("X.509 credentialsValue (PEM) is required when no credentialsId is provided")
	}
	fp, _, err := ParseAndFingerprint(credentialsValue)
	return fp, err
}
