package rpc

import "encoding/base64"

// base64URLDecode decodes a JWT segment. JWT payloads are unpadded base64url,
// which Go's RawURLEncoding handles; the padded form is accepted too so a
// hand-built token does not silently fail identity extraction.
//
// This exact class of bug has bitten the MQTT path before: an unpadded payload
// fed to a padding-requiring decoder yielded empty claims and silently dropped
// every message, with no error anywhere.
func base64URLDecode(seg string) ([]byte, error) {
	if b, err := base64.RawURLEncoding.DecodeString(seg); err == nil {
		return b, nil
	}
	return base64.URLEncoding.DecodeString(seg)
}
