package security

import (
	"encoding/base64"

	"github.com/zeebo/blake3"
)

// KeyHash is the redacted, stable identifier for a (namespace, key) used in logs and metric labels
// (design/15 "Data redaction"): base64url(blake3(namespace + key)). Raw keys and values must never
// appear in logs/metrics by default.
func KeyHash(namespace string, key []byte) string {
	h := blake3.New()
	_, _ = h.WriteString(namespace)
	_, _ = h.Write(key)
	sum := h.Sum(nil)
	return base64.RawURLEncoding.EncodeToString(sum[:16]) // 128-bit prefix is plenty for a label
}

// RedactValue returns a placeholder unless the caller is admin and explicitly requested the value
// (design/15: debug endpoints require includeValue=true + admin auth).
func RedactValue(value []byte, role Role, includeValue bool) string {
	if includeValue && role == RoleAdmin {
		return string(value)
	}
	return "<redacted>"
}
