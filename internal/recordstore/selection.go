package recordstore

import "encoding/binary"

// NamespaceOfKey extracts the namespace from any CFKVData or CFKVMeta key, for
// partial-backup selection. It handles all key types in those CFs:
//   - TTL-sentinel index keys (CFKVMeta, leading 0xff) via parseTTLKey;
//   - latest-pointer (CFKVMeta) and versioned-record (CFKVData) keys, both of
//     which lead with lenPrefix(ns).
//
// It is defensive: a short, empty, or malformed key returns ("", false) rather
// than panicking.
func NamespaceOfKey(key []byte) (string, bool) {
	if len(key) == 0 {
		return "", false
	}
	if key[0] == ttlSentinel {
		ns, _, ok := parseTTLKey(key)
		return ns, ok
	}
	nsLen, n := binary.Uvarint(key)
	if n <= 0 || int(nsLen) > len(key)-n {
		return "", false
	}
	return string(key[n : n+int(nsLen)]), true
}
