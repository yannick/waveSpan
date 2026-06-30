package vector

// CollectionOfKey extracts the collection name from any CFVectorRaw or
// CFVectorIndex key, for partial-backup selection. All vector prefixes are 2 bytes.
// For raw ("vr") and meta ("vm") keys the collection is the first lp chunk after
// the prefix; for attach ("va") keys it is the second chunk (the key embeds the
// nodeID first). Returns ("", false) for an unknown prefix or a short/malformed
// key — never panics.
func CollectionOfKey(key []byte) (string, bool) {
	if len(key) < 2 {
		return "", false
	}
	switch string(key[:2]) {
	case pfxRaw, pfxMeta:
		c, tail := decodeLP(key[2:])
		if tail == nil {
			return "", false
		}
		return c, true
	case pfxAttach:
		_, rest := decodeLP(key[2:]) // skip nodeID chunk
		if rest == nil {
			return "", false
		}
		c, tail := decodeLP(rest)
		if tail == nil {
			return "", false
		}
		return c, true
	default:
		return "", false
	}
}
