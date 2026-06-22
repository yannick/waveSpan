package vector

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"math"
)

// VecHash derives a deterministic vector id from a vector's canonical little-endian float32 bytes.
// Using the embedding itself as the identity makes "the vector is the key": two identical embeddings
// map to the same id (so they dedupe in MergeTopK and overwrite on Put), and lookups are by exact
// vector. 128 bits is ample to avoid collisions for any realistic corpus.
func VecHash(vec []float32) string {
	buf := make([]byte, 4*len(vec))
	for i, f := range vec {
		binary.LittleEndian.PutUint32(buf[i*4:], math.Float32bits(f))
	}
	sum := sha256.Sum256(buf)
	return base64.RawURLEncoding.EncodeToString(sum[:16])
}
