package collections

import (
	"bytes"

	"github.com/yannick/wavespan/internal/storage"
)

// Subrange freeze (design/30 §6.1). During a migrate-on-split the source shard freezes the subrange
// being moved: mutations to it are rejected (frozenMark) so the client retries and the write lands on
// the new shard once the directory cuts over — no acknowledged write is lost. Reads are unaffected.
// Because migrateScan reads after the freeze commits, every write committed before the freeze is
// captured (and migrated); every write after it is rejected. Frozen ranges live in the shard's meta
// space:  <prefix>|subMeta|frozenTag|<start> -> end
const frozenTag byte = 0x01

func (b *baseSM) frozenSpace() []byte {
	return append(append(append([]byte{}, b.prefix...), subMeta), frozenTag)
}
func (b *baseSM) frozenKey(start []byte) []byte { return append(b.frozenSpace(), start...) }

func encodeFreeze(start, end []byte) []byte {
	buf := []byte{byte(opFreeze)}
	buf = appendChunk(buf, start)
	return appendChunk(buf, end)
}

func encodeUnfreeze(start, end []byte) []byte {
	buf := []byte{byte(opUnfreeze)}
	buf = appendChunk(buf, start)
	return appendChunk(buf, end)
}

// frozenRange is one frozen [start,end) (empty bounds = ±inf).
type frozenRange struct{ start, end []byte }

// loadFrozen reads all frozen ranges from the store (usually empty; fast path is len 0).
func (b *baseSM) loadFrozen() ([]frozenRange, error) {
	fs := b.frozenSpace()
	it, err := b.store.Scan(storage.CFReplData, fs, prefixEnd(fs), 0)
	if err != nil {
		return nil, fatal(err)
	}
	defer func() { _ = it.Close() }()
	var out []frozenRange
	for it.Valid() {
		out = append(out, frozenRange{
			start: append([]byte(nil), it.Key()[len(fs):]...),
			end:   append([]byte(nil), it.Value()...),
		})
		it.Next()
	}
	return out, fatal(it.Err())
}

func frozenCovers(ranges []frozenRange, key []byte) bool {
	for _, r := range ranges {
		if (len(r.start) == 0 || bytes.Compare(key, r.start) >= 0) &&
			(len(r.end) == 0 || bytes.Compare(key, r.end) < 0) {
			return true
		}
	}
	return false
}

// applyFreeze handles opFreeze/opUnfreeze in a data shard's Update.
func (u *updateCtx) applyFreeze(cmd []byte) error {
	start, end, err := decodePurge(cmd) // op byte + chunk(start) + chunk(end)
	if err != nil {
		return err
	}
	if opKind(cmd[0]) == opFreeze {
		u.ops = append(u.ops, storage.StoreOp{CF: storage.CFReplData, Key: u.s.frozenKey(start), Value: append([]byte(nil), end...)})
	} else {
		u.ops = append(u.ops, storage.StoreOp{CF: storage.CFReplData, Key: u.s.frozenKey(start), Delete: true})
	}
	return nil
}
