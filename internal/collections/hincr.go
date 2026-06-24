package collections

import (
	"encoding/binary"
	"math"
	"strconv"

	"github.com/yannick/wavespan/internal/storage"
)

// Atomic hash-field counters (HINCRBY / HINCRBYFLOAT, design/30 §13.5). The field value is stored as a
// decimal string (so HGet returns it verbatim); the increment parses it, adds the delta, and writes it
// back — all in one Raft entry, so concurrent increments are exact (no lost updates / LWW loss). The
// new value is returned to the caller via Result.Data; a non-numeric field yields the notNumber
// sentinel. The integer delta rides in item.Val (8-byte int64); the float delta rides in item.Score.

// fieldVal returns a hash field's current value, honoring in-batch overlays (so two increments of the
// same field in one Raft batch compose).
func (u *updateCtx) fieldVal(ek []byte) ([]byte, bool, error) {
	if v, ok := u.vals[string(ek)]; ok {
		return v, v != nil, nil // nil = deleted earlier in this batch
	}
	v, found, err := u.s.getData(ek)
	if err != nil || !found {
		return nil, false, err
	}
	return v, true, nil
}

// setFieldVal records a field's new value in the overlay and the batch.
func (u *updateCtx) setFieldVal(ek, val []byte) {
	u.vals[string(ek)] = val
	u.ops = append(u.ops, storage.StoreOp{CF: storage.CFReplData, Key: ek, Value: val})
}

// applyHIncrInt applies one HINCRBY. Returns (newFields, encoded-new-value); notNumber data when the
// existing value isn't a base-10 integer.
func (u *updateCtx) applyHIncrInt(c command, it item) (int64, []byte, error) {
	if len(it.Val) < 8 { // corrupt HINCRBY delta (encoder always writes an 8-byte int64) — skip (non-fatal)
		return 0, nil, errShortCommand
	}
	ek := u.s.elemKey(c.NS, c.Coll, it.Key)
	cur, found, err := u.fieldVal(ek)
	if err != nil {
		return 0, nil, err
	}
	var base int64
	if found {
		b, perr := strconv.ParseInt(string(cur), 10, 64)
		if perr != nil {
			return 0, notNumber, nil
		}
		base = b
	}
	nv := base + int64(binary.BigEndian.Uint64(it.Val))
	u.setFieldVal(ek, []byte(strconv.FormatInt(nv, 10)))
	changed := u.touchField(c, ek, found)
	out := make([]byte, 8)
	binary.BigEndian.PutUint64(out, uint64(nv))
	return changed, out, nil
}

// applyHIncrFloat applies one HINCRBYFLOAT (delta in it.Score). Returns the encoded float64 new value.
func (u *updateCtx) applyHIncrFloat(c command, it item) (int64, []byte, error) {
	ek := u.s.elemKey(c.NS, c.Coll, it.Key)
	cur, found, err := u.fieldVal(ek)
	if err != nil {
		return 0, nil, err
	}
	var base float64
	if found {
		f, perr := strconv.ParseFloat(string(cur), 64)
		if perr != nil {
			return 0, notNumber, nil
		}
		base = f
	}
	nv := base + it.Score
	u.setFieldVal(ek, []byte(strconv.FormatFloat(nv, 'g', -1, 64)))
	changed := u.touchField(c, ek, found)
	out := make([]byte, 8)
	binary.BigEndian.PutUint64(out, math.Float64bits(nv))
	return changed, out, nil
}

// touchField bumps the cardinality when a counter creates a new field.
func (u *updateCtx) touchField(c command, ek []byte, existed bool) int64 {
	u.exists[string(ek)] = true
	if existed {
		return 0
	}
	u.cardDelta[string(u.s.cardKey(c.NS, c.Coll))]++
	return 1
}
