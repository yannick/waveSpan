package collections

import (
	"encoding/binary"
	"errors"
)

// Budget sub-scope bytes. Existing collScope sub-scopes are 0x00..0x04 (scopeCard/Elem/ZPtr/Type in
// statemachine.go:36-39, scopeTTLPtr in ttl.go:17), so 0x05 is the first free byte.
const (
	scopeBudPool  byte = 0x05 // the pool record (combined cfg+state; append-extensible)
	scopeBudLease byte = 0x06 // <leaseID> -> leaseRec
	// reserved (NOT used in Stage 1): 0x07 lease-expiry index (Stage 2), 0x08 settled tombstone (Stage 3)
)

// Budget modes (Stage 1 ships STRICT only; modeRelaxed reserved for Stage 4 and rejected at init here).
const (
	modeStrict  uint8 = 1
	modeRelaxed uint8 = 2
)

// Sentinels returned in ProposeResult.Data (mirror wrongType/notNumber in command.go:42-44).
var (
	budNoCapacity = []byte("BUDNOCAP")    // grant could not allocate (>0) in STRICT
	budExists     = []byte("BUDEXISTS")   // init on an existing pool
	budNoLease    = []byte("BUDNOLEASE")  // report/return on unknown lease
	budNoBudget   = []byte("BUDNOBUDGET") // grant against a pool that does not exist (B9: distinct from budNoLease)
	budBadMode    = []byte("BUDBADMODE")  // init with a non-STRICT mode, or invalid cap (B3/B4)
)

var (
	errShortPool  = errors.New("collections: short budget pool record")
	errShortLease = errors.New("collections: short budget lease record")
)

// poolRec is the budget pool. Quantities are int64 micro-units.
type poolRec struct {
	Cap       int64
	Available int64
	LeasedOut int64 // Σ (lease.amount - lease.spent) over outstanding leases (UNSPENT held)
	Spent     int64 // Σ lease.spent (cumulative consumed)
	Epoch     uint64
	Mode      uint8
	Rate      int64 // micro-units/sec; Stage 1 ignores (0 = no pacing)
	Burst     int64 // ceiling; Stage 1: == Cap
}

func (s *shardSM) budPoolKey(ns, coll []byte) []byte {
	return append(s.collScope(ns, coll), scopeBudPool)
}
func (s *shardSM) budLeasePrefix(ns, coll []byte) []byte {
	return append(s.collScope(ns, coll), scopeBudLease)
}
func (s *shardSM) budLeaseKey(ns, coll, leaseID []byte) []byte {
	return append(s.budLeasePrefix(ns, coll), leaseID...)
}

func putI64(b []byte, v int64) { binary.BigEndian.PutUint64(b, uint64(v)) }
func getI64(b []byte) int64    { return int64(binary.BigEndian.Uint64(b)) }

func encodePool(p poolRec) []byte {
	b := make([]byte, 8*4+8+1+8*2) // cap,avail,leased,spent | epoch | mode | rate,burst
	putI64(b[0:], p.Cap)
	putI64(b[8:], p.Available)
	putI64(b[16:], p.LeasedOut)
	putI64(b[24:], p.Spent)
	binary.BigEndian.PutUint64(b[32:], p.Epoch)
	b[40] = p.Mode
	putI64(b[41:], p.Rate)
	putI64(b[49:], p.Burst)
	return b
}

// decodePool is APPEND-TOLERANT (B1): it reads only the fixed 57-byte prefix and ignores any trailing
// bytes, so Stages 2-3 may append fields (lastRefillMs, tokens, pendingReclaim) to the same record with
// NO snapshot migration. Contract: append-only — never reorder or resize an existing field.
func decodePool(b []byte) (poolRec, error) {
	if len(b) < 57 { // a shorter buffer is corruption; a longer one is a newer-stage record (tolerated)
		return poolRec{}, errShortPool
	}
	return poolRec{
		Cap: getI64(b[0:]), Available: getI64(b[8:]), LeasedOut: getI64(b[16:]), Spent: getI64(b[24:]),
		Epoch: binary.BigEndian.Uint64(b[32:]), Mode: b[40], Rate: getI64(b[41:]), Burst: getI64(b[49:]),
	}, nil
}

// leaseRec is one outstanding lease.
type leaseRec struct {
	Holder []byte
	Amount int64
	Spent  int64
	Epoch  uint64
}

func encodeLease(l leaseRec) []byte {
	b := make([]byte, 0, 8*2+8+4+len(l.Holder))
	var num [8]byte
	putI64(num[:], l.Amount)
	b = append(b, num[:]...)
	putI64(num[:], l.Spent)
	b = append(b, num[:]...)
	binary.BigEndian.PutUint64(num[:], l.Epoch)
	b = append(b, num[:]...)
	b = appendChunk(b, l.Holder) // uint32 len-prefixed (command.go:276)
	return b
}

func decodeLease(b []byte) (leaseRec, error) {
	if len(b) < 24 {
		return leaseRec{}, errShortLease
	}
	l := leaseRec{Amount: getI64(b[0:]), Spent: getI64(b[8:]), Epoch: binary.BigEndian.Uint64(b[16:])}
	holder, _, err := takeChunk(b[24:]) // command.go:283
	if err != nil {
		return leaseRec{}, err
	}
	l.Holder = append([]byte{}, holder...)
	return l, nil
}
