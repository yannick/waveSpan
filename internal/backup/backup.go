// Package backup implements a record-level backup/restore prototype (design/15, design/08). It
// snapshots the authoritative column families; derived ANN vector indexes are NEVER backed up —
// they live in memory and are rebuilt from the raw vector records on restore. (Production backup
// uses wavesdb object-store mode + PromoteToPrimary; this is the logical equivalent.)
package backup

import (
	"bufio"
	"encoding/binary"
	"io"

	"github.com/yannick/wavespan/internal/storage"
)

// authoritativeCFs are the column families backed up. cache_meta and repl_log are excluded
// (derived / replayable); ANN segments are not stored (rebuilt from vector_raw).
var authoritativeCFs = []storage.ColumnFamily{
	storage.CFSys, storage.CFKVData, storage.CFKVMeta,
	storage.CFGraphData, storage.CFGraphIndex,
	storage.CFVectorRaw, storage.CFVectorIndex,
}

// Manifest summarizes a backup.
type Manifest struct {
	Entries int
}

func putUvarint(w *bufio.Writer, v uint64) {
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(tmp[:], v)
	_, _ = w.Write(tmp[:n])
}

func writeBytes(w *bufio.Writer, b []byte) {
	putUvarint(w, uint64(len(b)))
	_, _ = w.Write(b)
}

// Backup scans the authoritative column families and writes a length-prefixed entry stream
// (cf, key, value) to w. includeVectorIndexes is accepted for CRD compatibility but ANN segments
// are never in storage, so they are never included regardless.
func Backup(src storage.LocalStore, w io.Writer, _ bool) (*Manifest, error) {
	bw := bufio.NewWriter(w)
	count := 0
	for _, cf := range authoritativeCFs {
		it, err := src.Scan(cf, nil, nil, 0)
		if err != nil {
			return nil, err
		}
		for it.Valid() {
			putUvarint(bw, uint64(cf))
			writeBytes(bw, it.Key())
			writeBytes(bw, it.Value())
			count++
			it.Next()
		}
		cerr := it.Err()
		_ = it.Close()
		if cerr != nil {
			return nil, cerr
		}
	}
	if err := bw.Flush(); err != nil {
		return nil, err
	}
	return &Manifest{Entries: count}, nil
}
