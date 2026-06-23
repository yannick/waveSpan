package backup

import (
	"bufio"
	"encoding/binary"
	"io"

	"github.com/yannick/wavespan/internal/storage"
)

func readBytes(r *bufio.Reader) ([]byte, error) {
	n, err := binary.ReadUvarint(r)
	if err != nil {
		return nil, err
	}
	b := make([]byte, n)
	if _, err := io.ReadFull(r, b); err != nil {
		return nil, err
	}
	return b, nil
}

// Restore reads a backup entry stream and writes it into a fresh store. After restore, the caller
// rebuilds derived vector ANN indexes from the restored raw records (vector.RebuildLiveIndex), so a
// backup that omitted ANN segments still yields working approximate search.
func Restore(dst storage.LocalStore, r io.Reader) (*Manifest, error) {
	br := bufio.NewReader(r)
	count := 0
	const batchSize = 1000
	var ops []storage.StoreOp
	flush := func() error {
		if len(ops) == 0 {
			return nil
		}
		if err := dst.Batch(ops); err != nil {
			return err
		}
		ops = ops[:0]
		return nil
	}
	for {
		cf, err := binary.ReadUvarint(br)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		key, err := readBytes(br)
		if err != nil {
			return nil, err
		}
		val, err := readBytes(br)
		if err != nil {
			return nil, err
		}
		ops = append(ops, storage.StoreOp{CF: storage.ColumnFamily(cf), Key: key, Value: val})
		count++
		if len(ops) >= batchSize {
			if err := flush(); err != nil {
				return nil, err
			}
		}
	}
	if err := flush(); err != nil {
		return nil, err
	}
	return &Manifest{Entries: count}, nil
}
