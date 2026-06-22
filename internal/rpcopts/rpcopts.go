// Package rpcopts centralizes the shared Connect handler/client options so every WaveSpan service
// is wired consistently.
package rpcopts

import "connectrpc.com/connect"

// CompressMinBytes is the response-size threshold below which Connect skips gzip. KV records,
// latest pointers, and most point responses are well under 1 KiB, so they go out uncompressed —
// gzip on tiny payloads is pure CPU + allocation with no meaningful size benefit (it was the top
// heap allocator on the KV hot path, ~23% of write-load allocation). Larger scan and vector
// responses still compress, since they cross the threshold.
const CompressMinBytes = 1024

// Handler returns the shared Connect handler options.
func Handler() []connect.HandlerOption {
	return []connect.HandlerOption{connect.WithCompressMinBytes(CompressMinBytes)}
}

// Client returns the shared Connect client options.
func Client() []connect.ClientOption {
	return []connect.ClientOption{connect.WithCompressMinBytes(CompressMinBytes)}
}
