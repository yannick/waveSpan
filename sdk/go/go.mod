// Module github.com/yannick/wavespan/sdk/go is the ergonomic Go client SDK for WaveSpan. It wraps the
// generated Connect stubs (vendored under internal/gen, regenerated from ../../proto via
// `buf generate --template sdk/go/buf.gen.yaml`) with typed methods, transport/auth management, and
// stream→iterator adapters.
//
// It depends ONLY on protobuf + Connect — never on the server module (which is private and carries a
// local `replace wavesdb`) — so `go get github.com/yannick/wavespan/sdk/go` stays clean and
// self-contained.
module github.com/yannick/wavespan/sdk/go

go 1.26.4

require (
	connectrpc.com/connect v1.20.0
	google.golang.org/protobuf v1.36.11
)
