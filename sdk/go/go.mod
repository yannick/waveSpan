// Module github.com/yannick/wavespan/sdk/go is the ergonomic Go client SDK for WaveSpan. It wraps the
// generated grpc-go stubs (vendored under internal/gen, regenerated from ../../proto via
// `buf generate --template sdk/go/buf.gen.yaml`) with typed methods, transport/auth management, and
// stream→iterator adapters.
//
// It depends ONLY on protobuf + grpc-go — never on the server module (which is private and carries a
// local `replace wavesdb`) — so `go get github.com/yannick/wavespan/sdk/go` stays clean and
// self-contained.
module github.com/yannick/wavespan/sdk/go

go 1.26.4

require (
	golang.org/x/sys v0.46.0
	google.golang.org/grpc v1.81.1
	google.golang.org/protobuf v1.36.11
)

require (
	golang.org/x/net v0.56.0 // indirect
	golang.org/x/text v0.38.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260226221140-a57be14db171 // indirect
)
