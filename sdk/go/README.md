# WaveSpan Go SDK

The official Go client for [WaveSpan](../../README.md) — a Kubernetes-native, eventually-consistent
distributed KV / graph / vector store. It wraps the generated Connect stubs with typed, ergonomic
methods so you never touch request envelopes or stream plumbing directly.

```go
import wavespan "github.com/yannick/wavespan/sdk/go"
```

## Why a separate module

The WaveSpan server module (`github.com/yannick/wavespan`) is private and pins its storage engine via a
local `replace` directive, so it is not `go get`-able. This SDK is its **own module** that vendors a
private copy of the protobuf/Connect stubs (under `internal/gen`) and depends on **only**
`connectrpc.com/connect` + `google.golang.org/protobuf`. Importing it never drags in the server or
storage engine.

## Install

```sh
go get github.com/yannick/wavespan/sdk/go
```

## Quick start

```go
ctx := context.Background()

c, err := wavespan.Dial(wavespan.Options{Endpoint: "localhost:7800"})
if err != nil {
	log.Fatal(err)
}
defer c.Close()

// Key/Value
if _, err := c.Put(ctx, "users", []byte("u1"), []byte("alice")); err != nil {
	log.Fatal(err)
}
rec, err := c.Get(ctx, "users", []byte("u1"))
if err == nil && rec.Found {
	fmt.Printf("%s (served by %s, %s)\n", rec.Value, rec.Meta.GetServedByMemberId(), rec.Meta.GetSource())
}
```

A runnable version is in [`examples/quickstart`](./examples/quickstart):

```sh
go run ./examples/quickstart --addr localhost:7800
```

## API surface

The SDK covers the **data-plane** services exposed on a node's data port (default `:7800`).

### Key/Value

```go
c.Put(ctx, ns, key, value, wavespan.WithTTL(60_000))      // origin+1 by default
c.Get(ctx, ns, key)                                        // Record{Found, Value, ExpiresAt, Meta}
c.MultiGet(ctx, ns, [][]byte{k1, k2})                      // one round-trip
c.Delete(ctx, ns, key, wavespan.WithoutOriginPlusOne())

scan, _ := c.Scan(ctx, ns, wavespan.WithScanMode(wavespan.ScanRoutedEventual))
fmt.Println(scan.Completeness())                           // honest header completeness
for row, err := range scan.Rows() {                        // streaming → range-over-func
	if err != nil { /* … */ }
	fmt.Printf("%s = %s\n", row.Key, row.Value)
}
fmt.Println(scan.FinalCompleteness(), scan.Warnings())     // trailer, after draining
```

Write options: `WithoutOriginPlusOne`, `WithTTL`, `WithIdempotencyKey`.
Read options: `WithoutDynamicCache`, `WithHideExpired`.
Scan options: `WithRange`, `WithLimit`, `WithScanMode`.

### Vectors (vector-as-key)

```go
v := c.Vector()
v.Put(ctx, "embeddings", queryVec, payload)
res, _ := v.Search(ctx, "embeddings", queryVec, 10,
	wavespan.WithNProbe(8), wavespan.WithRerank(), wavespan.WithPayload())
for _, n := range res.Neighbors {
	fmt.Println(n.VectorID, n.Score)
}
```

### Cypher (graph)

Parameters and row values convert to/from idiomatic Go values (`nil`, `bool`, `int64`, `float64`,
`string`, `[]byte`, `[]any`, `map[string]any`):

```go
q, _ := c.Query(ctx, "social", "MATCH (u:User {id:$id}) RETURN u.name AS name",
	map[string]any{"id": "u1"})
for row, err := range q.Rows() {
	if err != nil { /* … */ }
	fmt.Println(row["name"]) // row is map[string]any
}
fmt.Println(q.Meta().GetCompleteness())
```

## Honest consistency metadata

WaveSpan is eventually consistent, and the SDK never hides it: every read carries a `ResponseMeta`
(serving node, read source, completeness, conflict state) and scans/searches report `Completeness`
(`COMPLETE` / `PARTIAL` / `BEST_EFFORT`).

## Errors

Transport failures are returned as `*wavespan.Error` (preserving the Connect status code). Helpers:
`wavespan.IsNotFound(err)`, `wavespan.IsUnavailable(err)`, `wavespan.CodeOf(err)`. Note: a missing KV
key is reported via `Record.Found`, not as an error.

## TLS & auth

```go
c, _ := wavespan.Dial(wavespan.Options{
	Endpoint: "node.example.com:7800",
	TLS:      &tls.Config{ /* … */ },   // switches to https
	Token:    os.Getenv("WAVESPAN_TOKEN"), // Authorization: Bearer …
})
```

For HTTP/2 cleartext (h2c), supply your own `Options.HTTPClient`; the default client uses HTTP/1.1,
which the Connect protocol supports for both unary and server-streaming calls.

## Regenerating the stubs

The `.proto` files in [`../../proto`](../../proto) are the single source of truth. After changing
them (and running `buf generate` for the server), refresh the SDK's vendored copy from the repo root:

```sh
buf generate --template sdk/go/buf.gen.yaml     # or: make sdk-proto / just sdk-proto
```

## Other languages

The same `.proto` contract drives the in-browser TypeScript client today; a Python SDK can follow the
same shape (generate stubs with a Connect/protobuf Python plugin, wrap them ergonomically). See the
repo-level SDK notes for the cross-language plan.
