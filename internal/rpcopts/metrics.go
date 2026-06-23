package rpcopts

import (
	"context"
	"strings"

	"connectrpc.com/connect"
	"github.com/prometheus/client_golang/prometheus"
)

// rpcRequests counts every inbound RPC by short method name and access kind (read/write/other), so
// QPS, reads/sec and writes/sec are all derivable via rate() over one counter. Nil until
// InstallMetrics wires it, in which case Handler() omits the interceptor (e.g. in unit tests).
var rpcRequests *prometheus.CounterVec

// InstallMetrics registers the RPC request counter and enables the server-side metrics interceptor on
// every handler built through Handler(). Call once at startup, before any service handler is built.
func InstallMetrics(reg *prometheus.Registry) {
	rpcRequests = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "wavespan_rpc_requests_total",
		Help: "Inbound RPCs by method and access kind (read/write/other).",
	}, []string{"method", "kind"})
	reg.MustRegister(rpcRequests)
}

// shortMethod reduces a Connect procedure ("/wavespan.v1.KvService/Put") to its method ("Put").
func shortMethod(procedure string) string {
	if i := strings.LastIndexByte(procedure, '/'); i >= 0 && i+1 < len(procedure) {
		return procedure[i+1:]
	}
	return procedure
}

// classify buckets a method into read/write/other by name. Write keywords win over read keywords so
// e.g. SetTunable and StoreReplica classify as writes; VectorGet/VectorSearch as reads.
func classify(method string) string {
	containsAny := func(s string, subs ...string) bool {
		for _, sub := range subs {
			if strings.Contains(s, sub) {
				return true
			}
		}
		return false
	}
	switch {
	case containsAny(method, "Put", "Delete", "Store", "Write", "Set", "Apply", "Train"):
		return "write"
	case containsAny(method, "Get", "Scan", "Search", "Query", "Read", "List", "Sample", "Inspect", "Explore", "View", "Subgraph"):
		return "read"
	default:
		return "other" // gossip exchange, subscribe streams, health, …
	}
}

func observeRPC(procedure string) {
	if rpcRequests == nil {
		return
	}
	m := shortMethod(procedure)
	rpcRequests.WithLabelValues(m, classify(m)).Inc()
}

// metricsInterceptor counts each unary call and each streaming handler invocation.
type metricsInterceptor struct{}

func (metricsInterceptor) WrapUnary(next connect.UnaryFunc) connect.UnaryFunc {
	return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
		observeRPC(req.Spec().Procedure)
		return next(ctx, req)
	}
}

func (metricsInterceptor) WrapStreamingClient(next connect.StreamingClientFunc) connect.StreamingClientFunc {
	return next // client-side streams aren't counted by the server metrics
}

func (metricsInterceptor) WrapStreamingHandler(next connect.StreamingHandlerFunc) connect.StreamingHandlerFunc {
	return func(ctx context.Context, conn connect.StreamingHandlerConn) error {
		observeRPC(conn.Spec().Procedure)
		return next(ctx, conn)
	}
}
