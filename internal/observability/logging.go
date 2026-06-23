// Package observability provides structured logging, a Prometheus metrics registry, and
// process health/readiness handlers for WaveSpan binaries (design/14_observability.md).
package observability

import (
	"log/slog"
	"os"

	"github.com/yannick/wavespan/internal/config"
)

// NewLogger builds a slog.Logger: JSON output under the kubernetes runtime, human-readable
// text in dev. clusterId and memberId are attached as default attributes so every line is
// attributable (design/14 "Structured logs").
func NewLogger(runtime config.Runtime, clusterID, memberID string) *slog.Logger {
	var handler slog.Handler
	opts := &slog.HandlerOptions{Level: slog.LevelInfo}
	if runtime == config.RuntimeKubernetes {
		handler = slog.NewJSONHandler(os.Stdout, opts)
	} else {
		handler = slog.NewTextHandler(os.Stdout, opts)
	}
	return slog.New(handler).With(
		slog.String("cluster_id", clusterID),
		slog.String("member_id", memberID),
	)
}
