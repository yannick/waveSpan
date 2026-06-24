package grpcsrv

import (
	"testing"

	"github.com/yannick/wavespan/internal/security"
)

func TestNewReturnsServer(t *testing.T) {
	srv := New(Options{Identity: security.Identity{DevMode: true}})
	if srv == nil {
		t.Fatal("New returned nil *grpc.Server")
	}
	srv.Stop()
}
