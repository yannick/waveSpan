package collections

import (
	"context"
	"testing"

	"connectrpc.com/connect"

	"github.com/yannick/wavespan/internal/membership"
	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
)

// TestBackupServiceUnconfigured proves the dual-transport wiring: a *Service with no Coordinator wired
// still satisfies the BackupServiceHandler interface and answers every RPC with CodeUnimplemented
// (rather than panicking or 404ing). This pins the skeleton before the coordinator lands.
func TestBackupServiceUnconfigured(t *testing.T) {
	svc := NewService(nil, membership.Member{})
	ctx := context.Background()

	if _, err := svc.ListBackups(ctx, connect.NewRequest(&wavespanv1.ListBackupsRequest{})); connect.CodeOf(err) != connect.CodeUnimplemented {
		t.Fatalf("ListBackups code = %v, want Unimplemented", connect.CodeOf(err))
	}
	if _, err := svc.BeginBackup(ctx, connect.NewRequest(&wavespanv1.BeginBackupRequest{})); connect.CodeOf(err) != connect.CodeUnimplemented {
		t.Fatalf("BeginBackup code = %v, want Unimplemented", connect.CodeOf(err))
	}
	if _, err := svc.BackupStatus(ctx, connect.NewRequest(&wavespanv1.BackupStatusRequest{})); connect.CodeOf(err) != connect.CodeUnimplemented {
		t.Fatalf("BackupStatus code = %v, want Unimplemented", connect.CodeOf(err))
	}

	// The handler must be mountable (proves NewBackupServiceHandler accepts *Service).
	if path, h := svc.BackupHandler(); path == "" || h == nil {
		t.Fatalf("BackupHandler returned empty path/handler")
	}
}
