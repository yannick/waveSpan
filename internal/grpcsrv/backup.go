package grpcsrv

import (
	"context"

	"connectrpc.com/connect"

	"github.com/yannick/wavespan/internal/collections"
	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
)

// Backup is the gRPC BackupServiceServer adapter (design/backup phase 3a). Like the Budget adapter, it
// delegates to the same *collections.Service Connect core — the coordinator, intent catalog, error
// mapping, and ResponseMeta all live there — translating Connect codes to gRPC status codes. One core,
// two transports: the admin RPCs are reached over Connect (UI) and the node-internal PrepareBackup/
// ExportBackup over gRPC (the data plane the coordinator fans out on).
type Backup struct {
	wavespanv1.UnimplementedBackupServiceServer
	svc *collections.Service
}

// NewBackup wires the gRPC BackupService adapter over the existing collections service core.
func NewBackup(svc *collections.Service) *Backup {
	return &Backup{svc: svc}
}

// BeginBackup implements the BackupServiceServer gRPC method by delegating to the Connect service.
func (s *Backup) BeginBackup(ctx context.Context, m *wavespanv1.BeginBackupRequest) (*wavespanv1.BeginBackupResult, error) {
	res, err := s.svc.BeginBackup(ctx, connect.NewRequest(m))
	if err != nil {
		return nil, connectToGRPC(err)
	}
	return res.Msg, nil
}

// BackupStatus implements the BackupServiceServer gRPC method by delegating to the Connect service.
func (s *Backup) BackupStatus(ctx context.Context, m *wavespanv1.BackupStatusRequest) (*wavespanv1.BackupState, error) {
	res, err := s.svc.BackupStatus(ctx, connect.NewRequest(m))
	if err != nil {
		return nil, connectToGRPC(err)
	}
	return res.Msg, nil
}

// ListBackups implements the BackupServiceServer gRPC method by delegating to the Connect service.
func (s *Backup) ListBackups(ctx context.Context, m *wavespanv1.ListBackupsRequest) (*wavespanv1.ListBackupsResult, error) {
	res, err := s.svc.ListBackups(ctx, connect.NewRequest(m))
	if err != nil {
		return nil, connectToGRPC(err)
	}
	return res.Msg, nil
}

// DeleteBackup implements the BackupServiceServer gRPC method by delegating to the Connect service.
func (s *Backup) DeleteBackup(ctx context.Context, m *wavespanv1.DeleteBackupRequest) (*wavespanv1.DeleteBackupResult, error) {
	res, err := s.svc.DeleteBackup(ctx, connect.NewRequest(m))
	if err != nil {
		return nil, connectToGRPC(err)
	}
	return res.Msg, nil
}

// PrepareBackup implements the BackupServiceServer gRPC method by delegating to the Connect service.
func (s *Backup) PrepareBackup(ctx context.Context, m *wavespanv1.PrepareBackupRequest) (*wavespanv1.PrepareBackupResult, error) {
	res, err := s.svc.PrepareBackup(ctx, connect.NewRequest(m))
	if err != nil {
		return nil, connectToGRPC(err)
	}
	return res.Msg, nil
}

// ExportBackup implements the BackupServiceServer gRPC method by delegating to the Connect service.
func (s *Backup) ExportBackup(ctx context.Context, m *wavespanv1.ExportBackupRequest) (*wavespanv1.ExportBackupResult, error) {
	res, err := s.svc.ExportBackup(ctx, connect.NewRequest(m))
	if err != nil {
		return nil, connectToGRPC(err)
	}
	return res.Msg, nil
}
