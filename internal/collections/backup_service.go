package collections

import (
	"context"
	"errors"
	"net/http"

	"connectrpc.com/connect"

	"github.com/yannick/wavespan/internal/rpcopts"
	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
	"github.com/yannick/wavespan/proto/wavespan/v1/wavespanv1connect"
)

// backupCoordinator is the subset of the backup engine the BackupService RPCs delegate to. It is an
// interface (not a direct *backup.Coordinator) to avoid an import cycle: internal/backup already imports
// internal/collections (for NamespaceCollectionOfKey/RerouteSuffix on the restore path), so collections
// cannot import backup. The coordinator speaks proto types directly, keeping this service layer a thin
// transport adapter. main.go (which imports both) supplies the concrete *backup.Coordinator.
type backupCoordinator interface {
	BeginBackup(ctx context.Context, spec *wavespanv1.BackupSpec) (string, error)
	BackupStatus(ctx context.Context, backupID string) (*wavespanv1.BackupState, error)
	ListBackups(ctx context.Context) ([]*wavespanv1.BackupSummary, error)
	DeleteBackup(ctx context.Context, backupID string) (bool, error)
	PrepareLocal(ctx context.Context, req *wavespanv1.PrepareBackupRequest) (*wavespanv1.PrepareBackupResult, error)
	ExportLocal(ctx context.Context, req *wavespanv1.ExportBackupRequest) (*wavespanv1.ExportBackupResult, error)
}

// WithBackup wires the backup Coordinator into the service: this node can then coordinate cluster
// backups (BeginBackup/BackupStatus/ListBackups/DeleteBackup) and serve the node-internal
// PrepareBackup/ExportBackup RPCs fanned out by a coordinator (design/backup phase 3a). The same
// *Service backs CollectionService, BudgetService, and BackupService — one engine, one error mapper.
func (s *Service) WithBackup(c backupCoordinator) *Service {
	s.backup = c
	return s
}

// BackupHandler returns the mountable Connect handler (path, handler) for the BackupService admin API.
func (s *Service) BackupHandler() (string, http.Handler) {
	return wavespanv1connect.NewBackupServiceHandler(s, rpcopts.Handler()...)
}

// errBackupUnconfigured is returned when a backup RPC reaches a node with no Coordinator wired.
var errBackupUnconfigured = errors.New("collections: backup coordinator not configured on this node")

// BeginBackup records a durable BackupIntent, picks a cluster frontier, and drives the phased backup.
func (s *Service) BeginBackup(ctx context.Context, req *connect.Request[wavespanv1.BeginBackupRequest]) (*connect.Response[wavespanv1.BeginBackupResult], error) {
	if s.backup == nil {
		return nil, connect.NewError(connect.CodeUnimplemented, errBackupUnconfigured)
	}
	id, err := s.backup.BeginBackup(ctx, req.Msg.GetSpec())
	if err != nil {
		return nil, collErr(err)
	}
	return connect.NewResponse(&wavespanv1.BeginBackupResult{BackupId: id, Meta: s.meta()}), nil
}

// BackupStatus reports a backup's current state (status, phase, per-node progress, gaps).
func (s *Service) BackupStatus(ctx context.Context, req *connect.Request[wavespanv1.BackupStatusRequest]) (*connect.Response[wavespanv1.BackupState], error) {
	if s.backup == nil {
		return nil, connect.NewError(connect.CodeUnimplemented, errBackupUnconfigured)
	}
	st, err := s.backup.BackupStatus(ctx, req.Msg.GetBackupId())
	if err != nil {
		return nil, collErr(err)
	}
	st.Meta = s.meta()
	return connect.NewResponse(st), nil
}

// ListBackups lists known backups from the meta-shard catalog.
func (s *Service) ListBackups(ctx context.Context, req *connect.Request[wavespanv1.ListBackupsRequest]) (*connect.Response[wavespanv1.ListBackupsResult], error) {
	if s.backup == nil {
		return nil, connect.NewError(connect.CodeUnimplemented, errBackupUnconfigured)
	}
	list, err := s.backup.ListBackups(ctx)
	if err != nil {
		return nil, collErr(err)
	}
	return connect.NewResponse(&wavespanv1.ListBackupsResult{Backups: list, Meta: s.meta()}), nil
}

// DeleteBackup removes a backup's catalog intent (object GC is Phase 3d).
func (s *Service) DeleteBackup(ctx context.Context, req *connect.Request[wavespanv1.DeleteBackupRequest]) (*connect.Response[wavespanv1.DeleteBackupResult], error) {
	if s.backup == nil {
		return nil, connect.NewError(connect.CodeUnimplemented, errBackupUnconfigured)
	}
	deleted, err := s.backup.DeleteBackup(ctx, req.Msg.GetBackupId())
	if err != nil {
		return nil, collErr(err)
	}
	return connect.NewResponse(&wavespanv1.DeleteBackupResult{Deleted: deleted, Meta: s.meta()}), nil
}

// PrepareBackup (node-internal) seals this node's view at the frontier and reports its held ranges.
func (s *Service) PrepareBackup(ctx context.Context, req *connect.Request[wavespanv1.PrepareBackupRequest]) (*connect.Response[wavespanv1.PrepareBackupResult], error) {
	if s.backup == nil {
		return nil, connect.NewError(connect.CodeUnimplemented, errBackupUnconfigured)
	}
	res, err := s.backup.PrepareLocal(ctx, req.Msg)
	if err != nil {
		return nil, collErr(err)
	}
	res.Meta = s.meta()
	return connect.NewResponse(res), nil
}

// ExportBackup (node-internal) exports this node's assignment to the object store.
func (s *Service) ExportBackup(ctx context.Context, req *connect.Request[wavespanv1.ExportBackupRequest]) (*connect.Response[wavespanv1.ExportBackupResult], error) {
	if s.backup == nil {
		return nil, connect.NewError(connect.CodeUnimplemented, errBackupUnconfigured)
	}
	res, err := s.backup.ExportLocal(ctx, req.Msg)
	if err != nil {
		return nil, collErr(err)
	}
	res.Meta = s.meta()
	return connect.NewResponse(res), nil
}
