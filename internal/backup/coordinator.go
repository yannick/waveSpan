package backup

// Coordinator drives a consistent cluster backup: it records a durable BackupIntent in the meta
// shard, picks a cluster HLC frontier, assigns owners, fans PrepareBackup/ExportBackup out to the
// live nodes over the BackupService client, and commits a cluster.manifest (design/backup phase 3a).
//
// The full implementation (phases, assignment, commit, resume) lands in later tasks; this declaration
// lets the dual-transport BackupService skeleton reference the type.
type Coordinator struct{}
