// Package security implements WaveSpan's transport security and authorization (design/15): mTLS,
// the five-role model with internal/public separation, key/value redaction, peer audit, and
// per-peer rate limiting. The headline guarantee is that replication credentials cannot drive
// public writes.
package security

// Role is an authorization role (design/15 "Roles").
type Role string

// The five roles.
const (
	RoleAdmin      Role = "admin"      // everything
	RoleReader     Role = "reader"     // KV get/scan, Cypher read
	RoleWriter     Role = "writer"     // KV put/delete, Cypher writes
	RoleReplicator Role = "replicator" // internal replication ONLY (never public writes)
	RoleOperator   Role = "operator"   // admin lifecycle
	RoleNone       Role = ""           // unauthenticated
)

// Surface classifies an API surface (design/15 "Internal vs public separation").
type Surface int

// API surfaces.
const (
	// SurfacePublicRead is the public read API (KV Get/Scan, Cypher read).
	SurfacePublicRead Surface = iota
	// SurfacePublicWrite is the public write API (KV Put/Delete, Cypher write).
	SurfacePublicWrite
	// SurfaceInternal is the internal replication API (StoreReplica, PushGlobal, FetchReplica).
	SurfaceInternal
	// SurfaceAdmin is the admin/lifecycle API.
	SurfaceAdmin
)

// Allowed reports whether a role may call a surface. The critical rule: replicator may call ONLY
// the internal surface — never public writes; and the internal surface accepts only replicator and
// admin (no anonymous or public-only role).
func Allowed(role Role, surface Surface) bool {
	switch surface {
	case SurfacePublicRead:
		return role == RoleReader || role == RoleWriter || role == RoleAdmin
	case SurfacePublicWrite:
		// replicator is deliberately excluded: replication creds cannot drive public writes.
		return role == RoleWriter || role == RoleAdmin
	case SurfaceInternal:
		return role == RoleReplicator || role == RoleAdmin
	case SurfaceAdmin:
		return role == RoleOperator || role == RoleAdmin
	default:
		return false
	}
}

// SurfaceForProcedure maps a Connect procedure path to its surface (design/15). Unknown procedures
// default to admin (deny-by-default for unclassified internal calls).
func SurfaceForProcedure(procedure string) Surface {
	switch procedure {
	case "/wavespan.v1.KvService/Get", "/wavespan.v1.KvService/Scan", "/wavespan.v1.Cypher/Query",
		"/wavespan.v1.ObservabilityService/StreamGossip", "/wavespan.v1.ObservabilityService/InspectLocal",
		"/wavespan.v1.ObservabilityService/InspectGlobal", "/wavespan.v1.ObservabilityService/GetClusterView",
		"/wavespan.v1.ObservabilityService/GraphExplore":
		return SurfacePublicRead
	case "/wavespan.v1.KvService/Put", "/wavespan.v1.KvService/Delete", "/wavespan.v1.VectorService/Put":
		return SurfacePublicWrite
	case "/wavespan.v1.ReplicationService/StoreReplica", "/wavespan.v1.ReplicationService/FetchReplica",
		"/wavespan.v1.ReplicationService/SubscribeKey", "/wavespan.v1.ReplicationService/ScanLocal",
		"/wavespan.v1.GlobalReplication/PushGlobal", "/wavespan.v1.GlobalReplication/RangeSummary",
		"/wavespan.v1.GlobalReplication/FetchRange":
		return SurfaceInternal
	default:
		return SurfaceAdmin
	}
}
