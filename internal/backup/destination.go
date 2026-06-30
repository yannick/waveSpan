package backup

import (
	"fmt"
	"os"

	"github.com/yannick/wavespan/internal/config"
	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
	"wavesdb/objstore"
)

// DestinationSpec is the resolution input: a request's destination, mapped from the proto, optionally
// carrying transient inline credentials for an explicit ad-hoc destination. Inline creds are used for a
// single run and never persisted or logged. They ARE carried over the wire (CredentialRef.access_key/
// secret_key on the BackupNodeService ExportBackup RPC, Phase 3c Task 0) so a remote node can resolve an
// explicit destination — that RPC is on the mTLS-authenticated data port, and only the non-secret
// descriptor + credential reference is ever persisted.
type DestinationSpec struct {
	Name            string
	Bucket          string
	Prefix          string
	Region          string
	Endpoint        string
	UseSSL          bool
	UsePathStyle    bool
	SecretRef       string // names credential env vars (<ref>_ACCESS_KEY / <ref>_SECRET_KEY) for an explicit destination
	InlineAccessKey string // transient; never persisted
	InlineSecretKey string // transient; never persisted
}

// ResolvedDestination is the concrete target to open for a backup. UseFS selects the node's local FS
// fallback (dev); otherwise S3 carries the (possibly transient) credentials for this run.
type ResolvedDestination struct {
	UseFS bool
	S3    objstore.S3Config
}

// destinationSpecFromProto maps a proto Destination to the resolution input. CredentialRef.secret_name is
// a credential env-var reference (resolved server-side); CredentialRef.access_key/secret_key are transient
// inline credentials (carried over the mTLS data port for an explicit destination, never persisted).
func destinationSpecFromProto(d *wavespanv1.Destination) DestinationSpec {
	if d == nil {
		return DestinationSpec{}
	}
	return DestinationSpec{
		Name:            d.GetName(),
		Bucket:          d.GetBucket(),
		Prefix:          d.GetPrefix(),
		Region:          d.GetRegion(),
		Endpoint:        d.GetEndpoint(),
		UseSSL:          d.GetUseSsl(),
		UsePathStyle:    d.GetUsePathStyle(),
		SecretRef:       d.GetCredential().GetSecretName(),
		InlineAccessKey: d.GetCredential().GetAccessKey(),
		InlineSecretKey: d.GetCredential().GetSecretKey(),
	}
}

// ResolveDestination maps a request's destination spec to a concrete target plus a NON-SECRET descriptor
// to persist in the intent/manifest. getenv resolves credential env-var references (nil = os.Getenv).
//
//   - empty spec        → the configured default destination (FS when it has no bucket).
//   - spec.Name set     → the named destination (unknown name is an error); no secrets in the request.
//   - explicit bucket   → an ad-hoc destination. Inline creds (transient) require
//     bc.AllowInlineDestinationCreds, else it is rejected (named-only mode); otherwise the credentials
//     come from the SecretRef env vars.
//
// The returned Descriptor never contains a raw secret — only the bucket/prefix/region/endpoint and a
// credential REFERENCE (the env-var name, or the "inline" marker).
func ResolveDestination(bc config.BackupConfig, spec DestinationSpec, getenv func(string) string) (ResolvedDestination, Descriptor, error) {
	if getenv == nil {
		getenv = os.Getenv
	}
	switch {
	case spec.Name != "":
		nd, ok := bc.NamedDestination(spec.Name)
		if !ok {
			return ResolvedDestination{}, Descriptor{}, fmt.Errorf("backup: unknown named destination %q", spec.Name)
		}
		return fromConfigDest(nd, getenv)

	case spec.Bucket != "":
		rd := ResolvedDestination{S3: objstore.S3Config{
			Endpoint:     spec.Endpoint,
			Bucket:       spec.Bucket,
			Prefix:       spec.Prefix,
			Region:       spec.Region,
			UseSSL:       spec.UseSSL,
			UsePathStyle: spec.UsePathStyle,
		}}
		desc := Descriptor{
			Bucket:       spec.Bucket,
			Prefix:       spec.Prefix,
			Region:       spec.Region,
			Endpoint:     spec.Endpoint,
			UseSSL:       spec.UseSSL,
			UsePathStyle: spec.UsePathStyle,
		}
		if spec.InlineAccessKey != "" || spec.InlineSecretKey != "" {
			if !bc.AllowInlineDestinationCreds {
				return ResolvedDestination{}, Descriptor{}, fmt.Errorf("backup: inline destination credentials are disabled (named-only mode)")
			}
			rd.S3.AccessKey = spec.InlineAccessKey
			rd.S3.SecretKey = spec.InlineSecretKey
			desc.SecretName = "inline" // marker only — the raw secret is never persisted
		} else {
			rd.S3.AccessKey = getenv(spec.SecretRef + "_ACCESS_KEY")
			rd.S3.SecretKey = getenv(spec.SecretRef + "_SECRET_KEY")
			desc.SecretName = spec.SecretRef
		}
		return rd, desc, nil

	default:
		dd := bc.DefaultDestination
		if dd.Bucket == "" {
			return ResolvedDestination{UseFS: true}, Descriptor{DefaultFS: true}, nil
		}
		return fromConfigDest(dd, getenv)
	}
}

// fromConfigDest builds a resolved S3 target + non-secret descriptor from a configured destination,
// reading its credentials from the env vars it names.
func fromConfigDest(cd config.BackupDestination, getenv func(string) string) (ResolvedDestination, Descriptor, error) {
	rd := ResolvedDestination{S3: objstore.S3Config{
		Endpoint:     cd.Endpoint,
		Bucket:       cd.Bucket,
		Prefix:       cd.Prefix,
		Region:       cd.Region,
		UseSSL:       cd.UseSSL,
		UsePathStyle: cd.UsePathStyle,
		AccessKey:    getenv(cd.AccessKeyEnv),
		SecretKey:    getenv(cd.SecretKeyEnv),
	}}
	desc := Descriptor{
		Name:         cd.Name,
		Bucket:       cd.Bucket,
		Prefix:       cd.Prefix,
		Region:       cd.Region,
		Endpoint:     cd.Endpoint,
		UseSSL:       cd.UseSSL,
		UsePathStyle: cd.UsePathStyle,
		SecretName:   cd.AccessKeyEnv, // reference (env-var name), not the credential value
	}
	return rd, desc, nil
}
