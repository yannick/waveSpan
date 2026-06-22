package cache

import (
	"bytes"
	"sync"

	wavespanv1 "github.com/cwire/wavespan/proto/wavespan/v1"
)

// CertStore holds RangeCoverageCertificates this node has been issued for cache-complete scans
// (design/03 "Cache range coverage certificate"). A cache-complete scan reports COMPLETE only when
// a held certificate is current-epoch, unexpired, and covers the requested range. Certificates are
// granted by a range owner alongside an active SubscribeRange (the subscription stream lands with
// the broader range-cache work; the certificate gate — property 4 — is enforced here).
type CertStore struct {
	mu    sync.RWMutex
	certs map[string][]*wavespanv1.RangeCoverageCertificate // namespace -> certs
	epoch func(namespace string, ownerMemberID string) uint64
}

// NewCertStore builds a certificate store. epoch returns the current owner epoch for validation
// (a stale-epoch certificate is rejected); a nil epoch accepts any epoch.
func NewCertStore(epoch func(namespace, ownerMemberID string) uint64) *CertStore {
	return &CertStore{certs: map[string][]*wavespanv1.RangeCoverageCertificate{}, epoch: epoch}
}

// Put records an issued certificate.
func (s *CertStore) Put(cert *wavespanv1.RangeCoverageCertificate) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.certs[cert.GetNamespace()] = append(s.certs[cert.GetNamespace()], cert)
}

// Covers reports whether a valid certificate covers [start, end) at nowMs (implements the kv
// scanner's certValidator). Validation: unexpired, current owner epoch, and range containment.
func (s *CertStore) Covers(namespace string, start, end []byte, nowMs int64) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, c := range s.certs[namespace] {
		if c.GetValidUntilUnixMs() <= nowMs {
			continue // expired
		}
		if s.epoch != nil && c.GetOwnerEpoch() != s.epoch(namespace, c.GetOwnerMemberId()) {
			continue // stale epoch
		}
		if rangeContains(c.GetStartKey(), c.GetEndKey(), start, end) {
			return true
		}
	}
	return false
}

// rangeContains reports whether [cs, ce) contains [qs, qe). A nil bound is unbounded on that side.
func rangeContains(cs, ce, qs, qe []byte) bool {
	if cs != nil && (qs == nil || bytes.Compare(qs, cs) < 0) {
		return false
	}
	if ce != nil && (qe == nil || bytes.Compare(qe, ce) > 0) {
		return false
	}
	return true
}
