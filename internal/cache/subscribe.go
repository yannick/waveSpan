package cache

import (
	"context"
	"net/http"
	"strconv"
	"sync"

	"github.com/yannick/wavespan/internal/membership"
	"github.com/yannick/wavespan/internal/recordstore"
	"github.com/yannick/wavespan/internal/rpcopts"
	wavespanv1 "github.com/yannick/wavespan/proto/wavespan/v1"
)

// --- Source side: push CacheUpdates to subscribers (design/05 "Update propagation") ---

type subEntry struct {
	id string
	ch chan *wavespanv1.CacheUpdate
}

// SubscriptionSource serves SubscribeKey streams and pushes a CacheUpdate to every active
// subscriber when a key it holds is updated. A slow subscriber is signalled to resync rather than
// blocking the source (design/05 "Update propagation" 7).
type SubscriptionSource struct {
	rec    *recordstore.Store
	mu     sync.Mutex
	subs   map[string]map[string]*subEntry // keyID -> subID -> entry
	seq    map[string]uint64               // keyID -> last stream sequence
	nextID uint64
}

// NewSubscriptionSource builds a source over the local record store.
func NewSubscriptionSource(rec *recordstore.Store) *SubscriptionSource {
	return &SubscriptionSource{rec: rec, subs: map[string]map[string]*subEntry{}, seq: map[string]uint64{}}
}

func (s *SubscriptionSource) register(keyID string) *subEntry {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nextID++
	e := &subEntry{id: strconv.FormatUint(s.nextID, 10), ch: make(chan *wavespanv1.CacheUpdate, 32)}
	if s.subs[keyID] == nil {
		s.subs[keyID] = map[string]*subEntry{}
	}
	s.subs[keyID][e.id] = e
	return e
}

func (s *SubscriptionSource) unregister(keyID, id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if m := s.subs[keyID]; m != nil {
		delete(m, id)
		if len(m) == 0 {
			delete(s.subs, keyID)
		}
	}
}

// Subscribe streams the current record then live updates for a key (implements the
// local.SubscriptionSource interface consumed by the ReplicaServer).
func (s *SubscriptionSource) Subscribe(ctx context.Context, req *wavespanv1.SubscribeKeyRequest, send func(*wavespanv1.CacheUpdate) error) error {
	ns, key := req.GetNamespace(), req.GetKey()
	keyID := string(combinedKey(ns, key))
	e := s.register(keyID)
	defer s.unregister(keyID, e.id)

	// initial snapshot
	if rec, found, err := s.rec.GetRecord(ns, key); err == nil && found {
		if err := send(&wavespanv1.CacheUpdate{Namespace: ns, Key: key, Record: rec, StreamSequence: s.curSeq(keyID)}); err != nil {
			return err
		}
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case u := <-e.ch:
			if err := send(u); err != nil {
				return err // subscriber gone: drop it
			}
		}
	}
}

func (s *SubscriptionSource) curSeq(keyID string) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.seq[keyID]
}

// Notify pushes the current record to all subscribers of a key (called after the node applies a
// newer version locally).
func (s *SubscriptionSource) Notify(ns string, key []byte) {
	keyID := string(combinedKey(ns, key))
	s.mu.Lock()
	m := s.subs[keyID]
	if len(m) == 0 {
		s.mu.Unlock()
		return
	}
	s.seq[keyID]++
	seq := s.seq[keyID]
	entries := make([]*subEntry, 0, len(m))
	for _, e := range m {
		entries = append(entries, e)
	}
	s.mu.Unlock()

	rec, found, err := s.rec.GetRecord(ns, key)
	if err != nil || !found {
		return
	}
	u := &wavespanv1.CacheUpdate{Namespace: ns, Key: key, Record: rec, StreamSequence: seq}
	for _, e := range entries {
		select {
		case e.ch <- u:
		default:
			// slow subscriber: ask it to resync instead of blocking the source
			select {
			case e.ch <- &wavespanv1.CacheUpdate{Namespace: ns, Key: key, SnapshotRequired: true, StreamSequence: seq}:
			default:
			}
		}
	}
}

// --- Subscriber side: receive updates and keep the dynamic cache fresh ---

// Subscriber opens SubscribeKey streams to holders and applies updates to the local cache, with
// resync on a sequence gap or a snapshot_required signal (design/05 "Subscription state machine").
type Subscriber struct {
	self    membership.Member
	store   *Store
	fetcher *Fetcher
	baseCtx context.Context

	mu      sync.Mutex
	active  map[string]context.CancelFunc // keyID -> cancel
	clients map[string]wavespanv1.ReplicationServiceClient
}

// NewSubscriber builds a subscriber. Subscriptions live under the base context (set via
// SetBaseContext to the node lifetime), not the per-read request context. The hc argument is
// retained for call-site compatibility but is unused: subscription streams now dial peers over gRPC.
func NewSubscriber(self membership.Member, store *Store, fetcher *Fetcher, _ *http.Client) *Subscriber {
	return &Subscriber{self: self, store: store, fetcher: fetcher, baseCtx: context.Background(), active: map[string]context.CancelFunc{}, clients: map[string]wavespanv1.ReplicationServiceClient{}}
}

// SetBaseContext sets the node-lifetime context under which subscription streams run.
func (s *Subscriber) SetBaseContext(ctx context.Context) {
	s.mu.Lock()
	s.baseCtx = ctx
	s.mu.Unlock()
}

func (s *Subscriber) client(addr string) (wavespanv1.ReplicationServiceClient, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if c, ok := s.clients[addr]; ok {
		return c, nil
	}
	conn, err := rpcopts.GRPCConn(addr)
	if err != nil {
		return nil, err
	}
	c := wavespanv1.NewReplicationServiceClient(conn)
	s.clients[addr] = c
	return c, nil
}

// Ensure opens a subscription for a key if one is not already active for it. The subscription
// runs under the node-lifetime base context, not the caller's request context.
func (s *Subscriber) Ensure(_ context.Context, ns string, key []byte, offer *wavespanv1.SubscriptionOffer) {
	if offer == nil || offer.GetSourceDataAddr() == "" {
		return
	}
	keyID := string(combinedKey(ns, key))
	s.mu.Lock()
	if _, ok := s.active[keyID]; ok {
		s.mu.Unlock()
		return
	}
	cctx, cancel := context.WithCancel(s.baseCtx)
	s.active[keyID] = cancel
	s.mu.Unlock()
	go s.run(cctx, ns, key, offer.GetSourceDataAddr(), keyID)
}

func (s *Subscriber) run(ctx context.Context, ns string, key []byte, addr, keyID string) {
	defer func() {
		s.mu.Lock()
		delete(s.active, keyID)
		s.mu.Unlock()
	}()

	c, err := s.client(addr)
	if err != nil {
		return
	}
	stream, err := c.SubscribeKey(ctx, &wavespanv1.SubscribeKeyRequest{
		Namespace: ns, Key: key, SubscriberMemberId: s.self.MemberID,
	})
	if err != nil {
		return
	}
	var lastSeq uint64
	for {
		u, err := stream.Recv()
		if err != nil {
			break // io.EOF on a clean close, or a transport error
		}
		if u.GetSnapshotRequired() || (lastSeq != 0 && u.GetStreamSequence() > lastSeq+1) {
			// gap or explicit resync: refetch the authoritative record
			if fr, e := s.fetcher.Fetch(ctx, ns, key); e == nil && fr.Found {
				_ = s.store.Put(fr.Record)
			}
			lastSeq = u.GetStreamSequence()
			continue
		}
		if rec := u.GetRecord(); rec != nil {
			_ = s.store.Put(rec) // LWW apply via the record store
		}
		lastSeq = u.GetStreamSequence()
	}
	// stream closed/errored: the entry is removed so the next read re-fetches and re-subscribes.
}
