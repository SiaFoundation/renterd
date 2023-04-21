package worker

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	rhpv2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/internal/tracing"
)

func (s *Session) appendSector(ctx context.Context, sector *[rhpv2.SectorSize]byte, currentHeight uint64) (types.Hash256, error) {
	if currentHeight > uint64(s.Revision().Revision.WindowStart) {
		return types.Hash256{}, fmt.Errorf("contract has expired")
	}
	storageDuration := uint64(s.Revision().Revision.WindowStart) - currentHeight
	price, collateral := rhpv2.RPCAppendCost(s.settings, storageDuration)
	root, err := s.Append(ctx, sector, price, collateral)
	if err != nil {
		return types.Hash256{}, err
	}
	return root, nil
}

func (s *Session) readSector(ctx context.Context, w io.Writer, root types.Hash256, offset, length uint64) error {
	sections := []rhpv2.RPCReadRequestSection{{
		MerkleRoot: root,
		Offset:     offset,
		Length:     length,
	}}
	price := rhpv2.RPCReadCost(s.settings, sections)
	if err := s.Read(ctx, w, sections, price); err != nil {
		return err
	}
	return nil
}

func (s *Session) deleteSectors(ctx context.Context, roots []types.Hash256) error {
	// download the full set of SectorRoots
	contractSectors := s.Revision().NumSectors()
	rootIndices := make(map[types.Hash256]uint64, contractSectors)
	for offset := uint64(0); offset < contractSectors; {
		n := uint64(130000) // a little less than 4MiB of roots
		if offset+n > contractSectors {
			n = contractSectors - offset
		}
		price := rhpv2.RPCSectorRootsCost(s.settings, n)
		roots, err := s.SectorRoots(ctx, offset, n, price)
		if err != nil {
			return err
		}
		for i, root := range roots {
			rootIndices[root] = offset + uint64(i)
		}
		offset += n
	}

	// look up the index of each sector
	badIndices := make([]uint64, 0, len(roots))
	for _, r := range roots {
		if index, ok := rootIndices[r]; ok {
			badIndices = append(badIndices, index)
			delete(rootIndices, r) // prevent duplicates
		}
	}

	price := rhpv2.RPCDeleteCost(s.settings, len(badIndices))
	return s.Delete(ctx, badIndices, price)
}

// sharedSession implements Host via the renter-host protocol.
type sharedSession struct {
	hostKey    types.PublicKey
	hostIP     string
	contractID types.FileContractID
	renterKey  types.PrivateKey
	pool       *sessionPool
}

func (ss *sharedSession) Contract() types.FileContractID {
	return ss.contractID
}

func (ss *sharedSession) HostKey() types.PublicKey {
	return ss.hostKey
}

func (ss *sharedSession) Revision(ctx context.Context) (rhpv2.ContractRevision, error) {
	s, err := ss.pool.acquire(ctx, ss)
	if err != nil {
		return rhpv2.ContractRevision{}, err
	}
	defer ss.pool.release(s)
	return s.Revision(), nil
}

func (ss *sharedSession) RenewContract(ctx context.Context, prepareFn func(rev types.FileContractRevision, host rhpv2.HostSettings) ([]types.Transaction, types.Currency, func(), error)) (rhpv2.ContractRevision, []types.Transaction, error) {
	s, err := ss.pool.acquire(ctx, ss)
	if err != nil {
		return rhpv2.ContractRevision{}, nil, err
	}
	defer ss.pool.release(s)

	if breakdown := GougingCheckerFromContext(ctx).Check(&s.settings, nil); breakdown.Gouging() {
		return rhpv2.ContractRevision{}, nil, fmt.Errorf("failed reneww contract, gouging check failed: %v", breakdown.Reasons())
	}

	renterTxnSet, finalPayment, discard, err := prepareFn(s.Revision().Revision, s.Settings())
	if err != nil {
		return rhpv2.ContractRevision{}, nil, err
	}
	rev, txnSet, err := s.RenewContract(ctx, renterTxnSet, finalPayment)
	if err != nil {
		discard()
		return rhpv2.ContractRevision{}, nil, err
	}
	return rev, txnSet, nil
}

func (ss *sharedSession) UploadSector(ctx context.Context, sector *[rhpv2.SectorSize]byte) (types.Hash256, error) {
	ctx, span := tracing.Tracer.Start(ctx, "sharedSession.UploadSector")
	defer span.End()
	currentHeight := ss.pool.currentHeight()
	if currentHeight == 0 {
		panic("cannot upload without knowing current height") // developer error
	}
	s, err := ss.pool.acquire(ctx, ss)
	if err != nil {
		return types.Hash256{}, err
	}
	defer ss.pool.release(s)
	if breakdown := GougingCheckerFromContext(ctx).Check(&s.settings, nil); breakdown.Gouging() {
		return types.Hash256{}, fmt.Errorf("failed to upload sector, gouging check failed: %v", breakdown.Reasons())
	}
	return s.appendSector(ctx, sector, currentHeight)
}

func (ss *sharedSession) DownloadSector(ctx context.Context, w io.Writer, root types.Hash256, offset, length uint64) error {
	s, err := ss.pool.acquire(ctx, ss)
	if err != nil {
		return err
	}
	defer ss.pool.release(s)
	if breakdown := GougingCheckerFromContext(ctx).Check(&s.settings, nil); breakdown.Gouging() {
		return fmt.Errorf("failed to download sector, gouging check failed: %v", breakdown.Reasons())
	}
	return s.readSector(ctx, w, root, offset, length)
}

func (ss *sharedSession) DeleteSectors(ctx context.Context, roots []types.Hash256) error {
	s, err := ss.pool.acquire(ctx, ss)
	if err != nil {
		return err
	}
	defer ss.pool.release(s)
	return s.deleteSectors(ctx, roots)
}

// A sessionPool is a set of sessions that can be used for uploading and
// downloading.
type sessionPool struct {
	sessionLockTimeout      time.Duration
	sessionReconnectTimeout time.Duration
	sessionTTL              time.Duration

	mu     sync.Mutex
	height uint64
	hosts  map[types.PublicKey]*Session
}

func (sp *sessionPool) acquire(ctx context.Context, ss *sharedSession) (_ *Session, err error) {
	sp.mu.Lock()
	if sp.hosts[ss.hostKey] == nil {
		sp.hosts[ss.hostKey] = &Session{}
	}
	s := sp.hosts[ss.hostKey]
	sp.mu.Unlock()

	s.mu.Lock()
	defer func() {
		if err != nil {
			s.mu.Unlock()
		}
	}()

	// if contract of session was renewed, update the sharedSession.
	if s.renewedFrom != (types.FileContractID{}) && ss.contractID == s.renewedFrom {
		ss.contractID = s.renewedTo
	}

	// try refreshing the session and reconnect if it failed
	if err := s.Refresh(ctx, sp.sessionLockTimeout, sp.sessionTTL, ss.renterKey, ss.contractID); err != nil {
		if sp.sessionReconnectTimeout > 0 {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, sp.sessionReconnectTimeout)
			defer cancel()
		}

		if err := s.Reconnect(ctx, sp.sessionLockTimeout, ss.hostIP, ss.hostKey, ss.renterKey, ss.contractID); err != nil {
			return nil, err
		}
	}

	return s, nil
}

func (sp *sessionPool) release(s *Session) {
	s.mu.Unlock()
}

// setCurrentHeight sets the pol's current height. This value is used when
// calculating the storage duration for new data, so it must be called before
// (*session).UploadSector.
func (sp *sessionPool) setCurrentHeight(height uint64) {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	sp.height = height
}

func (sp *sessionPool) currentHeight() uint64 {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	return sp.height
}

// session adds a RHPv2 session to the pool. The session is initiated lazily; no
// I/O is performed until the first RPC call is made.
func (sp *sessionPool) session(hostKey types.PublicKey, hostIP string, contractID types.FileContractID, renterKey types.PrivateKey) *sharedSession {
	return &sharedSession{
		hostKey:    hostKey,
		hostIP:     hostIP,
		contractID: contractID,
		renterKey:  renterKey,
		pool:       sp,
	}
}

func (sp *sessionPool) unlockContract(ctx context.Context, ss *sharedSession) {
	sp.mu.Lock()
	s, ok := ss.pool.hosts[ss.hostKey]
	sp.mu.Unlock()
	if !ok {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.transport != nil && s.Revision().ID() == ss.contractID {
		s.Unlock(ctx)
	}
}

// Close gracefully closes all of the sessions in the pool.
func (sp *sessionPool) Close() error {
	for hostKey, sess := range sp.hosts {
		sess.Close()
		delete(sp.hosts, hostKey)
	}
	return nil
}

// newSessionPool creates a new sessionPool.
func newSessionPool(sessionLockTimeout, sessionReconectTimeout, sessionTTL time.Duration) *sessionPool {
	return &sessionPool{
		sessionLockTimeout:      sessionLockTimeout,
		sessionReconnectTimeout: sessionReconectTimeout,
		sessionTTL:              sessionTTL,
		hosts:                   make(map[types.PublicKey]*Session),
	}
}
