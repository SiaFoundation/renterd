package worker

import (
	"context"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"time"

	"go.sia.tech/core/types"
	rhpv2 "go.sia.tech/renterd/rhp/v2"
)

// A HostError associates an error with a given host.
type HostError struct {
	HostKey types.PublicKey
	Err     error
}

// Error implements error.
func (he HostError) Error() string {
	return fmt.Sprintf("%x: %v", he.HostKey[:4], he.Err.Error())
}

// Unwrap returns the underlying error.
func (he HostError) Unwrap() error {
	return he.Err
}

// A HostErrorSet is a collection of errors from various hosts.
type HostErrorSet []*HostError

// Error implements error.
func (hes HostErrorSet) Error() string {
	strs := make([]string, len(hes))
	for i := range strs {
		strs[i] = hes[i].Error()
	}
	// include a leading newline so that the first error isn't printed on the
	// same line as the error context
	return "\n" + strings.Join(strs, "\n")
}

// A sharedSession wraps a RHPv2 session with useful metadata and methods.
type sharedSession struct {
	sess     *rhpv2.Session
	conn     net.Conn
	settings rhpv2.HostSettings
	lastSeen time.Time
	mu       sync.Mutex
}

func (s *sharedSession) appendSector(ctx context.Context, sector *[rhpv2.SectorSize]byte, currentHeight uint64) (types.Hash256, error) {
	if currentHeight > uint64(s.sess.Revision().Revision.WindowStart) {
		return types.Hash256{}, fmt.Errorf("contract has expired")
	}
	storageDuration := uint64(s.sess.Revision().Revision.WindowStart) - currentHeight
	price, collateral := rhpv2.RPCAppendCost(s.settings, storageDuration)
	return s.sess.Append(ctx, sector, price, collateral)
}

func (s *sharedSession) readSector(ctx context.Context, w io.Writer, root types.Hash256, offset, length uint32) error {
	sections := []rhpv2.RPCReadRequestSection{{
		MerkleRoot: root,
		Offset:     uint64(offset),
		Length:     uint64(length),
	}}
	price := rhpv2.RPCReadCost(s.settings, sections)
	return s.sess.Read(ctx, w, sections, price)
}

func (s *sharedSession) deleteSectors(ctx context.Context, roots []types.Hash256) error {
	// download the full set of SectorRoots
	contractSectors := s.sess.Revision().NumSectors()
	rootIndices := make(map[types.Hash256]uint64, contractSectors)
	for offset := uint64(0); offset < contractSectors; {
		n := uint64(130000) // a little less than 4MiB of roots
		if offset+n > contractSectors {
			n = contractSectors - offset
		}
		price := rhpv2.RPCSectorRootsCost(s.settings, n)
		roots, err := s.sess.SectorRoots(ctx, offset, n, price)
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
	return s.sess.Delete(ctx, badIndices, price)
}

func (s *sharedSession) Close() error {
	if s.sess != nil {
		return s.sess.Close()
	}
	return nil
}

// session implements Host via the renter-host protocol.
type session struct {
	hostKey    types.PublicKey
	hostIP     string
	contractID types.FileContractID
	renterKey  types.PrivateKey
	pool       *sessionPool
}

func (s *session) Contract() types.FileContractID {
	return s.contractID
}

func (s *session) PublicKey() types.PublicKey {
	return s.hostKey
}

func (s *session) Revision(ctx context.Context) (rhpv2.ContractRevision, error) {
	ss, err := s.pool.acquire(ctx, s)
	if err != nil {
		return rhpv2.ContractRevision{}, err
	}
	defer s.pool.release(ss)
	return ss.sess.Revision(), nil
}

func (s *session) UploadSector(ctx context.Context, sector *[rhpv2.SectorSize]byte) (hash types.Hash256, err error) {
	currentHeight := s.pool.currentHeight()
	if currentHeight == 0 {
		panic("cannot upload without knowing current height") // developer error
	}

	ss, err := s.pool.acquire(ctx, s)
	if err != nil {
		return types.Hash256{}, err
	}
	defer s.pool.release(ss)

	errs := PerformGougingChecks(ctx, ss.settings).CanUpload()
	if len(errs) > 0 {
		return types.Hash256{}, fmt.Errorf("failed to upload sector, gouging check failed: %v", errs)
	}

	return ss.appendSector(ctx, sector, currentHeight)
}

func (s *session) DownloadSector(ctx context.Context, w io.Writer, root types.Hash256, offset, length uint32) error {
	ss, err := s.pool.acquire(ctx, s)
	if err != nil {
		return err
	}
	defer s.pool.release(ss)
	if errs := PerformGougingChecks(ctx, ss.settings).CanDownload(); len(errs) > 0 {
		return fmt.Errorf("failed to download sector, gouging check failed: %v", errs)
	}
	return ss.readSector(ctx, w, root, offset, length)
}

func (s *session) DeleteSectors(ctx context.Context, roots []types.Hash256) error {
	ss, err := s.pool.acquire(ctx, s)
	if err != nil {
		return err
	}
	defer s.pool.release(ss)
	return ss.deleteSectors(ctx, roots)
}

// A sessionPool is a set of sessions that can be used for uploading and
// downloading.
type sessionPool struct {
	sessionReconnectTimeout time.Duration
	sessionTTL              time.Duration

	mu     sync.Mutex
	height uint64
	hosts  map[types.PublicKey]*sharedSession
}

func (sp *sessionPool) acquire(ctx context.Context, s *session) (_ *sharedSession, err error) {
	sp.mu.Lock()
	if sp.hosts[s.hostKey] == nil {
		sp.hosts[s.hostKey] = &sharedSession{}
	}
	ss := sp.hosts[s.hostKey]
	sp.mu.Unlock()

	ss.mu.Lock()
	defer func() {
		if err != nil {
			ss.mu.Unlock()
		}
	}()

	// reuse existing session or transport if possible
	if ss.sess != nil {
		t := ss.sess.Transport()
		if time.Since(ss.lastSeen) >= sp.sessionTTL {
			// use RPCSettings as a generic "ping"
			ss.settings, err = rhpv2.RPCSettings(ctx, t)
			if err != nil {
				t.Close()
				goto reconnect
			}
		}
		if ss.sess.Revision().ID() != s.contractID {
			if ss.sess.Revision().ID() != (types.FileContractID{}) {
				if err := ss.sess.Unlock(); err != nil {
					t.Close()
					goto reconnect
				}
			}
			ss.sess, err = rhpv2.RPCLock(ctx, t, s.contractID, s.renterKey, 10*time.Second)
			if err != nil {
				t.Close()
				goto reconnect
			}
		}
		ss.lastSeen = time.Now()
		return ss, nil
	}

reconnect:
	if ss.sess != nil && sp.sessionReconnectTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, sp.sessionReconnectTimeout)
		defer cancel()
	}

	ss.conn, err = (&net.Dialer{}).DialContext(ctx, "tcp", s.hostIP)
	if err != nil {
		ss.sess = nil
		return nil, err
	}
	t, err := rhpv2.NewRenterTransport(ss.conn, s.hostKey)
	if err != nil {
		ss.sess = nil
		return nil, err
	}
	ss.settings, err = rhpv2.RPCSettings(ctx, t)
	if err != nil {
		ss.sess = nil
		t.Close()
		return nil, err
	}
	ss.sess, err = rhpv2.RPCLock(ctx, t, s.contractID, s.renterKey, 10*time.Second)
	if err != nil {
		t.Close()
		return nil, err
	}
	ss.lastSeen = time.Now()
	return ss, nil
}

func (sp *sessionPool) release(ss *sharedSession) {
	ss.mu.Unlock()
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
func (sp *sessionPool) session(hostKey types.PublicKey, hostIP string, contractID types.FileContractID, renterKey types.PrivateKey) *session {
	return &session{
		hostKey:    hostKey,
		hostIP:     hostIP,
		contractID: contractID,
		renterKey:  renterKey,
		pool:       sp,
	}
}

func (sp *sessionPool) unlockContract(s *session) {
	sp.mu.Lock()
	ss, ok := s.pool.hosts[s.hostKey]
	sp.mu.Unlock()
	if !ok {
		return
	}
	ss.mu.Lock()
	defer ss.mu.Unlock()
	if ss.sess != nil && ss.sess.Revision().ID() == s.contractID {
		ss.sess.Unlock()
	}
}

func (sp *sessionPool) forceClose(s *session) {
	sp.mu.Lock()
	ss, ok := s.pool.hosts[s.hostKey]
	sp.mu.Unlock()
	if !ok {
		return
	}
	ss.mu.Lock()
	defer ss.mu.Unlock()
	if ss.conn != nil {
		ss.conn.Close()
		ss.sess = nil
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
func newSessionPool(sessionReconectTimeout, sessionTTL time.Duration) *sessionPool {
	return &sessionPool{
		sessionReconnectTimeout: sessionReconectTimeout,
		sessionTTL:              sessionTTL,
		hosts:                   make(map[types.PublicKey]*sharedSession),
	}
}
