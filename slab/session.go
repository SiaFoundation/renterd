package slab

import (
	"context"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"time"

	"go.sia.tech/renterd/internal/consensus"
	rhpv2 "go.sia.tech/renterd/rhp/v2"
	"go.sia.tech/siad/types"
)

// A HostError associates an error with a given host.
type HostError struct {
	HostKey consensus.PublicKey
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

func (s *sharedSession) appendSector(sector *[rhpv2.SectorSize]byte, currentHeight uint64, mr MetricsRecorder) (consensus.Hash256, error) {
	if currentHeight > uint64(s.sess.Contract().Revision.NewWindowStart) {
		return consensus.Hash256{}, fmt.Errorf("contract has expired")
	}
	storageDuration := uint64(s.sess.Contract().Revision.NewWindowStart) - currentHeight
	price, collateral := rhpv2.RPCAppendCost(s.settings, storageDuration)
	start := time.Now()
	root, err := s.sess.Append(sector, price, collateral)
	mr.RecordMetric(MetricSectorUpload{
		HostKey:    s.sess.HostKey(),
		Contract:   s.sess.Contract().ID(),
		Timestamp:  start,
		Elapsed:    time.Since(start),
		Err:        err,
		Cost:       price,
		Collateral: collateral,
	})
	return root, err
}

func (s *sharedSession) readSector(w io.Writer, root consensus.Hash256, offset, length uint32, mr MetricsRecorder) error {
	sections := []rhpv2.RPCReadRequestSection{{
		MerkleRoot: root,
		Offset:     uint64(offset),
		Length:     uint64(length),
	}}
	price := rhpv2.RPCReadCost(s.settings, sections)
	start := time.Now()
	err := s.sess.Read(w, sections, price)
	mr.RecordMetric(MetricSectorDownload{
		HostKey:   s.sess.HostKey(),
		Contract:  s.sess.Contract().ID(),
		Timestamp: start,
		Elapsed:   time.Since(start),
		Err:       err,
		Cost:      price,
	})
	return err
}

func (s *sharedSession) deleteSectors(roots []consensus.Hash256, mr MetricsRecorder) error {
	// download the full set of SectorRoots
	start := time.Now()
	contractSectors := s.sess.Contract().NumSectors()
	rootIndices := make(map[consensus.Hash256]uint64, contractSectors)
	for offset := uint64(0); offset < contractSectors; {
		n := uint64(130000) // a little less than 4MiB of roots
		if offset+n > contractSectors {
			n = contractSectors - offset
		}
		price := rhpv2.RPCSectorRootsCost(s.settings, n)
		roots, err := s.sess.SectorRoots(offset, n, price)
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
	err := s.sess.Delete(badIndices, price)
	mr.RecordMetric(MetricSectorDeletion{
		HostKey:   s.sess.HostKey(),
		Contract:  s.sess.Contract().ID(),
		Timestamp: start,
		Elapsed:   time.Since(start),
		Err:       err,
		Cost:      price,
		NumRoots:  uint64(len(badIndices)),
	})
	return err
}

// Close gracefully closes the Session.
func (s *sharedSession) Close() error {
	if s.sess != nil {
		return s.sess.Close()
	}
	return nil
}

// Session implements Host via the renter-host protocol.
type Session struct {
	hostKey    consensus.PublicKey
	hostIP     string
	contractID types.FileContractID
	renterKey  consensus.PrivateKey
	mr         MetricsRecorder
	pool       *SessionPool
}

// PublicKey returns the host's public key.
func (s *Session) PublicKey() consensus.PublicKey {
	return s.hostKey
}

// UploadSector implements Host.
func (s *Session) UploadSector(sector *[rhpv2.SectorSize]byte) (consensus.Hash256, error) {
	currentHeight := s.pool.currentHeight()
	if currentHeight == 0 {
		panic("cannot upload without knowing current height") // developer error
	}
	ss, err := s.pool.acquire(s)
	if err != nil {
		return consensus.Hash256{}, err
	}
	defer s.pool.release(ss)
	return ss.appendSector(sector, currentHeight, s.mr)
}

// DownloadSector implements Host.
func (s *Session) DownloadSector(w io.Writer, root consensus.Hash256, offset, length uint32) error {
	ss, err := s.pool.acquire(s)
	if err != nil {
		return err
	}
	defer s.pool.release(ss)
	return ss.readSector(w, root, offset, length, s.mr)
}

// DeleteSectors implements Host.
func (s *Session) DeleteSectors(roots []consensus.Hash256) error {
	ss, err := s.pool.acquire(s)
	if err != nil {
		return err
	}
	defer s.pool.release(ss)
	return ss.deleteSectors(roots, s.mr)
}

// A SessionPool is a set of sessions that can be used for uploading and
// downloading.
type SessionPool struct {
	hosts  map[consensus.PublicKey]*sharedSession
	height uint64
	mu     sync.Mutex
}

func (sp *SessionPool) acquire(s *Session) (_ *sharedSession, err error) {
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
		if time.Since(ss.lastSeen) >= 2*time.Minute {
			// use RPCSettings as a generic "ping"
			ss.settings, err = rhpv2.RPCSettings(t)
			if err != nil {
				t.Close()
				goto reconnect
			}
		}
		if ss.sess.Contract().ID() != s.contractID {
			if ss.sess.Contract().ID() != (types.FileContractID{}) {
				if err := ss.sess.Unlock(); err != nil {
					t.Close()
					goto reconnect
				}
			}
			ss.sess, err = rhpv2.RPCLock(t, s.contractID, s.renterKey, 10*time.Second)
			if err != nil {
				t.Close()
				goto reconnect
			}
		}
		ss.lastSeen = time.Now()
		return ss, nil
	}

reconnect:
	ss.conn, err = (&net.Dialer{}).DialContext(context.TODO(), "tcp", s.hostIP)
	if err != nil {
		return nil, err
	}
	t, err := rhpv2.NewRenterTransport(ss.conn, s.hostKey)
	if err != nil {
		return nil, err
	}
	ss.settings, err = rhpv2.RPCSettings(t)
	if err != nil {
		t.Close()
		return nil, err
	}
	ss.sess, err = rhpv2.RPCLock(t, s.contractID, s.renterKey, 10*time.Second)
	if err != nil {
		t.Close()
		return nil, err
	}
	ss.lastSeen = time.Now()
	return ss, nil
}

func (sp *SessionPool) release(ss *sharedSession) {
	ss.mu.Unlock()
}

// SetCurrentHeight sets the current height. This value is used when calculating
// the storage duration for new data, so it must be called before
// (*Session).UploadSector.
func (sp *SessionPool) SetCurrentHeight(height uint64) {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	sp.height = height
}

func (sp *SessionPool) currentHeight() uint64 {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	return sp.height
}

// Session adds a RHPv2 session to the pool. The session is initiated lazily; no
// I/O is performed until the first RPC call is made.
func (sp *SessionPool) Session(hostKey consensus.PublicKey, hostIP string, contractID types.FileContractID, renterKey consensus.PrivateKey, mr MetricsRecorder) *Session {
	return &Session{
		hostKey:    hostKey,
		hostIP:     hostIP,
		contractID: contractID,
		renterKey:  renterKey,
		mr:         mr,
		pool:       sp,
	}
}

// UnlockContract unlocks a Session's contract.
func (sp *SessionPool) UnlockContract(s *Session) {
	sp.mu.Lock()
	ss, ok := s.pool.hosts[s.hostKey]
	sp.mu.Unlock()
	if !ok {
		return
	}
	ss.mu.Lock()
	defer ss.mu.Unlock()
	if ss.sess != nil && ss.sess.Contract().ID() == s.contractID {
		ss.sess.Unlock()
	}
}

// ForceClose forcibly closes a Session's underlying connection.
func (sp *SessionPool) ForceClose(s *Session) {
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
func (sp *SessionPool) Close() error {
	for hostKey, sess := range sp.hosts {
		sess.Close()
		delete(sp.hosts, hostKey)
	}
	return nil
}

// NewSessionPool creates a new SessionPool.
func NewSessionPool() *SessionPool {
	return &SessionPool{
		hosts: make(map[consensus.PublicKey]*sharedSession),
	}
}
