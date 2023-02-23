package worker

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"sort"
	"strings"
	"sync"
	"time"

	rhpv2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/metrics"
)

var (
	// ErrInsufficientFunds is returned by various RPCs when the renter is
	// unable to provide sufficient payment to the host.
	ErrInsufficientFunds = errors.New("insufficient funds")

	// ErrInsufficientCollateral is returned by various RPCs when the host is
	// unable to provide sufficient collateral.
	ErrInsufficientCollateral = errors.New("insufficient collateral")

	// ErrInvalidMerkleProof is returned by various RPCs when the host supplies
	// an invalid Merkle proof.
	ErrInvalidMerkleProof = errors.New("host supplied invalid Merkle proof")

	// ErrContractLocked is returned by the Lock RPC when the contract in
	// question is already locked by another party. This is a transient error;
	// the caller should retry later.
	ErrContractLocked = errors.New("contract is locked by another party")

	// ErrNoContractLocked is returned by RPCs that require a locked contract
	// when no contract is locked.
	ErrNoContractLocked = errors.New("no contract locked")

	// ErrContractFinalized is returned by the Lock RPC when the contract in
	// question has reached its maximum revision number, meaning the contract
	// can no longer be revised.
	ErrContractFinalized = errors.New("contract cannot be revised further")
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

func wrapErr(err *error, fnName string) {
	if *err != nil {
		*err = fmt.Errorf("%s: %w", fnName, *err)
	}
}

// wrapResponseErr formats RPC response errors nicely, wrapping them in either
// readCtx or rejectCtx depending on whether we encountered an I/O error or the
// host sent an explicit error message.
func wrapResponseErr(err error, readCtx, rejectCtx string) error {
	if errors.As(err, new(*rhpv2.RPCError)) {
		return fmt.Errorf("%s: %w", rejectCtx, err)
	}
	if err != nil {
		return fmt.Errorf("%s: %w", readCtx, err)
	}
	return nil
}

// MetricRPC contains metrics relating to a single RPC.
type MetricRPC struct {
	HostKey    types.PublicKey
	RPC        types.Specifier
	Timestamp  time.Time
	Elapsed    time.Duration
	Contract   types.FileContractID // possibly empty
	Uploaded   uint64
	Downloaded uint64
	Cost       types.Currency
	Collateral types.Currency
	Err        error
}

// IsMetric implements metrics.Metric.
func (MetricRPC) IsMetric() {}

// IsSuccess implements metrics.Metric.
func (m MetricRPC) IsSuccess() bool { return m.Err == nil }

// helper type for ensuring that we always write in multiples of LeafSize,
// which is required by e.g. (renter.EncryptionKey).XORKeyStream
type segWriter struct {
	w   io.Writer
	buf [rhpv2.LeafSize * 64]byte
	len int
}

func (sw *segWriter) Write(p []byte) (int, error) {
	lenp := len(p)
	for len(p) > 0 {
		n := copy(sw.buf[sw.len:], p)
		sw.len += n
		p = p[n:]
		segs := sw.buf[:sw.len-(sw.len%rhpv2.LeafSize)]
		if _, err := sw.w.Write(segs); err != nil {
			return 0, err
		}
		sw.len = copy(sw.buf[:], sw.buf[len(segs):sw.len])
	}
	return lenp, nil
}

func hashRevision(rev types.FileContractRevision) types.Hash256 {
	h := types.NewHasher()
	rev.EncodeTo(h.E)
	return h.Sum()
}

func updateRevisionOutputs(rev *types.FileContractRevision, cost, collateral types.Currency) (valid, missed []types.Currency) {
	// allocate new slices; don't want to risk accidentally sharing memory
	rev.ValidProofOutputs = append([]types.SiacoinOutput(nil), rev.ValidProofOutputs...)
	rev.MissedProofOutputs = append([]types.SiacoinOutput(nil), rev.MissedProofOutputs...)

	// move valid payout from renter to host
	rev.ValidProofOutputs[0].Value = rev.ValidProofOutputs[0].Value.Sub(cost)
	rev.ValidProofOutputs[1].Value = rev.ValidProofOutputs[1].Value.Add(cost)

	// move missed payout from renter to void
	rev.MissedProofOutputs[0].Value = rev.MissedProofOutputs[0].Value.Sub(cost)
	rev.MissedProofOutputs[2].Value = rev.MissedProofOutputs[2].Value.Add(cost)

	// move collateral from host to void
	rev.MissedProofOutputs[1].Value = rev.MissedProofOutputs[1].Value.Sub(collateral)
	rev.MissedProofOutputs[2].Value = rev.MissedProofOutputs[2].Value.Add(collateral)

	return []types.Currency{rev.ValidProofOutputs[0].Value, rev.ValidProofOutputs[1].Value},
		[]types.Currency{rev.MissedProofOutputs[0].Value, rev.MissedProofOutputs[1].Value, rev.MissedProofOutputs[2].Value}
}

func recordRPC(ctx context.Context, t *rhpv2.Transport, c rhpv2.ContractRevision, id types.Specifier, err *error) func() {
	startTime := time.Now()
	contractID := c.ID()
	var startFunds types.Currency
	if len(c.Revision.ValidProofOutputs) > 0 {
		startFunds = c.Revision.ValidProofOutputs[0].Value
	}
	var startCollateral types.Currency
	if len(c.Revision.MissedProofOutputs) > 1 {
		startCollateral = c.Revision.MissedProofOutputs[1].Value
	}
	startW, startR := t.BytesWritten(), t.BytesRead()
	return func() {
		m := MetricRPC{
			HostKey:    t.HostKey(),
			RPC:        id,
			Timestamp:  startTime,
			Elapsed:    time.Since(startTime),
			Contract:   contractID,
			Uploaded:   t.BytesWritten() - startW,
			Downloaded: t.BytesRead() - startR,
			Err:        *err,
		}
		if len(c.Revision.ValidProofOutputs) > 0 && startFunds.Cmp(c.Revision.ValidProofOutputs[0].Value) > 0 {
			m.Cost = startFunds.Sub(c.Revision.ValidProofOutputs[0].Value)
		}
		if len(c.Revision.MissedProofOutputs) > 1 && startCollateral.Cmp(c.Revision.MissedProofOutputs[1].Value) > 0 {
			m.Collateral = startCollateral.Sub(c.Revision.MissedProofOutputs[1].Value)
		}
		metrics.Record(ctx, m)
	}
}

// A Session pairs a Transport with a Contract, enabling RPCs that modify the
// Contract.
type Session struct {
	transport   *rhpv2.Transport
	renewedFrom types.FileContractID
	renewedTo   types.FileContractID
	revision    rhpv2.ContractRevision
	key         types.PrivateKey
	appendRoots []types.Hash256
	settings    rhpv2.HostSettings
	lastSeen    time.Time
	mu          sync.Mutex
}

// Append calls the Write RPC with a single action, appending the provided
// sector. It returns the Merkle root of the sector.
func (s *Session) Append(ctx context.Context, sector *[rhpv2.SectorSize]byte, price, collateral types.Currency) (types.Hash256, error) {
	err := s.Write(ctx, []rhpv2.RPCWriteAction{{
		Type: rhpv2.RPCWriteActionAppend,
		Data: sector[:],
	}}, price, collateral)
	if err != nil {
		return types.Hash256{}, err
	}
	return s.appendRoots[0], nil
}

// Close gracefully terminates the session and closes the underlying connection.
func (s *Session) Close() (err error) {
	defer wrapErr(&err, "Close")
	return s.transport.Close()
}

// Delete calls the Write RPC with a set of Swap and Trim actions that delete
// the specified sectors.
func (s *Session) Delete(ctx context.Context, sectorIndices []uint64, price types.Currency) error {
	if len(sectorIndices) == 0 {
		return nil
	}

	// sort in descending order so that we can use 'range'
	sort.Slice(sectorIndices, func(i, j int) bool {
		return sectorIndices[i] > sectorIndices[j]
	})

	// iterate backwards from the end of the contract, swapping each "good"
	// sector with one of the "bad" sectors.
	var actions []rhpv2.RPCWriteAction
	cIndex := s.revision.NumSectors() - 1
	for _, rIndex := range sectorIndices {
		if cIndex != rIndex {
			// swap a "good" sector for a "bad" sector
			actions = append(actions, rhpv2.RPCWriteAction{
				Type: rhpv2.RPCWriteActionSwap,
				A:    uint64(cIndex),
				B:    uint64(rIndex),
			})
		}
		cIndex--
	}
	// trim all "bad" sectors
	actions = append(actions, rhpv2.RPCWriteAction{
		Type: rhpv2.RPCWriteActionTrim,
		A:    uint64(len(sectorIndices)),
	})

	// request the swap+delete operation
	//
	// NOTE: siad hosts will accept up to 20 MiB of data in the request,
	// which should be sufficient to delete up to 2.5 TiB of sector data
	// at a time.
	return s.Write(ctx, actions, price, types.ZeroCurrency)
}

// HostKey returns the public key of the host.
func (s *Session) HostKey() types.PublicKey { return s.revision.HostKey() }

// Read calls the Read RPC, writing the requested sections of sector data to w.
// Merkle proofs are always requested.
//
// Note that sector data is streamed to w before it has been validated. Callers
// MUST check the returned error, and discard any data written to w if the error
// is non-nil. Failure to do so may allow an attacker to inject malicious data.
func (s *Session) Read(ctx context.Context, w io.Writer, sections []rhpv2.RPCReadRequestSection, price types.Currency) (err error) {
	defer wrapErr(&err, "Read")
	defer recordRPC(ctx, s.transport, s.revision, rhpv2.RPCReadID, &err)()
	defer recordContractSpending(ctx, s.revision.ID(), api.ContractSpending{Downloads: price}, &err)

	empty := true
	for _, s := range sections {
		empty = empty && s.Length == 0
	}
	if empty || len(sections) == 0 {
		return nil
	}

	if !s.isRevisable() {
		return ErrContractFinalized
	} else if !s.sufficientFunds(price) {
		return ErrInsufficientFunds
	}

	// construct new revision
	rev := s.revision.Revision
	rev.RevisionNumber++
	newValid, newMissed := updateRevisionOutputs(&rev, price, types.ZeroCurrency)
	revisionHash := hashRevision(rev)
	renterSig := s.key.SignHash(revisionHash)

	// construct the request
	req := &rhpv2.RPCReadRequest{
		Sections:    sections,
		MerkleProof: true,

		RevisionNumber:    rev.RevisionNumber,
		ValidProofValues:  newValid,
		MissedProofValues: newMissed,
		Signature:         renterSig,
	}

	var hostSig *types.Signature
	if err := s.withTransport(ctx, func(transport *rhpv2.Transport) error {
		if err := transport.WriteRequest(rhpv2.RPCReadID, req); err != nil {
			return err
		}

		// ensure we send RPCLoopReadStop before returning
		defer transport.WriteResponse(&rhpv2.RPCReadStop)

		// read all sections
		for _, sec := range sections {
			hostSig, err = s.readSection(w, transport, sec)
			if err != nil {
				return err
			}
			if hostSig != nil {
				break // exit the loop; they won't be sending any more data
			}
		}

		// the host is required to send a signature; if they haven't sent one
		// yet, they should send an empty ReadResponse containing just the
		// signature.
		if hostSig == nil {
			var resp rhpv2.RPCReadResponse
			if err := transport.ReadResponse(&resp, 4096); err != nil {
				return wrapResponseErr(err, "couldn't read signature", "host rejected Read request")
			}
			hostSig = &resp.Signature
		}
		return nil
	}); err != nil {
		return err
	}

	// verify the host signature
	if !s.HostKey().VerifyHash(revisionHash, *hostSig) {
		return errors.New("host's signature is invalid")
	}
	s.revision.Revision = rev
	s.revision.Signatures[0].Signature = renterSig[:]
	s.revision.Signatures[1].Signature = hostSig[:]

	return nil
}

// Reconnect re-establishes a connection to the host by recreating the
// transport, updating the settings and calling the lock RPC.
func (s *Session) Reconnect(ctx context.Context, hostIP string, hostKey types.PublicKey, renterKey types.PrivateKey, contractID types.FileContractID) (err error) {
	defer wrapErr(&err, "Reconnect")

	if s.transport != nil {
		s.transport.Close()
	}

	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", hostIP)
	if err != nil {
		return err
	}
	s.transport, err = rhpv2.NewRenterTransport(conn, hostKey)
	if err != nil {
		return err
	}

	s.key = renterKey
	if err = s.lock(ctx, contractID, renterKey, 10*time.Second); err != nil {
		s.transport.Close()
		return err
	}

	if err := s.updateSettings(ctx); err != nil {
		s.transport.Close()
		return err
	}

	s.lastSeen = time.Now()
	return nil
}

// Refresh checks whether the session is still usable, if an error is returned
// the session must be reconnected.
func (s *Session) Refresh(ctx context.Context, sessionTTL time.Duration, renterKey types.PrivateKey, contractID types.FileContractID) error {
	if s.transport == nil {
		return errors.New("no transport")
	}

	if time.Since(s.lastSeen) >= sessionTTL {
		// use RPCSettings as a generic "ping"
		if err := s.updateSettings(ctx); err != nil {
			return err
		}
	}

	if s.revision.ID() != contractID {
		// connected, but not locking the correct contract
		if s.revision.ID() != (types.FileContractID{}) {
			if err := s.Unlock(ctx); err != nil {
				return err
			}
		}
		if err := s.lock(ctx, contractID, renterKey, 10*time.Second); err != nil {
			return err
		}

		s.key = renterKey
		if err := s.updateSettings(ctx); err != nil {
			return err
		}
	}
	s.lastSeen = time.Now()
	return nil
}

// RenewContract negotiates a new file contract and initial revision for data
// already stored with a host. The old contract is "cleared," reverting its
// filesize to zero.
func (s *Session) RenewContract(ctx context.Context, txnSet []types.Transaction, finalPayment types.Currency) (_ rhpv2.ContractRevision, _ []types.Transaction, err error) {
	defer wrapErr(&err, "RenewContract")

	// strip our signatures before sending
	parents, txn := txnSet[:len(txnSet)-1], txnSet[len(txnSet)-1]
	renterContractSignatures := txn.Signatures
	txnSet[len(txnSet)-1].Signatures = nil

	// construct the final revision of the old contract
	finalOldRevision := s.revision.Revision
	newValid, _ := updateRevisionOutputs(&finalOldRevision, finalPayment, types.ZeroCurrency)
	finalOldRevision.MissedProofOutputs = finalOldRevision.ValidProofOutputs
	finalOldRevision.Filesize = 0
	finalOldRevision.FileMerkleRoot = types.Hash256{}
	finalOldRevision.RevisionNumber = math.MaxUint64

	// construct the renew request
	req := &rhpv2.RPCRenewAndClearContractRequest{
		Transactions:           txnSet,
		RenterKey:              s.revision.Revision.UnlockConditions.PublicKeys[0],
		FinalValidProofValues:  newValid,
		FinalMissedProofValues: newValid,
	}

	// send the request
	var resp rhpv2.RPCFormContractAdditions
	if err := s.withTransport(ctx, func(transport *rhpv2.Transport) error {
		if err := transport.WriteRequest(rhpv2.RPCRenewClearContractID, req); err != nil {
			return err
		}
		return transport.ReadResponse(&resp, 65536)
	}); err != nil {
		return rhpv2.ContractRevision{}, nil, err
	}

	// merge host additions with txn
	txn.SiacoinInputs = append(txn.SiacoinInputs, resp.Inputs...)
	txn.SiacoinOutputs = append(txn.SiacoinOutputs, resp.Outputs...)

	// create initial (no-op) revision, transaction, and signature
	fc := txn.FileContracts[0]
	initRevision := types.FileContractRevision{
		ParentID:         txn.FileContractID(0),
		UnlockConditions: s.revision.Revision.UnlockConditions,
		FileContract: types.FileContract{
			RevisionNumber:     1,
			Filesize:           fc.Filesize,
			FileMerkleRoot:     fc.FileMerkleRoot,
			WindowStart:        fc.WindowStart,
			WindowEnd:          fc.WindowEnd,
			ValidProofOutputs:  fc.ValidProofOutputs,
			MissedProofOutputs: fc.MissedProofOutputs,
			UnlockHash:         fc.UnlockHash,
		},
	}
	revSig := s.key.SignHash(hashRevision(initRevision))
	renterRevisionSig := types.TransactionSignature{
		ParentID:       types.Hash256(initRevision.ParentID),
		CoveredFields:  types.CoveredFields{FileContractRevisions: []uint64{0}},
		PublicKeyIndex: 0,
		Signature:      revSig[:],
	}

	// create  signatures
	finalRevSig := s.key.SignHash(hashRevision(finalOldRevision))
	renterSigs := &rhpv2.RPCRenewAndClearContractSignatures{
		ContractSignatures:     renterContractSignatures,
		RevisionSignature:      renterRevisionSig,
		FinalRevisionSignature: finalRevSig,
	}

	// send the signatures and read the host's signatures
	var hostSigs rhpv2.RPCRenewAndClearContractSignatures
	if err := s.withTransport(ctx, func(transport *rhpv2.Transport) error {
		if err := transport.WriteResponse(renterSigs); err != nil {
			return err
		}
		return transport.ReadResponse(&hostSigs, 4096)
	}); err != nil {
		return rhpv2.ContractRevision{}, nil, err
	}

	// merge host signatures with our own
	txn.Signatures = append(renterContractSignatures, hostSigs.ContractSignatures...)
	signedTxnSet := append(resp.Parents, append(parents, txn)...)
	return rhpv2.ContractRevision{
		Revision:   initRevision,
		Signatures: [2]types.TransactionSignature{renterRevisionSig, hostSigs.RevisionSignature},
	}, signedTxnSet, nil
}

// Revision returns the current revision of the contract.
func (s *Session) Revision() rhpv2.ContractRevision { return s.revision }

// SectorRoots calls the SectorRoots RPC, returning the requested range of
// sector Merkle roots of the currently-locked contract.
func (s *Session) SectorRoots(ctx context.Context, offset, n uint64, price types.Currency) (roots []types.Hash256, err error) {
	defer wrapErr(&err, "SectorRoots")
	defer recordRPC(ctx, s.transport, s.revision, rhpv2.RPCSectorRootsID, &err)()

	if !s.isRevisable() {
		return nil, ErrContractFinalized
	} else if offset+n > s.revision.NumSectors() {
		return nil, errors.New("requested range is out-of-bounds")
	} else if n == 0 {
		return nil, nil
	} else if !s.sufficientFunds(price) {
		return nil, ErrInsufficientFunds
	}

	// construct new revision
	rev := s.revision.Revision
	rev.RevisionNumber++
	newValid, newMissed := updateRevisionOutputs(&rev, price, types.ZeroCurrency)
	revisionHash := hashRevision(rev)

	req := &rhpv2.RPCSectorRootsRequest{
		RootOffset: uint64(offset),
		NumRoots:   uint64(n),

		RevisionNumber:    rev.RevisionNumber,
		ValidProofValues:  newValid,
		MissedProofValues: newMissed,
		Signature:         s.key.SignHash(revisionHash),
	}

	// execute the sector roots RPC
	var resp rhpv2.RPCSectorRootsResponse
	err = s.withTransport(ctx, func(t *rhpv2.Transport) error {
		if err := t.WriteRequest(rhpv2.RPCSectorRootsID, req); err != nil {
			return err
		} else if err := t.ReadResponse(&resp, uint64(4096+32*n)); err != nil {
			readCtx := fmt.Sprintf("couldn't read %v response", rhpv2.RPCSectorRootsID)
			rejectCtx := fmt.Sprintf("host rejected %v request", rhpv2.RPCSectorRootsID)
			return wrapResponseErr(err, readCtx, rejectCtx)
		} else {
			return nil
		}
	})

	// verify the host signature
	if !s.HostKey().VerifyHash(revisionHash, resp.Signature) {
		return nil, errors.New("host's signature is invalid")
	}
	s.revision.Revision = rev
	s.revision.Signatures[0].Signature = req.Signature[:]
	s.revision.Signatures[1].Signature = resp.Signature[:]

	// verify the proof
	if !rhpv2.VerifySectorRangeProof(resp.MerkleProof, resp.SectorRoots, offset, offset+n, s.revision.NumSectors(), rev.FileMerkleRoot) {
		return nil, ErrInvalidMerkleProof
	}
	return resp.SectorRoots, nil
}

// Settings returns the host's current settings.
func (s *Session) Settings() rhpv2.HostSettings { return s.settings }

// Unlock calls the Unlock RPC, unlocking the currently-locked contract and
// rendering the Session unusable.
//
// Note that it is typically not necessary to explicitly unlock a contract; the
// host will do so automatically when the connection closes.
func (s *Session) Unlock(ctx context.Context) (err error) {
	defer wrapErr(&err, "Unlock")
	s.revision = rhpv2.ContractRevision{}
	s.key = nil

	return s.withTransport(ctx, func(transport *rhpv2.Transport) error {
		return transport.WriteRequest(rhpv2.RPCUnlockID, nil)
	})
}

// Write implements the Write RPC, except for ActionUpdate. A Merkle proof is
// always requested.
func (s *Session) Write(ctx context.Context, actions []rhpv2.RPCWriteAction, price, collateral types.Currency) (err error) {
	defer wrapErr(&err, "Write")
	defer recordRPC(ctx, s.transport, s.revision, rhpv2.RPCWriteID, &err)()
	defer recordContractSpending(ctx, s.revision.ID(), api.ContractSpending{Uploads: price}, &err)

	if !s.isRevisable() {
		return ErrContractFinalized
	} else if len(actions) == 0 {
		return nil
	} else if !s.sufficientFunds(price) {
		return ErrInsufficientFunds
	} else if !s.sufficientCollateral(collateral) {
		return ErrInsufficientCollateral
	}

	rev := s.revision.Revision
	newFilesize := rev.Filesize
	for _, action := range actions {
		switch action.Type {
		case rhpv2.RPCWriteActionAppend:
			newFilesize += rhpv2.SectorSize
		case rhpv2.RPCWriteActionTrim:
			newFilesize -= rhpv2.SectorSize * action.A
		}
	}

	// calculate new revision outputs
	newValid, newMissed := updateRevisionOutputs(&rev, price, collateral)

	// compute appended roots in parallel with I/O
	precompChan := make(chan struct{})
	go func() {
		s.appendRoots = s.appendRoots[:0]
		for _, action := range actions {
			if action.Type == rhpv2.RPCWriteActionAppend {
				s.appendRoots = append(s.appendRoots, rhpv2.SectorRoot((*[rhpv2.SectorSize]byte)(action.Data)))
			}
		}
		close(precompChan)
	}()
	// ensure that the goroutine has exited before we return
	defer func() { <-precompChan }()

	// create request
	req := &rhpv2.RPCWriteRequest{
		Actions:     actions,
		MerkleProof: true,

		RevisionNumber:    rev.RevisionNumber + 1,
		ValidProofValues:  newValid,
		MissedProofValues: newMissed,
	}

	// send request and read merkle proof
	var merkleResp rhpv2.RPCWriteMerkleProof
	if err := s.withTransport(ctx, func(transport *rhpv2.Transport) error {
		if err := transport.WriteRequest(rhpv2.RPCWriteID, req); err != nil {
			return err
		} else if err := transport.ReadResponse(&merkleResp, 4096); err != nil {
			return wrapResponseErr(err, "couldn't read Merkle proof response", "host rejected Write request")
		} else {
			return nil
		}
	}); err != nil {
		return err
	}

	// verify proof
	proofHashes := merkleResp.OldSubtreeHashes
	leafHashes := merkleResp.OldLeafHashes
	oldRoot, newRoot := types.Hash256(rev.FileMerkleRoot), merkleResp.NewMerkleRoot
	<-precompChan
	if newFilesize > 0 && !rhpv2.VerifyDiffProof(actions, s.revision.NumSectors(), proofHashes, leafHashes, oldRoot, newRoot, s.appendRoots) {
		err := ErrInvalidMerkleProof
		s.withTransport(ctx, func(transport *rhpv2.Transport) error { return transport.WriteResponseErr(err) })
		return err
	}

	// update revision
	rev.RevisionNumber++
	rev.Filesize = newFilesize
	copy(rev.FileMerkleRoot[:], newRoot[:])
	revisionHash := hashRevision(rev)
	renterSig := &rhpv2.RPCWriteResponse{
		Signature: s.key.SignHash(revisionHash),
	}

	// exchange signatures
	var hostSig rhpv2.RPCWriteResponse
	if err := s.withTransport(ctx, func(transport *rhpv2.Transport) error {
		if err := transport.WriteResponse(renterSig); err != nil {
			return fmt.Errorf("couldn't write signature response: %w", err)
		} else if err := transport.ReadResponse(&hostSig, 4096); err != nil {
			return wrapResponseErr(err, "couldn't read signature response", "host rejected Write signature")
		} else {
			return nil
		}
	}); err != nil {
		return err
	}

	// verify the host signature
	if !s.HostKey().VerifyHash(revisionHash, hostSig.Signature) {
		return errors.New("host's signature is invalid")
	}
	s.revision.Revision = rev
	s.revision.Signatures[0].Signature = renterSig.Signature[:]
	s.revision.Signatures[1].Signature = hostSig.Signature[:]
	return nil
}

func (s *Session) isRevisable() bool {
	return s.revision.Revision.RevisionNumber < math.MaxUint64
}

// lock calls the lock RPC, updating the current contract revision. The timeout
// specifies how long the host should wait while attempting to acquire the lock.
// Note that timeouts are serialized in milliseconds, so a timeout of less than
// 1ms will be rounded down to 0. (A timeout of 0 is valid: it means that the
// lock will only be acquired if the contract is unlocked at the moment the host
// receives the RPC.)
func (s *Session) lock(ctx context.Context, id types.FileContractID, key types.PrivateKey, timeout time.Duration) (err error) {
	defer wrapErr(&err, "Lock")
	defer recordRPC(ctx, s.transport, rhpv2.ContractRevision{}, rhpv2.RPCLockID, &err)()
	req := &rhpv2.RPCLockRequest{
		ContractID: id,
		Signature:  s.transport.SignChallenge(key),
		Timeout:    uint64(timeout.Milliseconds()),
	}

	// execute lock RPC
	var resp rhpv2.RPCLockResponse
	if err := s.withTransport(ctx, func(transport *rhpv2.Transport) error {
		if err := transport.Call(rhpv2.RPCLockID, req, &resp); err != nil {
			return err
		}
		transport.SetChallenge(resp.NewChallenge)
		return nil
	}); err != nil {
		return err
	}

	// verify claimed revision
	if len(resp.Signatures) != 2 {
		return fmt.Errorf("host returned wrong number of signatures (expected 2, got %v)", len(resp.Signatures))
	} else if len(resp.Signatures[0].Signature) != 64 || len(resp.Signatures[1].Signature) != 64 {
		return errors.New("signatures on claimed revision have wrong length")
	}
	revHash := hashRevision(resp.Revision)
	if !key.PublicKey().VerifyHash(revHash, *(*types.Signature)(resp.Signatures[0].Signature)) {
		return errors.New("renter's signature on claimed revision is invalid")
	} else if !s.transport.HostKey().VerifyHash(revHash, *(*types.Signature)(resp.Signatures[1].Signature)) {
		return errors.New("host's signature on claimed revision is invalid")
	} else if !resp.Acquired {
		return ErrContractLocked
	} else if resp.Revision.RevisionNumber == math.MaxUint64 {
		return ErrContractFinalized
	}
	s.revision = rhpv2.ContractRevision{
		Revision:   resp.Revision,
		Signatures: [2]types.TransactionSignature{resp.Signatures[0], resp.Signatures[1]},
	}
	return nil
}

func (s *Session) readSection(w io.Writer, t *rhpv2.Transport, sec rhpv2.RPCReadRequestSection) (hostSig *types.Signature, _ error) {
	// NOTE: normally, we would call ReadResponse here to read an AEAD RPC
	// message, verify the tag and decrypt, and then pass the data to
	// VerifyProof. As an optimization, we instead stream the message
	// through a Merkle proof verifier before verifying the AEAD tag.
	// Security therefore depends on the caller of Read discarding any data
	// written to w in the event that verification fails.
	msgReader, err := t.RawResponse(4096 + uint64(sec.Length))
	if err != nil {
		return nil, wrapResponseErr(err, "couldn't read sector data", "host rejected Read request")
	}
	// Read the signature, which may or may not be present.
	lenbuf := make([]byte, 8)
	if _, err := io.ReadFull(msgReader, lenbuf); err != nil {
		return nil, fmt.Errorf("couldn't read signature len: %w", err)
	}
	if n := binary.LittleEndian.Uint64(lenbuf); n > 0 {
		hostSig = new(types.Signature)
		if _, err := io.ReadFull(msgReader, hostSig[:]); err != nil {
			return nil, fmt.Errorf("couldn't read signature: %w", err)
		}
	}
	// stream the sector data into w and the proof verifier
	if _, err := io.ReadFull(msgReader, lenbuf); err != nil {
		return nil, fmt.Errorf("couldn't read data len: %w", err)
	} else if binary.LittleEndian.Uint64(lenbuf) != uint64(sec.Length) {
		return nil, errors.New("host sent wrong amount of sector data")
	}
	proofStart := sec.Offset / rhpv2.LeafSize
	proofEnd := proofStart + sec.Length/rhpv2.LeafSize
	rpv := rhpv2.NewRangeProofVerifier(proofStart, proofEnd)
	tee := io.TeeReader(io.LimitReader(msgReader, int64(sec.Length)), &segWriter{w: w})
	// the proof verifier Reads one segment at a time, so bufio is crucial
	// for performance here
	if _, err := rpv.ReadFrom(bufio.NewReaderSize(tee, 1<<16)); err != nil {
		return nil, fmt.Errorf("couldn't stream sector data: %w", err)
	}
	// read the Merkle proof
	if _, err := io.ReadFull(msgReader, lenbuf); err != nil {
		return nil, fmt.Errorf("couldn't read proof len: %w", err)
	}
	if binary.LittleEndian.Uint64(lenbuf) != uint64(rhpv2.RangeProofSize(rhpv2.LeavesPerSector, proofStart, proofEnd)) {
		return nil, errors.New("invalid proof size")
	}
	proof := make([]types.Hash256, binary.LittleEndian.Uint64(lenbuf))
	for i := range proof {
		if _, err := io.ReadFull(msgReader, proof[i][:]); err != nil {
			return nil, fmt.Errorf("couldn't read Merkle proof: %w", err)
		}
	}
	// verify the message tag and the Merkle proof
	if err := msgReader.VerifyTag(); err != nil {
		return nil, err
	}
	if !rpv.Verify(proof, sec.MerkleRoot) {
		return nil, ErrInvalidMerkleProof
	}
	return
}

func (s *Session) sufficientFunds(price types.Currency) bool {
	return s.revision.RenterFunds().Cmp(price) >= 0
}

func (s *Session) sufficientCollateral(collateral types.Currency) bool {
	return s.revision.Revision.MissedProofOutputs[1].Value.Cmp(collateral) >= 0
}

func (s *Session) updateSettings(ctx context.Context) (err error) {
	defer wrapErr(&err, "Settings")
	defer recordRPC(ctx, s.transport, rhpv2.ContractRevision{}, rhpv2.RPCSettingsID, &err)()

	var resp rhpv2.RPCSettingsResponse
	if err := s.withTransport(ctx, func(transport *rhpv2.Transport) error {
		return transport.Call(rhpv2.RPCSettingsID, nil, &resp)
	}); err != nil {
		return err
	}

	if err := json.Unmarshal(resp.Settings, &s.settings); err != nil {
		return fmt.Errorf("couldn't unmarshal json: %w", err)
	}
	return
}

func (s *Session) withTransport(ctx context.Context, fn func(t *rhpv2.Transport) error) (err error) {
	errChan := make(chan error)
	go func() {
		defer close(errChan)
		errChan <- fn(s.transport)
	}()

	select {
	case err = <-errChan:
		return
	case <-ctx.Done():
		_ = s.transport.Close() // ignore error
		if err = <-errChan; err == nil {
			err = ctx.Err()
		}
	}
	return
}

// NewSession returns a Session locking the provided contract.
func NewSession(t *rhpv2.Transport, key types.PrivateKey, rev rhpv2.ContractRevision, settings rhpv2.HostSettings) *Session {
	return &Session{
		transport: t,
		key:       key,
		revision:  rev,
		settings:  settings,
	}
}

// RPCSettings calls the Settings RPC, returning the host's reported settings.
func RPCSettings(ctx context.Context, t *rhpv2.Transport) (settings rhpv2.HostSettings, err error) {
	defer wrapErr(&err, "Settings")
	defer recordRPC(ctx, t, rhpv2.ContractRevision{}, rhpv2.RPCSettingsID, &err)()

	var resp rhpv2.RPCSettingsResponse
	if err := t.Call(rhpv2.RPCSettingsID, nil, &resp); err != nil {
		return rhpv2.HostSettings{}, err
	} else if err := json.Unmarshal(resp.Settings, &settings); err != nil {
		return rhpv2.HostSettings{}, fmt.Errorf("couldn't unmarshal json: %w", err)
	}
	return settings, nil
}

// RPCFormContract forms a contract with a host.
func RPCFormContract(ctx context.Context, t *rhpv2.Transport, renterKey types.PrivateKey, txnSet []types.Transaction) (_ rhpv2.ContractRevision, _ []types.Transaction, err error) {
	defer wrapErr(&err, "FormContract")

	// strip our signatures before sending
	parents, txn := txnSet[:len(txnSet)-1], txnSet[len(txnSet)-1]
	renterContractSignatures := txn.Signatures
	txnSet[len(txnSet)-1].Signatures = nil

	// create request
	renterPubkey := renterKey.PublicKey()
	req := &rhpv2.RPCFormContractRequest{
		Transactions: txnSet,
		RenterKey:    renterPubkey.UnlockKey(),
	}
	if err := t.WriteRequest(rhpv2.RPCFormContractID, req); err != nil {
		return rhpv2.ContractRevision{}, nil, err
	}

	// execute form contract RPC
	var resp rhpv2.RPCFormContractAdditions
	if err := t.ReadResponse(&resp, 65536); err != nil {
		return rhpv2.ContractRevision{}, nil, err
	}

	// merge host additions with txn
	txn.SiacoinInputs = append(txn.SiacoinInputs, resp.Inputs...)
	txn.SiacoinOutputs = append(txn.SiacoinOutputs, resp.Outputs...)

	// create initial (no-op) revision, transaction, and signature
	fc := txn.FileContracts[0]
	initRevision := types.FileContractRevision{
		ParentID: txn.FileContractID(0),
		UnlockConditions: types.UnlockConditions{
			PublicKeys: []types.UnlockKey{
				renterPubkey.UnlockKey(),
				t.HostKey().UnlockKey(),
			},
			SignaturesRequired: 2,
		},
		FileContract: types.FileContract{
			RevisionNumber:     1,
			Filesize:           fc.Filesize,
			FileMerkleRoot:     fc.FileMerkleRoot,
			WindowStart:        fc.WindowStart,
			WindowEnd:          fc.WindowEnd,
			ValidProofOutputs:  fc.ValidProofOutputs,
			MissedProofOutputs: fc.MissedProofOutputs,
			UnlockHash:         fc.UnlockHash,
		},
	}
	revSig := renterKey.SignHash(hashRevision(initRevision))
	renterRevisionSig := types.TransactionSignature{
		ParentID:       types.Hash256(initRevision.ParentID),
		CoveredFields:  types.CoveredFields{FileContractRevisions: []uint64{0}},
		PublicKeyIndex: 0,
		Signature:      revSig[:],
	}

	// write our signatures
	renterSigs := &rhpv2.RPCFormContractSignatures{
		ContractSignatures: renterContractSignatures,
		RevisionSignature:  renterRevisionSig,
	}
	if err := t.WriteResponse(renterSigs); err != nil {
		return rhpv2.ContractRevision{}, nil, err
	}

	// read the host's signatures and merge them with our own
	var hostSigs rhpv2.RPCFormContractSignatures
	if err := t.ReadResponse(&hostSigs, 4096); err != nil {
		return rhpv2.ContractRevision{}, nil, err
	}

	txn.Signatures = append(renterContractSignatures, hostSigs.ContractSignatures...)
	signedTxnSet := append(resp.Parents, append(parents, txn)...)
	return rhpv2.ContractRevision{
		Revision: initRevision,
		Signatures: [2]types.TransactionSignature{
			renterRevisionSig,
			hostSigs.RevisionSignature,
		},
	}, signedTxnSet, nil
}
