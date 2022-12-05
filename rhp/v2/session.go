package rhp

import (
	"bufio"
	"context"
	"crypto/ed25519"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/bits"
	"net"
	"sort"
	"time"

	"go.sia.tech/renterd/metrics"
	"go.sia.tech/siad/types"
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

// wrapResponseErr formats RPC response errors nicely, wrapping them in either
// readCtx or rejectCtx depending on whether we encountered an I/O error or the
// host sent an explicit error message.
func wrapResponseErr(err error, readCtx, rejectCtx string) error {
	if errors.As(err, new(*RPCError)) {
		return fmt.Errorf("%s: %w", rejectCtx, err)
	}
	if err != nil {
		return fmt.Errorf("%s: %w", readCtx, err)
	}
	return nil
}

func updateRevisionOutputs(rev *types.FileContractRevision, cost, collateral types.Currency) (valid, missed []types.Currency) {
	// allocate new slices; don't want to risk accidentally sharing memory
	rev.NewValidProofOutputs = append([]types.SiacoinOutput(nil), rev.NewValidProofOutputs...)
	rev.NewMissedProofOutputs = append([]types.SiacoinOutput(nil), rev.NewMissedProofOutputs...)

	// move valid payout from renter to host
	rev.NewValidProofOutputs[0].Value = rev.NewValidProofOutputs[0].Value.Sub(cost)
	rev.NewValidProofOutputs[1].Value = rev.NewValidProofOutputs[1].Value.Add(cost)

	// move missed payout from renter to void
	rev.NewMissedProofOutputs[0].Value = rev.NewMissedProofOutputs[0].Value.Sub(cost)
	rev.NewMissedProofOutputs[2].Value = rev.NewMissedProofOutputs[2].Value.Add(cost)

	// move collateral from host to void
	rev.NewMissedProofOutputs[1].Value = rev.NewMissedProofOutputs[1].Value.Sub(collateral)
	rev.NewMissedProofOutputs[2].Value = rev.NewMissedProofOutputs[2].Value.Add(collateral)

	return []types.Currency{rev.NewValidProofOutputs[0].Value, rev.NewValidProofOutputs[1].Value},
		[]types.Currency{rev.NewMissedProofOutputs[0].Value, rev.NewMissedProofOutputs[1].Value, rev.NewMissedProofOutputs[2].Value}
}

// RPCSectorRootsCost returns the price of a SectorRoots RPC.
func RPCSectorRootsCost(settings HostSettings, n uint64) types.Currency {
	return settings.BaseRPCPrice.
		Add(settings.DownloadBandwidthPrice.Mul64(n * 32)).  // roots
		Add(settings.DownloadBandwidthPrice.Mul64(128 * 32)) // proof
}

// RPCReadCost returns the price of a Read RPC.
func RPCReadCost(settings HostSettings, sections []RPCReadRequestSection) types.Currency {
	sectorAccessPrice := settings.SectorAccessPrice.Mul64(uint64(len(sections)))
	var bandwidth uint64
	for _, sec := range sections {
		bandwidth += sec.Length
		bandwidth += 2 * uint64(bits.Len64(LeavesPerSector)) * 32 // proof
	}
	if bandwidth < minMessageSize {
		bandwidth = minMessageSize
	}
	bandwidthPrice := settings.DownloadBandwidthPrice.Mul64(bandwidth)
	return settings.BaseRPCPrice.Add(sectorAccessPrice).Add(bandwidthPrice)
}

// RPCAppendCost returns the price and collateral of a Write RPC with a single
// append operation.
func RPCAppendCost(settings HostSettings, storageDuration uint64) (price, collateral types.Currency) {
	price = settings.BaseRPCPrice.
		Add(settings.StoragePrice.Mul64(SectorSize).Mul64(storageDuration)).
		Add(settings.UploadBandwidthPrice.Mul64(SectorSize)).
		Add(settings.DownloadBandwidthPrice.Mul64(128 * 32)) // proof
	collateral = settings.Collateral.Mul64(SectorSize).Mul64(storageDuration)
	// add some leeway to reduce chance of host rejecting
	price = price.MulFloat(1.25)
	collateral = collateral.MulFloat(0.95)
	return
}

// RPCDeleteCost returns the price of a Write RPC that deletes n sectors.
func RPCDeleteCost(settings HostSettings, n int) types.Currency {
	price := settings.BaseRPCPrice.
		Add(settings.DownloadBandwidthPrice.Mul64(128 * 32)) // proof
	return price.MulFloat(1.05)
}

// A Session pairs a Transport with a Contract, enabling RPCs that modify the
// Contract.
type Session struct {
	transport   *Transport
	contract    Contract
	key         PrivateKey
	appendRoots []Hash256
}

// Transport returns the underlying Transport of the session.
func (s *Session) Transport() *Transport { return s.transport }

// HostKey returns the public key of the host.
func (s *Session) HostKey() PublicKey { return s.contract.HostKey() }

// Contract returns the current revision of the contract.
func (s *Session) Contract() Contract { return s.contract }

func (s *Session) isRevisable() bool {
	return s.contract.Revision.NewRevisionNumber < math.MaxUint64
}

func (s *Session) sufficientFunds(price types.Currency) bool {
	return s.contract.RenterFunds().Cmp(price) >= 0
}

func (s *Session) sufficientCollateral(collateral types.Currency) bool {
	return s.contract.Revision.NewMissedProofOutputs[1].Value.Cmp(collateral) >= 0
}

func recordRPC(ctx context.Context, t *Transport, c Contract, id Specifier, err *error) func() {
	startTime := time.Now()
	contractID := c.ID()
	var startFunds types.Currency
	if len(c.Revision.NewValidProofOutputs) > 0 {
		startFunds = c.Revision.NewValidProofOutputs[0].Value
	}
	var startCollateral types.Currency
	if len(c.Revision.NewMissedProofOutputs) > 1 {
		startCollateral = c.Revision.NewMissedProofOutputs[1].Value
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
		if len(c.Revision.NewValidProofOutputs) > 0 && startFunds.Cmp(c.Revision.NewValidProofOutputs[0].Value) > 0 {
			m.Cost = startFunds.Sub(c.Revision.NewValidProofOutputs[0].Value)
		}
		if len(c.Revision.NewMissedProofOutputs) > 1 && startCollateral.Cmp(c.Revision.NewMissedProofOutputs[1].Value) > 0 {
			m.Collateral = startCollateral.Sub(c.Revision.NewMissedProofOutputs[1].Value)
		}
		metrics.Record(ctx, m)
	}
}

// SectorRoots calls the SectorRoots RPC, returning the requested range of
// sector Merkle roots of the currently-locked contract.
func (s *Session) SectorRoots(ctx context.Context, offset, n uint64, price types.Currency) (_ []Hash256, err error) {
	defer wrapErr(&err, "SectorRoots")
	defer recordRPC(ctx, s.transport, s.contract, RPCSectorRootsID, &err)()

	if !s.isRevisable() {
		return nil, ErrContractFinalized
	} else if offset+n > s.contract.NumSectors() {
		return nil, errors.New("requested range is out-of-bounds")
	} else if n == 0 {
		return nil, nil
	} else if !s.sufficientFunds(price) {
		return nil, ErrInsufficientFunds
	}

	// construct new revision
	rev := s.contract.Revision
	rev.NewRevisionNumber++
	newValid, newMissed := updateRevisionOutputs(&rev, price, types.ZeroCurrency)
	revisionHash := hashRevision(rev)

	req := &RPCSectorRootsRequest{
		RootOffset: uint64(offset),
		NumRoots:   uint64(n),

		NewRevisionNumber:    rev.NewRevisionNumber,
		NewValidProofValues:  newValid,
		NewMissedProofValues: newMissed,
		Signature:            s.key.SignHash(revisionHash),
	}
	var resp RPCSectorRootsResponse
	if err := s.transport.WriteRequest(RPCSectorRootsID, req); err != nil {
		return nil, err
	}
	if err := s.transport.ReadResponse(&resp, uint64(4096+32*n)); err != nil {
		readCtx := fmt.Sprintf("couldn't read %v response", RPCSectorRootsID)
		rejectCtx := fmt.Sprintf("host rejected %v request", RPCSectorRootsID)
		return nil, wrapResponseErr(err, readCtx, rejectCtx)
	}

	// verify the host signature
	if !s.HostKey().VerifyHash(revisionHash, resp.Signature) {
		return nil, errors.New("host's signature is invalid")
	}
	s.contract.Revision = rev
	s.contract.Signatures[0].Signature = req.Signature[:]
	s.contract.Signatures[1].Signature = resp.Signature[:]

	// verify the proof
	if !VerifySectorRangeProof(resp.MerkleProof, resp.SectorRoots, offset, offset+n, s.contract.NumSectors(), Hash256(rev.NewFileMerkleRoot)) {
		return nil, ErrInvalidMerkleProof
	}
	return resp.SectorRoots, nil
}

// helper type for ensuring that we always write in multiples of LeafSize,
// which is required by e.g. (renter.EncryptionKey).XORKeyStream
type segWriter struct {
	w   io.Writer
	buf [LeafSize * 64]byte
	len int
}

func (sw *segWriter) Write(p []byte) (int, error) {
	lenp := len(p)
	for len(p) > 0 {
		n := copy(sw.buf[sw.len:], p)
		sw.len += n
		p = p[n:]
		segs := sw.buf[:sw.len-(sw.len%LeafSize)]
		if _, err := sw.w.Write(segs); err != nil {
			return 0, err
		}
		sw.len = copy(sw.buf[:], sw.buf[len(segs):sw.len])
	}
	return lenp, nil
}

// Read calls the Read RPC, writing the requested sections of sector data to w.
// Merkle proofs are always requested.
//
// Note that sector data is streamed to w before it has been validated. Callers
// MUST check the returned error, and discard any data written to w if the error
// is non-nil. Failure to do so may allow an attacker to inject malicious data.
func (s *Session) Read(ctx context.Context, w io.Writer, sections []RPCReadRequestSection, price types.Currency) (err error) {
	defer wrapErr(&err, "Read")
	defer recordRPC(ctx, s.transport, s.contract, RPCReadID, &err)()

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
	rev := s.contract.Revision
	rev.NewRevisionNumber++
	newValid, newMissed := updateRevisionOutputs(&rev, price, types.ZeroCurrency)
	revisionHash := hashRevision(rev)
	renterSig := s.key.SignHash(revisionHash)

	// send request
	req := &RPCReadRequest{
		Sections:    sections,
		MerkleProof: true,

		NewRevisionNumber:    rev.NewRevisionNumber,
		NewValidProofValues:  newValid,
		NewMissedProofValues: newMissed,
		Signature:            renterSig,
	}
	if err := s.transport.WriteRequest(RPCReadID, req); err != nil {
		return err
	}

	// host will now stream back responses; ensure we send RPCLoopReadStop
	// before returning
	defer s.transport.WriteResponse(&RPCReadStop)
	var hostSig *Signature
	for _, sec := range sections {
		// NOTE: normally, we would call ReadResponse here to read an AEAD RPC
		// message, verify the tag and decrypt, and then pass the data to
		// VerifyProof. As an optimization, we instead stream the message
		// through a Merkle proof verifier before verifying the AEAD tag.
		// Security therefore depends on the caller of Read discarding any data
		// written to w in the event that verification fails.
		msgReader, err := s.transport.RawResponse(4096 + uint64(sec.Length))
		if err != nil {
			return wrapResponseErr(err, "couldn't read sector data", "host rejected Read request")
		}
		// Read the signature, which may or may not be present.
		lenbuf := make([]byte, 8)
		if _, err := io.ReadFull(msgReader, lenbuf); err != nil {
			return fmt.Errorf("couldn't read signature len: %w", err)
		}
		if n := binary.LittleEndian.Uint64(lenbuf); n > 0 {
			hostSig = new(Signature)
			if _, err := io.ReadFull(msgReader, hostSig[:]); err != nil {
				return fmt.Errorf("couldn't read signature: %w", err)
			}
		}
		// stream the sector data into w and the proof verifier
		if _, err := io.ReadFull(msgReader, lenbuf); err != nil {
			return fmt.Errorf("couldn't read data len: %w", err)
		} else if binary.LittleEndian.Uint64(lenbuf) != uint64(sec.Length) {
			return errors.New("host sent wrong amount of sector data")
		}
		proofStart := sec.Offset / LeafSize
		proofEnd := proofStart + sec.Length/LeafSize
		rpv := NewRangeProofVerifier(proofStart, proofEnd)
		tee := io.TeeReader(io.LimitReader(msgReader, int64(sec.Length)), &segWriter{w: w})
		// the proof verifier Reads one segment at a time, so bufio is crucial
		// for performance here
		if _, err := rpv.ReadFrom(bufio.NewReaderSize(tee, 1<<16)); err != nil {
			return fmt.Errorf("couldn't stream sector data: %w", err)
		}
		// read the Merkle proof
		if _, err := io.ReadFull(msgReader, lenbuf); err != nil {
			return fmt.Errorf("couldn't read proof len: %w", err)
		}
		if binary.LittleEndian.Uint64(lenbuf) != uint64(RangeProofSize(LeavesPerSector, proofStart, proofEnd)) {
			return errors.New("invalid proof size")
		}
		proof := make([]Hash256, binary.LittleEndian.Uint64(lenbuf))
		for i := range proof {
			if _, err := io.ReadFull(msgReader, proof[i][:]); err != nil {
				return fmt.Errorf("couldn't read Merkle proof: %w", err)
			}
		}
		// verify the message tag and the Merkle proof
		if err := msgReader.VerifyTag(); err != nil {
			return err
		}
		if !rpv.Verify(proof, sec.MerkleRoot) {
			return ErrInvalidMerkleProof
		}
		// if the host sent a signature, exit the loop; they won't be sending
		// any more data
		if hostSig != nil {
			break
		}
	}
	if hostSig == nil {
		// the host is required to send a signature; if they haven't sent one
		// yet, they should send an empty ReadResponse containing just the
		// signature.
		var resp RPCReadResponse
		if err := s.transport.ReadResponse(&resp, 4096); err != nil {
			return wrapResponseErr(err, "couldn't read signature", "host rejected Read request")
		}
		hostSig = &resp.Signature
	}

	// verify the host signature
	if !s.HostKey().VerifyHash(revisionHash, *hostSig) {
		return errors.New("host's signature is invalid")
	}
	s.contract.Revision = rev
	s.contract.Signatures[0].Signature = renterSig[:]
	s.contract.Signatures[1].Signature = hostSig[:]

	return nil
}

// Write implements the Write RPC, except for ActionUpdate. A Merkle proof is
// always requested.
func (s *Session) Write(ctx context.Context, actions []RPCWriteAction, price, collateral types.Currency) (err error) {
	defer wrapErr(&err, "Write")
	defer recordRPC(ctx, s.transport, s.contract, RPCWriteID, &err)()

	if !s.isRevisable() {
		return ErrContractFinalized
	} else if len(actions) == 0 {
		return nil
	} else if !s.sufficientFunds(price) {
		return ErrInsufficientFunds
	} else if !s.sufficientCollateral(collateral) {
		return ErrInsufficientCollateral
	}

	rev := s.contract.Revision
	newFileSize := rev.NewFileSize
	for _, action := range actions {
		switch action.Type {
		case RPCWriteActionAppend:
			newFileSize += SectorSize
		case RPCWriteActionTrim:
			newFileSize -= SectorSize * action.A
		}
	}

	// calculate new revision outputs
	newValid, newMissed := updateRevisionOutputs(&rev, price, collateral)

	// compute appended roots in parallel with I/O
	precompChan := make(chan struct{})
	go func() {
		s.appendRoots = precomputeAppendRoots(actions)
		close(precompChan)
	}()
	// ensure that the goroutine has exited before we return
	defer func() { <-precompChan }()

	// send request
	req := &RPCWriteRequest{
		Actions:     actions,
		MerkleProof: true,

		NewRevisionNumber:    rev.NewRevisionNumber + 1,
		NewValidProofValues:  newValid,
		NewMissedProofValues: newMissed,
	}
	if err := s.transport.WriteRequest(RPCWriteID, req); err != nil {
		return err
	}

	// read and verify Merkle proof
	var merkleResp RPCWriteMerkleProof
	if err := s.transport.ReadResponse(&merkleResp, 4096); err != nil {
		return wrapResponseErr(err, "couldn't read Merkle proof response", "host rejected Write request")
	}
	proofHashes := merkleResp.OldSubtreeHashes
	leafHashes := merkleResp.OldLeafHashes
	oldRoot, newRoot := Hash256(rev.NewFileMerkleRoot), merkleResp.NewMerkleRoot
	<-precompChan
	if newFileSize > 0 && !VerifyDiffProof(actions, s.contract.NumSectors(), proofHashes, leafHashes, oldRoot, newRoot, s.appendRoots) {
		err := ErrInvalidMerkleProof
		s.transport.WriteResponseErr(err)
		return err
	}

	// update revision and exchange signatures
	rev.NewRevisionNumber++
	rev.NewFileSize = newFileSize
	copy(rev.NewFileMerkleRoot[:], newRoot[:])
	revisionHash := hashRevision(rev)
	renterSig := &RPCWriteResponse{
		Signature: s.key.SignHash(revisionHash),
	}
	if err := s.transport.WriteResponse(renterSig); err != nil {
		return fmt.Errorf("couldn't write signature response: %w", err)
	}
	var hostSig RPCWriteResponse
	if err := s.transport.ReadResponse(&hostSig, 4096); err != nil {
		return wrapResponseErr(err, "couldn't read signature response", "host rejected Write signature")
	}

	// verify the host signature
	if !s.HostKey().VerifyHash(revisionHash, hostSig.Signature) {
		return errors.New("host's signature is invalid")
	}
	s.contract.Revision = rev
	s.contract.Signatures[0].Signature = renterSig.Signature[:]
	s.contract.Signatures[1].Signature = hostSig.Signature[:]

	return nil
}

// Append calls the Write RPC with a single action, appending the provided
// sector. It returns the Merkle root of the sector.
func (s *Session) Append(ctx context.Context, sector *[SectorSize]byte, price, collateral types.Currency) (Hash256, error) {
	err := s.Write(ctx, []RPCWriteAction{{
		Type: RPCWriteActionAppend,
		Data: sector[:],
	}}, price, collateral)
	if err != nil {
		return Hash256{}, err
	}
	return s.appendRoots[0], nil
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
	var actions []RPCWriteAction
	cIndex := s.contract.NumSectors() - 1
	for _, rIndex := range sectorIndices {
		if cIndex != rIndex {
			// swap a "good" sector for a "bad" sector
			actions = append(actions, RPCWriteAction{
				Type: RPCWriteActionSwap,
				A:    uint64(cIndex),
				B:    uint64(rIndex),
			})
		}
		cIndex--
	}
	// trim all "bad" sectors
	actions = append(actions, RPCWriteAction{
		Type: RPCWriteActionTrim,
		A:    uint64(len(sectorIndices)),
	})

	// request the swap+delete operation
	//
	// NOTE: siad hosts will accept up to 20 MiB of data in the request,
	// which should be sufficient to delete up to 2.5 TiB of sector data
	// at a time.
	return s.Write(ctx, actions, price, types.ZeroCurrency)
}

// Unlock calls the Unlock RPC, unlocking the currently-locked contract and
// rendering the Session unusable.
//
// Note that it is typically not necessary to explicitly unlock a contract; the
// host will do so automatically when the connection closes.
func (s *Session) Unlock() (err error) {
	defer wrapErr(&err, "Unlock")
	s.contract = Contract{}
	s.key = nil
	return s.transport.WriteRequest(RPCUnlockID, nil)
}

// Close gracefully terminates the session and closes the underlying connection.
func (s *Session) Close() (err error) {
	defer wrapErr(&err, "Close")
	return s.transport.Close()
}

// RPCSettings calls the Settings RPC, returning the host's reported settings.
func RPCSettings(ctx context.Context, t *Transport) (settings HostSettings, err error) {
	defer wrapErr(&err, "Settings")
	defer recordRPC(ctx, t, Contract{}, RPCSettingsID, &err)()
	var resp RPCSettingsResponse
	if err := t.Call(RPCSettingsID, nil, &resp); err != nil {
		return HostSettings{}, err
	} else if err := json.Unmarshal(resp.Settings, &settings); err != nil {
		return HostSettings{}, fmt.Errorf("couldn't unmarshal json: %w", err)
	}
	return settings, nil
}

// RPCLock calls the Lock RPC, returning a Session that can modify the specified
// contract. The timeout specifies how long the host should wait while
// attempting to acquire the lock. Note that timeouts are serialized in
// milliseconds, so a timeout of less than 1ms will be rounded down to 0. (A
// timeout of 0 is valid: it means that the lock will only be acquired if the
// contract is unlocked at the moment the host receives the RPC.)
func RPCLock(ctx context.Context, t *Transport, id types.FileContractID, key PrivateKey, timeout time.Duration) (_ *Session, err error) {
	defer wrapErr(&err, "Lock")
	defer recordRPC(ctx, t, Contract{}, RPCLockID, &err)()
	req := &RPCLockRequest{
		ContractID: id,
		Signature:  t.SignChallenge(key),
		Timeout:    uint64(timeout.Milliseconds()),
	}
	var resp RPCLockResponse
	if err := t.Call(RPCLockID, req, &resp); err != nil {
		return nil, err
	}
	t.SetChallenge(resp.NewChallenge)
	// verify claimed revision
	if len(resp.Signatures) != 2 {
		return nil, fmt.Errorf("host returned wrong number of signatures (expected 2, got %v)", len(resp.Signatures))
	}
	revHash := hashRevision(resp.Revision)
	if !key.PublicKey().VerifyHash(revHash, *(*Signature)(resp.Signatures[0].Signature)) {
		return nil, errors.New("renter's signature on claimed revision is invalid")
	} else if !ed25519.Verify(resp.Revision.UnlockConditions.PublicKeys[1].Key, revHash[:], resp.Signatures[1].Signature) {
		return nil, errors.New("host's signature on claimed revision is invalid")
	} else if !resp.Acquired {
		return nil, ErrContractLocked
	} else if resp.Revision.NewRevisionNumber == math.MaxUint64 {
		return nil, ErrContractFinalized
	}
	return &Session{
		transport: t,
		contract: Contract{
			Revision:   resp.Revision,
			Signatures: [2]types.TransactionSignature{resp.Signatures[0], resp.Signatures[1]},
		},
		key: key,
	}, nil
}

// DialSession is a convenience function that connects to the specified host and
// locks the specified contract.
func DialSession(ctx context.Context, hostIP string, hostKey PublicKey, id types.FileContractID, renterKey PrivateKey) (_ *Session, err error) {
	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", hostIP)
	if err != nil {
		return nil, err
	}
	done := make(chan struct{})
	go func() {
		select {
		case <-done:
		case <-ctx.Done():
			conn.Close()
		}
	}()
	defer func() {
		close(done)
		if ctx.Err() != nil {
			err = ctx.Err()
		}
	}()

	t, err := NewRenterTransport(conn, hostKey)
	if err != nil {
		conn.Close()
		return nil, err
	}
	var timeout time.Duration
	if d, ok := ctx.Deadline(); ok {
		timeout = time.Until(d)
	}
	s, err := RPCLock(ctx, t, id, renterKey, timeout)
	if err != nil {
		t.Close()
		return nil, err
	}
	return s, nil
}

// DeleteSectorActions calculates a set of Write actions that will delete the
// specified sectors from the contract.
func DeleteSectorActions(allRoots, toDelete []Hash256) []RPCWriteAction {
	rootIndices := make(map[Hash256]uint64, len(allRoots))
	for i, root := range allRoots {
		rootIndices[root] = uint64(i)
	}

	// look up the index of each sector to delete
	deleteIndices := make([]uint64, 0, len(toDelete))
	for _, r := range toDelete {
		// if a root isn't present, skip it; the caller probably deleted it
		// previously
		if index, ok := rootIndices[r]; ok {
			deleteIndices = append(deleteIndices, index)
			// deleting here ensures that we only add each root index once, i.e.
			// it guards against duplicates in roots
			delete(rootIndices, r)
		}
	}
	// sort in descending order so that we can use 'range'
	sort.Slice(deleteIndices, func(i, j int) bool {
		return deleteIndices[i] > deleteIndices[j]
	})

	// iterate backwards from the end of the contract, swapping each "good"
	// sector with one of the "bad" sectors, such that all "bad" sectors end up
	// at the end of the contract
	var actions []RPCWriteAction
	cIndex := uint64(len(allRoots) - 1)
	for _, rIndex := range deleteIndices {
		if cIndex != rIndex {
			actions = append(actions, RPCWriteAction{
				Type: RPCWriteActionSwap,
				A:    cIndex,
				B:    rIndex,
			})
		}
		cIndex--
	}
	// trim all "bad" sectors
	actions = append(actions, RPCWriteAction{
		Type: RPCWriteActionTrim,
		A:    uint64(len(deleteIndices)),
	})

	return actions
}
