package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/bits"
	"sort"
	"strings"
	"time"

	rhpv2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"go.sia.tech/siad/build"
)

const (
	// minMessageSize is the minimum size of an RPC message
	minMessageSize = 4096
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

func hashRevision(rev types.FileContractRevision) types.Hash256 {
	h := types.NewHasher()
	rev.EncodeTo(h.E)
	return h.Sum()
}

func updateRevisionOutputs(rev *types.FileContractRevision, cost, collateral types.Currency) (valid, missed []types.Currency, err error) {
	// allocate new slices; don't want to risk accidentally sharing memory
	rev.ValidProofOutputs = append([]types.SiacoinOutput(nil), rev.ValidProofOutputs...)
	rev.MissedProofOutputs = append([]types.SiacoinOutput(nil), rev.MissedProofOutputs...)

	// move valid payout from renter to host
	var underflow, overflow bool
	rev.ValidProofOutputs[0].Value, underflow = rev.ValidProofOutputs[0].Value.SubWithUnderflow(cost)
	rev.ValidProofOutputs[1].Value, overflow = rev.ValidProofOutputs[1].Value.AddWithOverflow(cost)
	if underflow || overflow {
		err = errors.New("insufficient funds to pay host")
		return
	}

	// move missed payout from renter to void
	rev.MissedProofOutputs[0].Value, underflow = rev.MissedProofOutputs[0].Value.SubWithUnderflow(cost)
	rev.MissedProofOutputs[2].Value, overflow = rev.MissedProofOutputs[2].Value.AddWithOverflow(cost)
	if underflow || overflow {
		err = errors.New("insufficient funds to move missed payout to void")
		return
	}

	// move collateral from host to void
	rev.MissedProofOutputs[1].Value, underflow = rev.MissedProofOutputs[1].Value.SubWithUnderflow(collateral)
	rev.MissedProofOutputs[2].Value, overflow = rev.MissedProofOutputs[2].Value.AddWithOverflow(collateral)
	if underflow || overflow {
		err = errors.New("insufficient collateral")
		return
	}

	return []types.Currency{rev.ValidProofOutputs[0].Value, rev.ValidProofOutputs[1].Value},
		[]types.Currency{rev.MissedProofOutputs[0].Value, rev.MissedProofOutputs[1].Value, rev.MissedProofOutputs[2].Value}, nil
}

// RPCSettings calls the Settings RPC, returning the host's reported settings.
func RPCSettings(ctx context.Context, t *rhpv2.Transport) (settings rhpv2.HostSettings, err error) {
	defer wrapErr(&err, "Settings")

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
	if err := t.ReadResponse(&hostSigs, minMessageSize); err != nil {
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

// FetchSignedRevision fetches the latest signed revision for a contract from a host.
// TODO: stop using rhpv2 and upgrade to newer protocol when possible.
func (w *worker) FetchSignedRevision(ctx context.Context, hostIP string, hostKey types.PublicKey, renterKey types.PrivateKey, contractID types.FileContractID, timeout time.Duration) (rhpv2.ContractRevision, error) {
	var rev rhpv2.ContractRevision
	err := w.withTransportV2(ctx, hostKey, hostIP, func(t *rhpv2.Transport) error {
		req := &rhpv2.RPCLockRequest{
			ContractID: contractID,
			Signature:  t.SignChallenge(renterKey),
			Timeout:    uint64(timeout.Milliseconds()),
		}

		// execute lock RPC
		var resp rhpv2.RPCLockResponse
		if err := t.Call(rhpv2.RPCLockID, req, &resp); err != nil {
			return err
		}
		t.SetChallenge(resp.NewChallenge)

		// defer unlock RPC
		defer t.WriteRequest(rhpv2.RPCUnlockID, nil)

		// verify claimed revision
		if resp.Revision.RevisionNumber == math.MaxUint64 {
			return ErrContractFinalized
		} else if len(resp.Signatures) != 2 {
			return fmt.Errorf("host returned wrong number of signatures (expected 2, got %v)", len(resp.Signatures))
		} else if len(resp.Signatures[0].Signature) != 64 || len(resp.Signatures[1].Signature) != 64 {
			return errors.New("signatures on claimed revision have wrong length")
		}
		revHash := hashRevision(resp.Revision)
		if !renterKey.PublicKey().VerifyHash(revHash, *(*types.Signature)(resp.Signatures[0].Signature)) {
			return errors.New("renter's signature on claimed revision is invalid")
		} else if !t.HostKey().VerifyHash(revHash, *(*types.Signature)(resp.Signatures[1].Signature)) {
			return errors.New("host's signature on claimed revision is invalid")
		} else if !resp.Acquired {
			return ErrContractLocked
		}
		rev = rhpv2.ContractRevision{
			Revision:   resp.Revision,
			Signatures: [2]types.TransactionSignature{resp.Signatures[0], resp.Signatures[1]},
		}
		return nil
	})
	return rev, err
}

func (w *worker) PruneContract(ctx context.Context, hostIP string, hostKey types.PublicKey, fcid types.FileContractID, lastKnownRevisionNumber uint64) (deleted, remaining uint64, err error) {
	err = w.withContractLock(ctx, fcid, lockingPriorityPruning, func() error {
		return w.withTransportV2(ctx, hostKey, hostIP, func(t *rhpv2.Transport) error {
			return w.withRevisionV2(ctx, defaultLockTimeout, t, hostKey, fcid, lastKnownRevisionNumber, func(t *rhpv2.Transport, rev rhpv2.ContractRevision, settings rhpv2.HostSettings) (err error) {
				// delete roots
				got, err := w.fetchContractRoots(t, &rev, settings)
				if err != nil {
					return err
				}

				// fetch the roots from the bus
				want, pending, err := w.bus.ContractRoots(ctx, fcid)
				if err != nil {
					return err
				}
				keep := make(map[types.Hash256]struct{})
				for _, root := range append(want, pending...) {
					keep[root] = struct{}{}
				}

				// collect indices for roots we want to prune
				var indices []uint64
				for i, root := range got {
					if _, wanted := keep[root]; wanted {
						delete(keep, root) // prevent duplicates
						continue
					}
					indices = append(indices, uint64(i))
				}
				if len(indices) == 0 {
					return fmt.Errorf("no sectors to prune, database holds %d (%d pending), contract contains %d", len(want)+len(pending), len(pending), len(got))
				}

				// delete the roots from the contract
				deleted, err = w.deleteContractRoots(t, &rev, settings, indices)
				if deleted < uint64(len(indices)) {
					remaining = uint64(len(indices)) - deleted
				}

				// return sizes instead of number of roots
				deleted *= rhpv2.SectorSize
				remaining *= rhpv2.SectorSize
				return
			})
		})
	})
	return
}

func (w *worker) deleteContractRoots(t *rhpv2.Transport, rev *rhpv2.ContractRevision, settings rhpv2.HostSettings, indices []uint64) (deleted uint64, err error) {
	w.logger.Debugw(fmt.Sprintf("deleting %d contract roots (%v)", len(indices), humanReadableSize(len(indices)*rhpv2.SectorSize)), "hk", rev.HostKey(), "fcid", rev.ID())

	// return early
	if len(indices) == 0 {
		return 0, nil
	}

	// sort in descending order so that we can use 'range'
	sort.Slice(indices, func(i, j int) bool {
		return indices[i] > indices[j]
	})

	// decide on the batch size, defaults to ~20mib of sector data but for old
	// hosts we use a much smaller batch size to ensure we nibble away at the
	// problem rather than outright failing or timing out
	batchSize := int(batchSizeDeleteSectors)
	if build.VersionCmp(settings.Version, "1.6.0") < 0 {
		batchSize = 100
	}

	// split the indices into batches
	var batches [][]uint64
	for {
		if len(indices) < batchSize {
			batchSize = len(indices)
		}
		batches = append(batches, indices[:batchSize])
		indices = indices[batchSize:]
		if len(indices) == 0 {
			break
		}
	}

	// derive the renter key
	renterKey := w.deriveRenterKey(rev.HostKey())

	// range over the batches and delete the sectors batch per batch
	for i, batch := range batches {
		if err = func() error {
			var cost types.Currency
			start := time.Now()
			w.logger.Debugw(fmt.Sprintf("starting batch %d/%d of size %d", i+1, len(batches), len(batch)))
			defer func() {
				w.logger.Debugw(fmt.Sprintf("processing batch %d/%d of size %d took %v", i+1, len(batches), len(batch), time.Since(start)), "cost", cost)
			}()

			numSectors := rev.NumSectors()

			// build a set of actions that move the sectors we want to delete
			// towards the end of the contract, preparing them to be trimmed off
			var actions []rhpv2.RPCWriteAction
			cIndex := numSectors - 1
			for _, rIndex := range batch {
				if cIndex != rIndex {
					actions = append(actions, rhpv2.RPCWriteAction{
						Type: rhpv2.RPCWriteActionSwap,
						A:    uint64(cIndex),
						B:    uint64(rIndex),
					})
				}
				cIndex--
			}
			actions = append(actions, rhpv2.RPCWriteAction{
				Type: rhpv2.RPCWriteActionTrim,
				A:    uint64(len(batch)),
			})

			// check funds
			proofSize := uint64(len(batch)) * 2 * uint64(bits.Len64(numSectors)) * 32
			if proofSize < minMessageSize {
				proofSize = minMessageSize
			}

			// calculate the cost
			//
			// TODO: switch out for exact cost calculations once it is added to core
			cost = settings.BaseRPCPrice.Add(settings.DownloadBandwidthPrice.Mul64(proofSize))
			cost = cost.Mul64(125).Div64(100) // leeway
			if rev.RenterFunds().Cmp(cost) < 0 {
				return ErrInsufficientFunds
			}

			// update the revision number
			if rev.Revision.RevisionNumber == math.MaxUint64 {
				return ErrContractFinalized
			}
			rev.Revision.RevisionNumber++

			// update the revision filesize
			rev.Revision.Filesize -= rhpv2.SectorSize * actions[len(actions)-1].A

			// update the revision outputs
			newValid, newMissed, err := updateRevisionOutputs(&rev.Revision, cost, types.ZeroCurrency)
			if err != nil {
				return err
			}

			// create request
			wReq := &rhpv2.RPCWriteRequest{
				Actions:     actions,
				MerkleProof: true,

				RevisionNumber:    rev.Revision.RevisionNumber,
				ValidProofValues:  newValid,
				MissedProofValues: newMissed,
			}

			// send request and read merkle proof
			var merkleResp rhpv2.RPCWriteMerkleProof
			if err := t.WriteRequest(rhpv2.RPCWriteID, wReq); err != nil {
				return err
			} else if err := t.ReadResponse(&merkleResp, minMessageSize+proofSize); err != nil {
				return fmt.Errorf("couldn't read Merkle proof response, err: %v", err)
			}

			// verify proof
			proofHashes := merkleResp.OldSubtreeHashes
			leafHashes := merkleResp.OldLeafHashes
			oldRoot, newRoot := types.Hash256(rev.Revision.FileMerkleRoot), merkleResp.NewMerkleRoot
			if rev.Revision.Filesize > 0 && !rhpv2.VerifyDiffProof(actions, numSectors, proofHashes, leafHashes, oldRoot, newRoot, nil) {
				err := ErrInvalidMerkleProof
				t.WriteResponseErr(err)
				return err
			}

			// update merkle root
			copy(rev.Revision.FileMerkleRoot[:], newRoot[:])

			// build the write response
			revisionHash := hashRevision(rev.Revision)
			renterSig := &rhpv2.RPCWriteResponse{
				Signature: renterKey.SignHash(revisionHash),
			}

			// exchange signatures
			var hostSig rhpv2.RPCWriteResponse
			if err := t.WriteResponse(renterSig); err != nil {
				return fmt.Errorf("couldn't write signature response: %w", err)
			} else if err := t.ReadResponse(&hostSig, minMessageSize); err != nil {
				return fmt.Errorf("couldn't read signature response, err: %v", err)
			}

			// verify the host signature
			if !rev.HostKey().VerifyHash(revisionHash, hostSig.Signature) {
				return errors.New("host's signature is invalid")
			}
			rev.Signatures[0].Signature = renterSig.Signature[:]
			rev.Signatures[1].Signature = hostSig.Signature[:]

			// update deleted count
			deleted += uint64(len(batch))

			// record spending
			w.contractSpendingRecorder.Record(rev.ID(), rev.Revision.RevisionNumber, rev.Revision.Filesize, api.ContractSpending{Deletions: cost})
			return nil
		}(); err != nil {
			return
		}
	}
	return
}

func (w *worker) FetchContractRoots(ctx context.Context, hostIP string, hostKey types.PublicKey, fcid types.FileContractID, lastKnownRevisionNumber uint64) (roots []types.Hash256, err error) {
	err = w.withTransportV2(ctx, hostKey, hostIP, func(t *rhpv2.Transport) error {
		return w.withRevisionV2(ctx, defaultLockTimeout, t, hostKey, fcid, lastKnownRevisionNumber, func(t *rhpv2.Transport, rev rhpv2.ContractRevision, settings rhpv2.HostSettings) (err error) {
			roots, err = w.fetchContractRoots(t, &rev, settings)
			return
		})
	})
	return
}

func (w *worker) fetchContractRoots(t *rhpv2.Transport, rev *rhpv2.ContractRevision, settings rhpv2.HostSettings) (roots []types.Hash256, _ error) {
	// derive the renter key
	renterKey := w.deriveRenterKey(rev.HostKey())

	// download the full set of SectorRoots
	numsectors := rev.NumSectors()
	for offset := uint64(0); offset < numsectors; {
		n := batchSizeFetchSectors
		if offset+n > numsectors {
			n = numsectors - offset
		}

		// check funds
		price, _ := settings.RPCSectorRootsCost(offset, n).Total()
		if rev.RenterFunds().Cmp(price) < 0 {
			return nil, ErrInsufficientFunds
		}

		// update the revision number
		if rev.Revision.RevisionNumber == math.MaxUint64 {
			return nil, ErrContractFinalized
		}
		rev.Revision.RevisionNumber++

		// update the revision outputs
		newValid, newMissed, err := updateRevisionOutputs(&rev.Revision, price, types.ZeroCurrency)
		if err != nil {
			return nil, err
		}

		// build the sector roots request
		revisionHash := hashRevision(rev.Revision)
		req := &rhpv2.RPCSectorRootsRequest{
			RootOffset: uint64(offset),
			NumRoots:   uint64(n),

			RevisionNumber:    rev.Revision.RevisionNumber,
			ValidProofValues:  newValid,
			MissedProofValues: newMissed,
			Signature:         renterKey.SignHash(revisionHash),
		}

		// calculate the proof size
		proofSize := rhpv2.RangeProofSize(rev.NumSectors(), offset, n)

		// execute the sector roots RPC
		var rootsResp rhpv2.RPCSectorRootsResponse
		if err := t.WriteRequest(rhpv2.RPCSectorRootsID, req); err != nil {
			return nil, err
		} else if err := t.ReadResponse(&rootsResp, uint64(minMessageSize+proofSize+32*n)); err != nil {
			return nil, fmt.Errorf("couldn't read sector roots response: %w", err)
		}

		// verify the host signature
		if !rev.HostKey().VerifyHash(revisionHash, rootsResp.Signature) {
			return nil, errors.New("host's signature is invalid")
		}
		rev.Signatures[0].Signature = req.Signature[:]
		rev.Signatures[1].Signature = rootsResp.Signature[:]

		// verify the proof
		if !rhpv2.VerifySectorRangeProof(rootsResp.MerkleProof, rootsResp.SectorRoots, offset, offset+n, numsectors, rev.Revision.FileMerkleRoot) {
			return nil, ErrInvalidMerkleProof
		}

		// append roots
		roots = append(roots, rootsResp.SectorRoots...)
		offset += n

		// record spending
		w.contractSpendingRecorder.Record(rev.ID(), rev.Revision.RevisionNumber, rev.Revision.Filesize, api.ContractSpending{SectorRoots: price})
	}
	return
}

func (w *worker) withRevisionV2(ctx context.Context, lockTimeout time.Duration, t *rhpv2.Transport, hk types.PublicKey, fcid types.FileContractID, lastKnownRevisionNumber uint64, fn func(t *rhpv2.Transport, rev rhpv2.ContractRevision, settings rhpv2.HostSettings) error) error {
	renterKey := w.deriveRenterKey(hk)

	// execute lock RPC
	var lockResp rhpv2.RPCLockResponse
	err := t.Call(rhpv2.RPCLockID, &rhpv2.RPCLockRequest{
		ContractID: fcid,
		Signature:  t.SignChallenge(renterKey),
		Timeout:    uint64(lockTimeout.Milliseconds()),
	}, &lockResp)
	if err != nil {
		return err
	}

	// set transport challenge
	t.SetChallenge(lockResp.NewChallenge)

	// defer unlock RPC
	defer t.WriteRequest(rhpv2.RPCUnlockID, nil)

	// convenience variables
	revision := lockResp.Revision
	sigs := lockResp.Signatures

	// sanity check the signature
	var sig types.Signature
	copy(sig[:], sigs[0].Signature)
	if !renterKey.PublicKey().VerifyHash(hashRevision(revision), sig) {
		return fmt.Errorf("unexpected renter signature on revision host revision")
	}

	// sanity check the revision number is not lower than our last known
	// revision number, host might be slipping us an outdated revision
	if revision.RevisionNumber < lastKnownRevisionNumber {
		return fmt.Errorf("unexpected revision number, %v!=%v", revision.RevisionNumber, lastKnownRevisionNumber)
	}

	// extract the revision
	rev := rhpv2.ContractRevision{
		Revision:   revision,
		Signatures: [2]types.TransactionSignature{sigs[0], sigs[1]},
	}

	// execute settings RPC
	var settingsResp rhpv2.RPCSettingsResponse
	if err := t.Call(rhpv2.RPCSettingsID, nil, &settingsResp); err != nil {
		return err
	}
	var settings rhpv2.HostSettings
	if err := json.Unmarshal(settingsResp.Settings, &settings); err != nil {
		return fmt.Errorf("couldn't unmarshal json: %w", err)
	}

	return fn(t, rev, settings)
}

func humanReadableSize(b int) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB",
		float64(b)/float64(div), "KMGTPE"[exp])
}
