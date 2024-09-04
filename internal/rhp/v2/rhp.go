package rhp

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net"
	"sort"
	"time"

	rhpv2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/internal/gouging"
	"go.sia.tech/renterd/internal/utils"
	"go.uber.org/zap"
	"lukechampine.com/frand"
)

const (
	batchSizeDeleteSectors = uint64(1000)  // 4GiB of contract data
	batchSizeFetchSectors  = uint64(25600) // 100GiB of contract data

	// default lock timeout
	defaultLockTimeout = time.Minute

	// minMessageSize is the minimum size of an RPC message
	minMessageSize = 4096

	// maxMerkleProofResponseSize caps the response message size to a generous
	// value of 100 MB worth of roots. This is approximately double the size of
	// what we have observed on the live network for 5TB+ contracts to be safe.
	maxMerkleProofResponseSize = 100 * 1 << 20 // 100 MB
)

var (
	// ErrInsufficientCollateral is returned by various RPCs when the host is
	// unable to provide sufficient collateral.
	ErrInsufficientCollateral = errors.New("insufficient collateral")

	// ErrInsufficientFunds is returned by various RPCs when the renter is
	// unable to provide sufficient payment to the host.
	ErrInsufficientFunds = errors.New("insufficient funds")

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

	// ErrNoSectorsToPrune is returned when we try to prune a contract that has
	// no sectors to prune.
	ErrNoSectorsToPrune = errors.New("no sectors to prune")
)

type (
	PrunableRootsFn = func(fcid types.FileContractID, roots []types.Hash256) (indices []uint64, err error)
)

type (
	Dialer interface {
		Dial(ctx context.Context, hk types.PublicKey, address string) (net.Conn, error)
	}
)

type Client struct {
	dialer Dialer
	logger *zap.SugaredLogger
}

func New(dialer Dialer, logger *zap.Logger) *Client {
	return &Client{
		dialer: dialer,
		logger: logger.Sugar().Named("rhp2"),
	}
}

func (w *Client) ContractRoots(ctx context.Context, renterKey types.PrivateKey, gougingChecker gouging.Checker, hostIP string, hostKey types.PublicKey, fcid types.FileContractID, lastKnownRevisionNumber uint64) (roots []types.Hash256, revision *types.FileContractRevision, cost types.Currency, err error) {
	err = w.withTransport(ctx, hostKey, hostIP, func(t *rhpv2.Transport) error {
		return w.withRevisionV2(renterKey, gougingChecker, t, fcid, lastKnownRevisionNumber, func(t *rhpv2.Transport, rev rhpv2.ContractRevision, settings rhpv2.HostSettings) (err error) {
			roots, cost, err = w.fetchContractRoots(t, renterKey, &rev, settings)
			revision = &rev.Revision
			return
		})
	})
	return
}

// SignedRevision fetches the latest signed revision for a contract from a host.
func (w *Client) SignedRevision(ctx context.Context, hostIP string, hostKey types.PublicKey, renterKey types.PrivateKey, contractID types.FileContractID, timeout time.Duration) (rhpv2.ContractRevision, error) {
	var rev rhpv2.ContractRevision
	err := w.withTransport(ctx, hostKey, hostIP, func(t *rhpv2.Transport) error {
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

func (c *Client) Settings(ctx context.Context, hostKey types.PublicKey, hostIP string) (settings rhpv2.HostSettings, err error) {
	err = c.withTransport(ctx, hostKey, hostIP, func(t *rhpv2.Transport) error {
		var err error
		if settings, err = rpcSettings(ctx, t); err != nil {
			return err
		}
		// NOTE: we overwrite the NetAddress with the host address here
		// since we just used it to dial the host we know it's valid
		settings.NetAddress = hostIP
		return nil
	})
	return
}

func (c *Client) FormContract(ctx context.Context, hostKey types.PublicKey, hostIP string, renterKey types.PrivateKey, txnSet []types.Transaction) (contract rhpv2.ContractRevision, fullTxnSet []types.Transaction, err error) {
	err = c.withTransport(ctx, hostKey, hostIP, func(t *rhpv2.Transport) (err error) {
		contract, fullTxnSet, err = rpcFormContract(ctx, t, renterKey, txnSet)
		return
	})
	return
}

func (c *Client) PruneContract(ctx context.Context, renterKey types.PrivateKey, gougingChecker gouging.Checker, hostIP string, hostKey types.PublicKey, fcid types.FileContractID, lastKnownRevisionNumber uint64, diffRootsFn PrunableRootsFn) (revision *types.FileContractRevision, spending api.ContractSpending, deleted, remaining uint64, err error) {
	log := c.logger.Named("performContractPruning")
	err = c.withTransport(ctx, hostKey, hostIP, func(t *rhpv2.Transport) error {
		return c.withRevisionV2(renterKey, gougingChecker, t, fcid, lastKnownRevisionNumber, func(t *rhpv2.Transport, rev rhpv2.ContractRevision, settings rhpv2.HostSettings) (err error) {
			// reference the revision
			revision = &rev.Revision

			// fetch roots to delete
			var indices []uint64
			indices, spending.SectorRoots, err = c.prunableContractRoots(t, renterKey, &rev, settings, func(fcid types.FileContractID, roots []types.Hash256) (indices []uint64, err error) {
				startt := time.Now()
				defer func() {
					log.Debugf("batch diff roots took %v", time.Since(startt))
				}()
				return diffRootsFn(fcid, roots)
			})
			if err != nil {
				return err
			} else if len(indices) == 0 {
				return ErrNoSectorsToPrune
			}

			// delete the roots from the contract
			deleted, spending.Deletions, err = c.deleteContractRoots(t, renterKey, &rev, settings, indices)
			if deleted < uint64(len(indices)) {
				remaining = uint64(len(indices)) - deleted
			}

			// return sizes instead of number of roots
			deleted *= rhpv2.SectorSize
			remaining *= rhpv2.SectorSize
			return
		})
	})
	return
}

func (c *Client) deleteContractRoots(t *rhpv2.Transport, renterKey types.PrivateKey, rev *rhpv2.ContractRevision, settings rhpv2.HostSettings, indices []uint64) (deleted uint64, cost types.Currency, err error) {
	id := frand.Entropy128()
	logger := c.logger.
		With("id", hex.EncodeToString(id[:])).
		With("hostKey", rev.HostKey()).
		With("hostVersion", settings.Version).
		With("fcid", rev.ID()).
		With("revisionNumber", rev.Revision.RevisionNumber).
		Named("deleteContractRoots")
	logger.Infow(fmt.Sprintf("deleting %d contract roots (%v)", len(indices), utils.HumanReadableSize(len(indices)*rhpv2.SectorSize)), "hk", rev.HostKey(), "fcid", rev.ID())

	// return early
	if len(indices) == 0 {
		return 0, types.ZeroCurrency, nil
	}

	// sort in descending order so that we can use 'range'
	sort.Slice(indices, func(i, j int) bool {
		return indices[i] > indices[j]
	})

	// decide on the batch size, defaults to ~20mib of sector data but for old
	// hosts we use a much smaller batch size to ensure we nibble away at the
	// problem rather than outright failing or timing out
	batchSize := int(batchSizeDeleteSectors)
	if utils.VersionCmp(settings.Version, "1.6.0") < 0 {
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

	// range over the batches and delete the sectors batch per batch
	for i, batch := range batches {
		if err = func() error {
			var batchCost types.Currency
			start := time.Now()
			logger.Infow(fmt.Sprintf("starting batch %d/%d of size %d", i+1, len(batches), len(batch)))
			defer func() {
				logger.Infow(fmt.Sprintf("processing batch %d/%d of size %d took %v", i+1, len(batches), len(batch), time.Since(start)), "cost", batchCost)
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

			// calculate the cost
			var remainingDuration uint64 // not needed for deletions
			rpcCost, err := settings.RPCWriteCost(actions, numSectors, remainingDuration, true)
			if err != nil {
				return err
			}
			batchCost, _ = rpcCost.Total()

			// NOTE: we currently overpay hosts by quite a large margin (~10x)
			// to ensure we cover both 1.5.9 and pre v0.2.1 hosts.
			//
			// TODO: remove once host network is updated, or once we include the
			// host release in the scoring and stop using old hosts
			proofSize := (128 + uint64(len(actions))) * rhpv2.LeafSize
			compatCost := settings.BaseRPCPrice.Add(settings.DownloadBandwidthPrice.Mul64(proofSize))
			if batchCost.Cmp(compatCost) < 0 {
				batchCost = compatCost
			}

			if rev.RenterFunds().Cmp(batchCost) < 0 {
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
			newRevision, err := updatedRevision(rev.Revision, batchCost, types.ZeroCurrency)
			if err != nil {
				return err
			}

			// create request
			wReq := &rhpv2.RPCWriteRequest{
				Actions:     actions,
				MerkleProof: true,

				RevisionNumber: rev.Revision.RevisionNumber,
				ValidProofValues: []types.Currency{
					newRevision.ValidProofOutputs[0].Value,
					newRevision.ValidProofOutputs[1].Value,
				},
				MissedProofValues: []types.Currency{
					newRevision.MissedProofOutputs[0].Value,
					newRevision.MissedProofOutputs[1].Value,
					newRevision.MissedProofOutputs[2].Value,
				},
			}

			// send request and read merkle proof
			var merkleResp rhpv2.RPCWriteMerkleProof
			if err := t.WriteRequest(rhpv2.RPCWriteID, wReq); err != nil {
				return err
			} else if err := t.ReadResponse(&merkleResp, maxMerkleProofResponseSize); err != nil {
				err := fmt.Errorf("couldn't read Merkle proof response, err: %v", err)
				logger.Infow(fmt.Sprintf("processing batch %d/%d failed, err %v", i+1, len(batches), err))
				return err
			}

			// verify proof
			proofHashes := merkleResp.OldSubtreeHashes
			leafHashes := merkleResp.OldLeafHashes
			oldRoot, newRoot := types.Hash256(newRevision.FileMerkleRoot), merkleResp.NewMerkleRoot
			if newRevision.Filesize > 0 && !rhpv2.VerifyDiffProof(actions, numSectors, proofHashes, leafHashes, oldRoot, newRoot, nil) {
				err := fmt.Errorf("couldn't verify delete proof, host %v, version %v; %w", rev.HostKey(), settings.Version, ErrInvalidMerkleProof)
				logger.Infow(fmt.Sprintf("processing batch %d/%d failed, err %v", i+1, len(batches), err))
				t.WriteResponseErr(err)
				return err
			}

			// update merkle root
			copy(newRevision.FileMerkleRoot[:], newRoot[:])

			// build the write response
			revisionHash := hashRevision(newRevision)
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

			// update revision
			rev.Revision = newRevision
			cost = cost.Add(batchCost)
			return nil
		}(); err != nil {
			return
		}
	}
	return
}

func (c *Client) prunableContractRoots(t *rhpv2.Transport, renterKey types.PrivateKey, rev *rhpv2.ContractRevision, settings rhpv2.HostSettings, prunableRootsFn PrunableRootsFn) (indices []uint64, cost types.Currency, _ error) {
	numsectors := rev.NumSectors()
	for offset := uint64(0); offset < numsectors; {
		// calculate the batch size
		n := batchSizeFetchSectors
		if offset+n > numsectors {
			n = numsectors - offset
		}

		// fetch the batch
		batch, batchCost, err := c.fetchContractRootsBatch(t, renterKey, rev, settings, offset, n)
		if err != nil {
			return nil, types.ZeroCurrency, err
		}

		// fetch prunable roots for this batch
		prunable, err := prunableRootsFn(rev.ID(), batch)
		if err != nil {
			return nil, types.ZeroCurrency, err
		}

		// append the roots, make sure to take the offset into account
		for _, index := range prunable {
			indices = append(indices, index+offset)
		}
		offset += n

		// update the cost
		cost = cost.Add(batchCost)
	}
	return
}

func (c *Client) fetchContractRoots(t *rhpv2.Transport, renterKey types.PrivateKey, rev *rhpv2.ContractRevision, settings rhpv2.HostSettings) (roots []types.Hash256, cost types.Currency, _ error) {
	numsectors := rev.NumSectors()
	for offset := uint64(0); offset < numsectors; {
		// calculate the batch size
		n := batchSizeFetchSectors
		if offset+n > numsectors {
			n = numsectors - offset
		}

		// fetch the batch
		batch, batchCost, err := c.fetchContractRootsBatch(t, renterKey, rev, settings, offset, n)
		if err != nil {
			return nil, types.ZeroCurrency, err
		}

		// append the roots
		roots = append(roots, batch...)
		offset += n

		// update the cost
		cost = cost.Add(batchCost)
	}
	return
}

func (c *Client) fetchContractRootsBatch(t *rhpv2.Transport, renterKey types.PrivateKey, rev *rhpv2.ContractRevision, settings rhpv2.HostSettings, offset, limit uint64) ([]types.Hash256, types.Currency, error) {
	// calculate the cost
	cost, _ := settings.RPCSectorRootsCost(offset, limit).Total()

	// TODO: remove once host network is updated
	if utils.VersionCmp(settings.Version, "1.6.0") < 0 {
		// calculate the response size
		proofSize := rhpv2.RangeProofSize(rev.NumSectors(), offset, offset+limit)
		responseSize := (proofSize + limit) * 32
		if responseSize < minMessageSize {
			responseSize = minMessageSize
		}
		cost = settings.BaseRPCPrice.Add(settings.DownloadBandwidthPrice.Mul64(responseSize))
		cost = cost.Mul64(2) // generous leeway
	}

	// check funds
	if rev.RenterFunds().Cmp(cost) < 0 {
		return nil, types.ZeroCurrency, ErrInsufficientFunds
	}

	// update the revision number
	if rev.Revision.RevisionNumber == math.MaxUint64 {
		return nil, types.ZeroCurrency, ErrContractFinalized
	}
	rev.Revision.RevisionNumber++

	// update the revision outputs
	newRevision, err := updatedRevision(rev.Revision, cost, types.ZeroCurrency)
	if err != nil {
		return nil, types.ZeroCurrency, err
	}

	// build the sector roots request
	revisionHash := hashRevision(newRevision)
	req := &rhpv2.RPCSectorRootsRequest{
		RootOffset: offset,
		NumRoots:   limit,

		RevisionNumber: rev.Revision.RevisionNumber,
		ValidProofValues: []types.Currency{
			newRevision.ValidProofOutputs[0].Value,
			newRevision.ValidProofOutputs[1].Value,
		},
		MissedProofValues: []types.Currency{
			newRevision.MissedProofOutputs[0].Value,
			newRevision.MissedProofOutputs[1].Value,
			newRevision.MissedProofOutputs[2].Value,
		},
		Signature: renterKey.SignHash(revisionHash),
	}

	// execute the sector roots RPC
	var rootsResp rhpv2.RPCSectorRootsResponse
	if err := t.WriteRequest(rhpv2.RPCSectorRootsID, req); err != nil {
		return nil, types.ZeroCurrency, err
	} else if err := t.ReadResponse(&rootsResp, maxMerkleProofResponseSize); err != nil {
		return nil, types.ZeroCurrency, fmt.Errorf("couldn't read sector roots response: %w", err)
	}

	// verify the host signature
	if !rev.HostKey().VerifyHash(revisionHash, rootsResp.Signature) {
		return nil, cost, errors.New("host's signature is invalid")
	}
	rev.Signatures[0].Signature = req.Signature[:]
	rev.Signatures[1].Signature = rootsResp.Signature[:]

	// verify the proof
	if uint64(len(rootsResp.SectorRoots)) != limit {
		return nil, cost, fmt.Errorf("couldn't verify contract roots proof, host %v, version %v, err: number of roots does not match range %d != %d (num sectors: %d rev size: %d offset: %d)", rev.HostKey(), settings.Version, len(rootsResp.SectorRoots), limit, rev.NumSectors(), rev.Revision.Filesize, offset)
	} else if !rhpv2.VerifySectorRangeProof(rootsResp.MerkleProof, rootsResp.SectorRoots, offset, offset+limit, rev.NumSectors(), rev.Revision.FileMerkleRoot) {
		return nil, cost, fmt.Errorf("couldn't verify contract roots proof, host %v, version %v; %w", rev.HostKey(), settings.Version, ErrInvalidMerkleProof)
	}

	// update revision
	rev.Revision = newRevision

	return rootsResp.SectorRoots, cost, nil
}

func (w *Client) withRevisionV2(renterKey types.PrivateKey, gougingChecker gouging.Checker, t *rhpv2.Transport, fcid types.FileContractID, lastKnownRevisionNumber uint64, fn func(t *rhpv2.Transport, rev rhpv2.ContractRevision, settings rhpv2.HostSettings) error) error {
	// execute lock RPC
	var lockResp rhpv2.RPCLockResponse
	err := t.Call(rhpv2.RPCLockID, &rhpv2.RPCLockRequest{
		ContractID: fcid,
		Signature:  t.SignChallenge(renterKey),
		Timeout:    uint64(defaultLockTimeout.Milliseconds()),
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

	// perform gouging checks on settings
	if breakdown := gougingChecker.CheckSettings(settings); breakdown.Gouging() {
		return fmt.Errorf("%w: %v", gouging.ErrHostSettingsGouging, breakdown)
	}

	return fn(t, rev, settings)
}

func (c *Client) withTransport(ctx context.Context, hostKey types.PublicKey, hostIP string, fn func(*rhpv2.Transport) error) (err error) {
	conn, err := c.dialer.Dial(ctx, hostKey, hostIP)
	if err != nil {
		return err
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
		if context.Cause(ctx) != nil {
			err = context.Cause(ctx)
		}
	}()
	t, err := rhpv2.NewRenterTransport(conn, hostKey)
	if err != nil {
		return err
	}
	defer t.Close()

	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic (withTransportV2): %v", r)
		}
	}()
	return fn(t)
}

func hashRevision(rev types.FileContractRevision) types.Hash256 {
	h := types.NewHasher()
	rev.EncodeTo(h.E)
	return h.Sum()
}

func updatedRevision(rev types.FileContractRevision, cost, collateral types.Currency) (types.FileContractRevision, error) {
	// allocate new slices; don't want to risk accidentally sharing memory
	rev.ValidProofOutputs = append([]types.SiacoinOutput(nil), rev.ValidProofOutputs...)
	rev.MissedProofOutputs = append([]types.SiacoinOutput(nil), rev.MissedProofOutputs...)

	// move valid payout from renter to host
	var underflow, overflow bool
	rev.ValidProofOutputs[0].Value, underflow = rev.ValidProofOutputs[0].Value.SubWithUnderflow(cost)
	rev.ValidProofOutputs[1].Value, overflow = rev.ValidProofOutputs[1].Value.AddWithOverflow(cost)
	if underflow || overflow {
		return types.FileContractRevision{}, errors.New("insufficient funds to pay host")
	}

	// move missed payout from renter to void
	rev.MissedProofOutputs[0].Value, underflow = rev.MissedProofOutputs[0].Value.SubWithUnderflow(cost)
	rev.MissedProofOutputs[2].Value, overflow = rev.MissedProofOutputs[2].Value.AddWithOverflow(cost)
	if underflow || overflow {
		return types.FileContractRevision{}, errors.New("insufficient funds to move missed payout to void")
	}

	// move collateral from host to void
	rev.MissedProofOutputs[1].Value, underflow = rev.MissedProofOutputs[1].Value.SubWithUnderflow(collateral)
	rev.MissedProofOutputs[2].Value, overflow = rev.MissedProofOutputs[2].Value.AddWithOverflow(collateral)
	if underflow || overflow {
		return types.FileContractRevision{}, errors.New("insufficient collateral")
	}
	return rev, nil
}
