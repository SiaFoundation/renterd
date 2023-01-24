package rhp

import (
	"math"
	"math/bits"

	"go.sia.tech/core/consensus"
	"go.sia.tech/core/types"
	stypes "go.sia.tech/siad/types"
)

// A TransactionSigner can sign transaction inputs.
type TransactionSigner interface {
	SignTransaction(cs consensus.State, txn *types.Transaction, toSign []types.Hash256) error
}

func hashRevision(rev types.FileContractRevision) types.Hash256 {
	h := types.NewHasher()
	rev.EncodeTo(h.E)
	return h.Sum()
}

// ContractFormationCost returns the cost of forming a contract.
func ContractFormationCost(fc types.FileContract, contractFee types.Currency) types.Currency {
	return fc.ValidRenterPayout().Add(contractFee).Add(contractTax(fc))
}

// PrepareContractFormation constructs a contract formation transaction.
func PrepareContractFormation(renterKey types.PrivateKey, hostKey types.PublicKey, renterPayout, hostCollateral types.Currency, endHeight uint64, host HostSettings, refundAddr types.Address) types.FileContract {
	renterPubkey := renterKey.PublicKey()
	uc := types.UnlockConditions{
		PublicKeys: []types.UnlockKey{
			{Algorithm: types.SpecifierEd25519, Key: renterPubkey[:]},
			{Algorithm: types.SpecifierEd25519, Key: hostKey[:]},
		},
		SignaturesRequired: 2,
	}

	hostPayout := host.ContractPrice.Add(hostCollateral)
	payout := taxAdjustedPayout(renterPayout.Add(hostPayout))

	return types.FileContract{
		Filesize:       0,
		FileMerkleRoot: types.Hash256{},
		WindowStart:    uint64(endHeight),
		WindowEnd:      uint64(endHeight + host.WindowSize),
		Payout:         payout,
		UnlockHash:     types.Hash256(uc.UnlockHash()),
		RevisionNumber: 0,
		ValidProofOutputs: []types.SiacoinOutput{
			// outputs need to account for tax
			{Value: renterPayout, Address: refundAddr},
			// collateral is returned to host
			{Value: hostPayout, Address: host.Address},
		},
		MissedProofOutputs: []types.SiacoinOutput{
			// same as above
			{Value: renterPayout, Address: refundAddr},
			// same as above
			{Value: hostPayout, Address: host.Address},
			// once we start doing revisions, we'll move some coins to the host and some to the void
			{Value: types.ZeroCurrency, Address: types.Address{}},
		},
	}
}

// ContractRenewalCost returns the cost of renewing a contract.
func ContractRenewalCost(fc types.FileContract, contractFee types.Currency) types.Currency {
	return fc.ValidRenterPayout().Add(contractFee).Add(contractTax(fc))
}

// PrepareContractRenewal constructs a contract renewal transaction.
func PrepareContractRenewal(currentRevision types.FileContractRevision, renterKey types.PrivateKey, hostKey types.PublicKey, renterPayout types.Currency, endHeight uint64, host HostSettings, refundAddr types.Address) types.FileContract {
	// calculate "base" price and collateral -- the storage cost and collateral
	// contribution for the amount of data already in contract. If the contract
	// height did not increase, basePrice and baseCollateral are zero.
	var basePrice, baseCollateral types.Currency
	if contractEnd := uint64(endHeight + host.WindowSize); contractEnd > currentRevision.WindowEnd {
		timeExtension := uint64(contractEnd - currentRevision.WindowEnd)
		basePrice = host.StoragePrice.Mul64(currentRevision.Filesize).Mul64(timeExtension)
		baseCollateral = host.Collateral.Mul64(currentRevision.Filesize).Mul64(timeExtension)
	}

	// estimate collateral for new contract
	var newCollateral types.Currency
	if costPerByte := host.UploadBandwidthPrice.Add(host.StoragePrice).Add(host.DownloadBandwidthPrice); !costPerByte.IsZero() {
		bytes := renterPayout.Div(costPerByte)
		newCollateral = host.Collateral.Mul(bytes)
	}

	// the collateral can't be greater than MaxCollateral
	totalCollateral := baseCollateral.Add(newCollateral)
	if totalCollateral.Cmp(host.MaxCollateral) > 0 {
		totalCollateral = host.MaxCollateral
	}

	// Calculate payouts: the host gets their contract fee, plus the cost of the
	// data already in the contract, plus their collateral. In the event of a
	// missed payout, the cost and collateral of the data already in the
	// contract is subtracted from the host, and sent to the void instead.
	//
	// However, it is possible for this subtraction to underflow; this can
	// happen if baseCollateral is large and MaxCollateral is small. We cannot
	// simply replace the underflow with a zero, because the host performs the
	// same subtraction and returns an error on underflow. Nor can we increase
	// the valid payout, because the host calculates its collateral contribution
	// by subtracting the contract price and base price from this payout, and
	// we're already at MaxCollateral. Thus the host has conflicting
	// requirements, and renewing the contract is impossible until they change
	// their settings.
	hostValidPayout := host.ContractPrice.Add(basePrice).Add(totalCollateral)
	voidMissedPayout := basePrice.Add(baseCollateral)
	if hostValidPayout.Cmp(voidMissedPayout) < 0 {
		// TODO: detect this elsewhere
		panic("host's settings are unsatisfiable")
	}
	hostMissedPayout := hostValidPayout.Sub(voidMissedPayout)

	return types.FileContract{
		Filesize:       currentRevision.Filesize,
		FileMerkleRoot: currentRevision.FileMerkleRoot,
		WindowStart:    uint64(endHeight),
		WindowEnd:      uint64(endHeight + host.WindowSize),
		Payout:         taxAdjustedPayout(renterPayout.Add(hostValidPayout)),
		UnlockHash:     currentRevision.UnlockHash,
		RevisionNumber: 0,
		ValidProofOutputs: []types.SiacoinOutput{
			{Value: renterPayout, Address: refundAddr},
			{Value: hostValidPayout, Address: host.Address},
		},
		MissedProofOutputs: []types.SiacoinOutput{
			{Value: renterPayout, Address: refundAddr},
			{Value: hostMissedPayout, Address: host.Address},
			{Value: voidMissedPayout, Address: types.Address{}},
		},
	}
}

// RPCFormContract forms a contract with a host.
func RPCFormContract(t *Transport, renterKey types.PrivateKey, txnSet []types.Transaction) (_ ContractRevision, _ []types.Transaction, err error) {
	defer wrapErr(&err, "FormContract")

	hostKey := t.HostKey()

	// strip our signatures before sending
	parents, txn := txnSet[:len(txnSet)-1], txnSet[len(txnSet)-1]
	renterContractSignatures := txn.Signatures
	txnSet[len(txnSet)-1].Signatures = nil

	renterPubkey := renterKey.PublicKey()
	req := &RPCFormContractRequest{
		Transactions: txnSet,
		RenterKey: types.UnlockKey{
			Algorithm: types.SpecifierEd25519,
			Key:       renterPubkey[:],
		},
	}
	if err := t.WriteRequest(RPCFormContractID, req); err != nil {
		return ContractRevision{}, nil, err
	}

	var resp RPCFormContractAdditions
	if err := t.ReadResponse(&resp, 65536); err != nil {
		return ContractRevision{}, nil, err
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
				{Algorithm: types.SpecifierEd25519, Key: renterPubkey[:]},
				{Algorithm: types.SpecifierEd25519, Key: hostKey[:]},
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
	renterSigs := &RPCFormContractSignatures{
		ContractSignatures: renterContractSignatures,
		RevisionSignature:  renterRevisionSig,
	}
	if err := t.WriteResponse(renterSigs); err != nil {
		return ContractRevision{}, nil, err
	}

	// read the host's signatures and merge them with our own
	var hostSigs RPCFormContractSignatures
	if err := t.ReadResponse(&hostSigs, 4096); err != nil {
		return ContractRevision{}, nil, err
	}
	txn.Signatures = append(renterContractSignatures, hostSigs.ContractSignatures...)
	signedTxnSet := append(resp.Parents, append(parents, txn)...)

	return ContractRevision{
		Revision: initRevision,
		Signatures: [2]types.TransactionSignature{
			renterRevisionSig,
			hostSigs.RevisionSignature,
		},
	}, signedTxnSet, nil
}

// RenewContract negotiates a new file contract and initial revision for data
// already stored with a host. The old contract is "cleared," reverting its
// filesize to zero.
func (s *Session) RenewContract(txnSet []types.Transaction, finalPayment types.Currency) (_ ContractRevision, _ []types.Transaction, err error) {
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

	req := &RPCRenewAndClearContractRequest{
		Transactions:           txnSet,
		RenterKey:              s.revision.Revision.UnlockConditions.PublicKeys[0],
		FinalValidProofValues:  newValid,
		FinalMissedProofValues: newValid,
	}
	if err := s.transport.WriteRequest(RPCRenewClearContractID, req); err != nil {
		return ContractRevision{}, nil, err
	}

	var resp RPCFormContractAdditions
	if err := s.transport.ReadResponse(&resp, 65536); err != nil {
		return ContractRevision{}, nil, err
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

	// send signatures
	finalRevSig := s.key.SignHash(hashRevision(finalOldRevision))
	renterSigs := &RPCRenewAndClearContractSignatures{
		ContractSignatures:     renterContractSignatures,
		RevisionSignature:      renterRevisionSig,
		FinalRevisionSignature: finalRevSig,
	}
	if err := s.transport.WriteResponse(renterSigs); err != nil {
		return ContractRevision{}, nil, err
	}

	// read the host signatures and merge them with our own
	var hostSigs RPCRenewAndClearContractSignatures
	if err := s.transport.ReadResponse(&hostSigs, 4096); err != nil {
		return ContractRevision{}, nil, err
	}
	txn.Signatures = append(renterContractSignatures, hostSigs.ContractSignatures...)
	signedTxnSet := append(resp.Parents, append(parents, txn)...)

	return ContractRevision{
		Revision:   initRevision,
		Signatures: [2]types.TransactionSignature{renterRevisionSig, hostSigs.RevisionSignature},
	}, signedTxnSet, nil
}

func contractTax(fc types.FileContract) types.Currency {
	// NOTE: siad uses different hardfork heights when -tags=testing is set,
	// so we have to alter cs accordingly.
	// TODO: remove this
	cs := consensus.State{Index: types.ChainIndex{Height: fc.WindowStart}}
	switch {
	case cs.Index.Height >= uint64(stypes.TaxHardforkHeight):
		cs.Index.Height = 21000
	}
	return cs.FileContractTax(fc)
}

// NOTE: due to a bug in the transaction validation code, calculating payouts
// is way harder than it needs to be. Tax is calculated on the post-tax
// contract payout (instead of the sum of the renter and host payouts). So the
// equation for the payout is:
//
//	   payout = renterPayout + hostPayout + payout*tax
//	∴  payout = (renterPayout + hostPayout) / (1 - tax)
//
// This would work if 'tax' were a simple fraction, but because the tax must
// be evenly distributed among siafund holders, 'tax' is actually a function
// that multiplies by a fraction and then rounds down to the nearest multiple
// of the siafund count. Thus, when inverting the function, we have to make an
// initial guess and then fix the rounding error.
func taxAdjustedPayout(target types.Currency) types.Currency {
	// compute initial guess as target * (1 / 1-tax); since this does not take
	// the siafund rounding into account, the guess will be up to
	// types.SiafundCount greater than the actual payout value.
	guess := target.Mul64(1000).Div64(961)

	// now, adjust the guess to remove the rounding error. We know that:
	//
	//   (target % types.SiafundCount) == (payout % types.SiafundCount)
	//
	// therefore, we can simply adjust the guess to have this remainder as
	// well. The only wrinkle is that, since we know guess >= payout, if the
	// guess remainder is smaller than the target remainder, we must subtract
	// an extra types.SiafundCount.
	//
	// for example, if target = 87654321 and types.SiafundCount = 10000, then:
	//
	//   initial_guess  = 87654321 * (1 / (1 - tax))
	//                  = 91211572
	//   target % 10000 =     4321
	//   adjusted_guess = 91204321

	mod64 := func(c types.Currency, v uint64) types.Currency {
		var r uint64
		if c.Hi < v {
			_, r = bits.Div64(c.Hi, c.Lo, v)
		} else {
			_, r = bits.Div64(0, c.Hi, v)
			_, r = bits.Div64(r, c.Lo, v)
		}
		return types.NewCurrency64(r)
	}
	sfc := (consensus.State{}).SiafundCount()
	tm := mod64(target, sfc)
	gm := mod64(guess, sfc)
	if gm.Cmp(tm) < 0 {
		guess = guess.Sub(types.NewCurrency64(sfc))
	}
	return guess.Add(tm).Sub(gm)
}
