package rhp

import (
	"bytes"
	"errors"
	"math"
	"math/big"

	"go.sia.tech/siad/crypto"
	"go.sia.tech/siad/types"
	"golang.org/x/crypto/blake2b"
)

// A TransactionSigner can sign transaction inputs.
type TransactionSigner interface {
	SignTransaction(cs ConsensusState, txn *types.Transaction, toSign []types.OutputID) error
}

// A SingleKeySigner signs transaction using a single key.
type SingleKeySigner PrivateKey

// SignTransaction implements TransactionSigner.
func (s SingleKeySigner) SignTransaction(cs ConsensusState, txn *types.Transaction, toSign []types.OutputID) error {
outer:
	for _, id := range toSign {
		for i := range txn.TransactionSignatures {
			ts := &txn.TransactionSignatures[i]
			if ts.ParentID == crypto.Hash(id) {
				sig := PrivateKey(s).SignHash(cs.InputSigHash(*txn, i))
				ts.Signature = sig[:]
				continue outer
			}
		}
		return errors.New("no signature with specified ID")
	}
	return nil
}

func hashRevision(rev types.FileContractRevision) Hash256 {
	or := (*objFileContractRevision)(&rev)
	var b objBuffer
	b.buf = *bytes.NewBuffer(make([]byte, 0, 1024))
	b.grow(or.marshalledSize()) // just in case 1024 is too small
	or.marshalBuffer(&b)
	return blake2b.Sum256(b.bytes())
}

// ContractFormationCost returns the cost of forming a contract.
func ContractFormationCost(fc types.FileContract, contractFee types.Currency) types.Currency {
	return fc.ValidRenterPayout().Add(contractFee).Add(types.Tax(fc.WindowStart, fc.Payout))
}

// PrepareContractFormation constructs a contract formation transaction.
func PrepareContractFormation(renterKey PrivateKey, hostKey PublicKey, renterPayout, hostCollateral types.Currency, endHeight uint64, host HostSettings, refundAddr types.UnlockHash) types.FileContract {
	renterPubkey := renterKey.PublicKey()
	uc := types.UnlockConditions{
		PublicKeys: []types.SiaPublicKey{
			{Algorithm: types.SignatureEd25519, Key: renterPubkey[:]},
			{Algorithm: types.SignatureEd25519, Key: hostKey[:]},
		},
		SignaturesRequired: 2,
	}

	hostPayout := host.ContractPrice.Add(hostCollateral)
	payout := taxAdjustedPayout(renterPayout.Add(hostPayout))

	return types.FileContract{
		FileSize:       0,
		FileMerkleRoot: crypto.Hash{},
		WindowStart:    types.BlockHeight(endHeight),
		WindowEnd:      types.BlockHeight(endHeight + host.WindowSize),
		Payout:         payout,
		UnlockHash:     uc.UnlockHash(),
		RevisionNumber: 0,
		ValidProofOutputs: []types.SiacoinOutput{
			// outputs need to account for tax
			{Value: renterPayout, UnlockHash: refundAddr},
			// collateral is returned to host
			{Value: hostPayout, UnlockHash: host.UnlockHash},
		},
		MissedProofOutputs: []types.SiacoinOutput{
			// same as above
			{Value: renterPayout, UnlockHash: refundAddr},
			// same as above
			{Value: hostPayout, UnlockHash: host.UnlockHash},
			// once we start doing revisions, we'll move some coins to the host and some to the void
			{Value: types.ZeroCurrency, UnlockHash: types.UnlockHash{}},
		},
	}
}

// ContractRenewalCost returns the cost of renewing a contract.
func ContractRenewalCost(fc types.FileContract, contractFee types.Currency) types.Currency {
	return fc.ValidRenterPayout().Add(contractFee).Add(types.Tax(fc.WindowStart, fc.Payout))
}

// PrepareContractRenewal constructs a contract renewal transaction.
func PrepareContractRenewal(currentRevision types.FileContractRevision, renterKey PrivateKey, hostKey PublicKey, renterPayout, hostCollateral types.Currency, endHeight uint64, host HostSettings, refundAddr types.UnlockHash) types.FileContract {
	// calculate "base" price and collateral -- the storage cost and collateral
	// contribution for the amount of data already in contract. If the contract
	// height did not increase, basePrice and baseCollateral are zero.
	var basePrice, baseCollateral types.Currency
	if contractEnd := types.BlockHeight(endHeight + host.WindowSize); contractEnd > currentRevision.NewWindowEnd {
		timeExtension := uint64(contractEnd - currentRevision.NewWindowEnd)
		basePrice = host.StoragePrice.Mul64(currentRevision.NewFileSize).Mul64(timeExtension)
		baseCollateral = host.Collateral.Mul64(currentRevision.NewFileSize).Mul64(timeExtension)
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
		FileSize:       currentRevision.NewFileSize,
		FileMerkleRoot: currentRevision.NewFileMerkleRoot,
		WindowStart:    types.BlockHeight(endHeight),
		WindowEnd:      types.BlockHeight(endHeight + host.WindowSize),
		Payout:         taxAdjustedPayout(renterPayout.Add(hostValidPayout)),
		UnlockHash:     currentRevision.NewUnlockHash,
		RevisionNumber: 0,
		ValidProofOutputs: []types.SiacoinOutput{
			{Value: renterPayout, UnlockHash: refundAddr},
			{Value: hostValidPayout, UnlockHash: host.UnlockHash},
		},
		MissedProofOutputs: []types.SiacoinOutput{
			{Value: renterPayout, UnlockHash: refundAddr},
			{Value: hostMissedPayout, UnlockHash: host.UnlockHash},
			{Value: voidMissedPayout, UnlockHash: types.UnlockHash{}},
		},
	}
}

// RPCFormContract forms a contract with a host.
func RPCFormContract(t *Transport, cs ConsensusState, renterKey PrivateKey, hostKey PublicKey, txnSet []types.Transaction) (_ Contract, _ []types.Transaction, err error) {
	defer wrapErr(&err, "FormContract")

	// strip our signatures before sending
	parents, txn := txnSet[:len(txnSet)-1], txnSet[len(txnSet)-1]
	renterContractSignatures := txn.TransactionSignatures
	txnSet[len(txnSet)-1].TransactionSignatures = nil

	renterPubkey := renterKey.PublicKey()
	req := &RPCFormContractRequest{
		Transactions: txnSet,
		RenterKey: types.SiaPublicKey{
			Algorithm: types.SignatureEd25519,
			Key:       renterPubkey[:],
		},
	}
	if err := t.WriteRequest(RPCFormContractID, req); err != nil {
		return Contract{}, nil, err
	}

	var resp RPCFormContractAdditions
	if err := t.ReadResponse(&resp, 65536); err != nil {
		return Contract{}, nil, err
	}

	// merge host additions with txn
	txn.SiacoinInputs = append(txn.SiacoinInputs, resp.Inputs...)
	txn.SiacoinOutputs = append(txn.SiacoinOutputs, resp.Outputs...)

	// create initial (no-op) revision, transaction, and signature
	fc := txn.FileContracts[0]
	initRevision := types.FileContractRevision{
		ParentID: txn.FileContractID(0),
		UnlockConditions: types.UnlockConditions{
			PublicKeys: []types.SiaPublicKey{
				{Algorithm: types.SignatureEd25519, Key: renterPubkey[:]},
				{Algorithm: types.SignatureEd25519, Key: hostKey[:]},
			},
			SignaturesRequired: 2,
		},
		NewRevisionNumber: 1,

		NewFileSize:           fc.FileSize,
		NewFileMerkleRoot:     fc.FileMerkleRoot,
		NewWindowStart:        fc.WindowStart,
		NewWindowEnd:          fc.WindowEnd,
		NewValidProofOutputs:  fc.ValidProofOutputs,
		NewMissedProofOutputs: fc.MissedProofOutputs,
		NewUnlockHash:         fc.UnlockHash,
	}
	revSig := renterKey.SignHash(hashRevision(initRevision))
	renterRevisionSig := types.TransactionSignature{
		ParentID:       crypto.Hash(initRevision.ParentID),
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
		return Contract{}, nil, err
	}

	// read the host's signatures and merge them with our own
	var hostSigs RPCFormContractSignatures
	if err := t.ReadResponse(&hostSigs, 4096); err != nil {
		return Contract{}, nil, err
	}
	txn.TransactionSignatures = append(renterContractSignatures, hostSigs.ContractSignatures...)
	signedTxnSet := append(resp.Parents, append(parents, txn)...)

	return Contract{
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
func (s *Session) RenewContract(cs ConsensusState, txnSet []types.Transaction, finalPayment types.Currency) (_ Contract, _ []types.Transaction, err error) {
	defer wrapErr(&err, "RenewContract")

	// strip our signatures before sending
	parents, txn := txnSet[:len(txnSet)-1], txnSet[len(txnSet)-1]
	renterContractSignatures := txn.TransactionSignatures
	txnSet[len(txnSet)-1].TransactionSignatures = nil

	// construct the final revision of the old contract
	finalOldRevision := s.contract.Revision
	newValid, _ := updateRevisionOutputs(&finalOldRevision, finalPayment, types.ZeroCurrency)
	finalOldRevision.NewMissedProofOutputs = finalOldRevision.NewValidProofOutputs
	finalOldRevision.NewFileSize = 0
	finalOldRevision.NewFileMerkleRoot = crypto.Hash{}
	finalOldRevision.NewRevisionNumber = math.MaxUint64

	req := &RPCRenewAndClearContractRequest{
		Transactions:           txnSet,
		RenterKey:              s.contract.Revision.UnlockConditions.PublicKeys[0],
		FinalValidProofValues:  newValid,
		FinalMissedProofValues: newValid,
	}
	if err := s.transport.WriteRequest(RPCRenewClearContractID, req); err != nil {
		return Contract{}, nil, err
	}

	var resp RPCFormContractAdditions
	if err := s.transport.ReadResponse(&resp, 65536); err != nil {
		return Contract{}, nil, err
	}

	// merge host additions with txn
	txn.SiacoinInputs = append(txn.SiacoinInputs, resp.Inputs...)
	txn.SiacoinOutputs = append(txn.SiacoinOutputs, resp.Outputs...)

	// create initial (no-op) revision, transaction, and signature
	fc := txn.FileContracts[0]
	initRevision := types.FileContractRevision{
		ParentID:          txn.FileContractID(0),
		UnlockConditions:  s.contract.Revision.UnlockConditions,
		NewRevisionNumber: 1,

		NewFileSize:           fc.FileSize,
		NewFileMerkleRoot:     fc.FileMerkleRoot,
		NewWindowStart:        fc.WindowStart,
		NewWindowEnd:          fc.WindowEnd,
		NewValidProofOutputs:  fc.ValidProofOutputs,
		NewMissedProofOutputs: fc.MissedProofOutputs,
		NewUnlockHash:         fc.UnlockHash,
	}
	revSig := s.key.SignHash(hashRevision(initRevision))
	renterRevisionSig := types.TransactionSignature{
		ParentID:       crypto.Hash(initRevision.ParentID),
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
		return Contract{}, nil, err
	}

	// read the host signatures and merge them with our own
	var hostSigs RPCRenewAndClearContractSignatures
	if err := s.transport.ReadResponse(&hostSigs, 4096); err != nil {
		return Contract{}, nil, err
	}
	txn.TransactionSignatures = append(renterContractSignatures, hostSigs.ContractSignatures...)
	signedTxnSet := append(resp.Parents, append(parents, txn)...)

	return Contract{
		Revision:   initRevision,
		Signatures: [2]types.TransactionSignature{renterRevisionSig, hostSigs.RevisionSignature},
	}, signedTxnSet, nil
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
	guess := target.Big()
	guess.Mul(guess, big.NewInt(1000))
	guess.Div(guess, big.NewInt(961))

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
	sfc := types.SiafundCount.Big()
	tm := new(big.Int).Mod(target.Big(), sfc)
	gm := new(big.Int).Mod(guess, sfc)
	if gm.Cmp(tm) < 0 {
		guess.Sub(guess, sfc)
	}
	guess.Sub(guess, gm)
	guess.Add(guess, tm)

	return types.NewCurrency(guess)
}
