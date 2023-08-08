package wallet

import (
	"errors"
	"fmt"
	"reflect"
	"sort"
	"sync"
	"time"

	"gitlab.com/NebulousLabs/encoding"
	"go.sia.tech/core/consensus"
	"go.sia.tech/core/types"
	stypes "go.sia.tech/siad/types"
	"lukechampine.com/frand"
)

// BytesPerInput is the encoded size of a SiacoinInput and corresponding
// TransactionSignature, assuming standard UnlockConditions.
const BytesPerInput = 241

// ErrInsufficientBalance is returned when there aren't enough unused outputs to
// cover the requested amount.
var ErrInsufficientBalance = errors.New("insufficient balance")

// StandardUnlockConditions returns the standard unlock conditions for a single
// Ed25519 key.
func StandardUnlockConditions(pk types.PublicKey) types.UnlockConditions {
	return types.UnlockConditions{
		PublicKeys: []types.UnlockKey{{
			Algorithm: types.SpecifierEd25519,
			Key:       pk[:],
		}},
		SignaturesRequired: 1,
	}
}

// StandardAddress returns the standard address for an Ed25519 key.
func StandardAddress(pk types.PublicKey) types.Address {
	return StandardUnlockConditions(pk).UnlockHash()
}

// StandardTransactionSignature returns the standard signature object for a
// siacoin or siafund input.
func StandardTransactionSignature(id types.Hash256) types.TransactionSignature {
	return types.TransactionSignature{
		ParentID:       id,
		CoveredFields:  types.CoveredFields{WholeTransaction: true},
		PublicKeyIndex: 0,
	}
}

// ExplicitCoveredFields returns a CoveredFields that covers all elements
// present in txn.
func ExplicitCoveredFields(txn types.Transaction) (cf types.CoveredFields) {
	for i := range txn.SiacoinInputs {
		cf.SiacoinInputs = append(cf.SiacoinInputs, uint64(i))
	}
	for i := range txn.SiacoinOutputs {
		cf.SiacoinOutputs = append(cf.SiacoinOutputs, uint64(i))
	}
	for i := range txn.FileContracts {
		cf.FileContracts = append(cf.FileContracts, uint64(i))
	}
	for i := range txn.FileContractRevisions {
		cf.FileContractRevisions = append(cf.FileContractRevisions, uint64(i))
	}
	for i := range txn.StorageProofs {
		cf.StorageProofs = append(cf.StorageProofs, uint64(i))
	}
	for i := range txn.SiafundInputs {
		cf.SiafundInputs = append(cf.SiafundInputs, uint64(i))
	}
	for i := range txn.SiafundOutputs {
		cf.SiafundOutputs = append(cf.SiafundOutputs, uint64(i))
	}
	for i := range txn.MinerFees {
		cf.MinerFees = append(cf.MinerFees, uint64(i))
	}
	for i := range txn.ArbitraryData {
		cf.ArbitraryData = append(cf.ArbitraryData, uint64(i))
	}
	for i := range txn.Signatures {
		cf.Signatures = append(cf.Signatures, uint64(i))
	}
	return
}

// A SiacoinElement is a SiacoinOutput along with its ID.
type SiacoinElement struct {
	types.SiacoinOutput
	ID             types.Hash256
	MaturityHeight uint64
}

// A Transaction is an on-chain transaction relevant to a particular wallet,
// paired with useful metadata.
type Transaction struct {
	Raw       types.Transaction   `json:"raw,omitempty"`
	Index     types.ChainIndex    `json:"index"`
	ID        types.TransactionID `json:"id"`
	Inflow    types.Currency      `json:"inflow"`
	Outflow   types.Currency      `json:"outflow"`
	Timestamp time.Time           `json:"timestamp"`
}

// A SingleAddressStore stores the state of a single-address wallet.
// Implementations are assumed to be thread safe.
type SingleAddressStore interface {
	Height() uint64
	UnspentSiacoinElements(matured bool) ([]SiacoinElement, error)
	Transactions(before, since time.Time, offset, limit int) ([]Transaction, error)
}

// A TransactionPool contains transactions that have not yet been included in a
// block.
type TransactionPool interface {
	ContainsElement(id types.Hash256) bool
}

// A SingleAddressWallet is a hot wallet that manages the outputs controlled by
// a single address.
type SingleAddressWallet struct {
	priv           types.PrivateKey
	addr           types.Address
	store          SingleAddressStore
	usedUTXOExpiry time.Duration

	// for building transactions
	mu       sync.Mutex
	lastUsed map[types.Hash256]time.Time
}

// PrivateKey returns the private key of the wallet.
func (w *SingleAddressWallet) PrivateKey() types.PrivateKey {
	return w.priv
}

// Address returns the address of the wallet.
func (w *SingleAddressWallet) Address() types.Address {
	return w.addr
}

// Balance returns the balance of the wallet.
func (w *SingleAddressWallet) Balance() (spendable, confirmed types.Currency, _ error) {
	sces, err := w.store.UnspentSiacoinElements(true)
	if err != nil {
		return types.Currency{}, types.Currency{}, err
	}
	for _, sce := range sces {
		if !w.isOutputUsed(sce.ID) {
			spendable = spendable.Add(sce.Value)
		}
		confirmed = confirmed.Add(sce.Value)
	}
	return
}

func (w *SingleAddressWallet) Height() uint64 {
	return w.store.Height()
}

// UnspentOutputs returns the set of unspent Siacoin outputs controlled by the
// wallet.
func (w *SingleAddressWallet) UnspentOutputs() ([]SiacoinElement, error) {
	return w.store.UnspentSiacoinElements(false)
}

// Transactions returns up to max transactions relevant to the wallet that have
// a timestamp later than since.
func (w *SingleAddressWallet) Transactions(before, since time.Time, offset, limit int) ([]Transaction, error) {
	return w.store.Transactions(before, since, offset, limit)
}

// FundTransaction adds siacoin inputs worth at least the requested amount to
// the provided transaction. A change output is also added, if necessary. The
// inputs will not be available to future calls to FundTransaction unless
// ReleaseInputs is called.
func (w *SingleAddressWallet) FundTransaction(cs consensus.State, txn *types.Transaction, amount types.Currency, pool []types.Transaction) ([]types.Hash256, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if amount.IsZero() {
		return nil, nil
	}

	// avoid reusing any inputs currently in the transaction pool
	inPool := make(map[types.Hash256]bool)
	for _, ptxn := range pool {
		for _, in := range ptxn.SiacoinInputs {
			inPool[types.Hash256(in.ParentID)] = true
		}
	}

	utxos, err := w.store.UnspentSiacoinElements(false)
	if err != nil {
		return nil, err
	}
	// choose outputs randomly
	frand.Shuffle(len(utxos), reflect.Swapper(utxos))

	var outputSum types.Currency
	var fundingElements []SiacoinElement
	for _, sce := range utxos {
		if w.isOutputUsed(sce.ID) || inPool[sce.ID] || cs.Index.Height < sce.MaturityHeight {
			continue
		}
		fundingElements = append(fundingElements, sce)
		outputSum = outputSum.Add(sce.Value)
		if outputSum.Cmp(amount) >= 0 {
			break
		}
	}
	if outputSum.Cmp(amount) < 0 {
		return nil, fmt.Errorf("%w: outputSum: %v, amount: %v", ErrInsufficientBalance, outputSum.String(), amount.String())
	} else if outputSum.Cmp(amount) > 0 {
		txn.SiacoinOutputs = append(txn.SiacoinOutputs, types.SiacoinOutput{
			Value:   outputSum.Sub(amount),
			Address: w.addr,
		})
	}

	toSign := make([]types.Hash256, len(fundingElements))
	for i, sce := range fundingElements {
		txn.SiacoinInputs = append(txn.SiacoinInputs, types.SiacoinInput{
			ParentID:         types.SiacoinOutputID(sce.ID),
			UnlockConditions: StandardUnlockConditions(w.priv.PublicKey()),
		})
		toSign[i] = sce.ID
		w.lastUsed[sce.ID] = time.Now()
	}

	return toSign, nil
}

// ReleaseInputs is a helper function that releases the inputs of txn for use in
// other transactions. It should only be called on transactions that are invalid
// or will never be broadcast.
func (w *SingleAddressWallet) ReleaseInputs(txn types.Transaction) {
	for _, in := range txn.SiacoinInputs {
		delete(w.lastUsed, types.Hash256(in.ParentID))
	}
}

// SignTransaction adds a signature to each of the specified inputs.
func (w *SingleAddressWallet) SignTransaction(cs consensus.State, txn *types.Transaction, toSign []types.Hash256, cf types.CoveredFields) error {
	// NOTE: siad uses different hardfork heights when -tags=testing is set,
	// so we have to alter cs accordingly.
	// TODO: remove this
	switch {
	case cs.Index.Height >= uint64(stypes.FoundationHardforkHeight):
		cs.Index.Height = 298000
	case cs.Index.Height >= uint64(stypes.ASICHardforkHeight):
		cs.Index.Height = 179000
	}

	for _, id := range toSign {
		ts := types.TransactionSignature{
			ParentID:       id,
			CoveredFields:  cf,
			PublicKeyIndex: 0,
		}
		var h types.Hash256
		if cf.WholeTransaction {
			h = cs.WholeSigHash(*txn, ts.ParentID, ts.PublicKeyIndex, ts.Timelock, cf.Signatures)
		} else {
			h = cs.PartialSigHash(*txn, cf)
		}
		sig := w.priv.SignHash(h)
		ts.Signature = sig[:]
		txn.Signatures = append(txn.Signatures, ts)
	}
	return nil
}

// Redistribute returns a transaction that redistributes money in the wallet by
// selecting a minimal set of inputs to cover the creation of the requested
// outputs. It also returns a list of output IDs that need to be signed.
//
// NOTE: we can not reuse 'FundTransaction' because it randomizes the unspent
// transaction outputs it uses and we need a minimal set of inputs
func (w *SingleAddressWallet) Redistribute(cs consensus.State, outputs int, amount, feePerByte types.Currency, pool []types.Transaction) (types.Transaction, []types.Hash256, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	// prepare all outputs
	var txn types.Transaction
	for i := 0; i < int(outputs); i++ {
		txn.SiacoinOutputs = append(txn.SiacoinOutputs, types.SiacoinOutput{
			Value:   amount,
			Address: w.Address(),
		})
	}

	// fetch unspent transaction outputs
	utxos, err := w.store.UnspentSiacoinElements(false)
	if err != nil {
		return types.Transaction{}, nil, err
	}

	// desc sort
	sort.Slice(utxos, func(i, j int) bool {
		return utxos[i].Value.Cmp(utxos[j].Value) > 0
	})

	// map used outputs
	inPool := make(map[types.Hash256]bool)
	for _, ptxn := range pool {
		for _, in := range ptxn.SiacoinInputs {
			inPool[types.Hash256(in.ParentID)] = true
		}
	}

	// estimate the fees
	outputFees := feePerByte.Mul64(uint64(len(encoding.Marshal(txn.SiacoinOutputs))))
	feePerInput := feePerByte.Mul64(BytesPerInput)

	// collect outputs that cover the total amount
	var inputs []SiacoinElement
	want := amount.Mul64(uint64(outputs))
	var amtInUse, amtSameValue, amtNotMatured types.Currency
	for _, sce := range utxos {
		inUse := w.isOutputUsed(sce.ID) || inPool[sce.ID]
		matured := cs.Index.Height >= sce.MaturityHeight
		sameValue := sce.Value.Equals(amount)
		if inUse {
			amtInUse = amtInUse.Add(sce.Value)
			continue
		} else if sameValue {
			amtSameValue = amtSameValue.Add(sce.Value)
			continue
		} else if !matured {
			amtNotMatured = amtNotMatured.Add(sce.Value)
			continue
		}

		inputs = append(inputs, sce)
		fee := feePerInput.Mul64(uint64(len(inputs))).Add(outputFees)
		if SumOutputs(inputs).Cmp(want.Add(fee)) > 0 {
			break
		}
	}

	// not enough outputs found
	fee := feePerInput.Mul64(uint64(len(inputs))).Add(outputFees)
	if sumOut := SumOutputs(inputs); sumOut.Cmp(want.Add(fee)) < 0 {
		return types.Transaction{}, nil, fmt.Errorf("%w: inputs %v < needed %v + txnFee %v (usable: %v, inUse: %v, sameValue: %v, notMatured: %v)",
			ErrInsufficientBalance, sumOut.String(), want.String(), fee.String(), sumOut.String(), amtInUse.String(), amtSameValue.String(), amtNotMatured.String())
	}

	// set the miner fee
	txn.MinerFees = []types.Currency{fee}

	// add the change output
	change := SumOutputs(inputs).Sub(want.Add(fee))
	if !change.IsZero() {
		txn.SiacoinOutputs = append(txn.SiacoinOutputs, types.SiacoinOutput{
			Value:   change,
			Address: w.addr,
		})
	}

	// add the inputs
	toSign := make([]types.Hash256, len(inputs))
	for i, sce := range inputs {
		txn.SiacoinInputs = append(txn.SiacoinInputs, types.SiacoinInput{
			ParentID:         types.SiacoinOutputID(sce.ID),
			UnlockConditions: StandardUnlockConditions(w.priv.PublicKey()),
		})
		toSign[i] = sce.ID
		w.lastUsed[sce.ID] = time.Now()
	}

	return txn, toSign, nil
}

func (w *SingleAddressWallet) isOutputUsed(id types.Hash256) bool {
	lastUsed := w.lastUsed[id]
	if w.usedUTXOExpiry == 0 {
		return !lastUsed.IsZero()
	}
	return time.Since(lastUsed) <= w.usedUTXOExpiry
}

// SumOutputs returns the total value of the supplied outputs.
func SumOutputs(outputs []SiacoinElement) (sum types.Currency) {
	for _, o := range outputs {
		sum = sum.Add(o.Value)
	}
	return
}

// NewSingleAddressWallet returns a new SingleAddressWallet using the provided private key and store.
func NewSingleAddressWallet(priv types.PrivateKey, store SingleAddressStore, usedUTXOExpiry time.Duration) *SingleAddressWallet {
	return &SingleAddressWallet{
		priv:           priv,
		addr:           StandardAddress(priv.PublicKey()),
		store:          store,
		lastUsed:       make(map[types.Hash256]time.Time),
		usedUTXOExpiry: usedUTXOExpiry,
	}
}
