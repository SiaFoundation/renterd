package wallet

import (
	"errors"
	"math/big"
	"reflect"
	"sort"
	"sync"
	"time"

	"gitlab.com/NebulousLabs/encoding"
	"go.sia.tech/renterd/internal/consensus"
	"go.sia.tech/siad/crypto"
	"go.sia.tech/siad/types"
	"lukechampine.com/frand"
)

// BytesPerInput is the encoded size of a SiacoinInput and corresponding
// TransactionSignature, assuming standard UnlockConditions.
const BytesPerInput = 241

// StandardUnlockConditions returns the standard unlock conditions for a single
// Ed25519 key.
func StandardUnlockConditions(pk consensus.PublicKey) types.UnlockConditions {
	return types.UnlockConditions{
		PublicKeys: []types.SiaPublicKey{{
			Algorithm: types.SignatureEd25519,
			Key:       pk[:],
		}},
		SignaturesRequired: 1,
	}
}

// StandardAddress returns the standard address for an Ed25519 key.
func StandardAddress(pk consensus.PublicKey) types.UnlockHash {
	return StandardUnlockConditions(pk).UnlockHash()
}

// StandardTransactionSignature returns the standard signature object for a
// siacoin or siafund input.
func StandardTransactionSignature(id types.OutputID) types.TransactionSignature {
	return types.TransactionSignature{
		ParentID:       crypto.Hash(id),
		CoveredFields:  types.FullCoveredFields,
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
	for i := range txn.TransactionSignatures {
		cf.TransactionSignatures = append(cf.TransactionSignatures, uint64(i))
	}
	return
}

// A SiacoinElement is a SiacoinOutput along with its ID.
type SiacoinElement struct {
	types.SiacoinOutput
	ID             types.OutputID
	MaturityHeight uint64
}

// A Transaction is an on-chain transaction relevant to a particular wallet,
// paired with useful metadata.
type Transaction struct {
	Raw       types.Transaction
	Index     consensus.ChainIndex
	ID        types.TransactionID
	Inflow    types.Currency
	Outflow   types.Currency
	Timestamp time.Time
}

// A SingleAddressStore stores the state of a single-address wallet.
// Implementations are assumed to be thread safe.
type SingleAddressStore interface {
	Balance() types.Currency
	UnspentSiacoinElements() ([]SiacoinElement, error)
	Transactions(since time.Time, max int) ([]Transaction, error)
}

// A TransactionPool contains transactions that have not yet been included in a
// block.
type TransactionPool interface {
	ContainsElement(id types.OutputID) bool
}

// A SingleAddressWallet is a hot wallet that manages the outputs controlled by
// a single address.
type SingleAddressWallet struct {
	priv  consensus.PrivateKey
	addr  types.UnlockHash
	store SingleAddressStore

	// for building transactions
	mu   sync.Mutex
	used map[types.OutputID]bool
}

// PrivateKey returns the private key of the wallet.
func (w *SingleAddressWallet) PrivateKey() consensus.PrivateKey {
	return w.priv
}

// Address returns the address of the wallet.
func (w *SingleAddressWallet) Address() types.UnlockHash {
	return w.addr
}

// Balance returns the balance of the wallet.
func (w *SingleAddressWallet) Balance() types.Currency {
	return w.store.Balance()
}

// UnspentOutputs returns the set of unspent Siacoin outputs controlled by the
// wallet.
func (w *SingleAddressWallet) UnspentOutputs() ([]SiacoinElement, error) {
	return w.store.UnspentSiacoinElements()
}

// Transactions returns up to max transactions relevant to the wallet that have
// a timestamp later than since.
func (w *SingleAddressWallet) Transactions(since time.Time, max int) ([]Transaction, error) {
	return w.store.Transactions(since, max)
}

// FundTransaction adds siacoin inputs worth at least the requested amount to
// the provided transaction. A change output is also added, if necessary. The
// inputs will not be available to future calls to FundTransaction unless
// ReleaseInputs is called.
func (w *SingleAddressWallet) FundTransaction(cs consensus.State, txn *types.Transaction, amount types.Currency, pool []types.Transaction) ([]types.OutputID, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if amount.IsZero() {
		return nil, nil
	}

	// avoid reusing any inputs currently in the transaction pool
	inPool := make(map[types.OutputID]bool)
	for _, ptxn := range pool {
		for _, in := range ptxn.SiacoinInputs {
			inPool[types.OutputID(in.ParentID)] = true
		}
	}

	utxos, err := w.store.UnspentSiacoinElements()
	if err != nil {
		return nil, err
	}
	// choose outputs randomly
	frand.Shuffle(len(utxos), reflect.Swapper(utxos))

	var outputSum types.Currency
	var fundingElements []SiacoinElement
	for _, sce := range utxos {
		if w.used[sce.ID] || inPool[sce.ID] || cs.Index.Height < sce.MaturityHeight {
			continue
		}
		fundingElements = append(fundingElements, sce)
		outputSum = outputSum.Add(sce.Value)
		if outputSum.Cmp(amount) >= 0 {
			break
		}
	}
	if outputSum.Cmp(amount) < 0 {
		return nil, errors.New("insufficient balance")
	} else if outputSum.Cmp(amount) > 0 {
		txn.SiacoinOutputs = append(txn.SiacoinOutputs, types.SiacoinOutput{
			Value:      outputSum.Sub(amount),
			UnlockHash: w.addr,
		})
	}

	toSign := make([]types.OutputID, len(fundingElements))
	for i, sce := range fundingElements {
		txn.SiacoinInputs = append(txn.SiacoinInputs, types.SiacoinInput{
			ParentID:         types.SiacoinOutputID(sce.ID),
			UnlockConditions: StandardUnlockConditions(w.priv.PublicKey()),
		})
		toSign[i] = sce.ID
		w.used[sce.ID] = true
	}

	return toSign, nil
}

// Split returns a transaction that splits the wallet in the given number of
// outputs with given amount. The transaction is funded but not signed.
//
// NOTE: split needs to use a minimal set of inputs and therefore does not reuse
// the fund logic which randomizes the unspent transaction outputs used to fund
// the transaction
func (w *SingleAddressWallet) Split(cs consensus.State, outputs int, amount, feePerByte types.Currency, pool []types.Transaction) (types.Transaction, []types.OutputID, error) {
	// prepare all outputs
	var txn types.Transaction
	for i := 0; i < int(outputs); i++ {
		txn.SiacoinOutputs = append(txn.SiacoinOutputs, types.SiacoinOutput{
			Value:      amount,
			UnlockHash: w.Address(),
		})
	}

	// fetch unspent transaction outputs
	utxos, err := w.store.UnspentSiacoinElements()
	if err != nil {
		return types.Transaction{}, nil, err
	}

	// desc sort
	sort.Slice(utxos, func(i, j int) bool {
		return utxos[i].Value.Cmp(utxos[j].Value) > 0
	})

	// map used outputs
	inPool := make(map[types.OutputID]bool)
	for _, ptxn := range pool {
		for _, in := range ptxn.SiacoinInputs {
			inPool[types.OutputID(in.ParentID)] = true
		}
	}

	// filter outputs that are in use
	var u int
	for _, sce := range utxos {
		inUse := w.used[sce.ID] || inPool[sce.ID]
		matured := cs.Index.Height >= sce.MaturityHeight
		sameValue := sce.Value.Equals(amount)
		if !inUse && !sameValue && matured {
			utxos[u] = sce
			u++
		}
	}
	utxos = utxos[:u]

	// estimate the fees
	outputFees := feePerByte.Mul64(uint64(len(encoding.Marshal(txn.SiacoinOutputs))))
	feePerInput := feePerByte.Mul64(BytesPerInput)

	// search for minimal set
	want := amount.Mul64(uint64(outputs))
	i := sort.Search(len(utxos)+1, func(i int) bool {
		fee := feePerInput.Mul64(uint64(i)).Add(outputFees)
		return SumOutputs(utxos[:i]).Cmp(want.Add(fee)) >= 0
	})

	// no set found
	if i == len(utxos)+1 {
		return types.Transaction{}, nil, errors.New("insufficient balance")
	}
	utxos = utxos[:i]

	// set the miner fee
	fee := feePerInput.Mul64(uint64(len(utxos))).Add(outputFees)
	txn.MinerFees = []types.Currency{fee}

	// add the change output
	change := SumOutputs(utxos).Sub(want.Add(fee))
	if !change.IsZero() {
		txn.SiacoinOutputs = append(txn.SiacoinOutputs, types.SiacoinOutput{
			Value:      change,
			UnlockHash: w.addr,
		})
	}

	// add the inputs
	toSign := make([]types.OutputID, len(utxos))
	for i, sce := range utxos {
		txn.SiacoinInputs = append(txn.SiacoinInputs, types.SiacoinInput{
			ParentID:         types.SiacoinOutputID(sce.ID),
			UnlockConditions: StandardUnlockConditions(w.priv.PublicKey()),
		})
		toSign[i] = sce.ID
		w.used[sce.ID] = true
	}

	return txn, toSign, nil
}

// ReleaseInputs is a helper function that releases the inputs of txn for use in
// other transactions. It should only be called on transactions that are invalid
// or will never be broadcast.
func (w *SingleAddressWallet) ReleaseInputs(txn types.Transaction) {
	for _, in := range txn.SiacoinInputs {
		delete(w.used, types.OutputID(in.ParentID))
	}
}

// SignTransaction adds a signature to each of the specified inputs.
func (w *SingleAddressWallet) SignTransaction(cs consensus.State, txn *types.Transaction, toSign []types.OutputID, cf types.CoveredFields) error {
	for _, id := range toSign {
		i := len(txn.TransactionSignatures)
		txn.TransactionSignatures = append(txn.TransactionSignatures, types.TransactionSignature{
			ParentID:       crypto.Hash(id),
			CoveredFields:  cf,
			PublicKeyIndex: 0,
		})
		sig := w.priv.SignHash(cs.InputSigHash(*txn, i))
		txn.TransactionSignatures[i].Signature = sig[:]
	}
	return nil
}

// SumOutputs returns the total value of the supplied outputs.
func SumOutputs(outputs []SiacoinElement) types.Currency {
	sum := new(big.Int)
	for _, o := range outputs {
		sum.Add(sum, o.Value.Big())
	}
	return types.NewCurrency(sum)
}

// NewSingleAddressWallet returns a new SingleAddressWallet using the provided private key and store.
func NewSingleAddressWallet(priv consensus.PrivateKey, store SingleAddressStore) *SingleAddressWallet {
	return &SingleAddressWallet{
		priv:  priv,
		addr:  StandardAddress(priv.PublicKey()),
		store: store,
		used:  make(map[types.OutputID]bool),
	}
}
