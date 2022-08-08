package wallet

import (
	"errors"
	"sync"
	"time"

	"go.sia.tech/renterd/internal/consensus"
	"go.sia.tech/siad/crypto"
	"go.sia.tech/siad/types"
)

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
		txn.TransactionSignatures = append(txn.TransactionSignatures, types.TransactionSignature{
			ParentID:       crypto.Hash(sce.ID),
			CoveredFields:  types.FullCoveredFields,
			PublicKeyIndex: 0,
		})
		toSign[i] = sce.ID
		w.used[sce.ID] = true
	}

	return toSign, nil
}

// ReleaseInputs is a helper function that releases the inputs of txn for use in
// other transactions. It should only be called on transactions that are invalid
// or will never be broadcast.
func (w *SingleAddressWallet) ReleaseInputs(txn types.Transaction) {
	for _, in := range txn.SiacoinInputs {
		delete(w.used, types.OutputID(in.ParentID))
	}
}

// SignTransaction adds a signature to each of the specified inputs using the
// provided seed.
func (w *SingleAddressWallet) SignTransaction(cs consensus.State, txn *types.Transaction, toSign []types.OutputID) error {
	inputWithID := func(id types.OutputID) *types.SiacoinInput {
		for i := range txn.SiacoinInputs {
			if in := &txn.SiacoinInputs[i]; types.OutputID(in.ParentID) == id {
				return in
			}
		}
		return nil
	}
	for _, id := range toSign {
		in := inputWithID(id)
		if in == nil {
			return errors.New("no input with specified ID")
		}
		sig := w.priv.SignHash(cs.InputSigHash(*txn, len(txn.TransactionSignatures)))
		txn.TransactionSignatures = append(txn.TransactionSignatures, types.TransactionSignature{
			ParentID:       crypto.Hash(id),
			CoveredFields:  types.FullCoveredFields,
			PublicKeyIndex: 0,
			Signature:      sig[:],
		})
	}
	return nil
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
