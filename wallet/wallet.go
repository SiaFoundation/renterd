package wallet

import (
	"errors"
	"reflect"
	"sync"
	"time"

	"go.sia.tech/renterd/internal/consensus"
	"go.sia.tech/siad/crypto"
	"go.sia.tech/siad/types"
	"lukechampine.com/frand"
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

// StandardTransactionSignature returns the standard signature object for a
// siacoin or siafund input.
func StandardTransactionSignature(id types.OutputID) types.TransactionSignature {
	return types.TransactionSignature{
		ParentID:       crypto.Hash(id),
		CoveredFields:  types.FullCoveredFields,
		PublicKeyIndex: 0,
	}
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

// ReleaseInputs is a helper function that releases the inputs of txn for use in
// other transactions. It should only be called on transactions that are invalid
// or will never be broadcast.
func (w *SingleAddressWallet) ReleaseInputs(txn types.Transaction) {
	for _, in := range txn.SiacoinInputs {
		delete(w.used, types.OutputID(in.ParentID))
	}
}

// SignTransaction adds a signature to each of the specified inputs. If an input
// does not already have a corresponding TransactionSignature, one will be
// appended.
func (w *SingleAddressWallet) SignTransaction(cs consensus.State, txn *types.Transaction, toSign []types.OutputID) error {
	for _, id := range toSign {
		var i int
		for i = range txn.TransactionSignatures {
			if txn.TransactionSignatures[i].ParentID == crypto.Hash(id) {
				break
			}
		}
		if i == len(txn.TransactionSignatures) {
			txn.TransactionSignatures = append(txn.TransactionSignatures, StandardTransactionSignature(id))
		}
		sig := w.priv.SignHash(cs.InputSigHash(*txn, i))
		txn.TransactionSignatures[i].Signature = sig[:]
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
