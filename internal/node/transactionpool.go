package node

import (
	"errors"
	"slices"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/bus"
	"go.sia.tech/siad/modules"
	stypes "go.sia.tech/siad/types"
)

type txpool struct {
	tp modules.TransactionPool
}

func (tp txpool) RecommendedFee() (fee types.Currency) {
	_, max := tp.tp.FeeEstimation()
	convertToCore(&max, (*types.V1Currency)(&fee))
	return
}

func (tp txpool) Transactions() []types.Transaction {
	stxns := tp.tp.Transactions()
	txns := make([]types.Transaction, len(stxns))
	for i := range txns {
		convertToCore(&stxns[i], &txns[i])
	}
	return txns
}

func (tp txpool) AcceptTransactionSet(txns []types.Transaction) error {
	stxns := make([]stypes.Transaction, len(txns))
	for i := range stxns {
		convertToSiad(&txns[i], &stxns[i])
	}
	err := tp.tp.AcceptTransactionSet(stxns)
	if errors.Is(err, modules.ErrDuplicateTransactionSet) {
		err = nil
	}
	return err
}

func (tp txpool) UnconfirmedParents(txn types.Transaction) ([]types.Transaction, error) {
	return unconfirmedParents(txn, tp.Transactions()), nil
}

func (tp txpool) Subscribe(subscriber modules.TransactionPoolSubscriber) {
	tp.tp.TransactionPoolSubscribe(subscriber)
}

func (tp txpool) Close() error {
	return tp.tp.Close()
}

func unconfirmedParents(txn types.Transaction, pool []types.Transaction) []types.Transaction {
	outputToParent := make(map[types.SiacoinOutputID]*types.Transaction)
	for i, txn := range pool {
		for j := range txn.SiacoinOutputs {
			outputToParent[txn.SiacoinOutputID(j)] = &pool[i]
		}
	}
	var parents []types.Transaction
	txnsToCheck := []*types.Transaction{&txn}
	seen := make(map[types.TransactionID]bool)
	for len(txnsToCheck) > 0 {
		nextTxn := txnsToCheck[0]
		txnsToCheck = txnsToCheck[1:]
		for _, sci := range nextTxn.SiacoinInputs {
			if parent, ok := outputToParent[sci.ParentID]; ok {
				if txid := parent.ID(); !seen[txid] {
					seen[txid] = true
					parents = append(parents, *parent)
					txnsToCheck = append(txnsToCheck, parent)
				}
			}
		}
	}
	slices.Reverse(parents)
	return parents
}

func NewTransactionPool(tp modules.TransactionPool) bus.TransactionPool {
	return &txpool{tp: tp}
}
