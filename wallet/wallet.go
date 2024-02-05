package wallet

import (
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/wallet"
)

type (
	// A SiacoinElement is a SiacoinOutput along with its ID.
	SiacoinElement struct {
		types.SiacoinOutput
		ID             types.Hash256 `json:"id"`
		MaturityHeight uint64        `json:"maturityHeight"`
	}

	// A Transaction is an on-chain transaction relevant to a particular wallet,
	// paired with useful metadata.
	Transaction struct {
		Raw       types.Transaction   `json:"raw,omitempty"`
		Index     types.ChainIndex    `json:"index"`
		ID        types.TransactionID `json:"id"`
		Inflow    types.Currency      `json:"inflow"`
		Outflow   types.Currency      `json:"outflow"`
		Timestamp time.Time           `json:"timestamp"`
	}
)

func ConvertToSiacoinElements(sces []types.SiacoinElement) []SiacoinElement {
	elements := make([]SiacoinElement, len(sces))
	for i, sce := range sces {
		elements[i] = convertToSiacoinElement(sce)
	}
	return elements
}

func convertToSiacoinElement(sce types.SiacoinElement) SiacoinElement {
	return SiacoinElement{
		ID: sce.StateElement.ID,
		SiacoinOutput: types.SiacoinOutput{
			Value:   sce.SiacoinOutput.Value,
			Address: sce.SiacoinOutput.Address,
		},
		MaturityHeight: sce.MaturityHeight,
	}
}

func ConvertToTransactions(txns []wallet.Transaction) []Transaction {
	transactions := make([]Transaction, len(txns))
	for i, txn := range txns {
		transactions[i] = converToTransaction(txn)
	}
	return transactions
}

func converToTransaction(txn wallet.Transaction) Transaction {
	return Transaction{
		Raw:       txn.Transaction,
		Index:     txn.Index,
		ID:        txn.ID,
		Inflow:    txn.Inflow,
		Outflow:   txn.Outflow,
		Timestamp: txn.Timestamp,
	}
}
