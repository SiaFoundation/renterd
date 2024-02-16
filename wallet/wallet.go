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

func ConvertToTransactions(events []wallet.Event) []Transaction {
	transactions := make([]Transaction, len(events))
	for i, event := range events {
		transactions[i] = converToTransaction(event)
	}
	return transactions
}

func converToTransaction(e wallet.Event) Transaction {
	return Transaction{
		Raw:       e.Transaction,
		Index:     e.Index,
		ID:        types.TransactionID(e.ID),
		Inflow:    e.Inflow,
		Outflow:   e.Outflow,
		Timestamp: e.Timestamp,
	}
}
