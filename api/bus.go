package api

import (
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/wallet"
)

type (
	// ConsensusState holds the current blockheight and whether we are synced or not.
	ConsensusState struct {
		BlockHeight   uint64      `json:"blockHeight"`
		LastBlockTime TimeRFC3339 `json:"lastBlockTime"`
		Synced        bool        `json:"synced"`
	}

	// ConsensusNetwork holds the name of the network.
	ConsensusNetwork struct {
		Name string
	}
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

func ConvertToSiacoinElements(sces []wallet.SiacoinElement) []SiacoinElement {
	elements := make([]SiacoinElement, len(sces))
	for i, sce := range sces {
		elements[i] = SiacoinElement{
			ID: sce.StateElement.ID,
			SiacoinOutput: types.SiacoinOutput{
				Value:   sce.SiacoinOutput.Value,
				Address: sce.SiacoinOutput.Address,
			},
			MaturityHeight: sce.MaturityHeight,
		}
	}
	return elements
}

func ConvertToTransactions(events []wallet.Event) []Transaction {
	transactions := make([]Transaction, len(events))
	for i, e := range events {
		transactions[i] = Transaction{
			Raw:       e.Transaction,
			Index:     e.Index,
			ID:        types.TransactionID(e.ID),
			Inflow:    e.Inflow,
			Outflow:   e.Outflow,
			Timestamp: e.Timestamp,
		}
	}
	return transactions
}

type (
	// UploadParams contains the metadata needed by a worker to upload an object.
	UploadParams struct {
		CurrentHeight uint64
		ContractSet   string
		UploadPacking bool
		GougingParams
	}

	// GougingParams contains the metadata needed by a worker to perform gouging
	// checks.
	GougingParams struct {
		ConsensusState     ConsensusState
		GougingSettings    GougingSettings
		RedundancySettings RedundancySettings
		TransactionFee     types.Currency
	}
)

type (
	// BusStateResponse is the response type for the /bus/state endpoint.
	BusStateResponse struct {
		StartTime TimeRFC3339 `json:"startTime"`
		BuildState
	}
)
