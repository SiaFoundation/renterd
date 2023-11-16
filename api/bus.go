package api

import (
	"time"

	"go.sia.tech/core/types"
)

type (
	// ConsensusState holds the current blockheight and whether we are synced or not.
	ConsensusState struct {
		BlockHeight   uint64    `json:"blockHeight"`
		LastBlockTime time.Time `json:"lastBlockTime"`
		Synced        bool      `json:"synced"`
	}

	// ConsensusNetwork holds the name of the network.
	ConsensusNetwork struct {
		Name string
	}
)

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
		StartTime time.Time `json:"startTime"`
		BuildState
	}
)
