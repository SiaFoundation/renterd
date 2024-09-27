package api

import (
	"errors"

	rhpv3 "go.sia.tech/core/rhp/v3"
	"go.sia.tech/core/types"
)

var (
	ErrMarkerNotFound        = errors.New("marker not found")
	ErrMaxFundAmountExceeded = errors.New("renewal exceeds max fund amount")
	ErrExplorerDisabled      = errors.New("explorer is disabled")
)

type (
	// ConsensusState holds the current blockheight and whether we are synced or not.
	ConsensusState struct {
		BlockHeight   uint64      `json:"blockHeight"`
		LastBlockTime TimeRFC3339 `json:"lastBlockTime"`
		Synced        bool        `json:"synced"`
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
	}
)

type (
	AccountsFundRequest struct {
		AccountID  rhpv3.Account        `json:"accountID"`
		Amount     types.Currency       `json:"amount"`
		ContractID types.FileContractID `json:"contractID"`
	}

	AccountsFundResponse struct {
		Deposit types.Currency `json:"deposit"`
	}

	AccountsSaveRequest struct {
		Accounts []Account `json:"accounts"`
	}

	// BusStateResponse is the response type for the /bus/state endpoint.
	BusStateResponse struct {
		StartTime TimeRFC3339 `json:"startTime"`
		Network   string      `json:"network"`
		BuildState
		Explorer ExplorerState `json:"explorer"`
	}

	// ExplorerState contains static information about explorer data sources.
	ExplorerState struct {
		Enabled bool   `json:"enabled"`
		URL     string `json:"url,omitempty"`
	}

	ContractSetUpdateRequest struct {
		ToAdd    []types.FileContractID `json:"toAdd"`
		ToRemove []types.FileContractID `json:"toRemove"`
	}
)
