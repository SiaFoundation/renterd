package api

import (
	"errors"

	rhpv3 "go.sia.tech/core/rhp/v3"
	"go.sia.tech/core/types"
)

var (
	ErrMarkerNotFound        = errors.New("marker not found")
	ErrMaxFundAmountExceeded = errors.New("renewal exceeds max fund amount")
	ErrInvalidDatabase       = errors.New("invalid database type")
	ErrBackupNotSupported    = errors.New("backups not supported for used database")
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

	BackupRequest struct {
		Database string `json:"database"`
		Path     string `json:"path"`
	}

	// BusStateResponse is the response type for the /bus/state endpoint.
	BusStateResponse struct {
		StartTime TimeRFC3339 `json:"startTime"`
		Network   string      `json:"network"`
		BuildState
	}

	ContractSetUpdateRequest struct {
		ToAdd    []types.FileContractID `json:"toAdd"`
		ToRemove []types.FileContractID `json:"toRemove"`
	}
)
