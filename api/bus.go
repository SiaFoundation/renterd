package api

import (
	"errors"

	rhpv2 "go.sia.tech/core/rhp/v2"
	rhpv3 "go.sia.tech/core/rhp/v3"
	"go.sia.tech/core/types"
)

var (
	ErrInvalidOffset         = errors.New("offset must be non-negative")
	ErrInvalidLength         = errors.New("length must be positive")
	ErrInvalidLimit          = errors.New("limit must be -1 or bigger")
	ErrMarkerNotFound        = errors.New("marker not found")
	ErrMaxFundAmountExceeded = errors.New("renewal exceeds max fund amount")
	ErrInvalidDatabase       = errors.New("invalid database type")
	ErrBackupNotSupported    = errors.New("backups not supported for used database")
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

	BackupRequest struct {
		Database string `json:"database"`
		Path     string `json:"path"`
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

	// HostScanRequest is the request type for the /host/scan endpoint.
	HostScanRequest struct {
		Timeout DurationMS `json:"timeout"`
	}

	// HostScanResponse is the response type for the /host/scan endpoint.
	HostScanResponse struct {
		Ping       DurationMS           `json:"ping"`
		ScanError  string               `json:"scanError,omitempty"`
		Settings   rhpv2.HostSettings   `json:"settings,omitempty"`
		PriceTable rhpv3.HostPriceTable `json:"priceTable,omitempty"`
	}

	// UpdateAutopilotRequest is the request type for the /autopilot endpoint.
	UpdateAutopilotRequest struct {
		Enabled   *bool            `json:"enabled"`
		Contracts *ContractsConfig `json:"contracts"`
		Hosts     *HostsConfig     `json:"hosts"`
	}
)
