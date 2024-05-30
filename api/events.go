package api

import (
	"time"

	"go.sia.tech/core/types"
)

const (
	ModuleContractSet = "contract_set"
	ModuleConsensus   = "consensus"
	ModuleContracts   = "contracts"
	ModuleSettings    = "settings"

	EventUpdate  = "update"
	EventDelete  = "delete"
	EventArchive = "archive"
	EventRenew   = "renewed"
)

type (
	EventConsensusUpdate struct {
		ConsensusState
		TransactionFee types.Currency `json:"transactionFee"`
		Timestamp      time.Time      `json:"timestamp"`
	}

	EventContractArchive struct {
		ContractID types.FileContractID `json:"contractID"`
		Reason     string               `json:"reason"`
		Timestamp  time.Time            `json:"timestamp"`
	}

	EventContractRenew struct {
		ContractID    types.FileContractID `json:"contractID"`
		RenewedFromID types.FileContractID `json:"renewedFromID"`
		Timestamp     time.Time            `json:"timestamp"`
	}

	EventContractSetUpdate struct {
		Name        string                 `json:"name"`
		ContractIDs []types.FileContractID `json:"contractIDs"`
		Timestamp   time.Time              `json:"timestamp"`
	}

	EventSettingUpdate struct {
		Key       string      `json:"key"`
		Update    interface{} `json:"update"`
		Timestamp time.Time   `json:"timestamp"`
	}

	EventSettingDelete struct {
		Key       string    `json:"key"`
		Timestamp time.Time `json:"timestamp"`
	}
)

func (e EventConsensusUpdate) Kind() (string, string)   { return ModuleConsensus, EventUpdate }
func (e EventContractArchive) Kind() (string, string)   { return ModuleContracts, EventArchive }
func (e EventContractRenew) Kind() (string, string)     { return ModuleContracts, EventRenew }
func (e EventContractSetUpdate) Kind() (string, string) { return ModuleContractSet, EventUpdate }
func (e EventSettingUpdate) Kind() (string, string)     { return ModuleSettings, EventUpdate }
func (e EventSettingDelete) Kind() (string, string)     { return ModuleSettings, EventDelete }
