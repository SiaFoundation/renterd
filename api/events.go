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

	EventUpdated  = "updated"
	EventArchived = "archived"
	EventRenewed  = "renewed"
)

type (
	EventConsensusUpdate struct {
		ConsensusState
		TransactionFee types.Currency `json:"transactionFee"`
		Timestamp      time.Time      `json:"timestamp"`
	}

	EventContractArchived struct {
		ContractID types.FileContractID `json:"contractID"`
		Reason     string               `json:"reason"`
		Timestamp  time.Time            `json:"timestamp"`
	}

	EventContractRenewed struct {
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
		Update    interface{} `json:"update,omitempty"`
		Timestamp time.Time   `json:"timestamp"`
	}
)

func (e EventConsensusUpdate) Kind() (string, string)   { return ModuleConsensus, EventUpdated }
func (e EventContractArchived) Kind() (string, string)  { return ModuleContracts, EventArchived }
func (e EventContractRenewed) Kind() (string, string)   { return ModuleContracts, EventRenewed }
func (e EventContractSetUpdate) Kind() (string, string) { return ModuleContractSet, EventUpdated }
func (e EventSettingUpdate) Kind() (string, string)     { return ModuleSettings, EventUpdated }
