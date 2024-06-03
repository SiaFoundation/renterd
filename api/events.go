package api

import (
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/webhooks"
)

const (
	ModuleContractSet = "contract_set"
	ModuleConsensus   = "consensus"
	ModuleContract    = "contract"
	ModuleSetting     = "setting"

	EventUpdate  = "update"
	EventDelete  = "delete"
	EventArchive = "archive"
	EventRenew   = "renew"
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

func (e EventConsensusUpdate) Event() webhooks.Event {
	return webhooks.Event{
		Module:  ModuleConsensus,
		Event:   EventUpdate,
		Payload: e,
	}
}

func (e EventContractArchive) Event() webhooks.Event {
	return webhooks.Event{
		Module:  ModuleContract,
		Event:   EventArchive,
		Payload: e,
	}
}

func (e EventContractRenew) Event() webhooks.Event {
	return webhooks.Event{
		Module:  ModuleContract,
		Event:   EventRenew,
		Payload: e,
	}
}

func (e EventContractSetUpdate) Event() webhooks.Event {
	return webhooks.Event{
		Module:  ModuleContractSet,
		Event:   EventUpdate,
		Payload: e,
	}
}

func (e EventSettingUpdate) Event() webhooks.Event {
	return webhooks.Event{
		Module:  ModuleSetting,
		Event:   EventUpdate,
		Payload: e,
	}
}

func (e EventSettingDelete) Event() webhooks.Event {
	return webhooks.Event{
		Module:  ModuleSetting,
		Event:   EventDelete,
		Payload: e,
	}
}
