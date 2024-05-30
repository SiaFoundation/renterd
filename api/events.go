package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/webhooks"
)

const (
	ModuleConsensus   = "consensus"
	ModuleContract    = "contract"
	ModuleContractSet = "contract_set"
	ModuleSetting     = "setting"

	EventUpdate  = "update"
	EventDelete  = "delete"
	EventArchive = "archive"
	EventRenew   = "renew"
)

var (
	ErrUnknownEvent = errors.New("unknown event")
)

type (
	Event interface {
		Event() webhooks.Event
	}

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
		Renewal   ContractMetadata `json:"renewal"`
		Timestamp time.Time        `json:"timestamp"`
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

func NewEventWebhook(url string, e Event) webhooks.Webhook {
	return webhooks.Webhook{
		Module: e.Event().Module,
		Event:  e.Event().Event,
		URL:    url,
	}
}

func ParseEvent(event webhooks.Event) (interface{}, error) {
	bytes, err := json.Marshal(event.Payload)
	if err != nil {
		return nil, err
	}
	switch event.Module {
	case ModuleContract:
		if event.Event == EventArchive {
			var e EventContractArchive
			if err := json.Unmarshal(bytes, &e); err != nil {
				return nil, err
			}
			return e, nil
		} else if event.Event == EventRenew {
			var e EventContractRenew
			if err := json.Unmarshal(bytes, &e); err != nil {
				return nil, err
			}
			return e, nil
		}
	case ModuleContractSet:
		if event.Event == EventUpdate {
			var e EventContractSetUpdate
			if err := json.Unmarshal(bytes, &e); err != nil {
				return nil, err
			}
			return e, nil
		}
	case ModuleConsensus:
		if event.Event == EventUpdate {
			var e EventConsensusUpdate
			if err := json.Unmarshal(bytes, &e); err != nil {
				return nil, err
			}
			return e, nil
		}
	case ModuleSetting:
		if event.Event == EventUpdate {
			var e EventSettingUpdate
			if err := json.Unmarshal(bytes, &e); err != nil {
				return nil, err
			}
			return e, nil
		} else if event.Event == EventDelete {
			var e EventSettingDelete
			if err := json.Unmarshal(bytes, &e); err != nil {
				return nil, err
			}
			return e, nil
		}
	}
	return nil, fmt.Errorf("%w: module %s event %s", ErrUnknownEvent, event.Module, event.Event)
}
