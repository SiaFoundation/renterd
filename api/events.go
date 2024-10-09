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
	ModuleACL         = "acl"
	ModuleConsensus   = "consensus"
	ModuleContract    = "contract"
	ModuleContractSet = "contract_set"
	ModuleHost        = "host"
	ModuleSetting     = "setting"

	EventAdd     = "add"
	EventUpdate  = "update"
	EventDelete  = "delete"
	EventArchive = "archive"
	EventRenew   = "renew"
)

var (
	ErrUnknownEvent = errors.New("unknown event")
)

type (
	EventACLUpdate struct {
		Allowlist []types.PublicKey `json:"allowlist"`
		Blocklist []string          `json:"blocklist"`
		Timestamp time.Time         `json:"timestamp"`
	}

	EventConsensusUpdate struct {
		ConsensusState
		TransactionFee types.Currency `json:"transactionFee"`
		Timestamp      time.Time      `json:"timestamp"`
	}

	EventContractAdd struct {
		Added     ContractMetadata `json:"added"`
		Timestamp time.Time        `json:"timestamp"`
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

	EventHostUpdate struct {
		HostKey   types.PublicKey `json:"hostKey"`
		NetAddr   string          `json:"netAddr"`
		Timestamp time.Time       `json:"timestamp"`
	}

	EventContractSetUpdate struct {
		Name      string                 `json:"name"`
		ToAdd     []types.FileContractID `json:"toAdd"`
		ToRemove  []types.FileContractID `json:"toRemove"`
		Timestamp time.Time              `json:"timestamp"`
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

var (
	WebhookACLUpdate = func(url string, headers map[string]string) webhooks.Webhook {
		return webhooks.Webhook{
			Event:   EventUpdate,
			Headers: headers,
			Module:  ModuleACL,
			URL:     url,
		}
	}

	WebhookConsensusUpdate = func(url string, headers map[string]string) webhooks.Webhook {
		return webhooks.Webhook{
			Event:   EventUpdate,
			Headers: headers,
			Module:  ModuleConsensus,
			URL:     url,
		}
	}

	WebhookContractAdd = func(url string, headers map[string]string) webhooks.Webhook {
		return webhooks.Webhook{
			Event:   EventAdd,
			Headers: headers,
			Module:  ModuleContract,
			URL:     url,
		}
	}

	WebhookContractArchive = func(url string, headers map[string]string) webhooks.Webhook {
		return webhooks.Webhook{
			Event:   EventArchive,
			Headers: headers,
			Module:  ModuleContract,
			URL:     url,
		}
	}

	WebhookContractRenew = func(url string, headers map[string]string) webhooks.Webhook {
		return webhooks.Webhook{
			Event:   EventRenew,
			Headers: headers,
			Module:  ModuleContract,
			URL:     url,
		}
	}

	WebhookContractSetUpdate = func(url string, headers map[string]string) webhooks.Webhook {
		return webhooks.Webhook{
			Event:   EventUpdate,
			Headers: headers,
			Module:  ModuleContractSet,
			URL:     url,
		}
	}

	WebhookHostUpdate = func(url string, headers map[string]string) webhooks.Webhook {
		return webhooks.Webhook{
			Event:   EventUpdate,
			Headers: headers,
			Module:  ModuleHost,
			URL:     url,
		}
	}

	WebhookSettingUpdate = func(url string, headers map[string]string) webhooks.Webhook {
		return webhooks.Webhook{
			Event:   EventUpdate,
			Headers: headers,
			Module:  ModuleSetting,
			URL:     url,
		}
	}

	WebhookSettingDelete = func(url string, headers map[string]string) webhooks.Webhook {
		return webhooks.Webhook{
			Event:   EventDelete,
			Headers: headers,
			Module:  ModuleSetting,
			URL:     url,
		}
	}
)

func ParseEventWebhook(event webhooks.Event) (interface{}, error) {
	bytes, err := json.Marshal(event.Payload)
	if err != nil {
		return nil, err
	}
	switch event.Module {
	case ModuleACL:
		if event.Event == EventUpdate {
			var e EventACLUpdate
			if err := json.Unmarshal(bytes, &e); err != nil {
				return nil, err
			}
			return e, nil
		}
	case ModuleContract:
		switch event.Event {
		case EventAdd:
			var e EventContractAdd
			if err := json.Unmarshal(bytes, &e); err != nil {
				return nil, err
			}
			return e, nil
		case EventArchive:
			var e EventContractArchive
			if err := json.Unmarshal(bytes, &e); err != nil {
				return nil, err
			}
			return e, nil
		case EventRenew:
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
	case ModuleHost:
		if event.Event == EventUpdate {
			var e EventHostUpdate
			if err := json.Unmarshal(bytes, &e); err != nil {
				return nil, err
			}
			return e, nil
		}
	case ModuleSetting:
		switch event.Event {
		case EventUpdate:
			var e EventSettingUpdate
			if err := json.Unmarshal(bytes, &e); err != nil {
				return nil, err
			}
			return e, nil
		case EventDelete:
			var e EventSettingDelete
			if err := json.Unmarshal(bytes, &e); err != nil {
				return nil, err
			}
			return e, nil
		}
	}
	return nil, fmt.Errorf("%w: module %s event %s", ErrUnknownEvent, event.Module, event.Event)
}
