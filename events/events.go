package events

import (
	"context"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/webhooks"
)

const (
	WebhookEventsModule = "events"

	WebhookEventConsensusUpdate   = "consensus_update"
	WebhookEventContractArchived  = "contract_archived"
	WebhookEventContractRenewal   = "contract_renewal"
	WebhookEventContractSetUpdate = "contract_set_update"
	WebhookEventSettingUpdate     = "setting_update"
)

type (
	// A BroadCaster broadcasts events using webhooks. This tiny wrapper
	// hardcodes the webhook 'module' and validates we only broadcasts events.
	BroadCaster struct {
		broadcaster webhooks.Broadcaster
	}

	Event interface{ Event() string }

	EventSettingUpdate struct {
		Key       string      `json:"key"`
		Update    interface{} `json:"update,omitempty"`
		Timestamp time.Time   `json:"timestamp"`
	}

	EventContractArchived struct {
		ContractID types.FileContractID `json:"contractID"`
		Reason     string               `json:"reason"`
		Timestamp  time.Time            `json:"timestamp"`
	}

	EventContractRenewal struct {
		ContractID    types.FileContractID `json:"contractID"`
		RenewedFromID types.FileContractID `json:"renewedFromID"`
		Timestamp     time.Time            `json:"timestamp"`
	}

	EventContractSetUpdate struct {
		Name      string                 `json:"name"`
		Contracts []api.ContractMetadata `json:"contracts"`
		Timestamp time.Time              `json:"timestamp"`
	}

	EventConsensusUpdate struct {
		api.ConsensusState
		TransactionFee types.Currency `json:"transactionFee"`
	}
)

func (e EventConsensusUpdate) Event() string   { return WebhookEventConsensusUpdate }
func (e EventContractArchived) Event() string  { return WebhookEventContractArchived }
func (e EventContractRenewal) Event() string   { return WebhookEventContractRenewal }
func (e EventContractSetUpdate) Event() string { return WebhookEventContractSetUpdate }
func (e EventSettingUpdate) Event() string     { return WebhookEventSettingUpdate }

func NewEventWebhook(url string, event string) webhooks.Webhook {
	return webhooks.Webhook{
		Module: WebhookEventsModule,
		Event:  event,
		URL:    url,
	}
}

func NewBroadcaster(broadcaster webhooks.Broadcaster) *BroadCaster {
	return &BroadCaster{
		broadcaster: broadcaster,
	}
}

func (s *BroadCaster) BroadcastEvent(ctx context.Context, event Event) error {
	return s.broadcaster.BroadcastAction(ctx, webhooks.Event{
		Module:  WebhookEventsModule,
		Event:   event.Event(),
		Payload: event,
	})
}
