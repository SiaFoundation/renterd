package events

import (
	"context"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/webhooks"
	"go.uber.org/zap"
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
		logger      *zap.SugaredLogger
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
		Renewal   api.ContractMetadata `json:"renewal"`
		Timestamp time.Time            `json:"timestamp"`
	}

	EventContractSetUpdate struct {
		Name        string                 `json:"name"`
		ContractIDs []types.FileContractID `json:"contractIDs"`
		Timestamp   time.Time              `json:"timestamp"`
	}

	EventConsensusUpdate struct {
		api.ConsensusState
		TransactionFee types.Currency `json:"transactionFee"`
		Timestamp      time.Time      `json:"timestamp"`
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

func NewBroadcaster(b webhooks.Broadcaster, l *zap.SugaredLogger) *BroadCaster {
	return &BroadCaster{
		broadcaster: b,
		logger:      l,
	}
}

// BroadcastEvent will pass the given event to the underlying broadcaster who
// will in turn enqueue it so it'll eventually get broadcasted to all registered
// webhooks that match. Enqueueing is quick but it is advised to call this
// method in a goroutine to ensure the caller is not blocked what so ever.
func (s *BroadCaster) BroadcastEvent(event Event) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	if err := s.broadcaster.BroadcastAction(ctx, webhooks.Event{
		Module:  WebhookEventsModule,
		Event:   event.Event(),
		Payload: event,
	}); err != nil {
		s.logger.Errorw("failed to broadcast event", "event", event.Event(), "error", err)
	}
	cancel()
}
