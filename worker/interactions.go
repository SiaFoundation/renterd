package worker

import (
	"context"
	"sync"
	"time"

	"go.sia.tech/renterd/hostdb"
	"go.uber.org/zap"
)

type (
	HostInteractionRecorder interface {
		RecordHostScan(...hostdb.HostScan)
		RecordPriceTableUpdate(...hostdb.PriceTableUpdate)
		Stop(context.Context)
	}

	hostInteractionRecorder struct {
		flushInterval time.Duration

		bus    Bus
		logger *zap.SugaredLogger

		mu                sync.Mutex
		hostScans         []hostdb.HostScan
		priceTableUpdates []hostdb.PriceTableUpdate

		flushCtx   context.Context
		flushTimer *time.Timer
	}
)

func isSuccessfulInteraction(err error) bool {
	// No error always means success.
	if err == nil {
		return true
	}
	// List of errors that are considered successful interactions.
	if isInsufficientFunds(err) {
		return true
	}
	if isBalanceInsufficient(err) {
		return true
	}
	return false
}
