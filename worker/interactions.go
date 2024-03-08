package worker

import (
	"go.sia.tech/renterd/hostdb"
)

type (
	HostInteractionRecorder interface {
		RecordHostScan(...hostdb.HostScan)
		RecordPriceTableUpdate(...hostdb.PriceTableUpdate)
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
