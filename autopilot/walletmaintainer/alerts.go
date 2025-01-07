package walletmaintainer

import (
	"fmt"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/alerts"
)

var (
	alertLowBalanceID = alerts.RandomAlertID() // constant until restarted
)

func newAccountLowBalanceAlert(address types.Address, balance, initialFunding types.Currency) alerts.Alert {
	return alerts.Alert{
		ID:       alertLowBalanceID,
		Severity: alerts.SeverityWarning,
		Message:  "Wallet is low on funds",
		Data: map[string]any{
			"address":        address,
			"balance":        balance,
			"initialFunding": initialFunding,
			"hint":           fmt.Sprintf("The current wallet balance of %v is less than the configured initialFunding of %v times the number of contracts to form. Ideally, a wallet holds at least enough funds to make sure it can form a fresh set of contracts.", balance, initialFunding),
		},
		Timestamp: time.Now(),
	}
}
