package bus

import (
	"context"
	"fmt"
	"net/http"

	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/internal/utils"
)

type (
	Explorer struct {
		url string
	}
)

// NewExplorer returns a new Explorer.
func NewExplorer(url string) *Explorer {
	return &Explorer{
		url: url,
	}
}

// BaseURL returns the base URL of the Explorer.
func (e *Explorer) BaseURL() string {
	return e.url
}

// Enabled returns true if the explorer is enabled.
func (e *Explorer) Enabled() bool {
	return e.url != ""
}

// SiacoinExchangeRate returns the exchange rate for the given currency.
func (e *Explorer) SiacoinExchangeRate(ctx context.Context, currency string) (rate float64, err error) {
	// return early if the explorer is disabled
	if !e.Enabled() {
		return 0, api.ErrExplorerDisabled
	}

	// create request
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("%s/exchange-rate/siacoin/%s", e.url, currency), http.NoBody)
	if err != nil {
		return 0, fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Accept", "application/json")

	_, _, err = utils.DoRequest(req, &rate)
	return
}
