package bus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	"go.sia.tech/renterd/api"
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

	// create http client
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	// check status code
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var errorMessage string
		if err := json.NewDecoder(io.LimitReader(resp.Body, 1024)).Decode(&errorMessage); err != nil {
			return 0, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
		}
		return 0, errors.New(errorMessage)
	}

	// decode exchange rate
	if err := json.NewDecoder(resp.Body).Decode(&rate); err != nil {
		return 0, fmt.Errorf("failed to decode response: %w", err)
	}
	return
}