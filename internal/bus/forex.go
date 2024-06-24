package bus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
)

type (
	client struct {
		url string
	}
)

func NewForexClient(url string) *client {
	return &client{url: url}
}

func (f *client) SiacoinExchangeRate(ctx context.Context, currency string) (rate float64, err error) {
	// create request
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("%s/%s", f.url, currency), http.NoBody)
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
