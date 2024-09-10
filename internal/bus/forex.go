package bus

import (
	"context"
	"fmt"
	"net/http"

	"go.sia.tech/renterd/internal/utils"
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

	_, _, err = utils.DoRequest(req, &rate)
	return
}
