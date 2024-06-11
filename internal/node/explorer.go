package node

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"
)

var client = &http.Client{Timeout: 30 * time.Second}

type (
	Explorer interface {
		SiacoinExchangeRate(ctx context.Context, currency string) (rate float64, err error)
	}

	explorer struct {
		url string
	}
)

// NewExplorer returns a new Explorer client.
func NewExplorer(url string) (Explorer, error) {
	e := &explorer{url: url}

	// apply sane timeout
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// fetch USD exchange rate by means of testing the explorer
	_, err := e.SiacoinExchangeRate(ctx, "usd")
	if err != nil {
		return nil, fmt.Errorf("failed to fetch exchange rate: %w", err)
	}

	return e, nil
}

// BaseURL returns the base URL of the Explorer.
func (e *explorer) BaseURL() string {
	return e.url
}

// SiacoinExchangeRate returns the exchange rate for the given currency.
func (e *explorer) SiacoinExchangeRate(ctx context.Context, currency string) (rate float64, err error) {
	err = makeRequest(ctx, http.MethodGet, fmt.Sprintf("%s/exchange-rate/siacoin/%s", e.url, currency), nil, &rate)
	return
}

func drainAndClose(r io.ReadCloser) {
	io.Copy(io.Discard, io.LimitReader(r, 1024*1024))
	r.Close()
}

func makeRequest(ctx context.Context, method, url string, requestBody, response any) error {
	var body io.Reader
	if requestBody != nil {
		js, _ := json.Marshal(requestBody)
		body = bytes.NewReader(js)
	}

	req, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send request: %w", err)
	}
	defer drainAndClose(resp.Body)

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var errorMessage string
		if err := json.NewDecoder(io.LimitReader(resp.Body, 1024)).Decode(&errorMessage); err != nil {
			return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
		}
		return errors.New(errorMessage)
	}

	if response == nil {
		return nil
	} else if err := json.NewDecoder(resp.Body).Decode(response); err != nil {
		return fmt.Errorf("failed to decode response: %w", err)
	}
	return nil
}
