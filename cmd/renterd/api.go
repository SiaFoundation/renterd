package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"time"
)

func timeoutMiddleware(def, min, max time.Duration, param string) func(http.Handler) http.Handler {
	return func(h http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			if timeout, err := parseTimeout(req, def, min, max, param); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			} else if timeout > 0 {
				ctx, cancel := context.WithTimeout(req.Context(), timeout)
				defer cancel()
				req = req.WithContext(ctx)
			}
			h.ServeHTTP(w, req)
		})
	}
}

func (cfg workerConfig) timeout() func(http.Handler) http.Handler {
	if !cfg.reqTimeoutEnabled {
		panic("timeout disabled") // developer error
	}
	return timeoutMiddleware(
		cfg.reqTimeoutDef,
		cfg.reqTimeoutMin,
		cfg.reqTimeoutMax,
		cfg.reqTimeoutParam,
	)
}

func parseTimeout(req *http.Request, def, min, max time.Duration, param string) (time.Duration, error) {
	timeout, err := parseDuration(req, param)
	if err != nil {
		return 0, err
	}
	if timeout == 0 {
		return def, nil
	}
	if max > 0 && timeout > max {
		return 0, fmt.Errorf("'%s' exceeds maximum duration of %v", param, max)
	}
	if min > 0 && timeout < min {
		return 0, fmt.Errorf("'%s' falls below minimum duration of %v", param, min)
	}
	return timeout, nil
}

func parseDuration(req *http.Request, param string) (time.Duration, error) {
	queryForm, err := url.ParseQuery(req.URL.RawQuery)
	if err != nil {
		return 0, errors.New(http.StatusText(http.StatusBadRequest))
	}

	durationStr := queryForm.Get(param)
	if durationStr == "" {
		return 0, nil
	}

	dur, err := time.ParseDuration(durationStr)
	if err != nil {
		return 0, fmt.Errorf("unable to parse '%s', err %w", param, err)
	}

	return dur, nil
}
