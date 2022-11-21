package main

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestTimeoutMiddleware(t *testing.T) {
	ms := time.Millisecond
	timeout := timeoutMiddleware(ms*50, ms, ms*100, paramTimeout)
	handler := timeout(http.HandlerFunc(testHandler))

	cases := []struct {
		name    string
		timeout time.Duration
		request time.Duration // duration of sleep in handler
		status  int
		body    string
	}{
		{
			"below min duration",
			time.Nanosecond,
			0,
			http.StatusBadRequest,
			fmt.Sprintf("'%s' falls below minimum duration of 1ms", paramTimeout),
		},
		{
			"above max duration",
			time.Minute,
			0,
			http.StatusBadRequest,
			fmt.Sprintf("'%s' exceeds maximum duration of 100ms", paramTimeout),
		},
		{
			"zero timeout",
			0,
			0,
			http.StatusOK,
			"OK",
		},
		{
			"no timeout", // passing neg value ensures query param is not set
			-1,
			0,
			http.StatusOK,
			"OK",
		},
		{
			"request timeout, exceeds default timeout",
			0, // uses default of 50ms
			time.Second,
			http.StatusRequestTimeout,
			"request timed out",
		},
		{
			"request timeout, exceeds provided timeout",
			time.Millisecond * 20,
			time.Millisecond * 200,
			http.StatusRequestTimeout,
			"request timed out",
		},
		{
			"no timeout, custom timeout provided",
			0, // uses default of 50ms
			time.Millisecond * 10,
			http.StatusOK,
			"OK",
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			// build request url
			query := url.Values{}
			query.Set("request", tt.request.String())
			if tt.timeout >= 0 {
				query.Set(paramTimeout, tt.timeout.String())
			}
			url := fmt.Sprintf("/foo?%s", query.Encode())

			// process request
			req := httptest.NewRequest(http.MethodGet, url, nil)
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, req)

			// assert response
			res := w.Result()
			defer res.Body.Close()
			if want, got := tt.status, res.StatusCode; want != got {
				t.Fatalf("unexpected status code, %v != %v", got, want)
			}

			data, err := ioutil.ReadAll(res.Body)
			if err != nil {
				t.Fatal("failed to read body", err)
			}
			if want, got := tt.body, strings.TrimSuffix(string(data), "\n"); want != got {
				t.Fatalf("unexpected response body: '%v' != '%v'", got, want)
			}
		})
	}
}

func testHandler(w http.ResponseWriter, r *http.Request) {
	dur, err := parseDuration(r, "request")
	if err != nil {
		panic(err) // developer error
	}

	select {
	case <-time.After(dur):
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, http.StatusText(http.StatusOK))
	case <-r.Context().Done():
		w.WriteHeader(http.StatusRequestTimeout)
		fmt.Fprintf(w, "request timed out")
	}
}
