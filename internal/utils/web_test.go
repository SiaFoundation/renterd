package utils

import (
	"net/http"
	"net/http/httptest"
	_ "net/http/pprof"
	"testing"
)

func TestAuth(t *testing.T) {
	// test server with authenticated route that returns '200 OK'
	srv := httptest.NewServer(Auth("password")(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})))
	defer srv.Close()

	client := srv.Client()

	// helper to perform request and assert status code
	assertResponse := func(statusCode int, modifyReq func(*http.Request)) {
		t.Helper()

		// prepare request
		req, err := http.NewRequest("GET", srv.URL, http.NoBody)
		if err != nil {
			t.Fatal(err)
		}
		modifyReq(req)

		// assert status code
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		} else if resp.StatusCode != statusCode {
			t.Fatalf("expected status code %d, got %d", statusCode, resp.StatusCode)
		}
	}

	// unauthenticated
	assertResponse(http.StatusUnauthorized, func(req *http.Request) {})

	// authenticate using cookie
	assertResponse(http.StatusOK, func(req *http.Request) {
		req.AddCookie(&http.Cookie{
			Name:  "renterd_auth",
			Value: "password",
		})
	})

	// authenticate using basic auth
	assertResponse(http.StatusOK, func(req *http.Request) {
		req.SetBasicAuth("", "password")
	})
}
