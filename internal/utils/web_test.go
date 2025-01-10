package utils

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	_ "net/http/pprof"
	"testing"
	"time"
)

func TestAuth(t *testing.T) {
	// test server with authenticated route that returns '200 OK'
	pw := "password"
	srv := httptest.NewServer(Auth(pw)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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

	generateToken := func() string {
		t.Helper()

		// fake a server to get the auth token from
		authSrv := httptest.NewServer(AuthHandler(pw))
		defer authSrv.Close()
		req, err := http.NewRequest("POST", authSrv.URL+"?validity=1000", http.NoBody)
		if err != nil {
			t.Fatal(err)
		}
		req.SetBasicAuth("", pw)
		res, err := authSrv.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		} else if res.StatusCode != http.StatusOK {
			t.Fatal("expected status code 200, got", res.StatusCode)
		}
		b, err := io.ReadAll(res.Body)
		if err != nil {
			t.Fatal(err)
		}
		var resp struct {
			Token string `json:"token"`
		}
		if err := json.Unmarshal(b, &resp); err != nil {
			t.Fatal(err)
		}
		return resp.Token
	}

	// unauthenticated
	assertResponse(http.StatusUnauthorized, func(req *http.Request) {})

	// authenticate using basic auth
	assertResponse(http.StatusOK, func(req *http.Request) {
		req.SetBasicAuth("", "password")
	})

	// authenticate using cookie
	token := generateToken()
	assertResponse(http.StatusOK, func(req *http.Request) {
		req.AddCookie(&http.Cookie{
			Name:  authCookieName,
			Value: token,
		})
	})

	// make sure token expires
	time.Sleep(time.Second)
	assertResponse(http.StatusUnauthorized, func(req *http.Request) {
		req.AddCookie(&http.Cookie{
			Name:  authCookieName,
			Value: token,
		})
	})
}
