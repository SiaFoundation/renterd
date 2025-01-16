package utils

import (
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

	generateCookie := func(host string) *http.Cookie {
		t.Helper()

		// fake a server to get the auth token from
		authSrv := httptest.NewServer(AuthHandler(pw))
		defer authSrv.Close()
		req, err := http.NewRequest("POST", authSrv.URL+"?validity=1000", http.NoBody)
		if err != nil {
			t.Fatal(err)
		}
		req.SetBasicAuth("", pw)
		req.Host = host
		res, err := authSrv.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		} else if res.StatusCode != http.StatusNoContent {
			t.Fatal("expected status code 204, got", res.StatusCode)
		}
		for _, cookie := range res.Cookies() {
			if cookie.Name == authCookieName {
				return cookie
			}
		}
		t.Fatal("cookie not found")
		return nil
	}

	// unauthenticated
	assertResponse(http.StatusUnauthorized, func(req *http.Request) {})

	// authenticate using basic auth
	assertResponse(http.StatusOK, func(req *http.Request) {
		req.SetBasicAuth("", "password")
	})

	// authenticate using cookie
	cookie := generateCookie("")
	assertResponse(http.StatusOK, func(req *http.Request) {
		req.AddCookie(cookie)
	})

	// make sure token expires
	time.Sleep(time.Duration(cookie.MaxAge) * time.Second)
	assertResponse(http.StatusUnauthorized, func(req *http.Request) {
		req.AddCookie(cookie)
	})

	// authenticate using cookie and custom domain set
	cookie = generateCookie("foo.com") // without port
	if cookie.Domain != "foo.com" {
		t.Fatalf("expected domain to be 'foo.com', got '%s'", cookie.Domain)
	}
	assertResponse(http.StatusOK, func(req *http.Request) {
		req.AddCookie(cookie)
	})
	cookie = generateCookie("foo.com:80") // with port
	if cookie.Domain != "foo.com" {
		t.Fatalf("expected domain to be 'foo.com', got '%s'", cookie.Domain)
	}
	assertResponse(http.StatusOK, func(req *http.Request) {
		req.AddCookie(cookie)
	})
}
