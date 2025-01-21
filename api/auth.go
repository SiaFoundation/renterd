package api

import (
	"encoding/hex"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"go.sia.tech/jape"
	"lukechampine.com/frand"
)

const (
	authCookieName = "renterd_auth"
	authQueryParam = "apikey"
)

type TokenStore struct {
	mu     sync.Mutex
	tokens map[string]time.Time
}

func (s *TokenStore) GenerateNew(validFor time.Duration) string {
	s.mu.Lock()
	defer s.mu.Unlock()

	token := hex.EncodeToString(frand.Bytes(16))
	s.tokens[token] = time.Now().Add(validFor)
	return token
}

func (s *TokenStore) Validate(token string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	// prune tokens while also checking if the token is valid
	validToken := false
	for t, validUntil := range s.tokens {
		if validUntil.Before(time.Now()) {
			delete(s.tokens, t)
			continue
		}
		validToken = validToken || token == t
	}
	return validToken
}

func NewTokenStore() *TokenStore {
	return &TokenStore{tokens: make(map[string]time.Time)}
}

func httpWriteError(w http.ResponseWriter, msg string, statusCode int) {
	w.Header().Set("Content-Type", "text/plain")
	w.WriteHeader(statusCode)
	w.Write([]byte(msg))
}

// Auth wraps an http.Handler to force authentication with either a basic auth
// password or a cookie.
func Auth(tokens *TokenStore, password string) func(http.Handler) http.Handler {
	return func(h http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			if req.URL.Query().Has(authQueryParam) {
				// token found - overrides cookie
				if !tokens.Validate(req.URL.Query().Get(authQueryParam)) {
					httpWriteError(w, "API key for authentication found but is either invalid or expired", http.StatusUnauthorized)
				} else {
					h.ServeHTTP(w, req)
				}
			} else if cookie, err := req.Cookie(authCookieName); err == nil {
				// cookie found
				if !tokens.Validate(cookie.Value) {
					// do our best to tell the browser to delete the cookie
					cookie.Value = ""
					cookie.MaxAge = -1
					cookie.Expires = time.Now().Add(-time.Hour).UTC()
					http.SetCookie(w, cookie)
					httpWriteError(w, "Cookie for authentication found but is either invalid or expired", http.StatusUnauthorized)
				} else {
					h.ServeHTTP(w, req)
				}
			} else {
				// try basic auth
				jape.BasicAuth(password)(h).ServeHTTP(w, req)
			}
		})
	}
}

func AuthHandler(tokens *TokenStore, password string) http.Handler {
	// NOTE: we use BasicAuth instead of Auth since only basic auth should be
	// allowed to create a new token
	return jape.BasicAuth(password)(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return // only POST is allowed
		}

		// parse validity
		validityMS := req.FormValue("validity")
		if validityMS == "" {
			httpWriteError(w, "'validity' parameter is missing", http.StatusBadRequest)
			return
		}
		var validity time.Duration
		if _, err := fmt.Sscan(validityMS, &validity); err != nil {
			httpWriteError(w, "failed to parse validity", http.StatusBadRequest)
			return
		}
		validity *= time.Millisecond

		// generate token
		token := tokens.GenerateNew(validity)

		// get host domain from header
		var host string
		if fwdHost := req.Header.Get("X-Forwarded-Host"); fwdHost != "" {
			host = fwdHost
			// handle header containing port
			if strings.Contains(host, ":") {
				var err error
				host, _, err = net.SplitHostPort(host)
				if err != nil {
					httpWriteError(w, "failed to split host name from its port", http.StatusBadRequest)
					return
				}
			}
		}

		// set cookie
		http.SetCookie(w, &http.Cookie{
			Domain:   host,
			Expires:  time.Now().Add(validity).UTC(),
			HttpOnly: true,
			MaxAge:   int(validity / time.Second),
			Name:     authCookieName,
			Path:     "/",
			SameSite: http.SameSiteStrictMode,
			Secure:   true,
			Value:    token,
		})

		// send token
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(fmt.Sprintf(`{"token": %q}`, token)))
	}))
}

// WorkerAuth is a wrapper for Auth that allows unauthenticated downloads if
// 'unauthenticatedDownloads' is true.
func WorkerAuth(tokens *TokenStore, password string, unauthenticatedDownloads bool) func(http.Handler) http.Handler {
	return func(h http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			if unauthenticatedDownloads && req.Method == http.MethodGet && strings.HasPrefix(req.URL.Path, "/object/") {
				h.ServeHTTP(w, req)
			} else {
				Auth(tokens, password)(h).ServeHTTP(w, req)
			}
		})
	}
}
