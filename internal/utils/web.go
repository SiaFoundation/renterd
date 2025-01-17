package utils

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os/exec"
	"runtime"
	"strings"
	"sync"
	"time"

	"go.sia.tech/jape"
	"go.uber.org/zap"
	"lukechampine.com/frand"
)

var authTokens = &TokenStore{tokens: make(map[string]time.Time)}

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

type TreeMux struct {
	Handler http.Handler
	Sub     map[string]TreeMux
}

func (t TreeMux) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if strings.HasPrefix(req.URL.Path, "/debug/pprof") {
		http.DefaultServeMux.ServeHTTP(w, req)
		return
	}

	for prefix, c := range t.Sub {
		if strings.HasPrefix(req.URL.Path, prefix) {
			req.URL.Path = strings.TrimPrefix(req.URL.Path, prefix)
			c.ServeHTTP(w, req)
			return
		}
	}
	if t.Handler != nil {
		t.Handler.ServeHTTP(w, req)
		return
	}
	http.NotFound(w, req)
}

const (
	authCookieName = "renterd_auth"
	authQueryParam = "apikey"
)

// Auth wraps an http.Handler to force authentication with either a basic auth
// password or a cookie.
func Auth(password string) func(http.Handler) http.Handler {
	return func(h http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			if req.URL.Query().Has(authQueryParam) {
				// token found - overrides cookie
				if !authTokens.Validate(req.URL.Query().Get(authQueryParam)) {
					httpWriteError(w, "API key for authentication found but is either invalid or expired", http.StatusUnauthorized)
				} else {
					h.ServeHTTP(w, req)
				}
			} else if cookie, err := req.Cookie(authCookieName); err == nil {
				// cookie found
				if !authTokens.Validate(cookie.Value) {
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

func AuthHandler(password string) http.Handler {
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
		token := authTokens.GenerateNew(validity)

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
func WorkerAuth(password string, unauthenticatedDownloads bool) func(http.Handler) http.Handler {
	return func(h http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			if unauthenticatedDownloads && req.Method == http.MethodGet && strings.HasPrefix(req.URL.Path, "/object/") {
				h.ServeHTTP(w, req)
			} else {
				Auth(password)(h).ServeHTTP(w, req)
			}
		})
	}
}

func ListenTCP(addr string, logger *zap.Logger) (net.Listener, error) {
	l, err := net.Listen("tcp", addr)
	if IsErr(err, errors.New("no such host")) && strings.Contains(addr, "localhost") {
		// fall back to 127.0.0.1 if 'localhost' doesn't work
		_, port, err := net.SplitHostPort(addr)
		if err != nil {
			return nil, err
		}
		fallbackAddr := fmt.Sprintf("127.0.0.1:%s", port)
		logger.Sugar().Warnf("failed to listen on %s, falling back to %s", addr, fallbackAddr)
		return net.Listen("tcp", fallbackAddr)
	} else if err != nil {
		return nil, err
	}
	return l, nil
}

func OpenBrowser(url string) error {
	switch runtime.GOOS {
	case "linux":
		return exec.Command("xdg-open", url).Start()
	case "windows":
		return exec.Command("rundll32", "url.dll,FileProtocolHandler", url).Start()
	case "darwin":
		return exec.Command("open", url).Start()
	default:
		return fmt.Errorf("unsupported platform %q", runtime.GOOS)
	}
}

func DoRequest(req *http.Request, resp interface{}) (http.Header, int, error) {
	r, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer r.Body.Close()
	defer io.Copy(io.Discard, r.Body)

	if r.StatusCode < 200 || r.StatusCode >= 300 {
		lr := io.LimitReader(r.Body, 1<<20) // 1MiB
		errMsg, _ := io.ReadAll(lr)
		return r.Header, r.StatusCode, fmt.Errorf("HTTP error: %s (status: %d)", string(errMsg), r.StatusCode)
	} else if resp != nil {
		return r.Header, r.StatusCode, json.NewDecoder(r.Body).Decode(resp)
	}
	return r.Header, r.StatusCode, nil
}

func httpWriteError(w http.ResponseWriter, msg string, statusCode int) {
	w.Header().Set("Content-Type", "text/plain")
	w.WriteHeader(statusCode)
	w.Write([]byte(msg))
}
