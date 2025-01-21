package utils

import (
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

	"go.uber.org/zap"
)

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
