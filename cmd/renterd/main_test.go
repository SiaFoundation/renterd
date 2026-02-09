package main_test

import (
	"bytes"
	"database/sql"
	"encoding/hex"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"go.sia.tech/coreutils/wallet"
	"lukechampine.com/frand"
)

const (
	apiPassword = "test"
)

var (
	binaryPath string
)

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "renterd-testmain-*")
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create temp dir: %v\n", err)
		os.Exit(1)
	}

	binaryPath = filepath.Join(dir, "renterd")
	cmd := exec.Command("go", "build", "-o", binaryPath, ".")
	if out, err := cmd.CombinedOutput(); err != nil {
		os.RemoveAll(dir)
		fmt.Fprintf(os.Stderr, "failed to build renterd: %s\n%v\n", out, err)
		os.Exit(1)
	}

	code := m.Run()
	os.RemoveAll(dir)
	os.Exit(code)
}

func TestSmoke(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	t.Run("regular", func(t *testing.T) {
		dir := t.TempDir()
		apiPort := randomPort(t)
		gatewayPort := randomPort(t)

		runSmokeTest(t,
			[]string{
				"-dir", dir,
				"-http", fmt.Sprintf("127.0.0.1:%d", apiPort),
				"-bus.gatewayAddr", fmt.Sprintf("127.0.0.1:%d", gatewayPort),
				"-bus.bootstrap=false",
				"-autopilot.enabled=false",
				"-s3.enabled=false",
				"-network", "zen",
				"-env",
				"-openui=false",
			},
			prepareTestEnv(t, wallet.NewSeedPhrase(), "normal"),
			apiPort,
			30*time.Second,
		)
	})

	t.Run("instant sync", func(t *testing.T) {
		dir := t.TempDir()
		apiPort := randomPort(t)
		gatewayPort := randomPort(t)

		runSmokeTest(t,
			[]string{
				"-dir", dir,
				"-http", fmt.Sprintf("127.0.0.1:%d", apiPort),
				"-bus.gatewayAddr", fmt.Sprintf("127.0.0.1:%d", gatewayPort),
				"-autopilot.enabled=false",
				"-s3.enabled=false",
				"-network", "zen",
				"-env",
				"-openui=false",
				"-instant",
			},
			prepareTestEnv(t, wallet.NewSeedPhrase(), "instant"),
			apiPort,
			3*time.Minute,
		)
	})
}

// prepareTestEnv prepares the environment variables for a smoke test
func prepareTestEnv(t *testing.T, seed, suffix string) []string {
	t.Helper()

	// copy existing env
	var env []string
	for _, e := range os.Environ() {
		if !strings.HasPrefix(e, "RENTERD_") {
			env = append(env, e)
		}
	}

	// set renterd env vars
	env = append(env,
		"RENTERD_SEED="+seed,
		"RENTERD_API_PASSWORD="+apiPassword,
	)

	// if we are running on MySQL, create test database
	if uri := os.Getenv("RENTERD_DB_URI"); uri != "" {
		user := os.Getenv("RENTERD_DB_USER")
		password := os.Getenv("RENTERD_DB_PASSWORD")
		dbID := hex.EncodeToString(frand.Bytes(4))
		dbName := "renterd_smoke_" + suffix + "_" + dbID
		metricsName := "renterd_smoke_" + suffix + "_" + dbID + "_metrics"

		// open connection
		db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s)/?charset=utf8mb4&parseTime=True&loc=Local&multiStatements=true", user, password, uri))
		if err != nil {
			t.Fatal(err)
		}

		// prepare databases
		for _, name := range []string{dbName, metricsName} {
			if _, err := db.Exec("CREATE DATABASE IF NOT EXISTS `" + name + "`"); err != nil {
				db.Close()
				t.Fatal(err)
			}
		}

		// cleanup after test
		t.Cleanup(func() {
			db.Exec("DROP DATABASE IF EXISTS `" + dbName + "`")
			db.Exec("DROP DATABASE IF EXISTS `" + metricsName + "`")
			db.Close()
		})

		env = append(env,
			"RENTERD_DB_URI="+uri,
			"RENTERD_DB_USER="+user,
			"RENTERD_DB_PASSWORD="+password,
			"RENTERD_DB_NAME="+dbName,
			"RENTERD_DB_METRICS_NAME="+metricsName,
		)
	}

	return env
}

// runSmokeTest runs the renterd binary with given args and environment. It will
// wait for the API to become responsive and then attempt a graceful shutdown.
func runSmokeTest(t *testing.T, args, env []string, port int, timeout time.Duration) {
	t.Helper()

	// capture output
	var output bytes.Buffer
	cmd := exec.Command(binaryPath, args...)
	cmd.Env = env
	cmd.Stdout = &output
	cmd.Stderr = &output

	// start renterd
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer cmd.Process.Kill()

	// wait for process in background thread
	doneCh := make(chan struct{})
	var err error
	go func() {
		err = cmd.Wait()
		close(doneCh)
	}()

	// wait for the bus to come online in background thread
	successCh := make(chan struct{})
	go func() {
		ticker := time.NewTicker(250 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-doneCh:
				return
			case <-ticker.C:
			}

			req, err := http.NewRequest("GET", fmt.Sprintf("http://127.0.0.1:%d/api/bus/state", port), http.NoBody)
			if err != nil {
				t.Error(err)
				continue
			}
			req.SetBasicAuth("", apiPassword)

			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				continue // ignored
			}
			resp.Body.Close()

			if resp.StatusCode == http.StatusOK {
				close(successCh)
				return
			}
		}
	}()

	// wait for API ready, process exit, or timeout
	select {
	case <-successCh:
	case <-doneCh:
		t.Fatalf("renterd exited before bus was ready, err: %v, output: %s", err, output.String())
	case <-time.After(timeout):
		cmd.Process.Kill()
		<-doneCh // ensure process is done before reading output
		t.Fatalf("API not ready after %v, process output: %s", timeout, output.String())
	}

	// send shutdown signal
	time.Sleep(time.Second) // allow signal handler registration
	cmd.Process.Signal(os.Interrupt)
	select {
	case <-doneCh:
		if err != nil {
			t.Fatalf("process exited with error during shutdown: %v, output: %s", err, output.String())
		}
	case <-time.After(30 * time.Second):
		cmd.Process.Kill()
		<-doneCh // ensure process is done before reading output
		t.Fatalf("graceful shutdown took longer than 30s, output: %s", output.String())
	}
}

func randomPort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()
	return port
}
