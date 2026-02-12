package main

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
	"runtime"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"go.sia.tech/coreutils/wallet"
	"go.sia.tech/renterd/v2/config"
	"gopkg.in/yaml.v3"
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

	binaryName := "renterd"
	if runtime.GOOS == "windows" {
		binaryName += ".exe"
	}
	binaryPath = filepath.Join(dir, binaryName)
	cmd := exec.Command("go", "build", "-tags=netgo,timetzdata", "-o", binaryPath, ".")
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
		writeTestConfig(t, dir, apiPort)
		runSmokeTest(t, dir, []string{"-openui=false"}, apiPort, 30*time.Second)
	})

	t.Run("instant sync", func(t *testing.T) {
		dir := t.TempDir()
		apiPort := randomPort(t)
		writeTestConfig(t, dir, apiPort)
		runSmokeTest(t, dir, []string{"-openui=false", "-instant"}, apiPort, 3*time.Minute)
	})
}

// writeTestConfig writes a renterd.yml config file to dir.
func writeTestConfig(t *testing.T, dir string, apiPort int) {
	t.Helper()

	// copy default config and override some fields
	c := cfg
	c.Seed = wallet.NewSeedPhrase()
	c.Directory = "."
	c.Network = "zen"
	c.HTTP.Address = fmt.Sprintf("127.0.0.1:%d", apiPort)
	c.HTTP.Password = apiPassword
	c.Bus.GatewayAddr = fmt.Sprintf("127.0.0.1:%d", randomPort(t))
	c.S3.Address = fmt.Sprintf("127.0.0.1:%d", randomPort(t))

	// prepare MySQL test database if necessary
	if uri := os.Getenv("RENTERD_DB_URI"); uri != "" {
		user := os.Getenv("RENTERD_DB_USER")
		password := os.Getenv("RENTERD_DB_PASSWORD")
		dbID := hex.EncodeToString(frand.Bytes(4))
		dbName := "renterd_smoke_" + dbID
		metricsName := "renterd_smoke_" + dbID + "_metrics"

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

		c.Database.MySQL = config.MySQL{
			URI:             uri,
			User:            user,
			Password:        password,
			Database:        dbName,
			MetricsDatabase: metricsName,
		}
	}

	// write config file
	buf, err := yaml.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "renterd.yml"), buf, 0644); err != nil {
		t.Fatal(err)
	}
}

// runSmokeTest runs the renterd binary with given args. It will wait for the
// API to become responsive and then attempt a graceful shutdown.
func runSmokeTest(t *testing.T, dir string, args []string, port int, timeout time.Duration) {
	t.Helper()

	// capture output
	var output bytes.Buffer
	cmd := exec.Command(binaryPath, args...)
	cmd.Stdout = &output
	cmd.Stderr = &output
	cmd.Dir = dir

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
		client := &http.Client{Timeout: time.Second}
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
				continue
			}
			req.SetBasicAuth("", apiPassword)

			resp, err := client.Do(req)
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
	if runtime.GOOS == "windows" {
		cmd.Process.Kill()
	} else {
		cmd.Process.Signal(os.Interrupt)
	}
	select {
	case <-doneCh:
		if err != nil && runtime.GOOS != "windows" {
			t.Fatalf("process exited with error during shutdown: %v, output: %s", err, output.String())
		}
	case <-time.After(30 * time.Second):
		cmd.Process.Kill()
		<-doneCh // ensure process is done before reading output
		t.Fatalf("graceful shutdown took longer than 30s, output: %s", output.String())
	}

	t.Log(output.String())
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
