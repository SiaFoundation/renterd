package node

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"go.sia.tech/renterd/internal/consensus"
)

type noopHandler struct{}

func (h *noopHandler) ServeHTTP(http.ResponseWriter, *http.Request) {

}

// TestNewNode is a smoke test which creates a node that runs all components
// (bus, worker and autopilot).
func TestNewNode(t *testing.T) {
	apiAddr := "127.0.0.1:0"
	gatewayAddr := "127.0.0.1:9981"
	apiPassword := "password"
	dir := t.TempDir()

	n, err := NewNode(apiAddr, apiPassword, dir, &noopHandler{}, consensus.GeneratePrivateKey())
	if err != nil {
		t.Fatal(err)
	}

	// Add a bus.
	if err := n.CreateBus(true, gatewayAddr); err != nil {
		t.Fatal(err)
	}

	// Add a worker.
	if err := n.CreateWorker(); err != nil {
		t.Fatal(err)
	}

	// Add an autopilot.
	if err := n.CreateAutopilot(time.Minute); err != nil {
		t.Fatal(err)
	}

	// Serve API.
	doneServing := make(chan struct{})
	go func() {
		defer close(doneServing)
		if err := n.Serve(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			t.Error(err)
		}
	}()

	// Wait a second to guarantee that we started serving the API.
	time.Sleep(time.Second)

	// Close node again.
	if err := n.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}

	// Wait for goroutine to finish.
	select {
	case <-time.After(10 * time.Second):
		t.Fatal("goroutine is stuck")
	case <-doneServing:
	}
}
