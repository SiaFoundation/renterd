package node

import (
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

	n := NewNode(apiAddr, apiPassword, dir, &noopHandler{}, consensus.GeneratePrivateKey())

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
		if err := n.Serve(); err != nil {
			t.Error(err)
		}
	}()

	// Close node again.
	if err := n.Close(); err != nil {
		t.Fatal(err)
	}
}
