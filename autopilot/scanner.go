package autopilot

import (
	"encoding/json"
	"sync"
	"time"

	"go.sia.tech/renterd/hostdb"
	"go.sia.tech/renterd/internal/consensus"
	rhpv2 "go.sia.tech/renterd/rhp/v2"
)

type scanner struct {
	ap *Autopilot

	mu      sync.Mutex
	running bool
}

func newScanner(ap *Autopilot) *scanner {
	return &scanner{
		ap: ap,
	}
}

func (s *scanner) tryPerformHostScan() {
	s.mu.Lock()
	if s.running {
		s.mu.Unlock()
		return
	}
	s.running = true
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.running = false
	}()

	// TODO: initial scan with much shorter
	// scan up to 10 hosts that we haven't interacted with in at least 1 week
	hosts, err := s.ap.bus.Hosts(time.Now().Add(-7*24*time.Hour), 10)
	if err != nil {
		return
	}
	type res struct {
		hostKey  consensus.PublicKey
		settings rhpv2.HostSettings
		err      error
	}
	resChan := make(chan res)
	for _, h := range hosts {
		go func(h hostdb.Host) {
			scan, err := s.ap.worker.RHPScan(h.PublicKey, h.NetAddress())
			resChan <- res{h.PublicKey, scan.Settings, err}
		}(h)
	}
	for range hosts {
		var r res

		select {
		case <-s.ap.stopChan:
			return
		case r = <-resChan:
		}

		if r.err != nil {
			err := s.ap.bus.RecordHostInteraction(r.hostKey, hostdb.Interaction{
				Timestamp: time.Now(),
				Type:      "scan",
				Success:   false,
				Result:    json.RawMessage(`{"error": "` + r.err.Error() + `"}`),
			})
			_ = err // TODO
		} else {
			js, _ := json.Marshal(r.settings)
			err := s.ap.bus.RecordHostInteraction(r.hostKey, hostdb.Interaction{
				Timestamp: time.Now(),
				Type:      "scan",
				Success:   true,
				Result:    json.RawMessage(js),
			})
			_ = err // TODO
		}
	}
}
