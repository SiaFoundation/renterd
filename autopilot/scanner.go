package autopilot

import (
	"encoding/json"
	"errors"
	"sync"
	"time"

	"go.sia.tech/renterd/hostdb"
	"go.sia.tech/renterd/internal/consensus"
	rhpv2 "go.sia.tech/renterd/rhp/v2"
)

type scanner struct {
	ap               *Autopilot
	scanLoopInterval time.Duration

	wg       sync.WaitGroup
	stopChan chan struct{}
	wakeChan chan struct{}
}

func newScanner(ap *Autopilot, scanLoopInterval time.Duration) *scanner {
	return &scanner{
		ap:               ap,
		scanLoopInterval: scanLoopInterval,
		stopChan:         make(chan struct{}),
		wakeChan:         make(chan struct{}),
	}
}

func (s *scanner) run() {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.scanLoop()
	}()

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.triggerLoop()
	}()
}

func (s *scanner) stop() error {
	close(s.stopChan)

	doneChan := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(doneChan)
	}()
	select {
	case <-doneChan:
	case <-time.After(stopTimeout):
		return errors.New("unclean scanner shutdown")
	}
	return nil
}

func (s *scanner) triggerLoop() {
	for {
		select {
		case <-s.stopChan:
			return
		case <-time.After(s.scanLoopInterval):
			s.wake()
		}
	}
}

func (s *scanner) wake() {
	select {
	case s.wakeChan <- struct{}{}:
	default:
	}
}

func (s *scanner) scanLoop() {
	// TODO: initial scan with much shorter
	for {
		select {
		case <-s.stopChan:
			return
		case <-s.wakeChan:
		}

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
			r := <-resChan
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
}
