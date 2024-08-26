package scanner

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/internal/utils"
	"go.uber.org/zap"
)

const (
	DefaultScanTimeout = 10 * time.Second
)

type (
	HostStore interface {
		HostsForScanning(ctx context.Context, opts api.HostsForScanningOptions) ([]api.HostAddress, error)
		RemoveOfflineHosts(ctx context.Context, minRecentScanFailures uint64, maxDowntime time.Duration) (uint64, error)
	}

	Scanner interface {
		Scan(ctx context.Context, w WorkerRHPScan, force bool)
		Shutdown(ctx context.Context) error
		Status() (bool, time.Time)
		UpdateHostsConfig(cfg api.HostsConfig)
	}

	WorkerRHPScan interface {
		RHPScan(ctx context.Context, hostKey types.PublicKey, hostIP string, timeout time.Duration) (api.RHPScanResponse, error)
	}
)

type (
	scanner struct {
		hs HostStore

		scanBatchSize int
		scanThreads   int
		scanInterval  time.Duration

		statsHostPingMS *utils.DataPoints

		shutdownChan chan struct{}
		wg           sync.WaitGroup

		logger *zap.SugaredLogger

		mu       sync.Mutex
		hostsCfg *api.HostsConfig

		scanning          bool
		scanningLastStart time.Time

		interruptChan chan struct{}
	}

	scanJob struct {
		hostKey types.PublicKey
		hostIP  string
	}
)

func New(hs HostStore, scanBatchSize, scanThreads uint64, scanMinInterval time.Duration, logger *zap.Logger) (Scanner, error) {
	logger = logger.Named("scanner")
	if scanBatchSize == 0 {
		return nil, errors.New("scanner batch size has to be greater than zero")
	}
	if scanThreads == 0 {
		return nil, errors.New("scanner threads has to be greater than zero")
	}
	return &scanner{
		hs: hs,

		scanBatchSize: int(scanBatchSize),
		scanThreads:   int(scanThreads),
		scanInterval:  scanMinInterval,

		statsHostPingMS: utils.NewDataPoints(0),
		logger:          logger.Sugar(),

		interruptChan: make(chan struct{}),
		shutdownChan:  make(chan struct{}),
	}, nil
}

func (s *scanner) Scan(ctx context.Context, w WorkerRHPScan, force bool) {
	if s.canSkipScan(force) {
		s.logger.Debug("host scan skipped")
		return
	}

	cutoff := time.Now()
	if !force {
		cutoff = cutoff.Add(-s.scanInterval)
	}

	s.logger.Infow("scan started",
		"batch", s.scanBatchSize,
		"force", force,
		"threads", s.scanThreads,
		"cutoff", cutoff,
	)

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()

		hosts := s.fetchHosts(ctx, cutoff)
		scanned := s.scanHosts(ctx, w, hosts)
		removed := s.removeOfflineHosts(ctx)

		s.mu.Lock()
		defer s.mu.Unlock()
		s.scanning = false
		s.logger.Infow("scan finished",
			"force", force,
			"duration", time.Since(s.scanningLastStart),
			"pingMSAvg", s.statsHostPingMS.Average(),
			"pingMSP90", s.statsHostPingMS.P90(),
			"removed", removed,
			"scanned", scanned)
	}()
}

func (s *scanner) Shutdown(ctx context.Context) error {
	defer close(s.shutdownChan)

	waitChan := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(waitChan)
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-waitChan:
	}

	return nil
}

func (s *scanner) Status() (bool, time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.scanning, s.scanningLastStart
}

func (s *scanner) UpdateHostsConfig(cfg api.HostsConfig) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.hostsCfg = &cfg
}

func (s *scanner) fetchHosts(ctx context.Context, cutoff time.Time) chan scanJob {
	jobsChan := make(chan scanJob, s.scanBatchSize)
	go func() {
		defer close(jobsChan)

		var exhausted bool
		for offset := 0; !exhausted; offset += s.scanBatchSize {
			hosts, err := s.hs.HostsForScanning(ctx, api.HostsForScanningOptions{
				MaxLastScan: api.TimeRFC3339(cutoff),
				Offset:      offset,
				Limit:       s.scanBatchSize,
			})
			if err != nil {
				s.logger.Errorf("could not get hosts for scanning, err: %v", err)
				return
			} else if len(hosts) < s.scanBatchSize {
				exhausted = true
			}

			s.logger.Debugf("fetched %d hosts for scanning", len(hosts))
			for _, h := range hosts {
				select {
				case <-s.interruptChan:
					return
				case <-s.shutdownChan:
					return
				case jobsChan <- scanJob{
					hostKey: h.PublicKey,
					hostIP:  h.NetAddress,
				}:
				}
			}
		}
	}()

	return jobsChan
}

func (s *scanner) scanHosts(ctx context.Context, w WorkerRHPScan, hosts chan scanJob) (scanned uint64) {
	// define worker
	worker := func() {
		for h := range hosts {
			if s.isShutdown() || s.isInterrupted() {
				break // shutdown
			}

			scan, err := w.RHPScan(ctx, h.hostKey, h.hostIP, DefaultScanTimeout)
			if err != nil {
				s.logger.Errorw("worker stopped", zap.Error(err), "hk", h.hostKey)
				break // abort
			} else if err := scan.Error(); err != nil {
				s.logger.Debugw("host scan failed", zap.Error(err), "hk", h.hostKey, "ip", h.hostIP)
			} else {
				s.statsHostPingMS.Track(float64(time.Duration(scan.Ping).Milliseconds()))
				atomic.AddUint64(&scanned, 1)
			}
		}
	}

	// launch all workers
	var wg sync.WaitGroup
	for t := 0; t < s.scanThreads; t++ {
		wg.Add(1)
		go func() {
			worker()
			wg.Done()
		}()
	}

	// wait until they're done
	wg.Wait()

	s.statsHostPingMS.Recompute()
	return
}

func (s *scanner) isInterrupted() bool {
	select {
	case <-s.interruptChan:
		return true
	default:
	}
	return false
}

func (s *scanner) isShutdown() bool {
	select {
	case <-s.shutdownChan:
		return true
	default:
	}
	return false
}

func (s *scanner) removeOfflineHosts(ctx context.Context) (removed uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.hostsCfg == nil {
		s.logger.Info("no hosts config set, skipping removal of offline hosts")
		return
	}

	maxDowntime := time.Duration(s.hostsCfg.MaxDowntimeHours) * time.Hour
	if maxDowntime == 0 {
		s.logger.Info("hosts config has no max downtime set, skipping removal of offline hosts")
		return
	}

	s.logger.Infow("removing offline hosts",
		"maxDowntime", maxDowntime,
		"minRecentScanFailures", s.hostsCfg.MinRecentScanFailures)

	var err error
	removed, err = s.hs.RemoveOfflineHosts(ctx, s.hostsCfg.MinRecentScanFailures, maxDowntime)
	if err != nil {
		s.logger.Errorw("removing offline hosts failed", zap.Error(err), "maxDowntime", maxDowntime, "minRecentScanFailures", s.hostsCfg.MinRecentScanFailures)
		return
	}

	return
}

func (s *scanner) canSkipScan(force bool) bool {
	if s.isShutdown() {
		return true
	}

	s.mu.Lock()
	if force {
		close(s.interruptChan)
		s.mu.Unlock()

		s.logger.Infof("host scan interrupted, waiting for ongoing scan to complete")
		s.wg.Wait()

		s.mu.Lock()
		s.interruptChan = make(chan struct{})
	} else if s.scanning || time.Since(s.scanningLastStart) < s.scanInterval {
		s.mu.Unlock()
		return true
	}
	s.scanningLastStart = time.Now()
	s.scanning = true
	s.mu.Unlock()

	return false
}
