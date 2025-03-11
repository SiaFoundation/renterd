package scanner

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/v2/api"
	"go.sia.tech/renterd/v2/internal/utils"
	"go.uber.org/zap"
)

const (
	DefaultScanTimeout = 10 * time.Second
)

var (
	ErrShuttingDown    = errors.New("scanner is shutting down")
	ErrScanInterrupted = errors.New("scanning was interrupted")
)

type (
	HostScanner interface {
		ScanHost(ctx context.Context, hostKey types.PublicKey, timeout time.Duration) (api.HostScanResponse, error)
	}

	HostStore interface {
		Hosts(ctx context.Context, opts api.HostOptions) ([]api.Host, error)
		RemoveOfflineHosts(ctx context.Context, maxConsecutiveScanFailures uint64, maxDowntime time.Duration) (uint64, error)
	}
)

type (
	Scanner struct {
		hs HostStore

		scanBatchSize int
		scanThreads   int
		scanInterval  time.Duration

		statsHostPingMS *utils.DataPoints

		wg sync.WaitGroup

		logger *zap.SugaredLogger

		mu       sync.Mutex
		hostsCfg *api.HostsConfig

		scanning          bool
		scanningLastStart time.Time
		scanningCtxCancel context.CancelCauseFunc
	}

	scanJob struct {
		hostKey types.PublicKey
		hostIP  string
	}
)

func New(hs HostStore, scanBatchSize, scanThreads uint64, scanMinInterval time.Duration, logger *zap.Logger) (*Scanner, error) {
	logger = logger.Named("scanner")
	if scanBatchSize == 0 {
		return nil, errors.New("scanner batch size has to be greater than zero")
	}
	if scanThreads == 0 {
		return nil, errors.New("scanner threads has to be greater than zero")
	}
	return &Scanner{
		hs: hs,

		scanBatchSize: int(scanBatchSize),
		scanThreads:   int(scanThreads),
		scanInterval:  scanMinInterval,

		statsHostPingMS: utils.NewDataPoints(0),
		logger:          logger.Sugar(),
	}, nil
}

func (s *Scanner) Scan(ctx context.Context, hs HostScanner, force bool) {
	s.mu.Lock()
	if force {
		if s.scanning {
			s.scanningCtxCancel(ErrScanInterrupted)
			s.mu.Unlock()

			s.logger.Infof("host scan interrupted, waiting for ongoing scan to complete")
			s.wg.Wait()
			s.mu.Lock()
		}
	} else if s.scanning || time.Since(s.scanningLastStart) < s.scanInterval {
		s.mu.Unlock()
		s.logger.Debug("host scan skipped")
		return
	}

	ctx, cancel := context.WithCancelCause(ctx)
	s.scanningCtxCancel = cancel
	s.scanningLastStart = time.Now()
	s.scanning = true
	s.mu.Unlock()

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
		defer func() {
			s.mu.Lock()
			s.scanning = false
			s.mu.Unlock()

			s.wg.Done()
			cancel(nil)
		}()
		scanned := s.scanHosts(ctx, hs, cutoff)
		removed := s.removeOfflineHosts(ctx)
		s.logger.Infow("scan finished",
			"force", force,
			"duration", time.Since(s.scanningLastStart),
			"pingMSAvg", s.statsHostPingMS.Average(),
			"pingMSP90", s.statsHostPingMS.P90(),
			"removed", removed,
			"scanned", scanned)
	}()
}

func (s *Scanner) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	if !s.scanning {
		s.mu.Unlock()
		return nil
	}
	s.scanningCtxCancel(ErrShuttingDown)
	s.mu.Unlock()

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

func (s *Scanner) Status() (bool, time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.scanning, s.scanningLastStart
}

func (s *Scanner) UpdateHostsConfig(cfg api.HostsConfig) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.hostsCfg = &cfg
}

func (s *Scanner) scanHosts(ctx context.Context, hs HostScanner, cutoff time.Time) (scanned uint64) {
	// define worker
	worker := func(jobs <-chan scanJob) {
		for h := range jobs {
			scan, err := hs.ScanHost(ctx, h.hostKey, DefaultScanTimeout)
			if errors.Is(err, ErrShuttingDown) || errors.Is(err, ErrScanInterrupted) || errors.Is(err, context.Canceled) {
				return
			} else if err != nil {
				s.logger.Errorw("worker stopped", zap.Error(err), "hk", h.hostKey)
				return // abort
			} else if err := scan.Error(); err != nil {
				s.logger.Debugw("host scan failed", zap.Error(err), "hk", h.hostKey, "ip", h.hostIP)
			} else {
				s.statsHostPingMS.Track(float64(time.Duration(scan.Ping).Milliseconds()))
				atomic.AddUint64(&scanned, 1)
			}
		}
	}

	var exhausted bool
	for !exhausted {
		// check if we should stop
		select {
		case <-ctx.Done():
			return
		default:
		}

		// synchronisation vars
		jobs := make(chan scanJob)
		workersDone := make(chan struct{})
		joinWorkers := func() {
			close(jobs)
			<-workersDone
		}

		// launch all workers for this batch
		activeWorkers := int32(s.scanThreads)
		for i := 0; i < s.scanThreads; i++ {
			go func() {
				worker(jobs)
				if atomic.AddInt32(&activeWorkers, -1) == 0 {
					close(workersDone) // close when all workers are done
				}
			}()
		}

		// fetch batch
		hosts, err := s.hs.Hosts(ctx, api.HostOptions{
			MaxLastScan: api.TimeRFC3339(cutoff),
			Offset:      0,
			Limit:       s.scanBatchSize,
		})
		if err != nil {
			s.logger.Errorf("could not get hosts for scanning, err: %v", err)
			joinWorkers()
			break
		}
		exhausted = len(hosts) < s.scanBatchSize

		// send batch to workers
		for _, h := range hosts {
			select {
			case jobs <- scanJob{
				hostKey: h.PublicKey,
				hostIP:  h.NetAddress,
			}:
			case <-ctx.Done():
				continue
			case <-workersDone:
				continue
			}
		}
		joinWorkers()
	}

	s.statsHostPingMS.Recompute()
	return
}

func (s *Scanner) removeOfflineHosts(ctx context.Context) (removed uint64) {
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
		"maxConsecutiveScanFailures", s.hostsCfg.MaxConsecutiveScanFailures)

	var err error
	removed, err = s.hs.RemoveOfflineHosts(ctx, s.hostsCfg.MaxConsecutiveScanFailures, maxDowntime)
	if err != nil && !errors.Is(err, context.Canceled) {
		s.logger.Errorw("removing offline hosts failed", zap.Error(err), "maxDowntime", maxDowntime, "maxConsecutiveScanFailures", s.hostsCfg.MaxConsecutiveScanFailures)
		return
	}

	return
}
