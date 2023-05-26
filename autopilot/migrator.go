package autopilot

import (
	"context"
	"math"
	"sync"

	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/object"
	"go.sia.tech/renterd/tracing"
	"go.uber.org/zap"
)

const (
	migratorBatchSize = math.MaxInt // TODO: change once we have a fix for the infinite loop
)

type migrator struct {
	ap           *Autopilot
	logger       *zap.SugaredLogger
	healthCutoff float64

	mu      sync.Mutex
	running bool
}

func newMigrator(ap *Autopilot, healthCutoff float64) *migrator {
	return &migrator{
		ap:           ap,
		logger:       ap.logger.Named("migrator"),
		healthCutoff: healthCutoff,
	}
}

func (m *migrator) tryPerformMigrations(ctx context.Context, wp *workerPool) {
	m.mu.Lock()
	if m.running || m.ap.isStopped() {
		m.mu.Unlock()
		return
	}
	m.running = true
	m.mu.Unlock()

	m.ap.wg.Add(1)
	go func(cfg api.AutopilotConfig) {
		defer m.ap.wg.Done()
		m.performMigrations(wp, cfg)
		m.mu.Lock()
		m.running = false
		m.mu.Unlock()
	}(m.ap.state.cfg)
}

func (m *migrator) performMigrations(p *workerPool, cfg api.AutopilotConfig) {
	m.logger.Info("performing migrations")
	b := m.ap.bus
	ctx, span := tracing.Tracer.Start(context.Background(), "migrator.performMigrations")
	defer span.End()

	// fetch slabs for migration
	toMigrate, err := b.SlabsForMigration(ctx, m.healthCutoff, cfg.Contracts.Set, migratorBatchSize)
	if err != nil {
		m.logger.Errorf("failed to fetch slabs for migration, err: %v", err)
		return
	}
	m.logger.Debugf("%d slabs to migrate", len(toMigrate))

	// return if there are no slabs to migrate
	if len(toMigrate) == 0 {
		return
	}

	// prepare a channel to push work to the workers
	type job struct {
		object.Slab
		slabIdx int
	}
	jobs := make(chan job)
	var wg sync.WaitGroup
	defer func() {
		close(jobs)
		wg.Wait()
	}()

	p.withWorkers(func(workers []Worker) {
		for _, w := range workers {
			wg.Add(1)
			go func(w Worker) {
				defer wg.Done()

				id, err := w.ID(ctx)
				if err != nil {
					m.logger.Errorf("failed to fetch worker id: %v", err)
					return
				}

				for j := range jobs {
					err := w.MigrateSlab(ctx, j.Slab)
					if err != nil {
						m.logger.Errorf("%v: failed to migrate slab %d/%d, err: %v", id, j.slabIdx+1, len(toMigrate), err)
						continue
					}
					m.logger.Debugf("%v: successfully migrated slab '%v' %d/%d", id, j.Key, j.slabIdx+1, len(toMigrate))
				}
			}(w)
		}
	})

	// push work to workers
	for i, slab := range toMigrate {
		select {
		case <-m.ap.stopChan:
			return
		case jobs <- job{slab, i}:
		}
	}
}
