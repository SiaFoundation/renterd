package autopilot

import (
	"context"
	"math"
	"sync"

	"go.sia.tech/renterd/api"
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

func (m *migrator) tryPerformMigrations(ctx context.Context, w Worker) {
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
		m.performMigrations(w, cfg)
		m.mu.Lock()
		m.running = false
		m.mu.Unlock()
	}(m.ap.state.cfg)
}

func (m *migrator) performMigrations(w Worker, cfg api.AutopilotConfig) {
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

	// migrate the slabs one by one
	//
	// TODO: when we support parallel uploads we should parallelize this
	for i, slab := range toMigrate {
		if m.ap.isStopped() {
			break
		}

		err := w.MigrateSlab(ctx, slab)
		if err != nil {
			m.logger.Errorf("failed to migrate slab %d/%d, err: %v", i+1, len(toMigrate), err)
			continue
		}
		m.logger.Debugf("successfully migrated slab '%v' %d/%d", slab.Key, i+1, len(toMigrate))
	}
}
