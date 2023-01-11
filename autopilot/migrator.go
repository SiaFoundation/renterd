package autopilot

import (
	"sync"

	"go.uber.org/zap"
)

const (
	migratorBatchSize   = 100
	migratorContractset = "autopilot"
)

type migrator struct {
	ap     *Autopilot
	logger *zap.SugaredLogger

	mu      sync.Mutex
	running bool
}

func newMigrator(ap *Autopilot) *migrator {
	return &migrator{
		ap:     ap,
		logger: ap.logger.Named("migrator"),
	}
}

func (m *migrator) TryPerformMigrations() {
	m.mu.Lock()
	if m.running {
		m.mu.Unlock()
		return
	}
	m.running = true
	m.mu.Unlock()

	go func() {
		m.performMigrations()
		m.mu.Lock()
		m.running = false
		m.mu.Unlock()
	}()
}

func (m *migrator) performMigrations() {
	b := m.ap.bus

	for {
		// fetch slabs for migration
		toMigrate, err := b.SlabsForMigration(migratorContractset, migratorBatchSize)
		if err != nil {
			return // TODO log
		}

		// escape early if there's no slabs to migrate
		if len(toMigrate) == 0 {
			return
		}

		// migrat them one by one
		for _, slab := range toMigrate {
			if err := m.ap.worker.MigrateSlab(slab); err != nil {
				continue // TODO log
			}
		}
	}
}
