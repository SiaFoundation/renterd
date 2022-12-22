package autopilot

import (
	"sync"
	"time"

	"go.sia.tech/renterd/object"
	"go.sia.tech/siad/types"
	"go.uber.org/zap"
)

type migrator struct {
	ap     *Autopilot
	logger *zap.SugaredLogger

	mu            sync.Mutex
	goodContracts []types.FileContractID
	running       bool
}

func newMigrator(ap *Autopilot) *migrator {
	return &migrator{
		ap:     ap,
		logger: ap.logger.Named("migrator"),
	}
}

// UpdateContracts updates the set of contracts that the migrator considers
// good. Missing sectors in slabs will be migrated to these contracts.
func (m *migrator) UpdateContracts() error {
	bus := m.ap.bus

	contracts, err := bus.AllContracts()
	if err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.goodContracts = m.goodContracts[:0]
	for _, c := range contracts {
		// TODO: filter out contracts that are not good.
		m.goodContracts = append(m.goodContracts, c.ID)
	}
	return nil
}

// TryPerformMigrations launches a migration routine unless it's already
// running. A launched routine runs until there are no more slabs to migrate.
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

func (m *migrator) fetchSlabsForMigration() ([]object.Slab, error) {
	return m.ap.bus.SlabsForMigration(10, time.Now().Add(-time.Hour), m.goodContracts)
}

func (m *migrator) performMigrations() {
	for {
		// Fetch slabs for repair
		slabsToRepair, err := m.fetchSlabsForMigration()
		if err != nil {
			return // TODO log
		}
		if len(slabsToRepair) == 0 {
			return // nothing to do
		}

		// Repair them
		for _, slab := range slabsToRepair {
			if err := m.ap.worker.MigrateSlab(slab); err != nil {
				continue // TODO log
			}
		}
	}
}
