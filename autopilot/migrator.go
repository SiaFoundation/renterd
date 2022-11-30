package autopilot

import (
	"sync"
	"time"

	"go.sia.tech/renterd/bus"
	"go.sia.tech/renterd/worker"
	"go.sia.tech/siad/types"
)

type migrator struct {
	ap *Autopilot

	mu            sync.Mutex
	goodContracts []worker.Contract
	running       bool
}

func newMigrator(ap *Autopilot) *migrator {
	return &migrator{
		ap: ap,
	}
}

// UpdateContracts updates the set of contracts that the migrator considers
// good. Missing sectors in slabs will be migrated to these contracts.
func (m *migrator) UpdateContracts() error {
	bus := m.ap.bus

	contracts, err := bus.ActiveContracts()
	if err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.goodContracts = m.goodContracts[:0]
	for _, c := range contracts {
		// TODO: filter out contracts that are not good.
		m.goodContracts = append(m.goodContracts, worker.Contract{
			ID:        c.ID,
			HostKey:   c.HostKey,
			HostIP:    c.HostIP,
			RenterKey: m.ap.deriveRenterKey(c.HostKey),
		})
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

func (m *migrator) fetchSlabsForMigration() ([]bus.SlabID, error) {
	goodContracts := make([]types.FileContractID, len(m.goodContracts))
	for i := range m.goodContracts {
		goodContracts[i] = m.goodContracts[i].ID
	}
	return m.ap.bus.SlabsForMigration(10, time.Now().Add(-time.Hour), goodContracts)
}

func (m *migrator) migrateSlab(slabID bus.SlabID) (bool, error) {
	// Fetch slab data.
	slab, contracts, err := m.ap.bus.SlabForMigration(slabID)
	if err != nil {
		return false, err
	}

	// Copy contracts to release lock before starting migration.
	m.mu.Lock()
	goodContracts := append([]worker.Contract{}, m.goodContracts...)
	m.mu.Unlock()

	// Fetch the current consensus height.
	cs, err := m.ap.bus.ConsensusState()
	if err != nil {
		return false, err
	}

	// Migrate the slab. If this fails we consider the upload failed. This
	// is not very accurate (yet) but since we were already able to fetch a
	// slab before we can at least be sure that it's probably not a
	// connection issue between autopilot, bus and database.
	err = m.ap.worker.MigrateSlab(&slab, contracts, goodContracts, cs.BlockHeight)
	if err != nil {
		return true, err
	}
	return false, nil
}

func (m *migrator) markFailedMigration(slabID bus.SlabID) error {
	updates, err := m.ap.bus.MarkSlabsMigrationFailure([]bus.SlabID{slabID})
	if err != nil {
		return err
	}
	if updates != 1 {
		// TODO: log warning
	}
	return nil
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
		for _, slabID := range slabsToRepair {
			failed, err := m.migrateSlab(slabID)
			if err != nil {
				continue // TODO log
			}
			if !failed {
				continue
			}
			// Mark failed repairs
			if err := m.markFailedMigration(slabID); err != nil {
				// TODO log
			}
		}
	}
}
