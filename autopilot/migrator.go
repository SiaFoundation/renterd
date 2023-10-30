package autopilot

import (
	"context"
	"math"
	"sort"
	"sync"
	"time"

	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/object"
	"go.sia.tech/renterd/tracing"
	"go.uber.org/zap"
)

const (
	migratorBatchSize = math.MaxInt // TODO: change once we have a fix for the infinite loop
)

type migrator struct {
	ap                        *Autopilot
	logger                    *zap.SugaredLogger
	healthCutoff              float64
	parallelSlabsPerWorker    uint64
	signalMaintenanceFinished chan struct{}

	mu                 sync.Mutex
	migrating          bool
	migratingLastStart time.Time
}

func newMigrator(ap *Autopilot, healthCutoff float64, parallelSlabsPerWorker uint64) *migrator {
	return &migrator{
		ap:                        ap,
		logger:                    ap.logger.Named("migrator"),
		healthCutoff:              healthCutoff,
		parallelSlabsPerWorker:    parallelSlabsPerWorker,
		signalMaintenanceFinished: make(chan struct{}, 1),
	}
}

func (m *migrator) SignalMaintenanceFinished() {
	select {
	case m.signalMaintenanceFinished <- struct{}{}:
	default:
	}
}

func (m *migrator) Status() (bool, time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.migrating, m.migratingLastStart
}

func (m *migrator) tryPerformMigrations(ctx context.Context, wp *workerPool) {
	m.mu.Lock()
	if m.migrating || m.ap.isStopped() {
		m.mu.Unlock()
		return
	}
	m.migrating = true
	m.migratingLastStart = time.Now()
	m.mu.Unlock()

	m.ap.wg.Add(1)
	go func() {
		defer m.ap.wg.Done()
		m.performMigrations(wp)
		m.mu.Lock()
		m.migrating = false
		m.mu.Unlock()
	}()
}

func (m *migrator) performMigrations(p *workerPool) {
	m.logger.Info("performing migrations")
	b := m.ap.bus
	ctx, span := tracing.Tracer.Start(context.Background(), "migrator.performMigrations")
	defer span.End()

	// prepare a channel to push work to the workers
	type job struct {
		api.UnhealthySlab
		slabIdx   int
		batchSize int
		set       string
	}
	jobs := make(chan job)
	var wg sync.WaitGroup
	defer func() {
		close(jobs)
		wg.Wait()
	}()

	// launch workers
	p.withWorkers(func(workers []Worker) {
		for _, w := range workers {
			for i := uint64(0); i < m.parallelSlabsPerWorker; i++ {
				wg.Add(1)
				go func(w Worker) {
					defer wg.Done()

					id, err := w.ID(ctx)
					if err != nil {
						m.logger.Errorf("failed to fetch worker id: %v", err)
						return
					}

					for j := range jobs {
						slab, err := b.Slab(ctx, j.Key)
						if err != nil {
							m.logger.Errorf("%v: failed to fetch slab for migration %d/%d, health: %v, err: %v", id, j.slabIdx+1, j.batchSize, j.Health, err)
							continue
						}
						ap, err := b.Autopilot(ctx, m.ap.id)
						if err != nil {
							m.logger.Errorf("%v: failed to fetch autopilot settings for migration %d/%d, health: %v, err: %v", id, j.slabIdx+1, j.batchSize, j.Health, err)
							continue
						}
						res, err := w.MigrateSlab(ctx, slab, ap.Config.Contracts.Set)
						if err != nil {
							m.ap.RegisterAlert(ctx, newSlabMigrationFailedAlert(slab, j.Health, err))
							m.logger.Errorf("%v: failed to migrate slab %d/%d, health: %v, err: %v", id, j.slabIdx+1, j.batchSize, j.Health, err)
							continue
						} else {
							m.ap.DismissAlert(ctx, alertIDForSlab(alertMigrationID, slab))
						}
						m.logger.Debugf("%v: successfully migrated slab (health: %v migrated shards: %d) %d/%d", id, j.Health, res.NumShardsMigrated, j.slabIdx+1, j.batchSize)
					}
				}(w)
			}
		}
	})
	var toMigrate []api.UnhealthySlab

	// ignore a potential signal before the first iteration of the 'OUTER' loop
	select {
	case <-m.signalMaintenanceFinished:
	default:
	}

OUTER:
	for {
		// fetch currently configured set
		set := m.ap.State().cfg.Contracts.Set
		if set == "" {
			m.logger.Error("could not perform migrations, no contract set configured")
			return
		}

		// recompute health.
		start := time.Now()
		if err := b.RefreshHealth(ctx); err != nil {
			rerr := m.ap.alerts.RegisterAlert(ctx, newRefreshHealthFailedAlert(err))
			if rerr != nil {
				m.logger.Errorf("failed to register alert: err %v", rerr)
			}
			m.logger.Errorf("failed to recompute cached health before migration", err)
			return
		}
		m.logger.Debugf("recomputed slab health in %v", time.Since(start))

		// fetch slabs for migration
		toMigrateNew, err := b.SlabsForMigration(ctx, m.healthCutoff, set, migratorBatchSize)
		if err != nil {
			m.logger.Errorf("failed to fetch slabs for migration, err: %v", err)
			return
		}
		m.logger.Debugf("%d potential slabs fetched for migration", len(toMigrateNew))

		// merge toMigrateNew with toMigrate
		// NOTE: when merging, we remove all slabs from toMigrate that don't
		// require migration anymore. However, slabs that have been in toMigrate
		// before will be repaired before any new slabs. This is to prevent
		// starvation.
		migrateNewMap := make(map[object.EncryptionKey]*api.UnhealthySlab)
		for i, slab := range toMigrateNew {
			migrateNewMap[slab.Key] = &toMigrateNew[i]
		}
		removed := 0
		for i := 0; i < len(toMigrate)-removed; {
			slab := toMigrate[i]
			if _, exists := migrateNewMap[slab.Key]; exists {
				delete(migrateNewMap, slab.Key) // delete from map to leave only new slabs
				i++
			} else {
				toMigrate[i] = toMigrate[len(toMigrate)-1-removed]
				removed++
			}
		}
		toMigrate = toMigrate[:len(toMigrate)-removed]
		for _, slab := range migrateNewMap {
			toMigrate = append(toMigrate, *slab)
		}

		// sort the newsly added slabs by health
		newSlabs := toMigrate[len(toMigrate)-len(migrateNewMap):]
		sort.Slice(newSlabs, func(i, j int) bool {
			return newSlabs[i].Health < newSlabs[j].Health
		})
		migrateNewMap = nil // free map

		// log the updated list of slabs to migrate
		m.logger.Debugf("%d slabs to migrate", len(toMigrate))

		// register an alert to notify users about ongoing migrations.
		if len(toMigrate) > 0 {
			err = m.ap.alerts.RegisterAlert(ctx, newOngoingMigrationsAlert(len(toMigrate)))
			if err != nil {
				m.logger.Errorf("failed to register alert: err %v", err)
			}
		}

		// return if there are no slabs to migrate
		if len(toMigrate) == 0 {
			return
		}

		for i, slab := range toMigrate {
			select {
			case <-m.ap.stopChan:
				return
			case <-m.signalMaintenanceFinished:
				m.logger.Info("migrations interrupted - updating slabs for migration")
				continue OUTER
			case jobs <- job{slab, i, len(toMigrate), set}:
			}
		}
	}
}
