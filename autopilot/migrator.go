package autopilot

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"

	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/internal/utils"
	"go.sia.tech/renterd/object"
	"go.sia.tech/renterd/stats"
	"go.uber.org/zap"
)

const (
	migratorBatchSize = math.MaxInt // TODO: change once we have a fix for the infinite loop
)

type (
	migrator struct {
		ap                        *Autopilot
		logger                    *zap.SugaredLogger
		healthCutoff              float64
		parallelSlabsPerWorker    uint64
		signalMaintenanceFinished chan struct{}
		statsSlabMigrationSpeedMS *stats.DataPoints

		mu                 sync.Mutex
		migrating          bool
		migratingLastStart time.Time
	}

	job struct {
		api.UnhealthySlab
		slabIdx   int
		batchSize int
		set       string

		b Bus
	}
)

func (j *job) execute(ctx context.Context, w Worker) (_ api.MigrateSlabResponse, err error) {
	slab, err := j.b.Slab(ctx, j.Key)
	if err != nil {
		return api.MigrateSlabResponse{}, fmt.Errorf("failed to fetch slab; %w", err)
	}

	res, err := w.MigrateSlab(ctx, slab, j.set)
	if err != nil {
		return api.MigrateSlabResponse{}, fmt.Errorf("failed to migrate slab; %w", err)
	} else if res.Error != "" {
		return res, fmt.Errorf("failed to migrate slab; %w", errors.New(res.Error))
	}

	return res, nil
}

func newMigrator(ap *Autopilot, healthCutoff float64, parallelSlabsPerWorker uint64) *migrator {
	return &migrator{
		ap:                        ap,
		logger:                    ap.logger.Named("migrator"),
		healthCutoff:              healthCutoff,
		parallelSlabsPerWorker:    parallelSlabsPerWorker,
		signalMaintenanceFinished: make(chan struct{}, 1),
		statsSlabMigrationSpeedMS: stats.New(time.Hour),
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

func (m *migrator) slabMigrationEstimate(remaining int) time.Duration {
	// recompute p90
	m.statsSlabMigrationSpeedMS.Recompute()

	// return 0 if p90 is 0 (can happen if we haven't collected enough data points)
	p90 := m.statsSlabMigrationSpeedMS.P90()
	if p90 == 0 {
		return 0
	}

	totalNumMS := float64(remaining) * p90 / float64(m.parallelSlabsPerWorker)
	return time.Duration(totalNumMS) * time.Millisecond
}

func (m *migrator) tryPerformMigrations(wp *workerPool) {
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

	// prepare a channel to push work to the workers
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

					// derive ctx from shutdown ctx
					ctx, cancel := context.WithCancel(m.ap.shutdownCtx)
					defer cancel()

					// fetch worker id once
					id, err := w.ID(ctx)
					if err != nil {
						m.logger.Errorf("failed to fetch worker id: %v", err)
						return
					}

					// process jobs
					for j := range jobs {
						start := time.Now()
						res, err := j.execute(ctx, w)
						m.statsSlabMigrationSpeedMS.Track(float64(time.Since(start).Milliseconds()))

						if err != nil {
							m.logger.Errorf("%v: migration %d/%d failed, key: %v, health: %v, overpaid: %v, err: %v", id, j.slabIdx+1, j.batchSize, j.Key, j.Health, res.SurchargeApplied, err)
							skipAlert := utils.IsErr(err, api.ErrSlabNotFound)
							if !skipAlert {
								if res.SurchargeApplied {
									m.ap.RegisterAlert(ctx, newCriticalMigrationFailedAlert(j.Key, j.Health, err))
								} else {
									m.ap.RegisterAlert(ctx, newMigrationFailedAlert(j.Key, j.Health, err))
								}
							}
						} else {
							m.logger.Infof("%v: migration %d/%d succeeded, key: %v, health: %v, overpaid: %v, shards migrated: %v", id, j.slabIdx+1, j.batchSize, j.Key, j.Health, res.SurchargeApplied, res.NumShardsMigrated)
							m.ap.DismissAlert(ctx, alertIDForSlab(alertMigrationID, j.Key))
							if res.SurchargeApplied {
								// this alert confirms the user his gouging
								// settings are working, it will be dismissed
								// automatically the next time this slab is
								// successfully migrated
								m.ap.RegisterAlert(ctx, newCriticalMigrationSucceededAlert(j.Key))
							}
						}
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
		if err := b.RefreshHealth(m.ap.shutdownCtx); err != nil {
			m.ap.RegisterAlert(m.ap.shutdownCtx, newRefreshHealthFailedAlert(err))
			m.logger.Errorf("failed to recompute cached health before migration: %v", err)
			return
		}
		m.logger.Infof("recomputed slab health in %v", time.Since(start))

		// fetch slabs for migration
		toMigrateNew, err := b.SlabsForMigration(m.ap.shutdownCtx, m.healthCutoff, set, migratorBatchSize)
		if err != nil {
			m.logger.Errorf("failed to fetch slabs for migration, err: %v", err)
			return
		}
		m.logger.Infof("%d potential slabs fetched for migration", len(toMigrateNew))

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

		// sort the newly added slabs by health
		newSlabs := toMigrate[len(toMigrate)-len(migrateNewMap):]
		sort.Slice(newSlabs, func(i, j int) bool {
			return newSlabs[i].Health < newSlabs[j].Health
		})
		migrateNewMap = nil // free map

		// log the updated list of slabs to migrate
		m.logger.Infof("%d slabs to migrate", len(toMigrate))

		// register an alert to notify users about ongoing migrations.
		if len(toMigrate) > 0 {
			m.ap.RegisterAlert(m.ap.shutdownCtx, newOngoingMigrationsAlert(len(toMigrate), m.slabMigrationEstimate(len(toMigrate))))
		}

		// return if there are no slabs to migrate
		if len(toMigrate) == 0 {
			return
		}

		for i, slab := range toMigrate {
			select {
			case <-m.ap.shutdownCtx.Done():
				return
			case <-m.signalMaintenanceFinished:
				m.logger.Info("migrations interrupted - updating slabs for migration")
				continue OUTER
			case jobs <- job{slab, i, len(toMigrate), set, b}:
			}
		}

		return
	}
}
