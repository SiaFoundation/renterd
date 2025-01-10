package migrator

import (
	"context"
	"math"
	"net"
	"sort"
	"sync"
	"time"

	rhpv3 "go.sia.tech/core/rhp/v3"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/alerts"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/internal/accounts"
	"go.sia.tech/renterd/internal/contracts"
	"go.sia.tech/renterd/internal/download"
	"go.sia.tech/renterd/internal/hosts"
	"go.sia.tech/renterd/internal/memory"
	"go.sia.tech/renterd/internal/rhp"
	rhp4 "go.sia.tech/renterd/internal/rhp/v4"
	"go.sia.tech/renterd/internal/upload"
	"go.sia.tech/renterd/internal/utils"
	"go.sia.tech/renterd/object"
	"go.uber.org/zap"
)

const (
	// migrationAlertRegisterInterval is the interval at which we update the
	// ongoing migrations alert to indicate progress
	migrationAlertRegisterInterval = 30 * time.Second

	// migratorBatchSize is the amount of slabs we fetch for migration from the
	// slab store at once
	migratorBatchSize = math.MaxInt // TODO: change once we have a fix for the infinite loop
)

type (
	Bus interface {
		Accounts(context.Context, string) ([]api.Account, error)
		AddMultipartPart(ctx context.Context, bucket, key, ETag, uploadID string, partNumber int, slices []object.SlabSlice) (err error)
		AddObject(ctx context.Context, bucket, key string, o object.Object, opts api.AddObjectOptions) error
		AddPartialSlab(ctx context.Context, data []byte, minShards, totalShards uint8) (slabs []object.SlabSlice, slabBufferMaxSizeSoftReached bool, err error)
		AddUploadingSectors(ctx context.Context, uID api.UploadID, root []types.Hash256) error
		AcquireContract(ctx context.Context, fcid types.FileContractID, priority int, d time.Duration) (lockID uint64, err error)
		ConsensusState(ctx context.Context) (api.ConsensusState, error)
		Contracts(ctx context.Context, opts api.ContractsOpts) ([]api.ContractMetadata, error)
		DeleteHostSector(ctx context.Context, hk types.PublicKey, root types.Hash256) error
		FetchPartialSlab(ctx context.Context, key object.EncryptionKey, offset, length uint32) ([]byte, error)
		FinishUpload(ctx context.Context, uID api.UploadID) error
		FundAccount(ctx context.Context, account rhpv3.Account, fcid types.FileContractID, amount types.Currency) (types.Currency, error)
		GougingParams(ctx context.Context) (api.GougingParams, error)
		Host(ctx context.Context, hostKey types.PublicKey) (api.Host, error)
		KeepaliveContract(ctx context.Context, fcid types.FileContractID, lockID uint64, d time.Duration) (err error)
		MarkPackedSlabsUploaded(ctx context.Context, slabs []api.UploadedPackedSlab) error
		Objects(ctx context.Context, prefix string, opts api.ListObjectOptions) (resp api.ObjectsResponse, err error)
		RecordContractSpending(ctx context.Context, records []api.ContractSpendingRecord) error
		ReleaseContract(ctx context.Context, fcid types.FileContractID, lockID uint64) (err error)
		RenewedContract(ctx context.Context, renewedFrom types.FileContractID) (api.ContractMetadata, error)
		Slab(ctx context.Context, key object.EncryptionKey) (object.Slab, error)
		TrackUpload(ctx context.Context, uID api.UploadID) error
		UpdateAccounts(context.Context, []api.Account) error
		UpdateSlab(ctx context.Context, key object.EncryptionKey, sectors []api.UploadedSector) error
		UploadParams(ctx context.Context) (api.UploadParams, error)
		UsableHosts(ctx context.Context) (hosts []api.HostInfo, err error)
	}

	SlabStore interface {
		RefreshHealth(ctx context.Context) error
		Slab(ctx context.Context, key object.EncryptionKey) (object.Slab, error)
		SlabsForMigration(ctx context.Context, healthCutoff float64, limit int) ([]api.UnhealthySlab, error)
	}
)

type (
	Migrator struct {
		alerts alerts.Alerter
		bus    Bus
		ss     SlabStore

		healthCutoff float64
		numThreads   uint64

		accounts        *accounts.Manager
		downloadManager *download.Manager
		uploadManager   *upload.Manager
		hostManager     hosts.Manager

		rhp4Client *rhp4.Client

		signalConsensusNotSynced  chan struct{}
		signalMaintenanceFinished chan struct{}

		statsSlabMigrationSpeedMS *utils.DataPoints

		shutdownCtx context.Context
		wg          sync.WaitGroup

		logger *zap.SugaredLogger

		mu                 sync.Mutex
		migrating          bool
		migratingLastStart time.Time
	}
)

func New(ctx context.Context, masterKey [32]byte, alerts alerts.Alerter, ss SlabStore, b Bus, healthCutoff float64, numThreads, downloadMaxOverdrive, uploadMaxOverdrive uint64, downloadOverdriveTimeout, uploadOverdriveTimeout, accountsRefillInterval time.Duration, logger *zap.Logger) (*Migrator, error) {
	logger = logger.Named("migrator")
	m := &Migrator{
		alerts: alerts,
		bus:    b,
		ss:     ss,

		healthCutoff: healthCutoff,
		numThreads:   numThreads,

		signalConsensusNotSynced:  make(chan struct{}, 1),
		signalMaintenanceFinished: make(chan struct{}, 1),

		statsSlabMigrationSpeedMS: utils.NewDataPoints(time.Hour),

		shutdownCtx: ctx,

		logger: logger.Sugar(),
	}

	// derive keys
	mk := utils.MasterKey(masterKey)
	ak := mk.DeriveAccountsKey("migrator")
	uk := mk.DeriveUploadKey()

	// create account manager
	am, err := accounts.NewManager(ak, "migrator", alerts, m, m, b, b, b, b, accountsRefillInterval, logger)
	if err != nil {
		return nil, err
	}
	m.accounts = am

	// create host manager
	dialer := rhp.NewFallbackDialer(b, net.Dialer{}, logger)
	csr := contracts.NewSpendingRecorder(ctx, b, 5*time.Second, logger)
	m.hostManager = hosts.NewManager(masterKey, am, csr, dialer, logger)
	m.rhp4Client = rhp4.New(dialer)

	// create upload & download manager
	mm := memory.NewManager(math.MaxInt64, logger)
	m.downloadManager = download.NewManager(ctx, &uk, m.hostManager, mm, b, downloadMaxOverdrive, downloadOverdriveTimeout, logger)
	m.uploadManager = upload.NewManager(ctx, &uk, m.hostManager, mm, b, b, b, uploadMaxOverdrive, uploadOverdriveTimeout, logger)

	return m, nil
}

func (m *Migrator) Migrate(ctx context.Context) {
	m.mu.Lock()
	if m.migrating {
		m.mu.Unlock()
		return
	}
	m.migrating = true
	m.migratingLastStart = time.Now()
	m.mu.Unlock()

	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		m.performMigrations(ctx)
		m.mu.Lock()
		m.migrating = false
		m.mu.Unlock()
	}()
}

func (m *Migrator) Shutdown(ctx context.Context) error {
	m.wg.Wait()

	// stop uploads and downloads
	m.downloadManager.Stop()
	m.uploadManager.Stop()

	// stop account manager
	return m.accounts.Shutdown(ctx)
}

func (m *Migrator) SignalMaintenanceFinished() {
	select {
	case m.signalMaintenanceFinished <- struct{}{}:
	default:
	}
}

func (m *Migrator) Status() (bool, time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.migrating, m.migratingLastStart
}

func (m *Migrator) slabMigrationEstimate(remaining int) time.Duration {
	// recompute p90
	m.statsSlabMigrationSpeedMS.Recompute()

	// return 0 if p90 is 0 (can happen if we haven't collected enough data points)
	p90 := m.statsSlabMigrationSpeedMS.P90()
	if p90 == 0 {
		return 0
	}

	totalNumMS := float64(remaining) * p90 / float64(m.numThreads)
	return time.Duration(totalNumMS) * time.Millisecond
}

func (m *Migrator) performMigrations(ctx context.Context) {
	m.logger.Info("performing migrations")

	// prepare jobs channel
	jobs := make(chan api.UnhealthySlab)
	var wg sync.WaitGroup
	defer func() {
		close(jobs)
		wg.Wait()
	}()

	// launch workers
	for i := uint64(0); i < m.numThreads; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			// process jobs
			for j := range jobs {
				start := time.Now()
				err := m.migrateSlab(ctx, j.EncryptionKey)
				m.statsSlabMigrationSpeedMS.Track(float64(time.Since(start).Milliseconds()))
				if utils.IsErr(err, api.ErrConsensusNotSynced) {
					// interrupt migrations if consensus is not synced
					select {
					case m.signalConsensusNotSynced <- struct{}{}:
					default:
					}
					return
				} else if err != nil {
					m.logger.Errorw("migration failed",
						zap.Float64("health", j.Health),
						zap.Stringer("slab", j.EncryptionKey))
				}
			}
		}()
	}
	var toMigrate []api.UnhealthySlab

	// ignore a potential signal before the first iteration of the 'OUTER' loop
	select {
	case <-m.signalMaintenanceFinished:
	default:
	}

	// helper to update 'toMigrate'
	updateToMigrate := func() {
		// fetch slabs for migration
		toMigrateNew, err := m.ss.SlabsForMigration(ctx, m.healthCutoff, migratorBatchSize)
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
			migrateNewMap[slab.EncryptionKey] = &toMigrateNew[i]
		}
		removed := 0
		for i := 0; i < len(toMigrate)-removed; {
			slab := toMigrate[i]
			if _, exists := migrateNewMap[slab.EncryptionKey]; exists {
				delete(migrateNewMap, slab.EncryptionKey) // delete from map to leave only new slabs
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
	}

	// unregister the ongoing migrations alert when we're done
	defer func() {
		if err := m.alerts.DismissAlerts(ctx, alertOngoingMigrationsID); err != nil {
			m.logger.Errorf("failed to dismiss alert: %v", err)
		}
	}()

OUTER:
	for {
		// recompute health.
		start := time.Now()
		if err := m.ss.RefreshHealth(ctx); err != nil {
			if err := m.alerts.RegisterAlert(ctx, newRefreshHealthFailedAlert(err)); err != nil {
				m.logger.Errorf("failed to register alert: %v", err)
			}
			m.logger.Errorf("failed to recompute cached health before migration: %v", err)
		} else {
			if err := m.alerts.DismissAlerts(ctx, alertHealthRefreshID); err != nil {
				m.logger.Errorf("failed to dismiss alert: %v", err)
			}
			m.logger.Infof("recomputed slab health in %v", time.Since(start))
			updateToMigrate()
		}

		// log the updated list of slabs to migrate
		m.logger.Infof("%d slabs to migrate", len(toMigrate))

		// return if there are no slabs to migrate
		if len(toMigrate) == 0 {
			return
		}

		var lastRegister time.Time
		for i, slab := range toMigrate {
			if time.Since(lastRegister) > migrationAlertRegisterInterval {
				// register an alert to notify users about ongoing migrations
				remaining := len(toMigrate) - i
				if err := m.alerts.RegisterAlert(ctx, newOngoingMigrationsAlert(remaining, m.slabMigrationEstimate(remaining))); err != nil {
					m.logger.Errorf("failed to register alert: %v", err)
				}
				lastRegister = time.Now()
			}
			select {
			case <-ctx.Done():
				return
			case <-m.signalConsensusNotSynced:
				m.logger.Info("migrations interrupted - consensus is not synced")
				return
			case <-m.signalMaintenanceFinished:
				m.logger.Info("migrations interrupted - updating slabs for migration")
				continue OUTER
			case jobs <- slab:
			}
		}

		// all slabs migrated
		return
	}
}
