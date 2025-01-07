package autopilot

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"runtime"
	"sync"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/jape"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/autopilot/contractor"
	"go.sia.tech/renterd/autopilot/scanner"
	"go.sia.tech/renterd/build"
	"go.sia.tech/renterd/internal/utils"
	"go.uber.org/zap"
)

type (
	Bus interface {
		AutopilotConfig(ctx context.Context) (api.AutopilotConfig, error)
		ConsensusState(ctx context.Context) (api.ConsensusState, error)
		GougingSettings(ctx context.Context) (gs api.GougingSettings, err error)
		Hosts(ctx context.Context, opts api.HostOptions) ([]api.Host, error)
		RecommendedFee(ctx context.Context) (types.Currency, error)
		ScanHost(ctx context.Context, hostKey types.PublicKey, timeout time.Duration) (api.HostScanResponse, error)
		SyncerPeers(ctx context.Context) (resp []string, err error)
		UploadSettings(ctx context.Context) (us api.UploadSettings, err error)
		Wallet(ctx context.Context) (api.WalletResponse, error)
	}

	Contractor interface {
		PerformContractMaintenance(context.Context, *contractor.MaintenanceState) (bool, error)
	}

	Migrator interface {
		Migrate(ctx context.Context)
		SignalMaintenanceFinished()
		Status() (bool, time.Time)
		Stop()
	}

	Pruner interface {
		PerformContractPruning(context.Context)
		Status() (bool, time.Time)
		Stop()
	}

	Scanner interface {
		Scan(ctx context.Context, hs scanner.HostScanner, force bool)
		Shutdown(ctx context.Context) error
		Status() (bool, time.Time)
		UpdateHostsConfig(cfg api.HostsConfig)
	}

	WalletMaintainer interface {
		PerformWalletMaintenance(ctx context.Context, cfg api.AutopilotConfig) error
	}
)

type autopilot struct {
	bus    Bus
	logger *zap.SugaredLogger

	contractor Contractor
	migrator   Migrator
	pruner     Pruner
	scanner    Scanner
	maintainer WalletMaintainer

	hearbeat time.Duration
	wg       sync.WaitGroup

	startStopMu       sync.Mutex
	startTime         time.Time
	shutdownCtx       context.Context
	shutdownCtxCancel context.CancelFunc
	ticker            *time.Ticker
	triggerChan       chan bool
}

// New initializes an Autopilot.
func New(ctx context.Context, cancel context.CancelFunc, b Bus, c Contractor, m Migrator, p Pruner, s Scanner, w WalletMaintainer, heartbeat time.Duration, logger *zap.Logger) *autopilot {
	return &autopilot{
		bus:    b,
		logger: logger.Named("autopilot").Sugar(),

		contractor: c,
		migrator:   m,
		pruner:     p,
		scanner:    s,
		maintainer: w,

		shutdownCtx:       ctx,
		shutdownCtxCancel: cancel,

		hearbeat: heartbeat,
	}
}

// Handler returns an HTTP handler that serves the autopilot api.
func (ap *autopilot) Handler() http.Handler {
	return jape.Mux(map[string]jape.Handler{
		"POST   /config/evaluate": ap.configEvaluateHandlerPOST,
		"GET    /state":           ap.stateHandlerGET,
		"POST   /trigger":         ap.triggerHandlerPOST,
	})
}

func (ap *autopilot) configEvaluateHandlerPOST(jc jape.Context) {
	ctx := jc.Request.Context()

	// decode request
	var req api.ConfigEvaluationRequest
	if jc.Decode(&req) != nil {
		return
	}

	// fetch necessary information
	reqCfg := req.AutopilotConfig
	gs := req.GougingSettings
	rs := req.RedundancySettings
	cs, err := ap.bus.ConsensusState(ctx)
	if jc.Check("failed to get consensus state", err) != nil {
		return
	}

	// fetch hosts
	hosts, err := ap.bus.Hosts(ctx, api.HostOptions{})
	if jc.Check("failed to get hosts", err) != nil {
		return
	}

	// evaluate the config
	res, err := contractor.EvaluateConfig(reqCfg, cs, rs, gs, hosts)
	if errors.Is(err, contractor.ErrMissingRequiredFields) {
		jc.Error(err, http.StatusBadRequest)
		return
	} else if err != nil {
		jc.Error(err, http.StatusInternalServerError)
		return
	}
	jc.Encode(res)
}

func (ap *autopilot) Run() {
	ap.startStopMu.Lock()
	if ap.isRunning() {
		ap.startStopMu.Unlock()
		return
	}
	ap.startTime = time.Now()
	ap.triggerChan = make(chan bool, 1)
	ap.ticker = time.NewTicker(ap.hearbeat)

	ap.wg.Add(1)
	defer ap.wg.Done()
	ap.startStopMu.Unlock()

	// block until the autopilot is online
	if online := ap.blockUntilOnline(); !online {
		ap.logger.Error("autopilot stopped before it was able to come online")
		return
	}

	// schedule a trigger when the wallet receives its first deposit
	if err := ap.tryScheduleTriggerWhenFunded(); err != nil {
		if !errors.Is(err, context.Canceled) {
			ap.logger.Error(err)
		}
		return
	}

	var forceScan bool
	for !ap.isStopped() {
		ap.logger.Info("autopilot iteration starting")
		tickerFired := make(chan struct{})
		ap.performMaintenance(forceScan, tickerFired)
		select {
		case <-ap.shutdownCtx.Done():
			return
		case forceScan = <-ap.triggerChan:
			ap.logger.Info("autopilot iteration triggered")
			ap.ticker.Reset(ap.hearbeat)
		case <-ap.ticker.C:
		case <-tickerFired:
		}
	}
}

// Shutdown shuts down the autopilot.
func (ap *autopilot) Shutdown(ctx context.Context) error {
	ap.startStopMu.Lock()
	defer ap.startStopMu.Unlock()

	if ap.isRunning() {
		ap.ticker.Stop()
		ap.shutdownCtxCancel()
		close(ap.triggerChan)
		ap.wg.Wait()
		ap.migrator.Stop()
		ap.pruner.Stop()
		ap.scanner.Shutdown(ctx)
		ap.startTime = time.Time{}
	}
	return nil
}

func (ap *autopilot) StartTime() time.Time {
	ap.startStopMu.Lock()
	defer ap.startStopMu.Unlock()
	return ap.startTime
}

func (ap *autopilot) Trigger(forceScan bool) bool {
	ap.startStopMu.Lock()
	defer ap.startStopMu.Unlock()

	select {
	case ap.triggerChan <- forceScan:
		return true
	default:
		return false
	}
}

func (ap *autopilot) Uptime() (dur time.Duration) {
	ap.startStopMu.Lock()
	defer ap.startStopMu.Unlock()
	if ap.isRunning() {
		dur = time.Since(ap.startTime)
	}
	return
}

func (ap *autopilot) blockUntilEnabled(interrupt <-chan time.Time) (enabled, interrupted bool) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	var once sync.Once

	for {
		apCfg, err := ap.bus.AutopilotConfig(ap.shutdownCtx)
		if err != nil && !errors.Is(err, context.Canceled) {
			ap.logger.Errorf("unable to fetch autopilot from the bus, err: %v", err)
		}

		if err != nil || !apCfg.Enabled {
			once.Do(func() { ap.logger.Info("autopilot is waiting to be enabled...") })
			select {
			case <-ap.shutdownCtx.Done():
				return false, false
			case <-interrupt:
				return false, true
			case <-ticker.C:
				continue
			}
		}
		return true, false
	}
}

func (ap *autopilot) blockUntilOnline() (online bool) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	var once sync.Once

	for {
		ctx, cancel := context.WithTimeout(ap.shutdownCtx, 30*time.Second)
		peers, err := ap.bus.SyncerPeers(ctx)
		online = len(peers) > 0
		cancel()

		if utils.IsErr(err, context.Canceled) {
			return
		} else if err != nil {
			ap.logger.Errorf("failed to get peers, err: %v", err)
		} else if !online {
			once.Do(func() { ap.logger.Info("autopilot is waiting on the bus to connect to peers...") })
		}

		if err != nil || !online {
			select {
			case <-ap.shutdownCtx.Done():
				return
			case <-ticker.C:
				continue
			}
		}
		return
	}
}

func (ap *autopilot) blockUntilSynced(interrupt <-chan time.Time) (synced, blocked, interrupted bool) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	var once sync.Once

	for {
		// try and fetch consensus
		ctx, cancel := context.WithTimeout(ap.shutdownCtx, 30*time.Second)
		cs, err := ap.bus.ConsensusState(ctx)
		synced = cs.Synced
		cancel()

		// if an error occurred, or if we're not synced, we continue
		if utils.IsErr(err, context.Canceled) {
			return
		} else if err != nil {
			ap.logger.Errorf("failed to get consensus state, err: %v", err)
		} else if !synced {
			once.Do(func() { ap.logger.Info("autopilot is waiting for consensus to sync...") })
		}

		if err != nil || !synced {
			blocked = true
			select {
			case <-ap.shutdownCtx.Done():
				return
			case <-interrupt:
				interrupted = true
				return
			case <-ticker.C:
				continue
			}
		}
		return
	}
}

func (ap *autopilot) performMaintenance(forceScan bool, tickerFired chan struct{}) {
	defer ap.logger.Info("autopilot iteration ended")

	// initiate a host scan - no need to be synced or configured for scanning
	ap.scanner.Scan(ap.shutdownCtx, ap.bus, forceScan)

	// reset forceScans
	forceScan = false

	// block until consensus is synced
	if synced, blocked, interrupted := ap.blockUntilSynced(ap.ticker.C); !synced {
		if interrupted {
			close(tickerFired)
			return
		}
		ap.logger.Info("autopilot stopped before consensus was synced")
		return
	} else if blocked {
		if scanning, _ := ap.scanner.Status(); !scanning {
			ap.scanner.Scan(ap.shutdownCtx, ap.bus, true)
		}
	}

	// block until the autopilot is enabled
	if enabled, interrupted := ap.blockUntilEnabled(ap.ticker.C); !enabled {
		if interrupted {
			close(tickerFired)
			return
		}
		ap.logger.Info("autopilot stopped before it was able to confirm it was enabled in the bus")
		return
	}

	// fetch autopilot config
	apCfg, err := ap.bus.AutopilotConfig(ap.shutdownCtx)
	if err != nil {
		ap.logger.Errorf("aborting maintenance, failed to fetch autopilot", zap.Error(err))
		return
	}

	// update the scanner with the hosts config
	ap.scanner.UpdateHostsConfig(apCfg.Hosts)

	// perform wallet maintenance
	err = ap.maintainer.PerformWalletMaintenance(ap.shutdownCtx, apCfg)
	if err != nil && utils.IsErr(err, context.Canceled) {
		return
	} else if err != nil {
		ap.logger.Errorf("wallet maintenance failed, err: %v", err)
	}

	// build maintenance state
	buildState, err := ap.buildState(ap.shutdownCtx)
	if err != nil {
		ap.logger.Errorf("aborting maintenance, failed to build state, err: %v", err)
		return
	}

	// perform maintenance
	setChanged, err := ap.contractor.PerformContractMaintenance(ap.shutdownCtx, buildState)
	if err != nil && utils.IsErr(err, context.Canceled) {
		return
	} else if err != nil {
		ap.logger.Errorf("contract maintenance failed, err: %v", err)
	}
	maintenanceSuccess := err == nil

	// upon success, notify the migrator. The health of slabs might have
	// changed.
	if maintenanceSuccess && setChanged {
		ap.migrator.SignalMaintenanceFinished()
	}

	// migration
	ap.migrator.Migrate(ap.shutdownCtx)

	// pruning
	if ap.pruner != nil {
		ap.pruner.PerformContractPruning(ap.shutdownCtx)
	} else {
		ap.logger.Info("pruning disabled")
	}
}

func (ap *autopilot) tryScheduleTriggerWhenFunded() error {
	// apply sane timeout
	ctx, cancel := context.WithTimeout(ap.shutdownCtx, time.Minute)
	defer cancel()

	// no need to schedule a trigger if the wallet is already funded
	wallet, err := ap.bus.Wallet(ctx)
	if err != nil {
		return err
	} else if !wallet.Confirmed.Add(wallet.Unconfirmed).IsZero() {
		return nil
	}

	// spin a goroutine that triggers the autopilot when we receive a deposit
	ap.logger.Info("autopilot loop trigger is scheduled for when the wallet receives a deposit")
	ap.wg.Add(1)
	go func() {
		defer ap.wg.Done()

		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ap.shutdownCtx.Done():
				return
			case <-ticker.C:
			}

			// fetch wallet info
			ctx, cancel := context.WithTimeout(ap.shutdownCtx, 30*time.Second)
			if wallet, err = ap.bus.Wallet(ctx); err != nil {
				ap.logger.Errorf("failed to get wallet info, err: %v", err)
			}
			cancel()

			// if we have received a deposit, trigger the autopilot
			if !wallet.Confirmed.Add(wallet.Unconfirmed).IsZero() {
				if ap.Trigger(false) {
					return
				}
			}
		}
	}()

	return nil
}

func (ap *autopilot) isRunning() bool {
	return !ap.startTime.IsZero()
}

func (ap *autopilot) isStopped() bool {
	select {
	case <-ap.shutdownCtx.Done():
		return true
	default:
		return false
	}
}

func (ap *autopilot) triggerHandlerPOST(jc jape.Context) {
	var req api.AutopilotTriggerRequest
	if jc.Decode(&req) != nil {
		return
	}
	jc.Encode(api.AutopilotTriggerResponse{
		Triggered: ap.Trigger(req.ForceScan),
	})
}

func (ap *autopilot) stateHandlerGET(jc jape.Context) {
	pruning, pLastStart := ap.pruner.Status()
	migrating, mLastStart := ap.migrator.Status()
	scanning, sLastStart := ap.scanner.Status()

	cfg, err := ap.bus.AutopilotConfig(jc.Request.Context())
	if err != nil {
		jc.Error(err, http.StatusInternalServerError)
		return
	}

	jc.Encode(api.AutopilotStateResponse{
		Enabled:            cfg.Enabled,
		Migrating:          migrating,
		MigratingLastStart: api.TimeRFC3339(mLastStart),
		Pruning:            pruning,
		PruningLastStart:   api.TimeRFC3339(pLastStart),
		Scanning:           scanning,
		ScanningLastStart:  api.TimeRFC3339(sLastStart),
		UptimeMS:           api.DurationMS(ap.Uptime()),

		StartTime: api.TimeRFC3339(ap.StartTime()),
		BuildState: api.BuildState{
			Version:   build.Version(),
			Commit:    build.Commit(),
			OS:        runtime.GOOS,
			BuildTime: api.TimeRFC3339(build.BuildTime()),
		},
	})
}

func (ap *autopilot) buildState(ctx context.Context) (*contractor.MaintenanceState, error) {
	// fetch autopilot config
	apCfg, err := ap.bus.AutopilotConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("could not fetch autopilot config, err: %v", err)
	}

	// fetch consensus state
	cs, err := ap.bus.ConsensusState(ctx)
	if err != nil {
		return nil, fmt.Errorf("could not fetch consensus state, err: %v", err)
	} else if !cs.Synced {
		return nil, errors.New("consensus not synced")
	}

	// fetch upload settings
	us, err := ap.bus.UploadSettings(ctx)
	if err != nil {
		return nil, fmt.Errorf("could not fetch upload settings, err: %v", err)
	}

	// fetch gouging settings
	gs, err := ap.bus.GougingSettings(ctx)
	if err != nil {
		return nil, fmt.Errorf("could not fetch gouging settings, err: %v", err)
	}

	// fetch recommended transaction fee
	fee, err := ap.bus.RecommendedFee(ctx)
	if err != nil {
		return nil, fmt.Errorf("could not fetch fee, err: %v", err)
	}

	// fetch our wallet address
	wi, err := ap.bus.Wallet(ctx)
	if err != nil {
		return nil, fmt.Errorf("could not fetch wallet address, err: %v", err)
	}
	address := wi.Address

	// no need to try and form contracts if wallet is completely empty
	skipContractFormations := wi.Confirmed.IsZero() && wi.Unconfirmed.IsZero()
	if skipContractFormations {
		ap.logger.Warn("contract formations skipped, wallet is empty")
	}

	return &contractor.MaintenanceState{
		GS: gs,
		RS: us.Redundancy,
		AP: apCfg,

		Address:                address,
		Fee:                    fee,
		SkipContractFormations: skipContractFormations,
	}, nil
}
