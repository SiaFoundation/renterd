package autopilot

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/alerts"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/internal/utils"
	"go.uber.org/zap"
)

var (
	errInvalidHandshake          = errors.New("couldn't read host's handshake")
	errInvalidHandshakeSignature = errors.New("host's handshake signature was invalid")
	errInvalidMerkleProof        = errors.New("host supplied invalid Merkle proof")
	errInvalidSectorRootsRange   = errors.New("number of roots does not match range")
)

const (
	// timeoutPruneContract defines the maximum amount of time we lock a
	// contract for pruning
	timeoutPruneContract = 10 * time.Minute
)

func (ap *Autopilot) dismissPruneAlerts(prunable []api.ContractPrunableData) {
	ap.mu.Lock()
	defer ap.mu.Unlock()

	// use a sane timeout
	ctx, cancel := context.WithTimeout(ap.shutdownCtx, 5*time.Minute)
	defer cancel()

	// fetch contract ids that are prunable
	prunableIDs := make(map[types.FileContractID]struct{})
	for _, contract := range prunable {
		prunableIDs[contract.ID] = struct{}{}
	}

	// dismiss alerts for contracts that are no longer prunable
	for fcid, alertID := range ap.pruningAlertIDs {
		if _, ok := prunableIDs[fcid]; !ok {
			ap.DismissAlert(ctx, alertID)
			delete(ap.pruningAlertIDs, fcid)
		}
	}
}

func (ap *Autopilot) fetchPrunableContracts() (prunable []api.ContractPrunableData, _ error) {
	// use a sane timeout
	ctx, cancel := context.WithTimeout(ap.shutdownCtx, time.Minute)
	defer cancel()

	// fetch prunable data
	res, err := ap.bus.PrunableData(ctx)
	if err != nil {
		return nil, err
	} else if res.TotalPrunable == 0 {
		return nil, nil
	}

	// fetch autopilot
	autopilot, err := ap.bus.Autopilot(ctx, ap.id)
	if err != nil {
		return nil, err
	}

	// fetch contract set contracts
	csc, err := ap.bus.Contracts(ctx, api.ContractsOpts{ContractSet: autopilot.Config.Contracts.Set})
	if err != nil {
		return nil, err
	}

	// build a map of in-set contracts
	contracts := make(map[types.FileContractID]struct{})
	for _, contract := range csc {
		contracts[contract.ID] = struct{}{}
	}

	// filter out contracts that are not in the set
	for _, contract := range res.Contracts {
		if _, ok := contracts[contract.ID]; ok && contract.Prunable > 0 {
			prunable = append(prunable, contract)
		}
	}
	return
}

func (ap *Autopilot) fetchHostContract(fcid types.FileContractID) (host api.Host, metadata api.ContractMetadata, err error) {
	// use a sane timeout
	ctx, cancel := context.WithTimeout(ap.shutdownCtx, time.Minute)
	defer cancel()

	// fetch the contract
	metadata, err = ap.bus.Contract(ctx, fcid)
	if err != nil {
		return
	}

	// fetch the host
	host, err = ap.bus.Host(ctx, metadata.HostKey)
	return
}

func (ap *Autopilot) performContractPruning() {
	log := ap.logger.Named("performContractPruning")
	log.Info("performing contract pruning")

	// fetch prunable contracts
	prunable, err := ap.fetchPrunableContracts()
	if err != nil {
		log.Error(err)
		return
	}
	log.Debugf("found %d prunable contracts", len(prunable))

	// dismiss alerts for contracts that are no longer prunable
	ap.dismissPruneAlerts(prunable)

	// loop prunable contracts
	var total uint64
	for _, contract := range prunable {
		// check if we're stopped
		if ap.isStopped() {
			break
		}

		// fetch host
		h, _, err := ap.fetchHostContract(contract.ID)
		if utils.IsErr(err, api.ErrContractNotFound) {
			log.Debugw("contract got archived", "contract", contract.ID)
			continue // contract got archived
		} else if err != nil {
			log.Errorw("failed to fetch host", zap.Error(err), "contract", contract.ID)
			continue
		}

		// prune contract
		n, err := ap.pruneContract(ap.shutdownCtx, contract.ID, h.PublicKey, h.Settings.Version, h.Settings.Release, log)
		if err != nil {
			log.Errorw("failed to prune contract", zap.Error(err), "contract", contract.ID)
			continue
		}

		// handle alerts
		ap.mu.Lock()
		alertID := alerts.IDForContract(alertPruningID, contract.ID)
		if shouldSendPruneAlert(err, h.Settings.Version, h.Settings.Release) {
			ap.RegisterAlert(ap.shutdownCtx, newContractPruningFailedAlert(h.PublicKey, h.Settings.Version, h.Settings.Release, contract.ID, err))
			ap.pruningAlertIDs[contract.ID] = alertID // store id to dismiss stale alerts
		} else {
			ap.DismissAlert(ap.shutdownCtx, alertID)
			delete(ap.pruningAlertIDs, contract.ID)
		}
		ap.mu.Unlock()

		// adjust total
		total += n
	}

	// log total pruned
	log.Info(fmt.Sprintf("pruned %d (%s) from %v contracts", total, humanReadableSize(int(total)), len(prunable)))
}

func (ap *Autopilot) pruneContract(ctx context.Context, fcid types.FileContractID, hk types.PublicKey, hostVersion, hostRelease string, logger *zap.SugaredLogger) (uint64, error) {
	// define logger
	log := logger.With(
		zap.Stringer("contract", fcid),
		zap.Stringer("host", hk),
		zap.String("version", hostVersion),
		zap.String("release", hostRelease))

	// prune the contract
	start := time.Now()
	res, err := ap.bus.PruneContract(ctx, fcid, timeoutPruneContract)
	if err != nil {
		return 0, err
	}

	// decorate logger
	log = log.With(
		zap.String("pruned", utils.HumanReadableSize(int(res.Pruned))),
		zap.String("remaining", utils.HumanReadableSize(int(res.Remaining))),
		zap.String("size", utils.HumanReadableSize(int(res.ContractSize))),
		zap.Duration("elapsed", time.Since(start)),
	)

	// ignore slow pruning until host network is 1.6.0+
	if res.Error != "" && utils.IsErr(errors.New(res.Error), context.DeadlineExceeded) && res.Pruned > 0 {
		res.Error = ""
	}

	// handle metrics
	if res.Pruned > 0 {
		if err := ap.bus.RecordContractPruneMetric(ctx, api.ContractPruneMetric{
			Timestamp: api.TimeRFC3339(start),

			ContractID:  fcid,
			HostKey:     hk,
			HostVersion: hostVersion,

			Pruned:    res.Pruned,
			Remaining: res.Remaining,
			Duration:  time.Since(start),
		}); err != nil {
			ap.logger.Error(err)
		}
	}

	// handle logs
	if res.Error != "" {
		log.Errorw("unexpected error interrupted pruning", zap.Error(errors.New(res.Error)))
	} else {
		log.Info("successfully pruned contract")
	}

	return res.Pruned, nil
}

func (ap *Autopilot) tryPerformPruning() {
	ap.mu.Lock()
	if ap.pruning || ap.isStopped() {
		ap.mu.Unlock()
		return
	}
	ap.pruning = true
	ap.pruningLastStart = time.Now()
	ap.mu.Unlock()

	ap.wg.Add(1)
	go func() {
		defer ap.wg.Done()
		ap.performContractPruning()
		ap.mu.Lock()
		ap.pruning = false
		ap.mu.Unlock()
	}()
}

func humanReadableSize(b int) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB",
		float64(b)/float64(div), "KMGTPE"[exp])
}

func shouldSendPruneAlert(err error, version, release string) bool {
	oldHost := (utils.VersionCmp(version, "1.6.0") < 0 || version == "1.6.0" && release == "")
	sectorRootsIssue := utils.IsErr(err, errInvalidSectorRootsRange) && oldHost
	merkleRootIssue := utils.IsErr(err, errInvalidMerkleProof) && oldHost
	return err != nil && !(sectorRootsIssue || merkleRootIssue ||
		utils.IsErr(err, utils.ErrConnectionRefused) ||
		utils.IsErr(err, utils.ErrConnectionTimedOut) ||
		utils.IsErr(err, utils.ErrConnectionResetByPeer) ||
		utils.IsErr(err, errInvalidHandshakeSignature) ||
		utils.IsErr(err, errInvalidHandshake) ||
		utils.IsErr(err, utils.ErrNoRouteToHost) ||
		utils.IsErr(err, utils.ErrNoSuchHost))
}
