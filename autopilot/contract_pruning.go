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
	"go.sia.tech/siad/build"
	"go.uber.org/zap"
)

var (
	errInvalidHandshake          = errors.New("couldn't read host's handshake")
	errInvalidHandshakeSignature = errors.New("host's handshake signature was invalid")
	errInvalidMerkleProof        = errors.New("host supplied invalid Merkle proof")
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

func (ap *Autopilot) performContractPruning(wp *workerPool) {
	ap.logger.Info("performing contract pruning")

	// fetch prunable contracts
	prunable, err := ap.fetchPrunableContracts()
	if err != nil {
		ap.logger.Error(err)
		return
	}

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
			continue // contract got archived
		} else if err != nil {
			ap.logger.Errorf("failed to fetch host for contract '%v', err %v", contract.ID, err)
			continue
		}

		// prune contract using a random worker
		wp.withWorker(func(w Worker) {
			total += ap.pruneContract(w, contract.ID, h.PublicKey, h.Settings.Version, h.Settings.Release)
		})
	}

	// log total pruned
	ap.logger.Info(fmt.Sprintf("pruned %d (%s) from %v contracts", total, humanReadableSize(int(total)), len(prunable)))
}

func (ap *Autopilot) pruneContract(w Worker, fcid types.FileContractID, hk types.PublicKey, hostVersion, hostRelease string) uint64 {
	// use a sane timeout
	ctx, cancel := context.WithTimeout(ap.shutdownCtx, timeoutPruneContract+5*time.Minute)
	defer cancel()

	// prune the contract
	start := time.Now()
	pruned, remaining, err := w.RHPPruneContract(ctx, fcid, timeoutPruneContract)
	duration := time.Since(start)

	// ignore slow pruning until host network is 1.6.0+
	if utils.IsErr(err, context.DeadlineExceeded) && pruned > 0 {
		err = nil
	}

	// handle metrics
	if err == nil || pruned > 0 {
		if err := ap.bus.RecordContractPruneMetric(ctx, api.ContractPruneMetric{
			Timestamp: api.TimeRFC3339(start),

			ContractID:  fcid,
			HostKey:     hk,
			HostVersion: hostVersion,

			Pruned:    pruned,
			Remaining: remaining,
			Duration:  duration,
		}); err != nil {
			ap.logger.Error(err)
		}
	}

	// handle logs
	log := ap.logger.With("contract", fcid, "host", hk, "version", hostVersion, "release", hostRelease, "pruned", pruned, "remaining", remaining, "elapsed", duration)
	if err != nil && pruned > 0 {
		log.With(zap.Error(err)).Error("unexpected error interrupted pruning")
	} else if err != nil {
		log.With(zap.Error(err)).Error("failed to prune contract")
	} else {
		log.Info("successfully pruned contract")
	}

	// handle alerts
	ap.mu.Lock()
	defer ap.mu.Unlock()
	alertID := alerts.IDForContract(alertPruningID, fcid)
	if shouldSendPruneAlert(err, hostVersion, hostRelease) {
		ap.RegisterAlert(ctx, newContractPruningFailedAlert(hk, hostVersion, hostRelease, fcid, err))
		ap.pruningAlertIDs[fcid] = alertID // store id to dismiss stale alerts
	} else {
		ap.DismissAlert(ctx, alertID)
		delete(ap.pruningAlertIDs, fcid)
	}

	return pruned
}

func (ap *Autopilot) tryPerformPruning(wp *workerPool) {
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
		ap.performContractPruning(wp)
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
	merkleRootIssue := utils.IsErr(err, errInvalidMerkleProof) &&
		(build.VersionCmp(version, "1.6.0") < 0 || version == "1.6.0" && release == "")
	return err != nil && !(merkleRootIssue ||
		utils.IsErr(err, utils.ErrConnectionRefused) ||
		utils.IsErr(err, utils.ErrConnectionTimedOut) ||
		utils.IsErr(err, utils.ErrConnectionResetByPeer) ||
		utils.IsErr(err, errInvalidHandshakeSignature) ||
		utils.IsErr(err, errInvalidHandshake) ||
		utils.IsErr(err, utils.ErrNoRouteToHost) ||
		utils.IsErr(err, utils.ErrNoSuchHost))
}
