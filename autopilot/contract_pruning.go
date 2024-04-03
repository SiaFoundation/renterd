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
)

var (
	errInvalidHandshakeSignature = errors.New("host's handshake signature was invalid")
	errInvalidMerkleProof        = errors.New("host supplied invalid Merkle proof")
)

const (
	// timeoutPruneContract is the amount of time we wait for a contract to get
	// pruned.
	timeoutPruneContract = 10 * time.Minute
)

type (
	pruneResult struct {
		ts time.Time

		fcid    types.FileContractID
		hk      types.PublicKey
		version string

		pruned    uint64
		remaining uint64
		duration  time.Duration

		err error
	}

	pruneMetrics []api.ContractPruneMetric
)

func (pr pruneResult) String() string {
	msg := fmt.Sprintf("contract %v", pr.fcid)
	if pr.hk != (types.PublicKey{}) {
		msg += fmt.Sprintf(", host %v version %s", pr.hk, pr.version)
	}
	if pr.pruned > 0 {
		msg += fmt.Sprintf(", pruned %d bytes, remaining %d bytes, elapsed %v", pr.pruned, pr.remaining, pr.duration)
	}
	if pr.err != nil {
		msg += fmt.Sprintf(", err: %v", pr.err)
	}
	return msg
}

func (pm pruneMetrics) String() string {
	var total uint64
	for _, m := range pm {
		total += m.Pruned
	}
	return fmt.Sprintf("pruned %d (%s) from %v contracts", total, humanReadableSize(int(total)), len(pm))
}

func (pr pruneResult) toAlert() (id types.Hash256, alert *alerts.Alert) {
	id = alerts.IDForContract(alertPruningID, pr.fcid)

	if shouldTrigger := pr.err != nil && !((utils.IsErr(pr.err, errInvalidMerkleProof) && build.VersionCmp(pr.version, "1.6.0") < 0) ||
		utils.IsErr(pr.err, api.ErrContractNotFound) || // contract got archived
		utils.IsErr(pr.err, utils.ErrConnectionRefused) ||
		utils.IsErr(pr.err, utils.ErrConnectionTimedOut) ||
		utils.IsErr(pr.err, utils.ErrConnectionResetByPeer) ||
		utils.IsErr(pr.err, errInvalidHandshakeSignature) ||
		utils.IsErr(pr.err, utils.ErrNoRouteToHost) ||
		utils.IsErr(pr.err, utils.ErrNoSuchHost)); shouldTrigger {
		alert = newContractPruningFailedAlert(pr.hk, pr.version, pr.fcid, pr.err)
	}
	return
}

func (pr pruneResult) toMetric() api.ContractPruneMetric {
	return api.ContractPruneMetric{
		Timestamp:  api.TimeRFC3339(pr.ts),
		ContractID: pr.fcid,
		HostKey:    pr.hk,
		Pruned:     pr.pruned,
		Remaining:  pr.remaining,
		Duration:   pr.duration,
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

func (ap *Autopilot) hostForContract(ctx context.Context, fcid types.FileContractID) (host api.Host, metadata api.ContractMetadata, err error) {
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
	} else if len(prunable) == 0 {
		ap.logger.Info("no contracts to prune")
		return
	}

	// prune every contract individually, one at a time and for a maximum
	// duration of 'timeoutPruneContract' to limit the amount of time we lock
	// the contract as contracts on old hosts can take a long time to prune
	var metrics pruneMetrics
	wp.withWorker(func(w Worker) {
		for _, contract := range prunable {
			// return if we're stopped
			if ap.isStopped() {
				return
			}

			// prune contract
			result := ap.pruneContract(w, contract.ID)
			if result.err != nil {
				ap.logger.Error(result)
			} else {
				ap.logger.Info(result)
			}

			// handle alert
			ctx, cancel := context.WithTimeout(ap.shutdownCtx, time.Minute)
			if id, alert := result.toAlert(); alert != nil {
				ap.RegisterAlert(ctx, *alert)
			} else {
				ap.DismissAlert(ctx, id)
			}
			cancel()

			// handle metrics
			metrics = append(metrics, result.toMetric())
		}
	})

	// record metrics
	ctx, cancel := context.WithTimeout(ap.shutdownCtx, time.Minute)
	if err := ap.bus.RecordContractPruneMetric(ctx, metrics...); err != nil {
		ap.logger.Error(err)
	}
	cancel()

	// log metrics
	ap.logger.Info(metrics)
}

func (ap *Autopilot) pruneContract(w Worker, fcid types.FileContractID) pruneResult {
	// create a sane timeout
	ctx, cancel := context.WithTimeout(ap.shutdownCtx, 2*timeoutPruneContract)
	defer cancel()

	// fetch the host
	host, _, err := ap.hostForContract(ctx, fcid)
	if err != nil {
		return pruneResult{fcid: fcid, err: err}
	}

	// prune the contract
	start := time.Now()
	pruned, remaining, err := w.RHPPruneContract(ctx, fcid, timeoutPruneContract)
	if err != nil && pruned == 0 {
		return pruneResult{fcid: fcid, hk: host.PublicKey, version: host.Settings.Version, err: err}
	} else if err != nil && utils.IsErr(err, context.DeadlineExceeded) {
		err = nil
	}

	return pruneResult{
		ts: start,

		fcid:    fcid,
		hk:      host.PublicKey,
		version: host.Settings.Version,

		pruned:    pruned,
		remaining: remaining,
		duration:  time.Since(start),

		err: err,
	}
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
