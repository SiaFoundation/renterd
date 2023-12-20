package autopilot

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/alerts"
	"go.sia.tech/renterd/api"
	"go.sia.tech/siad/build"
)

var (
	errConnectionRefused         = errors.New("connection refused")
	errConnectionTimedOut        = errors.New("connection timed out")
	errConnectionResetByPeer     = errors.New("connection reset by peer")
	errInvalidHandshakeSignature = errors.New("host's handshake signature was invalid")
	errInvalidMerkleProof        = errors.New("host supplied invalid Merkle proof")
	errNoRouteToHost             = errors.New("no route to host")
	errNoSuchHost                = errors.New("no such host")
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
	id = alertIDForContract(alertPruningID, pr.fcid)

	if shouldTrigger := pr.err != nil && !((isErr(pr.err, errInvalidMerkleProof) && build.VersionCmp(pr.version, "1.6.0") < 0) ||
		isErr(pr.err, api.ErrContractNotFound) || // contract got archived
		isErr(pr.err, errConnectionRefused) ||
		isErr(pr.err, errConnectionTimedOut) ||
		isErr(pr.err, errConnectionResetByPeer) ||
		isErr(pr.err, errInvalidHandshakeSignature) ||
		isErr(pr.err, errNoRouteToHost) ||
		isErr(pr.err, errNoSuchHost)); shouldTrigger {
		alert = newContractPruningFailedAlert(pr.hk, pr.version, pr.fcid, pr.err)
	}
	return
}

func (pr pruneResult) toMetric() api.ContractPruneMetric {
	return api.ContractPruneMetric{
		Timestamp:  pr.ts,
		ContractID: pr.fcid,
		HostKey:    pr.hk,
		Pruned:     pr.pruned,
		Remaining:  pr.remaining,
		Duration:   pr.duration,
	}
}

func (c *contractor) fetchPrunableContracts() (prunable []api.ContractPrunableData, _ error) {
	// use a sane timeout
	ctx, cancel := context.WithTimeout(c.ap.shutdownCtx, time.Minute)
	defer cancel()

	// fetch prunable data
	res, err := c.ap.bus.PrunableData(ctx)
	if err != nil {
		return nil, err
	} else if res.TotalPrunable == 0 {
		return nil, nil
	}

	// fetch contract set contracts
	csc, err := c.ap.bus.Contracts(ctx, api.ContractsOpts{ContractSet: c.ap.state.cfg.Contracts.Set})
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

func (c *contractor) performContractPruning(wp *workerPool) {
	c.logger.Info("performing contract pruning")

	// fetch prunable contracts
	prunable, err := c.fetchPrunableContracts()
	if err != nil {
		c.logger.Error(err)
		return
	} else if len(prunable) == 0 {
		c.logger.Info("no contracts to prune")
		return
	}

	// prune every contract individually, one at a time and for a maximum
	// duration of 'timeoutPruneContract' to limit the amount of time we lock
	// the contract as contracts on old hosts can take a long time to prune
	var metrics pruneMetrics
	wp.withWorker(func(w Worker) {
		for _, contract := range prunable {
			// return if we're stopped
			if c.ap.isStopped() {
				return
			}

			// prune contract
			result := c.pruneContract(w, contract.ID)
			if result.err != nil {
				c.logger.Error(result)
			} else {
				c.logger.Info(result)
			}

			// handle alert
			ctx, cancel := context.WithTimeout(c.ap.shutdownCtx, time.Minute)
			if id, alert := result.toAlert(); alert != nil {
				c.ap.RegisterAlert(ctx, *alert)
			} else {
				c.ap.DismissAlert(ctx, id)
			}
			cancel()

			// handle metrics
			metrics = append(metrics, result.toMetric())
		}
	})

	// record metrics
	ctx, cancel := context.WithTimeout(c.ap.shutdownCtx, time.Minute)
	if err := c.ap.bus.RecordContractPruneMetric(ctx, metrics...); err != nil {
		c.logger.Error(err)
	}
	cancel()

	// log metrics
	c.logger.Info(metrics)
}

func (c *contractor) pruneContract(w Worker, fcid types.FileContractID) pruneResult {
	// create a sane timeout
	ctx, cancel := context.WithTimeout(c.ap.shutdownCtx, 2*timeoutPruneContract)
	defer cancel()

	// fetch the host
	host, _, err := c.hostForContract(ctx, fcid)
	if err != nil {
		return pruneResult{fcid: fcid, err: err}
	}

	// prune the contract
	start := time.Now()
	pruned, remaining, err := w.RHPPruneContract(ctx, fcid, timeoutPruneContract)
	if err != nil && pruned == 0 {
		return pruneResult{fcid: fcid, hk: host.PublicKey, version: host.Settings.Version, err: err}
	} else if err != nil && isErr(err, context.DeadlineExceeded) {
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
