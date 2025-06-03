package pruner

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/v2/api"
	"go.sia.tech/renterd/v2/internal/utils"
	"go.uber.org/zap"
)

const (
	// timeoutPruneContract defines the maximum amount of time we lock a
	// contract for pruning
	timeoutPruneContract = 10 * time.Minute
)

type (
	Bus interface {
		Contract(ctx context.Context, id types.FileContractID) (api.ContractMetadata, error)
		Contracts(ctx context.Context, opts api.ContractsOpts) (contracts []api.ContractMetadata, err error)
		Host(ctx context.Context, hostKey types.PublicKey) (api.Host, error)
		PrunableData(ctx context.Context) (prunableData api.ContractsPrunableDataResponse, err error)
		PruneContract(ctx context.Context, id types.FileContractID, timeout time.Duration) (api.ContractPruneResponse, error)
		RecordContractPruneMetric(ctx context.Context, metrics ...api.ContractPruneMetric) error
	}
)

type Pruner struct {
	bus    Bus
	logger *zap.SugaredLogger

	wg sync.WaitGroup

	mu               sync.Mutex
	pruning          bool
	pruningLastStart time.Time
}

func New(bus Bus, logger *zap.Logger) *Pruner {
	return &Pruner{
		bus:    bus,
		logger: logger.Named("pruner").Sugar(),
	}
}

func (p *Pruner) PerformContractPruning(ctx context.Context) {
	p.mu.Lock()
	if p.pruning {
		p.mu.Unlock()
		return
	}
	p.pruning = true
	p.pruningLastStart = time.Now()
	p.mu.Unlock()

	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		p.performContractPruning(ctx)
		p.mu.Lock()
		p.pruning = false
		p.mu.Unlock()
	}()
}

func (p *Pruner) Status() (bool, time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.pruning, p.pruningLastStart
}

func (p *Pruner) Shutdown(_ context.Context) error {
	p.wg.Wait()
	return nil
}

func (p *Pruner) fetchPrunableContracts(ctx context.Context) (prunable []api.ContractPrunableData, _ error) {
	// use a sane timeout
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()

	// fetch prunable data
	res, err := p.bus.PrunableData(ctx)
	if err != nil {
		return nil, err
	} else if res.TotalPrunable == 0 {
		return nil, nil
	}

	// fetch good contracts
	contracts, err := p.bus.Contracts(ctx, api.ContractsOpts{FilterMode: api.ContractFilterModeGood})
	if err != nil {
		return nil, err
	}

	// build a map of good contracts
	good := make(map[types.FileContractID]struct{})
	for _, c := range contracts {
		good[c.ID] = struct{}{}
	}

	// filter out contracts that are not good
	for _, c := range res.Contracts {
		if _, ok := good[c.ID]; ok && c.Prunable > 0 {
			prunable = append(prunable, c)
		}
	}
	return
}

func (p *Pruner) fetchHostContract(ctx context.Context, fcid types.FileContractID) (host api.Host, metadata api.ContractMetadata, err error) {
	// use a sane timeout
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()

	// fetch the contract
	metadata, err = p.bus.Contract(ctx, fcid)
	if err != nil {
		return
	}

	// fetch the host
	host, err = p.bus.Host(ctx, metadata.HostKey)
	return
}

func (p *Pruner) performContractPruning(ctx context.Context) {
	log := p.logger.Named("performContractPruning")
	log.Info("performing contract pruning")

	// fetch prunable contracts
	prunable, err := p.fetchPrunableContracts(ctx)
	if err != nil {
		log.Error(err)
		return
	}
	log.Debugf("found %d prunable contracts", len(prunable))

	// loop prunable contracts
	var total uint64
	for _, contract := range prunable {
		// fetch host
		h, _, err := p.fetchHostContract(ctx, contract.ID)
		if utils.IsErr(err, api.ErrContractNotFound) {
			log.Debugw("contract got archived", "contract", contract.ID)
			continue // contract got archived
		} else if err != nil {
			log.Errorw("failed to fetch host", zap.Error(err), "contract", contract.ID)
			continue
		}

		// prune contract
		n, err := p.pruneContract(ctx, contract.ID, h.PublicKey, log)
		if err != nil {
			log.Errorw("failed to prune contract", zap.Error(err), "contract", contract.ID)
			continue
		}

		// adjust total
		total += n
	}

	// log total pruned
	if total > 0 {
		log.Info(fmt.Sprintf("pruned %d (%s) from %v contracts", total, utils.HumanReadableSize(int(total)), len(prunable)))
	}
}

func (p *Pruner) pruneContract(ctx context.Context, fcid types.FileContractID, hk types.PublicKey, logger *zap.SugaredLogger) (uint64, error) {
	// define logger
	log := logger.With(
		zap.Stringer("contract", fcid),
		zap.Stringer("host", hk))

	// prune the contract
	start := time.Now()
	res, err := p.bus.PruneContract(ctx, fcid, timeoutPruneContract)
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
		if err := p.bus.RecordContractPruneMetric(ctx, api.ContractPruneMetric{
			Timestamp: api.TimeRFC3339(start),

			ContractID: fcid,
			HostKey:    hk,

			Pruned:    res.Pruned,
			Remaining: res.Remaining,
			Duration:  time.Since(start),
		}); err != nil {
			log.Error(err)
		}
	}

	// handle logs
	if res.Error != "" {
		log.Errorw("unexpected error interrupted pruning", zap.Error(errors.New(res.Error)))
	} else if res.Pruned > 0 {
		log.Info("successfully pruned contract")
	}

	return res.Pruned, nil
}
