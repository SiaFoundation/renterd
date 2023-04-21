package autopilot

import (
	"context"
	"fmt"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/worker"
)

func (c *contractor) HostInfo(ctx context.Context, hostKey types.PublicKey) (api.HostHandlerGET, error) {
	cfg := c.ap.Config()
	if cfg.Contracts.Allowance.IsZero() {
		return api.HostHandlerGET{}, fmt.Errorf("can not score hosts because contracts allowance is zero")
	}
	if cfg.Contracts.Amount == 0 {
		return api.HostHandlerGET{}, fmt.Errorf("can not score hosts because contracts amount is zero")
	}
	if cfg.Contracts.Period == 0 {
		return api.HostHandlerGET{}, fmt.Errorf("can not score hosts because contract period is zero")
	}

	host, err := c.ap.bus.Host(ctx, hostKey)
	if err != nil {
		return api.HostHandlerGET{}, fmt.Errorf("failed to fetch requested host from bus: %w", err)
	}
	gs, err := c.ap.bus.GougingSettings(ctx)
	if err != nil {
		return api.HostHandlerGET{}, fmt.Errorf("failed to fetch gouging settings from bus: %w", err)
	}
	rs, err := c.ap.bus.RedundancySettings(ctx)
	if err != nil {
		return api.HostHandlerGET{}, fmt.Errorf("failed to fetch redundancy settings from bus: %w", err)
	}
	cs, err := c.ap.bus.ConsensusState(ctx)
	if err != nil {
		return api.HostHandlerGET{}, fmt.Errorf("failed to fetch consensus state from bus: %w", err)
	}
	fee, err := c.ap.bus.RecommendedFee(ctx)
	if err != nil {
		return api.HostHandlerGET{}, fmt.Errorf("failed to fetch recommended fee from bus: %w", err)
	}
	c.mu.Lock()
	storedData := c.cachedDataStored[hostKey]
	minScore := c.cachedMinScore
	c.mu.Unlock()

	f := newIPFilter(c.logger)
	gc := worker.NewGougingChecker(gs, rs, cs, fee, cfg.Contracts.Period, cfg.Contracts.RenewWindow)

	// ignore the pricetable's HostBlockHeight by setting it to our own blockheight
	host.Host.PriceTable.HostBlockHeight = cs.BlockHeight

	isUsable, unusableResult := isUsableHost(cfg, rs, gc, f, host.Host, minScore, storedData)
	return api.HostHandlerGET{
		Host: host.Host,

		Gouging:          unusableResult.gougingBreakdown.Gouging(),
		GougingBreakdown: unusableResult.gougingBreakdown,
		Score:            unusableResult.scoreBreakdown.Score(),
		ScoreBreakdown:   unusableResult.scoreBreakdown,
		Usable:           isUsable,
		UnusableReasons:  unusableResult.reasons(),
	}, nil
}

func (c *contractor) HostInfos(ctx context.Context, filterMode, addressContains string, keyIn []types.PublicKey, offset, limit int) ([]api.HostHandlerGET, error) {
	cfg := c.ap.Config()
	if cfg.Contracts.Allowance.IsZero() {
		return nil, fmt.Errorf("can not score hosts because contracts allowance is zero")
	}
	if cfg.Contracts.Amount == 0 {
		return nil, fmt.Errorf("can not score hosts because contracts amount is zero")
	}
	if cfg.Contracts.Period == 0 {
		return nil, fmt.Errorf("can not score hosts because contract period is zero")
	}

	hosts, err := c.ap.bus.SearchHosts(ctx, filterMode, addressContains, keyIn, offset, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch requested hosts from bus: %w", err)
	}
	gs, err := c.ap.bus.GougingSettings(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch gouging settings from bus: %w", err)
	}
	rs, err := c.ap.bus.RedundancySettings(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch redundancy settings from bus: %w", err)
	}
	cs, err := c.ap.bus.ConsensusState(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch consensus state from bus: %w", err)
	}
	fee, err := c.ap.bus.RecommendedFee(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch recommended fee from bus: %w", err)
	}

	f := newIPFilter(c.logger)
	gc := worker.NewGougingChecker(gs, rs, cs, fee, cfg.Contracts.Period, cfg.Contracts.RenewWindow)

	c.mu.Lock()
	storedData := make(map[types.PublicKey]uint64)
	for _, host := range hosts {
		storedData[host.PublicKey] = c.cachedDataStored[host.PublicKey]
	}
	minScore := c.cachedMinScore
	c.mu.Unlock()

	var hostInfos []api.HostHandlerGET
	for _, host := range hosts {
		// ignore the pricetable's HostBlockHeight by setting it to our own blockheight
		host.PriceTable.HostBlockHeight = cs.BlockHeight

		isUsable, unusableResult := isUsableHost(cfg, rs, gc, f, host, minScore, storedData[host.PublicKey])
		hostInfos = append(hostInfos, api.HostHandlerGET{
			Host: host,

			Gouging:          unusableResult.gougingBreakdown.Gouging(),
			GougingBreakdown: unusableResult.gougingBreakdown,
			Score:            unusableResult.scoreBreakdown.Score(),
			ScoreBreakdown:   unusableResult.scoreBreakdown,
			Usable:           isUsable,
			UnusableReasons:  unusableResult.reasons(),
		})
	}
	return hostInfos, nil
}
