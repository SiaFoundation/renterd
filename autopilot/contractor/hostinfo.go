package contractor

import (
	"context"
	"fmt"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/hostdb"
)

func (c *Contractor) HostInfo(ctx context.Context, hostKey types.PublicKey, state *MaintenanceState) (api.HostHandlerResponse, error) {
	if state.ContractsConfig().Allowance.IsZero() {
		return api.HostHandlerResponse{}, fmt.Errorf("can not score hosts because contracts allowance is zero")
	}
	if state.ContractsConfig().Amount == 0 {
		return api.HostHandlerResponse{}, fmt.Errorf("can not score hosts because contracts amount is zero")
	}
	if state.Period() == 0 {
		return api.HostHandlerResponse{}, fmt.Errorf("can not score hosts because contract period is zero")
	}

	host, err := c.bus.Host(ctx, hostKey)
	if err != nil {
		return api.HostHandlerResponse{}, fmt.Errorf("failed to fetch requested host from bus: %w", err)
	}
	cs, err := c.bus.ConsensusState(ctx)
	if err != nil {
		return api.HostHandlerResponse{}, fmt.Errorf("failed to fetch consensus state from bus: %w", err)
	}
	c.mu.Lock()
	storedData := c.cachedDataStored[hostKey]
	minScore := c.cachedMinScore
	c.mu.Unlock()

	gc := state.GougingChecker(cs)

	// ignore the pricetable's HostBlockHeight by setting it to our own blockheight
	host.Host.PriceTable.HostBlockHeight = cs.BlockHeight

	isUsable, unusableResult := isUsableHost(state.ContractsConfig(), state.RS, gc, host, minScore, storedData)
	return api.HostHandlerResponse{
		Host: host.Host,
		Checks: &api.HostHandlerResponseChecks{
			Gouging:          unusableResult.gougingBreakdown.Gouging(),
			GougingBreakdown: unusableResult.gougingBreakdown,
			Score:            unusableResult.scoreBreakdown.Score(),
			ScoreBreakdown:   unusableResult.scoreBreakdown,
			Usable:           isUsable,
			UnusableReasons:  unusableResult.reasons(),
		},
	}, nil
}

func (c *Contractor) hostInfoFromCache(ctx context.Context, state *MaintenanceState, host hostdb.HostInfo) (hi hostInfo, found bool) {
	// grab host details from cache
	c.mu.Lock()
	hi, found = c.cachedHostInfo[host.PublicKey]
	storedData := c.cachedDataStored[host.PublicKey]
	minScore := c.cachedMinScore
	c.mu.Unlock()

	// return early if the host info is not cached
	if !found {
		return
	}

	// try and refresh the host info if it got scanned in the meantime, this
	// inconsistency would resolve itself but trying to update it here improves
	// first time user experience
	if host.Scanned && hi.UnusableResult.notcompletingscan > 0 {
		cs, err := c.bus.ConsensusState(ctx)
		if err != nil {
			c.logger.Error("failed to fetch consensus state from bus: %v", err)
		} else {
			gc := state.GougingChecker(cs)
			isUsable, unusableResult := isUsableHost(state.ContractsConfig(), state.RS, gc, host, minScore, storedData)
			hi = hostInfo{
				Usable:         isUsable,
				UnusableResult: unusableResult,
			}

			// update cache
			c.mu.Lock()
			c.cachedHostInfo[host.PublicKey] = hi
			c.mu.Unlock()
		}
	}

	return
}

func (c *Contractor) HostInfos(ctx context.Context, state *MaintenanceState, filterMode, usabilityMode, addressContains string, keyIn []types.PublicKey, offset, limit int) ([]api.HostHandlerResponse, error) {
	// declare helper to decide whether to keep a host.
	if !isValidUsabilityFilterMode(usabilityMode) {
		return nil, fmt.Errorf("invalid usability mode: '%v', options are 'usable', 'unusable' or an empty string for no filter", usabilityMode)
	}

	keep := func(usable bool) bool {
		switch usabilityMode {
		case api.UsabilityFilterModeUsable:
			return usable // keep usable
		case api.UsabilityFilterModeUnusable:
			return !usable // keep unusable
		case api.UsabilityFilterModeAll:
			return true // keep all
		case "":
			return true // keep all
		default:
			panic("unreachable")
		}
	}

	var hostInfos []api.HostHandlerResponse
	wanted := limit
	for {
		// fetch up to 'limit' hosts.
		hosts, err := c.bus.SearchHosts(ctx, api.SearchHostOptions{
			Offset:          offset,
			Limit:           limit,
			FilterMode:      filterMode,
			AddressContains: addressContains,
			KeyIn:           keyIn,
		})
		if err != nil {
			return nil, err
		}
		offset += len(hosts)

		// if there are no more hosts, we're done.
		if len(hosts) == 0 {
			return hostInfos, nil // no more hosts
		}

		// decide how many of the returned hosts to keep.
		var keptHosts int
		for _, host := range hosts {
			hi, cached := c.hostInfoFromCache(ctx, state, host)
			if !cached {
				// when the filterMode is "all" we include uncached hosts and
				// set IsChecked = false.
				if usabilityMode == api.UsabilityFilterModeAll {
					hostInfos = append(hostInfos, api.HostHandlerResponse{
						Host: host.Host,
					})
					if wanted > 0 && len(hostInfos) == wanted {
						return hostInfos, nil // we're done.
					}
					keptHosts++
				}
				continue
			}
			if !keep(hi.Usable) {
				continue
			}
			hostInfos = append(hostInfos, api.HostHandlerResponse{
				Host: host.Host,
				Checks: &api.HostHandlerResponseChecks{
					Gouging:          hi.UnusableResult.gougingBreakdown.Gouging(),
					GougingBreakdown: hi.UnusableResult.gougingBreakdown,
					Score:            hi.UnusableResult.scoreBreakdown.Score(),
					ScoreBreakdown:   hi.UnusableResult.scoreBreakdown,
					Usable:           hi.Usable,
					UnusableReasons:  hi.UnusableResult.reasons(),
				},
			})
			if wanted > 0 && len(hostInfos) == wanted {
				return hostInfos, nil // we're done.
			}
			keptHosts++
		}

		// if no hosts were kept from this batch, double the limit.
		if limit > 0 && keptHosts == 0 {
			limit *= 2
		}
	}
}

func isValidUsabilityFilterMode(usabilityMode string) bool {
	switch usabilityMode {
	case api.UsabilityFilterModeUsable:
	case api.UsabilityFilterModeUnusable:
	case api.UsabilityFilterModeAll:
	case "":
	default:
		return false
	}
	return true
}