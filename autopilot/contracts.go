package autopilot

import (
	"go.sia.tech/renterd/bus"
	"go.sia.tech/siad/types"
)

const (
	// defaultSetName defines the name of the default contract set
	defaultSetName = "autopilot"
)

func (ap *Autopilot) updateDefaultContracts(active, renewed, formed, toDelete, toIgnore, toRenew []bus.Contract) error {
	// build some maps
	isDeleted := contractMapBool(toDelete)
	isIgnored := contractMapBool(toIgnore)
	isUpForRenew := contractMapBool(toRenew)

	// renewed map is special case since we need renewed from
	isRenewed := make(map[types.FileContractID]bool)
	for _, c := range renewed {
		isRenewed[c.RenewedFrom] = true
	}

	// build new contract set
	var contracts []types.FileContractID
	for _, c := range append(active, append(renewed, formed...)...) {
		if isDeleted[c.ID()] {
			continue // exclude deleted contracts
		}
		if isIgnored[c.ID()] {
			continue // exclude ignored contracts (contracts that became unusable)
		}
		if isRenewed[c.ID()] {
			continue // exclude (effectively) renewed contracts
		}
		if isUpForRenew[c.ID()] && !isRenewed[c.ID()] {
			continue // exclude contracts that were up for renewal but failed to renew
		}
		contracts = append(contracts, c.ID())
	}

	// TODO: contracts that are up for renewal could be used for dl, not ul
	// TODO: contracts should be sorted according to host score

	// update contract set
	return ap.bus.SetContractSet(defaultSetName, contracts)
}

func contractIds(contracts []bus.Contract) []types.FileContractID {
	ids := make([]types.FileContractID, len(contracts))
	for i, c := range contracts {
		ids[i] = c.ID()
	}
	return ids
}

func contractMapBool(contracts []bus.Contract) map[types.FileContractID]bool {
	cmap := make(map[types.FileContractID]bool)
	for _, c := range contracts {
		cmap[c.ID()] = true
	}
	return cmap
}
