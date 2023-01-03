package autopilot

import (
	"go.sia.tech/renterd/api"
	"go.sia.tech/siad/types"
)

const (
	// defaultSetName defines the name of the default contract set
	defaultSetName = "autopilot"
)

func (ap *Autopilot) updateDefaultContracts(active, formed, toDelete, toIgnore, toRefresh, toRenew []types.FileContractID, renewed []api.Contract) error {
	// build some maps
	isDeleted := contractMapBool(toDelete)
	isIgnored := contractMapBool(toIgnore)
	isUpForRenew := contractMapBool(append(toRefresh, toRenew...))

	// renewed map is special case since we need renewed from
	isRenewed := make(map[types.FileContractID]bool)
	renewedIDs := make([]types.FileContractID, len(renewed))
	for _, c := range renewed {
		isRenewed[c.RenewedFrom] = true
		renewedIDs = append(renewedIDs, c.ID)
	}

	// build new contract set
	var contracts []types.FileContractID
	for _, fcid := range append(active, append(renewedIDs, formed...)...) {
		if isDeleted[fcid] {
			continue // exclude deleted contracts
		}
		if isIgnored[fcid] {
			continue // exclude ignored contracts (contracts that became unusable)
		}
		if isRenewed[fcid] {
			continue // exclude (effectively) renewed contracts
		}
		if isUpForRenew[fcid] && !isRenewed[fcid] {
			continue // exclude contracts that were up for renewal but failed to renew
		}
		contracts = append(contracts, fcid)
	}

	// TODO: contracts that are up for renewal could be used for dl, not ul
	// TODO: contracts should be sorted according to host score

	// update contract set
	return ap.bus.SetContractSet(defaultSetName, contracts)
}

func contractIds(contracts []api.Revision) []types.FileContractID {
	ids := make([]types.FileContractID, len(contracts))
	for i, c := range contracts {
		ids[i] = c.ID
	}
	return ids
}

func contractMapBool(contracts []types.FileContractID) map[types.FileContractID]bool {
	cmap := make(map[types.FileContractID]bool)
	for _, fcid := range contracts {
		cmap[fcid] = true
	}
	return cmap
}
