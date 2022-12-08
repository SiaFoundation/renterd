package autopilot

import (
	"go.sia.tech/renterd/types"
	siatypes "go.sia.tech/siad/types"
)

const (
	// defaultSetName defines the name of the default contract set
	defaultSetName = "autopilot"
)

func (ap *Autopilot) updateDefaultContracts(active, toRenew, renewed, formed []types.Contract, deleted []siatypes.FileContractID) error {
	// build some maps
	isDeleted := make(map[siatypes.FileContractID]bool)
	for _, d := range deleted {
		isDeleted[d] = true
	}
	wasUpForRenewal := make(map[siatypes.FileContractID]bool)
	for _, r := range toRenew {
		wasUpForRenewal[r.ID()] = true
	}
	isRenewed := make(map[siatypes.FileContractID]bool)
	for _, r := range renewed {
		isRenewed[r.ContractMetadata.RenewedFrom] = true
	}

	// build new contract set
	var contracts []siatypes.FileContractID
	for _, c := range append(active, formed...) {
		// TODO: excluding contracts that are up for renewal but have not been
		// renewed yet, we probably want the autopilot to manage more than one
		// set of contracts (e.g. goodForUpload - goodForDownload contracts)
		upForRenewal := wasUpForRenewal[c.ID()] && !isRenewed[c.ID()]
		if !isDeleted[c.ID()] && !upForRenewal {
			contracts = append(contracts, c.ID())
		}
	}

	// update contract set
	return ap.bus.SetContractSet(defaultSetName, contracts)
}
