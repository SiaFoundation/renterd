package autopilot

import (
	"go.sia.tech/renterd/bus"
	"go.sia.tech/siad/types"
)

const (
	// defaultSetName defines the name of the default contract set
	defaultSetName = "autopilot"
)

func (ap *Autopilot) updateDefaultContracts(active, toRenew, renewed, formed []bus.Contract, deleted []types.FileContractID) error {
	// build some maps
	isDeleted := make(map[types.FileContractID]bool)
	for _, d := range deleted {
		isDeleted[d] = true
	}
	wasUpForRenewal := make(map[types.FileContractID]bool)
	for _, r := range toRenew {
		wasUpForRenewal[r.ID()] = true
	}
	isRenewed := make(map[types.FileContractID]bool)
	for _, r := range renewed {
		isRenewed[r.ContractMetadata.RenewedFrom] = true
	}

	// build new contract set
	var contracts []types.FileContractID
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
