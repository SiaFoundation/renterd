package autopilot

import (
	"go.sia.tech/renterd/bus"
	"go.sia.tech/renterd/internal/consensus"
	"go.sia.tech/renterd/worker"
	"go.sia.tech/siad/types"
)

const (
	// defaultSetName defines the name of the default contract set
	defaultSetName = "autopilot"
)

func (ap *Autopilot) toWorkerContracts(contracts []bus.Contract) []worker.Contract {
	out := make([]worker.Contract, 0, len(contracts))
	for _, c := range contracts {
		if c.ID == (types.FileContractID{}) || c.HostIP == "" {
			continue
		}
		out = append(out, worker.Contract{
			HostKey:   c.HostKey,
			HostIP:    c.HostIP,
			ID:        c.ID,
			RenterKey: ap.deriveRenterKey(c.HostKey),
		})
	}
	return out
}

func (ap *Autopilot) defaultContracts() ([]worker.Contract, error) {
	cs, err := ap.bus.HostSetContracts(defaultSetName)
	if err != nil {
		return nil, err
	}
	return ap.toWorkerContracts(cs), nil
}

func (ap *Autopilot) updateDefaultContracts(active, toRenew, renewed, formed []bus.Contract, deleted []types.FileContractID) error {
	// build some maps
	isDeleted := make(map[types.FileContractID]bool)
	for _, d := range deleted {
		isDeleted[d] = true
	}
	isUpForRenewal := make(map[types.FileContractID]bool)
	for _, r := range toRenew {
		isUpForRenewal[r.ID] = true
	}
	isRenewed := make(map[types.FileContractID]bool)
	for _, r := range renewed {
		isRenewed[r.ContractMetadata.RenewedFrom] = true
	}

	// build new contract set
	var contracts []consensus.PublicKey
	for _, c := range append(active, formed...) {
		if !isDeleted[c.ID] && !(isUpForRenewal[c.ID] && !isRenewed[c.ID]) {
			contracts = append(contracts, c.HostKey)
		}
	}

	// update contract set
	return ap.bus.SetHostSet(defaultSetName, contracts)
}
