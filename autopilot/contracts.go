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

func (ap *Autopilot) renewableContracts(endHeight uint64) ([]worker.Contract, error) {
	cs, err := ap.bus.ActiveContracts(endHeight)
	if err != nil {
		return nil, err
	}
	return ap.toWorkerContracts(cs), nil
}

func (ap *Autopilot) updateDefaultContracts(renewed, formed []worker.Contract) error {
	// fetch current set
	cs, err := ap.defaultContracts()
	if err != nil {
		return err
	}

	// build hostkey -> index map
	csMap := make(map[string]int)
	for i, contract := range cs {
		csMap[contract.HostKey.String()] = i
	}

	// swap renewed contracts
	for _, contract := range renewed {
		index, exists := csMap[contract.HostKey.String()]
		if !exists {
			// TODO: panic/log? shouldn't happen
			csMap[contract.HostKey.String()] = len(cs)
			cs = append(cs, contract)
			continue
		}
		cs[index] = contract
	}

	// append formations
	for _, contract := range formed {
		_, exists := csMap[contract.HostKey.String()]
		if exists {
			// TODO: panic/log? shouldn't happen
			continue
		}
		cs = append(cs, contract)
	}

	// update contract set
	contracts := make([]consensus.PublicKey, len(cs))
	for i, c := range cs {
		contracts[i] = c.HostKey
	}
	err = ap.bus.SetHostSet(defaultSetName, contracts)
	if err != nil {
		return err
	}
	return nil
}
