package autopilot

import (
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/internal/consensus"
	"go.sia.tech/siad/types"
	"golang.org/x/crypto/blake2b"
)

func (ap *Autopilot) currentHeight() uint64 {
	return ap.store.Tip().Height
}

func (ap *Autopilot) deriveRenterKey(hostKey consensus.PublicKey) consensus.PrivateKey {
	seed := blake2b.Sum256(append(ap.masterKey[:], hostKey[:]...))
	pk := consensus.NewPrivateKeyFromSeed(seed[:])
	for i := range seed {
		seed[i] = 0
	}
	return pk
}

func (ap *Autopilot) defaultContracts() ([]api.Contract, error) {
	cs, err := ap.renter.HostSetContracts("autopilot")
	if err != nil {
		return nil, err
	}
	contracts := make([]api.Contract, 0, len(cs))
	for _, c := range cs {
		if c.ID == (types.FileContractID{}) || c.HostIP == "" {
			continue
		}
		c.RenterKey = ap.deriveRenterKey(c.HostKey)
		contracts = append(contracts, c)
	}
	return contracts, nil
}
