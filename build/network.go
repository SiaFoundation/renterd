package build

//go:generate go run gen.go

import (
	"go.sia.tech/core/chain"
	"go.sia.tech/core/consensus"
	"go.sia.tech/core/types"
)

// Network returns the Sia network consts and genesis block for the current build.
func Network() (*consensus.Network, types.Block) {
	return chain.Mainnet()
}

func NetworkName() string {
	n, _ := Network()
	switch n.Name {
	case "mainnet":
		return "Mainnet"
	case "zen":
		return "Zen Testnet"
	default:
		return n.Name
	}
}
