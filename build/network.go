package build

//go:generate go run gen.go

import (
	"go.sia.tech/core/chain"
	"go.sia.tech/core/consensus"
	"go.sia.tech/core/types"
)

// Network returns the Sia network consts and genesis block for the current build.
func Network() (*consensus.Network, types.Block) {
	switch network {
	case "mainnet":
		return chain.Mainnet()
	case "zen":
		return chain.TestnetZen()
	default:
		panic("unknown network: " + network)
	}
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
