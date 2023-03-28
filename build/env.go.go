//go:build !testnet

package build

import (
	"go.sia.tech/core/chain"
)

const (
	ConsensusNetworkName = "Mainnet"
)

var ConsensusNetwork, _ = chain.Mainnet()
