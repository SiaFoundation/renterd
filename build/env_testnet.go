//go:build testnet

package build

import "go.sia.tech/core/chain"

const (
	ConsensusNetworkName = "Testnet-Zen"
)

var ConsensusNetwork, _ = chain.TestnetZen()
