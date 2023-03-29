//go:build testnet

package build

import "go.sia.tech/core/chain"

const (
	ConsensusNetworkName  = "Testnet-Zen"
	DefaultAPIAddress     = "localhost:9880"
	DefaultGatewayAddress = ":9881"
)

var ConsensusNetwork, _ = chain.TestnetZen()
