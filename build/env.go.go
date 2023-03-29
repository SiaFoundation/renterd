//go:build !testnet

package build

import (
	"go.sia.tech/core/chain"
)

const (
	ConsensusNetworkName  = "Mainnet"
	DefaultAPIAddress     = "localhost:9980"
	DefaultGatewayAddress = ":9981"
)

var ConsensusNetwork, _ = chain.Mainnet()
