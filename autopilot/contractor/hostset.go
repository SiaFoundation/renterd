package contractor

import (
	"errors"

	"go.sia.tech/renterd/api"
	"go.uber.org/zap"
)

var (
	errHostTooManySubnets = errors.New("host has more than two subnets")
)

type (
	hostSet struct {
		subnetToHostKey map[string]string

		logger *zap.SugaredLogger
	}
)

func (hs *hostSet) HasRedundantIP(host api.Host) bool {
	// validate host subnets
	if len(host.Subnets) == 0 {
		hs.logger.Errorf("host %v has no subnet, treating its IP %v as redundant", host.PublicKey, host.NetAddress)
		return true
	} else if len(host.Subnets) > 2 {
		hs.logger.Errorf("host %v has more than 2 subnets, treating its IP %v as redundant", host.PublicKey, errHostTooManySubnets)
		return true
	}

	// check if we know about this subnet
	var knownHost string
	for _, subnet := range host.Subnets {
		if knownHost = hs.subnetToHostKey[subnet]; knownHost != "" {
			break
		}
	}

	// if we know about the subnet, the host is redundant if it's not the same
	if knownHost != "" {
		return host.PublicKey.String() != knownHost
	}
	return false
}

func (hs *hostSet) Add(host api.Host) {
	for _, subnet := range host.Subnets {
		hs.subnetToHostKey[subnet] = host.PublicKey.String()
	}
}
