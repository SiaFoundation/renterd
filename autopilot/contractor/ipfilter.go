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
	ipFilter struct {
		subnetToHostKey map[string]string

		logger *zap.SugaredLogger
	}
)

func (c *Contractor) newIPFilter() *ipFilter {
	return &ipFilter{
		logger:          c.logger,
		subnetToHostKey: make(map[string]string),
	}
}

func (f *ipFilter) HasRedundantIP(host api.Host) bool {
	// validate host subnets
	if len(host.Subnets) == 0 {
		f.logger.Errorf("host %v has no subnet, treating its IP %v as redundant", host.PublicKey, host.NetAddress)
		return true
	} else if len(host.Subnets) > 2 {
		f.logger.Errorf("host %v has more than 2 subnets, treating its IP %v as redundant", host.PublicKey, errHostTooManySubnets)
		return true
	}

	// check if we know about this subnet
	var knownHost string
	for _, subnet := range host.Subnets {
		if knownHost = f.subnetToHostKey[subnet]; knownHost != "" {
			break
		}
	}

	// if we know about the subnet, the host is redundant if it's not the same
	if knownHost != "" {
		return host.PublicKey.String() != knownHost
	}
	return false
}

func (f *ipFilter) Add(host api.Host) {
	for _, subnet := range host.Subnets {
		f.subnetToHostKey[subnet] = host.PublicKey.String()
	}
}
