package contractor

import (
	"go.sia.tech/renterd/api"
	"go.uber.org/zap"
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

func (f *ipFilter) IsRedundantIP(host api.Host) bool {
	// return early if we couldn't resolve to a subnet
	if len(host.Subnets) == 0 {
		f.logger.Errorf("host %v has no subnet, treating its IP %v as redundant", host.PublicKey, host.NetAddress)
		return true
	}

	// check if we know about this subnet, if not register all the subnets
	existing, found := f.subnetToHostKey[host.Subnets[0]]
	if !found {
		for _, subnet := range host.Subnets {
			f.subnetToHostKey[subnet] = host.PublicKey.String()
		}
		return false
	}

	// otherwise compare host keys
	sameHost := host.PublicKey.String() == existing
	return !sameHost
}

func (f *ipFilter) Remove(h api.Host) {
	for k, v := range f.subnetToHostKey {
		if v == h.PublicKey.String() {
			delete(f.subnetToHostKey, k)
		}
	}
}
