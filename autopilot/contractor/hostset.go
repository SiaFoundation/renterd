package contractor

import (
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/internal/utils"
	"go.uber.org/zap"
)

type (
	hostSet struct {
		addressToHostKey map[string]string

		logger *zap.SugaredLogger
	}
)

func (hs *hostSet) HasRedundantIP(host api.Host) bool {
	// validate host addresses
	if len(host.ResolvedAddresses) == 0 {
		hs.logger.Errorf("host %v has no address, treating its IP %v as redundant", host.PublicKey, host.NetAddress)
		return true
	} else if len(host.ResolvedAddresses) > 2 {
		hs.logger.Errorf("host %v has more than 2 addresses, treating its IP %v as redundant", host.PublicKey, utils.ErrHostTooManyAddresses)
		return true
	}

	// check if we know about this address
	var knownHost string
	for _, address := range host.ResolvedAddresses {
		if knownHost = hs.addressToHostKey[address]; knownHost != "" {
			break
		}
	}

	// if we know about the address, the host is redundant if it's not the same
	if knownHost != "" {
		return host.PublicKey.String() != knownHost
	}
	return false
}

func (hs *hostSet) Add(host api.Host) {
	for _, subnet := range host.ResolvedAddresses {
		hs.addressToHostKey[subnet] = host.PublicKey.String()
	}
}
