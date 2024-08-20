package contractor

import (
	"context"
	"errors"
	"time"

	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/internal/utils"
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
	// compat code for hosts that have been scanned before ResolvedAddresses
	// were introduced
	if len(host.ResolvedAddresses) == 0 {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		host.ResolvedAddresses, _, _ = utils.ResolveHostIP(ctx, host.NetAddress)
	}

	subnets, err := utils.AddressesToSubnets(host.ResolvedAddresses)
	if err != nil {
		hs.logger.Errorf("failed to parse host %v subnets: %v", host.PublicKey, err)
		return true
	}
	// validate host subnets
	if len(subnets) == 0 {
		hs.logger.Errorf("host %v has no subnet, treating its IP %v as redundant", host.PublicKey, host.NetAddress)
		return true
	} else if len(subnets) > 2 {
		hs.logger.Errorf("host %v has more than 2 subnets, treating its IP %v as redundant", host.PublicKey, errHostTooManySubnets)
		return true
	}

	// check if we know about this subnet
	var knownHost string
	for _, subnet := range subnets {
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
	subnets, err := utils.AddressesToSubnets(host.ResolvedAddresses)
	if err != nil {
		hs.logger.Errorf("failed to parse host %v subnets: %v", host.PublicKey, err)
		return
	}
	for _, subnet := range subnets {
		hs.subnetToHostKey[subnet] = host.PublicKey.String()
	}
}
