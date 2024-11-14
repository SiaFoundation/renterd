package contractor

import (
	"context"
	"errors"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/internal/utils"
	"go.uber.org/zap"
)

type (
	hostSet struct {
		resolvedAddresses map[types.PublicKey][]string
		subnetToHostKey   map[string]string

		logger *zap.SugaredLogger
	}
)

func newHostSet(l *zap.SugaredLogger) *hostSet {
	return &hostSet{
		resolvedAddresses: make(map[types.PublicKey][]string),
		subnetToHostKey:   make(map[string]string),
		logger:            l,
	}
}

func (hs *hostSet) resolveHostIP(host api.Host) ([]string, error) {
	resolvedAddresses := hs.resolvedAddresses[host.PublicKey]
	if len(resolvedAddresses) > 0 {
		return resolvedAddresses, nil
	}
	// resolve host IP
	// NOTE: we ignore errors here since failing to resolve an address is either
	// 1. not the host's faul, so we give it the benefit of the doubt
	// 2. the host is unreachable of incorrectly announced, in which case the scans will fail
	//
	var hostAddrs []string
	if host.NetAddress != "" {
		hostAddrs = append(hostAddrs, host.NetAddress)
	}
	hostAddrs = append(hostAddrs, host.V2SiamuxAddresses...)
	resolvedAddresses, _, err := utils.ResolveHostIPs(context.Background(), hostAddrs)
	if err != nil {
		return nil, err
	}

	// update cache
	hs.resolvedAddresses[host.PublicKey] = resolvedAddresses
	return resolvedAddresses, nil
}

func (hs *hostSet) HasRedundantIP(host api.Host) bool {
	logger := hs.logger.Named("hasRedundantIP").
		With("hostKey", host.PublicKey).
		With("netAddress", host.NetAddress).
		With("v2SiamuxAddresses", host.V2SiamuxAddresses)

	resolvedAddresses, err := hs.resolveHostIP(host)
	if errors.Is(err, utils.ErrHostTooManyAddresses) {
		logger.Errorf("host has more than 2 subnets, treating its IP %v as redundant", host.PublicKey, utils.ErrHostTooManyAddresses)
		return true
	} else if err != nil {
		logger.With(zap.Error(err)).Error("failed to resolve host ip - treating it as redundant")
		return true
	}

	subnets, err := utils.AddressesToSubnets(resolvedAddresses)
	if err != nil {
		logger.With(zap.Error(err)).Errorf("failed to parse host subnets")
		return true
	}
	// validate host subnets
	if len(subnets) == 0 {
		logger.Warnf("host has no subnets")
		return false
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
	addresses, err := hs.resolveHostIP(host)
	if err != nil {
		hs.logger.Errorf("failed to resolve host %v addresses: %v", host.PublicKey, err)
		return
	}
	subnets, err := utils.AddressesToSubnets(addresses)
	if err != nil {
		hs.logger.Errorf("failed to parse host %v subnets: %v", host.PublicKey, err)
		return
	}
	for _, subnet := range subnets {
		hs.subnetToHostKey[subnet] = host.PublicKey.String()
	}
}
