package contractor

import (
	"context"
	"errors"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/internal/utils"
	"go.uber.org/zap"
)

var (
	errHostTooManySubnets = errors.New("host has more than two subnets")
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

func (hs *hostSet) resolveHostIP(host api.Host) []string {
	resolvedAddresses := hs.resolvedAddresses[host.PublicKey]
	if len(resolvedAddresses) > 0 {
		return resolvedAddresses
	}
	// resolve host IP
	// NOTE: we ignore errors here since failing to resolve an address is either
	// 1. not the host's faul, so we give it the benefit of the doubt
	// 2. the host is unreachable of incorrectly announced, in which case the scans will fail
	//
	// TODO: resolve v2 addresses
	resolvedAddresses, _, _ = utils.ResolveHostIP(context.Background(), host.NetAddress)

	// update cache
	hs.resolvedAddresses[host.PublicKey] = resolvedAddresses
	return resolvedAddresses
}

func (hs *hostSet) HasRedundantIP(host api.Host) bool {
	resolvedAddresses := hs.resolveHostIP(host)

	subnets, err := utils.AddressesToSubnets(resolvedAddresses)
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
	subnets, err := utils.AddressesToSubnets(hs.resolveHostIP(host))
	if err != nil {
		hs.logger.Errorf("failed to parse host %v subnets: %v", host.PublicKey, err)
		return
	}
	for _, subnet := range subnets {
		hs.subnetToHostKey[subnet] = host.PublicKey.String()
	}
}
