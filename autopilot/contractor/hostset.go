package contractor

import (
	"context"
	"net"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/internal/utils"
	"go.uber.org/zap"
)

type (
	hostFilter interface {
		Add(ctx context.Context, host api.Host)
		HasRedundantIP(ctx context.Context, host api.Host) bool
	}

	hostSet struct {
		resolvedAddresses map[types.PublicKey][]net.IPAddr
		subnetToHostKey   map[string]string

		logger *zap.SugaredLogger
	}
	noopFilter struct{}
)

func (n noopFilter) Add(context.Context, api.Host)                 {}
func (n noopFilter) HasRedundantIP(context.Context, api.Host) bool { return false }

func newHostFilter(allowRedundantHostIPs bool, l *zap.SugaredLogger) hostFilter {
	if allowRedundantHostIPs {
		return noopFilter{}
	}
	return &hostSet{
		resolvedAddresses: make(map[types.PublicKey][]net.IPAddr),
		subnetToHostKey:   make(map[string]string),
		logger:            l,
	}
}

func (hs *hostSet) resolveHostIP(ctx context.Context, host api.Host) ([]net.IPAddr, error) {
	resolvedAddresses := hs.resolvedAddresses[host.PublicKey]
	if len(resolvedAddresses) > 0 {
		return resolvedAddresses, nil
	}
	// resolve host IPs
	var hostAddrs []string
	if host.NetAddress != "" {
		hostAddrs = append(hostAddrs, host.NetAddress)
	}
	hostAddrs = append(hostAddrs, host.V2SiamuxAddresses...)
	resolvedAddresses, err := utils.ResolveHostIPs(ctx, hostAddrs)
	if err != nil {
		return nil, err
	}

	// update cache
	hs.resolvedAddresses[host.PublicKey] = resolvedAddresses
	return resolvedAddresses, nil
}

func (hs *hostSet) HasRedundantIP(ctx context.Context, host api.Host) bool {
	logger := hs.logger.Named("hasRedundantIP").
		With("hostKey", host.PublicKey).
		With("netAddress", host.NetAddress).
		With("v2SiamuxAddresses", host.V2SiamuxAddresses)

	// resolve addresses - if a host's addresses can't be resolved, we consider
	// it our own fault and don't consider the host redundant. This should be a
	// temporary error unless the host is misconfigured or offline, which means
	// the scanning code will lead to the host being marked as offline.
	resolvedAddresses, err := hs.resolveHostIP(ctx, host)
	if err != nil {
		logger.With(zap.Error(err)).Warn("failed to resolve host ips - not redundant")
		return false
	}

	// perform checks - a host that resolves successfully but doesn't pass the
	// checks will never pass them unless they reannounce so we consider them
	// redundant.
	if err := utils.PerformHostIPChecks(resolvedAddresses); err != nil {
		logger.With(zap.Error(err)).Errorf("host failed the ip checks, treating its IP as redundant (%v)", err, resolvedAddresses)
		return true
	}

	// parse the subnets - this should never fail considering we just
	// successfully parsed the IPs but if it does regardless, we consider the
	// host redundant. If a host has no subnets since it resolves to no IPs, we
	// also consider it redundant since it will reannounce to fix the issue.
	subnets, err := utils.AddressesToSubnets(resolvedAddresses)
	if err != nil {
		logger.With(zap.Error(err)).Errorf("failed to parse host subnets")
		return true
	} else if len(subnets) == 0 {
		logger.Warnf("host has no subnets")
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

func (hs *hostSet) Add(ctx context.Context, host api.Host) {
	addresses, err := hs.resolveHostIP(ctx, host)
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
