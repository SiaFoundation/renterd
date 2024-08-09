package utils

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sort"
)

const (
	ipv4FilterRange = 24
	ipv6FilterRange = 32
)

var (
	privateSubnets []*net.IPNet

	// ErrHostTooManyAddresses is returned by the worker API when a host has
	// more than two addresses of the same type.
	ErrHostTooManyAddresses = errors.New("host has more than two addresses, or two of the same type")
)

func init() {
	for _, subnet := range []string{
		"10.0.0.0/8",
		"172.16.0.0/12",
		"192.168.0.0/16",
		"100.64.0.0/10",
	} {
		_, subnet, err := net.ParseCIDR(subnet)
		if err != nil {
			panic(fmt.Sprintf("failed to parse subnet: %v", err))
		}
		privateSubnets = append(privateSubnets, subnet)
	}
}

func ResolveHostIP(ctx context.Context, hostIP string) (ips []string, private bool, _ error) {
	// resolve host address
	host, _, err := net.SplitHostPort(hostIP)
	if err != nil {
		return nil, false, err
	}
	addrs, err := (&net.Resolver{}).LookupIPAddr(ctx, host)
	if err != nil {
		return nil, false, err
	}

	// filter out hosts associated with more than two addresses or two of the same type
	if len(addrs) > 2 || (len(addrs) == 2) && (len(addrs[0].IP) == len(addrs[1].IP)) {
		return nil, false, fmt.Errorf("%w: %+v", ErrHostTooManyAddresses, addrs)
	}

	// get ips
	for _, address := range addrs {
		private = private || isPrivateIP(address.IP)

		// add it
		ips = append(ips, address.IP.String())
	}

	// sort the ips
	sort.Slice(ips, func(i, j int) bool {
		return ips[i] < ips[j]
	})
	return
}

func isPrivateIP(addr net.IP) bool {
	if addr.IsLoopback() || addr.IsLinkLocalUnicast() || addr.IsLinkLocalMulticast() {
		return true
	}

	for _, block := range privateSubnets {
		if block.Contains(addr) {
			return true
		}
	}
	return false
}
