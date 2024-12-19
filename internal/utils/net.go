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

func AddressesToSubnets(resolvedAddresses []net.IPAddr) ([]string, error) {
	var subnets []string
	for _, addr := range resolvedAddresses {
		// figure out the IP range
		ipRange := ipv6FilterRange
		if addr.IP.To4() != nil {
			ipRange = ipv4FilterRange
		}

		// parse the subnet
		cidr := fmt.Sprintf("%s/%d", addr.IP.String(), ipRange)
		_, ipnet, err := net.ParseCIDR(cidr)
		if err != nil {
			return nil, fmt.Errorf("failed to parse cidr: %w", err)
		}

		subnets = append(subnets, ipnet.String())
	}

	return subnets, nil
}

func PerformHostIPChecks(hostIPs []net.IPAddr) error {
	// filter out hosts associated with more than two addresses or two of the same type
	if len(hostIPs) > 2 || (len(hostIPs) == 2) && (len(hostIPs[0].IP.String()) == len(hostIPs[1].IP.String())) {
		return fmt.Errorf("%w: %+v", ErrHostTooManyAddresses, hostIPs)
	}
	return nil
}

func ResolveHostIPs(ctx context.Context, hostIPs []string) ([]net.IPAddr, error) {
	// resolve host addresses and deduplicate IPs
	addrMap := make(map[string]net.IPAddr)
	for _, hostIP := range hostIPs {
		host, _, err := net.SplitHostPort(hostIP)
		if err != nil {
			return nil, err
		}
		ips, err := (&net.Resolver{}).LookupIPAddr(ctx, host)
		if err != nil {
			return nil, err
		}
		for _, ip := range ips {
			addrMap[ip.String()] = ip
		}
	}
	var addrs []net.IPAddr
	for _, addr := range addrMap {
		addrs = append(addrs, addr)
	}
	// sort IPs
	sort.Slice(addrs, func(i, j int) bool {
		return addrs[i].IP.String() < addrs[j].IP.String()
	})
	return addrs, nil
}

func IsPrivateIP(addr net.IP) bool {
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
