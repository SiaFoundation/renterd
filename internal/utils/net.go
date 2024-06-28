package utils

import (
	"context"
	"fmt"
	"net"
	"sort"

	"go.sia.tech/renterd/api"
)

const (
	ipv4FilterRange = 24
	ipv6FilterRange = 32
)

var (
	privateSubnets []*net.IPNet
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

func ResolveHostIP(ctx context.Context, hostIP string) (subnets []string, private bool, _ error) {
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
		return nil, false, api.ErrHostTooManyAddresses
	}

	// parse out subnets
	for _, address := range addrs {
		private = private || isPrivateIP(address.IP)

		// figure out the IP range
		ipRange := ipv6FilterRange
		if address.IP.To4() != nil {
			ipRange = ipv4FilterRange
		}

		// parse the subnet
		cidr := fmt.Sprintf("%s/%d", address.String(), ipRange)
		_, ipnet, err := net.ParseCIDR(cidr)
		if err != nil {
			continue
		}

		// add it
		subnets = append(subnets, ipnet.String())
	}

	// sort the subnets
	sort.Slice(subnets, func(i, j int) bool {
		return subnets[i] < subnets[j]
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
