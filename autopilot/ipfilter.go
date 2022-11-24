package autopilot

import (
	"fmt"
	"net"
)

const (
	// number of unique bits the host IP must have to prevent it from being filtered
	IPv4FilterRange = 24
	IPv6FilterRange = 54
)

type ipFilter struct {
	subnets map[string]struct{}
}

func newIPFilter() *ipFilter {
	return &ipFilter{
		subnets: make(map[string]struct{}),
	}
}

func (f *ipFilter) exists(ip *net.IPAddr) bool {
	// figure out the IP range
	ipRange := IPv6FilterRange
	if ip.IP.To4() != nil {
		ipRange = IPv4FilterRange
	}

	// parse the subnet
	cidr := fmt.Sprintf("%s/%d", ip.String(), ipRange)
	_, ipnet, err := net.ParseCIDR(cidr)
	if err != nil {
		return false
	}

	// check if it exists
	_, exists := f.subnets[ipnet.String()]
	if exists {
		return true
	}

	// add it
	f.subnets[ipnet.String()] = struct{}{}
	return false
}
