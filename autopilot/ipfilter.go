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

type resolver interface {
	LookupIP(string) ([]net.IP, error)
}

type ipFilter struct {
	subnets  map[string]string
	resolver resolver
}

type prodResolver struct{}

func (r prodResolver) LookupIP(host string) ([]net.IP, error) {
	return net.LookupIP(host)
}

func newIPFilter() *ipFilter {
	return &ipFilter{
		subnets:  make(map[string]string),
		resolver: prodResolver{},
	}
}

func (f *ipFilter) filtered(h host) bool {
	// lookup all IP addresses for the given host
	host, _, err := net.SplitHostPort(h.NetAddress())
	if err != nil {
		return true
	}
	addresses, err := f.resolver.LookupIP(host)
	if err != nil {
		return true
	}

	// filter hosts associated with more than two addresses or two of the same type
	if len(addresses) > 2 || (len(addresses) == 2) && (len(addresses[0]) == len(addresses[1])) {
		return true
	}

	// defer a function that adds every subnet
	subnets := subnets(addresses)
	defer func() {
		for _, subnet := range subnets {
			f.subnets[subnet] = h.NetAddress()
		}
	}()

	// check whether the host's subnet was already in the list, if it is in the
	// list we compare the cached net address with the one from the host being
	// filtered - might be the same host
	for _, subnet := range subnets {
		if original, exists := f.subnets[subnet]; exists && h.NetAddress() != original {
			return true
		}
	}
	return false
}

func subnets(addresses []net.IP) []string {
	subnets := make([]string, 0, len(addresses))

	for _, address := range addresses {
		// figure out the IP range
		ipRange := IPv6FilterRange
		if address.To4() != nil {
			ipRange = IPv4FilterRange
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

	return subnets
}
