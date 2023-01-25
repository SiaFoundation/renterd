package autopilot

import (
	"context"
	"fmt"
	"net"
	"strings"
	"time"

	"go.sia.tech/renterd/hostdb"
	"go.uber.org/zap"
)

const (
	// number of unique bits the host IP must have to prevent it from being filtered
	ipv4FilterRange = 24
	ipv6FilterRange = 54

	// resolverLookupTimeout is the timeout we apply when resolving a host's IP address
	resolverLookupTimeout = 5 * time.Second
)

type resolver interface {
	LookupIPAddr(ctx context.Context, host string) ([]net.IPAddr, error)
}

type ipFilter struct {
	subnets  map[string]string
	resolver resolver
	timeout  time.Duration

	logger *zap.SugaredLogger
}

func newIPFilter(logger *zap.SugaredLogger) *ipFilter {
	return &ipFilter{
		subnets:  make(map[string]string),
		resolver: &net.Resolver{},
		timeout:  resolverLookupTimeout,

		logger: logger,
	}
}

func (f *ipFilter) isRedundantIP(h hostdb.Host) bool {
	// create a context
	ctx := context.Background()
	if f.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(context.Background(), f.timeout)
		defer cancel()
	}

	// lookup all IP addresses for the given host
	host, _, err := net.SplitHostPort(h.NetAddress)
	if err != nil {
		return true
	}
	addresses, err := f.resolver.LookupIPAddr(ctx, host)
	if err != nil {
		if !strings.Contains(err.Error(), "no such host") {
			f.logger.Errorf("failed to lookup IP for host %v, err: %v", h.PublicKey, err)
		}
		return true
	}

	// filter hosts associated with more than two addresses or two of the same type
	if len(addresses) > 2 || (len(addresses) == 2) && (len(addresses[0].IP) == len(addresses[1].IP)) {
		return true
	}

	// check whether the host's subnet was already in the list, if it is in the
	// list we compare the cached net address with the one from the host being
	// filtered as it might be the same host
	var filter bool
	for _, subnet := range subnets(addresses) {
		original, exists := f.subnets[subnet]
		if exists && h.PublicKey.String() != original {
			filter = true
		} else if !exists {
			f.subnets[subnet] = h.PublicKey.String()
		}
	}
	return filter
}

func subnets(addresses []net.IPAddr) []string {
	subnets := make([]string, 0, len(addresses))

	for _, address := range addresses {
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

	return subnets
}
