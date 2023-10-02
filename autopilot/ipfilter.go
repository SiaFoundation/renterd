package autopilot

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	"go.sia.tech/core/types"
	"go.uber.org/zap"
)

const (
	// number of unique bits the host IP must have to prevent it from being filtered
	ipv4FilterRange = 24
	ipv6FilterRange = 32

	// resolverLookupTimeout is the timeout we apply when resolving a host's IP address
	resolverLookupTimeout = 5 * time.Second
)

var (
	errNoSuchHost        = errors.New("no such host")
	errTooManyAddresses  = errors.New("host has more than two addresses, or two of the same type")
	errUnparsableAddress = errors.New("host address could not be parsed to a subnet")
)

type resolver interface {
	LookupIPAddr(ctx context.Context, host string) ([]net.IPAddr, error)
}

type ipFilter struct {
	hostIPToSubnetsCache map[string][]string
	subnets              map[string]string
	resolver             resolver
	timeout              time.Duration

	logger *zap.SugaredLogger
}

func newIPFilter(logger *zap.SugaredLogger) *ipFilter {
	return &ipFilter{
		hostIPToSubnetsCache: make(map[string][]string),
		subnets:              make(map[string]string),
		resolver:             &net.Resolver{},
		timeout:              resolverLookupTimeout,

		logger: logger,
	}
}

func (f *ipFilter) IsRedundantIP(hostIP string, hostKey types.PublicKey) bool {
	// perform DNS lookup
	subnets, err := f.performDNSLookup(hostIP)
	if err != nil {
		if !strings.Contains(err.Error(), errNoSuchHost.Error()) {
			f.logger.Errorf("failed to check for redundant IP, treating host %v with IP %v as redundant, err: %v", hostKey, hostIP, err)
		}
		return true
	}

	// if the lookup failed parse out the host
	if len(subnets) == 0 {
		f.logger.Errorf("failed to check for redundant IP, treating host %v with IP %v as redundant, err: %v", hostKey, hostIP, errUnparsableAddress)
		return true
	}

	// we register all subnets under the host key, so we can safely use the
	// first subnet to check whether we already know this host
	host, found := f.subnets[subnets[0]]
	if !found {
		for _, subnet := range subnets {
			f.subnets[subnet] = hostKey.String()
		}
		return false
	}

	// if the given host matches the known host, it's not redundant
	sameHost := host == hostKey.String()
	return !sameHost
}

// Resetting clears the subnets, but not the cache, allowing to rebuild the IP
// filter with a list of hosts.
func (f *ipFilter) Reset() {
	f.subnets = make(map[string]string)
}

func (f *ipFilter) performDNSLookup(hostIP string) ([]string, error) {
	// check the cache
	subnets, found := f.hostIPToSubnetsCache[hostIP]
	if found {
		return subnets, nil
	}

	// lookup all IP addresses for the given host
	host, _, err := net.SplitHostPort(hostIP)
	if err != nil {
		return nil, err
	}

	// create a context
	ctx := context.Background()
	if f.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(context.Background(), f.timeout)
		defer cancel()
	}

	// lookup IP addresses
	addrs, err := f.resolver.LookupIPAddr(ctx, host)
	if err != nil {
		return nil, err
	}

	// filter out hosts associated with more than two addresses or two of the same type
	if len(addrs) > 2 || (len(addrs) == 2) && (len(addrs[0].IP) == len(addrs[1].IP)) {
		return nil, errTooManyAddresses
	}

	// parse out subnets
	subnets = parseSubnets(addrs)

	// cache them and return
	if len(subnets) > 0 {
		f.hostIPToSubnetsCache[hostIP] = subnets
	}
	return subnets, nil
}

func parseSubnets(addresses []net.IPAddr) []string {
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
