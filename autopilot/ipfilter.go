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

	// ipCacheEntryValidity defines the amount of time the IP filter uses a
	// cached entry when it encounters an error while trying to resolve a host's
	// IP address
	ipCacheEntryValidity = 24 * time.Hour

	// resolverLookupTimeout is the timeout we apply when resolving a host's IP address
	resolverLookupTimeout = 5 * time.Second
)

var (
	errIOTimeout         = errors.New("i/o timeout")
	errServerMisbehaving = errors.New("server misbehaving")
	errTooManyAddresses  = errors.New("host has more than two addresses, or two of the same type")
	errUnparsableAddress = errors.New("host address could not be parsed to a subnet")
)

type (
	ipFilter struct {
		subnetToHostKey map[string]string

		resolver *ipResolver
		logger   *zap.SugaredLogger
	}
)

func (c *contractor) newIPFilter() *ipFilter {
	c.resolver.pruneCache()
	return &ipFilter{
		subnetToHostKey: make(map[string]string),

		resolver: c.resolver,
		logger:   c.logger,
	}
}

func (f *ipFilter) IsRedundantIP(hostIP string, hostKey types.PublicKey) bool {
	// perform lookup
	subnets, err := f.resolver.lookup(hostIP)
	if err != nil {
		if !strings.Contains(err.Error(), errNoSuchHost.Error()) {
			f.logger.Errorf("failed to check for redundant IP, treating host %v with IP %v as redundant, err: %v", hostKey, hostIP, err)
		}
		return true
	}

	// return early if we couldn't resolve to a subnet
	if len(subnets) == 0 {
		f.logger.Errorf("failed to resolve IP to a subnet, treating host %v with IP %v as redundant, err: %v", hostKey, hostIP, errUnparsableAddress)
		return true
	}

	// check if we know about this subnet, if not register all the subnets
	host, found := f.subnetToHostKey[subnets[0]]
	if !found {
		for _, subnet := range subnets {
			f.subnetToHostKey[subnet] = hostKey.String()
		}
		return false
	}

	// otherwise compare host keys
	sameHost := host == hostKey.String()
	return !sameHost
}

type (
	resolver interface {
		LookupIPAddr(ctx context.Context, host string) ([]net.IPAddr, error)
	}

	ipResolver struct {
		resolver resolver
		cache    map[string]ipCacheEntry
		timeout  time.Duration

		logger *zap.SugaredLogger
	}

	ipCacheEntry struct {
		created time.Time
		subnets []string
	}
)

func newIPResolver(timeout time.Duration, logger *zap.SugaredLogger) *ipResolver {
	if timeout == 0 {
		panic("timeout must be greater than zero") // developer error
	}
	return &ipResolver{
		resolver: &net.Resolver{},
		cache:    make(map[string]ipCacheEntry),
		timeout:  resolverLookupTimeout,
		logger:   logger,
	}
}

func (r *ipResolver) pruneCache() {
	for hostIP, entry := range r.cache {
		if time.Since(entry.created) > ipCacheEntryValidity {
			delete(r.cache, hostIP)
		}
	}
}

func (r *ipResolver) lookup(hostIP string) ([]string, error) {
	// split off host
	host, _, err := net.SplitHostPort(hostIP)
	if err != nil {
		return nil, err
	}

	// make sure we don't hang
	ctx, cancel := context.WithTimeout(context.Background(), r.timeout)
	defer cancel()

	// lookup IP addresses
	addrs, err := r.resolver.LookupIPAddr(ctx, host)
	if err != nil {
		// check the cache if it's an i/o timeout or server misbehaving error
		if isErr(err, errIOTimeout) || isErr(err, errServerMisbehaving) {
			if entry, found := r.cache[hostIP]; found && time.Since(entry.created) < ipCacheEntryValidity {
				r.logger.Debugf("using cached IP addresses for %v, err: %v", hostIP, err)
				return entry.subnets, nil
			}
		}
		return nil, err
	}

	// filter out hosts associated with more than two addresses or two of the same type
	if len(addrs) > 2 || (len(addrs) == 2) && (len(addrs[0].IP) == len(addrs[1].IP)) {
		return nil, errTooManyAddresses
	}

	// parse out subnets
	subnets := parseSubnets(addrs)

	// add to cache
	if len(subnets) > 0 {
		r.cache[hostIP] = ipCacheEntry{
			created: time.Now(),
			subnets: subnets,
		}
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

func isErr(err error, target error) bool {
	if errors.Is(err, target) {
		return true
	}
	return err != nil && target != nil && strings.Contains(err.Error(), target.Error())
}
