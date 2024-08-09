package worker

import (
	"context"
	"fmt"
	"net"
	"sync"

	"go.sia.tech/core/types"
)

// Cache to store resolved IPs
type hostCache struct {
	mu    sync.RWMutex
	cache map[string]string // hostname -> IP address
}

func NewhostCache() *hostCache {
	return &hostCache{
		cache: make(map[string]string),
	}
}

func (hc *hostCache) Get(hostname string) (string, bool) {
	hc.mu.RLock()
	defer hc.mu.RUnlock()
	ip, ok := hc.cache[hostname]
	return ip, ok
}

func (hc *hostCache) Set(hostname, ip string) {
	hc.mu.Lock()
	defer hc.mu.Unlock()
	hc.cache[hostname] = ip
}

func (hc *hostCache) Clear(hostname string) {
	hc.mu.Lock()
	defer hc.mu.Unlock()
	delete(hc.cache, hostname)
}

// fallbackDialer implements a custom net.Dialer with a fallback mechanism
type fallbackDialer struct {
	Cache *hostCache

	Bus    Bus
	Dialer net.Dialer
}

func newFallbackDialer(bus Bus, dialer net.Dialer) *fallbackDialer {
	return &fallbackDialer{
		Cache: NewhostCache(),

		Bus:    bus,
		Dialer: dialer,
	}
}

func (d *fallbackDialer) Dial(ctx context.Context, hk types.PublicKey, address string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return nil, err
	}

	// Try to resolve IP
	ipAddr, err := net.ResolveIPAddr("ip", host)
	if err == nil {
		// Cache the resolved IP and dial
		d.Cache.Set(host, ipAddr.String())
		return d.Dialer.DialContext(ctx, "tcp", net.JoinHostPort(ipAddr.String(), port))
	}

	// If resolution fails, check the cache
	if cachedIP, ok := d.Cache.Get(host); ok {
		conn, err := d.Dialer.DialContext(ctx, "tcp", net.JoinHostPort(cachedIP, port))
		if err == nil {
			return conn, nil
		}
		// Clear the cache if the cached IP doesn't work
		d.Cache.Clear(host)
	}

	// Attempt to resolve using the bus
	hostInfo, err := d.Bus.Host(ctx, hk)
	if err != nil {
		return nil, err
	}

	for _, addr := range hostInfo.ResolvedAddresses {
		conn, err := d.Dialer.DialContext(ctx, "tcp", net.JoinHostPort(addr, port))
		if err == nil {
			// Update cache on successful dial
			d.Cache.Set(host, addr)
			return conn, nil
		}
	}

	return nil, fmt.Errorf("failed to dial %s with all methods", address)
}
