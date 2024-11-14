package rhp

import (
	"context"
	"fmt"
	"net"
	"sync"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"go.uber.org/zap"
)

// Cache to store resolved IPs
type hostCache struct {
	mu    sync.RWMutex
	cache map[string]string // hostname -> IP address
}

func newHostCache() *hostCache {
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

func (hc *hostCache) Delete(hostname string) {
	hc.mu.Lock()
	defer hc.mu.Unlock()
	delete(hc.cache, hostname)
}

type DialerBus interface {
	Host(ctx context.Context, hostKey types.PublicKey) (api.Host, error)
}

// FallbackDialer implements a custom net.Dialer with a fallback mechanism
type FallbackDialer struct {
	cache *hostCache

	bus    DialerBus
	logger *zap.SugaredLogger
	dialer net.Dialer
}

func NewFallbackDialer(bus DialerBus, dialer net.Dialer, logger *zap.Logger) *FallbackDialer {
	return &FallbackDialer{
		cache: newHostCache(),

		bus:    bus,
		logger: logger.Sugar().Named("fallbackdialer"),
		dialer: dialer,
	}
}

func (d *FallbackDialer) Dial(ctx context.Context, hk types.PublicKey, address string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return nil, fmt.Errorf("failed to split host and port of host address '%v': %w", address, err)
	}
	logger := d.logger.With(zap.String("hostKey", hk.String()), zap.String("host", host))

	// Dial and cache the resolved IP if dial successful
	conn, err := d.dialer.DialContext(ctx, "tcp", address)
	if err == nil {
		d.cache.Set(host, conn.RemoteAddr().String())
		return conn, nil
	}

	// If resolution fails, check the cache
	if cachedIP, ok := d.cache.Get(host); ok {
		logger.Debug("Failed to resolve host, using cached IP", zap.Error(err))
		conn, err := d.dialer.DialContext(ctx, "tcp", net.JoinHostPort(cachedIP, port))
		if err == nil {
			return conn, nil
		}
		// Delete the cache if the cached IP doesn't work
		d.cache.Delete(host)
	}
	return nil, fmt.Errorf("failed to dial %s with all methods", address)
}
