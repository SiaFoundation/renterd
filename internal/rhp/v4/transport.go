package rhp

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.sia.tech/core/types"
	rhp "go.sia.tech/coreutils/rhp/v4"
)

type transportPool struct {
	dialer Dialer

	mu   sync.Mutex
	pool map[string]*transport
}

func newTransportPool(dialer Dialer) *transportPool {
	return &transportPool{
		dialer: dialer,
		pool:   make(map[string]*transport),
	}
}

func (p *transportPool) withTransport(ctx context.Context, hk types.PublicKey, addr string, fn func(rhp.TransportClient) error) (err error) {
	// fetch or create transport
	p.mu.Lock()
	t, found := p.pool[addr]
	if !found {
		t = &transport{
			dialer: p.dialer,
			hk:     hk,
			addr:   addr,
		}
		p.pool[addr] = t
	}
	t.refCount++
	p.mu.Unlock()

	// execute function
	err = func() (err error) {
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("panic (withTransport): %v", r)
			}
		}()
		client, err := t.Dial(ctx)
		if err != nil {
			return err
		}
		return fn(client)
	}()

	// Decrement refcounter again and clean up pool.
	p.mu.Lock()
	t.refCount--
	if t.refCount == 0 {
		// Cleanup
		if t.t != nil {
			_ = t.t.Close()
			t.t = nil
		}
		delete(p.pool, addr)
	}
	p.mu.Unlock()
	return err
}

type transport struct {
	dialer   Dialer
	refCount uint64 // locked by pool

	mu   sync.Mutex
	hk   types.PublicKey
	addr string
	t    rhp.TransportClient
}

// DialStream dials a new stream on the transport.
func (t *transport) Dial(ctx context.Context) (rhp.TransportClient, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.t == nil {
		start := time.Now()

		// dial host
		conn, err := t.dialer.Dial(ctx, t.hk, t.addr)
		if err != nil {
			return nil, err
		}

		// upgrade conn
		newTransport, err := rhp.UpgradeConn(ctx, conn, t.hk)
		if err != nil {
			return nil, fmt.Errorf("UpgradeConn: %w: %w (%v)", ErrDialTransport, err, time.Since(start))
		}
		t.t = newTransport
	}
	return t.t, nil
}
