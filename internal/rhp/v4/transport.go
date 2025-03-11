package rhp

import (
	"context"
	"fmt"
	"sync"
	"time"

	rhpv4 "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	rhp "go.sia.tech/coreutils/rhp/v4"
	"go.sia.tech/coreutils/rhp/v4/siamux"
	"go.sia.tech/renterd/v2/internal/utils"
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
		t = &transport{}
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
		client, err := t.Dial(ctx, p.dialer, hk, addr)
		if err != nil {
			return err
		}
		err = fn(client)
		if err != nil && rhpv4.ErrorCode(err) != rhpv4.ErrorCodeTransport {
			// wrap error to indicate that the error was returned by the host
			err = fmt.Errorf("%w: %w", utils.ErrHost, err)
		}
		return err
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
	refCount uint64 // locked by pool

	mu sync.Mutex
	t  rhp.TransportClient
}

// DialStream dials a new stream on the transport.
func (t *transport) Dial(ctx context.Context, dialer Dialer, hk types.PublicKey, addr string) (rhp.TransportClient, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.t == nil {
		start := time.Now()

		// dial host
		conn, err := dialer.Dial(ctx, hk, addr)
		if err != nil {
			return nil, err
		}

		// upgrade conn
		newTransport, err := siamux.Upgrade(ctx, conn, hk)
		if err != nil {
			return nil, fmt.Errorf("UpgradeConn: %w: %w (%v)", ErrDialTransport, err, time.Since(start))
		}
		t.t = newTransport
	}
	return t.t, nil
}
