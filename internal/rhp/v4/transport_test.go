package rhp

import (
	"context"
	"net"
	"testing"
	"time"

	"go.sia.tech/core/types"
	rhp "go.sia.tech/coreutils/rhp/v4"
)

// blockTransportClient is a TransportClient whose Close signals
// closeStarted and then blocks until unblock is closed.
type blockTransportClient struct {
	closing chan struct{}
	unblock chan struct{}
}

func (s *blockTransportClient) DialStream(context.Context) (net.Conn, error) { return nil, nil }
func (s *blockTransportClient) FrameSize() int                               { return 0 }
func (s *blockTransportClient) PeerKey() types.PublicKey                     { return types.PublicKey{} }
func (s *blockTransportClient) Close() error {
	close(s.closing)
	<-s.unblock
	return nil
}

type mockDialer struct{}

func (d *mockDialer) Dial(context.Context, types.PublicKey, string) (net.Conn, error) {
	return nil, nil
}

// TestTransportPoolClose is a regression test for a deadlock that could
// occur when a transport's Close blocks while holding the pool's mutex
func TestTransportPoolClose(t *testing.T) {
	slow := &blockTransportClient{
		closing: make(chan struct{}),
		unblock: make(chan struct{}),
	}

	tp := newTransportPool(&mockDialer{})
	addr := "1.2.3.4:1234"

	// inject a transport with refCount 0 so that withTransport increments
	// to 1, runs fn, then decrements back to 0 triggering cleanup
	tp.mu.Lock()
	tp.pool[addr] = &transport{t: slow}
	tp.mu.Unlock()

	// release the last reference in the background, this triggers Close
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = tp.withTransport(t.Context(), types.PublicKey{}, addr, func(rhp.TransportClient) error {
			return nil
		})
	}()

	// wait until Close has actually been called
	<-slow.closing

	// verify the pool is not blocked while Close is hanging
	lockAcquired := make(chan struct{})
	go func() {
		tp.mu.Lock()
		_ = len(tp.pool)
		tp.mu.Unlock()
		close(lockAcquired)
	}()

	select {
	case <-lockAcquired:
	case <-time.After(time.Second):
		t.Fatal("pool mutex blocked by slow Close")
	}

	// unblock the slow close
	close(slow.unblock)
	<-done
}
