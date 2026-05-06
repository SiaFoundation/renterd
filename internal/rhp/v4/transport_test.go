package rhp

import (
	"context"
	"net"
	"testing"
	"time"

	"go.sia.tech/core/types"
	rhp "go.sia.tech/coreutils/rhp/v4"
)

// blockTransportClient is a TransportClient whose Close blocks until
// the returned channel is closed.
type blockTransportClient struct {
	ch chan struct{}
}

func (s *blockTransportClient) DialStream(context.Context) (net.Conn, error) { return nil, nil }
func (s *blockTransportClient) FrameSize() int                               { return 0 }
func (s *blockTransportClient) PeerKey() types.PublicKey                     { return types.PublicKey{} }
func (s *blockTransportClient) Close() error {
	<-s.ch
	return nil
}

type mockDialer struct {
	client rhp.TransportClient
}

func (d *mockDialer) Dial(context.Context, types.PublicKey, string) (net.Conn, error) {
	return nil, nil
}

// TestTransportPoolClose is a regression test for a deadlock that could
// occur when a transport's Close blocks while holding the pool's mutex
func TestTransportPoolClose(t *testing.T) {
	ch := make(chan struct{})
	slow := &blockTransportClient{ch: ch}

	tp := newTransportPool(&mockDialer{client: slow})
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

	// wait briefly, then verify the pool is not blocked by trying to
	// use a different address
	time.Sleep(50 * time.Millisecond)

	lockAcquired := make(chan struct{})
	go func() {
		tp.mu.Lock()
		_ = len(tp.pool) // access pool to justify the lock
		tp.mu.Unlock()
		close(lockAcquired)
	}()

	select {
	case <-lockAcquired:
	case <-time.After(time.Second):
		t.Fatal("pool mutex blocked by slow Close")
	}

	// unblock the slow close
	close(ch)
	<-done
}
