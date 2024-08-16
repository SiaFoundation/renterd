package rhp

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	rhpv3 "go.sia.tech/core/rhp/v3"
	"go.sia.tech/core/types"
)

type transportPoolV3 struct {
	mu   sync.Mutex
	pool map[string]*transportV3
}

func newTransportPoolV3() *transportPoolV3 {
	return &transportPoolV3{
		pool: make(map[string]*transportV3),
	}
}

func (p *transportPoolV3) withTransportV3(ctx context.Context, hostKey types.PublicKey, siamuxAddr string, fn func(context.Context, *transportV3) error) (err error) {
	// Create or fetch transport.
	p.mu.Lock()
	t, found := p.pool[siamuxAddr]
	if !found {
		t = &transportV3{
			hostKey:    hostKey,
			siamuxAddr: siamuxAddr,
		}
		p.pool[siamuxAddr] = t
	}
	t.refCount++
	p.mu.Unlock()

	// Execute function.
	err = func() (err error) {
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("panic (withTransportV3): %v", r)
			}
		}()
		return fn(ctx, t)
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
		delete(p.pool, siamuxAddr)
	}
	p.mu.Unlock()
	return err
}

// transportPoolV3 is a pool of rhpv3.Transports which allows for reusing them.
func dialTransport(ctx context.Context, siamuxAddr string, hostKey types.PublicKey) (*rhpv3.Transport, error) {
	// Dial host.
	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", siamuxAddr)
	if err != nil {
		return nil, err
	}

	// Upgrade to rhpv3.Transport.
	var t *rhpv3.Transport
	done := make(chan struct{})
	go func() {
		t, err = rhpv3.NewRenterTransport(conn, hostKey)
		close(done)
	}()
	select {
	case <-ctx.Done():
		conn.Close()
		<-done
		return nil, context.Cause(ctx)
	case <-done:
		return t, err
	}
}

// transportV3 is a reference-counted wrapper for rhpv3.Transport.
type transportV3 struct {
	refCount uint64 // locked by pool

	mu         sync.Mutex
	hostKey    types.PublicKey
	siamuxAddr string
	t          *rhpv3.Transport
}

// DialStream dials a new stream on the transport.
func (t *transportV3) DialStream(ctx context.Context) (*streamV3, error) {
	t.mu.Lock()
	if t.t == nil {
		start := time.Now()
		newTransport, err := dialTransport(ctx, t.siamuxAddr, t.hostKey)
		if err != nil {
			t.mu.Unlock()
			return nil, fmt.Errorf("DialStream: %w: %w (%v)", ErrDialTransport, err, time.Since(start))
		}
		t.t = newTransport
	}
	transport := t.t
	t.mu.Unlock()

	// Close the stream when the context is closed to unblock any reads or
	// writes.
	stream := transport.DialStream()

	// Apply a sane timeout to the stream.
	if err := stream.SetDeadline(time.Now().Add(5 * time.Minute)); err != nil {
		_ = stream.Close()
		return nil, err
	}

	// Make sure the stream is closed when the context is closed.
	doneCtx, doneFn := context.WithCancel(ctx)
	go func() {
		select {
		case <-doneCtx.Done():
		case <-ctx.Done():
			_ = stream.Close()
		}
	}()
	return &streamV3{
		Stream: stream,
		cancel: doneFn,
	}, nil
}

type streamV3 struct {
	cancel context.CancelFunc
	*rhpv3.Stream
}

func (s *streamV3) ReadResponse(resp rhpv3.ProtocolObject, maxLen uint64) (err error) {
	defer wrapRPCErr(&err, "ReadResponse")
	return s.Stream.ReadResponse(resp, maxLen)
}

func (s *streamV3) WriteResponse(resp rhpv3.ProtocolObject) (err error) {
	defer wrapRPCErr(&err, "WriteResponse")
	return s.Stream.WriteResponse(resp)
}

func (s *streamV3) ReadRequest(req rhpv3.ProtocolObject, maxLen uint64) (err error) {
	defer wrapRPCErr(&err, "ReadRequest")
	return s.Stream.ReadRequest(req, maxLen)
}

func (s *streamV3) WriteRequest(rpcID types.Specifier, req rhpv3.ProtocolObject) (err error) {
	defer wrapRPCErr(&err, "WriteRequest")
	return s.Stream.WriteRequest(rpcID, req)
}

// Close closes the stream and cancels the goroutine launched by DialStream.
func (s *streamV3) Close() error {
	s.cancel()
	return s.Stream.Close()
}
