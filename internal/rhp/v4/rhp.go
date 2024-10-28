package rhp

import (
	"context"
	"errors"
	"net"

	rhp4 "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	rhp "go.sia.tech/coreutils/rhp/v4"
)

var (
	// errDialTransport is returned when the worker could not dial the host.
	ErrDialTransport = errors.New("could not dial transport")
)

type (
	Dialer interface {
		Dial(ctx context.Context, hk types.PublicKey, address string) (net.Conn, error)
	}
)

type Client struct {
	tpool *transportPool
}

func New(dialer Dialer) *Client {
	return &Client{
		tpool: newTransportPool(dialer),
	}
}

func (c *Client) Settings(ctx context.Context, hk types.PublicKey, addr string) (hs rhp4.HostSettings, _ error) {
	err := c.tpool.withTransport(hk, addr, func(t *transport) error {
		client, err := t.Dial(ctx)
		if err != nil {
			return err
		}
		hs, err = rhp.RPCSettings(ctx, client)
		return err
	})
	return hs, err
}
