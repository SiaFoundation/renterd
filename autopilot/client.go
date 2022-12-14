package autopilot

import (
	"fmt"

	"go.sia.tech/jape"
	"go.sia.tech/renterd/internal/consensus"
)

// A Client provides methods for interacting with a renterd API server.
type Client struct {
	c jape.Client
}

// NewClient returns a client that communicates with a renterd store server
// listening on the specified address.
func NewClient(addr, password string) *Client {
	return &Client{jape.Client{
		BaseURL:  addr,
		Password: password,
	}}
}

func (c *Client) SetConfig(cfg Config) error {
	return c.c.PUT("/config", cfg)
}

func (c *Client) Config() (cfg Config, err error) {
	err = c.c.GET("/config", &cfg)
	return
}

// RenterKey returns the renter's private key for a given host's public key.
func (c *Client) RenterKey(hostKey consensus.PublicKey) (rk consensus.PrivateKey, err error) {
	err = c.c.GET(fmt.Sprintf("/renterkey/%s", hostKey), &rk)
	return
}
