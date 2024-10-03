package client

import (
	"go.sia.tech/jape"
	"go.sia.tech/renterd/api"
)

// A Client provides methods for interacting with a bus.
type Client struct {
	c jape.Client
}

// New returns a new bus client.
func New(addr, password string) *Client {
	return &Client{jape.Client{
		BaseURL:  addr,
		Password: password,
	}}
}

// State returns the current state of the bus.
func (c *Client) State() (state api.BusStateResponse, err error) {
	err = c.c.GET("/state", &state)
	return
}
