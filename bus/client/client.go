package client

import (
	"net/http"

	"go.sia.tech/jape"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/internal/utils"
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

func (c *Client) do(req *http.Request, resp interface{}) error {
	req.Header.Set("Content-Type", "application/json")
	if c.c.Password != "" {
		req.SetBasicAuth("", c.c.Password)
	}
	_, _, err := utils.DoRequest(req, &resp)
	return err
}
