package autopilot

import "go.sia.tech/jape"

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
