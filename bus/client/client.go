package client

import (
	"context"

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

func (c *Client) Backup(ctx context.Context, database, dstPath string) (err error) {
	err = c.c.WithContext(ctx).POST("/system/sqlite3/backup", api.BackupRequest{
		Database: database,
		Path:     dstPath,
	}, nil)
	return
}

// State returns the current state of the bus.
func (c *Client) State() (state api.BusStateResponse, err error) {
	err = c.c.GET("/state", &state)
	return
}
