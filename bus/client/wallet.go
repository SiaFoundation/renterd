package client

import (
	"context"
	"net/url"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/wallet"
	"go.sia.tech/renterd/api"
)

// SendSiacoins is a helper method that sends siacoins to the given address.
func (c *Client) SendSiacoins(ctx context.Context, addr types.Address, amt types.Currency, useUnconfirmedTxns bool) (txnID types.TransactionID, err error) {
	err = c.c.WithContext(ctx).POST("/wallet/send", api.WalletSendRequest{
		Address:          addr,
		Amount:           amt,
		SubtractMinerFee: false,
		UseUnconfirmed:   useUnconfirmedTxns,
	}, &txnID)
	return
}

// Wallet calls the /wallet endpoint on the bus.
func (c *Client) Wallet(ctx context.Context) (resp api.WalletResponse, err error) {
	err = c.c.WithContext(ctx).GET("/wallet", &resp)
	return
}

// WalletPending returns the txpool transactions that are relevant to the
// wallet.
func (c *Client) WalletPending(ctx context.Context) (resp []wallet.Event, err error) {
	err = c.c.WithContext(ctx).GET("/wallet/pending", &resp)
	return
}

// WalletRedistribute broadcasts a transaction that redistributes the money in
// the wallet in the desired number of outputs of given amount. If the
// transaction was successfully broadcasted it will return the transaction ID.
func (c *Client) WalletRedistribute(ctx context.Context, outputs int, amount types.Currency) (ids []types.TransactionID, err error) {
	req := api.WalletRedistributeRequest{
		Amount:  amount,
		Outputs: outputs,
	}

	err = c.c.WithContext(ctx).POST("/wallet/redistribute", req, &ids)
	return
}

// WalletEvents returns all events relevant to the wallet.
func (c *Client) WalletEvents(ctx context.Context, opts ...api.WalletTransactionsOption) (resp []wallet.Event, err error) {
	values := url.Values{}
	for _, opt := range opts {
		opt(values)
	}
	err = c.c.WithContext(ctx).GET("/wallet/events?"+values.Encode(), &resp)
	return
}
