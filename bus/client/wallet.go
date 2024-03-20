package client

import (
	"context"
	"fmt"
	"net/http"
	"net/url"

	rhpv2 "go.sia.tech/core/rhp/v2"
	rhpv3 "go.sia.tech/core/rhp/v3"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/wallet"
)

// SendSiacoins is a helper method that sends siacoins to the given outputs.
func (c *Client) SendSiacoins(ctx context.Context, scos []types.SiacoinOutput, useUnconfirmedTxns bool) (err error) {
	var value types.Currency
	for _, sco := range scos {
		value = value.Add(sco.Value)
	}
	txn := types.Transaction{
		SiacoinOutputs: scos,
	}
	toSign, parents, err := c.WalletFund(ctx, &txn, value, useUnconfirmedTxns)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = c.WalletDiscard(ctx, txn)
		}
	}()
	err = c.WalletSign(ctx, &txn, toSign, types.CoveredFields{WholeTransaction: true})
	if err != nil {
		return err
	}
	return c.BroadcastTransaction(ctx, append(parents, txn))
}

// Wallet calls the /wallet endpoint on the bus.
func (c *Client) Wallet(ctx context.Context) (resp api.WalletResponse, err error) {
	err = c.c.WithContext(ctx).GET("/wallet", &resp)
	return
}

// WalletDiscard discards the provided txn, make its inputs usable again. This
// should only be called on transactions that will never be broadcast.
func (c *Client) WalletDiscard(ctx context.Context, txn types.Transaction) error {
	return c.c.WithContext(ctx).POST("/wallet/discard", txn, nil)
}

// WalletFund funds txn using inputs controlled by the wallet.
func (c *Client) WalletFund(ctx context.Context, txn *types.Transaction, amount types.Currency, useUnconfirmedTransactions bool) ([]types.Hash256, []types.Transaction, error) {
	req := api.WalletFundRequest{
		Transaction:        *txn,
		Amount:             amount,
		UseUnconfirmedTxns: useUnconfirmedTransactions,
	}
	var resp api.WalletFundResponse
	err := c.c.WithContext(ctx).POST("/wallet/fund", req, &resp)
	if err != nil {
		return nil, nil, err
	}
	*txn = resp.Transaction
	return resp.ToSign, resp.DependsOn, nil
}

// WalletOutputs returns the set of unspent outputs controlled by the wallet.
func (c *Client) WalletOutputs(ctx context.Context) (resp []wallet.SiacoinElement, err error) {
	err = c.c.WithContext(ctx).GET("/wallet/outputs", &resp)
	return
}

// WalletPending returns the txpool transactions that are relevant to the
// wallet.
func (c *Client) WalletPending(ctx context.Context) (resp []types.Transaction, err error) {
	err = c.c.WithContext(ctx).GET("/wallet/pending", &resp)
	return
}

// WalletPrepareForm funds and signs a contract transaction.
func (c *Client) WalletPrepareForm(ctx context.Context, renterAddress types.Address, renterKey types.PublicKey, renterFunds, hostCollateral types.Currency, hostKey types.PublicKey, hostSettings rhpv2.HostSettings, endHeight uint64) (txns []types.Transaction, err error) {
	req := api.WalletPrepareFormRequest{
		EndHeight:      endHeight,
		HostCollateral: hostCollateral,
		HostKey:        hostKey,
		HostSettings:   hostSettings,
		RenterAddress:  renterAddress,
		RenterFunds:    renterFunds,
		RenterKey:      renterKey,
	}
	err = c.c.WithContext(ctx).POST("/wallet/prepare/form", req, &txns)
	return
}

// WalletPrepareRenew funds and signs a contract renewal transaction.
func (c *Client) WalletPrepareRenew(ctx context.Context, revision types.FileContractRevision, hostAddress, renterAddress types.Address, renterKey types.PrivateKey, renterFunds, minNewCollateral types.Currency, pt rhpv3.HostPriceTable, endHeight, windowSize, expectedStorage uint64) (api.WalletPrepareRenewResponse, error) {
	req := api.WalletPrepareRenewRequest{
		Revision:           revision,
		EndHeight:          endHeight,
		ExpectedNewStorage: expectedStorage,
		HostAddress:        hostAddress,
		PriceTable:         pt,
		MinNewCollateral:   minNewCollateral,
		RenterAddress:      renterAddress,
		RenterFunds:        renterFunds,
		RenterKey:          renterKey,
		WindowSize:         windowSize,
	}
	var resp api.WalletPrepareRenewResponse
	err := c.c.WithContext(ctx).POST("/wallet/prepare/renew", req, &resp)
	return resp, err
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

// WalletSign signs txn using the wallet's private key.
func (c *Client) WalletSign(ctx context.Context, txn *types.Transaction, toSign []types.Hash256, cf types.CoveredFields) error {
	req := api.WalletSignRequest{
		Transaction:   *txn,
		ToSign:        toSign,
		CoveredFields: cf,
	}
	return c.c.WithContext(ctx).POST("/wallet/sign", req, txn)
}

// WalletTransactions returns all transactions relevant to the wallet.
func (c *Client) WalletTransactions(ctx context.Context, opts ...api.WalletTransactionsOption) (resp []wallet.Transaction, err error) {
	c.c.Custom("GET", "/wallet/transactions", nil, &resp)

	values := url.Values{}
	for _, opt := range opts {
		opt(values)
	}
	u, err := url.Parse(fmt.Sprintf("%v/wallet/transactions", c.c.BaseURL))
	if err != nil {
		panic(err)
	}
	u.RawQuery = values.Encode()
	req, err := http.NewRequestWithContext(ctx, "GET", u.String(), http.NoBody)
	if err != nil {
		panic(err)
	}
	err = c.do(req, &resp)
	return
}
