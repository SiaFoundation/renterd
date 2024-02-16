package api

import (
	"fmt"
	"net/url"
	"time"

	rhpv2 "go.sia.tech/core/rhp/v2"
	rhpv3 "go.sia.tech/core/rhp/v3"
	"go.sia.tech/core/types"
)

type (
	// WalletFundRequest is the request type for the /wallet/fund endpoint.
	WalletFundRequest struct {
		Transaction        types.Transaction `json:"transaction"`
		Amount             types.Currency    `json:"amount"`
		UseUnconfirmedTxns bool              `json:"useUnconfirmedTxns"`
	}

	// WalletFundResponse is the response type for the /wallet/fund endpoint.
	WalletFundResponse struct {
		Transaction types.Transaction   `json:"transaction"`
		ToSign      []types.Hash256     `json:"toSign"`
		DependsOn   []types.Transaction `json:"dependsOn"`
	}

	// WalletPrepareFormRequest is the request type for the /wallet/prepare/form
	// endpoint.
	WalletPrepareFormRequest struct {
		EndHeight      uint64             `json:"endHeight"`
		HostCollateral types.Currency     `json:"hostCollateral"`
		HostKey        types.PublicKey    `json:"hostKey"`
		HostSettings   rhpv2.HostSettings `json:"hostSettings"`
		RenterAddress  types.Address      `json:"renterAddress"`
		RenterFunds    types.Currency     `json:"renterFunds"`
		RenterKey      types.PublicKey    `json:"renterKey"`
	}

	// WalletPrepareRenewRequest is the request type for the /wallet/prepare/renew
	// endpoint.
	WalletPrepareRenewRequest struct {
		Revision           types.FileContractRevision `json:"revision"`
		EndHeight          uint64                     `json:"endHeight"`
		ExpectedNewStorage uint64                     `json:"expectedNewStorage"`
		HostAddress        types.Address              `json:"hostAddress"`
		PriceTable         rhpv3.HostPriceTable       `json:"priceTable"`
		MinNewCollateral   types.Currency             `json:"minNewCollateral"`
		RenterAddress      types.Address              `json:"renterAddress"`
		RenterFunds        types.Currency             `json:"renterFunds"`
		RenterKey          types.PrivateKey           `json:"renterKey"`
		WindowSize         uint64                     `json:"windowSize"`
	}

	// WalletPrepareRenewResponse is the response type for the /wallet/prepare/renew
	// endpoint.
	WalletPrepareRenewResponse struct {
		ToSign         []types.Hash256     `json:"toSign"`
		TransactionSet []types.Transaction `json:"transactionSet"`
	}

	// WalletRedistributeRequest is the request type for the /wallet/redistribute
	// endpoint.
	WalletRedistributeRequest struct {
		Amount  types.Currency `json:"amount"`
		Outputs int            `json:"outputs"`
	}

	// WalletResponse is the response type for the /wallet endpoint.
	WalletResponse struct {
		ScanHeight  uint64         `json:"scanHeight"`
		Address     types.Address  `json:"address"`
		Spendable   types.Currency `json:"spendable"`
		Confirmed   types.Currency `json:"confirmed"`
		Unconfirmed types.Currency `json:"unconfirmed"`
		Immature    types.Currency `json:"immature"`
	}

	// WalletSignRequest is the request type for the /wallet/sign endpoint.
	WalletSignRequest struct {
		Transaction   types.Transaction   `json:"transaction"`
		ToSign        []types.Hash256     `json:"toSign"`
		CoveredFields types.CoveredFields `json:"coveredFields"`
	}
)

// WalletTransactionsOption is an option for the WalletTransactions method.
type WalletTransactionsOption func(url.Values)

func WalletTransactionsWithBefore(before time.Time) WalletTransactionsOption {
	return func(q url.Values) {
		q.Set("before", before.Format(time.RFC3339))
	}
}

func WalletTransactionsWithSince(since time.Time) WalletTransactionsOption {
	return func(q url.Values) {
		q.Set("since", since.Format(time.RFC3339))
	}
}

func WalletTransactionsWithLimit(limit int) WalletTransactionsOption {
	return func(q url.Values) {
		q.Set("limit", fmt.Sprint(limit))
	}
}

func WalletTransactionsWithOffset(offset int) WalletTransactionsOption {
	return func(q url.Values) {
		q.Set("offset", fmt.Sprint(offset))
	}
}
