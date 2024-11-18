package worker

import (
	"context"
	"fmt"
	"io"
	"time"

	rhpv2 "go.sia.tech/core/rhp/v2"
	rhpv3 "go.sia.tech/core/rhp/v3"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/internal/gouging"
	"go.sia.tech/renterd/internal/host"
	rhp3 "go.sia.tech/renterd/internal/rhp/v3"
	"go.sia.tech/renterd/internal/worker"
	"go.uber.org/zap"
)

type (
	hostClient struct {
		hk         types.PublicKey
		renterKey  types.PrivateKey
		fcid       types.FileContractID
		siamuxAddr string

		acc                      *worker.Account
		client                   *rhp3.Client
		contractSpendingRecorder ContractSpendingRecorder
		logger                   *zap.SugaredLogger
		priceTables              *priceTables
	}
)

var (
	_ host.Host        = (*hostClient)(nil)
	_ host.HostManager = (*Worker)(nil)
)

func (w *Worker) Host(hk types.PublicKey, fcid types.FileContractID, siamuxAddr string) host.Host {
	return &hostClient{
		client:                   w.rhp3Client,
		hk:                       hk,
		acc:                      w.accounts.ForHost(hk),
		contractSpendingRecorder: w.contractSpendingRecorder,
		logger:                   w.logger.Named(hk.String()[:4]),
		fcid:                     fcid,
		siamuxAddr:               siamuxAddr,
		renterKey:                w.deriveRenterKey(hk),
		priceTables:              w.priceTables,
	}
}

func (h *hostClient) PublicKey() types.PublicKey { return h.hk }

func (h *hostClient) DownloadSector(ctx context.Context, w io.Writer, root types.Hash256, offset, length uint32, overpay bool) (err error) {
	var amount types.Currency
	return h.acc.WithWithdrawal(func() (types.Currency, error) {
		pt, uptc, err := h.priceTables.fetch(ctx, h.hk, nil)
		if err != nil {
			return types.ZeroCurrency, err
		}
		hpt := pt.HostPriceTable
		amount = uptc

		// check for download gouging specifically
		gc, err := GougingCheckerFromContext(ctx, overpay)
		if err != nil {
			return amount, err
		}
		if breakdown := gc.Check(nil, &hpt); breakdown.DownloadErr != "" {
			return amount, fmt.Errorf("%w: %v", gouging.ErrPriceTableGouging, breakdown.DownloadErr)
		}

		cost, err := h.client.ReadSector(ctx, offset, length, root, w, h.hk, h.siamuxAddr, h.acc.ID(), h.acc.Key(), hpt)
		if err != nil {
			return amount, err
		}
		return amount.Add(cost), nil
	})
}

func (h *hostClient) UploadSector(ctx context.Context, sectorRoot types.Hash256, sector *[rhpv2.SectorSize]byte, rev types.FileContractRevision) error {
	// fetch price table
	var pt rhpv3.HostPriceTable
	if err := h.acc.WithWithdrawal(func() (amount types.Currency, err error) {
		pt, amount, err = h.priceTable(ctx, nil)
		return
	}); err != nil {
		return err
	}

	// upload
	cost, err := h.client.AppendSector(ctx, sectorRoot, sector, &rev, h.hk, h.siamuxAddr, h.acc.ID(), pt, h.renterKey)
	if err != nil {
		return fmt.Errorf("failed to upload sector: %w", err)
	}
	// record spending
	h.contractSpendingRecorder.Record(rev, api.ContractSpending{Uploads: cost})
	return nil
}

func (h *hostClient) PriceTableUnpaid(ctx context.Context) (api.HostPriceTable, error) {
	return h.client.PriceTableUnpaid(ctx, h.hk, h.siamuxAddr)
}

func (h *hostClient) PriceTable(ctx context.Context, rev *types.FileContractRevision) (hpt api.HostPriceTable, cost types.Currency, err error) {
	// fetchPT is a helper function that performs the RPC given a payment function
	fetchPT := func(paymentFn rhp3.PriceTablePaymentFunc) (api.HostPriceTable, error) {
		return h.client.PriceTable(ctx, h.hk, h.siamuxAddr, paymentFn)
	}

	// fetch the price table
	if rev != nil {
		hpt, err = fetchPT(rhp3.PreparePriceTableContractPayment(rev, h.acc.ID(), h.renterKey))
	} else {
		hpt, err = fetchPT(rhp3.PreparePriceTableAccountPayment(h.acc.Key()))
	}

	// set the cost
	if err == nil {
		cost = hpt.UpdatePriceTableCost
	}
	return
}

// FetchRevision tries to fetch a contract revision from the host.
func (h *hostClient) FetchRevision(ctx context.Context, fetchTimeout time.Duration) (types.FileContractRevision, error) {
	if fetchTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, fetchTimeout)
		defer cancel()
	}
	// Try to fetch the revision with an account first.
	return h.client.Revision(ctx, h.fcid, h.hk, h.siamuxAddr)
}

func (h *hostClient) FundAccount(ctx context.Context, desired types.Currency, rev *types.FileContractRevision) error {
	log := h.logger.With(
		zap.Stringer("host", h.hk),
		zap.Stringer("account", h.acc.ID()),
	)

	// ensure we have at least 2H in the contract to cover the costs
	if types.NewCurrency64(2).Cmp(rev.ValidRenterPayout()) >= 0 {
		return fmt.Errorf("insufficient funds to fund account: %v <= %v", rev.ValidRenterPayout(), types.NewCurrency64(2))
	}

	// calculate the deposit amount
	return h.acc.WithDeposit(func(balance types.Currency) (types.Currency, error) {
		// return early if we have the desired balance
		if balance.Cmp(desired) >= 0 {
			return types.ZeroCurrency, nil
		}
		deposit := desired.Sub(balance)

		// fetch pricetable directly to bypass the gouging check
		pt, _, err := h.priceTables.fetch(ctx, h.hk, rev)
		if err != nil {
			return types.ZeroCurrency, err
		}

		// cap the deposit by what's left in the contract
		cost := types.NewCurrency64(1)
		availableFunds := rev.ValidRenterPayout().Sub(cost)
		if deposit.Cmp(availableFunds) > 0 {
			deposit = availableFunds
		}

		// fund the account
		if err := h.client.FundAccount(ctx, rev, h.hk, h.siamuxAddr, deposit, h.acc.ID(), pt.HostPriceTable, h.renterKey); err != nil {
			if rhp3.IsBalanceMaxExceeded(err) {
				h.acc.ScheduleSync()
			}
			return types.ZeroCurrency, fmt.Errorf("failed to fund account with %v; %w", deposit, err)
		}

		// record the spend
		h.contractSpendingRecorder.Record(*rev, api.ContractSpending{FundAccount: deposit.Add(cost)})

		// log the account balance after funding
		log.Debugw("fund account succeeded",
			"balance", balance.ExactString(),
			"deposit", deposit.ExactString(),
		)
		return deposit, nil
	})
}

func (h *hostClient) SyncAccount(ctx context.Context, rev *types.FileContractRevision) error {
	// fetch pricetable directly to bypass the gouging check
	pt, _, err := h.priceTables.fetch(ctx, h.hk, rev)
	if err != nil {
		return err
	}

	// check only the AccountBalanceCost
	if types.NewCurrency64(1).Cmp(pt.AccountBalanceCost) < 0 {
		return fmt.Errorf("%w: host is gouging on AccountBalanceCost", gouging.ErrPriceTableGouging)
	}

	return h.acc.WithSync(func() (types.Currency, error) {
		return h.client.SyncAccount(ctx, rev, h.hk, h.siamuxAddr, h.acc.ID(), pt.HostPriceTable, h.renterKey)
	})
}

// priceTable fetches a price table from the host. If a revision is provided, it
// will be used to pay for the price table. The returned price table is
// guaranteed to be safe to use.
func (h *hostClient) priceTable(ctx context.Context, rev *types.FileContractRevision) (rhpv3.HostPriceTable, types.Currency, error) {
	pt, cost, err := h.priceTables.fetch(ctx, h.hk, rev)
	if err != nil {
		return rhpv3.HostPriceTable{}, types.ZeroCurrency, err
	}
	gc, err := GougingCheckerFromContext(ctx, false)
	if err != nil {
		return rhpv3.HostPriceTable{}, cost, err
	}
	if breakdown := gc.Check(nil, &pt.HostPriceTable); breakdown.Gouging() {
		return rhpv3.HostPriceTable{}, cost, fmt.Errorf("%w: %v", gouging.ErrPriceTableGouging, breakdown)
	}
	return pt.HostPriceTable, cost, nil
}
