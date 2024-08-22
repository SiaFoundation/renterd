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
	rhp3 "go.sia.tech/renterd/internal/rhp/v3"
	"go.sia.tech/renterd/internal/worker"
	"go.uber.org/zap"
)

type (
	Host interface {
		PublicKey() types.PublicKey

		DownloadSector(ctx context.Context, w io.Writer, root types.Hash256, offset, length uint32, overpay bool) error
		UploadSector(ctx context.Context, sectorRoot types.Hash256, sector *[rhpv2.SectorSize]byte, rev types.FileContractRevision) error

		PriceTable(ctx context.Context, rev *types.FileContractRevision) (hpt api.HostPriceTable, err error)
		PriceTableUnpaid(ctx context.Context) (hpt api.HostPriceTable, err error)
		FetchRevision(ctx context.Context, fetchTimeout time.Duration) (types.FileContractRevision, error)

		FundAccount(ctx context.Context, balance types.Currency, rev *types.FileContractRevision) error
		SyncAccount(ctx context.Context, rev *types.FileContractRevision) error

		RenewContract(ctx context.Context, rrr api.RHPRenewRequest) (_ rhpv2.ContractRevision, _ []types.Transaction, _, _ types.Currency, err error)
	}

	HostManager interface {
		Host(hk types.PublicKey, fcid types.FileContractID, siamuxAddr string) Host
	}
)

type (
	host struct {
		hk         types.PublicKey
		renterKey  types.PrivateKey
		fcid       types.FileContractID
		siamuxAddr string

		acc                      *worker.Account
		client                   *rhp3.Client
		bus                      Bus
		contractSpendingRecorder ContractSpendingRecorder
		logger                   *zap.SugaredLogger
		priceTables              *priceTables
	}
)

var (
	_ Host        = (*host)(nil)
	_ HostManager = (*Worker)(nil)
)

func (w *Worker) Host(hk types.PublicKey, fcid types.FileContractID, siamuxAddr string) Host {
	return &host{
		client:                   w.rhp3Client,
		hk:                       hk,
		acc:                      w.accounts.ForHost(hk),
		bus:                      w.bus,
		contractSpendingRecorder: w.contractSpendingRecorder,
		logger:                   w.logger.Named(hk.String()[:4]),
		fcid:                     fcid,
		siamuxAddr:               siamuxAddr,
		renterKey:                w.deriveRenterKey(hk),
		priceTables:              w.priceTables,
	}
}

func (h *host) PublicKey() types.PublicKey { return h.hk }

func (h *host) DownloadSector(ctx context.Context, w io.Writer, root types.Hash256, offset, length uint32, overpay bool) (err error) {
	pt, err := h.priceTables.fetch(ctx, h.hk, nil)
	if err != nil {
		return err
	}
	hpt := pt.HostPriceTable

	// check for download gouging specifically
	gc, err := GougingCheckerFromContext(ctx, overpay)
	if err != nil {
		return err
	}
	if breakdown := gc.Check(nil, &hpt); breakdown.DownloadErr != "" {
		return fmt.Errorf("%w: %v", gouging.ErrPriceTableGouging, breakdown.DownloadErr)
	}

	return h.acc.WithWithdrawal(func() (amount types.Currency, err error) {
		return h.client.ReadSector(ctx, offset, length, root, w, h.hk, h.siamuxAddr, h.acc.ID(), h.acc.Key(), hpt)
	})
}

func (h *host) UploadSector(ctx context.Context, sectorRoot types.Hash256, sector *[rhpv2.SectorSize]byte, rev types.FileContractRevision) (err error) {
	// fetch price table
	pt, err := h.priceTable(ctx, nil)
	if err != nil {
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

func (h *host) RenewContract(ctx context.Context, rrr api.RHPRenewRequest) (_ rhpv2.ContractRevision, _ []types.Transaction, _, _ types.Currency, err error) {
	gc, err := h.gougingChecker(ctx, false)
	if err != nil {
		return rhpv2.ContractRevision{}, nil, types.ZeroCurrency, types.ZeroCurrency, err
	}
	revision, err := h.client.Revision(ctx, h.fcid, h.hk, h.siamuxAddr)
	if err != nil {
		return rhpv2.ContractRevision{}, nil, types.ZeroCurrency, types.ZeroCurrency, err
	}

	// helper to discard txn on error
	discardTxn := func(ctx context.Context, txn types.Transaction, err *error) {
		if *err == nil {
			return
		}

		ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		if dErr := h.bus.WalletDiscard(ctx, txn); dErr != nil {
			h.logger.Errorf("%v: %s, failed to discard txn: %v", *err, dErr)
		}
		cancel()
	}

	// helper to sign txn
	signTxn := func(ctx context.Context, txn *types.Transaction, toSign []types.Hash256, cf types.CoveredFields) error {
		// sign txn
		return h.bus.WalletSign(ctx, txn, toSign, cf)
	}

	// helper to prepare contract renewal
	prepareRenew := func(ctx context.Context, revision types.FileContractRevision, hostAddress, renterAddress types.Address, renterKey types.PrivateKey, renterFunds, minNewCollateral, maxFundAmount types.Currency, pt rhpv3.HostPriceTable, endHeight, windowSize, expectedStorage uint64) (api.WalletPrepareRenewResponse, func(context.Context, types.Transaction, *error), error) {
		resp, err := h.bus.WalletPrepareRenew(ctx, revision, hostAddress, renterAddress, renterKey, renterFunds, minNewCollateral, maxFundAmount, pt, endHeight, windowSize, expectedStorage)
		if err != nil {
			return api.WalletPrepareRenewResponse{}, nil, err
		}
		return resp, discardTxn, nil
	}

	// renew contract
	rev, txnSet, contractPrice, fundAmount, err := h.client.Renew(ctx, rrr, gc, prepareRenew, signTxn, revision, h.renterKey)
	if err != nil {
		return rhpv2.ContractRevision{}, nil, contractPrice, fundAmount, err
	}
	return rev, txnSet, contractPrice, fundAmount, err
}

func (h *host) PriceTableUnpaid(ctx context.Context) (api.HostPriceTable, error) {
	return h.client.PriceTableUnpaid(ctx, h.hk, h.siamuxAddr)
}

func (h *host) PriceTable(ctx context.Context, rev *types.FileContractRevision) (hpt api.HostPriceTable, err error) {
	// fetchPT is a helper function that performs the RPC given a payment function
	fetchPT := func(paymentFn rhp3.PriceTablePaymentFunc) (api.HostPriceTable, error) {
		return h.client.PriceTable(ctx, h.hk, h.siamuxAddr, paymentFn)
	}

	// pay by contract if a revision is given
	if rev != nil {
		return fetchPT(rhp3.PreparePriceTableContractPayment(rev, h.acc.ID(), h.renterKey))
	}

	// pay by account
	return fetchPT(rhp3.PreparePriceTableAccountPayment(h.acc.Key()))
}

// FetchRevision tries to fetch a contract revision from the host.
func (h *host) FetchRevision(ctx context.Context, fetchTimeout time.Duration) (types.FileContractRevision, error) {
	if fetchTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, fetchTimeout)
		defer cancel()
	}
	// Try to fetch the revision with an account first.
	return h.client.Revision(ctx, h.fcid, h.hk, h.siamuxAddr)
}

func (h *host) FundAccount(ctx context.Context, balance types.Currency, rev *types.FileContractRevision) error {
	return h.acc.WithDeposit(func(curr types.Currency) (types.Currency, error) {
		// return early if we have the desired balance
		if curr.Cmp(balance) >= 0 {
			return types.ZeroCurrency, nil
		}
		deposit := balance.Sub(curr)

		// fetch pricetable directly to bypass the gouging check
		pt, err := h.priceTables.fetch(ctx, h.hk, rev)
		if err != nil {
			return types.ZeroCurrency, err
		}

		// check whether we have money left in the contract
		cost := types.NewCurrency64(1)
		if cost.Cmp(rev.ValidRenterPayout()) >= 0 {
			return types.ZeroCurrency, fmt.Errorf("insufficient funds to fund account: %v <= %v", rev.ValidRenterPayout(), cost)
		}
		availableFunds := rev.ValidRenterPayout().Sub(cost)

		// cap the deposit amount by the money that's left in the contract
		if deposit.Cmp(availableFunds) > 0 {
			deposit = availableFunds
		}
		if err := h.client.FundAccount(ctx, rev, h.hk, h.siamuxAddr, deposit, h.acc.ID(), pt.HostPriceTable, h.renterKey); err != nil {
			return types.ZeroCurrency, fmt.Errorf("failed to fund account with %v; %w", deposit, err)
		}
		// record the spend
		h.contractSpendingRecorder.Record(*rev, api.ContractSpending{FundAccount: deposit.Add(cost)})
		return deposit, nil
	})
}

func (h *host) SyncAccount(ctx context.Context, rev *types.FileContractRevision) error {
	// fetch pricetable directly to bypass the gouging check
	pt, err := h.priceTables.fetch(ctx, h.hk, rev)
	if err != nil {
		return err
	}

	// check only the unused defaults
	gc, err := GougingCheckerFromContext(ctx, false)
	if err != nil {
		return err
	} else if err := gc.CheckUnusedDefaults(pt.HostPriceTable); err != nil {
		return fmt.Errorf("%w: %v", gouging.ErrPriceTableGouging, err)
	}

	return h.acc.WithSync(func() (types.Currency, error) {
		return h.client.SyncAccount(ctx, rev, h.hk, h.siamuxAddr, h.acc.ID(), pt.UID, h.renterKey)
	})
}

func (h *host) gougingChecker(ctx context.Context, criticalMigration bool) (gouging.Checker, error) {
	gp, err := h.bus.GougingParams(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get gouging params: %w", err)
	}
	return newGougingChecker(gp.GougingSettings, gp.ConsensusState, gp.TransactionFee, criticalMigration), nil
}

// priceTable fetches a price table from the host. If a revision is provided, it
// will be used to pay for the price table. The returned price table is
// guaranteed to be safe to use.
func (h *host) priceTable(ctx context.Context, rev *types.FileContractRevision) (rhpv3.HostPriceTable, error) {
	pt, err := h.priceTables.fetch(ctx, h.hk, rev)
	if err != nil {
		return rhpv3.HostPriceTable{}, err
	}
	gc, err := GougingCheckerFromContext(ctx, false)
	if err != nil {
		return rhpv3.HostPriceTable{}, err
	}
	if breakdown := gc.Check(nil, &pt.HostPriceTable); breakdown.Gouging() {
		return rhpv3.HostPriceTable{}, fmt.Errorf("%w: %v", gouging.ErrPriceTableGouging, breakdown)
	}
	return pt.HostPriceTable, nil
}
