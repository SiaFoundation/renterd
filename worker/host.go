package worker

import (
	"context"
	"fmt"
	"io"
	"math"

	rhpv2 "go.sia.tech/core/rhp/v2"
	rhpv3 "go.sia.tech/core/rhp/v3"
	rhpv4 "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	rhp "go.sia.tech/coreutils/rhp/v4"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/internal/gouging"
	"go.sia.tech/renterd/internal/host"
	"go.sia.tech/renterd/internal/prices"
	rhp3 "go.sia.tech/renterd/internal/rhp/v3"
	rhp4 "go.sia.tech/renterd/internal/rhp/v4"
	"go.sia.tech/renterd/internal/worker"
	"go.uber.org/zap"
)

type (
	hostClient struct {
		hk         types.PublicKey
		renterKey  types.PrivateKey
		siamuxAddr string

		acc    *worker.Account
		csr    ContractSpendingRecorder
		pts    *prices.PriceTables
		rhp3   *rhp3.Client
		logger *zap.SugaredLogger
	}

	hostDownloadClient struct {
		hi   api.HostInfo
		acc  *worker.Account
		pts  *prices.PriceTables
		rhp3 *rhp3.Client
	}

	hostV2DownloadClient struct {
		hi   api.HostInfo
		acc  *worker.Account
		pts  *prices.PricesCache
		rhp4 *rhp4.Client
	}

	hostUploadClient struct {
		fcid types.FileContractID
		hi   api.HostInfo
		rk   types.PrivateKey

		acc  *worker.Account
		csr  ContractSpendingRecorder
		pts  *prices.PriceTables
		rhp3 *rhp3.Client
	}

	hostV2UploadClient struct {
		fcid types.FileContractID
		hi   api.HostInfo
		rk   types.PrivateKey

		acc  *worker.Account
		csr  ContractSpendingRecorder
		pts  *prices.PricesCache
		rhp4 *rhp4.Client
	}
)

var (
	_ host.Host        = (*hostClient)(nil)
	_ host.HostManager = (*Worker)(nil)
)

func (w *Worker) Host(hk types.PublicKey, fcid types.FileContractID, siamuxAddr string) host.Host {
	return &hostClient{
		rhp3:       w.rhp3Client,
		hk:         hk,
		acc:        w.accounts.ForHost(hk),
		csr:        w.contractSpendingRecorder,
		logger:     w.logger.Named(hk.String()[:4]),
		siamuxAddr: siamuxAddr,
		renterKey:  w.deriveRenterKey(hk),
		pts:        w.priceTables,
	}
}

func (w *Worker) Downloader(hi api.HostInfo) host.Downloader {
	if hi.IsV2() {
		return &hostV2DownloadClient{
			hi:   hi,
			acc:  w.accounts.ForHost(hi.PublicKey),
			pts:  w.pricesCache,
			rhp4: w.rhp4Client,
		}
	}
	return &hostDownloadClient{
		hi:   hi,
		acc:  w.accounts.ForHost(hi.PublicKey),
		pts:  w.priceTables,
		rhp3: w.rhp3Client,
	}
}

func (w *Worker) Uploader(hi api.HostInfo, fcid types.FileContractID) host.Uploader {
	if hi.IsV2() {
		return &hostV2UploadClient{
			fcid: fcid,
			hi:   hi,
			rk:   w.deriveRenterKey(hi.PublicKey),

			acc:  w.accounts.ForHost(hi.PublicKey),
			csr:  w.contractSpendingRecorder,
			pts:  w.pricesCache,
			rhp4: w.rhp4Client,
		}
	}
	return &hostUploadClient{
		fcid: fcid,
		hi:   hi,
		rk:   w.deriveRenterKey(hi.PublicKey),

		acc:  w.accounts.ForHost(hi.PublicKey),
		csr:  w.contractSpendingRecorder,
		pts:  w.priceTables,
		rhp3: w.rhp3Client,
	}
}

func (c *hostClient) PublicKey() types.PublicKey           { return c.hk }
func (c *hostDownloadClient) PublicKey() types.PublicKey   { return c.hi.PublicKey }
func (c *hostV2DownloadClient) PublicKey() types.PublicKey { return c.hi.PublicKey }
func (c *hostUploadClient) PublicKey() types.PublicKey     { return c.hi.PublicKey }
func (c *hostV2UploadClient) PublicKey() types.PublicKey   { return c.hi.PublicKey }

func (h *hostClient) PriceTableUnpaid(ctx context.Context) (api.HostPriceTable, error) {
	return h.rhp3.PriceTableUnpaid(ctx, h.hk, h.siamuxAddr)
}

func (h *hostClient) PriceTable(ctx context.Context, rev *types.FileContractRevision) (hpt api.HostPriceTable, cost types.Currency, err error) {
	// fetchPT is a helper function that performs the RPC given a payment function
	fetchPT := func(paymentFn rhp3.PriceTablePaymentFunc) (api.HostPriceTable, error) {
		return h.rhp3.PriceTable(ctx, h.hk, h.siamuxAddr, paymentFn)
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
func (h *hostClient) FetchRevision(ctx context.Context, fcid types.FileContractID) (types.FileContractRevision, error) {
	return h.rhp3.Revision(ctx, fcid, h.hk, h.siamuxAddr)
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
		pt, _, err := h.pts.Fetch(ctx, h, rev)
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
		if err := h.rhp3.FundAccount(ctx, rev, h.hk, h.siamuxAddr, deposit, h.acc.ID(), pt.HostPriceTable, h.renterKey); err != nil {
			if rhp3.IsBalanceMaxExceeded(err) {
				h.acc.ScheduleSync()
			}
			return types.ZeroCurrency, fmt.Errorf("failed to fund account with %v; %w", deposit, err)
		}

		// record the spend
		h.csr.RecordV1(*rev, api.ContractSpending{FundAccount: deposit.Add(cost)})

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
	pt, _, err := h.pts.Fetch(ctx, h, rev)
	if err != nil {
		return err
	}

	// check only the AccountBalanceCost
	if types.NewCurrency64(1).Cmp(pt.AccountBalanceCost) < 0 {
		return fmt.Errorf("%w: host is gouging on AccountBalanceCost", gouging.ErrPriceTableGouging)
	}

	return h.acc.WithSync(func() (types.Currency, error) {
		return h.rhp3.SyncAccount(ctx, rev, h.hk, h.siamuxAddr, h.acc.ID(), pt.HostPriceTable, h.renterKey)
	})
}

func (c *hostDownloadClient) DownloadSector(ctx context.Context, w io.Writer, root types.Hash256, offset, length uint64) (err error) {
	return c.acc.WithWithdrawal(func() (types.Currency, error) {
		pt, ptc, err := c.pts.Fetch(ctx, c, nil)
		if err != nil {
			return types.ZeroCurrency, err
		}

		cost, err := c.rhp3.ReadSector(ctx, offset, length, root, w, c.hi.PublicKey, c.hi.SiamuxAddr, c.acc.ID(), c.acc.Key(), pt.HostPriceTable)
		if err != nil {
			return ptc, err
		}
		return ptc.Add(cost), nil
	})
}

func (c *hostDownloadClient) PriceTable(ctx context.Context, rev *types.FileContractRevision) (hpt api.HostPriceTable, cost types.Currency, err error) {
	hpt, err = c.rhp3.PriceTable(ctx, c.hi.PublicKey, c.hi.SiamuxAddr, rhp3.PreparePriceTableAccountPayment(c.acc.Key()))
	if err == nil {
		cost = hpt.UpdatePriceTableCost
	}
	return
}

func (c *hostV2DownloadClient) DownloadSector(ctx context.Context, w io.Writer, root types.Hash256, offset, length uint64) (err error) {
	return c.acc.WithWithdrawal(func() (types.Currency, error) {
		prices, err := c.pts.Fetch(ctx, c)
		if err != nil {
			return types.ZeroCurrency, err
		}

		res, err := c.rhp4.ReadSector(ctx, c.hi.PublicKey, c.hi.V2SiamuxAddr(), prices, c.acc.Token(), w, root, offset, length)
		if err != nil {
			return types.ZeroCurrency, err
		}
		return res.Usage.RenterCost(), nil
	})
}

func (c *hostV2DownloadClient) Prices(ctx context.Context) (rhpv4.HostPrices, error) {
	settings, err := c.rhp4.Settings(ctx, c.hi.PublicKey, c.hi.V2SiamuxAddr())
	if err != nil {
		return rhpv4.HostPrices{}, err
	}
	return settings.Prices, nil
}

func (c *hostUploadClient) PriceTable(ctx context.Context, rev *types.FileContractRevision) (hpt api.HostPriceTable, cost types.Currency, err error) {
	hpt, err = c.rhp3.PriceTable(ctx, c.hi.PublicKey, c.hi.SiamuxAddr, rhp3.PreparePriceTableAccountPayment(c.acc.Key()))
	if err == nil {
		cost = hpt.UpdatePriceTableCost
	}
	return
}

func (c *hostUploadClient) UploadSector(ctx context.Context, sectorRoot types.Hash256, sector *[rhpv2.SectorSize]byte) error {
	rev, err := c.rhp3.Revision(ctx, c.fcid, c.hi.PublicKey, c.hi.SiamuxAddr)
	if err != nil {
		return fmt.Errorf("%w; %w", rhp3.ErrFailedToFetchRevision, err)
	} else if rev.RevisionNumber == math.MaxUint64 {
		return rhp3.ErrMaxRevisionReached
	}

	var hpt rhpv3.HostPriceTable
	if err := c.acc.WithWithdrawal(func() (amount types.Currency, err error) {
		pt, cost, err := c.pts.Fetch(ctx, c, nil)
		if err != nil {
			return types.ZeroCurrency, err
		}
		hpt = pt.HostPriceTable

		gc, err := GougingCheckerFromContext(ctx)
		if err != nil {
			return cost, err
		}
		if breakdown := gc.CheckV1(nil, &pt.HostPriceTable); breakdown.Gouging() {
			return cost, fmt.Errorf("%w: %v", gouging.ErrPriceTableGouging, breakdown)
		}
		return cost, nil
	}); err != nil {
		return err
	}

	cost, err := c.rhp3.AppendSector(ctx, sectorRoot, sector, &rev, c.hi.PublicKey, c.hi.SiamuxAddr, c.acc.ID(), hpt, c.rk)
	if err != nil {
		return fmt.Errorf("failed to upload sector: %w", err)
	}

	c.csr.RecordV1(rev, api.ContractSpending{Uploads: cost})
	return nil
}

func (c *hostV2UploadClient) UploadSector(ctx context.Context, sectorRoot types.Hash256, sector *[rhpv2.SectorSize]byte) error {
	fc, err := c.rhp4.LatestRevision(ctx, c.hi.PublicKey, c.hi.V2SiamuxAddr(), c.fcid)
	if err != nil {
		return err
	}

	rev := rhp.ContractRevision{
		ID:       c.fcid,
		Revision: fc,
	}

	return c.acc.WithWithdrawal(func() (types.Currency, error) {
		prices, err := c.pts.Fetch(ctx, c)
		if err != nil {
			return types.ZeroCurrency, err
		}

		res, err := c.rhp4.WriteSector(ctx, c.hi.PublicKey, c.hi.V2SiamuxAddr(), prices, c.acc.Token(), NewReaderLen(sector[:]), rhpv2.SectorSize, api.BlocksPerDay*3)
		if err != nil {
			return types.ZeroCurrency, fmt.Errorf("failed to write sector: %w", err)
		}
		cost := res.Usage.RenterCost()

		res2, err := c.rhp4.AppendSectors(ctx, c.hi.PublicKey, c.hi.V2SiamuxAddr(), prices, c.rk, rev, []types.Hash256{res.Root})
		if err != nil {
			return cost, fmt.Errorf("failed to write sector: %w", err)
		}

		c.csr.RecordV2(rhp.ContractRevision{ID: rev.ID, Revision: res2.Revision}, api.ContractSpending{Uploads: res2.Usage.RenterCost()})
		return cost, nil
	})
}

func (c *hostV2UploadClient) Prices(ctx context.Context) (rhpv4.HostPrices, error) {
	settings, err := c.rhp4.Settings(ctx, c.hi.PublicKey, c.hi.V2SiamuxAddr())
	if err != nil {
		return rhpv4.HostPrices{}, err
	}
	return settings.Prices, nil
}
