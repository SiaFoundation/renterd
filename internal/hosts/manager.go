package hosts

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/v2/api"
	"go.sia.tech/renterd/v2/internal/accounts"
	"go.sia.tech/renterd/v2/internal/contracts"
	"go.sia.tech/renterd/v2/internal/host"
	"go.sia.tech/renterd/v2/internal/prices"
	"go.sia.tech/renterd/v2/internal/utils"
	"go.uber.org/zap"

	rhpv4 "go.sia.tech/core/rhp/v4"

	rhp4 "go.sia.tech/renterd/v2/internal/rhp/v4"

	rhp "go.sia.tech/coreutils/rhp/v4"
)

var (
	_ Manager = (*hostManager)(nil)
)

type (
	AccountStore interface {
		ForHost(pk types.PublicKey) *accounts.Account
	}

	Dialer interface {
		Dial(ctx context.Context, hk types.PublicKey, address string) (net.Conn, error)
	}

	Manager interface {
		Downloader(hi api.HostInfo) host.Downloader
		Uploader(hi api.HostInfo, fcid types.FileContractID) host.Uploader
	}
)

type (
	hostManager struct {
		masterKey utils.MasterKey

		rhp4Client *rhp4.Client

		accounts    AccountStore
		contracts   contracts.SpendingRecorder
		pricesCache *prices.PricesCache
		logger      *zap.SugaredLogger
	}

	hostV2DownloadClient struct {
		hi   api.HostInfo
		acc  *accounts.Account
		pts  *prices.PricesCache
		rhp4 *rhp4.Client
	}

	hostV2UploadClient struct {
		fcid types.FileContractID
		hi   api.HostInfo
		rk   types.PrivateKey

		acc  *accounts.Account
		csr  contracts.SpendingRecorder
		pts  *prices.PricesCache
		rhp4 *rhp4.Client
	}
)

func NewManager(masterKey utils.MasterKey, as AccountStore, csr contracts.SpendingRecorder, dialer Dialer, logger *zap.Logger) Manager {
	logger = logger.Named("hostmanager")
	return &hostManager{
		masterKey: masterKey,

		rhp4Client: rhp4.New(dialer),

		accounts:    as,
		contracts:   csr,
		pricesCache: prices.NewPricesCache(),

		logger: logger.Sugar(),
	}
}

func (m *hostManager) Downloader(hi api.HostInfo) host.Downloader {
	return &hostV2DownloadClient{
		hi:   hi,
		acc:  m.accounts.ForHost(hi.PublicKey),
		pts:  m.pricesCache,
		rhp4: m.rhp4Client,
	}
}

func (m *hostManager) Uploader(hi api.HostInfo, fcid types.FileContractID) host.Uploader {
	return &hostV2UploadClient{
		fcid: fcid,
		hi:   hi,
		rk:   m.masterKey.DeriveContractKey(hi.PublicKey),

		acc:  m.accounts.ForHost(hi.PublicKey),
		csr:  m.contracts,
		pts:  m.pricesCache,
		rhp4: m.rhp4Client,
	}
}

func (c *hostV2DownloadClient) PublicKey() types.PublicKey { return c.hi.PublicKey }
func (c *hostV2UploadClient) PublicKey() types.PublicKey   { return c.hi.PublicKey }

func (c *hostV2DownloadClient) DownloadSector(ctx context.Context, w io.Writer, root types.Hash256, offset, length uint64) (err error) {
	return c.acc.WithWithdrawal(func() (types.Currency, error) {
		prices, err := c.pts.Fetch(ctx, c)
		if err != nil {
			return types.ZeroCurrency, err
		}

		res, err := c.rhp4.ReadSector(ctx, c.hi.PublicKey, c.hi.SiamuxAddr(), prices, c.acc.Token(), w, root, offset, length)
		if err != nil {
			return types.ZeroCurrency, err
		}
		return res.Usage.RenterCost(), nil
	})
}

func (c *hostV2DownloadClient) Prices(ctx context.Context) (rhpv4.HostPrices, error) {
	settings, err := c.rhp4.Settings(ctx, c.hi.PublicKey, c.hi.SiamuxAddr())
	if err != nil {
		return rhpv4.HostPrices{}, err
	}
	return settings.Prices, nil
}

func (c *hostV2UploadClient) UploadSector(ctx context.Context, sectorRoot types.Hash256, sector *[rhpv4.SectorSize]byte) error {
	fc, err := c.rhp4.LatestRevision(ctx, c.hi.PublicKey, c.hi.SiamuxAddr(), c.fcid)
	if err != nil {
		return errors.Join(err, rhp4.ErrFailedToFetchRevision)
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

		res, err := c.rhp4.WriteSector(ctx, c.hi.PublicKey, c.hi.SiamuxAddr(), prices, c.acc.Token(), utils.NewReaderLen(sector[:]), rhpv4.SectorSize)
		if err != nil {
			return types.ZeroCurrency, fmt.Errorf("failed to write sector: %w", err)
		}
		cost := res.Usage.RenterCost()

		res2, err := c.rhp4.AppendSectors(ctx, c.hi.PublicKey, c.hi.SiamuxAddr(), prices, c.rk, rev, []types.Hash256{res.Root})
		if err != nil {
			return cost, fmt.Errorf("failed to write sector: %w", err)
		}

		c.csr.RecordV2(rhp.ContractRevision{ID: rev.ID, Revision: res2.Revision}, api.ContractSpending{Uploads: res2.Usage.RenterCost()})
		return cost, nil
	})
}

func (c *hostV2UploadClient) Prices(ctx context.Context) (rhpv4.HostPrices, error) {
	settings, err := c.rhp4.Settings(ctx, c.hi.PublicKey, c.hi.SiamuxAddr())
	if err != nil {
		return rhpv4.HostPrices{}, err
	}
	return settings.Prices, nil
}
