package host

import (
	"context"
	"io"
	"time"

	rhpv2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
)

type (
	Host interface {
		PublicKey() types.PublicKey

		DownloadSector(ctx context.Context, w io.Writer, root types.Hash256, offset, length uint32, overpay bool) error
		UploadSector(ctx context.Context, sectorRoot types.Hash256, sector *[rhpv2.SectorSize]byte, rev types.FileContractRevision) error

		PriceTable(ctx context.Context, rev *types.FileContractRevision) (api.HostPriceTable, types.Currency, error)
		FetchRevision(ctx context.Context, fetchTimeout time.Duration) (types.FileContractRevision, error)

		FundAccount(ctx context.Context, balance types.Currency, rev *types.FileContractRevision) error
		SyncAccount(ctx context.Context, rev *types.FileContractRevision) error
	}

	HostManager interface {
		Host(hk types.PublicKey, fcid types.FileContractID, siamuxAddr string) Host
	}
)