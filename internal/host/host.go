package host

import (
	"context"
	"io"

	rhpv2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
)

type (
	Downloader interface {
		DownloadSector(ctx context.Context, w io.Writer, root types.Hash256, offset, length uint64) error
		PublicKey() types.PublicKey
	}

	Uploader interface {
		UploadSector(context.Context, types.Hash256, *[rhpv2.SectorSize]byte) error
		PublicKey() types.PublicKey
	}

	Host interface {
		PublicKey() types.PublicKey

		PriceTable(ctx context.Context, rev *types.FileContractRevision) (api.HostPriceTable, types.Currency, error)
		FetchRevision(ctx context.Context, fcid types.FileContractID) (types.FileContractRevision, error)

		FundAccount(ctx context.Context, balance types.Currency, rev *types.FileContractRevision) error
		SyncAccount(ctx context.Context, rev *types.FileContractRevision) error
	}

	HostManager interface {
		Downloader(hi api.HostInfo) Downloader
		Uploader(hi api.HostInfo, cm api.ContractMetadata) Uploader
		Host(hk types.PublicKey, fcid types.FileContractID, siamuxAddr string) Host
	}
)
