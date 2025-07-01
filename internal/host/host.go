package host

import (
	"context"
	"io"

	rhpv4 "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
)

type (
	Downloader interface {
		DownloadSector(ctx context.Context, w io.Writer, root types.Hash256, offset, length uint64) error
		PublicKey() types.PublicKey
	}

	Uploader interface {
		UploadSector(context.Context, types.Hash256, *[rhpv4.SectorSize]byte) error
		PublicKey() types.PublicKey
	}

	Host interface {
		PublicKey() types.PublicKey

		FetchRevision(ctx context.Context, fcid types.FileContractID) (types.FileContractRevision, error)
	}
)
