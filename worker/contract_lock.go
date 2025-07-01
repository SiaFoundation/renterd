package worker

import (
	"context"
	"time"

	"go.sia.tech/core/types"
)

type ContractLocker interface {
	AcquireContract(ctx context.Context, fcid types.FileContractID, priority int, d time.Duration) (lockID uint64, err error)
	KeepaliveContract(ctx context.Context, fcid types.FileContractID, lockID uint64, d time.Duration) (err error)
	ReleaseContract(ctx context.Context, fcid types.FileContractID, lockID uint64) (err error)
}

var _ ContractLocker = (Bus)(nil)
