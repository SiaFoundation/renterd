package node

import (
	"context"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/coreutils/wallet"
	"go.sia.tech/renterd/api"
	"go.uber.org/zap"
)

// TODO: feels quite hacky

type metricRecorder interface {
	RecordWalletMetric(ctx context.Context, metrics ...api.WalletMetric) error
}

type singleAddressWallet struct {
	*wallet.SingleAddressWallet

	blockInterval time.Duration
	cm            *chain.Manager
	mr            metricRecorder
	logger        *zap.SugaredLogger
}

func NewSingleAddressWallet(seed types.PrivateKey, blockInterval time.Duration, cm *chain.Manager, store wallet.SingleAddressStore, mr metricRecorder, l *zap.SugaredLogger, opts ...wallet.Option) (*singleAddressWallet, error) {
	w, err := wallet.NewSingleAddressWallet(seed, cm, store, opts...)
	if err != nil {
		return nil, err
	}

	return &singleAddressWallet{w, blockInterval, cm, mr, l}, nil
}

func (w *singleAddressWallet) ProcessChainApplyUpdate(cau *chain.ApplyUpdate, mayCommit bool) error {
	// escape early if we're not synced
	if time.Since(cau.Block.Timestamp) >= 2*w.blockInterval {
		return nil
	}

	// record metric in a goroutine
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		w.recordMetric(ctx)
		cancel()
	}()

	return nil
}

func (w *singleAddressWallet) ProcessChainRevertUpdate(cru *chain.RevertUpdate) error { return nil }

func (w *singleAddressWallet) recordMetric(ctx context.Context) {
	if balance, err := w.Balance(); err != nil {
		w.logger.Errorf("failed to fetch wallet balance, err: %v", err)
		return
	} else if err := w.mr.RecordWalletMetric(ctx, api.WalletMetric{
		Timestamp:   api.TimeNow(),
		Confirmed:   balance.Confirmed,
		Unconfirmed: balance.Unconfirmed,
		Spendable:   balance.Spendable,
	}); err != nil {
		w.logger.Errorf("failed to record wallet metric, err: %v", err)
	}
}
