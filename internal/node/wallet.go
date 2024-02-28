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

type metricRecorder interface {
	RecordWalletMetric(ctx context.Context, metrics ...api.WalletMetric) error
}

type singleAddressWallet struct {
	*wallet.SingleAddressWallet

	cm     *chain.Manager
	mr     metricRecorder
	logger *zap.SugaredLogger
}

func NewSingleAddressWallet(seed types.PrivateKey, cm *chain.Manager, store wallet.SingleAddressStore, mr metricRecorder, l *zap.SugaredLogger, opts ...wallet.Option) (*singleAddressWallet, error) {
	w, err := wallet.NewSingleAddressWallet(seed, cm, store, opts...)
	if err != nil {
		return nil, err
	}

	return &singleAddressWallet{w, cm, mr, l}, nil
}

func (w *singleAddressWallet) ProcessChainApplyUpdate(cau *chain.ApplyUpdate, mayCommit bool) error {
	// escape early if we're not synced
	if !w.isSynced() {
		return nil
	}

	// fetch balance
	balance, err := w.Balance()
	if err != nil {
		w.logger.Errorf("failed to fetch wallet balance, err: %v", err)
		return nil
	}

	// apply sane timeout
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	// record wallet metric
	err = w.mr.RecordWalletMetric(ctx, api.WalletMetric{
		Timestamp:   api.TimeNow(),
		Confirmed:   balance.Confirmed,
		Unconfirmed: balance.Unconfirmed,
		Spendable:   balance.Spendable,
	})
	if err != nil {
		w.logger.Errorf("failed to record wallet metric, err: %v", err)
		return nil
	}

	return nil
}

func (w *singleAddressWallet) ProcessChainRevertUpdate(cru *chain.RevertUpdate) error { return nil }

func (w *singleAddressWallet) isSynced() bool {
	var synced bool
	if block, ok := w.cm.Block(w.cm.Tip().ID); ok && time.Since(block.Timestamp) < 2*w.cm.TipState().BlockInterval() {
		synced = true
	}
	return synced
}
