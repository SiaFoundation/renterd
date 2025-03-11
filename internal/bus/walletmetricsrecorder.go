package bus

import (
	"context"
	"sync"
	"time"

	"go.sia.tech/coreutils/wallet"
	"go.sia.tech/renterd/v2/api"
	"go.uber.org/zap"
)

type (
	WalletMetricsRecorder struct {
		store  MetricsStore
		wallet WalletBalance

		shutdownChan chan struct{}
		wg           sync.WaitGroup

		logger *zap.SugaredLogger
	}

	MetricsStore interface {
		RecordWalletMetric(ctx context.Context, metrics ...api.WalletMetric) error
	}

	WalletBalance interface {
		Balance() (wallet.Balance, error)
	}
)

// NewWalletMetricRecorder returns a recorder that periodically records wallet
// metrics. The recorder is already running and can be stopped by calling
// Shutdown.
func NewWalletMetricRecorder(store MetricsStore, wallet WalletBalance, interval time.Duration, logger *zap.Logger) *WalletMetricsRecorder {
	logger = logger.Named("walletmetricsrecorder")
	recorder := &WalletMetricsRecorder{
		store:        store,
		wallet:       wallet,
		shutdownChan: make(chan struct{}),
		logger:       logger.Sugar(),
	}
	recorder.run(interval)
	return recorder
}

func (wmr *WalletMetricsRecorder) run(interval time.Duration) {
	wmr.wg.Add(1)
	go func() {
		defer wmr.wg.Done()

		t := time.NewTicker(interval)
		defer t.Stop()

		for {
			balance, err := wmr.wallet.Balance()
			if err != nil {
				wmr.logger.Errorw("failed to get wallet balance", zap.Error(err))
			} else {
				ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
				if err = wmr.store.RecordWalletMetric(ctx, api.WalletMetric{
					Timestamp:   api.TimeRFC3339(time.Now().UTC()),
					Spendable:   balance.Spendable,
					Confirmed:   balance.Confirmed,
					Unconfirmed: balance.Unconfirmed,
					Immature:    balance.Immature,
				}); err != nil {
					wmr.logger.Errorw("failed to record wallet metric", zap.Error(err))
				} else {
					wmr.logger.Debugw("successfully recorded wallet metrics",
						zap.Stringer("spendable", balance.Spendable),
						zap.Stringer("confirmed", balance.Confirmed),
						zap.Stringer("unconfirmed", balance.Unconfirmed),
						zap.Stringer("immature", balance.Immature))
				}
				cancel()
			}

			select {
			case <-wmr.shutdownChan:
				return
			case <-t.C:
			}
		}
	}()
}

func (wmr *WalletMetricsRecorder) Shutdown(ctx context.Context) error {
	close(wmr.shutdownChan)

	waitChan := make(chan struct{})
	go func() {
		wmr.wg.Wait()
		close(waitChan)
	}()

	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-waitChan:
		return nil
	}
}
