package walletmaintainer

import (
	"context"
	"fmt"
	"sync"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/wallet"
	"go.sia.tech/renterd/alerts"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/autopilot/contractor"
	"go.uber.org/zap"
)

type (
	Bus interface {
		Wallet(ctx context.Context) (api.WalletResponse, error)
		WalletPending(ctx context.Context) (resp []wallet.Event, err error)
		WalletRedistribute(ctx context.Context, outputs int, amount types.Currency) (ids []types.TransactionID, err error)
	}
)

type (
	walletMaintainer struct {
		alerter alerts.Alerter
		bus     Bus
		logger  *zap.SugaredLogger

		mu                sync.Mutex
		maintenanceTxnIDs []types.TransactionID
	}
)

func New(alerter alerts.Alerter, bus Bus, logger *zap.Logger) *walletMaintainer {
	return &walletMaintainer{
		alerter: alerter,
		bus:     bus,
		logger:  logger.Named("wallet").Sugar(),
	}
}

func (w *walletMaintainer) PerformWalletMaintenance(ctx context.Context, cfg api.AutopilotConfig) error {
	w.logger.Info("performing wallet maintenance")

	wallet, err := w.bus.Wallet(ctx)
	if err != nil {
		w.logger.Warnf("wallet maintenance skipped, fetching wallet failed with err: %v", err)
		return fmt.Errorf("failed to fetch wallet: %w", err)
	}

	// register an alert if balance is low
	balance := wallet.Confirmed
	if balance.Cmp(contractor.InitialContractFunding.Mul64(cfg.Contracts.Amount)) < 0 {
		if err := w.alerter.RegisterAlert(ctx, newAccountLowBalanceAlert(wallet.Address, balance, contractor.InitialContractFunding)); err != nil {
			w.logger.Warnf("failed to register low balance alert: %v", err)
		}
	} else {
		if err := w.alerter.DismissAlerts(ctx, alertLowBalanceID); err != nil {
			w.logger.Warnf("failed to dismiss low balance alert: %v", err)
		}
	}

	w.mu.Lock()
	maintenanceTxnIDs := w.maintenanceTxnIDs
	w.mu.Unlock()

	// pending maintenance transaction - nothing to do
	pending, err := w.bus.WalletPending(ctx)
	if err != nil {
		return nil
	}
	for _, txn := range pending {
		for _, mTxnID := range maintenanceTxnIDs {
			if mTxnID == types.TransactionID(txn.ID) {
				w.logger.Debugf("wallet maintenance skipped, pending transaction found with id %v", mTxnID)
				return nil
			}
		}
	}

	// calculate num outputs
	const maxOutputs = 100
	amount := contractor.InitialContractFunding.Mul64(10)
	numOutputs := balance.Div(amount).Big().Uint64()
	if numOutputs > maxOutputs {
		numOutputs = maxOutputs
	}

	// skip maintenance if wallet balance is too low
	if numOutputs < 10 {
		w.logger.Warnf("wallet maintenance skipped, the balance of %v is too low to redistribute into outputs of %v, at a minimum we want to redistribute into 10 outputs, so the balance should be at least %v", balance, amount, amount.Mul64(10))
		return nil
	}

	// redistribute outputs
	ids, err := w.bus.WalletRedistribute(ctx, int(numOutputs), amount)
	if err != nil {
		return fmt.Errorf("failed to redistribute wallet into %d outputs of amount %v, balance %v, err %v", int(numOutputs), amount, balance, err)
	}

	w.logger.Infof("wallet maintenance succeeded, txns %v", ids)

	w.mu.Lock()
	maintenanceTxnIDs = ids
	w.mu.Unlock()

	return nil
}
