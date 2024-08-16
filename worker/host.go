package worker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"time"

	rhpv2 "go.sia.tech/core/rhp/v2"
	rhpv3 "go.sia.tech/core/rhp/v3"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/internal/gouging"
	"go.uber.org/zap"
)

var (
	errFailedToCreatePayment = errors.New("failed to create payment")
)

type (
	Host interface {
		PublicKey() types.PublicKey

		DownloadSector(ctx context.Context, w io.Writer, root types.Hash256, offset, length uint32, overpay bool) error
		UploadSector(ctx context.Context, sectorRoot types.Hash256, sector *[rhpv2.SectorSize]byte, rev types.FileContractRevision) error

		FetchPriceTable(ctx context.Context, rev *types.FileContractRevision) (api.HostPriceTable, types.Currency, error)
		FetchRevision(ctx context.Context, fetchTimeout time.Duration) (types.FileContractRevision, error)

		AccountBalance(ctx context.Context, rev *types.FileContractRevision) (types.Currency, types.Currency, error)
		FundAccount(ctx context.Context, balance types.Currency, rev *types.FileContractRevision) error
		SyncAccount(ctx context.Context, rev *types.FileContractRevision) error

		RenewContract(ctx context.Context, rrr api.RHPRenewRequest) (_ rhpv2.ContractRevision, _ []types.Transaction, _, _ types.Currency, err error)
	}

	HostManager interface {
		Host(hk types.PublicKey, fcid types.FileContractID, siamuxAddr string) Host
	}
)

type (
	host struct {
		hk         types.PublicKey
		renterKey  types.PrivateKey
		accountKey types.PrivateKey
		fcid       types.FileContractID
		siamuxAddr string

		acc                      *account
		bus                      Bus
		contractSpendingRecorder ContractSpendingRecorder
		logger                   *zap.SugaredLogger
		transportPool            *transportPoolV3
		priceTables              *priceTables
	}
)

var (
	_ Host        = (*host)(nil)
	_ HostManager = (*Worker)(nil)
)

func (w *Worker) Host(hk types.PublicKey, fcid types.FileContractID, siamuxAddr string) Host {
	return &host{
		hk:                       hk,
		acc:                      w.accounts.ForHost(hk),
		bus:                      w.bus,
		contractSpendingRecorder: w.contractSpendingRecorder,
		logger:                   w.logger.Named(hk.String()[:4]),
		fcid:                     fcid,
		siamuxAddr:               siamuxAddr,
		renterKey:                w.deriveRenterKey(hk),
		accountKey:               w.accounts.deriveAccountKey(hk),
		transportPool:            w.transportPoolV3,
		priceTables:              w.priceTables,
	}
}

func (h *host) PublicKey() types.PublicKey { return h.hk }

func (h *host) DownloadSector(ctx context.Context, w io.Writer, root types.Hash256, offset, length uint32, overpay bool) (err error) {
	// return errBalanceInsufficient if balance insufficient
	defer func() {
		if isBalanceInsufficient(err) {
			err = fmt.Errorf("%w %v, err: %v", errBalanceInsufficient, h.hk, err)
		}
	}()

	var amount types.Currency
	return h.acc.WithWithdrawal(ctx, func() (types.Currency, error) {
		err := h.transportPool.withTransportV3(ctx, h.hk, h.siamuxAddr, func(ctx context.Context, t *transportV3) error {
			pt, err := h.priceTables.fetch(ctx, h.hk, nil, &amount)
			if err != nil {
				return err
			}
			hpt := pt.HostPriceTable

			gc, err := GougingCheckerFromContext(ctx, overpay)
			if err != nil {
				return err
			}
			if breakdown := gc.Check(nil, &hpt); breakdown.DownloadErr != "" {
				return fmt.Errorf("%w: %v", gouging.ErrPriceTableGouging, breakdown.DownloadErr)
			}

			cost, err := readSectorCost(hpt, uint64(length))
			if err != nil {
				return err
			}

			payment := rhpv3.PayByEphemeralAccount(h.acc.id, cost, pt.HostBlockHeight+defaultWithdrawalExpiryBlocks, h.accountKey)
			cost, refund, err := RPCReadSector(ctx, t, w, hpt, &payment, offset, length, root)
			if err != nil {
				return err
			}
			amount = amount.Add(cost)
			amount = amount.Sub(refund)
			return nil
		})
		return amount, err
	})
}

func (h *host) UploadSector(ctx context.Context, sectorRoot types.Hash256, sector *[rhpv2.SectorSize]byte, rev types.FileContractRevision) error {
	// fetch price table
	var pt rhpv3.HostPriceTable
	if err := h.acc.WithWithdrawal(ctx, func() (amount types.Currency, err error) {
		pt, err = h.priceTable(ctx, nil, &amount)
		return
	}); err != nil {
		return err
	}

	// prepare payment
	//
	// TODO: change to account payments once we have the means to check for an
	// insufficient balance error
	expectedCost, _, _, err := uploadSectorCost(pt, rev.WindowEnd)
	if err != nil {
		return err
	}
	if rev.RevisionNumber == math.MaxUint64 {
		return fmt.Errorf("revision number has reached max, fcid %v", rev.ParentID)
	}
	payment, ok := rhpv3.PayByContract(&rev, expectedCost, h.acc.id, h.renterKey)
	if !ok {
		return errFailedToCreatePayment
	}

	var cost types.Currency
	err = h.transportPool.withTransportV3(ctx, h.hk, h.siamuxAddr, func(ctx context.Context, t *transportV3) error {
		cost, err = RPCAppendSector(ctx, t, h.renterKey, pt, &rev, &payment, sectorRoot, sector)
		return err
	})
	if err != nil {
		return err
	}

	// record spending
	h.contractSpendingRecorder.Record(rev, api.ContractSpending{Uploads: cost})
	return nil
}

func (h *host) RenewContract(ctx context.Context, rrr api.RHPRenewRequest) (_ rhpv2.ContractRevision, _ []types.Transaction, _, _ types.Currency, err error) {
	// try to get a valid pricetable.
	ptCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	var pt *rhpv3.HostPriceTable
	if err := h.acc.WithWithdrawal(ptCtx, func() (amount types.Currency, _ error) {
		hpt, err := h.priceTables.fetch(ptCtx, h.hk, nil, &amount)
		if err == nil {
			pt = &hpt.HostPriceTable
		}
		return amount, err
	}); err != nil {
		h.logger.Infof("unable to fetch price table for renew: %v", err)
	}
	cancel()

	var contractPrice types.Currency
	var rev rhpv2.ContractRevision
	var txnSet []types.Transaction
	var renewErr error
	var fundAmount types.Currency
	err = h.transportPool.withTransportV3(ctx, h.hk, h.siamuxAddr, func(ctx context.Context, t *transportV3) (err error) {
		// NOTE: to avoid an edge case where the contract is drained and can
		// therefore not be used to pay for the revision, we simply don't pay
		// for it.
		_, err = RPCLatestRevision(ctx, t, h.fcid, func(revision *types.FileContractRevision) (rhpv3.HostPriceTable, rhpv3.PaymentMethod, error) {
			// Renew contract.
			rev, txnSet, contractPrice, fundAmount, renewErr = RPCRenew(ctx, rrr, h.bus, t, pt, *revision, h.renterKey, h.logger)
			return rhpv3.HostPriceTable{}, nil, nil
		})
		return err
	})
	if err != nil {
		return rhpv2.ContractRevision{}, nil, contractPrice, fundAmount, err
	}
	return rev, txnSet, contractPrice, fundAmount, renewErr
}

func (h *host) FetchPriceTable(ctx context.Context, rev *types.FileContractRevision) (hpt api.HostPriceTable, cost types.Currency, _ error) {
	return hpt, cost, h.transportPool.withTransportV3(ctx, h.hk, h.siamuxAddr, func(ctx context.Context, t *transportV3) (err error) {
		// pay by contract
		if rev != nil {
			hpt, err = RPCPriceTable(ctx, t, func(pt rhpv3.HostPriceTable) (rhpv3.PaymentMethod, error) {
				payment, err := payByContract(rev, pt.UpdatePriceTableCost, rhpv3.Account(h.accountKey.PublicKey()), h.renterKey)
				if err != nil {
					return nil, err
				}
				return &payment, nil
			})
			return
		}

		// pay by account
		hpt, err = RPCPriceTable(ctx, t, func(pt rhpv3.HostPriceTable) (rhpv3.PaymentMethod, error) {
			cost = pt.UpdatePriceTableCost
			payment := rhpv3.PayByEphemeralAccount(rhpv3.Account(h.accountKey.PublicKey()), cost, pt.HostBlockHeight+defaultWithdrawalExpiryBlocks, h.accountKey)
			return &payment, nil
		})
		return
	})
}

func (h *host) FundAccount(ctx context.Context, desired types.Currency, rev *types.FileContractRevision) error {
	log := h.logger.With(
		zap.Stringer("host", h.hk),
		zap.Stringer("account", h.acc.id),
	)

	// ensure we have at least 2H in the contract to cover the costs
	if types.NewCurrency64(2).Cmp(rev.ValidRenterPayout()) >= 0 {
		return fmt.Errorf("insufficient funds to fund account: %v <= %v", rev.ValidRenterPayout(), types.NewCurrency64(2))
	}

	// fetch current balance
	balance, err := h.acc.Balance(ctx)
	if err != nil {
		return err
	}

	// return early if we have the desired balance
	if balance.Cmp(desired) >= 0 {
		return nil
	}

	// calculate the deposit amount
	deposit := desired.Sub(balance)
	return h.acc.WithDeposit(ctx, func() (types.Currency, error) {
		if err := h.transportPool.withTransportV3(ctx, h.hk, h.siamuxAddr, func(ctx context.Context, t *transportV3) error {
			// fetch pricetable directly to bypass the gouging check
			pt, err := h.priceTables.fetch(ctx, h.hk, rev, nil)
			if err != nil {
				return err
			}

			// check only the unused defaults
			gc, err := GougingCheckerFromContext(ctx, false)
			if err != nil {
				return err
			} else if err := gc.CheckUnusedDefaults(pt.HostPriceTable); err != nil {
				return fmt.Errorf("%w: %v", gouging.ErrPriceTableGouging, err)
			}

			// cap the deposit by what's left in the contract
			cost := types.NewCurrency64(1)
			availableFunds := rev.ValidRenterPayout().Sub(cost)
			if deposit.Cmp(availableFunds) > 0 {
				deposit = availableFunds
			}

			// create the payment
			amount := deposit.Add(cost)
			payment, err := payByContract(rev, amount, rhpv3.Account{}, h.renterKey) // no account needed for funding
			if err != nil {
				return err
			}

			// fund the account
			if err := RPCFundAccount(ctx, t, &payment, h.acc.id, pt.UID); err != nil {
				return fmt.Errorf("failed to fund account with %v (excluding cost %v);%w", deposit, cost, err)
			}

			// record the spend
			h.contractSpendingRecorder.Record(*rev, api.ContractSpending{FundAccount: amount})

			// log the account balance after funding
			log.Debugw("fund account succeeded",
				"balance", balance.ExactString(),
				"deposit", deposit.ExactString(),
			)

			return nil
		}); err != nil {
			return types.ZeroCurrency, err
		}
		return deposit, nil
	})
}

func (h *host) AccountBalance(ctx context.Context, rev *types.FileContractRevision) (types.Currency, types.Currency, error) {
	// fetch pricetable directly to bypass the gouging check
	pt, err := h.priceTables.fetch(ctx, h.hk, rev, nil)
	if err != nil {
		return types.ZeroCurrency, types.ZeroCurrency, err
	}

	// check only the unused defaults
	gc, err := GougingCheckerFromContext(ctx, false)
	if err != nil {
		return types.ZeroCurrency, types.ZeroCurrency, err
	} else if err := gc.CheckUnusedDefaults(pt.HostPriceTable); err != nil {
		return types.ZeroCurrency, types.ZeroCurrency, fmt.Errorf("%w: %v", gouging.ErrPriceTableGouging, err)
	}
	renterBalance, err := h.acc.Balance(ctx)
	if err != nil {
		return types.ZeroCurrency, types.ZeroCurrency, err
	}

	var hostBalance types.Currency
	err = h.transportPool.withTransportV3(ctx, h.hk, h.siamuxAddr, func(ctx context.Context, t *transportV3) error {
		payment, err := payByContract(rev, types.NewCurrency64(1), h.acc.id, h.renterKey)
		if err != nil {
			return err
		}
		hostBalance, err = RPCAccountBalance(ctx, t, &payment, h.acc.id, pt.UID)
		return err
	})
	return renterBalance, hostBalance, err
}

func (h *host) SyncAccount(ctx context.Context, rev *types.FileContractRevision) error {
	// fetch pricetable directly to bypass the gouging check
	pt, err := h.priceTables.fetch(ctx, h.hk, rev, nil)
	if err != nil {
		return err
	}

	// check only the unused defaults
	gc, err := GougingCheckerFromContext(ctx, false)
	if err != nil {
		return err
	} else if err := gc.CheckUnusedDefaults(pt.HostPriceTable); err != nil {
		return fmt.Errorf("%w: %v", gouging.ErrPriceTableGouging, err)
	}

	// sync the account
	return h.acc.WithSync(ctx, func() (balance types.Currency, _ error) {
		return balance, h.transportPool.withTransportV3(ctx, h.hk, h.siamuxAddr, func(ctx context.Context, t *transportV3) error {
			payment, err := payByContract(rev, types.NewCurrency64(1), h.acc.id, h.renterKey)
			if err != nil {
				return err
			}
			balance, err = RPCAccountBalance(ctx, t, &payment, h.acc.id, pt.UID)
			return err
		})
	})
}
