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
	"go.sia.tech/renterd/hostdb"
	"go.uber.org/zap"
)

type (
	Host interface {
		PublicKey() types.PublicKey

		DownloadSector(ctx context.Context, w io.Writer, root types.Hash256, offset, length uint32, overpay bool) error
		UploadSector(ctx context.Context, sectorRoot types.Hash256, sector *[rhpv2.SectorSize]byte, rev types.FileContractRevision) error

		FetchPriceTable(ctx context.Context, rev *types.FileContractRevision) (hpt hostdb.HostPriceTable, err error)
		FetchRevision(ctx context.Context, fetchTimeout time.Duration) (types.FileContractRevision, error)

		FundAccount(ctx context.Context, balance types.Currency, rev *types.FileContractRevision) error
		SyncAccount(ctx context.Context, rev *types.FileContractRevision) error

		RenewContract(ctx context.Context, rrr api.RHPRenewRequest) (_ rhpv2.ContractRevision, _ []types.Transaction, _ types.Currency, err error)
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
	_ HostManager = (*worker)(nil)
)

func (w *worker) Host(hk types.PublicKey, fcid types.FileContractID, siamuxAddr string) Host {
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
	pt, err := h.priceTables.fetch(ctx, h.hk, nil)
	if err != nil {
		return err
	}
	hpt := pt.HostPriceTable

	// check for download gouging specifically
	gc, err := GougingCheckerFromContext(ctx, overpay)
	if err != nil {
		return err
	}
	if breakdown := gc.Check(nil, &hpt); breakdown.Gouging() {
		return fmt.Errorf("%w: %v", errPriceTableGouging, breakdown)
	}

	// return errBalanceInsufficient if balance insufficient
	defer func() {
		if isBalanceInsufficient(err) {
			err = fmt.Errorf("%w %v, err: %v", errBalanceInsufficient, h.hk, err)
		}
	}()

	return h.acc.WithWithdrawal(ctx, func() (amount types.Currency, err error) {
		err = h.transportPool.withTransportV3(ctx, h.hk, h.siamuxAddr, func(ctx context.Context, t *transportV3) error {
			cost, err := readSectorCost(hpt, uint64(length))
			if err != nil {
				return err
			}

			var refund types.Currency
			payment := rhpv3.PayByEphemeralAccount(h.acc.id, cost, pt.HostBlockHeight+defaultWithdrawalExpiryBlocks, h.accountKey)
			cost, refund, err = RPCReadSector(ctx, t, w, hpt, &payment, offset, length, root)
			amount = cost.Sub(refund)
			return err
		})
		return
	})
}

func (h *host) UploadSector(ctx context.Context, sectorRoot types.Hash256, sector *[rhpv2.SectorSize]byte, rev types.FileContractRevision) (err error) {
	// fetch price table
	pt, err := h.priceTable(ctx, nil)
	if err != nil {
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
		return errors.New("failed to create payment")
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

func (h *host) RenewContract(ctx context.Context, rrr api.RHPRenewRequest) (_ rhpv2.ContractRevision, _ []types.Transaction, _ types.Currency, err error) {
	// Try to get a valid pricetable.
	ptCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	var pt *rhpv3.HostPriceTable
	hpt, err := h.priceTables.fetch(ptCtx, h.hk, nil)
	if err == nil {
		pt = &hpt.HostPriceTable
	} else {
		h.logger.Infof("unable to fetch price table for renew: %v", err)
	}

	var contractPrice types.Currency
	var rev rhpv2.ContractRevision
	var txnSet []types.Transaction
	var renewErr error
	err = h.transportPool.withTransportV3(ctx, h.hk, h.siamuxAddr, func(ctx context.Context, t *transportV3) (err error) {
		// NOTE: to avoid an edge case where the contract is drained and can
		// therefore not be used to pay for the revision, we simply don't pay
		// for it.
		_, err = RPCLatestRevision(ctx, t, h.fcid, func(revision *types.FileContractRevision) (rhpv3.HostPriceTable, rhpv3.PaymentMethod, error) {
			// Renew contract.
			rev, txnSet, contractPrice, renewErr = RPCRenew(ctx, rrr, h.bus, t, pt, *revision, h.renterKey, h.logger)
			return rhpv3.HostPriceTable{}, nil, nil
		})
		return err
	})
	if err != nil {
		return rhpv2.ContractRevision{}, nil, contractPrice, err
	}
	return rev, txnSet, contractPrice, renewErr
}

func (h *host) FetchPriceTable(ctx context.Context, rev *types.FileContractRevision) (hpt hostdb.HostPriceTable, err error) {
	// fetchPT is a helper function that performs the RPC given a payment function
	fetchPT := func(paymentFn PriceTablePaymentFunc) (hpt hostdb.HostPriceTable, err error) {
		err = h.transportPool.withTransportV3(ctx, h.hk, h.siamuxAddr, func(ctx context.Context, t *transportV3) (err error) {
			hpt, err = RPCPriceTable(ctx, t, paymentFn)
			h.bus.RecordPriceTables(ctx, []hostdb.PriceTableUpdate{
				{
					HostKey:    h.hk,
					Success:    isSuccessfulInteraction(err),
					Timestamp:  time.Now(),
					PriceTable: hpt,
				},
			})
			return
		})
		return
	}

	// pay by contract if a revision is given
	if rev != nil {
		return fetchPT(h.preparePriceTableContractPayment(rev))
	}

	// pay by account
	return fetchPT(h.preparePriceTableAccountPayment())
}

func (h *host) FundAccount(ctx context.Context, balance types.Currency, rev *types.FileContractRevision) error {
	// fetch current balance
	curr, err := h.acc.Balance(ctx)
	if err != nil {
		return err
	}

	// return early if we have the desired balance
	if curr.Cmp(balance) >= 0 {
		return nil
	}
	deposit := balance.Sub(curr)

	return h.acc.WithDeposit(ctx, func() (types.Currency, error) {
		if err := h.transportPool.withTransportV3(ctx, h.hk, h.siamuxAddr, func(ctx context.Context, t *transportV3) error {
			// fetch pricetable
			pt, err := h.priceTable(ctx, rev)
			if err != nil {
				return err
			}

			// check whether we have money left in the contract
			if pt.FundAccountCost.Cmp(rev.ValidRenterPayout()) >= 0 {
				return fmt.Errorf("insufficient funds to fund account: %v <= %v", rev.ValidRenterPayout(), pt.FundAccountCost)
			}
			availableFunds := rev.ValidRenterPayout().Sub(pt.FundAccountCost)

			// cap the deposit amount by the money that's left in the contract
			if deposit.Cmp(availableFunds) > 0 {
				deposit = availableFunds
			}

			// create the payment
			amount := deposit.Add(pt.FundAccountCost)
			payment, err := payByContract(rev, amount, rhpv3.Account{}, h.renterKey) // no account needed for funding
			if err != nil {
				return err
			}

			// fund the account
			if err := RPCFundAccount(ctx, t, &payment, h.acc.id, pt.UID); err != nil {
				return fmt.Errorf("failed to fund account with %v (excluding cost %v);%w", deposit, pt.FundAccountCost, err)
			}

			// record the spend
			h.contractSpendingRecorder.Record(*rev, api.ContractSpending{FundAccount: amount})
			return nil
		}); err != nil {
			return types.ZeroCurrency, err
		}
		return deposit, nil
	})
}

func (h *host) SyncAccount(ctx context.Context, rev *types.FileContractRevision) error {
	// fetch pricetable
	pt, err := h.priceTable(ctx, rev)
	if err != nil {
		return err
	}

	return h.acc.WithSync(ctx, func() (types.Currency, error) {
		var balance types.Currency
		err := h.transportPool.withTransportV3(ctx, h.hk, h.siamuxAddr, func(ctx context.Context, t *transportV3) error {
			payment, err := payByContract(rev, pt.AccountBalanceCost, h.acc.id, h.renterKey)
			if err != nil {
				return err
			}
			balance, err = RPCAccountBalance(ctx, t, &payment, h.acc.id, pt.UID)
			return err
		})
		return balance, err
	})
}

// preparePriceTableAccountPayment prepare a payment function to pay for a price
// table from the given host using the provided revision.
//
// NOTE: This is the preferred way of paying for a price table since it is
// faster and doesn't require locking a contract.
func (h *host) preparePriceTableAccountPayment() PriceTablePaymentFunc {
	return func(pt rhpv3.HostPriceTable) (rhpv3.PaymentMethod, error) {
		account := rhpv3.Account(h.accountKey.PublicKey())
		payment := rhpv3.PayByEphemeralAccount(account, pt.UpdatePriceTableCost, pt.HostBlockHeight+defaultWithdrawalExpiryBlocks, h.accountKey)
		return &payment, nil
	}
}

// preparePriceTableContractPayment prepare a payment function to pay for a
// price table from the given host using the provided revision.
//
// NOTE: This way of paying for a price table should only be used if payment by
// EA is not possible or if we already need a contract revision anyway. e.g.
// funding an EA.
func (h *host) preparePriceTableContractPayment(rev *types.FileContractRevision) PriceTablePaymentFunc {
	return func(pt rhpv3.HostPriceTable) (rhpv3.PaymentMethod, error) {
		refundAccount := rhpv3.Account(h.accountKey.PublicKey())
		payment, err := payByContract(rev, pt.UpdatePriceTableCost, refundAccount, h.renterKey)
		if err != nil {
			return nil, err
		}
		return &payment, nil
	}
}
