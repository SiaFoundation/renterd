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
		DownloadSector(ctx context.Context, w io.Writer, root types.Hash256, offset, length uint32, overpay bool) error
		FetchPriceTable(ctx context.Context, rev *types.FileContractRevision) (hpt hostdb.HostPriceTable, err error)
		FetchRevision(ctx context.Context, fetchTimeout time.Duration, blockHeight uint64) (types.FileContractRevision, error)
		FundAccount(ctx context.Context, balance types.Currency, rev *types.FileContractRevision) error
		RenewContract(ctx context.Context, rrr api.RHPRenewRequest) (_ rhpv2.ContractRevision, _ []types.Transaction, _ types.Currency, err error)
		SyncAccount(ctx context.Context, rev *types.FileContractRevision) error
		UploadSector(ctx context.Context, sector *[rhpv2.SectorSize]byte, rev types.FileContractRevision) (types.Hash256, error)
	}

	HostManager interface {
		Host(types.PublicKey, types.FileContractID, string) Host
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
		contractSpendingRecorder *contractSpendingRecorder
		logger                   *zap.SugaredLogger
		transportPool            *transportPoolV3
		priceTables              *priceTables
	}
)

var _ Host = (*host)(nil)

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

// UploadSector uploads a sector to the host.
func (h *host) UploadSector(ctx context.Context, sector *[rhpv2.SectorSize]byte, rev types.FileContractRevision) (root types.Hash256, err error) {
	// fetch price table
	pt, err := h.priceTable(ctx, nil)
	if err != nil {
		return types.Hash256{}, err
	}

	// prepare payment
	//
	// TODO: change to account payments once we have the means to check for an
	// insufficient balance error
	expectedCost, _, _, err := uploadSectorCost(pt, rev.WindowEnd)
	if err != nil {
		return types.Hash256{}, err
	}
	if rev.RevisionNumber == math.MaxUint64 {
		return types.Hash256{}, fmt.Errorf("revision number has reached max, fcid %v", rev.ParentID)
	}
	payment, ok := rhpv3.PayByContract(&rev, expectedCost, h.acc.id, h.renterKey)
	if !ok {
		return types.Hash256{}, errors.New("failed to create payment")
	}

	var cost types.Currency
	err = h.transportPool.withTransportV3(ctx, h.hk, h.siamuxAddr, func(ctx context.Context, t *transportV3) error {
		root, cost, err = RPCAppendSector(ctx, t, h.renterKey, pt, &rev, &payment, sector)
		return err
	})
	if err != nil {
		return types.Hash256{}, err
	}

	// record spending
	h.contractSpendingRecorder.Record(rev, api.ContractSpending{Uploads: cost})
	return root, nil
}

// Renew renews a contract with a host. To avoid an edge case where the contract
// is drained and can therefore not be used to pay for the revision, we simply
// don't pay for it.
func (h *host) RenewContract(ctx context.Context, rrr api.RHPRenewRequest) (_ rhpv2.ContractRevision, _ []types.Transaction, _ types.Currency, err error) {
	// Try to get a valid pricetable.
	ptCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	var pt *rhpv3.HostPriceTable
	hpt, err := h.priceTables.fetch(ptCtx, h.hk, nil)
	if err == nil {
		pt = &hpt.HostPriceTable
	} else {
		h.logger.Debugf("unable to fetch price table for renew: %v", err)
	}

	var contractPrice types.Currency
	var rev rhpv2.ContractRevision
	var txnSet []types.Transaction
	var renewErr error
	err = h.transportPool.withTransportV3(ctx, h.hk, h.siamuxAddr, func(ctx context.Context, t *transportV3) (err error) {
		_, err = RPCLatestRevision(ctx, t, h.fcid, func(revision *types.FileContractRevision) (rhpv3.HostPriceTable, rhpv3.PaymentMethod, error) {
			// Renew contract.
			contractPrice = pt.ContractPrice
			rev, txnSet, renewErr = RPCRenew(ctx, rrr, h.bus, t, pt, *revision, h.renterKey, h.logger)
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
			InteractionRecorderFromContext(ctx).RecordPriceTableUpdate(hostdb.PriceTableUpdate{
				HostKey:    h.hk,
				Success:    isSuccessfulInteraction(err),
				Timestamp:  time.Now(),
				PriceTable: hpt,
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
	cs, err := h.bus.ConsensusState(ctx)
	if err != nil {
		return hostdb.HostPriceTable{}, err
	}
	return fetchPT(h.preparePriceTableAccountPayment(cs.BlockHeight))
}
