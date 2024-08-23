package rhp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net"

	rhpv2 "go.sia.tech/core/rhp/v2"
	rhpv3 "go.sia.tech/core/rhp/v3"
	"go.sia.tech/core/types"
	"go.sia.tech/mux/v1"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/internal/gouging"
	"go.sia.tech/renterd/internal/utils"
	"go.uber.org/zap"
)

const (

	// defaultRPCResponseMaxSize is the default maxSize we use whenever we read
	// an RPC response.
	defaultRPCResponseMaxSize = 100 * 1024 // 100 KiB

	// defaultWithdrawalExpiryBlocks is the number of blocks we add to the
	// current blockheight when we define an expiry block height for withdrawal
	// messages.
	defaultWithdrawalExpiryBlocks = 12

	// maxPriceTableSize defines the maximum size of a price table
	maxPriceTableSize = 16 * 1024

	// responseLeeway is the amount of leeway given to the maxLen when we read
	// the response in the ReadSector RPC
	responseLeeway = 1 << 12 // 4 KiB
)

var (
	// ErrFailedToCreatePayment is returned when the client failed to pay using a contract.
	ErrFailedToCreatePayment = errors.New("failed to create contract payment")

	// errDialTransport is returned when the worker could not dial the host.
	ErrDialTransport = errors.New("could not dial transport")

	// errBalanceInsufficient occurs when a withdrawal failed because the
	// account balance was insufficient.
	ErrBalanceInsufficient = errors.New("ephemeral account balance was insufficient")

	// ErrMaxRevisionReached occurs when trying to revise a contract that has
	// already reached the highest possible revision number. Usually happens
	// when trying to use a renewed contract.
	ErrMaxRevisionReached = errors.New("contract has reached the maximum number of revisions")

	// ErrSectorNotFound is returned by a host when it can't find the requested
	// sector.
	ErrSectorNotFound = errors.New("sector not found")

	// errHost is used to wrap rpc errors returned by the host.
	errHost = errors.New("host responded with error")

	// errTransport is used to wrap rpc errors caused by the transport.
	errTransport = errors.New("transport error")

	// errBalanceMaxExceeded occurs when a deposit would push the account's
	// balance over the maximum allowed ephemeral account balance.
	errBalanceMaxExceeded = errors.New("ephemeral account maximum balance exceeded")

	// errInsufficientFunds is returned by various RPCs when the renter is
	// unable to provide sufficient payment to the host.
	errInsufficientFunds = errors.New("insufficient funds")

	// errPriceTableExpired is returned by the host when the price table that
	// corresponds to the id it was given is already expired and thus no longer
	// valid.
	errPriceTableExpired = errors.New("price table requested is expired")

	// errPriceTableNotFound is returned by the host when it can not find a
	// price table that corresponds with the id we sent it.
	errPriceTableNotFound = errors.New("price table not found")

	// errSectorNotFound is returned by the host when it can not find the
	// requested sector.
	errSectorNotFoundOld = errors.New("could not find the desired sector")

	// errWithdrawalsInactive occurs when the host is (perhaps temporarily)
	// unsynced and has disabled its account manager.
	errWithdrawalsInactive = errors.New("ephemeral account withdrawals are inactive because the host is not synced")

	// errWithdrawalExpired is returned by the host when the withdrawal request
	// has an expiry block height that is in the past.
	errWithdrawalExpired = errors.New("withdrawal request expired")
)

// IsErrHost indicates whether an error was returned by a host as part of an RPC.
func IsErrHost(err error) bool {
	return utils.IsErr(err, errHost)
}

func IsBalanceInsufficient(err error) bool { return utils.IsErr(err, ErrBalanceInsufficient) }
func IsBalanceMaxExceeded(err error) bool  { return utils.IsErr(err, errBalanceMaxExceeded) }
func IsClosedStream(err error) bool {
	return utils.IsErr(err, mux.ErrClosedStream) || utils.IsErr(err, net.ErrClosed)
}
func IsInsufficientFunds(err error) bool  { return utils.IsErr(err, errInsufficientFunds) }
func IsPriceTableExpired(err error) bool  { return utils.IsErr(err, errPriceTableExpired) }
func IsPriceTableGouging(err error) bool  { return utils.IsErr(err, gouging.ErrPriceTableGouging) }
func IsPriceTableNotFound(err error) bool { return utils.IsErr(err, errPriceTableNotFound) }
func IsSectorNotFound(err error) bool {
	return utils.IsErr(err, ErrSectorNotFound) || utils.IsErr(err, errSectorNotFoundOld)
}
func IsWithdrawalsInactive(err error) bool { return utils.IsErr(err, errWithdrawalsInactive) }
func IsWithdrawalExpired(err error) bool   { return utils.IsErr(err, errWithdrawalExpired) }

type (
	Dialer interface {
		Dial(ctx context.Context, hk types.PublicKey, address string) (net.Conn, error)
	}
)

type Client struct {
	logger *zap.SugaredLogger
	tpool  *transportPoolV3
}

func New(dialer Dialer, logger *zap.Logger) *Client {
	return &Client{
		logger: logger.Sugar().Named("rhp3"),
		tpool:  newTransportPoolV3(dialer),
	}
}

func (c *Client) AppendSector(ctx context.Context, sectorRoot types.Hash256, sector *[rhpv2.SectorSize]byte, rev *types.FileContractRevision, hk types.PublicKey, siamuxAddr string, accID rhpv3.Account, pt rhpv3.HostPriceTable, rk types.PrivateKey) (types.Currency, error) {
	expectedCost, _, _, err := uploadSectorCost(pt, rev.WindowEnd)
	if err != nil {
		return types.ZeroCurrency, err
	}
	payment, err := payByContract(rev, expectedCost, accID, rk)
	if err != nil {
		return types.ZeroCurrency, ErrFailedToCreatePayment
	}

	var cost types.Currency
	err = c.tpool.withTransport(ctx, hk, siamuxAddr, func(ctx context.Context, t *transportV3) error {
		cost, err = rpcAppendSector(ctx, t, rk, pt, rev, &payment, sectorRoot, sector)
		return err
	})
	return cost, err
}

func (c *Client) FundAccount(ctx context.Context, rev *types.FileContractRevision, hk types.PublicKey, siamuxAddr string, amount types.Currency, accID rhpv3.Account, pt rhpv3.HostPriceTable, renterKey types.PrivateKey) error {
	ppcr, err := payByContract(rev, amount.Add(types.NewCurrency64(1)), rhpv3.ZeroAccount, renterKey)
	if err != nil {
		return fmt.Errorf("failed to create payment: %w", err)
	}
	return c.tpool.withTransport(ctx, hk, siamuxAddr, func(ctx context.Context, t *transportV3) error {
		return rpcFundAccount(ctx, t, &ppcr, accID, pt.UID)
	})
}

func (c *Client) Renew(ctx context.Context, rrr api.ContractRenewRequest, gougingChecker gouging.Checker, renewer PrepareRenewFunc, signer SignFunc, rev types.FileContractRevision, renterKey types.PrivateKey) (newRev rhpv2.ContractRevision, txnSet []types.Transaction, contractPrice, fundAmount types.Currency, err error) {
	err = c.tpool.withTransport(ctx, rrr.HostKey, rrr.SiamuxAddr, func(ctx context.Context, t *transportV3) error {
		newRev, txnSet, contractPrice, fundAmount, err = rpcRenew(ctx, rrr, gougingChecker, renewer, signer, t, rev, renterKey)
		return err
	})
	return
}

func (c *Client) SyncAccount(ctx context.Context, rev *types.FileContractRevision, hk types.PublicKey, siamuxAddr string, accID rhpv3.Account, pt rhpv3.SettingsID, rk types.PrivateKey) (balance types.Currency, _ error) {
	return balance, c.tpool.withTransport(ctx, hk, siamuxAddr, func(ctx context.Context, t *transportV3) error {
		payment, err := payByContract(rev, types.NewCurrency64(1), accID, rk)
		if err != nil {
			return err
		}
		balance, err = rpcAccountBalance(ctx, t, &payment, accID, pt)
		return err
	})
}

func (c *Client) PriceTable(ctx context.Context, hk types.PublicKey, siamuxAddr string, paymentFn PriceTablePaymentFunc) (pt api.HostPriceTable, err error) {
	err = c.tpool.withTransport(ctx, hk, siamuxAddr, func(ctx context.Context, t *transportV3) error {
		pt, err = rpcPriceTable(ctx, t, paymentFn)
		return err
	})
	return
}

func (c *Client) PriceTableUnpaid(ctx context.Context, hk types.PublicKey, siamuxAddr string) (pt api.HostPriceTable, err error) {
	err = c.tpool.withTransport(ctx, hk, siamuxAddr, func(ctx context.Context, t *transportV3) error {
		pt, err = rpcPriceTable(ctx, t, func(pt rhpv3.HostPriceTable) (rhpv3.PaymentMethod, error) { return nil, nil })
		if err != nil {
			return fmt.Errorf("failed to fetch host price table: %w", err)
		}
		return err
	})
	return
}

func (c *Client) ReadSector(ctx context.Context, offset, length uint32, root types.Hash256, w io.Writer, hk types.PublicKey, siamuxAddr string, accID rhpv3.Account, accKey types.PrivateKey, pt rhpv3.HostPriceTable) (types.Currency, error) {
	var amount types.Currency
	err := c.tpool.withTransport(ctx, hk, siamuxAddr, func(ctx context.Context, t *transportV3) error {
		cost, err := readSectorCost(pt, uint64(length))
		if err != nil {
			return err
		}

		amount = cost // pessimistic cost estimate in case rpc fails
		payment := rhpv3.PayByEphemeralAccount(accID, cost, pt.HostBlockHeight+defaultWithdrawalExpiryBlocks, accKey)
		cost, refund, err := rpcReadSector(ctx, t, w, pt, &payment, offset, length, root)
		if err != nil {
			return err
		}

		amount = cost.Sub(refund)
		return nil
	})
	return amount, err
}

func (c *Client) Revision(ctx context.Context, fcid types.FileContractID, hk types.PublicKey, siamuxAddr string) (rev types.FileContractRevision, err error) {
	return rev, c.tpool.withTransport(ctx, hk, siamuxAddr, func(ctx context.Context, t *transportV3) error {
		rev, err = rpcLatestRevision(ctx, t, fcid)
		return err
	})
}

// PreparePriceTableAccountPayment prepare a payment function to pay for a price
// table from the given host using the provided revision.
//
// NOTE: This is the preferred way of paying for a price table since it is
// faster and doesn't require locking a contract.
func PreparePriceTableAccountPayment(accKey types.PrivateKey) PriceTablePaymentFunc {
	return func(pt rhpv3.HostPriceTable) (rhpv3.PaymentMethod, error) {
		accID := rhpv3.Account(accKey.PublicKey())
		payment := rhpv3.PayByEphemeralAccount(accID, pt.UpdatePriceTableCost, pt.HostBlockHeight+defaultWithdrawalExpiryBlocks, accKey)
		return &payment, nil
	}
}

// PreparePriceTableContractPayment prepare a payment function to pay for a
// price table from the given host using the provided revision.
//
// NOTE: This way of paying for a price table should only be used if payment by
// EA is not possible or if we already need a contract revision anyway. e.g.
// funding an EA.
func PreparePriceTableContractPayment(rev *types.FileContractRevision, refundAccID rhpv3.Account, renterKey types.PrivateKey) PriceTablePaymentFunc {
	return func(pt rhpv3.HostPriceTable) (rhpv3.PaymentMethod, error) {
		payment, err := payByContract(rev, pt.UpdatePriceTableCost, refundAccID, renterKey)
		if err != nil {
			return nil, err
		}
		return &payment, nil
	}
}

// padBandwitdh pads the bandwidth to the next multiple of 1460 bytes.  1460
// bytes is the maximum size of a TCP packet when using IPv4.
// TODO: once hostd becomes the only host implementation we can simplify this.
func padBandwidth(pt rhpv3.HostPriceTable, rc rhpv3.ResourceCost) rhpv3.ResourceCost {
	padCost := func(cost, paddingSize types.Currency) types.Currency {
		if paddingSize.IsZero() {
			return cost // might happen if bandwidth is free
		}
		return cost.Add(paddingSize).Sub(types.NewCurrency64(1)).Div(paddingSize).Mul(paddingSize)
	}
	minPacketSize := uint64(1460)
	minIngress := pt.UploadBandwidthCost.Mul64(minPacketSize)
	minEgress := pt.DownloadBandwidthCost.Mul64(3*minPacketSize + responseLeeway)
	rc.Ingress = padCost(rc.Ingress, minIngress)
	rc.Egress = padCost(rc.Egress, minEgress)
	return rc
}

// readSectorCost returns an overestimate for the cost of reading a sector from a host
func readSectorCost(pt rhpv3.HostPriceTable, length uint64) (types.Currency, error) {
	rc := pt.BaseCost()
	rc = rc.Add(pt.ReadSectorCost(length))
	rc = padBandwidth(pt, rc)
	cost, _ := rc.Total()

	// overestimate the cost by 10%
	cost, overflow := cost.Mul64WithOverflow(11)
	if overflow {
		return types.ZeroCurrency, errors.New("overflow occurred while adding leeway to read sector cost")
	}
	return cost.Div64(10), nil
}

// uploadSectorCost returns an overestimate for the cost of uploading a sector
// to a host
func uploadSectorCost(pt rhpv3.HostPriceTable, windowEnd uint64) (cost, collateral, storage types.Currency, _ error) {
	rc := pt.BaseCost()
	rc = rc.Add(pt.AppendSectorCost(windowEnd - pt.HostBlockHeight))
	rc = padBandwidth(pt, rc)
	cost, collateral = rc.Total()

	// overestimate the cost by 10%
	cost, overflow := cost.Mul64WithOverflow(11)
	if overflow {
		return types.ZeroCurrency, types.ZeroCurrency, types.ZeroCurrency, errors.New("overflow occurred while adding leeway to read sector cost")
	}
	return cost.Div64(10), collateral, rc.Storage, nil
}

func processPayment(s *streamV3, payment rhpv3.PaymentMethod) error {
	var paymentType types.Specifier
	switch payment.(type) {
	case *rhpv3.PayByContractRequest:
		paymentType = rhpv3.PaymentTypeContract
	case *rhpv3.PayByEphemeralAccountRequest:
		paymentType = rhpv3.PaymentTypeEphemeralAccount
	default:
		panic("unhandled payment method")
	}
	if err := s.WriteResponse(&paymentType); err != nil {
		return err
	} else if err := s.WriteResponse(payment); err != nil {
		return err
	}
	if _, ok := payment.(*rhpv3.PayByContractRequest); ok {
		var pr rhpv3.PaymentResponse
		if err := s.ReadResponse(&pr, defaultRPCResponseMaxSize); err != nil {
			return err
		}
		// TODO: return host signature
	}
	return nil
}

func hashRevision(rev types.FileContractRevision) types.Hash256 {
	h := types.NewHasher()
	rev.EncodeTo(h.E)
	return h.Sum()
}

// initialRevision returns the first revision of a file contract formation
// transaction.
func initialRevision(formationTxn types.Transaction, hostPubKey, renterPubKey types.UnlockKey) types.FileContractRevision {
	fc := formationTxn.FileContracts[0]
	return types.FileContractRevision{
		ParentID: formationTxn.FileContractID(0),
		UnlockConditions: types.UnlockConditions{
			PublicKeys:         []types.UnlockKey{renterPubKey, hostPubKey},
			SignaturesRequired: 2,
		},
		FileContract: types.FileContract{
			Filesize:           fc.Filesize,
			FileMerkleRoot:     fc.FileMerkleRoot,
			WindowStart:        fc.WindowStart,
			WindowEnd:          fc.WindowEnd,
			ValidProofOutputs:  fc.ValidProofOutputs,
			MissedProofOutputs: fc.MissedProofOutputs,
			UnlockHash:         fc.UnlockHash,
			RevisionNumber:     1,
		},
	}
}

func payByContract(rev *types.FileContractRevision, amount types.Currency, refundAcct rhpv3.Account, sk types.PrivateKey) (rhpv3.PayByContractRequest, error) {
	if rev.RevisionNumber == math.MaxUint64 {
		return rhpv3.PayByContractRequest{}, ErrMaxRevisionReached
	}
	payment, ok := rhpv3.PayByContract(rev, amount, refundAcct, sk)
	if !ok {
		return rhpv3.PayByContractRequest{}, errInsufficientFunds
	}
	return payment, nil
}

func updateRevisionOutputs(rev *types.FileContractRevision, cost, collateral types.Currency) (valid, missed []types.Currency, err error) {
	// allocate new slices; don't want to risk accidentally sharing memory
	rev.ValidProofOutputs = append([]types.SiacoinOutput(nil), rev.ValidProofOutputs...)
	rev.MissedProofOutputs = append([]types.SiacoinOutput(nil), rev.MissedProofOutputs...)

	// move valid payout from renter to host
	var underflow, overflow bool
	rev.ValidProofOutputs[0].Value, underflow = rev.ValidProofOutputs[0].Value.SubWithUnderflow(cost)
	rev.ValidProofOutputs[1].Value, overflow = rev.ValidProofOutputs[1].Value.AddWithOverflow(cost)
	if underflow || overflow {
		err = errors.New("insufficient funds to pay host")
		return
	}

	// move missed payout from renter to void
	rev.MissedProofOutputs[0].Value, underflow = rev.MissedProofOutputs[0].Value.SubWithUnderflow(cost)
	rev.MissedProofOutputs[2].Value, overflow = rev.MissedProofOutputs[2].Value.AddWithOverflow(cost)
	if underflow || overflow {
		err = errors.New("insufficient funds to move missed payout to void")
		return
	}

	// move collateral from host to void
	rev.MissedProofOutputs[1].Value, underflow = rev.MissedProofOutputs[1].Value.SubWithUnderflow(collateral)
	rev.MissedProofOutputs[2].Value, overflow = rev.MissedProofOutputs[2].Value.AddWithOverflow(collateral)
	if underflow || overflow {
		err = errors.New("insufficient collateral")
		return
	}

	return []types.Currency{rev.ValidProofOutputs[0].Value, rev.ValidProofOutputs[1].Value},
		[]types.Currency{rev.MissedProofOutputs[0].Value, rev.MissedProofOutputs[1].Value, rev.MissedProofOutputs[2].Value}, nil
}
