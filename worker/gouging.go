package worker

import (
	"context"
	"errors"
	"fmt"
	"time"

	rhpv2 "go.sia.tech/core/rhp/v2"
	rhpv3 "go.sia.tech/core/rhp/v3"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
)

const (
	keyGougingChecker contextKey = "GougingChecker"

	// maxBaseRPCPriceVsBandwidth is the max ratio for sane pricing between the
	// MinBaseRPCPrice and the MinDownloadBandwidthPrice. This ensures that 1
	// million base RPC charges are at most 1% of the cost to download 4TB. This
	// ratio should be used by checking that the MinBaseRPCPrice is less than or
	// equal to the MinDownloadBandwidthPrice multiplied by this constant
	maxBaseRPCPriceVsBandwidth = uint64(40e3)

	// maxSectorAccessPriceVsBandwidth is the max ratio for sane pricing between
	// the MinSectorAccessPrice and the MinDownloadBandwidthPrice. This ensures
	// that 1 million base accesses are at most 10% of the cost to download 4TB.
	// This ratio should be used by checking that the MinSectorAccessPrice is
	// less than or equal to the MinDownloadBandwidthPrice multiplied by this
	// constant
	maxSectorAccessPriceVsBandwidth = uint64(400e3)
)

var (
	errHostSettingsGouging = errors.New("host settings gouging detected")
	errPriceTableGouging   = errors.New("price table gouging detected")
)

type (
	GougingChecker interface {
		Check(_ *rhpv2.HostSettings, _ *rhpv3.HostPriceTable) api.HostGougingBreakdown
		BlocksUntilBlockHeightGouging(hostHeight uint64) int64
	}

	gougingChecker struct {
		consensusState api.ConsensusState
		settings       api.GougingSettings
		txFee          types.Currency

		period      *uint64
		renewWindow *uint64
	}

	contextKey string
)

var _ GougingChecker = gougingChecker{}

func GougingCheckerFromContext(ctx context.Context, criticalMigration bool) (GougingChecker, error) {
	gc, ok := ctx.Value(keyGougingChecker).(func(bool) (GougingChecker, error))
	if !ok {
		panic("no gouging checker attached to the context") // developer error
	}
	return gc(criticalMigration)
}

func WithGougingChecker(ctx context.Context, cs ConsensusState, gp api.GougingParams) context.Context {
	return context.WithValue(ctx, keyGougingChecker, func(criticalMigration bool) (GougingChecker, error) {
		consensusState, err := cs.ConsensusState(ctx)
		if err != nil {
			return gougingChecker{}, fmt.Errorf("failed to get consensus state: %w", err)
		}

		// adjust the max download price if we are dealing with a critical
		// migration that might be failing due to gouging checks
		settings := gp.GougingSettings
		if criticalMigration && gp.GougingSettings.MigrationSurchargeMultiplier > 0 {
			if adjustedMaxDownloadPrice, overflow := gp.GougingSettings.MaxDownloadPrice.Mul64WithOverflow(gp.GougingSettings.MigrationSurchargeMultiplier); !overflow {
				settings.MaxDownloadPrice = adjustedMaxDownloadPrice
			}
		}

		return gougingChecker{
			consensusState: consensusState,
			settings:       settings,
			txFee:          gp.TransactionFee,

			// NOTE:
			//
			// period and renew window are nil here and that's fine, gouging
			// checkers in the workers don't have easy access to these settings and
			// thus ignore them when perform gouging checks, the autopilot however
			// does have those and will pass them when performing gouging checks
			period:      nil,
			renewWindow: nil,
		}, nil
	})
}

func NewGougingChecker(gs api.GougingSettings, cs api.ConsensusState, txnFee types.Currency, period, renewWindow uint64) GougingChecker {
	return gougingChecker{
		consensusState: cs,
		settings:       gs,
		txFee:          txnFee,

		period:      &period,
		renewWindow: &renewWindow,
	}
}

func (gc gougingChecker) BlocksUntilBlockHeightGouging(hostHeight uint64) int64 {
	blockHeight := gc.consensusState.BlockHeight
	leeway := gc.settings.HostBlockHeightLeeway
	var min uint64
	if blockHeight >= uint64(leeway) {
		min = blockHeight - uint64(leeway)
	}
	return int64(hostHeight) - int64(min)
}

func (gc gougingChecker) Check(hs *rhpv2.HostSettings, pt *rhpv3.HostPriceTable) api.HostGougingBreakdown {
	if hs == nil && pt == nil {
		panic("gouging checker needs to be provided with at least host settings or a price table") // developer error
	}

	return api.HostGougingBreakdown{
		ContractErr: errsToStr(
			checkContractGougingRHPv2(gc.period, gc.renewWindow, hs),
			checkContractGougingRHPv3(gc.period, gc.renewWindow, pt),
		),
		DownloadErr: errsToStr(checkDownloadGougingRHPv3(gc.settings, pt)),
		GougingErr: errsToStr(
			checkPriceGougingPT(gc.settings, gc.consensusState, gc.txFee, pt),
			checkPriceGougingHS(gc.settings, hs),
		),
		PruneErr:  errsToStr(checkPruneGougingRHPv2(gc.settings, hs)),
		UploadErr: errsToStr(checkUploadGougingRHPv3(gc.settings, pt)),
	}
}

func checkPriceGougingHS(gs api.GougingSettings, hs *rhpv2.HostSettings) error {
	// check if we have settings
	if hs == nil {
		return nil
	}
	// check base rpc price
	if !gs.MaxRPCPrice.IsZero() && hs.BaseRPCPrice.Cmp(gs.MaxRPCPrice) > 0 {
		return fmt.Errorf("rpc price exceeds max: %v > %v", hs.BaseRPCPrice, gs.MaxRPCPrice)
	}
	maxBaseRPCPrice := hs.DownloadBandwidthPrice.Mul64(maxBaseRPCPriceVsBandwidth)
	if hs.BaseRPCPrice.Cmp(maxBaseRPCPrice) > 0 {
		return fmt.Errorf("rpc price too high, %v > %v", hs.BaseRPCPrice, maxBaseRPCPrice)
	}

	// check sector access price
	if hs.DownloadBandwidthPrice.IsZero() {
		hs.DownloadBandwidthPrice = types.NewCurrency64(1)
	}
	maxSectorAccessPrice := hs.DownloadBandwidthPrice.Mul64(maxSectorAccessPriceVsBandwidth)
	if hs.SectorAccessPrice.Cmp(maxSectorAccessPrice) > 0 {
		return fmt.Errorf("sector access price too high, %v > %v", hs.SectorAccessPrice, maxSectorAccessPrice)
	}

	// check max storage price
	if !gs.MaxStoragePrice.IsZero() && hs.StoragePrice.Cmp(gs.MaxStoragePrice) > 0 {
		return fmt.Errorf("storage price exceeds max: %v > %v", hs.StoragePrice, gs.MaxStoragePrice)
	}

	// check contract price
	if !gs.MaxContractPrice.IsZero() && hs.ContractPrice.Cmp(gs.MaxContractPrice) > 0 {
		return fmt.Errorf("contract price exceeds max: %v > %v", hs.ContractPrice, gs.MaxContractPrice)
	}

	// check max EA balance
	if hs.MaxEphemeralAccountBalance.Cmp(gs.MinMaxEphemeralAccountBalance) < 0 {
		return fmt.Errorf("'MaxEphemeralAccountBalance' is less than the allowed minimum value, %v < %v", hs.MaxEphemeralAccountBalance, gs.MinMaxEphemeralAccountBalance)
	}

	// check EA expiry
	if hs.EphemeralAccountExpiry < gs.MinAccountExpiry {
		return fmt.Errorf("'EphemeralAccountExpiry' is less than the allowed minimum value, %v < %v", hs.EphemeralAccountExpiry, gs.MinAccountExpiry)
	}

	return nil
}

// TODO: if we ever stop assuming that certain prices in the pricetable are
// always set to 1H we should account for those fields in
// `hostPeriodCostForScore` as well.
func checkPriceGougingPT(gs api.GougingSettings, cs api.ConsensusState, txnFee types.Currency, pt *rhpv3.HostPriceTable) error {
	// check if we have a price table
	if pt == nil {
		return nil
	}
	// check base rpc price
	if !gs.MaxRPCPrice.IsZero() && gs.MaxRPCPrice.Cmp(pt.InitBaseCost) < 0 {
		return fmt.Errorf("init base cost exceeds max: %v > %v", pt.InitBaseCost, gs.MaxRPCPrice)
	}

	// check contract price
	if !gs.MaxContractPrice.IsZero() && pt.ContractPrice.Cmp(gs.MaxContractPrice) > 0 {
		return fmt.Errorf("contract price exceeds max: %v > %v", pt.ContractPrice, gs.MaxContractPrice)
	}

	// check max storage
	if !gs.MaxStoragePrice.IsZero() && pt.WriteStoreCost.Cmp(gs.MaxStoragePrice) > 0 {
		return fmt.Errorf("storage price exceeds max: %v > %v", pt.WriteStoreCost, gs.MaxStoragePrice)
	}

	// check max collateral
	if pt.MaxCollateral.IsZero() {
		return errors.New("MaxCollateral of host is 0")
	}
	// check ReadLengthCost - should be 1H as it's unused by hosts
	if types.NewCurrency64(1).Cmp(pt.ReadLengthCost) < 0 {
		return fmt.Errorf("ReadLengthCost of host is %v but should be %v", pt.ReadLengthCost, types.NewCurrency64(1))
	}

	// check WriteLengthCost - should be 1H as it's unused by hosts
	if types.NewCurrency64(1).Cmp(pt.WriteLengthCost) < 0 {
		return fmt.Errorf("WriteLengthCost of %v exceeds 1H", pt.WriteLengthCost)
	}

	// check AccountBalanceCost - should be 1H as it's unused by hosts
	if types.NewCurrency64(1).Cmp(pt.AccountBalanceCost) < 0 {
		return fmt.Errorf("AccountBalanceCost of %v exceeds 1H", pt.AccountBalanceCost)
	}

	// check FundAccountCost - should be 1H as it's unused by hosts
	if types.NewCurrency64(1).Cmp(pt.FundAccountCost) < 0 {
		return fmt.Errorf("FundAccountCost of %v exceeds 1H", pt.FundAccountCost)
	}

	// check UpdatePriceTableCost - should be 1H as it's unused by hosts
	if types.NewCurrency64(1).Cmp(pt.UpdatePriceTableCost) < 0 {
		return fmt.Errorf("UpdatePriceTableCost of %v exceeds 1H", pt.UpdatePriceTableCost)
	}

	// check HasSectorBaseCost - should be 1H as it's unused by hosts
	if types.NewCurrency64(1).Cmp(pt.HasSectorBaseCost) < 0 {
		return fmt.Errorf("HasSectorBaseCost of %v exceeds 1H", pt.HasSectorBaseCost)
	}

	// check MemoryTimeCost - should be 1H as it's unused by hosts
	if types.NewCurrency64(1).Cmp(pt.MemoryTimeCost) < 0 {
		return fmt.Errorf("MemoryTimeCost of %v exceeds 1H", pt.MemoryTimeCost)
	}

	// check DropSectorsBaseCost - should be 1H as it's unused by hosts
	if types.NewCurrency64(1).Cmp(pt.DropSectorsBaseCost) < 0 {
		return fmt.Errorf("DropSectorsBaseCost of %v exceeds 1H", pt.DropSectorsBaseCost)
	}

	// check DropSectorsUnitCost - should be 1H as it's unused by hosts
	if types.NewCurrency64(1).Cmp(pt.DropSectorsUnitCost) < 0 {
		return fmt.Errorf("DropSectorsUnitCost of %v exceeds 1H", pt.DropSectorsUnitCost)
	}

	// check SwapSectorBaseCost - should be 1H as it's unused by hosts
	if types.NewCurrency64(1).Cmp(pt.SwapSectorBaseCost) < 0 {
		return fmt.Errorf("SwapSectorBaseCost of %v exceeds 1H", pt.SwapSectorBaseCost)
	}

	// check SubscriptionMemoryCost - expect 1H default
	if types.NewCurrency64(1).Cmp(pt.SubscriptionMemoryCost) < 0 {
		return fmt.Errorf("SubscriptionMemoryCost of %v exceeds 1H", pt.SubscriptionMemoryCost)
	}

	// check SubscriptionNotificationCost - expect 1H default
	if types.NewCurrency64(1).Cmp(pt.SubscriptionNotificationCost) < 0 {
		return fmt.Errorf("SubscriptionNotificationCost of %v exceeds 1H", pt.SubscriptionNotificationCost)
	}

	// check LatestRevisionCost - expect sane value
	maxRevisionCost := gs.MaxDownloadPrice.Div64(1 << 40).Mul64(4096)
	if pt.LatestRevisionCost.Cmp(maxRevisionCost) > 0 {
		return fmt.Errorf("LatestRevisionCost of %v exceeds maximum cost of %v", pt.LatestRevisionCost, maxRevisionCost)
	}

	// check RenewContractCost - expect 100nS default
	if types.Siacoins(1).Mul64(100).Div64(1e9).Cmp(pt.RenewContractCost) < 0 {
		return fmt.Errorf("RenewContractCost of %v exceeds 100nS", pt.RenewContractCost)
	}

	// check RevisionBaseCost - expect 0H default
	if types.ZeroCurrency.Cmp(pt.RevisionBaseCost) < 0 {
		return fmt.Errorf("RevisionBaseCost of %v exceeds 0H", pt.RevisionBaseCost)
	}

	// check block height - if too much time has passed since the last block
	// there is a chance we are not up-to-date anymore. So we only check whether
	// the host's height is at least equal to ours.
	if !cs.Synced || time.Since(cs.LastBlockTime.Std()) > time.Hour {
		if pt.HostBlockHeight < cs.BlockHeight {
			return fmt.Errorf("consensus not synced and host block height is lower, %v < %v", pt.HostBlockHeight, cs.BlockHeight)
		}
	} else {
		var min uint64
		if cs.BlockHeight >= uint64(gs.HostBlockHeightLeeway) {
			min = cs.BlockHeight - uint64(gs.HostBlockHeightLeeway)
		}
		max := cs.BlockHeight + uint64(gs.HostBlockHeightLeeway)
		if !(min <= pt.HostBlockHeight && pt.HostBlockHeight <= max) {
			return fmt.Errorf("consensus is synced and host block height is not within range, %v-%v %v", min, max, pt.HostBlockHeight)
		}
	}

	// check TxnFeeMaxRecommended - expect at most a multiple of our fee
	if !txnFee.IsZero() && pt.TxnFeeMaxRecommended.Cmp(txnFee.Mul64(5)) > 0 {
		return fmt.Errorf("TxnFeeMaxRecommended %v exceeds %v", pt.TxnFeeMaxRecommended, txnFee.Mul64(5))
	}

	// check TxnFeeMinRecommended - expect it to be lower or equal than the max
	if pt.TxnFeeMinRecommended.Cmp(pt.TxnFeeMaxRecommended) > 0 {
		return fmt.Errorf("TxnFeeMinRecommended is greater than TxnFeeMaxRecommended, %v > %v", pt.TxnFeeMinRecommended, pt.TxnFeeMaxRecommended)
	}

	// check Validity
	if pt.Validity < gs.MinPriceTableValidity {
		return fmt.Errorf("'Validity' is less than the allowed minimum value, %v < %v", pt.Validity, gs.MinPriceTableValidity)
	}

	return nil
}

func checkContractGougingRHPv2(period, renewWindow *uint64, hs *rhpv2.HostSettings) (err error) {
	// period and renew window might be nil since we don't always have access to
	// these settings when performing gouging checks
	if hs == nil || period == nil || renewWindow == nil {
		return nil
	}

	err = checkContractGouging(*period, *renewWindow, hs.MaxDuration, hs.WindowSize)
	if err != nil {
		err = fmt.Errorf("%w: %v", errHostSettingsGouging, err)
	}
	return
}

func checkContractGougingRHPv3(period, renewWindow *uint64, pt *rhpv3.HostPriceTable) (err error) {
	// period and renew window might be nil since we don't always have access to
	// these settings when performing gouging checks
	if pt == nil || period == nil || renewWindow == nil {
		return nil
	}
	err = checkContractGouging(*period, *renewWindow, pt.MaxDuration, pt.WindowSize)
	if err != nil {
		err = fmt.Errorf("%w: %v", errPriceTableGouging, err)
	}
	return
}

func checkContractGouging(period, renewWindow, maxDuration, windowSize uint64) error {
	// check MaxDuration
	if period != 0 && period > maxDuration {
		return fmt.Errorf("MaxDuration %v is lower than the period %v", maxDuration, period)
	}

	// check WindowSize
	if renewWindow != 0 && renewWindow < windowSize {
		return fmt.Errorf("minimum WindowSize %v is greater than the renew window %v", windowSize, renewWindow)
	}

	return nil
}

func checkPruneGougingRHPv2(gs api.GougingSettings, hs *rhpv2.HostSettings) error {
	if hs == nil {
		return nil
	}
	// pruning costs are similar to sector read costs in a way because they
	// include base costs and download bandwidth costs, to avoid re-adding all
	// RHPv2 cost calculations we reuse download gouging checks to cover pruning
	sectorDownloadPrice, overflow := sectorReadCost(
		types.NewCurrency64(1), // 1H
		hs.SectorAccessPrice,
		hs.BaseRPCPrice,
		hs.DownloadBandwidthPrice,
		hs.UploadBandwidthPrice,
	)
	if overflow {
		return fmt.Errorf("%w: overflow detected when computing sector download price", errHostSettingsGouging)
	}
	dpptb, overflow := sectorDownloadPrice.Mul64WithOverflow(1 << 40 / rhpv2.SectorSize) // sectors per TiB
	if overflow {
		return fmt.Errorf("%w: overflow detected when computing download price per TiB", errHostSettingsGouging)
	}
	if !gs.MaxDownloadPrice.IsZero() && dpptb.Cmp(gs.MaxDownloadPrice) > 0 {
		return fmt.Errorf("%w: cost per TiB exceeds max dl price: %v > %v", errHostSettingsGouging, dpptb, gs.MaxDownloadPrice)
	}
	return nil
}

func checkDownloadGougingRHPv3(gs api.GougingSettings, pt *rhpv3.HostPriceTable) error {
	if pt == nil {
		return nil
	}
	sectorDownloadPrice, overflow := sectorReadCostRHPv3(*pt)
	if overflow {
		return fmt.Errorf("%w: overflow detected when computing sector download price", errPriceTableGouging)
	}
	dpptb, overflow := sectorDownloadPrice.Mul64WithOverflow(1 << 40 / rhpv2.SectorSize) // sectors per TiB
	if overflow {
		return fmt.Errorf("%w: overflow detected when computing download price per TiB", errPriceTableGouging)
	}
	if !gs.MaxDownloadPrice.IsZero() && dpptb.Cmp(gs.MaxDownloadPrice) > 0 {
		return fmt.Errorf("%w: cost per TiB exceeds max dl price: %v > %v", errPriceTableGouging, dpptb, gs.MaxDownloadPrice)
	}
	return nil
}

func checkUploadGougingRHPv3(gs api.GougingSettings, pt *rhpv3.HostPriceTable) error {
	if pt == nil {
		return nil
	}
	sectorUploadPricePerMonth, overflow := sectorUploadCostRHPv3(*pt)
	if overflow {
		return fmt.Errorf("%w: overflow detected when computing sector price", errPriceTableGouging)
	}
	uploadPrice, overflow := sectorUploadPricePerMonth.Mul64WithOverflow(1 << 40 / rhpv2.SectorSize) // sectors per TiB
	if overflow {
		return fmt.Errorf("%w: overflow detected when computing upload price per TiB", errPriceTableGouging)
	}
	if !gs.MaxUploadPrice.IsZero() && uploadPrice.Cmp(gs.MaxUploadPrice) > 0 {
		return fmt.Errorf("%w: cost per TiB exceeds max ul price: %v > %v", errPriceTableGouging, uploadPrice, gs.MaxUploadPrice)
	}
	return nil
}

func sectorReadCostRHPv3(pt rhpv3.HostPriceTable) (types.Currency, bool) {
	return sectorReadCost(
		pt.ReadLengthCost,
		pt.ReadBaseCost,
		pt.InitBaseCost,
		pt.UploadBandwidthCost,
		pt.DownloadBandwidthCost,
	)
}

func sectorReadCost(readLengthCost, readBaseCost, initBaseCost, ulBWCost, dlBWCost types.Currency) (types.Currency, bool) {
	// base
	base, overflow := readLengthCost.Mul64WithOverflow(rhpv2.SectorSize)
	if overflow {
		return types.ZeroCurrency, true
	}
	base, overflow = base.AddWithOverflow(readBaseCost)
	if overflow {
		return types.ZeroCurrency, true
	}
	base, overflow = base.AddWithOverflow(initBaseCost)
	if overflow {
		return types.ZeroCurrency, true
	}
	// bandwidth
	ingress, overflow := ulBWCost.Mul64WithOverflow(32)
	if overflow {
		return types.ZeroCurrency, true
	}
	egress, overflow := dlBWCost.Mul64WithOverflow(rhpv2.SectorSize)
	if overflow {
		return types.ZeroCurrency, true
	}
	// total
	total, overflow := base.AddWithOverflow(ingress)
	if overflow {
		return types.ZeroCurrency, true
	}
	total, overflow = total.AddWithOverflow(egress)
	if overflow {
		return types.ZeroCurrency, true
	}
	return total, false
}

func sectorUploadCostRHPv3(pt rhpv3.HostPriceTable) (types.Currency, bool) {
	// write
	writeCost, overflow := pt.WriteLengthCost.Mul64WithOverflow(rhpv2.SectorSize)
	if overflow {
		return types.ZeroCurrency, true
	}
	writeCost, overflow = writeCost.AddWithOverflow(pt.WriteBaseCost)
	if overflow {
		return types.ZeroCurrency, true
	}
	writeCost, overflow = writeCost.AddWithOverflow(pt.InitBaseCost)
	if overflow {
		return types.ZeroCurrency, true
	}
	// bandwidth
	ingress, overflow := pt.UploadBandwidthCost.Mul64WithOverflow(rhpv2.SectorSize)
	if overflow {
		return types.ZeroCurrency, true
	}
	// total
	total, overflow := writeCost.AddWithOverflow(ingress)
	if overflow {
		return types.ZeroCurrency, true
	}
	return total, false
}

func errsToStr(errs ...error) string {
	if err := errors.Join(errs...); err != nil {
		return err.Error()
	}
	return ""
}
