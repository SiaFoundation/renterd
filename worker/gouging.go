package worker

import (
	"context"
	"errors"
	"fmt"
	"math/bits"

	rhpv2 "go.sia.tech/core/rhp/v2"
	rhpv3 "go.sia.tech/core/rhp/v3"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
)

const (
	keyGougingChecker contextKey = "GougingChecker"
)

type (
	GougingChecker interface {
		Check(*rhpv2.HostSettings, *rhpv3.HostPriceTable) api.HostGougingBreakdown
	}

	gougingChecker struct {
		consensusState api.ConsensusState
		settings       api.GougingSettings
		redundancy     api.RedundancySettings
		txFee          types.Currency

		period      *uint64
		renewWindow *uint64
	}

	contextKey string
)

var _ GougingChecker = gougingChecker{}

func GougingCheckerFromContext(ctx context.Context) GougingChecker {
	gc, ok := ctx.Value(keyGougingChecker).(GougingChecker)
	if !ok {
		panic("no gouging checker attached to the context") // developer error
	}
	return gc
}

func WithGougingChecker(ctx context.Context, gp api.GougingParams) context.Context {
	return context.WithValue(ctx, keyGougingChecker, gougingChecker{
		consensusState: gp.ConsensusState,
		settings:       gp.GougingSettings,
		redundancy:     gp.RedundancySettings,
		txFee:          gp.TransactionFee,

		// NOTE:
		//
		// period and renew window are nil here and that's fine, gouging
		// checkers in the workers don't have easy access to these settings and
		// thus ignore them when perform gouging checks, the autopilot however
		// does have those and will pass them when performing gouging checks
		period:      nil,
		renewWindow: nil,
	})
}

func NewGougingChecker(gs api.GougingSettings, rs api.RedundancySettings, cs api.ConsensusState, txnFee types.Currency, period, renewWindow uint64) GougingChecker {
	return gougingChecker{
		consensusState: cs,
		settings:       gs,
		redundancy:     rs,
		txFee:          txnFee,

		period:      &period,
		renewWindow: &renewWindow,
	}
}

func (gc gougingChecker) Check(hs *rhpv2.HostSettings, pt *rhpv3.HostPriceTable) (breakdown api.HostGougingBreakdown) {
	if hs == nil && pt == nil {
		panic("gouging checker needs to be provided with at least host settings or a price table") // developer error
	}

	breakdown.V2 = gc.checkHS(hs)
	breakdown.V3 = gc.checkPT(pt)
	return
}

func (gc gougingChecker) checkHS(hs *rhpv2.HostSettings) (check api.GougingChecks) {
	if hs != nil {
		check = api.GougingChecks{
			ContractErr: checkContractGougingRHPv2(gc.period, gc.renewWindow, *hs),
			DownloadErr: checkDownloadGougingRHPv2(gc.settings, gc.redundancy, *hs),
			GougingErr:  checkPriceGougingHS(gc.settings, *hs),
			UploadErr:   checkUploadGougingRHPv2(gc.settings, gc.redundancy, *hs),
		}
	}
	return
}

func (gc gougingChecker) checkPT(pt *rhpv3.HostPriceTable) (check api.GougingChecks) {
	if pt != nil {
		check = api.GougingChecks{
			ContractErr: checkContractGougingRHPv3(gc.period, gc.renewWindow, *pt),
			DownloadErr: checkDownloadGougingRHPv3(gc.settings, gc.redundancy, *pt),
			GougingErr:  checkPriceGougingPT(gc.settings, gc.consensusState, gc.txFee, *pt),
			UploadErr:   checkUploadGougingRHPv3(gc.settings, gc.redundancy, *pt),
		}
	}
	return
}

func checkPriceGougingHS(gs api.GougingSettings, hs rhpv2.HostSettings) error {
	// check base rpc price
	if !gs.MaxRPCPrice.IsZero() && hs.BaseRPCPrice.Cmp(gs.MaxRPCPrice) > 0 {
		return fmt.Errorf("rpc price exceeds max: %v>%v", hs.BaseRPCPrice, gs.MaxRPCPrice)
	}

	// check max storage price
	if !gs.MaxStoragePrice.IsZero() && hs.StoragePrice.Cmp(gs.MaxStoragePrice) > 0 {
		return fmt.Errorf("storage price exceeds max: %v>%v", hs.StoragePrice, gs.MaxStoragePrice)
	}

	// check contract price
	if !gs.MaxContractPrice.IsZero() && hs.ContractPrice.Cmp(gs.MaxContractPrice) > 0 {
		return fmt.Errorf("contract price exceeds max: %v>%v", hs.ContractPrice, gs.MaxContractPrice)
	}

	// check max collateral
	if hs.MaxCollateral.IsZero() {
		return errors.New("MaxCollateral of host is 0")
	}
	if hs.MaxCollateral.Cmp(gs.MinMaxCollateral) < 0 {
		return fmt.Errorf("MaxCollateral is below minimum: %v<%v", hs.MaxCollateral, gs.MinMaxCollateral)
	}

	return nil
}

// TODO: if we ever stop assuming that certain prices in the pricetable are
// always set to 1H we should account for those fields in
// `hostPeriodCostForScore` as well.
func checkPriceGougingPT(gs api.GougingSettings, cs api.ConsensusState, txnFee types.Currency, pt rhpv3.HostPriceTable) error {
	// check base rpc price
	if !gs.MaxRPCPrice.IsZero() && gs.MaxRPCPrice.Cmp(pt.InitBaseCost) < 0 {
		return fmt.Errorf("init base cost exceeds max: %v>%v", pt.InitBaseCost, gs.MaxRPCPrice)
	}

	// check contract price
	if !gs.MaxContractPrice.IsZero() && pt.ContractPrice.Cmp(gs.MaxContractPrice) > 0 {
		return fmt.Errorf("contract price exceeds max: %v>%v", pt.ContractPrice, gs.MaxContractPrice)
	}

	// check max storage
	if !gs.MaxStoragePrice.IsZero() && pt.WriteStoreCost.Cmp(gs.MaxStoragePrice) > 0 {
		return fmt.Errorf("storage price exceeds max: %v>%v", pt.WriteStoreCost, gs.MaxStoragePrice)
	}

	// check max collateral
	if pt.MaxCollateral.IsZero() {
		return errors.New("MaxCollateral of host is 0")
	}
	if pt.MaxCollateral.Cmp(gs.MinMaxCollateral) < 0 {
		return fmt.Errorf("MaxCollateral is below minimum: %v<%v", pt.MaxCollateral, gs.MinMaxCollateral)
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

	// check block height
	if !cs.Synced {
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
		return fmt.Errorf("TxnFeeMinRecommended is greater than TxnFeeMaxRecommended, %v>%v", pt.TxnFeeMinRecommended, pt.TxnFeeMaxRecommended)
	}

	return nil
}

func checkContractGougingRHPv2(period, renewWindow *uint64, hs rhpv2.HostSettings) error {
	// period and renew window might be nil since we don't always have access to
	// these settings when performing gouging checks
	if period == nil || renewWindow == nil {
		return nil
	}
	return checkContractGouging(*period, *renewWindow, hs.MaxDuration, hs.WindowSize)
}

func checkContractGougingRHPv3(period, renewWindow *uint64, pt rhpv3.HostPriceTable) error {
	// period and renew window might be nil since we don't always have access to
	// these settings when performing gouging checks
	if period == nil || renewWindow == nil {
		return nil
	}
	return checkContractGouging(*period, *renewWindow, pt.MaxDuration, pt.WindowSize)
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

func checkDownloadGougingRHPv2(gs api.GougingSettings, rs api.RedundancySettings, hs rhpv2.HostSettings) error {
	sectorDownloadPrice, overflow := sectorReadCostRHPv2(hs)
	if overflow {
		return fmt.Errorf("overflow detected when computing sector download price")
	}
	return checkDownloadGouging(gs, rs, sectorDownloadPrice)
}

func checkDownloadGougingRHPv3(gs api.GougingSettings, rs api.RedundancySettings, pt rhpv3.HostPriceTable) error {
	sectorDownloadPrice, overflow := sectorReadCostRHPv3(pt)
	if overflow {
		return fmt.Errorf("overflow detected when computing sector download price")
	}
	return checkDownloadGouging(gs, rs, sectorDownloadPrice)
}

func checkDownloadGouging(gs api.GougingSettings, rs api.RedundancySettings, sectorDownloadPrice types.Currency) error {
	dpptb, overflow := sectorDownloadPrice.Mul64WithOverflow(1 << 40 / rhpv2.SectorSize) // sectors per TiB
	if overflow {
		return fmt.Errorf("overflow detected when computing download price per TiB")
	}
	downloadPriceTotalShards, overflow := dpptb.Mul64WithOverflow(uint64(rs.TotalShards))
	if overflow {
		return fmt.Errorf("overflow detected when multiplying %v * %v in download gouging", dpptb, rs.TotalShards)
	}
	downloadPrice := downloadPriceTotalShards.Div64(uint64(rs.MinShards))
	if !gs.MaxDownloadPrice.IsZero() && downloadPrice.Cmp(gs.MaxDownloadPrice) > 0 {
		return fmt.Errorf("cost per TiB exceeds max dl price: %v>%v", downloadPrice, gs.MaxDownloadPrice)
	}
	return nil
}

func checkUploadGougingRHPv2(gs api.GougingSettings, rs api.RedundancySettings, hs rhpv2.HostSettings) error {
	sectorUploadPricePerMonth, overflow := sectorUploadCostPerMonthRHPv2(hs)
	if overflow {
		return fmt.Errorf("overflow detected when computing sector price")
	}
	return checkUploadGouging(gs, rs, sectorUploadPricePerMonth)
}

func checkUploadGougingRHPv3(gs api.GougingSettings, rs api.RedundancySettings, pt rhpv3.HostPriceTable) error {
	sectorUploadPricePerMonth, overflow := sectorUploadCostPerMonthRHPv3(pt)
	if overflow {
		return fmt.Errorf("overflow detected when computing sector price")
	}
	return checkUploadGouging(gs, rs, sectorUploadPricePerMonth)
}

func checkUploadGouging(gs api.GougingSettings, rs api.RedundancySettings, sectorUploadPricePerMonth types.Currency) error {
	upptb, overflow := sectorUploadPricePerMonth.Mul64WithOverflow(1 << 40 / rhpv2.SectorSize) // sectors per TiB
	if overflow {
		return fmt.Errorf("overflow detected when computing upload price per TiB")
	}
	uploadPriceTotalShards, overflow := upptb.Mul64WithOverflow(uint64(rs.TotalShards))
	if overflow {
		return fmt.Errorf("overflow detected when multiplying %v * %v in upload gouging", upptb, rs.TotalShards)
	}
	uploadPrice := uploadPriceTotalShards.Div64(uint64(rs.MinShards))
	if !gs.MaxUploadPrice.IsZero() && uploadPrice.Cmp(gs.MaxUploadPrice) > 0 {
		return fmt.Errorf("cost per TiB exceeds max ul price: %v>%v", uploadPrice, gs.MaxUploadPrice)
	}
	return nil
}

func sectorReadCostRHPv2(settings rhpv2.HostSettings) (types.Currency, bool) {
	bandwidth := rhpv2.SectorSize + 2*uint64(bits.Len64(rhpv2.SectorSize/rhpv2.LeavesPerSector))*32
	bandwidthPrice, overflow := settings.DownloadBandwidthPrice.Mul64WithOverflow(bandwidth)
	if overflow {
		return types.ZeroCurrency, true
	}
	total, overflow := settings.BaseRPCPrice.AddWithOverflow(settings.SectorAccessPrice)
	if overflow {
		return types.ZeroCurrency, true
	}
	total, overflow = total.AddWithOverflow(bandwidthPrice)
	if overflow {
		return types.ZeroCurrency, true
	}
	return total, false
}

func sectorUploadCostPerMonthRHPv2(settings rhpv2.HostSettings) (types.Currency, bool) {
	// base
	base := settings.BaseRPCPrice
	// storage
	storage, overflow := settings.StoragePrice.Mul64WithOverflow(rhpv2.SectorSize)
	if overflow {
		return types.ZeroCurrency, true
	}
	storage, overflow = storage.Mul64WithOverflow(4032)
	if overflow {
		return types.ZeroCurrency, true
	}
	// bandwidth
	upload, overflow := settings.UploadBandwidthPrice.Mul64WithOverflow(rhpv2.SectorSize)
	if overflow {
		return types.ZeroCurrency, true
	}
	download, overflow := settings.DownloadBandwidthPrice.Mul64WithOverflow(128 * 32) // proof
	if overflow {
		return types.ZeroCurrency, true
	}
	// total
	total, overflow := base.AddWithOverflow(storage)
	if overflow {
		return types.ZeroCurrency, true
	}
	total, overflow = total.AddWithOverflow(upload)
	if overflow {
		return types.ZeroCurrency, true
	}
	total, overflow = total.AddWithOverflow(download)
	if overflow {
		return types.ZeroCurrency, true
	}
	return total, false
}

func sectorReadCostRHPv3(pt rhpv3.HostPriceTable) (types.Currency, bool) {
	// base
	base, overflow := pt.ReadLengthCost.Mul64WithOverflow(rhpv2.SectorSize)
	if overflow {
		return types.ZeroCurrency, true
	}
	base, overflow = base.AddWithOverflow(pt.ReadBaseCost)
	if overflow {
		return types.ZeroCurrency, true
	}
	// bandwidth
	ingress, overflow := pt.UploadBandwidthCost.Mul64WithOverflow(32)
	if overflow {
		return types.ZeroCurrency, true
	}
	egress, overflow := pt.DownloadBandwidthCost.Mul64WithOverflow(rhpv2.SectorSize)
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

func sectorUploadCostPerMonthRHPv3(pt rhpv3.HostPriceTable) (types.Currency, bool) {
	// write
	writeCost, overflow := pt.WriteLengthCost.Mul64WithOverflow(rhpv2.SectorSize)
	if overflow {
		return types.ZeroCurrency, true
	}
	writeCost, overflow = writeCost.AddWithOverflow(pt.WriteBaseCost)
	if overflow {
		return types.ZeroCurrency, true
	}
	// storage
	storage, overflow := pt.WriteStoreCost.Mul64WithOverflow(rhpv2.SectorSize)
	if overflow {
		return types.ZeroCurrency, true
	}
	storage, overflow = storage.Mul64WithOverflow(4032)
	if overflow {
		return types.ZeroCurrency, true
	}
	// bandwidth
	ingress, overflow := pt.UploadBandwidthCost.Mul64WithOverflow(rhpv2.SectorSize)
	if overflow {
		return types.ZeroCurrency, true
	}
	// total
	total, overflow := writeCost.AddWithOverflow(storage)
	if overflow {
		return types.ZeroCurrency, true
	}
	total, overflow = total.AddWithOverflow(ingress)
	if overflow {
		return types.ZeroCurrency, true
	}
	return total, false
}
