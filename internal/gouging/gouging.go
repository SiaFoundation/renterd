package gouging

import (
	"context"
	"errors"
	"fmt"
	"time"

	rhpv4 "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/v2/api"
	"go.sia.tech/renterd/v2/internal/rhp/v4"
)

const (
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
	ErrHostSettingsGouging = errors.New("host settings gouging detected")
	ErrPriceTableGouging   = errors.New("price table gouging detected")
)

type (
	ConsensusState interface {
		ConsensusState(ctx context.Context) (api.ConsensusState, error)
	}

	Checker interface {
		Check(rhp.HostSettings) api.HostGougingBreakdown
		BlocksUntilBlockHeightGouging(hostHeight uint64) int64
	}

	checker struct {
		consensusState api.ConsensusState
		settings       api.GougingSettings
	}
)

var _ Checker = checker{}

func NewChecker(gs api.GougingSettings, cs api.ConsensusState) Checker {
	return checker{
		consensusState: cs,
		settings:       gs,
	}
}

func (gc checker) BlocksUntilBlockHeightGouging(hostHeight uint64) int64 {
	blockHeight := gc.consensusState.BlockHeight
	leeway := gc.settings.HostBlockHeightLeeway
	var minHeight uint64
	if blockHeight >= uint64(leeway) {
		minHeight = blockHeight - uint64(leeway)
	}
	return int64(hostHeight) - int64(minHeight)
}

func (gc checker) Check(hs rhp.HostSettings) (gb api.HostGougingBreakdown) {
	prices := hs.Prices
	gs := gc.settings

	// upload gouging
	var uploadErrs []error
	if prices.StoragePrice.Cmp(gs.MaxStoragePrice) > 0 {
		uploadErrs = append(uploadErrs, fmt.Errorf("%v: storage price exceeds max storage price: %v > %v", ErrPriceTableGouging, prices.StoragePrice, gs.MaxStoragePrice))
	}
	if prices.IngressPrice.Cmp(gs.MaxUploadPrice) > 0 {
		uploadErrs = append(uploadErrs, fmt.Errorf("%v: ingress price exceeds max upload price: %v > %v", ErrPriceTableGouging, prices.IngressPrice, gs.MaxUploadPrice))
	}
	gb.UploadErr = errsToStr(uploadErrs...)
	if gougingErr := errsToStr(uploadErrs...); gougingErr != "" {
		gb.UploadErr = fmt.Sprintf("%v: %s", ErrPriceTableGouging, gougingErr)
	}

	// download gouging
	if prices.EgressPrice.Cmp(gs.MaxDownloadPrice) > 0 {
		gb.DownloadErr = fmt.Sprintf("%v: egress price exceeds max download price: %v > %v", ErrPriceTableGouging, prices.EgressPrice, gs.MaxDownloadPrice)
	}

	// prune gouging
	maxFreeSectorCost := types.Siacoins(1).Div64((1 << 40) / rhpv4.SectorSize) // 1 SC / TiB
	if prices.FreeSectorPrice.Cmp(maxFreeSectorCost) > 0 {
		gb.PruneErr = fmt.Sprintf("%v: cost to free a sector exceeds max free sector cost: %v  > %v", ErrPriceTableGouging, prices.FreeSectorPrice, maxFreeSectorCost)
	}

	// general gouging
	var errs []error
	if prices.ContractPrice.Cmp(gs.MaxContractPrice) > 0 {
		errs = append(errs, fmt.Errorf("contract price exceeds max contract price: %v > %v", prices.ContractPrice, gs.MaxContractPrice))
	}
	if hs.MaxCollateral.IsZero() {
		errs = append(errs, errors.New("max collateral is zero"))
	}
	if hs.Validity < time.Duration(gs.MinPriceTableValidity) {
		errs = append(errs, fmt.Errorf("price table validity is less than %v: %v", gs.MinPriceTableValidity, hs.Validity))
	}
	if err := checkBlockHeight(gc.consensusState, hs.Prices.TipHeight, uint64(gs.HostBlockHeightLeeway)); err != nil {
		errs = append(errs, err)
	}
	if gougingErr := errsToStr(errs...); gougingErr != "" {
		gb.GougingErr = fmt.Sprintf("%v: %s", ErrPriceTableGouging, gougingErr)
	}
	gb.GougingErr = errsToStr(errs...)

	return
}

func checkBlockHeight(cs api.ConsensusState, hostBH, leeway uint64) error {
	// check block height - if too much time has passed since the last block
	// there is a chance we are not up-to-date anymore. So we only check whether
	// the host's height is at least equal to ours.
	if !cs.Synced || time.Since(cs.LastBlockTime.Std()) > time.Hour {
		if hostBH < cs.BlockHeight {
			return fmt.Errorf("consensus not synced and host block height is lower, %v < %v", hostBH, cs.BlockHeight)
		}
	} else {
		var minHeight uint64
		if cs.BlockHeight >= leeway {
			minHeight = cs.BlockHeight - leeway
		}
		maxHeight := cs.BlockHeight + leeway
		if !(minHeight <= hostBH && hostBH <= maxHeight) {
			return fmt.Errorf("consensus is synced and host block height is not within range, %v-%v %v", minHeight, maxHeight, hostBH)
		}
	}
	return nil
}

func sectorReadCost(readLengthCost, readBaseCost, initBaseCost, ulBWCost, dlBWCost types.Currency) (types.Currency, bool) {
	// base
	base, overflow := readLengthCost.Mul64WithOverflow(rhpv4.SectorSize)
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
	egress, overflow := dlBWCost.Mul64WithOverflow(rhpv4.SectorSize)
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

func errsToStr(errs ...error) string {
	if err := errors.Join(errs...); err != nil {
		return err.Error()
	}
	return ""
}
