package worker

import (
	"context"
	"errors"
	"fmt"
	"strings"

	rhpv2 "go.sia.tech/core/rhp/v2"
	rhpv3 "go.sia.tech/core/rhp/v3"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"go.sia.tech/siad/modules"
)

const keyGougingChecker contextKey = "GougingChecker"

type (
	GougingChecker interface {
		CheckHS(*rhpv2.HostSettings) GougingResults
		CheckPT(*rhpv3.HostPriceTable) GougingResults
	}

	GougingResults struct {
		downloadErr error
		gougingErr  error
		uploadErr   error
	}

	gougingChecker struct {
		settings   api.GougingSettings
		redundancy api.RedundancySettings
	}

	contextKey string
)

func PerformGougingChecks(ctx context.Context, hs *rhpv2.HostSettings, pt *rhpv3.HostPriceTable) (results GougingResults) {
	gc, ok := ctx.Value(keyGougingChecker).(GougingChecker)
	if !ok {
		panic("no gouging checker attached to the context") // developer error
	}

	results.merge(gc.CheckHS(hs))
	results.merge(gc.CheckPT(pt))
	return
}

func WithGougingChecker(ctx context.Context, gp api.GougingParams) context.Context {
	return context.WithValue(ctx, keyGougingChecker, gougingChecker{
		settings:   gp.GougingSettings,
		redundancy: gp.RedundancySettings,
	})
}

func IsGouging(gs api.GougingSettings, rs api.RedundancySettings, hs *rhpv2.HostSettings, pt *rhpv3.HostPriceTable) (bool, string) {
	var errs []error
	if hs != nil {
		errs = append(errs,
			filterErrors(
				checkDownloadGouging(gs, rs, hs.BaseRPCPrice, hs.SectorAccessPrice, hs.DownloadBandwidthPrice),
				checkPriceGougingHS(gs, hs),
				checkUploadGouging(gs, rs, hs.BaseRPCPrice, hs.StoragePrice, hs.UploadBandwidthPrice),
			)...,
		)
	}

	if pt != nil {
		errs = append(errs,
			filterErrors(
				checkDownloadGouging(gs, rs, pt.InitBaseCost, pt.ReadBaseCost.Add(pt.ReadLengthCost.Mul64(modules.SectorSize)), pt.DownloadBandwidthCost),
				checkPriceGougingPT(gs, pt),
				checkUploadGouging(gs, rs, pt.InitBaseCost, pt.WriteBaseCost.Add(pt.WriteLengthCost.Mul64(modules.SectorSize)), pt.UploadBandwidthCost),
			)...,
		)
	}

	if len(errs) == 0 {
		return false, ""
	}
	return true, joinErrors(errs...).Error()
}

func (gc gougingChecker) CheckHS(hs *rhpv2.HostSettings) (results GougingResults) {
	if hs != nil {
		results = GougingResults{
			downloadErr: checkDownloadGouging(gc.settings, gc.redundancy, hs.BaseRPCPrice, hs.SectorAccessPrice, hs.DownloadBandwidthPrice),
			gougingErr:  checkPriceGougingHS(gc.settings, hs),
			uploadErr:   checkUploadGouging(gc.settings, gc.redundancy, hs.BaseRPCPrice, hs.StoragePrice, hs.UploadBandwidthPrice),
		}
	}
	return
}

func (gc gougingChecker) CheckPT(pt *rhpv3.HostPriceTable) (results GougingResults) {
	if pt != nil {
		results = GougingResults{
			downloadErr: checkDownloadGouging(gc.settings, gc.redundancy, pt.InitBaseCost, pt.ReadBaseCost.Add(pt.ReadLengthCost.Mul64(modules.SectorSize)), pt.DownloadBandwidthCost),
			gougingErr:  checkPriceGougingPT(gc.settings, pt),
			uploadErr:   checkUploadGouging(gc.settings, gc.redundancy, pt.InitBaseCost, pt.WriteBaseCost.Add(pt.WriteLengthCost.Mul64(modules.SectorSize)), pt.UploadBandwidthCost),
		}
	}
	return
}

func (gr GougingResults) CanDownload() (errs []error) {
	return filterErrors(
		gr.downloadErr,
	)
}

func (gr GougingResults) CanForm() []error {
	return gr.CanUpload() // same conditions apply
}

func (gr GougingResults) CanUpload() []error {
	return filterErrors(
		gr.downloadErr,
		gr.gougingErr,
		gr.uploadErr,
	)
}

func (gr *GougingResults) merge(other GougingResults) {
	gr.downloadErr = joinErrors(gr.downloadErr, other.downloadErr)
	gr.gougingErr = joinErrors(gr.gougingErr, other.gougingErr)
	gr.uploadErr = joinErrors(gr.uploadErr, other.uploadErr)
}

func checkPriceGougingHS(gs api.GougingSettings, hs *rhpv2.HostSettings) error {
	// check base rpc price
	if !gs.MaxRPCPrice.IsZero() && hs.BaseRPCPrice.Cmp(gs.MaxRPCPrice) > 0 {
		return fmt.Errorf("rpc price exceeds max: %v>%v", hs.BaseRPCPrice, gs.MaxRPCPrice)
	}

	// check max storage price
	if !gs.MaxStoragePrice.IsZero() && hs.StoragePrice.Cmp(gs.MaxStoragePrice) > 0 {
		return fmt.Errorf("storage price exceeds max: %v>%v", hs.StoragePrice, gs.MaxUploadPrice)
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

func checkPriceGougingPT(gs api.GougingSettings, pt *rhpv3.HostPriceTable) error {
	// check base rpc price
	if !gs.MaxRPCPrice.IsZero() && gs.MaxRPCPrice.Cmp(pt.InitBaseCost) < 0 {
		return fmt.Errorf("init base cost exceeds max: %v>%v", pt.InitBaseCost, gs.MaxRPCPrice)
	}

	// check ReadLengthCost - should be 1H as it's unused by hosts
	if types.NewCurrency64(1).Cmp(pt.ReadLengthCost) < 0 {
		return fmt.Errorf("ReadLengthCost of host is %v but should be %v", pt.ReadLengthCost, types.NewCurrency64(1))
	}

	// check WriteLengthCost - should be 1H as it's unused by hosts
	if types.NewCurrency64(1).Cmp(pt.WriteLengthCost) < 0 {
		return fmt.Errorf("WriteLengthCost of %v exceeds 1H", pt.WriteLengthCost)
	}

	// check MemoryTimeCost - should be 1H as it's unused by hosts
	if types.NewCurrency64(1).Cmp(pt.MemoryTimeCost) < 0 {
		return fmt.Errorf("MemoryTimeCost of %v exceeds 1H", pt.WriteLengthCost)
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

	return nil
}

func checkDownloadGouging(gs api.GougingSettings, rs api.RedundancySettings, baseRPCPrice, sectorAccessPrice, downloadBandwidthPrice types.Currency) error {
	downloadPrice, overflow := downloadBandwidthPrice.Mul64WithOverflow(modules.SectorSize)
	if overflow {
		return fmt.Errorf("overflow detected when computing download bandwidth price")
	}
	sectorPrice, overflow := sectorAccessPrice.AddWithOverflow(baseRPCPrice)
	if overflow {
		return fmt.Errorf("overflow detected when computing sector price")
	}
	sectorPrice, overflow = sectorPrice.AddWithOverflow(downloadPrice)
	if overflow {
		return fmt.Errorf("overflow detected when computing download price per sector")
	}
	dpptb, overflow := sectorPrice.Mul64WithOverflow(1 << 40 / modules.SectorSize) // sectors per TiB
	if overflow {
		return fmt.Errorf("overflow detected when computing download price per TiB")
	}
	downloadPriceTotalShards, overflow := dpptb.Mul64WithOverflow(uint64(rs.TotalShards))
	if overflow {
		return fmt.Errorf("overflow detected when multiplying %v * %v in download gouging", dpptb, rs.TotalShards)
	}
	downloadPrice = downloadPriceTotalShards.Div64(uint64(rs.MinShards))
	if !gs.MaxDownloadPrice.IsZero() && downloadPrice.Cmp(gs.MaxDownloadPrice) > 0 {
		return fmt.Errorf("cost per TiB exceeds max dl price: %v>%v", downloadPrice, gs.MaxDownloadPrice)
	}
	return nil
}

func checkUploadGouging(gs api.GougingSettings, rs api.RedundancySettings, baseRPCPrice, storagePrice, uploadBandwidthPrice types.Currency) error {
	uploadPrice, overflow := uploadBandwidthPrice.Mul64WithOverflow(modules.SectorSize)
	if overflow {
		return fmt.Errorf("overflow detected when computing upload bandwidth price")
	}
	sectorPrice, overflow := storagePrice.AddWithOverflow(baseRPCPrice)
	if overflow {
		return fmt.Errorf("overflow detected when computing sector price")
	}
	sectorPrice, overflow = sectorPrice.AddWithOverflow(uploadPrice)
	if overflow {
		return fmt.Errorf("overflow detected when computing upload price per sector")
	}
	upptb, overflow := sectorPrice.Mul64WithOverflow(1 << 40 / modules.SectorSize) // sectors per TiB
	if overflow {
		return fmt.Errorf("overflow detected when computing upload price per TiB")
	}
	uploadPriceTotalShards, overflow := upptb.Mul64WithOverflow(uint64(rs.TotalShards))
	if overflow {
		return fmt.Errorf("overflow detected when multiplying %v * %v in upload gouging", upptb, rs.TotalShards)
	}
	uploadPrice = uploadPriceTotalShards.Div64(uint64(rs.MinShards))
	if !gs.MaxUploadPrice.IsZero() && uploadPrice.Cmp(gs.MaxUploadPrice) > 0 {
		return fmt.Errorf("cost per TiB exceeds max ul price: %v>%v", uploadPrice, gs.MaxUploadPrice)
	}
	return nil
}

func filterErrors(errs ...error) []error {
	filtered := errs[:0]
	for _, err := range errs {
		if err != nil {
			filtered = append(filtered, err)
		}
	}
	return filtered
}

func joinErrors(errs ...error) error {
	filtered := errs[:0]
	for _, err := range errs {
		if err != nil {
			filtered = append(filtered, err)
		}
	}

	switch len(filtered) {
	case 0:
		return nil
	case 1:
		return filtered[0]
	default:
		strs := make([]string, len(filtered))
		for i := range strs {
			strs[i] = filtered[i].Error()
		}
		return errors.New(strings.Join(strs, ";"))
	}
}
