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
		downloadErr     error
		formContractErr error
		uploadErr       error
	}

	gougingChecker struct {
		settings    api.GougingSettings
		minShards   int
		totalShards int
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
		settings:    gp.GougingSettings,
		minShards:   gp.RedundancySettings.MinShards,
		totalShards: gp.RedundancySettings.TotalShards,
	})
}

func IsGouging(gs api.GougingSettings, hs *rhpv2.HostSettings, pt *rhpv3.HostPriceTable, minShards, totalShards int) (bool, string) {
	var hsErrs []error
	if hs != nil {
		hsErrs = filterErrors(
			checkDownloadGougingHS(gs, hs, minShards, totalShards),
			checkFormContractGougingHS(gs, hs),
			checkUploadGougingHS(gs, hs, minShards, totalShards),
		)
	}

	var ptErrs []error
	if pt != nil {
		ptErrs = filterErrors(
			checkDownloadGougingPT(gs, pt, minShards, totalShards),
			checkFormContractGougingPT(gs, pt),
			checkUploadGougingPT(gs, pt, minShards, totalShards),
		)
	}

	if len(hsErrs)+len(ptErrs) == 0 {
		return false, ""
	}
	return true, joinErrors(append(hsErrs, ptErrs...)...).Error()
}

func (gc gougingChecker) CheckHS(hs *rhpv2.HostSettings) (results GougingResults) {
	if hs != nil {
		results = GougingResults{
			downloadErr:     checkDownloadGougingHS(gc.settings, hs, gc.minShards, gc.totalShards),
			formContractErr: checkFormContractGougingHS(gc.settings, hs),
			uploadErr:       checkUploadGougingHS(gc.settings, hs, gc.minShards, gc.totalShards),
		}
	}
	return
}

func (gc gougingChecker) CheckPT(pt *rhpv3.HostPriceTable) (results GougingResults) {
	if pt != nil {
		results = GougingResults{
			downloadErr:     checkDownloadGougingPT(gc.settings, pt, gc.minShards, gc.totalShards),
			formContractErr: checkFormContractGougingPT(gc.settings, pt),
			uploadErr:       checkUploadGougingPT(gc.settings, pt, gc.minShards, gc.totalShards),
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
		gr.uploadErr,
		gr.formContractErr,
	)
}

func (gr *GougingResults) merge(other GougingResults) {
	gr.downloadErr = joinErrors(gr.downloadErr, other.downloadErr)
	gr.uploadErr = joinErrors(gr.uploadErr, other.uploadErr)
	gr.formContractErr = joinErrors(gr.formContractErr, other.formContractErr)
}

func checkDownloadGougingHS(gs api.GougingSettings, hs *rhpv2.HostSettings, minShards, totalShards int) error {
	// check base rpc price
	if !gs.MaxRPCPrice.IsZero() && hs.BaseRPCPrice.Cmp(gs.MaxRPCPrice) > 0 {
		return fmt.Errorf("rpc price exceeds max: %v>%v", hs.BaseRPCPrice, gs.MaxRPCPrice)
	}

	// check download cost
	dpptb, overflow := downloadPricePerTB(hs.BaseRPCPrice, hs.SectorAccessPrice, hs.DownloadBandwidthPrice)
	if overflow {
		return fmt.Errorf("overflow detected when computing download price per TiB")
	}
	downloadPriceTotalShards, overflow := dpptb.Mul64WithOverflow(uint64(totalShards))
	if overflow {
		return fmt.Errorf("overflow detected when multiplying %v * %v in download gouging", dpptb, totalShards)
	}
	downloadPrice := downloadPriceTotalShards.Div64(uint64(minShards))
	if !gs.MaxDownloadPrice.IsZero() && downloadPrice.Cmp(gs.MaxDownloadPrice) > 0 {
		return fmt.Errorf("cost per TiB exceeds max dl price: %v>%v", downloadPrice, gs.MaxDownloadPrice)
	}

	return nil
}

func checkDownloadGougingPT(gs api.GougingSettings, pt *rhpv3.HostPriceTable, minShards, totalShards int) error {
	// check base rpc price
	if !gs.MaxRPCPrice.IsZero() && gs.MaxRPCPrice.Cmp(pt.InitBaseCost) < 0 {
		return fmt.Errorf("init base cost exceeds max: %v>%v", pt.InitBaseCost, gs.MaxRPCPrice)
	}

	// check ReadLengthCost - should be 1H as it's unused by hosts
	if types.NewCurrency64(1).Cmp(pt.ReadLengthCost) < 0 {
		return fmt.Errorf("ReadLengthCost of host is %v but should be %v", pt.ReadLengthCost, types.NewCurrency64(1))
	}

	// check download cost
	dpptb, overflow := downloadPricePerTB(pt.InitBaseCost, pt.ReadBaseCost.Add(pt.ReadLengthCost.Mul64(modules.SectorSize)), pt.DownloadBandwidthCost)
	if overflow {
		return fmt.Errorf("overflow detected when computing download price per TiB")
	}
	downloadPriceTotalShards, overflow := dpptb.Mul64WithOverflow(uint64(totalShards))
	if overflow {
		return fmt.Errorf("overflow detected when multiplying %v * %v in download gouging", dpptb, totalShards)
	}
	downloadPrice := downloadPriceTotalShards.Div64(uint64(minShards))
	if !gs.MaxDownloadPrice.IsZero() && downloadPrice.Cmp(gs.MaxDownloadPrice) > 0 {
		return fmt.Errorf("cost per TiB exceeds max dl price: %v>%v", downloadPrice, gs.MaxDownloadPrice)
	}

	return nil
}

func checkUploadGougingHS(gs api.GougingSettings, hs *rhpv2.HostSettings, minShards, totalShards int) error {
	// check base rpc price
	if !gs.MaxRPCPrice.IsZero() && hs.BaseRPCPrice.Cmp(gs.MaxRPCPrice) > 0 {
		return fmt.Errorf("rpc price exceeds max: %v>%v", hs.BaseRPCPrice, gs.MaxRPCPrice)
	}

	// check max storage price
	if !gs.MaxStoragePrice.IsZero() && hs.StoragePrice.Cmp(gs.MaxStoragePrice) > 0 {
		return fmt.Errorf("storage price exceeds max: %v>%v", hs.StoragePrice, gs.MaxUploadPrice)
	}

	// check upload cost
	upptb, overflow := uploadPricePerTB(hs.BaseRPCPrice, hs.SectorAccessPrice, hs.UploadBandwidthPrice)
	if overflow {
		return fmt.Errorf("overflow detected when computing upload price per TiB")
	}
	uploadPriceTotalShards, overflow := upptb.Mul64WithOverflow(uint64(totalShards))
	if overflow {
		return fmt.Errorf("overflow detected when multiplying %v * %v in upload gouging", upptb, totalShards)
	}
	uploadPrice := uploadPriceTotalShards.Div64(uint64(minShards))
	if !gs.MaxUploadPrice.IsZero() && uploadPrice.Cmp(gs.MaxUploadPrice) > 0 {
		return fmt.Errorf("cost per TiB exceeds max ul price: %v>%v", uploadPrice, gs.MaxUploadPrice)
	}

	return nil
}

func checkUploadGougingPT(gs api.GougingSettings, pt *rhpv3.HostPriceTable, minShards, totalShards int) error {
	// check base rpc price
	if !gs.MaxRPCPrice.IsZero() && gs.MaxRPCPrice.Cmp(pt.InitBaseCost) < 0 {
		return fmt.Errorf("init base cost exceeds max: %v>%v", pt.InitBaseCost, gs.MaxRPCPrice)
	}

	// check WriteLengthCost - should be 1H as it's unused by hosts
	if types.NewCurrency64(1).Cmp(pt.WriteLengthCost) < 0 {
		return fmt.Errorf("WriteLengthCost of %v exceeds 1H", pt.WriteLengthCost)
	}

	// check MemoryTimeCost - should be 1H as it's unused by hosts
	if types.NewCurrency64(1).Cmp(pt.MemoryTimeCost) < 0 {
		return fmt.Errorf("MemoryTimeCost of %v exceeds 1H", pt.WriteLengthCost)
	}

	// check upload cost
	upptb, overflow := uploadPricePerTB(pt.InitBaseCost, pt.WriteBaseCost.Add(pt.WriteLengthCost.Mul64(modules.SectorSize)), pt.UploadBandwidthCost)
	if overflow {
		return fmt.Errorf("overflow detected when computing upload price per TiB")
	}
	uploadPriceTotalShards, overflow := upptb.Mul64WithOverflow(uint64(totalShards))
	if overflow {
		return fmt.Errorf("overflow detected when multiplying %v * %v in upload gouging", upptb, totalShards)
	}
	uploadPrice := uploadPriceTotalShards.Div64(uint64(minShards))
	if !gs.MaxUploadPrice.IsZero() && uploadPrice.Cmp(gs.MaxUploadPrice) > 0 {
		return fmt.Errorf("cost per TiB exceeds max ul price: %v>%v", uploadPrice, gs.MaxUploadPrice)
	}

	return nil
}

func checkFormContractGougingHS(gs api.GougingSettings, hs *rhpv2.HostSettings) error {
	// check base rpc price
	if !gs.MaxRPCPrice.IsZero() && hs.BaseRPCPrice.Cmp(gs.MaxRPCPrice) > 0 {
		return fmt.Errorf("rpc price exceeds max: %v>%v", hs.BaseRPCPrice, gs.MaxRPCPrice)
	}

	// check contract price
	if !gs.MaxContractPrice.IsZero() && hs.ContractPrice.Cmp(gs.MaxContractPrice) > 0 {
		return fmt.Errorf("contract price exceeds max: %v>%v", hs.ContractPrice, gs.MaxContractPrice)
	}

	// check MaxCollateral
	if hs.MaxCollateral.IsZero() {
		return errors.New("MaxCollateral of host is 0")
	}
	if hs.MaxCollateral.Cmp(gs.MinMaxCollateral) < 0 {
		return fmt.Errorf("MaxCollateral is below minimum: %v<%v", hs.MaxCollateral, gs.MinMaxCollateral)
	}
	return nil
}

func checkFormContractGougingPT(gs api.GougingSettings, pt *rhpv3.HostPriceTable) error {
	// check base rpc price
	if !gs.MaxRPCPrice.IsZero() && gs.MaxRPCPrice.Cmp(pt.InitBaseCost) < 0 {
		return fmt.Errorf("init base cost exceeds max: %v>%v", pt.InitBaseCost, gs.MaxRPCPrice)
	}

	// check contract price
	if !gs.MaxContractPrice.IsZero() && pt.ContractPrice.Cmp(gs.MaxContractPrice) > 0 {
		return fmt.Errorf("contract price exceeds max: %v>%v", pt.ContractPrice, gs.MaxContractPrice)
	}

	// check MaxCollateral
	if pt.MaxCollateral.IsZero() {
		return errors.New("MaxCollateral of host is 0")
	}
	if pt.MaxCollateral.Cmp(gs.MinMaxCollateral) < 0 {
		return fmt.Errorf("MaxCollateral is below minimum: %v<%v", pt.MaxCollateral, gs.MinMaxCollateral)
	}

	return nil
}

func uploadPricePerTB(baseRPCPrice, sectorAccessPrice, UploadBandwidthPrice types.Currency) (types.Currency, bool) {
	uploadPrice, overflow := UploadBandwidthPrice.Mul64WithOverflow(modules.SectorSize)
	if overflow {
		return types.ZeroCurrency, true
	}
	sectorPrice, overflow := sectorAccessPrice.AddWithOverflow(baseRPCPrice)
	if overflow {
		return types.ZeroCurrency, true
	}
	sectorPrice, overflow = sectorPrice.AddWithOverflow(uploadPrice)
	if overflow {
		return types.ZeroCurrency, true
	}
	return sectorPrice.Mul64WithOverflow(1 << 40 / modules.SectorSize) // sectors per TiB
}

func downloadPricePerTB(baseRPCPrice, sectorAccessPrice, downloadBandwidthPrice types.Currency) (types.Currency, bool) {
	downloadPrice, overflow := downloadBandwidthPrice.Mul64WithOverflow(modules.SectorSize)
	if overflow {
		return types.ZeroCurrency, true
	}
	sectorPrice, overflow := sectorAccessPrice.AddWithOverflow(baseRPCPrice)
	if overflow {
		return types.ZeroCurrency, true
	}
	sectorPrice, overflow = sectorPrice.AddWithOverflow(downloadPrice)
	if overflow {
		return types.ZeroCurrency, true
	}
	return sectorPrice.Mul64WithOverflow(1 << 40 / modules.SectorSize) // sectors per TiB
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
