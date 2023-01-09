package worker

import (
	"context"
	"fmt"
	"strings"

	"go.sia.tech/renterd/api"
	rhpv2 "go.sia.tech/renterd/rhp/v2"
	"go.sia.tech/siad/modules"
	"go.sia.tech/siad/types"
)

const keyGougingChecker contextKey = "GougingChecker"

type (
	GougingChecker interface {
		Check(rhpv2.HostSettings) GougingResults
	}

	GougingResults struct {
		downloadErr     error
		formContractErr error
		uploadErr       error
	}

	gougingChecker struct {
		settings   api.GougingSettings
		redundancy float64
	}

	contextKey string
)

func PerformGougingChecks(ctx context.Context, hs rhpv2.HostSettings) GougingResults {
	if gc, ok := ctx.Value(keyGougingChecker).(GougingChecker); ok {
		return gc.Check(hs)
	}
	panic("no gouging checker attached to the context") // developer error
}

func WithGougingChecker(ctx context.Context, gp api.GougingParams) context.Context {
	rs := gp.RedundancySettings
	return context.WithValue(ctx, keyGougingChecker, gougingChecker{
		settings:   gp.GougingSettings,
		redundancy: float64(rs.TotalShards) / float64(rs.MinShards),
	})
}

func IsGouging(gs api.GougingSettings, hs rhpv2.HostSettings, redundancy float64) (bool, string) {
	errs := filterErrors(
		checkDownloadGouging(gs, hs, redundancy),
		checkFormContractGouging(gs, hs),
		checkUploadGouging(gs, hs, redundancy),
	)
	if len(errs) == 0 {
		return false, ""
	}

	var reasons []string
	for _, err := range errs {
		reasons = append(reasons, err.Error())
	}
	return true, strings.Join(reasons, ", ")
}

func (gc gougingChecker) Check(hs rhpv2.HostSettings) GougingResults {
	return GougingResults{
		downloadErr:     checkDownloadGouging(gc.settings, hs, gc.redundancy),
		formContractErr: checkFormContractGouging(gc.settings, hs),
		uploadErr:       checkUploadGouging(gc.settings, hs, gc.redundancy),
	}
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

func checkDownloadGouging(gs api.GougingSettings, hs rhpv2.HostSettings, redundancy float64) error {
	// check base rpc price
	if !gs.MaxRPCPrice.IsZero() && hs.BaseRPCPrice.Cmp(gs.MaxRPCPrice) > 0 {
		return fmt.Errorf("rpc price exceeds max: %v>%v", hs.BaseRPCPrice, gs.MaxRPCPrice)
	}

	// check download cost
	downloadPrice := downloadPricePerTB(hs).MulFloat(redundancy)
	if !gs.MaxDownloadPrice.IsZero() && downloadPrice.Cmp(gs.MaxDownloadPrice) > 0 {
		return fmt.Errorf("cost per TiB exceeds max dl price: %v>%v", downloadPrice, gs.MaxDownloadPrice)
	}

	return nil
}

func checkUploadGouging(gs api.GougingSettings, hs rhpv2.HostSettings, redundancy float64) error {
	// check base rpc price
	if !gs.MaxRPCPrice.IsZero() && hs.BaseRPCPrice.Cmp(gs.MaxRPCPrice) > 0 {
		return fmt.Errorf("rpc price exceeds max: %v>%v", hs.BaseRPCPrice, gs.MaxRPCPrice)
	}

	// check max storage price
	if !gs.MaxStoragePrice.IsZero() && hs.StoragePrice.Cmp(gs.MaxStoragePrice) > 0 {
		return fmt.Errorf("storage price exceeds max: %v>%v", hs.StoragePrice, gs.MaxUploadPrice)
	}

	// check upload cost
	uploadPrice := uploadPricePerTB(hs).MulFloat(redundancy)
	if !gs.MaxUploadPrice.IsZero() && uploadPrice.Cmp(gs.MaxUploadPrice) > 0 {
		return fmt.Errorf("cost per TiB exceeds max ul price: %v>%v", uploadPrice, gs.MaxUploadPrice)
	}

	return nil
}

func checkFormContractGouging(gs api.GougingSettings, hs rhpv2.HostSettings) error {
	// check base rpc price
	if !gs.MaxRPCPrice.IsZero() && hs.BaseRPCPrice.Cmp(gs.MaxRPCPrice) > 0 {
		return fmt.Errorf("rpc price exceeds max: %v>%v", hs.BaseRPCPrice, gs.MaxRPCPrice)
	}

	// check contract price
	if !gs.MaxContractPrice.IsZero() && hs.ContractPrice.Cmp(gs.MaxContractPrice) > 0 {
		return fmt.Errorf("contract price exceeds max: %v>%v", hs.ContractPrice, gs.MaxContractPrice)
	}

	return nil
}

func uploadPricePerTB(hs rhpv2.HostSettings) types.Currency {
	sectorPrice := hs.SectorAccessPrice.
		Add(hs.BaseRPCPrice).
		Add(hs.UploadBandwidthPrice.Mul64(modules.SectorSize))

	return sectorPrice.Mul64(1 << 40 / modules.SectorSize) // sectors per TiB
}

func downloadPricePerTB(hs rhpv2.HostSettings) types.Currency {
	sectorPrice := hs.SectorAccessPrice.
		Add(hs.BaseRPCPrice).
		Add(hs.DownloadBandwidthPrice.Mul64(modules.SectorSize))

	return sectorPrice.Mul64(1 << 40 / modules.SectorSize) // sectors per TiB
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
