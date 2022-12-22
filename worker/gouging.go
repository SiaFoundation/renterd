package worker

import (
	"fmt"
	"strings"

	"go.sia.tech/renterd/api/bus"
	rhpv2 "go.sia.tech/renterd/rhp/v2"
	"go.sia.tech/siad/modules"
	"go.sia.tech/siad/types"
)

type (
	GougingResults struct {
		downloadErr     error
		formContractErr error
		uploadErr       error
	}
)

func (gr GougingResults) IsGouging() (bool, string) {
	errs := filterErrors(
		gr.downloadErr,
		gr.uploadErr,
		gr.formContractErr,
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

func (gr GougingResults) CanDownload() (errs []error) {
	return filterErrors(
		gr.downloadErr,
	)
}

func (gr GougingResults) CanForm() []error {
	return gr.CanUpload() // same conditions apply
}

func (gr GougingResults) CanUpload() (errs []error) {
	return filterErrors(
		gr.downloadErr,
		gr.uploadErr,
		gr.formContractErr,
	)
}

func PerformGougingChecks(gs bus.GougingSettings, hs rhpv2.HostSettings, period uint64, redundancy float64) GougingResults {
	return GougingResults{
		downloadErr:     checkDownloadGouging(gs, hs, redundancy),
		formContractErr: checkFormContractGouging(gs, hs),
		uploadErr:       checkUploadGouging(gs, hs, period, redundancy),
	}
}

func checkDownloadGouging(gs bus.GougingSettings, hs rhpv2.HostSettings, redundancy float64) error {
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

func checkUploadGouging(gs bus.GougingSettings, hs rhpv2.HostSettings, period uint64, redundancy float64) error {
	// check base rpc price
	if !gs.MaxRPCPrice.IsZero() && hs.BaseRPCPrice.Cmp(gs.MaxRPCPrice) > 0 {
		return fmt.Errorf("rpc price exceeds max: %v>%v", hs.BaseRPCPrice, gs.MaxRPCPrice)
	}

	// check upload cost
	uploadPrice := uploadPricePerTB(hs, period).MulFloat(redundancy)
	if !gs.MaxUploadPrice.IsZero() && uploadPrice.Cmp(gs.MaxUploadPrice) > 0 {
		return fmt.Errorf("cost per TiB exceeds max ul price: %v>%v", uploadPrice, gs.MaxUploadPrice)
	}

	return nil
}

func checkFormContractGouging(gs bus.GougingSettings, hs rhpv2.HostSettings) error {
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

func uploadPricePerTB(hs rhpv2.HostSettings, period uint64) types.Currency {
	sectorPrice := hs.SectorAccessPrice.
		Add(hs.BaseRPCPrice).
		Add(hs.UploadBandwidthPrice.Mul64(modules.SectorSize)).
		Add(hs.StoragePrice.Mul64(period).Mul64(modules.SectorSize))

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
