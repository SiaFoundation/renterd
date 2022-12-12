package gouging

import (
	"fmt"

	"gitlab.com/NebulousLabs/errors"
	"go.sia.tech/renterd/internal/utils"
	rhpv2 "go.sia.tech/renterd/rhp/v2"
	"go.sia.tech/siad/modules"
	"go.sia.tech/siad/types"
)

var (
	// ErrGougingDetected is returned when price gouging was detected in one of
	// the fields of either the price table or the host's external settings.
	ErrGougingDetected = errors.New("price gouging detected")
)

type (
	GougingResults struct {
		downloadErr     error
		formContractErr error
		uploadErr       error
	}

	GougingSettings struct {
		MaxRPCPrice      types.Currency
		MaxContractPrice types.Currency
		MaxDownloadPrice types.Currency // per TiB
		MaxUploadPrice   types.Currency // per TiB
	}
)

func (gr GougingResults) CanDownload() (errs []error) {
	return utils.FilterErrors(
		gr.downloadErr,
	)
}

func (gr GougingResults) CanForm() []error {
	return gr.CanUpload() // same conditions apply
}

func (gr GougingResults) CanUpload() (errs []error) {
	return utils.FilterErrors(
		gr.downloadErr,
		gr.uploadErr,
		gr.formContractErr,
	)
}

func PerformGougingChecks(gs GougingSettings, hs rhpv2.HostSettings, period uint64, redundancy float64) GougingResults {
	return GougingResults{
		downloadErr:     checkDownloadGouging(gs, hs, redundancy),
		formContractErr: checkFormContractGouging(gs, hs),
		uploadErr:       checkUploadGouging(gs, hs, period, redundancy),
	}
}

func checkDownloadGouging(gs GougingSettings, hs rhpv2.HostSettings, redundancy float64) error {
	// check base rpc price
	if !gs.MaxRPCPrice.IsZero() && hs.BaseRPCPrice.Cmp(gs.MaxRPCPrice) > 0 {
		return fmt.Errorf("rpc price exceeds max: %v>%v, %w", hs.BaseRPCPrice, gs.MaxRPCPrice, ErrGougingDetected)
	}

	// check download cost
	downloadPrice := downloadPricePerTB(hs).MulFloat(redundancy)
	if !gs.MaxDownloadPrice.IsZero() && downloadPrice.Cmp(gs.MaxDownloadPrice) > 0 {
		return fmt.Errorf("cost per TiB exceeds max dl price: %v>%v, %w", downloadPrice, gs.MaxDownloadPrice, ErrGougingDetected)
	}

	return nil
}

func checkUploadGouging(gs GougingSettings, hs rhpv2.HostSettings, period uint64, redundancy float64) error {
	// check base rpc price
	if !gs.MaxRPCPrice.IsZero() && hs.BaseRPCPrice.Cmp(gs.MaxRPCPrice) > 0 {
		return fmt.Errorf("rpc price exceeds max: %v>%v, %w", hs.BaseRPCPrice, gs.MaxRPCPrice, ErrGougingDetected)
	}

	// check upload cost
	uploadPrice := uploadPricePerTB(hs, period).MulFloat(redundancy)
	if !gs.MaxUploadPrice.IsZero() && uploadPrice.Cmp(gs.MaxUploadPrice) > 0 {
		return fmt.Errorf("cost per TiB exceeds max ul price: %v>%v, %w", uploadPrice, gs.MaxUploadPrice, ErrGougingDetected)
	}

	return nil
}

func checkFormContractGouging(gs GougingSettings, hs rhpv2.HostSettings) error {
	// check base rpc price
	if !gs.MaxRPCPrice.IsZero() && hs.BaseRPCPrice.Cmp(gs.MaxRPCPrice) > 0 {
		return fmt.Errorf("rpc price exceeds max: %v>%v, %w", hs.BaseRPCPrice, gs.MaxRPCPrice, ErrGougingDetected)
	}

	// check contract price
	if !gs.MaxContractPrice.IsZero() && hs.ContractPrice.Cmp(gs.MaxContractPrice) > 0 {
		return fmt.Errorf("contract price exceeds max: %v>%v, %w", hs.ContractPrice, gs.MaxContractPrice, ErrGougingDetected)
	}

	return nil
}

func uploadPricePerTB(hs rhpv2.HostSettings, period uint64) types.Currency {
	return hs.SectorAccessPrice.
		Add(hs.BaseRPCPrice).
		Add(hs.UploadBandwidthPrice).Mul64(modules.SectorSize).
		Add(hs.StoragePrice).Mul64(period).Mul64(modules.SectorSize).
		Mul64(1 << 18) // sectors per TiB
}

func downloadPricePerTB(hs rhpv2.HostSettings) types.Currency {
	return hs.SectorAccessPrice.
		Add(hs.BaseRPCPrice).
		Add(hs.DownloadBandwidthPrice).Mul64(modules.SectorSize).
		Mul64(1 << 18) // sectors per TiB
}
