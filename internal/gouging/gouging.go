package gouging

import (
	"fmt"
	"math"

	"gitlab.com/NebulousLabs/errors"
	rhpv2 "go.sia.tech/renterd/rhp/v2"
	"go.sia.tech/siad/modules"
	"go.sia.tech/siad/types"
)

var (
	// ErrGougingDetected is returned when price gouging was detected in one of
	// the fields of either the price table or the host's external settings.
	ErrGougingDetected = errors.New("price gouging detected")

	downloadOverdriveEstimatePct = .1
)

type (
	GougingChecks struct {
		Download     GougingCheck
		FormContract GougingCheck
		Upload       GougingCheck
	}

	GougingCheck interface {
		IsGouging() bool
		error
	}

	GougingSettings struct {
		MaxRPCPrice      types.Currency
		MaxContractPrice types.Currency
		MaxDownloadPrice types.Currency // per 1TiB
		MaxUploadPrice   types.Currency // per 1TiB
	}

	check struct {
		err error
	}

	// TODO: use type defined in bus package
	RedundancySettings struct {
		MinShards   uint64
		TotalShards uint64
	}
)

func (rs *RedundancySettings) redundancy() float64 {
	return float64(rs.TotalShards) / float64(rs.MinShards)
}

func (c *check) IsGouging() bool {
	return c.err != nil
}

func (c *check) Error() string {
	if c.err != nil {
		return c.err.Error()
	}
	return ""
}

func PerformGougingChecks(gs GougingSettings, hs rhpv2.HostSettings, rs RedundancySettings, period uint64) GougingChecks {
	return GougingChecks{
		Download:     &check{err: checkDownloadGouging(gs, hs, rs)},
		FormContract: &check{err: checkFormContractGouging(gs, hs)},
		Upload:       &check{err: checkUploadGouging(gs, hs, rs, period)},
	}
}

func checkDownloadGouging(gs GougingSettings, hs rhpv2.HostSettings, rs RedundancySettings) error {
	// check base rpc price
	if hs.BaseRPCPrice.Cmp(gs.MaxRPCPrice) > 0 {
		return fmt.Errorf("rpc price exceeds max: %v>%v, %w", hs.BaseRPCPrice, gs.MaxRPCPrice, ErrGougingDetected)
	}

	// check download cost
	redundancy := uint64(math.Ceil(rs.redundancy()))
	downloadPrice := downloadPricePerTB(hs).Mul64(redundancy)
	if downloadPrice.Cmp(gs.MaxDownloadPrice) > 0 {
		return fmt.Errorf("cost per TiB exceeds max dl price: %v>%v, %w", downloadPrice, gs.MaxDownloadPrice, ErrGougingDetected)
	}

	return nil
}

func checkUploadGouging(gs GougingSettings, hs rhpv2.HostSettings, rs RedundancySettings, period uint64) error {
	// check base rpc price
	if hs.BaseRPCPrice.Cmp(gs.MaxRPCPrice) > 0 {
		return fmt.Errorf("rpc price exceeds max: %v>%v, %w", hs.BaseRPCPrice, gs.MaxRPCPrice, ErrGougingDetected)
	}

	// check upload cost
	redundancy := uint64(math.Ceil(rs.redundancy()))
	uploadPrice := uploadPricePerTB(hs, period).Mul64(redundancy)
	if uploadPrice.Cmp(gs.MaxUploadPrice) > 0 {
		return fmt.Errorf("cost per TiB exceeds max ul price: %v>%v, %w", uploadPrice, gs.MaxUploadPrice, ErrGougingDetected)
	}

	return nil
}

func checkFormContractGouging(gs GougingSettings, hs rhpv2.HostSettings) error {
	// check base rpc price
	if hs.BaseRPCPrice.Cmp(gs.MaxRPCPrice) > 0 {
		return fmt.Errorf("rpc price exceeds max: %v>%v, %w", hs.BaseRPCPrice, gs.MaxRPCPrice, ErrGougingDetected)
	}

	// check contract price
	if hs.ContractPrice.Cmp(gs.MaxContractPrice) > 0 {
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
		MulFloat(downloadOverdriveEstimatePct). //
		Mul64(1 << 18)                          // sectors per TiB
}
