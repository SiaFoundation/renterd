package autopilot

import (
	"fmt"
	"math"
	"math/big"

	"go.sia.tech/renterd/bus"
	"go.sia.tech/siad/modules"
	"go.sia.tech/siad/types"
)

const (
	// minContractFundUploadThreshold is the percentage of contract funds
	// remaining at which the contract gets marked as not good for upload
	minContractFundUploadThreshold = float64(0.05) // 5%
)

type hostFilter struct {
	host     host
	metadata bus.ContractMetadata
	reasons  []string

	initialGFU bool
	initialGFR bool
}

func newHostFilter(h host, m bus.ContractMetadata) *hostFilter {
	return &hostFilter{
		host:     h,
		metadata: m,

		// save initial GFU & GFR values
		initialGFU: m.GoodForUpload,
		initialGFR: m.GoodForRenew,
	}
}

func (f *hostFilter) finalize() (bus.ContractMetadata, []string, bool) {
	updatedGFU := !f.metadata.GoodForUpload && f.initialGFU
	updatedGFR := !f.metadata.GoodForRenew && f.initialGFR
	return f.metadata, f.reasons, updatedGFU || updatedGFR
}

func (f *hostFilter) withBlackListFilter(cfg Config) *hostFilter {
	for _, host := range cfg.Hosts.Blacklist {
		if f.host.IsHost(host) {
			f.metadata.GoodForUpload = false
			f.metadata.GoodForRenew = false
			f.reasons = append(f.reasons, fmt.Sprintf("host is blacklisted, GFU: %v -> false, GFR: %v -> false", f.initialGFU, f.initialGFR))
			break
		}
	}
	return f
}

func (f *hostFilter) withMaxRevisionFilter() *hostFilter {
	if f.host.Revision.NewRevisionNumber == math.MaxUint64 {
		f.metadata.GoodForUpload = false
		f.metadata.GoodForRenew = false
		f.reasons = append(f.reasons, fmt.Sprintf("max revision check failed, GFU: %v -> false, GFR: %v -> false", f.initialGFU, f.initialGFR))
	}
	return f
}

func (f *hostFilter) withOfflineFilter() *hostFilter {
	if !f.host.IsOnline() {
		f.metadata.GoodForUpload = false
		f.metadata.GoodForRenew = false
		f.reasons = append(f.reasons, fmt.Sprintf("host is not online, GFU: %v -> false, GFR: %v -> false", f.initialGFU, f.initialGFR))
	}
	return f
}

func (f *hostFilter) withRedundantIPFilter(filter *ipFilter) *hostFilter {
	if filter.filtered(f.host) {
		f.metadata.GoodForUpload = false
		f.metadata.GoodForRenew = false
		f.reasons = append(f.reasons, fmt.Sprintf("host IP is redundant, GFU: %v -> false, GFR: %v -> false", f.initialGFU, f.initialGFR))
	}
	return f
}

func (f *hostFilter) withRemainingFundsFilter(cfg Config) *hostFilter {
	settings, _, found := f.host.LastKnownSettings()
	if !found {
		return f
	}

	blockBytes := types.NewCurrency64(modules.SectorSize * cfg.Contracts.Period)
	sectorStoragePrice := settings.StoragePrice.Mul(blockBytes)
	sectorUploadBandwidthPrice := settings.UploadBandwidthPrice.Mul64(modules.SectorSize)
	sectorDownloadBandwidthPrice := settings.DownloadBandwidthPrice.Mul64(modules.SectorSize)
	sectorBandwidthPrice := sectorUploadBandwidthPrice.Add(sectorDownloadBandwidthPrice)
	sectorPrice := sectorStoragePrice.Add(sectorBandwidthPrice)
	percentRemaining, _ := big.NewRat(0, 1).SetFrac(f.host.RenterFunds().Big(), f.metadata.TotalCost.Big()).Float64()

	if f.host.RenterFunds().Cmp(sectorPrice.Mul64(3)) < 0 || percentRemaining < minContractFundUploadThreshold {
		f.metadata.GoodForUpload = false
		f.reasons = append(f.reasons, fmt.Sprintf("contract has insufficient funds, GFU: %v -> false", f.initialGFU))
	}
	return f
}

func (f *hostFilter) withUpForRenewalFilter(cfg Config, blockHeight uint64) *hostFilter {
	if blockHeight+cfg.Contracts.RenewWindow/2 >= f.host.EndHeight() {
		f.metadata.GoodForUpload = false
		f.reasons = append(f.reasons, fmt.Sprintf("contract is up for renewal, GFU: %v -> false", f.initialGFU))
	}
	return f
}

func (f *hostFilter) withWhiteListFilter(cfg Config) *hostFilter {
	if len(cfg.Hosts.Whitelist) > 0 {
		var found bool
		for _, host := range cfg.Hosts.Whitelist {
			if f.host.IsHost(host) {
				found = true
				break
			}
		}
		if !found {
			f.metadata.GoodForUpload = false
			f.metadata.GoodForRenew = false
			f.reasons = append(f.reasons, fmt.Sprintf("host is not on whitelist, GFU: %v -> false, GFR: %v -> false", f.initialGFU, f.initialGFR))
		}
	}
	return f
}
