package autopilot

import (
	"math"
	"math/big"

	"go.sia.tech/renterd/bus"
	rhpv2 "go.sia.tech/renterd/rhp/v2"
	"go.sia.tech/siad/modules"
	"go.sia.tech/siad/types"
)

const (
	// minContractFundUploadThreshold is the percentage of contract funds
	// remaining at which the contract gets marked as not good for upload
	minContractFundUploadThreshold = float64(0.05) // 5%
)

// isUsableHost returns whether the given host is usable along with a list of
// reasons why it was deemed unusable.
func isUsableHost(cfg Config, f *ipFilter, h Host) (bool, []string) {
	var reasons []string

	if !cfg.isWhitelisted(h) {
		reasons = append(reasons, "host is not on whitelist")
	}
	if cfg.isBlacklisted(h) {
		reasons = append(reasons, "host is on blacklist")
	}
	if !h.IsOnline() {
		reasons = append(reasons, "host is not online")
	}
	if f.isRedundantIP(h) {
		reasons = append(reasons, "host IP is redundant")
	}
	// TODO: isLowScore

	return len(reasons) == 0, reasons
}

// isUsableContract returns whether the given contract is usable and whether it
// can be renewed, along with a list of reasons why it was deemed unusable.
func isUsableContract(cfg Config, h Host, c rhpv2.Contract, m bus.ContractMetadata, bh uint64) (bool, bool, []string) {
	var reasons []string
	renewable := true

	if isOutOfFunds(cfg, h, c, m) {
		reasons = append(reasons, "contract is out of funds")
	}
	if isUpForRenewal(cfg, c, bh) {
		reasons = append(reasons, "contract is up for renewal")
	}
	if isMaxRevision(c) {
		reasons = append(reasons, "contract reached max revision number")
		renewable = false
	}

	return len(reasons) == 0, renewable, reasons
}

func isMaxRevision(c rhpv2.Contract) bool {
	return c.Revision.NewRevisionNumber == math.MaxUint64
}

func isOutOfFunds(cfg Config, h Host, c rhpv2.Contract, m bus.ContractMetadata) bool {
	settings, _, found := h.LastKnownSettings()
	if !found {
		return false
	}

	blockBytes := types.NewCurrency64(modules.SectorSize * cfg.Contracts.Period)
	sectorStoragePrice := settings.StoragePrice.Mul(blockBytes)
	sectorUploadBandwidthPrice := settings.UploadBandwidthPrice.Mul64(modules.SectorSize)
	sectorDownloadBandwidthPrice := settings.DownloadBandwidthPrice.Mul64(modules.SectorSize)
	sectorBandwidthPrice := sectorUploadBandwidthPrice.Add(sectorDownloadBandwidthPrice)
	sectorPrice := sectorStoragePrice.Add(sectorBandwidthPrice)
	percentRemaining, _ := big.NewRat(0, 1).SetFrac(c.RenterFunds().Big(), m.TotalCost.Big()).Float64()

	return c.RenterFunds().Cmp(sectorPrice.Mul64(3)) < 0 || percentRemaining < minContractFundUploadThreshold
}

func isUpForRenewal(cfg Config, c rhpv2.Contract, blockHeight uint64) bool {
	return blockHeight+cfg.Contracts.RenewWindow/2 >= c.EndHeight()
}

func (cfg Config) isBlacklisted(h Host) bool {
	for _, host := range cfg.Hosts.Blacklist {
		if h.IsHost(host) {
			return true
		}
	}
	return false
}

func (cfg Config) isWhitelisted(h Host) bool {
	if len(cfg.Hosts.Whitelist) > 0 {
		var found bool
		for _, host := range cfg.Hosts.Whitelist {
			if h.IsHost(host) {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}
