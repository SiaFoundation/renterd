package autopilot

import (
	"math"

	"go.sia.tech/renterd/bus"
	"go.sia.tech/renterd/hostdb"
	"go.sia.tech/renterd/internal/consensus"
	rhpv2 "go.sia.tech/renterd/rhp/v2"
	"go.sia.tech/renterd/worker"
	"go.sia.tech/siad/types"
)

const (
	// estimatedFileContractTransactionSetSize is the estimated blockchain size
	// of a transaction set between a renter and a host that contains a file
	// contract.
	estimatedFileContractTransactionSetSize = 2048
)

type contractor struct {
	ap *Autopilot
}

func newContractor(ap *Autopilot) *contractor {
	return &contractor{
		ap: ap,
	}
}

func (c *contractor) performContractMaintenance() error {
	// fetch consensus state
	cs, err := c.ap.bus.ConsensusState()
	if err != nil {
		return err
	}

	// don't perform any maintenance if we're not synced
	if !cs.Synced {
		return nil
	}

	// re-use same state and config in every iteration
	cfg := c.ap.store.Config()
	state := c.ap.store.State()

	// return early if no hosts are requested
	if cfg.Contracts.Hosts == 0 {
		return nil
	}

	// fetch our wallet address
	address, err := c.ap.bus.WalletAddress()
	if err != nil {
		return err
	}

	// run checks
	err = c.runContractChecks(cfg, cs.BlockHeight)
	if err != nil {
		return err
	}

	// figure out remaining funds
	spent, err := c.periodSpending()
	if err != nil {
		return err
	}
	var remaining types.Currency
	if cfg.Contracts.Allowance.Cmp(spent) > 0 {
		remaining = cfg.Contracts.Allowance.Sub(spent)
	}

	// run renewals
	renewed, err := c.runContractRenewals(cfg, state, &remaining, address, cs.BlockHeight)
	if err != nil {
		return err
	}

	// run formations
	formed, err := c.runContractFormations(cfg, state, &remaining, address)
	if err != nil {
		return err
	}

	// update contract set
	err = c.ap.updateDefaultContracts(renewed, formed)
	if err != nil {
		return err
	}

	return nil
}

func (c *contractor) runContractChecks(cfg Config, blockHeight uint64) error {
	// fetch all active contracts
	active, err := c.ap.bus.ActiveContracts(math.MaxUint64)
	if err != nil {
		return err
	}

	// loop variables
	ipNets := make(map[string]struct{})
	numActive := uint64(len(active))

	// run checks on the contracts individually
	for _, contract := range active {
		// fetch host from hostdb
		host, err := c.host(contract)
		if err != nil {
			return err
		}

		// fetch metadata
		metadata, err := c.ap.bus.ContractMetadata(contract.ID)
		if err != nil {
			return err
		}

		// apply host filters
		if result := hostFilter(
			cfg.isBlackListed,
			cfg.isWhiteListed,
			// TODO: isGouging,
			isLowScore(cfg, 0), // TODO: set threshold
			isMaxRevision,
			isOffline,
			isOutOfFunds(cfg, metadata),
			isRedundantIP(ipNets),
			isSuperfluous(cfg, &numActive),
			isUpForRenewal(cfg, blockHeight),
		)(host); result.filtered() {
			if transformed := metadata.Apply(result.transformer); transformed {
				err = c.ap.bus.UpdateContractMetadata(contract.ID, metadata)
				if err != nil {
					return err
				}
				// TODO: log the update
			} else {
				// TODO: log this event as it should not occur
			}
		}
	}

	return nil
}

func (c *contractor) runContractRenewals(cfg Config, s State, budget *types.Currency, renterAddress types.UnlockHash, blockHeight uint64) ([]worker.Contract, error) {
	// fetch all contracts that are up for renew
	toRenew, err := c.ap.renewableContracts(blockHeight + cfg.Contracts.RenewWindow)
	if err != nil {
		return nil, err
	}

	// perform the renewals
	var renewed []worker.Contract
	for _, renew := range toRenew {
		// TODO: break if autopilot was stopped

		// check our budget
		renterFunds, err := c.renewFundingEstimate(cfg, s, renew.ID, blockHeight)
		if budget.Cmp(renterFunds) < 0 {
			break
		}

		// derive the renter key
		renterKey := c.ap.deriveRenterKey(renew.HostKey)
		if err != nil {
			return nil, err
		}

		var hostCollateral types.Currency // TODO
		contract, err := c.renewContract(cfg, s, renew, renterKey, renterAddress, renterFunds, hostCollateral)
		if err != nil {
			// TODO: handle error properly, if the wallet ran out of outputs
			// here there's no point in renewing more contracts until a
			// block is mined, maybe we could/should wait for pending transactions?
			return nil, err
		}

		// update the budget
		*budget = budget.Sub(renterFunds)

		// persist the contract
		err = c.ap.bus.AddContract(contract)
		if err != nil {
			return nil, err
		}

		// persist the metadata
		err = c.ap.bus.UpdateContractMetadata(contract.ID(), bus.ContractMetadata{ParentID: renew.ID, TotalCost: renterFunds})
		if err != nil {
			return nil, err
		}

		// add to set
		renewed = append(renewed, worker.Contract{
			HostKey:   renew.HostKey,
			HostIP:    renew.HostIP,
			ID:        contract.ID(),
			RenterKey: renterKey,
		})
	}

	return renewed, nil
}

func (c *contractor) runContractFormations(cfg Config, s State, budget *types.Currency, renterAddress types.UnlockHash) ([]worker.Contract, error) {
	// fetch all active contracts
	active, err := c.ap.bus.ActiveContracts(math.MaxUint64)
	if err != nil {
		return nil, err
	}

	// fetch recommended txn fee
	fee, err := c.ap.bus.RecommendedFee()
	if err != nil {
		return nil, err
	}

	// calculate min/max contract funds
	allowance := cfg.Contracts.Allowance.Div64(cfg.Contracts.Hosts)
	maxInitialContractFunds := allowance.Div64(10) // TODO: arbitrary divisor
	minInitialContractFunds := allowance.Div64(20) // TODO: arbitrary divisor

	// form missing contracts
	var formed []worker.Contract
	missing := int(cfg.Contracts.Hosts) - len(active) // TODO: add leeway so we don't form hosts if we dip slightly under `needed` (?)
	canidates, _ := c.candidateHosts(cfg, missing)    // TODO: add leeway so we have more than enough canidates
	for h := 0; missing > 0 && h < len(canidates); h++ {
		// TODO: break if autopilot was stopped

		// fetch host IP
		candidate := canidates[h]
		host, err := c.ap.bus.Host(candidate)
		if err != nil {
			// TODO: log error
			continue
		}
		hostIP := host.NetAddress()

		// fetch host settings
		scan, err := c.ap.worker.RHPScan(host.PublicKey, hostIP)
		if err != nil {
			// TODO: log error
			continue
		}
		hostSettings := scan.Settings

		// check our budget
		txnFee := fee.Mul64(estimatedFileContractTransactionSetSize)
		renterFunds := c.initialContractFunding(hostSettings, txnFee, minInitialContractFunds, maxInitialContractFunds)
		if budget.Cmp(renterFunds) < 0 {
			break
		}

		// form contract
		renterKey := c.ap.deriveRenterKey(candidate)
		var hostCollateral types.Currency // TODO
		contract, err := c.formContract(cfg, s, candidate, hostIP, hostSettings, renterKey, renterAddress, renterFunds, hostCollateral)
		if err != nil {
			// TODO: handle error properly, if the wallet ran out of outputs
			// here there's no point in forming more contracts until a block
			// is mined, maybe we could/should wait for pending transactions?
			continue
		}

		// update the budget
		*budget = budget.Sub(renterFunds)

		// persist contract in store
		err = c.ap.bus.AddContract(contract)
		if err != nil {
			// TODO: log error
			continue
		}

		// persist the metadata
		err = c.ap.bus.UpdateContractMetadata(contract.ID(), bus.ContractMetadata{TotalCost: renterFunds})
		if err != nil {
			return nil, err
		}

		if err != nil {
			return nil, err
		}

		// add contract to contract set
		formed = append(formed, worker.Contract{
			HostKey:   candidate,
			HostIP:    hostIP,
			ID:        contract.ID(),
			RenterKey: renterKey,
		})

		missing--
	}

	return formed, nil
}

func (c *contractor) renewContract(cfg Config, s State, toRenew worker.Contract, renterKey consensus.PrivateKey, renterAddress types.UnlockHash, renterFunds, hostCollateral types.Currency) (rhpv2.Contract, error) {
	// handle contract locking
	revision, err := c.ap.bus.AcquireContractLock(toRenew.ID)
	if err != nil {
		return rhpv2.Contract{}, nil
	}
	defer c.ap.bus.ReleaseContractLock(toRenew.ID)

	// fetch host settings
	scan, err := c.ap.worker.RHPScan(toRenew.HostKey, toRenew.HostIP)
	if err != nil {
		return rhpv2.Contract{}, nil
	}

	// prepare the renewal
	endHeight := s.CurrentPeriod + cfg.Contracts.Period + cfg.Contracts.RenewWindow
	fc, cost, finalPayment, err := c.ap.worker.RHPPrepareRenew(revision, renterKey, toRenew.HostKey, renterFunds, renterAddress, hostCollateral, endHeight, scan.Settings)
	if err != nil {
		return rhpv2.Contract{}, nil
	}

	// fund the transaction
	txn := types.Transaction{FileContracts: []types.FileContract{fc}}
	toSign, parents, err := c.ap.bus.WalletFund(&txn, cost)
	if err != nil {
		_ = c.ap.bus.WalletDiscard(txn) // ignore error
		return rhpv2.Contract{}, err
	}

	// sign the transaction
	err = c.ap.bus.WalletSign(&txn, toSign, types.FullCoveredFields)
	if err != nil {
		_ = c.ap.bus.WalletDiscard(txn) // ignore error
		return rhpv2.Contract{}, err
	}

	// renew the contract
	txnSet := append(parents, txn)
	renewed, _, err := c.ap.worker.RHPRenew(renterKey, toRenew.HostKey, toRenew.HostIP, toRenew.ID, txnSet, finalPayment)
	if err != nil {
		_ = c.ap.bus.WalletDiscard(txn) // ignore error
		return rhpv2.Contract{}, err
	}
	return renewed, nil
}

func (c *contractor) formContract(cfg Config, s State, hostKey consensus.PublicKey, hostIP string, hostSettings rhpv2.HostSettings, renterKey consensus.PrivateKey, renterAddress types.UnlockHash, renterFunds, hostCollateral types.Currency) (rhpv2.Contract, error) {
	// prepare contract formation
	endHeight := s.CurrentPeriod + cfg.Contracts.Period + cfg.Contracts.RenewWindow
	fc, cost, err := c.ap.worker.RHPPrepareForm(renterKey, hostKey, renterFunds, renterAddress, hostCollateral, endHeight, hostSettings)
	if err != nil {
		return rhpv2.Contract{}, err
	}

	// fund the transaction
	txn := types.Transaction{FileContracts: []types.FileContract{fc}}
	toSign, parents, err := c.ap.bus.WalletFund(&txn, cost)
	if err != nil {
		_ = c.ap.bus.WalletDiscard(txn) // ignore error
		return rhpv2.Contract{}, err
	}

	// sign the transaction
	err = c.ap.bus.WalletSign(&txn, toSign, types.FullCoveredFields)
	if err != nil {
		_ = c.ap.bus.WalletDiscard(txn) // ignore error
		return rhpv2.Contract{}, err
	}

	// form the contract
	contract, _, err := c.ap.worker.RHPForm(renterKey, hostKey, hostIP, append(parents, txn))
	if err != nil {
		_ = c.ap.bus.WalletDiscard(txn) // ignore error
		return rhpv2.Contract{}, err
	}

	return contract, nil
}

// TODO
func (c *contractor) periodSpending() (types.Currency, error) {
	return types.ZeroCurrency, nil
}

func (c *contractor) initialContractFunding(settings rhpv2.HostSettings, txnFee, min, max types.Currency) types.Currency {
	if !max.IsZero() && min.Cmp(max) > 0 {
		panic("given min is larger than max") // developer error
	}

	funding := settings.ContractPrice.Add(txnFee).Mul64(10) // TODO arbitrary multiplier
	if !min.IsZero() && funding.Cmp(min) < 0 {
		return min
	}
	if !max.IsZero() && funding.Cmp(max) > 0 {
		return max
	}
	return funding
}

func (c *contractor) renewFundingEstimate(cfg Config, s State, cID types.FileContractID, blockHeight uint64) (types.Currency, error) {
	// fetch contract
	contract, err := c.ap.bus.ContractData(cID)
	if err != nil {
		return types.ZeroCurrency, err
	}

	// fetch host
	host, err := c.ap.bus.Host(contract.HostKey())
	if err != nil {
		return types.ZeroCurrency, err
	}

	// fetch host settings
	scan, err := c.ap.worker.RHPScan(contract.HostKey(), host.NetAddress())
	if err != nil {
		return types.ZeroCurrency, err
	}

	// estimate the cost of the current data stored
	dataStored := contract.Revision.ToTransaction().FileContractRevisions[0].NewFileSize
	storageCost := types.NewCurrency64(dataStored).Mul64(cfg.Contracts.Period).Mul(scan.Settings.StoragePrice)

	// loop over the contract history to figure out the amount of money spent
	var prevUploadSpending types.Currency
	var prevDownloadSpending types.Currency
	var prevFundAccountSpending types.Currency
	spendingHistory, err := c.ap.bus.SpendingHistory(cID, s.CurrentPeriod)
	if err != nil {
		return types.ZeroCurrency, err
	}
	for _, spending := range spendingHistory {
		prevUploadSpending = prevUploadSpending.Add(spending.Uploads)
		prevDownloadSpending = prevUploadSpending.Add(spending.Downloads)
		prevFundAccountSpending = prevUploadSpending.Add(spending.FundAccount)
	}

	// estimate the amount of data uploaded, sanity check with data stored
	//
	// TODO: estimate is not ideal because price can change, better would be to
	// look at the amount of data stored in the contract from the previous cycle
	prevUploadDataEstimate := prevUploadSpending
	if !scan.Settings.UploadBandwidthPrice.IsZero() {
		prevUploadDataEstimate = prevUploadDataEstimate.Div(scan.Settings.UploadBandwidthPrice)
	}
	if prevUploadDataEstimate.Cmp(types.NewCurrency64(dataStored)) > 0 {
		prevUploadDataEstimate = types.NewCurrency64(dataStored)
	}

	// estimate the
	// - upload cost: previous uploads + prev storage
	// - download cost: assumed to be the same
	// - fund acount cost: assumed to be the same
	newUploadsCost := prevUploadSpending.Add(prevUploadDataEstimate.Mul64(cfg.Contracts.Period).Mul(scan.Settings.StoragePrice))
	newDownloadsCost := prevDownloadSpending
	newFundAccountCost := prevFundAccountSpending

	// estimate the siafund fees
	//
	// NOTE: the transaction fees are not included in the siafunds estimate
	// because users are not charged siafund fees on money that doesn't go into
	// the file contract (and the transaction fee goes to the miners, not the
	// file contract).
	subTtotal := storageCost.Add(newUploadsCost).Add(newDownloadsCost).Add(newFundAccountCost).Add(scan.Settings.ContractPrice)
	siaFundFeeEstimate := types.Tax(types.BlockHeight(blockHeight), subTtotal)

	// estimate the txn fee
	txnFee, err := c.ap.bus.RecommendedFee()
	if err != nil {
		return types.ZeroCurrency, err
	}
	txnFeeEstimate := txnFee.Mul64(estimatedFileContractTransactionSetSize)

	// add them all up and then return the estimate plus 33% for error margin
	// and just general volatility of usage pattern.
	estimatedCost := subTtotal.Add(siaFundFeeEstimate).Add(txnFeeEstimate)
	estimatedCost = estimatedCost.Add(estimatedCost.Div64(3)) // TODO: arbitrary divisor

	// check for a sane minimum that is equal to the initial contract funding
	// but without an upper cap.
	initialContractFunds := cfg.Contracts.Allowance.Div64(cfg.Contracts.Hosts)
	minInitialContractFunds := initialContractFunds.Div64(20) // TODO: arbitrary divisor
	minimum := c.initialContractFunding(scan.Settings, txnFeeEstimate, minInitialContractFunds, types.ZeroCurrency)
	if estimatedCost.Cmp(minimum) < 0 {
		estimatedCost = minimum
	}
	return estimatedCost, nil
}

func (c *contractor) candidateHosts(cfg Config, wanted int) ([]consensus.PublicKey, error) {
	// fetch all candidate hosts
	hosts, err := c.ap.bus.CandidateHosts()
	if err != nil {
		return nil, err
	}

	// loop variables
	ipNets := make(map[string]struct{})

	// apply host filter
	filterFn := hostFilter(
		cfg.isWhiteListed,
		cfg.isBlackListed,
		// TODO: isGouging,
		isLowScore(cfg, 0), // TODO: set threshold
		isOffline,
		isRedundantIP(ipNets),
	)
	hosts = hosts[:0]
	for _, h := range hosts {
		if result := filterFn(host{h, rhpv2.Contract{}}); !result.filtered() {
			hosts = append(hosts, h)
		}
	}

	// update num wanted
	if wanted > len(hosts) {
		wanted = len(hosts)
	}

	// score each host
	scoreFn := HostScore(
		ageScore,
		collateralScore(cfg),
		interactionScore,
		settingsScore(cfg),
		uptimeScore,
		versionScore,
		// TODO: priceScore
	)
	scores := make([]float64, len(hosts))
	for i, h := range hosts {
		scores[i] = scoreFn(host{h, rhpv2.Contract{}})
	}

	// select hosts
	var selected []consensus.PublicKey
	for len(selected) < wanted {
		i := randSelectByWeight(scores)
		selected = append(selected, hosts[i].PublicKey)

		// remove selected host
		hosts[i], hosts = hosts[len(hosts)-1], hosts[:len(hosts)-1]
		scores[i], scores = scores[len(scores)-1], scores[:len(scores)-1]
	}
	return selected, nil
}

// host is a convenience type that bundles host and contract info
type host struct {
	hostdb.Host
	rhpv2.Contract
}

// TODO: bus should expose all data in single call
func (c *contractor) host(contract bus.Contract) (host, error) {
	h, err := c.ap.bus.Host(contract.HostKey)
	if err != nil {
		return host{}, err
	}

	data, err := c.ap.bus.ContractData(contract.ID)
	if err != nil {
		return host{}, err
	}

	return host{h, data}, nil
}
