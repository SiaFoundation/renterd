package autopilot

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"time"

	"go.sia.tech/renterd/bus"
	"go.sia.tech/renterd/internal/consensus"
	rhpv2 "go.sia.tech/renterd/rhp/v2"
	"go.sia.tech/siad/types"
	"go.uber.org/zap"
)

const (
	// estimatedFileContractTransactionSetSize is the estimated blockchain size
	// of a transaction set between a renter and a host that contains a file
	// contract.
	estimatedFileContractTransactionSetSize = 2048

	contractLockingDurationRenew = 30 * time.Second

	// leewayPctCandidateHosts is the leeway we apply when fetching candidate
	// hosts, we fetch ~10% more than required
	leewayPctCandidateHosts = .1

	// leewayPctRequiredContracts is the leeway we apply on the amount of
	// contracts the config dictates we should have, this leeway is negative
	// which means that we'll only form new contracts if the number of active
	// contracts dips below 87.5% of the required contracts
	leewayPctRequiredContracts = -.125
)

type (
	contractor struct {
		ap     *Autopilot
		logger *zap.SugaredLogger

		blockHeight   uint64
		currentPeriod uint64
	}
)

func newContractor(ap *Autopilot) *contractor {
	return &contractor{
		ap:     ap,
		logger: ap.logger.Named("contractor"),
	}
}

func (c *contractor) applyConsensusState(cfg Config, state bus.ConsensusState) {
	c.blockHeight = state.BlockHeight
	if c.blockHeight > c.currentPeriod+cfg.Contracts.Period {
		c.currentPeriod += cfg.Contracts.Period
	}
}

func (c *contractor) contractSpending(id types.FileContractID, contracts []bus.Contract) (bus.ContractSpending, error) {
	// build a map
	cmap := make(map[string]bus.Contract)
	for _, contract := range contracts {
		cmap[contract.ID().String()] = contract
	}

	// fetch contract chain
	curr, exists := cmap[id.String()]
	if !exists {
		return bus.ContractSpending{}, fmt.Errorf("contract with id '%v' not found", id)
	}

	// no history
	if curr.RenewedFrom == (types.FileContractID{}) {
		return curr.Spending, nil
	}

	// compute total spending
	total := curr.Spending
	for exists {
		if curr, exists = cmap[curr.RenewedFrom.String()]; exists {
			total = total.Add(curr.Spending)
		}
	}
	return total, nil
}

func (c *contractor) currentPeriodSpending() (types.Currency, error) {
	// fetch all contracts
	contracts, err := c.ap.bus.Contracts()
	if err != nil {
		return types.ZeroCurrency, err
	}

	// filter contracts in the current period
	filtered := contracts[:0]
	for _, contract := range contracts {
		if contract.EndHeight() <= c.currentPeriod {
			filtered = append(filtered, contract)
		}
	}

	// calculate the money spent
	var spent types.Currency
	for _, contract := range filtered {
		remaining := contract.RenterFunds()
		if remaining.Cmp(contract.TotalCost) <= 0 {
			spent = spent.Add(contract.TotalCost.Sub(remaining))
		}

		// TODO: log event where remaining is highter than total cost
	}

	return spent, nil
}

func (c *contractor) isStopped() bool {
	select {
	case <-c.ap.stopChan:
		return true
	default:
		return false
	}
}

func (c *contractor) performContractMaintenance(cfg Config) error {
	// return early if no hosts are requested
	if cfg.Contracts.Hosts == 0 {
		return nil
	}

	// fetch our wallet address
	address, err := c.ap.bus.WalletAddress()
	if err != nil {
		return err
	}

	// fetch all active contracts
	active, err := c.ap.bus.Contracts()
	if err != nil {
		return err
	}

	// run checks
	toRenew, toDelete, err := c.runContractChecks(cfg, active)
	if err != nil {
		return fmt.Errorf("failed to run contract checks, err: %v", err)
	}

	// delete contracts
	if len(toDelete) > 0 {
		err = c.ap.bus.DeleteContracts(toDelete)
		if err != nil {
			return fmt.Errorf("failed to delete contracts, err: %v", err)
		}
	}

	// find out how much we spent in the current period
	spent, err := c.currentPeriodSpending()
	if err != nil {
		return err
	}

	// figure out remaining funds
	var remaining types.Currency
	if cfg.Contracts.Allowance.Cmp(spent) > 0 {
		remaining = cfg.Contracts.Allowance.Sub(spent)
	}

	// run renewals
	renewed, err := c.runContractRenewals(cfg, &remaining, address, toRenew)
	if err != nil {
		return fmt.Errorf("failed to renew contracts, err: %v", err)
	}

	// run formations
	formed, err := c.runContractFormations(cfg, &remaining, address)
	if err != nil {
		return fmt.Errorf("failed to form contracts, err: %v", err)
	}

	// update contract set
	err = c.ap.updateDefaultContracts(active, toRenew, renewed, formed, toDelete)
	if err != nil {
		return fmt.Errorf("failed to update default contracts, err: %v", err)
	}

	return nil
}

func (c *contractor) runContractChecks(cfg Config, contracts []bus.Contract) ([]bus.Contract, []types.FileContractID, error) {
	// collect contracts to renew and to delete
	toDelete := make([]types.FileContractID, 0, len(contracts))
	toRenew := make([]bus.Contract, 0, len(contracts))

	// create a new ip filter
	f := newIPFilter()

	// state variables
	contractIds := make([]types.FileContractID, 0, len(contracts))
	contractSizes := make(map[types.FileContractID]uint64)
	renewIndices := make(map[types.FileContractID]int)

	// fetch gouging settings
	gs, err := c.ap.bus.GougingSettings()
	if err != nil {
		return nil, nil, err
	}

	// fetch redundancy settings
	rs, err := c.ap.bus.RedundancySettings()
	if err != nil {
		return nil, nil, err
	}

	// check every active contract
	for _, contract := range contracts {
		// fetch host from hostdb
		host, err := c.ap.bus.Host(contract.HostKey())
		if err != nil {
			c.logger.Errorw(
				fmt.Sprintf("missing host, err: %v", err),
				"hk", contract.HostKey,
			)
			continue
		}

		// fetch contract from contract store
		contractData, err := c.ap.bus.Contract(contract.ID())
		if err != nil {
			c.logger.Errorw(
				fmt.Sprintf("missing contract, err: %v", err),
				"hk", contract.HostKey,
				"fcid", contract.ID(),
			)
			continue
		}

		// decide whether the host is still good
		usable, reasons := isUsableHost(cfg, gs, rs, f, Host{host})
		if !usable {
			c.logger.Infow(
				"unusable host",
				"hk", host.PublicKey,
				"fcid", contract.ID(),
				"reasons", reasons,
			)
			toDelete = append(toDelete, contract.ID())
			continue
		}

		// decide whether the contract is still good
		usable, renewable, reasons := isUsableContract(cfg, Host{host}, contractData, c.blockHeight)
		if !usable {
			c.logger.Infow(
				"unusable contract",
				"hk", host.PublicKey,
				"fcid", contract.ID(),
				"reasons", reasons,
				"renewable", renewable,
			)
			if !renewable {
				toDelete = append(toDelete, contract.ID())
				continue
			} else {
				renewIndices[contract.ID()] = len(toRenew)
				toRenew = append(toRenew, contract)
			}
		}

		// keep track of file size
		contractIds = append(contractIds, contract.ID())
		contractSizes[contract.ID()] = contractData.Revision.NewFileSize
	}

	// apply active contract limit
	active := len(contracts) - len(toDelete)
	if active > int(cfg.Contracts.Hosts) {
		// sort by contract size
		sort.Slice(contractIds, func(i, j int) bool {
			return contractSizes[contractIds[i]] < contractSizes[contractIds[j]]
		})
		for i := 0; i < active-int(cfg.Contracts.Hosts); i++ {
			// remove from renewal list if necessary
			if index, exists := renewIndices[contractIds[i]]; exists {
				toRenew[index] = toRenew[len(toRenew)-1]
				toRenew = toRenew[:len(toRenew)-1]
			}
			toDelete = append(toDelete, contractIds[i])
		}
	}

	return toRenew, toDelete, nil
}

func (c *contractor) runContractRenewals(cfg Config, budget *types.Currency, renterAddress types.UnlockHash, toRenew []bus.Contract) ([]bus.Contract, error) {
	renewed := make([]bus.Contract, 0, len(toRenew))

	// log contracts renewed
	c.logger.Debugw(
		"renewing contracts initiated",
		"torenew", len(toRenew),
		"budget", budget.HumanString(),
	)
	defer func() {
		c.logger.Debugw(
			"renewing contracts done",
			"renewed", len(renewed),
			"budget", budget.HumanString(),
		)
	}()

	// perform the renewals
	for _, renew := range toRenew {
		// break if the contractor was stopped
		if c.isStopped() {
			break
		}

		// check our budget
		renterFunds, err := c.renewFundingEstimate(cfg, renew.ID())
		if err != nil {
			return nil, fmt.Errorf("could not get renew funding estimate, err: %v", err)
		}
		if budget.Cmp(renterFunds) < 0 {
			c.logger.Debugw(
				"insufficient budget",
				"budget", budget.HumanString(),
				"needed", renterFunds.HumanString(),
				"renewal", true,
			)
			break
		}

		// derive the renter key
		renterKey := c.ap.deriveRenterKey(renew.HostKey())

		var hostCollateral types.Currency // TODO
		contract, err := c.renewContract(cfg, renew, renterKey, renterAddress, renterFunds, hostCollateral)
		if err != nil {
			// TODO: keep track of consecutive failures and break at some point
			// TODO: log error
			continue
		}

		// update the budget
		*budget = budget.Sub(renterFunds)

		// persist the contract
		renewedContract, err := c.ap.bus.AddRenewedContract(contract, renterFunds, c.blockHeight, renew.ID())
		if err != nil {
			c.logger.Errorw(
				fmt.Sprintf("renewal failed to persist, err: %v", err),
				"hk", renew.HostKey(),
				"fcid", renew.ID(),
			)
			return nil, err
		}

		// add to renewed set
		renewed = append(renewed, renewedContract)
	}

	return renewed, nil
}

func (c *contractor) runContractFormations(cfg Config, budget *types.Currency, renterAddress types.UnlockHash) ([]bus.Contract, error) {
	// fetch all active contracts
	active, err := c.ap.bus.Contracts()
	if err != nil {
		return nil, err
	}

	// calculate how many contracts we are missing
	required := addLeeway(cfg.Contracts.Hosts, leewayPctRequiredContracts)
	missing := int(required) - len(active)
	if missing <= 0 {
		return nil, nil
	}

	// fetch recommended txn fee
	fee, err := c.ap.bus.RecommendedFee()
	if err != nil {
		return nil, err
	}

	// fetch candidate hosts
	canidates, _ := c.candidateHosts(cfg, addLeeway(uint64(missing), leewayPctCandidateHosts))

	// calculate min/max contract funds
	allowance := cfg.Contracts.Allowance.Div64(cfg.Contracts.Hosts)
	maxInitialContractFunds := allowance.Div64(10) // TODO: arbitrary divisor
	minInitialContractFunds := allowance.Div64(20) // TODO: arbitrary divisor

	// form missing contracts
	var formed []bus.Contract

	// log contracts formed
	c.logger.Debugw(
		"forming contracts initiated",
		"active", len(active),
		"required", cfg.Contracts.Hosts,
		"missing", missing,
		"budget", budget.HumanString(),
	)
	defer func() {
		c.logger.Debugw(
			"forming contracts done",
			"formed", len(formed),
			"budget", budget.HumanString(),
		)
	}()

	for h := 0; missing > 0 && h < len(canidates); h++ {
		// break if the contractor was stopped
		if c.isStopped() {
			break
		}

		// fetch host
		candidate := canidates[h]
		host, err := c.ap.bus.Host(candidate)
		if err != nil {
			c.logger.Errorw(
				fmt.Sprintf("missing host, err: %v", err),
				"hk", candidate,
			)
			continue
		}

		// fetch host settings
		scan, err := c.ap.worker.RHPScan(candidate, host.NetAddress(), 0)
		if err != nil {
			c.logger.Debugw(
				fmt.Sprintf("failed scan, err: %v", err),
				"hk", candidate,
			)
			continue
		}
		settings := scan.Settings

		// check our budget
		txnFee := fee.Mul64(estimatedFileContractTransactionSetSize)
		renterFunds := c.initialContractFunding(settings, txnFee, minInitialContractFunds, maxInitialContractFunds)
		if budget.Cmp(renterFunds) < 0 {
			c.logger.Debugw(
				"insufficient budget",
				"budget", budget.HumanString(),
				"needed", renterFunds.HumanString(),
				"renewal", false,
			)
			break
		}

		// calculate the host collateral
		hostCollateral, err := calculateHostCollateral(cfg, settings, renterFunds, txnFee)
		if err != nil {
			// TODO: keep track of consecutive failures and break at some point
			c.logger.Errorw(
				fmt.Sprintf("failed contract formation, err : %v", err),
				"hk", candidate,
			)
			continue
		}

		// form contract
		renterKey := c.ap.deriveRenterKey(candidate)
		contract, err := c.formContract(cfg, candidate, host.NetAddress(), settings, renterKey, renterAddress, renterFunds, hostCollateral)
		if err != nil {
			// TODO: keep track of consecutive failures and break at some point
			// TODO: log error
			continue
		}
		missing--

		// update the budget
		*budget = budget.Sub(renterFunds)

		// persist contract in store
		formedContract, err := c.ap.bus.AddContract(contract, renterFunds, c.blockHeight)
		if err != nil {
			c.logger.Errorw(
				fmt.Sprintf("new contract failed to persist, err: %v", err),
				"hk", candidate,
			)
			continue
		}

		// add contract to contract set
		formed = append(formed, formedContract)
		missing--
	}

	return formed, nil
}

func (c *contractor) renewContract(cfg Config, toRenew bus.Contract, renterKey consensus.PrivateKey, renterAddress types.UnlockHash, renterFunds, hostCollateral types.Currency) (rhpv2.ContractRevision, error) {
	// handle contract locking
	revision, err := c.ap.bus.AcquireContract(toRenew.ID(), contractLockingDurationRenew)
	if err != nil {
		return rhpv2.ContractRevision{}, nil
	}
	defer c.ap.bus.ReleaseContract(toRenew.ID())

	// fetch host settings
	scan, err := c.ap.worker.RHPScan(toRenew.HostKey(), toRenew.HostIP, 0)
	if err != nil {
		c.logger.Debugw(
			fmt.Sprintf("failed scan, err: %v", err),
			"hk", toRenew.HostKey(),
		)
		return rhpv2.ContractRevision{}, nil
	}

	// prepare the renewal
	endHeight := c.currentPeriod + cfg.Contracts.Period + cfg.Contracts.RenewWindow
	fc, cost, finalPayment, err := c.ap.worker.RHPPrepareRenew(revision, renterKey, toRenew.HostKey(), renterFunds, renterAddress, endHeight, scan.Settings)
	if err != nil {
		return rhpv2.ContractRevision{}, nil
	}

	// fund the transaction
	txn := types.Transaction{FileContracts: []types.FileContract{fc}}
	toSign, parents, err := c.ap.bus.WalletFund(&txn, cost)
	if err != nil {
		_ = c.ap.bus.WalletDiscard(txn) // ignore error
		return rhpv2.ContractRevision{}, err
	}

	// sign the transaction
	err = c.ap.bus.WalletSign(&txn, toSign, types.FullCoveredFields)
	if err != nil {
		_ = c.ap.bus.WalletDiscard(txn) // ignore error
		return rhpv2.ContractRevision{}, err
	}

	// renew the contract
	txnSet := append(parents, txn)
	renewed, _, err := c.ap.worker.RHPRenew(renterKey, toRenew.HostKey(), toRenew.HostIP, toRenew.ID(), txnSet, finalPayment)
	if err != nil {
		_ = c.ap.bus.WalletDiscard(txn) // ignore error
		return rhpv2.ContractRevision{}, err
	}
	return renewed, nil
}

func (c *contractor) formContract(cfg Config, hostKey consensus.PublicKey, hostIP string, hostSettings rhpv2.HostSettings, renterKey consensus.PrivateKey, renterAddress types.UnlockHash, renterFunds, hostCollateral types.Currency) (rhpv2.ContractRevision, error) {
	// prepare contract formation
	endHeight := c.currentPeriod + cfg.Contracts.Period + cfg.Contracts.RenewWindow
	fc, cost, err := c.ap.worker.RHPPrepareForm(renterKey, hostKey, renterFunds, renterAddress, hostCollateral, endHeight, hostSettings)
	if err != nil {
		return rhpv2.ContractRevision{}, err
	}

	// fund the transaction
	txn := types.Transaction{FileContracts: []types.FileContract{fc}}
	toSign, parents, err := c.ap.bus.WalletFund(&txn, cost)
	if err != nil {
		_ = c.ap.bus.WalletDiscard(txn) // ignore error
		return rhpv2.ContractRevision{}, err
	}

	// sign the transaction
	err = c.ap.bus.WalletSign(&txn, toSign, types.FullCoveredFields)
	if err != nil {
		_ = c.ap.bus.WalletDiscard(txn) // ignore error
		return rhpv2.ContractRevision{}, err
	}

	// form the contract
	contract, _, err := c.ap.worker.RHPForm(renterKey, hostKey, hostIP, append(parents, txn))
	if err != nil {
		_ = c.ap.bus.WalletDiscard(txn) // ignore error
		return rhpv2.ContractRevision{}, err
	}

	return contract, nil
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

func (c *contractor) renewFundingEstimate(cfg Config, id types.FileContractID) (types.Currency, error) {
	// fetch contract
	contract, err := c.ap.bus.Contract(id)
	if err != nil {
		c.logger.Errorw(
			fmt.Sprintf("missing contract, err: %v", err),
			"hk", contract.HostKey,
			"fcid", contract.ID(),
		)
		return types.ZeroCurrency, err
	}

	// fetch host
	host, err := c.ap.bus.Host(contract.HostKey())
	if err != nil {
		c.logger.Errorw(
			fmt.Sprintf("missing host, err: %v", err),
			"hk", contract.HostKey,
		)
		return types.ZeroCurrency, err
	}

	// fetch host settings
	scan, err := c.ap.worker.RHPScan(contract.HostKey(), host.NetAddress(), 0)
	if err != nil {
		c.logger.Debugw(
			fmt.Sprintf("failed scan, err: %v", err),
			"hk", contract.HostKey(),
		)
		return types.ZeroCurrency, err
	}

	// estimate the cost of the current data stored
	dataStored := contract.Revision.ToTransaction().FileContractRevisions[0].NewFileSize
	storageCost := types.NewCurrency64(dataStored).Mul64(cfg.Contracts.Period).Mul(scan.Settings.StoragePrice)

	// fetch the spending of the contract we want to renew.
	prevSpending := contract.ContractMetadata.Spending

	// estimate the amount of data uploaded, sanity check with data stored
	//
	// TODO: estimate is not ideal because price can change, better would be to
	// look at the amount of data stored in the contract from the previous cycle
	prevUploadDataEstimate := prevSpending.Uploads
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
	newUploadsCost := prevSpending.Uploads.Add(prevUploadDataEstimate.Mul64(cfg.Contracts.Period).Mul(scan.Settings.StoragePrice))
	newDownloadsCost := prevSpending.Downloads
	newFundAccountCost := prevSpending.FundAccount

	// estimate the siafund fees
	//
	// NOTE: the transaction fees are not included in the siafunds estimate
	// because users are not charged siafund fees on money that doesn't go into
	// the file contract (and the transaction fee goes to the miners, not the
	// file contract).
	subTtotal := storageCost.Add(newUploadsCost).Add(newDownloadsCost).Add(newFundAccountCost).Add(scan.Settings.ContractPrice)
	siaFundFeeEstimate := types.Tax(types.BlockHeight(c.blockHeight), subTtotal)

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

func (c *contractor) candidateHosts(cfg Config, wanted uint64) ([]consensus.PublicKey, error) {
	// fetch all contracts
	active, err := c.ap.bus.Contracts()
	if err != nil {
		return nil, err
	}

	// fetch gouging settings
	gs, err := c.ap.bus.GougingSettings()
	if err != nil {
		return nil, err
	}

	// fetch redundancy settings
	rs, err := c.ap.bus.RedundancySettings()
	if err != nil {
		return nil, err
	}

	// build a map
	used := make(map[string]bool)
	for _, contract := range active {
		used[contract.HostKey().String()] = true
	}

	// create IP filter
	ipFilter := newIPFilter()

	// fetch all hosts
	hosts, err := c.ap.bus.AllHosts()
	if err != nil {
		return nil, err
	}

	// filter unusable hosts
	filtered := hosts[:0]
	for _, h := range hosts {
		if used[h.PublicKey.String()] {
			continue
		}

		if usable, _ := isUsableHost(cfg, gs, rs, ipFilter, Host{h}); usable {
			filtered = append(filtered, h)
		}
	}

	// score each host
	scores := make([]float64, 0, len(filtered))
	scored := filtered[:0]
	for _, host := range filtered {
		score := hostScore(cfg, Host{host})
		if score == 0 {
			// TODO: should not happen at this point, log this event
			continue
		}

		scores = append(scores, score)
		scored = append(scored, host)
	}

	// update num wanted
	if wanted > uint64(len(scores)) {
		wanted = uint64(len(scores))
	}

	// select hosts
	var selected []consensus.PublicKey
	for uint64(len(selected)) < wanted && len(scored) > 0 {
		i := randSelectByWeight(scores)
		selected = append(selected, scored[i].PublicKey)

		// remove selected host
		scored[i], scored = scored[len(scored)-1], scored[:len(scored)-1]
		scores[i], scores = scores[len(scores)-1], scores[:len(scores)-1]
	}
	return selected, nil
}

func calculateHostCollateral(cfg Config, settings rhpv2.HostSettings, renterFunds, txnFee types.Currency) (types.Currency, error) {
	// check underflow
	if settings.ContractPrice.Add(txnFee).Cmp(renterFunds) > 0 {
		return types.ZeroCurrency, errors.New("contract price + fees exceeds funding")
	}

	// calculate the host collateral
	renterPayout := renterFunds.Sub(settings.ContractPrice).Sub(txnFee)
	maxStorage := renterPayout.Div(settings.StoragePrice)
	expectedStorage := cfg.Contracts.Storage / cfg.Contracts.Hosts
	hostCollateral := maxStorage.Mul(settings.Collateral)

	// don't add more than 5x the collateral for the expected storage to save on fees
	maxRenterCollateral := settings.Collateral.Mul64(cfg.Contracts.Period).Mul64(expectedStorage).Mul64(5)
	if hostCollateral.Cmp(maxRenterCollateral) > 0 {
		hostCollateral = maxRenterCollateral
	}

	// don't add more collateral than the host would allow
	if hostCollateral.Cmp(settings.MaxCollateral) > 0 {
		hostCollateral = settings.MaxCollateral
	}

	return hostCollateral, nil
}

func addLeeway(n uint64, pct float64) uint64 {
	if pct < -1 || pct > 1 {
		panic("given leeway percent is out of bounds")
	}
	return uint64(float64(n) + math.Ceil(float64(n)*pct))
}
