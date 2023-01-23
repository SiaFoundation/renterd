package autopilot

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	"go.sia.tech/core/consensus"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/hostdb"
	rhpv2 "go.sia.tech/renterd/rhp/v2"
	"go.sia.tech/renterd/wallet"
	"go.uber.org/zap"
)

const (
	// contractHostTimeout is the amount of time we wait to receive the latest
	// revision from the host
	contractHostTimeout = 30 * time.Second

	// estimatedFileContractTransactionSetSize is the estimated blockchain size
	// of a transaction set between a renter and a host that contains a file
	// contract.
	estimatedFileContractTransactionSetSize = 2048

	// leewayPctCandidateHosts is the leeway we apply when fetching candidate
	// hosts, we fetch ~10% more than required
	leewayPctCandidateHosts = 1.1

	// leewayPctRequiredContracts is the leeway we apply on the amount of
	// contracts the config dictates we should have, we'll only form new
	// contracts if the number of active contracts dips below 87.5% of the
	// required contracts
	leewayPctRequiredContracts = 0.875

	// maxInitialContractFundingDivisor and minInitialContractFundingDivisor
	// define a range we use when calculating the initial contract funding
	maxInitialContractFundingDivisor = uint64(10)
	minInitialContractFundingDivisor = uint64(20)
)

type (
	contractor struct {
		ap     *Autopilot
		logger *zap.SugaredLogger

		maintenanceTxnID types.TransactionID

		mu         sync.Mutex
		currPeriod uint64
	}

	contractInfo struct {
		contract api.Contract
		settings rhpv2.HostSettings
	}
)

func newContractor(ap *Autopilot) *contractor {
	return &contractor{
		ap:     ap,
		logger: ap.logger.Named("contractor"),
	}
}

func (c *contractor) currentPeriod() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.currPeriod
}

func (c *contractor) updateCurrentPeriod(cfg api.AutopilotConfig, cs api.ConsensusState) uint64 {
	c.mu.Lock()
	defer func(prevPeriod uint64) {
		if c.currPeriod != prevPeriod {
			c.logger.Debugf("updated current period, %d->%d", prevPeriod, c.currPeriod)
		}
		c.mu.Unlock()
	}(c.currPeriod)

	if c.currPeriod == 0 {
		c.currPeriod = cs.BlockHeight
	} else if cs.BlockHeight >= c.currPeriod+cfg.Contracts.Period {
		c.currPeriod += cfg.Contracts.Period
	}
	return c.currPeriod
}

func (c *contractor) contractSpending(contract api.Contract, currentPeriod uint64) (api.ContractSpending, error) {
	ancestors, err := c.ap.bus.AncestorContracts(contract.ID, currentPeriod)
	if err != nil {
		return api.ContractSpending{}, err
	}
	// compute total spending
	total := contract.Spending
	for _, ancestor := range ancestors {
		total = total.Add(ancestor.Spending)
	}
	return total, nil
}

func (c *contractor) currentPeriodSpending(contracts []api.Contract, currentPeriod uint64) (types.Currency, error) {
	totalCosts := make(map[types.FileContractID]types.Currency)
	for _, c := range contracts {
		totalCosts[c.ID] = c.TotalCost
	}

	// filter contracts in the current period
	var filtered []api.Contract
	c.mu.Lock()
	for _, rev := range contracts {
		if rev.EndHeight() <= currentPeriod {
			filtered = append(filtered, rev)
		}
	}
	c.mu.Unlock()

	// calculate the money spent
	var spent types.Currency
	for _, rev := range filtered {
		remaining := rev.RenterFunds()
		totalCost := totalCosts[rev.ID]
		if remaining.Cmp(totalCost) <= 0 {
			spent = spent.Add(totalCost.Sub(remaining))
		} else {
			c.logger.DPanicw("sanity check failed", "remaining", remaining, "totalcost", totalCost)
		}
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

func (c *contractor) performContractMaintenance(cfg api.AutopilotConfig, cs api.ConsensusState) error {
	if !cs.Synced {
		return nil // skip contract maintenance if we're not synced
	}

	c.logger.Info("performing contract maintenance")

	// update the current period.
	currentPeriod := c.updateCurrentPeriod(cfg, cs)
	blockHeight := cs.BlockHeight

	// return early if no hosts are requested
	if cfg.Contracts.Amount == 0 {
		c.logger.Debug("no hosts requested, skipping contract maintenance")
		return nil
	}

	// fetch our wallet address
	address, err := c.ap.bus.WalletAddress()
	if err != nil {
		return err
	}

	// fetch all active contracts from the worker
	start := time.Now()
	resp, err := c.ap.worker.ActiveContracts(contractHostTimeout)
	if err != nil {
		return err
	}
	if resp.Error != "" {
		c.logger.Error(resp.Error)
	}
	c.logger.Debugf("fetched %d active contracts, took %v", len(resp.Contracts), time.Since(start))
	active := resp.Contracts

	// fetch gouging settings
	gs, err := c.ap.bus.GougingSettings()
	if err != nil {
		return err
	}

	// fetch redundancy settings
	rs, err := c.ap.bus.RedundancySettings()
	if err != nil {
		return err
	}

	// run checks
	toDelete, toIgnore, toRefresh, toRenew, err := c.runContractChecks(cfg, blockHeight, gs, rs, active)
	if err != nil {
		return fmt.Errorf("failed to run contract checks, err: %v", err)
	}

	// delete contracts
	if len(toDelete) > 0 {
		c.logger.Debugf("deleting %d contracts: %+v", len(toDelete), toDelete)
		if err := c.ap.bus.DeleteContracts(toDelete); err != nil {
			c.logger.Errorf("failed to delete contracts, err: %v", err)
			// continue - a failure to delete should not prevent us from renewing or forming new contracts
		}
	}

	// find out how much we spent in the current period
	spent, err := c.currentPeriodSpending(active, currentPeriod)
	if err != nil {
		return err
	}

	// figure out remaining funds
	var remaining types.Currency
	if cfg.Contracts.Allowance.Cmp(spent) > 0 {
		remaining = cfg.Contracts.Allowance.Sub(spent)
	}

	// run renewals + refreshes
	renewed, err := c.runContractRenewals(cfg, blockHeight, currentPeriod, &remaining, address, toRefresh, toRenew)
	if err != nil {
		c.logger.Errorf("failed to renew contracts, err: %v", err)
		// continue - failing to renew contracts should not prevent us from forming new contracts
	}

	// build the new contract set (excluding formed contracts)
	contractset := buildContractSet(active, toDelete, toIgnore, toRefresh, toRenew, renewed)
	numContracts := uint64(len(contractset))

	// check if we need to form contracts and add them to the contract set
	var formed []types.FileContractID
	if numContracts < addLeeway(cfg.Contracts.Amount, leewayPctRequiredContracts) {
		if formed, err = c.runContractFormations(cfg, active, cfg.Contracts.Amount-numContracts, blockHeight, currentPeriod, &remaining, address); err != nil {
			c.logger.Errorf("failed to form contracts, err: %v", err)
			// continue - failing to form contracts should not prevent us from updating the contract set
		}
	}
	contractset = append(contractset, formed...)

	c.logger.Debugw(
		"contracts after maintenance",
		"formed", len(formed),
		"renewed", len(renewed),
		"contractset", len(contractset),
	)

	// update contract set
	if len(contractset) < int(rs.TotalShards) {
		c.logger.Warnf("contractset does not have enough contracts, %v<%v", len(contractset), rs.TotalShards)
	}
	return c.ap.bus.SetContractSet(cfg.Contracts.Set, contractset)
}

func (c *contractor) performWalletMaintenance(cfg api.AutopilotConfig, cs api.ConsensusState) error {
	c.logger.Info("performing wallet maintenance")
	b := c.ap.bus
	l := c.logger

	// no contracts - nothing to do
	if cfg.Contracts.Amount == 0 {
		l.Debug("wallet maintenance skipped, no contracts wanted")
		return nil
	}

	// no allowance - nothing to do
	if cfg.Contracts.Allowance.IsZero() {
		l.Debug("wallet maintenance skipped, no allowance set")
		return nil
	}

	// pending maintenance transaction - nothing to do
	pending, err := c.ap.bus.WalletPending()
	if err != nil {
		return nil
	}
	for _, txn := range pending {
		if c.maintenanceTxnID == txn.ID() {
			l.Debugf("wallet maintenance skipped, pending transaction found with id %v", c.maintenanceTxnID)
			return nil
		}
	}

	// enough outputs - nothing to do
	outputs, err := b.WalletOutputs()
	if err != nil {
		return err
	}
	if uint64(len(outputs)) >= cfg.Contracts.Amount {
		l.Debugf("no wallet maintenance needed, plenty of outputs available (%v>=%v)", len(outputs), cfg.Contracts.Amount)
		return nil
	}

	// not enough balance - nothing to do
	amount := cfg.Contracts.Allowance.Div64(cfg.Contracts.Amount)
	balance, err := b.WalletBalance()
	if err != nil {
		return err
	}
	if balance.Cmp(amount.Mul64(cfg.Contracts.Amount)) < 0 {
		l.Debugf("wallet maintenance skipped, insufficient balance %v < (%v*%v)", balance, cfg.Contracts.Amount, amount)
		return nil
	}

	// redistribute outputs
	id, err := b.WalletRedistribute(int(cfg.Contracts.Amount), amount)
	if err != nil {
		return fmt.Errorf("failed to redistribute wallet into %d outputs of amount %v, balance %v, err %v", cfg.Contracts.Amount, amount, balance, err)
	}

	l.Debugf("wallet maintenance succeeded, tx %v", id)
	c.maintenanceTxnID = id
	return nil
}

func (c *contractor) runContractChecks(cfg api.AutopilotConfig, blockHeight uint64, gs api.GougingSettings, rs api.RedundancySettings, contracts []api.Contract) (toDelete, toIgnore []types.FileContractID, toRefresh, toRenew []contractInfo, _ error) {
	c.logger.Debug("running contract checks")

	var notfound int
	defer func() {
		c.logger.Debugw(
			"contracts checks completed",
			"active", len(contracts),
			"notfound", notfound,
			"toDelete", len(toDelete),
			"toIgnore", len(toIgnore),
			"toRefresh", len(toRefresh),
			"toRenew", len(toRenew),
		)
	}()

	// create a new ip filter
	f := newIPFilter(c.logger)

	// state variables
	contractIds := make([]types.FileContractID, 0, len(contracts))
	contractSizes := make(map[types.FileContractID]uint64)
	contractMap := make(map[types.FileContractID]api.ContractMetadata)
	renewIndices := make(map[types.FileContractID]int)

	// check every active contract
	for _, contract := range contracts {
		hk := contract.HostKey()

		// fetch host from hostdb
		host, err := c.ap.bus.Host(hk)
		if err != nil {
			c.logger.Errorw(
				fmt.Sprintf("missing host, err: %v", err),
				"hk", hk,
			)
			notfound++
			continue
		}

		// decide whether the host is still good
		usable, reasons := isUsableHost(cfg, gs, rs, f, host)
		if !usable {
			c.logger.Infow(
				"unusable host",
				"hk", hk,
				"fcid", contract.ID,
				"reasons", reasons,
			)

			toIgnore = append(toIgnore, contract.ID)
			continue
		}
		settings := *host.Settings

		// decide whether the contract is still good
		usable, refresh, renew, reasons := isUsableContract(cfg, settings, contract, blockHeight)
		if !usable {
			c.logger.Infow(
				"unusable contract",
				"hk", hk,
				"fcid", contract.ID,
				"reasons", reasons,
				"refresh", refresh,
				"renew", renew,
			)
			if renew {
				renewIndices[contract.ID] = len(toRenew)
				toRenew = append(toRenew, contractInfo{
					contract: contract,
					settings: settings,
				})
			} else if refresh {
				toRefresh = append(toRefresh, contractInfo{
					contract: contract,
					settings: settings,
				})
			} else {
				toDelete = append(toDelete, contract.ID)
				continue
			}
		}

		// keep track of file size
		contractIds = append(contractIds, contract.ID)
		contractMap[contract.ID] = contract.ContractMetadata
		contractSizes[contract.ID] = contract.FileSize()
	}

	// apply active contract limit
	numContractsTooMany := len(contracts) - len(toIgnore) - len(toDelete) - int(cfg.Contracts.Amount)
	if numContractsTooMany > 0 {
		// sort by contract size
		sort.Slice(contractIds, func(i, j int) bool {
			return contractSizes[contractIds[i]] < contractSizes[contractIds[j]]
		})

		// remove superfluous contract from renewal list and add to ignore list
		prev := len(toIgnore)
		for _, id := range contractIds[:numContractsTooMany] {
			if index, exists := renewIndices[id]; exists {
				toRenew[index] = toRenew[len(toRenew)-1]
				toRenew = toRenew[:len(toRenew)-1]
			}
			toIgnore = append(toIgnore, contractMap[id].ID)
		}
		c.logger.Debugf("%d contracts too many, added %d smallest contracts to the ignore list", numContractsTooMany, len(toIgnore)-prev)
	}

	return toDelete, toIgnore, toRefresh, toRenew, nil
}

func (c *contractor) runContractRenewals(cfg api.AutopilotConfig, blockHeight, currentPeriod uint64, budget *types.Currency, renterAddress types.Address, toRefresh, toRenew []contractInfo) ([]api.ContractMetadata, error) {
	renewed := make([]api.ContractMetadata, 0, len(toRenew)+len(toRefresh))

	c.logger.Debugw(
		"run contracts renewals",
		"torefresh", len(toRefresh),
		"torenew", len(toRenew),
		"budget", budget,
	)
	defer func() {
		c.logger.Debugw(
			"contracts renewals completed",
			"renewed", len(renewed),
			"budget", budget,
		)
	}()

	// perform renewals first
	for i, ci := range append(toRenew, toRefresh...) {
		isRefresh := i >= len(toRenew)

		// break if the contractor was stopped
		if c.isStopped() {
			break
		}

		// calculate the renter funds
		renterFunds, err := c.renterFundsEstimate(cfg, currentPeriod, blockHeight, ci, isRefresh)
		if err != nil {
			c.logger.Errorw(
				fmt.Sprintf("could not get refresh funding estimate, err: %v", err),
				"hk", ci.contract.HostKey(),
				"fcid", ci.contract.ID,
				"refresh", isRefresh,
			)
			continue
		}

		// check our budget
		if budget.Cmp(renterFunds) < 0 {
			c.logger.Debugw(
				"insufficient budget",
				"budget", budget,
				"needed", renterFunds,
				"renew", !isRefresh,
				"refresh", isRefresh,
			)
			break
		}

		// derive the renter key
		newRevision, err := c.renewContract(cfg, currentPeriod, ci.contract, renterAddress, renterFunds, isRefresh)
		if err != nil {
			c.logger.Errorw(
				fmt.Sprintf("renewal failed, err: %v", err),
				"hk", ci.contract.HostKey(),
				"fcid", ci.contract.ID,
				"refresh", isRefresh,
			)
			if strings.Contains(err.Error(), wallet.ErrInsufficientBalance.Error()) {
				break
			}
			continue
		}

		// update the budget
		*budget = budget.Sub(renterFunds)

		// persist the contract
		renewedContract, err := c.ap.bus.AddRenewedContract(newRevision, renterFunds, blockHeight, ci.contract.ID)
		if err != nil {
			c.logger.Errorw(
				fmt.Sprintf("renewal failed to persist, err: %v", err),
				"hk", ci.contract.HostKey(),
				"fcid", ci.contract.ID,
			)
			return nil, err
		}
		// add to renewed set
		renewed = append(renewed, renewedContract)

		c.logger.Debugw(
			"renewal succeeded",
			"fcid", renewedContract.ID,
			"renewedFrom", ci.contract.ID,
			"refresh", isRefresh,
		)
	}

	return renewed, nil
}

func (c *contractor) runContractFormations(cfg api.AutopilotConfig, active []api.Contract, missing, blockHeight, currentPeriod uint64, budget *types.Currency, renterAddress types.Address) ([]types.FileContractID, error) {
	var formed []types.FileContractID

	c.logger.Debugw(
		"run contract formations",
		"active", len(active),
		"required", cfg.Contracts.Amount,
		"missing", missing,
		"budget", budget,
	)
	defer func() {
		c.logger.Debugw(
			"contract formations completed",
			"formed", len(formed),
			"budget", budget,
		)
	}()

	// create a map of used hosts
	used := make(map[types.PublicKey]bool)
	for _, contract := range active {
		used[contract.HostKey()] = true
	}

	// fetch recommended txn fee
	fee, err := c.ap.bus.RecommendedFee()
	if err != nil {
		return nil, err
	}

	// fetch candidate hosts
	wanted := int(addLeeway(missing, leewayPctCandidateHosts))
	candidates, err := c.candidateHosts(cfg, used, wanted)
	if err != nil {
		return nil, err
	}

	// calculate min/max contract funds
	minInitialContractFunds, maxInitialContractFunds := initialContractFundingMinMax(cfg)

	for h := 0; missing > 0 && h < len(candidates); h++ {
		host := candidates[h]

		// break if the contractor was stopped
		if c.isStopped() {
			break
		}

		// fetch host settings
		scan, err := c.ap.worker.RHPScan(host.PublicKey, host.NetAddress, 0)
		if err != nil {
			c.logger.Debugw(err.Error(), "hk", host.PublicKey)
			continue
		}
		settings := scan.Settings

		// check our budget
		txnFee := fee.Mul64(estimatedFileContractTransactionSetSize)
		renterFunds := c.initialContractFunding(settings, txnFee, minInitialContractFunds, maxInitialContractFunds)
		if budget.Cmp(renterFunds) < 0 {
			c.logger.Debugw(
				"insufficient budget",
				"budget", budget,
				"needed", renterFunds,
				"renewal", false,
			)
			break
		}

		// calculate the host collateral
		hostCollateral := calculateHostCollateral(cfg, settings)

		// form contract
		contract, _, err := c.ap.worker.RHPForm(c.endHeight(cfg, currentPeriod), host.PublicKey, host.NetAddress, renterAddress, renterFunds, hostCollateral)
		if err != nil {
			// TODO: keep track of consecutive failures and break at some point
			c.logger.Errorw(
				"failed contract formation",
				"hk", host.PublicKey,
				"err", err,
			)
			if strings.Contains(err.Error(), wallet.ErrInsufficientBalance.Error()) {
				break
			}
			continue
		}

		// update the budget
		*budget = budget.Sub(renterFunds)

		// persist contract in store
		formedContract, err := c.ap.bus.AddContract(contract, renterFunds, blockHeight)
		if err != nil {
			c.logger.Errorw(
				fmt.Sprintf("new contract failed to persist, err: %v", err),
				"hk", host.PublicKey,
			)
			continue
		}

		// add contract to contract set
		formed = append(formed, formedContract.ID)
		missing--

		c.logger.Debugw(
			"formation succeeded",
			"hk", host.PublicKey,
			"fcid", formedContract.ID,
		)
	}

	return formed, nil
}

func (c *contractor) renewContract(cfg api.AutopilotConfig, currentPeriod uint64, toRenew api.Contract, renterAddress types.Address, renterFunds types.Currency, isRefresh bool) (rhpv2.ContractRevision, error) {
	// if we are refreshing the contract we use the contract's end height
	endHeight := c.endHeight(cfg, currentPeriod)
	if isRefresh {
		endHeight = toRenew.EndHeight()
	}

	// renew the contract
	renewed, _, err := c.ap.worker.RHPRenew(toRenew.ID, endHeight, toRenew.HostKey(), toRenew.HostIP, renterAddress, renterFunds)
	if err != nil {
		return rhpv2.ContractRevision{}, err
	}
	return renewed, nil
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

func (c *contractor) renterFundsEstimate(cfg api.AutopilotConfig, currentPeriod, blockHeight uint64, ci contractInfo, isRefresh bool) (types.Currency, error) {
	if isRefresh {
		return c.refreshFundingEstimate(cfg, ci)
	}
	return c.renewFundingEstimate(cfg, blockHeight, currentPeriod, ci)
}

func (c *contractor) refreshFundingEstimate(cfg api.AutopilotConfig, ci contractInfo) (types.Currency, error) {
	// refresh with double the funds
	refreshAmount := ci.contract.TotalCost.Mul64(2)

	// estimate the txn fee
	txnFee, err := c.ap.bus.RecommendedFee()
	if err != nil {
		return types.ZeroCurrency, err
	}
	txnFeeEstimate := txnFee.Mul64(estimatedFileContractTransactionSetSize)

	// check for a sane minimum that is equal to the initial contract funding
	// but without an upper cap.
	minInitialContractFunds, _ := initialContractFundingMinMax(cfg)
	minimum := c.initialContractFunding(ci.settings, txnFeeEstimate, minInitialContractFunds, types.ZeroCurrency)
	if refreshAmount.Cmp(minimum) < 0 {
		refreshAmount = minimum
	}
	return refreshAmount, nil
}

func (c *contractor) renewFundingEstimate(cfg api.AutopilotConfig, currentPeriod, blockHeight uint64, ci contractInfo) (types.Currency, error) {
	// estimate the cost of the current data stored
	dataStored := ci.contract.FileSize()
	storageCost := types.NewCurrency64(dataStored).Mul64(cfg.Contracts.Period).Mul(ci.settings.StoragePrice)

	// fetch the spending of the contract we want to renew.
	prevSpending, err := c.contractSpending(ci.contract, currentPeriod)
	if err != nil {
		c.logger.Errorw(
			fmt.Sprintf("could not retrieve contract spending, err: %v", err),
			"hk", ci.contract.HostKey,
			"fcid", ci.contract.ID,
		)
		return types.ZeroCurrency, err
	}

	// estimate the amount of data uploaded, sanity check with data stored
	//
	// TODO: estimate is not ideal because price can change, better would be to
	// look at the amount of data stored in the contract from the previous cycle
	prevUploadDataEstimate := prevSpending.Uploads
	if !ci.settings.UploadBandwidthPrice.IsZero() {
		prevUploadDataEstimate = prevUploadDataEstimate.Div(ci.settings.UploadBandwidthPrice)
	}
	if prevUploadDataEstimate.Cmp(types.NewCurrency64(dataStored)) > 0 {
		prevUploadDataEstimate = types.NewCurrency64(dataStored)
	}

	// estimate the
	// - upload cost: previous uploads + prev storage
	// - download cost: assumed to be the same
	// - fund acount cost: assumed to be the same
	newUploadsCost := prevSpending.Uploads.Add(prevUploadDataEstimate.Mul64(cfg.Contracts.Period).Mul(ci.settings.StoragePrice))
	newDownloadsCost := prevSpending.Downloads
	newFundAccountCost := prevSpending.FundAccount

	// estimate the siafund fees
	//
	// NOTE: the transaction fees are not included in the siafunds estimate
	// because users are not charged siafund fees on money that doesn't go into
	// the file contract (and the transaction fee goes to the miners, not the
	// file contract).
	subTotal := storageCost.Add(newUploadsCost).Add(newDownloadsCost).Add(newFundAccountCost).Add(ci.settings.ContractPrice)
	siaFundFeeEstimate := (consensus.State{Index: types.ChainIndex{Height: blockHeight}}).FileContractTax(types.FileContract{Payout: subTotal})

	// estimate the txn fee
	txnFee, err := c.ap.bus.RecommendedFee()
	if err != nil {
		return types.ZeroCurrency, err
	}
	txnFeeEstimate := txnFee.Mul64(estimatedFileContractTransactionSetSize)

	// add them all up and then return the estimate plus 33% for error margin
	// and just general volatility of usage pattern.
	estimatedCost := subTotal.Add(siaFundFeeEstimate).Add(txnFeeEstimate)
	estimatedCost = estimatedCost.Add(estimatedCost.Div64(3)) // TODO: arbitrary divisor

	// check for a sane minimum that is equal to the initial contract funding
	// but without an upper cap.
	minInitialContractFunds, _ := initialContractFundingMinMax(cfg)
	minimum := c.initialContractFunding(ci.settings, txnFeeEstimate, minInitialContractFunds, types.ZeroCurrency)
	if estimatedCost.Cmp(minimum) < 0 {
		estimatedCost = minimum
	}
	return estimatedCost, nil
}

func (c *contractor) candidateHosts(cfg api.AutopilotConfig, used map[types.PublicKey]bool, wanted int) ([]hostdb.Host, error) {
	c.logger.Debugf("looking for %d candidate hosts", wanted)

	// nothing to do
	if wanted == 0 {
		return nil, nil
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

	// create IP filter
	ipFilter := newIPFilter(c.logger)

	// fetch all hosts
	hosts, err := c.ap.bus.Hosts(0, -1)
	if err != nil {
		return nil, err
	}

	c.logger.Debugf("found %d candidate hosts", len(hosts)-len(used))

	// collect scores for all usable hosts
	start := time.Now()
	scores := make([]float64, 0, len(hosts))
	scored := make([]hostdb.Host, 0, len(hosts))
	for _, h := range hosts {
		if used[h.PublicKey] {
			continue
		}
		if usable, _ := isUsableHost(cfg, gs, rs, ipFilter, h); !usable {
			continue
		}

		score := hostScore(cfg, h)
		if score == 0 {
			c.logger.DPanicw("sanity check failed", "score", score, "hk", h.PublicKey)
			continue
		}

		scored = append(scored, h)
		scores = append(scores, score)
	}

	c.logger.Debugf("scored %d candidate hosts, took %v", len(hosts)-len(used), time.Since(start))

	// select hosts
	var selected []hostdb.Host
	for len(selected) < wanted && len(scored) > 0 {
		i := randSelectByWeight(scores)
		selected = append(selected, scored[i])

		// remove selected host
		scored[i], scored = scored[len(scored)-1], scored[:len(scored)-1]
		scores[i], scores = scores[len(scores)-1], scores[:len(scores)-1]
	}

	if len(selected) < wanted {
		c.logger.Debugf("could not fetch 'wanted' candidate hosts, %d<%d", len(selected), wanted)
	}
	return selected, nil
}

func (c *contractor) endHeight(cfg api.AutopilotConfig, currentPeriod uint64) uint64 {
	return currentPeriod + cfg.Contracts.Period + cfg.Contracts.RenewWindow
}

func buildContractSet(active []api.Contract, toDelete, toIgnore []types.FileContractID, toRefresh, toRenew []contractInfo, renewed []api.ContractMetadata) []types.FileContractID {
	// collect ids
	var activeIds []types.FileContractID
	for _, c := range active {
		activeIds = append(activeIds, c.ID)
	}
	var renewIds []types.FileContractID
	for _, c := range append(toRefresh, toRenew...) {
		renewIds = append(renewIds, c.contract.ID)
	}

	// build some maps
	isDeleted := contractMapBool(toDelete)
	isIgnored := contractMapBool(toIgnore)
	isUpForRenew := contractMapBool(renewIds)

	// renewed map is special case since we need renewed from
	isRenewed := make(map[types.FileContractID]bool)
	renewedIDs := make([]types.FileContractID, 0, len(renewed))
	for _, c := range renewed {
		isRenewed[c.RenewedFrom] = true
		renewedIDs = append(renewedIDs, c.ID)
	}

	// build new contract set
	var contracts []types.FileContractID
	for _, fcid := range append(activeIds, renewedIDs...) {
		if isDeleted[fcid] {
			continue // exclude deleted contracts
		}
		if isIgnored[fcid] {
			continue // exclude ignored contracts (contracts that became unusable)
		}
		if isRenewed[fcid] {
			continue // exclude (effectively) renewed contracts
		}
		if isUpForRenew[fcid] && !isRenewed[fcid] {
			continue // exclude contracts that were up for renewal but failed to renew
		}
		contracts = append(contracts, fcid)
	}
	return contracts
}

func contractMapBool(contracts []types.FileContractID) map[types.FileContractID]bool {
	cmap := make(map[types.FileContractID]bool)
	for _, fcid := range contracts {
		cmap[fcid] = true
	}
	return cmap
}

func calculateHostCollateral(cfg api.AutopilotConfig, settings rhpv2.HostSettings) types.Currency {
	expectedStorage := cfg.Contracts.Storage / cfg.Contracts.Amount
	hostCollateral := settings.Collateral.Mul64(expectedStorage)
	if hostCollateral.Cmp(settings.MaxCollateral) > 0 {
		hostCollateral = settings.MaxCollateral
	}
	return hostCollateral
}

func addLeeway(n uint64, pct float64) uint64 {
	if pct < 0 {
		panic("given leeway percent has to be positive")
	}
	return uint64(math.Ceil(float64(n) * pct))
}

func initialContractFundingMinMax(cfg api.AutopilotConfig) (min types.Currency, max types.Currency) {
	allowance := cfg.Contracts.Allowance.Div64(cfg.Contracts.Amount)
	min = allowance.Div64(minInitialContractFundingDivisor)
	max = allowance.Div64(maxInitialContractFundingDivisor)
	return
}
