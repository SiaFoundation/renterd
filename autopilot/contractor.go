package autopilot

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	rhpv2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/hostdb"
	"go.sia.tech/renterd/tracing"
	"go.sia.tech/renterd/wallet"
	"go.sia.tech/renterd/worker"
	"go.uber.org/zap"
)

const (
	// estimatedFileContractTransactionSetSize is the estimated blockchain size
	// of a transaction set between a renter and a host that contains a file
	// contract.
	estimatedFileContractTransactionSetSize = 2048

	// leewayPctCandidateHosts is the leeway we apply when fetching candidate
	// hosts, we fetch ~10% more than required
	leewayPctCandidateHosts = 1.1

	// leewayPctRequiredContracts is the leeway we apply on the amount of
	// contracts the config dictates we should have, we'll only form new
	// contracts if the number of contracts dips below 87.5% of the required
	// contracts
	leewayPctRequiredContracts = 0.875

	// maxInitialContractFundingDivisor and minInitialContractFundingDivisor
	// define a range we use when calculating the initial contract funding
	maxInitialContractFundingDivisor = uint64(10)
	minInitialContractFundingDivisor = uint64(20)

	// minAllowedScoreLeeway is a factor by which a host can be under the lowest
	// score found in a random sample of scores before being considered not
	// usable.
	minAllowedScoreLeeway = 500

	// timeoutHostPriceTable is the amount of time we wait to receive a price
	// table from the host
	timeoutHostPriceTable = 30 * time.Second

	// timeoutHostRevision is the amount of time we wait to receive the latest
	// revision from the host
	timeoutHostRevision = 30 * time.Second

	// timeoutHostScan is the amount of time we wait for a host scan to be
	// completed
	timeoutHostScan = 30 * time.Second
)

type (
	contractor struct {
		ap     *Autopilot
		logger *zap.SugaredLogger

		maintenanceTxnID types.TransactionID

		mu               sync.Mutex
		cachedDataStored map[types.PublicKey]uint64
		cachedMinScore   float64
		currPeriod       uint64
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

func (c *contractor) performContractMaintenance(ctx context.Context, w Worker) (err error) {
	ctx, span := tracing.Tracer.Start(ctx, "contractor.performContractMaintenance")
	defer span.End()

	if c.ap.isStopped() || !c.ap.isSynced() {
		return nil // skip contract maintenance if we're not synced
	}

	c.logger.Info("performing contract maintenance")

	// convenience variables
	state := c.ap.state

	// no maintenance if no hosts are requested
	//
	// NOTE: this is an important check because we assume Contracts.Amount is
	// not zero in several places
	if state.cfg.Contracts.Amount == 0 {
		c.logger.Warn("contracts is set to zero, skipping contract maintenance")
		return nil
	}

	// no maintenance if no allowance was set
	if state.cfg.Contracts.Allowance.IsZero() {
		c.logger.Warn("allowance is set to zero, skipping contract maintenance")
		return nil
	}

	// no maintenance if no period was set
	if state.cfg.Contracts.Period == 0 {
		c.logger.Warn("period is set to zero, skipping contract maintenance")
		return nil
	}

	// fetch our wallet address
	address, err := c.ap.bus.WalletAddress(ctx)
	if err != nil {
		return err
	}

	// fetch current contract set
	currentSet, err := c.ap.bus.ContractSetContracts(ctx, state.cfg.Contracts.Set)
	if err != nil && !strings.Contains(err.Error(), api.ErrContractSetNotFound.Error()) {
		return err
	}
	c.logger.Debugf("contract set '%s' holds %d contracts", state.cfg.Contracts.Set, len(currentSet))

	// fetch used hosts.
	contracts, err := c.ap.bus.Contracts(ctx)
	if err != nil {
		return err
	}
	usedHosts := make(map[types.PublicKey]struct{})
	for _, contract := range contracts {
		usedHosts[contract.HostKey] = struct{}{}
	}

	// fetch all contracts from the worker
	start := time.Now()
	resp, err := w.Contracts(ctx, timeoutHostRevision)
	if err != nil {
		return err
	}
	if resp.Error != "" {
		c.logger.Error(resp.Error)
	}
	c.logger.Debugf("fetched %d contracts, took %v", len(resp.Contracts), time.Since(start))
	active := resp.Contracts

	// fetch all hosts
	hosts, err := c.ap.bus.Hosts(ctx, 0, -1)
	if err != nil {
		return err
	}

	// compile map of stored data per host
	storedData := make(map[types.PublicKey]uint64)
	for _, c := range active {
		storedData[c.HostKey()] += c.FileSize()
	}

	// min score to pass checks.
	var minScore float64
	if len(hosts) > 0 {
		minScore, err = c.managedFindMinAllowedHostScores(ctx, w, hosts, storedData)
		if err != nil {
			return fmt.Errorf("failed to determine min score for contract check: %w", err)
		}
	} else {
		c.logger.Warn("could not calculate min score, no hosts found")
	}

	// update cache.
	c.mu.Lock()
	c.cachedDataStored = storedData
	c.cachedMinScore = minScore
	c.mu.Unlock()

	// run checks
	updatedSet, toArchive, toRefresh, toRenew, err := c.runContractChecks(ctx, w, active, minScore)
	if err != nil {
		return fmt.Errorf("failed to run contract checks, err: %v", err)
	}

	// archive contracts
	if len(toArchive) > 0 {
		c.logger.Debugf("archiving %d contracts: %+v", len(toArchive), toArchive)
		if err := c.ap.bus.ArchiveContracts(ctx, toArchive); err != nil {
			c.logger.Errorf("failed to archive contracts, err: %v", err) // continue
		}
	}

	// calculate remaining funds
	remaining, err := c.remainingFunds(contracts)
	if err != nil {
		return err
	}

	// run renewals
	var renewed []types.FileContractID
	if limit := int(state.cfg.Contracts.Amount) - len(updatedSet); limit > 0 {
		renewed, err = c.runContractRenewals(ctx, w, &remaining, address, toRenew, uint64(limit))
		if err != nil {
			c.logger.Errorf("failed to renew contracts, err: %v", err) // continue
		} else {
			updatedSet = append(updatedSet, renewed...)
		}
	}

	// run contract refreshes
	refreshed, err := c.runContractRefreshes(ctx, w, &remaining, address, toRefresh)
	if err != nil {
		c.logger.Errorf("failed to refresh contracts, err: %v", err) // continue
	} else {
		updatedSet = append(updatedSet, refreshed...)
	}

	// check if we need to form contracts and add them to the contract set
	var formed []types.FileContractID
	if uint64(len(updatedSet)) < addLeeway(state.cfg.Contracts.Amount, leewayPctRequiredContracts) {
		formed, err = c.runContractFormations(ctx, w, hosts, usedHosts, state.cfg.Contracts.Amount-uint64(len(updatedSet)), &remaining, address, minScore)
		if err != nil {
			c.logger.Errorf("failed to form contracts, err: %v", err) // continue
		} else {
			updatedSet = append(updatedSet, formed...)
		}
	}

	// defer logging
	defer func() {
		numContractsInSet := len(currentSet)
		if err == nil {
			numContractsInSet = len(updatedSet)
		}
		if numContractsInSet < int(state.rs.TotalShards) {
			c.logger.Warnw(
				"contracts after maintenance are below the minimum required",
				"formed", len(formed),
				"renewed", len(renewed),
				"contractset", numContractsInSet,
			)
		} else {
			c.logger.Debugw(
				"contracts after maintenance",
				"formed", len(formed),
				"renewed", len(renewed),
				"contractset", numContractsInSet,
			)
		}
	}()

	// cap the amount of contracts we want to keep to the configured amount
	if len(updatedSet) > int(state.cfg.Contracts.Amount) {
		// build sizemap
		sizemap := make(map[types.FileContractID]uint64)
		for _, c := range active {
			sizemap[c.ID] = c.FileSize()
		}

		// sort by contract size
		sort.Slice(updatedSet, func(i, j int) bool {
			return sizemap[updatedSet[i]] > sizemap[updatedSet[j]]
		})

		updatedSet = updatedSet[:state.cfg.Contracts.Amount]
	}

	// update contract set
	if c.ap.isStopped() {
		err = errors.New("autopilot stopped before maintenance could be completed")
		return
	}
	err = c.ap.bus.SetContractSet(ctx, state.cfg.Contracts.Set, updatedSet)
	return
}

func (c *contractor) performWalletMaintenance(ctx context.Context) error {
	ctx, span := tracing.Tracer.Start(ctx, "contractor.performWalletMaintenance")
	defer span.End()

	if c.ap.isStopped() || !c.ap.isSynced() {
		return nil // skip contract maintenance if we're not synced
	}

	c.logger.Info("performing wallet maintenance")
	b := c.ap.bus
	l := c.logger

	// no contracts - nothing to do
	cfg := c.ap.state.cfg
	if cfg.Contracts.Amount == 0 {
		l.Warn("wallet maintenance skipped, no contracts wanted")
		return nil
	}

	// no allowance - nothing to do
	if cfg.Contracts.Allowance.IsZero() {
		l.Warn("wallet maintenance skipped, no allowance set")
		return nil
	}

	// pending maintenance transaction - nothing to do
	pending, err := b.WalletPending(ctx)
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
	available, err := b.WalletOutputs(ctx)
	if err != nil {
		return err
	}
	if uint64(len(available)) >= cfg.Contracts.Amount {
		l.Debugf("no wallet maintenance needed, plenty of outputs available (%v>=%v)", len(available), cfg.Contracts.Amount)
		return nil
	}

	// not enough balance - nothing to do
	balance, err := b.WalletBalance(ctx)
	if err != nil {
		l.Errorf("wallet maintenance skipped, fetching wallet balance failed with err: %v", err)
		return err
	}
	amount := cfg.Contracts.Allowance.Div64(cfg.Contracts.Amount)
	outputs := balance.Div(amount).Big().Uint64()
	if outputs < 2 {
		l.Warnf("wallet maintenance skipped, wallet has insufficient balance %v", balance)
		return err
	}
	if outputs > cfg.Contracts.Amount {
		outputs = cfg.Contracts.Amount
	}

	// redistribute outputs
	id, err := b.WalletRedistribute(ctx, int(outputs), amount)
	if err != nil {
		return fmt.Errorf("failed to redistribute wallet into %d outputs of amount %v, balance %v, err %v", outputs, amount, balance, err)
	}

	l.Debugf("wallet maintenance succeeded, tx %v", id)
	c.maintenanceTxnID = id
	return nil
}

func (c *contractor) runContractChecks(ctx context.Context, w Worker, contracts []api.Contract, minScore float64) (toKeep []types.FileContractID, toArchive map[types.FileContractID]string, toRefresh, toRenew []contractInfo, _ error) {
	if c.ap.isStopped() {
		return
	}
	c.logger.Debug("running contract checks")

	var notfound int
	defer func() {
		c.logger.Debugw(
			"contracts checks completed",
			"active", len(contracts),
			"notfound", notfound,
			"toKeep", len(toKeep),
			"toArchive", len(toArchive),
			"toRefresh", len(toRefresh),
			"toRenew", len(toRenew),
		)
	}()

	// create a new ip filter
	f := newIPFilter(c.logger)

	// convenience variables
	state := c.ap.state

	// create a gouging checker
	gc := worker.NewGougingChecker(state.gs, state.rs, state.cs, state.fee, state.cfg.Contracts.Period, state.cfg.Contracts.RenewWindow)

	// state variables
	renewIndices := make(map[types.FileContractID]int)

	// return variables
	toArchive = make(map[types.FileContractID]string)

	// check all contracts
	for _, contract := range contracts {
		// convenience variables
		hk := contract.HostKey()
		fcid := contract.ID

		// check if contract is ready to be archived.
		if state.cs.BlockHeight > contract.EndHeight() {
			toArchive[fcid] = errContractExpired.Error()
			continue
		} else if contract.Revision.RevisionNumber == math.MaxUint64 {
			toArchive[fcid] = errContractMaxRevisionNumber.Error()
			continue
		}

		// fetch host from hostdb
		host, err := c.ap.bus.Host(ctx, hk)
		if err != nil {
			c.logger.Errorw(fmt.Sprintf("missing host, err: %v", err), "hk", hk)
			notfound++
			continue
		}

		// if the host is blocked we ignore it, it might be unblocked later
		if host.Blocked {
			c.logger.Infow("unusable host", "hk", hk, "fcid", fcid, "reasons", errHostBlocked.Error())
			continue
		}

		// fetch recent price table and attach it to host.
		host.PriceTable, err = c.priceTable(ctx, w, host.Host)
		if err != nil {
			c.logger.Errorf("could not fetch price table for host %v: %v", host.PublicKey, err)
			continue
		}

		// set the host's block height to ours to disable the height check in
		// the gouging checks, in certain edge cases the renter might unsync and
		// would therefor label all hosts as unusable and go on to create a
		// whole new set of contracts with new hosts
		host.PriceTable.HostBlockHeight = state.cs.BlockHeight

		// decide whether the host is still good
		usable, unusableResult := isUsableHost(state.cfg, state.rs, gc, f, host.Host, minScore, contract.FileSize())
		if !usable {
			c.logger.Infow("unusable host", "hk", hk, "fcid", fcid, "reasons", unusableResult.reasons())
			continue
		}

		// decide whether the contract is still good
		ci := contractInfo{contract: contract, settings: host.Settings}
		renterFunds, err := c.renewFundingEstimate(ctx, ci, false)
		if err != nil {
			c.logger.Errorw(fmt.Sprintf("failed to compute renterFunds for contract: %v", err))
		}

		usable, refresh, renew, reasons := isUsableContract(state.cfg, ci, state.cs.BlockHeight, renterFunds)
		if !usable {
			c.logger.Infow(
				"unusable contract",
				"hk", hk,
				"fcid", fcid,
				"reasons", errStr(joinErrors(reasons)),
				"refresh", refresh,
				"renew", renew,
			)
		}

		if renew {
			renewIndices[fcid] = len(toRenew)
			toRenew = append(toRenew, contractInfo{
				contract: contract,
				settings: host.Settings,
			})
		} else if refresh {
			toRefresh = append(toRefresh, contractInfo{
				contract: contract,
				settings: host.Settings,
			})
		} else {
			toKeep = append(toKeep, fcid)
		}
	}

	return toKeep, toArchive, toRefresh, toRenew, nil
}

func (c *contractor) runContractFormations(ctx context.Context, w Worker, hosts []hostdb.Host, usedHosts map[types.PublicKey]struct{}, missing uint64, budget *types.Currency, renterAddress types.Address, minScore float64) ([]types.FileContractID, error) {
	ctx, span := tracing.Tracer.Start(ctx, "runContractFormations")
	defer span.End()

	if c.ap.isStopped() {
		return nil, nil
	}
	var formed []types.FileContractID

	c.logger.Debugw(
		"run contract formations",
		"usedHosts", len(usedHosts),
		"required", c.ap.state.cfg.Contracts.Amount,
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

	// convenience variables
	state := c.ap.state

	// fetch candidate hosts
	wanted := int(addLeeway(missing, leewayPctCandidateHosts))
	candidates, _, err := c.candidateHosts(ctx, w, hosts, usedHosts, make(map[types.PublicKey]uint64), wanted, minScore)
	if err != nil {
		return nil, err
	}

	// calculate min/max contract funds
	minInitialContractFunds, maxInitialContractFunds := initialContractFundingMinMax(state.cfg)

	for h := 0; missing > 0 && h < len(candidates); h++ {
		if c.ap.isStopped() {
			break
		}

		host := candidates[h]

		// break if the autopilot is stopped
		if c.ap.isStopped() {
			break
		}

		// fetch price table on the fly
		host.PriceTable, err = c.priceTable(ctx, w, host)
		if err != nil {
			c.logger.Errorf("failed to fetch price table for candidate host %v: %v", host.PublicKey, err)
			continue
		}

		// fetch consensus state on the fly for the gouging check.
		cs, err := c.ap.bus.ConsensusState(ctx)
		if err != nil {
			c.logger.Errorf("failed to fetch consensus state for gouging check: %v", err)
			continue
		}

		// create a gouging checker
		gc := worker.NewGougingChecker(state.gs, state.rs, cs, state.fee, state.cfg.Contracts.Period, state.cfg.Contracts.RenewWindow)

		// perform gouging checks on the fly to ensure the host is not gouging its prices
		if breakdown := gc.Check(nil, &host.PriceTable.HostPriceTable); breakdown.Gouging() {
			c.logger.Errorw("candidate host became unusable", "hk", host.PublicKey, "reasons", breakdown.Reasons())
			continue
		}

		formedContract, proceed, err := c.formContract(ctx, w, host, minInitialContractFunds, maxInitialContractFunds, budget, renterAddress)
		if err == nil {
			// add contract to contract set
			formed = append(formed, formedContract.ID)
			missing--
		}
		if !proceed {
			break
		}
	}

	return formed, nil
}

func (c *contractor) runContractRenewals(ctx context.Context, w Worker, budget *types.Currency, renterAddress types.Address, toRenew []contractInfo, limit uint64) (renewed []types.FileContractID, _ error) {
	ctx, span := tracing.Tracer.Start(ctx, "runContractRenewals")
	defer span.End()

	c.logger.Debugw(
		"run contracts renewals",
		"torenew", len(toRenew),
		"limit", limit,
		"budget", budget,
	)
	defer func() {
		c.logger.Debugw(
			"contracts renewals completed",
			"renewed", len(renewed),
			"budget", budget,
		)
	}()
	// start renewing from the largest contract to lose the least amount of data
	// in case we have more contracts than we need.
	sort.Slice(toRenew, func(i, j int) bool {
		return toRenew[i].contract.Revision.Filesize > toRenew[j].contract.Revision.Filesize
	})

	var nRenewed uint64
	for _, ci := range toRenew {
		// TODO: keep track of consecutive failures and break at some point

		// limit the number of contracts to renew
		if nRenewed >= limit {
			break
		}

		// break if the autopilot is stopped
		if c.ap.isStopped() {
			break
		}

		contract, proceed, err := c.renewContract(ctx, w, ci, budget, renterAddress)
		if err == nil {
			renewed = append(renewed, contract.ID)
		}
		if !proceed {
			break
		}
	}

	return renewed, nil
}

func (c *contractor) runContractRefreshes(ctx context.Context, w Worker, budget *types.Currency, renterAddress types.Address, toRefresh []contractInfo) (refreshed []types.FileContractID, _ error) {
	ctx, span := tracing.Tracer.Start(ctx, "runContractRefreshes")
	defer span.End()

	c.logger.Debugw(
		"run contracts refreshes",
		"torefresh", len(toRefresh),
		"budget", budget,
	)
	defer func() {
		c.logger.Debugw(
			"contracts refreshes completed",
			"refreshed", len(refreshed),
			"budget", budget,
		)
	}()

	for _, ci := range toRefresh {
		// TODO: keep track of consecutive failures and break at some point

		// break if the autopilot is stopped
		if c.ap.isStopped() {
			break
		}

		contract, proceed, err := c.refreshContract(ctx, w, ci, budget, renterAddress)
		if err == nil {
			refreshed = append(refreshed, contract.ID)
		}
		if !proceed {
			break
		}
	}

	return refreshed, nil
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

func (c *contractor) refreshFundingEstimate(ctx context.Context, cfg api.AutopilotConfig, ci contractInfo) (types.Currency, error) {
	// refresh with 1.2x the funds
	refreshAmount := ci.contract.TotalCost.Mul64(6).Div64(5)

	// estimate the txn fee
	txnFeeEstimate := c.ap.state.fee.Mul64(estimatedFileContractTransactionSetSize)

	// check for a sane minimum that is equal to the initial contract funding
	// but without an upper cap.
	minInitialContractFunds, _ := initialContractFundingMinMax(cfg)
	minimum := c.initialContractFunding(ci.settings, txnFeeEstimate, minInitialContractFunds, types.ZeroCurrency)
	refreshAmountCapped := refreshAmount
	if refreshAmountCapped.Cmp(minimum) < 0 {
		refreshAmountCapped = minimum
	}
	c.logger.Debugw("refresh estimate",
		"fcid", ci.contract.ID,
		"refreshAmount", refreshAmount,
		"refreshAmountCapped", refreshAmountCapped)
	return refreshAmountCapped, nil
}

func (c *contractor) renewFundingEstimate(ctx context.Context, ci contractInfo, renewing bool) (types.Currency, error) {
	cfg := c.ap.state.cfg

	// estimate the cost of the current data stored
	dataStored := ci.contract.FileSize()
	storageCost := types.NewCurrency64(dataStored).Mul64(cfg.Contracts.Period).Mul(ci.settings.StoragePrice)

	// fetch the spending of the contract we want to renew.
	prevSpending, err := c.contractSpending(ctx, ci.contract, c.currentPeriod())
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
	siaFundFeeEstimate, err := c.ap.bus.FileContractTax(ctx, subTotal)
	if err != nil {
		return types.ZeroCurrency, err
	}

	// estimate the txn fee
	txnFeeEstimate := c.ap.state.fee.Mul64(estimatedFileContractTransactionSetSize)

	// add them all up and then return the estimate plus 33% for error margin
	// and just general volatility of usage pattern.
	estimatedCost := subTotal.Add(siaFundFeeEstimate).Add(txnFeeEstimate)
	estimatedCost = estimatedCost.Add(estimatedCost.Div64(3)) // TODO: arbitrary divisor

	// check for a sane minimum that is equal to the initial contract funding
	// but without an upper cap.
	minInitialContractFunds, _ := initialContractFundingMinMax(cfg)
	minimum := c.initialContractFunding(ci.settings, txnFeeEstimate, minInitialContractFunds, types.ZeroCurrency)
	cappedEstimatedCost := estimatedCost
	if cappedEstimatedCost.Cmp(minimum) < 0 {
		cappedEstimatedCost = minimum
	}

	if renewing {
		c.logger.Debugw("renew estimate",
			"fcid", ci.contract.ID,
			"dataStored", dataStored,
			"storageCost", storageCost.String(),
			"newUploadsCost", newUploadsCost.String(),
			"newDownloadsCost", newDownloadsCost.String(),
			"newFundAccountCost", newFundAccountCost.String(),
			"contractPrice", ci.settings.ContractPrice.String(),
			"prevUploadDataEstimate", prevUploadDataEstimate.String(),
			"estimatedCost", estimatedCost.String(),
			"minInitialContractFunds", minInitialContractFunds.String(),
			"minimum", minimum.String(),
			"cappedEstimatedCost", cappedEstimatedCost.String(),
		)
	}
	return cappedEstimatedCost, nil
}

func (c *contractor) managedFindMinAllowedHostScores(ctx context.Context, w Worker, hosts []hostdb.Host, storedData map[types.PublicKey]uint64) (float64, error) {
	// Pull a new set of hosts from the hostdb that could be used as a new set
	// to match the allowance. The lowest scoring host of these new hosts will
	// be used as a baseline for determining whether our existing contracts are
	// worthwhile.
	numContracts := c.ap.state.cfg.Contracts.Amount
	buffer := 50
	candidates, scores, err := c.candidateHosts(ctx, w, hosts, make(map[types.PublicKey]struct{}), storedData, int(numContracts)+int(buffer), math.SmallestNonzeroFloat64) // avoid 0 score hosts
	if err != nil {
		return 0, err
	}
	if len(candidates) == 0 {
		c.logger.Warn("min host score is set to the smallest non-zero float because there are no candidate hosts")
		return math.SmallestNonzeroFloat64, nil
	}

	// Find the minimum score that a host is allowed to have to be considered
	// good for upload.
	lowestScore := math.MaxFloat64
	for _, score := range scores {
		if score < lowestScore {
			lowestScore = score
		}
	}
	return lowestScore / minAllowedScoreLeeway, nil
}

func (c *contractor) candidateHosts(ctx context.Context, w Worker, hosts []hostdb.Host, usedHosts map[types.PublicKey]struct{}, storedData map[types.PublicKey]uint64, wanted int, minScore float64) ([]hostdb.Host, []float64, error) {
	c.logger.Debugf("looking for %d candidate hosts", wanted)

	// nothing to do
	if wanted == 0 {
		return nil, nil, nil
	}

	state := c.ap.state

	// create an IP filter
	ipFilter := newIPFilter(c.logger)

	// create a gouging checker
	gc := worker.NewGougingChecker(state.gs, state.rs, state.cs, state.fee, state.cfg.Contracts.Period, state.cfg.Contracts.RenewWindow)

	// create list of candidate hosts
	var candidates []hostdb.Host
	var excluded, notcompletedscan int
	for _, h := range hosts {
		// filter out used hosts
		if _, exclude := usedHosts[h.PublicKey]; exclude {
			_ = ipFilter.isRedundantIP(h) // ensure the host's IP is registered as used
			excluded++
			continue
		}
		// filter out unscanned hosts
		if !h.Scanned {
			notcompletedscan++
			continue
		}
		candidates = append(candidates, h)
	}

	c.logger.Debugw(fmt.Sprintf("selected %d candidate hosts out of %d", len(candidates), len(hosts)),
		"excluded", excluded,
		"notcompletedscan", notcompletedscan)

	// score all candidate hosts
	start := time.Now()
	var results unusableHostResult
	scores := make([]float64, 0, len(candidates))
	scored := make([]hostdb.Host, 0, len(candidates))
	var unusable, zeros int
	for _, h := range candidates {
		// NOTE: use the price table stored on the host for gouging checks when
		// looking for candidate hosts, fetching the price table on the fly here
		// slows contract maintenance down way too much, we re-evaluate the host
		// right before forming the contract to ensure we do not form a contract
		// with a host that's gouging its prices.
		//
		// NOTE: ignore the pricetable's HostBlockHeight by setting it to our
		// own blockheight
		h.PriceTable.HostBlockHeight = state.cs.BlockHeight
		if usable, result := isUsableHost(state.cfg, state.rs, gc, ipFilter, h, minScore, storedData[h.PublicKey]); usable {
			scored = append(scored, h)
			scores = append(scores, result.scoreBreakdown.Score())
		} else {
			results.merge(result)
			if result.scoreBreakdown.Score() == 0 {
				zeros++
			}
			unusable++
		}
	}

	c.logger.Debugw(fmt.Sprintf("scored %d candidate hosts out of %v, took %v", len(scored), len(candidates), time.Since(start)),
		"zeroscore", zeros,
		"unusable", unusable)

	// select hosts
	var selectedHosts []hostdb.Host
	var selectedScores []float64
	for len(selectedHosts) < wanted && len(scored) > 0 {
		i := randSelectByWeight(scores)
		selectedHosts = append(selectedHosts, scored[i])
		selectedScores = append(selectedScores, scores[i])

		// remove selected host
		scored[i], scored = scored[len(scored)-1], scored[:len(scored)-1]
		scores[i], scores = scores[len(scores)-1], scores[:len(scores)-1]
	}

	// print warning if no candidate hosts were found
	if len(selectedHosts) < wanted {
		msg := "no candidate hosts found"
		if len(selectedHosts) > 0 {
			msg = fmt.Sprintf("only found %d candidate host(s) out of the %d we wanted", len(selectedHosts), wanted)
		}
		if len(candidates) >= wanted {
			c.logger.Warnw(msg, results.keysAndValues()...)
		} else {
			c.logger.Debugw(msg, results.keysAndValues()...)
		}
	}

	return selectedHosts, selectedScores, nil
}

func (c *contractor) renewContract(ctx context.Context, w Worker, ci contractInfo, budget *types.Currency, renterAddress types.Address) (cm api.ContractMetadata, proceed bool, err error) {
	ctx, span := tracing.Tracer.Start(ctx, "renewContract")
	defer span.End()
	defer func() {
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "failed to renew contract")
		}
	}()
	span.SetAttributes(attribute.Stringer("host", ci.contract.HostKey()))
	span.SetAttributes(attribute.Stringer("contract", ci.contract.ID))

	// convenience variables
	cfg := c.ap.state.cfg
	cs := c.ap.state.cs
	contract := ci.contract
	settings := ci.settings
	fcid := contract.ID
	rev := contract.Revision
	hk := contract.HostKey()

	// calculate the renter funds
	renterFunds, err := c.renewFundingEstimate(ctx, ci, true)
	if err != nil {
		c.logger.Errorw(fmt.Sprintf("could not get renew funding estimate, err: %v", err), "hk", hk, "fcid", fcid)
		return api.ContractMetadata{}, true, err
	}

	// check our budget
	if budget.Cmp(renterFunds) < 0 {
		c.logger.Debugw("insufficient budget", "budget", budget, "needed", renterFunds)
		return api.ContractMetadata{}, false, errors.New("insufficient budget")
	}

	// calculate the host collateral
	endHeight := endHeight(cfg, c.currentPeriod())
	expectedStorage := renterFundsToExpectedStorage(renterFunds, endHeight-cs.BlockHeight, settings)
	newCollateral := rhpv2.ContractRenewalCollateral(rev.FileContract, expectedStorage, settings, cs.BlockHeight, endHeight)

	// renew the contract
	newRevision, _, err := w.RHPRenew(ctx, fcid, endHeight, hk, contract.SiamuxAddr, settings.Address, renterAddress, renterFunds, newCollateral, settings.WindowSize)
	if err != nil {
		c.logger.Errorw(fmt.Sprintf("renewal failed, err: %v", err), "hk", hk, "fcid", fcid)
		if containsError(err, wallet.ErrInsufficientBalance) {
			return api.ContractMetadata{}, false, err
		}
		return api.ContractMetadata{}, true, err
	}

	// update the budget
	*budget = budget.Sub(renterFunds)

	// persist the contract
	renewedContract, err := c.ap.bus.AddRenewedContract(ctx, newRevision, renterFunds, cs.BlockHeight, fcid)
	if err != nil {
		c.logger.Errorw(fmt.Sprintf("renewal failed to persist, err: %v", err), "hk", hk, "fcid", fcid)
		return api.ContractMetadata{}, false, err
	}

	c.logger.Debugw(
		"renewal succeeded",
		"fcid", renewedContract.ID,
		"renewedFrom", fcid,
		"renterFunds", renterFunds.String(),
		"newCollateral", newCollateral.String(),
	)
	return renewedContract, true, nil
}

func (c *contractor) refreshContract(ctx context.Context, w Worker, ci contractInfo, budget *types.Currency, renterAddress types.Address) (cm api.ContractMetadata, proceed bool, err error) {
	ctx, span := tracing.Tracer.Start(ctx, "refreshContract")
	defer span.End()
	defer func() {
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "failed to refresh contract")
		}
	}()
	span.SetAttributes(attribute.Stringer("host", ci.contract.HostKey()))
	span.SetAttributes(attribute.Stringer("contract", ci.contract.ID))

	// convenience variables
	cfg := c.ap.state.cfg
	cs := c.ap.state.cs
	contract := ci.contract
	settings := ci.settings
	fcid := contract.ID
	rev := contract.Revision
	hk := contract.HostKey()

	// calculate the renter funds
	renterFunds, err := c.refreshFundingEstimate(ctx, cfg, ci)
	if err != nil {
		c.logger.Errorw(fmt.Sprintf("could not get refresh funding estimate, err: %v", err), "hk", hk, "fcid", fcid)
		return api.ContractMetadata{}, true, err
	}

	// check our budget
	if budget.Cmp(renterFunds) < 0 {
		c.logger.Warnw("insufficient budget for refresh", "hk", hk, "fcid", fcid, "budget", budget, "needed", renterFunds)
		return api.ContractMetadata{}, false, fmt.Errorf("insufficient budget: %s < %s", budget.String(), renterFunds.String())
	}

	// calculate the new collateral
	expectedStorage := renterFundsToExpectedStorage(renterFunds, contract.EndHeight()-cs.BlockHeight, settings)
	newCollateral := rhpv2.ContractRenewalCollateral(rev.FileContract, expectedStorage, settings, cs.BlockHeight, contract.EndHeight())

	// do not refresh if the contract's updated collateral will fall below the threshold anyway
	_, hostMissedPayout, _, _ := rhpv2.CalculateHostPayouts(rev.FileContract, newCollateral, settings, contract.EndHeight())
	var newRemainingCollateral types.Currency
	if hostMissedPayout.Cmp(settings.ContractPrice) > 0 {
		newRemainingCollateral = hostMissedPayout.Sub(settings.ContractPrice)
	}
	if isBelowCollateralThreshold(newCollateral, newRemainingCollateral) {
		err := errors.New("refresh failed, new collateral is below the threshold")
		c.logger.Errorw(err.Error(), "hk", hk, "fcid", fcid, "expectedCollateral", newCollateral.String(), "actualCollateral", newRemainingCollateral.String(), "maxCollateral", settings.MaxCollateral)
		return api.ContractMetadata{}, true, err
	}

	// renew the contract
	newRevision, _, err := w.RHPRenew(ctx, contract.ID, contract.EndHeight(), hk, contract.SiamuxAddr, settings.Address, renterAddress, renterFunds, newCollateral, settings.WindowSize)
	if err != nil {
		c.logger.Errorw(fmt.Sprintf("refresh failed, err: %v", err), "hk", hk, "fcid", fcid)
		if containsError(err, wallet.ErrInsufficientBalance) {
			return api.ContractMetadata{}, false, err
		}
		return api.ContractMetadata{}, true, err
	}

	// update the budget
	*budget = budget.Sub(renterFunds)

	// persist the contract
	refreshedContract, err := c.ap.bus.AddRenewedContract(ctx, newRevision, renterFunds, cs.BlockHeight, contract.ID)
	if err != nil {
		c.logger.Errorw(fmt.Sprintf("refresh failed, err: %v", err), "hk", hk, "fcid", fcid)
		return api.ContractMetadata{}, false, err
	}

	// add to renewed set
	c.logger.Debugw("refresh succeeded",
		"fcid", refreshedContract.ID,
		"renewedFrom", contract.ID,
		"renterFunds", renterFunds.String(),
		"newCollateral", newCollateral.String(),
	)
	return refreshedContract, true, nil
}

func (c *contractor) formContract(ctx context.Context, w Worker, host hostdb.Host, minInitialContractFunds, maxInitialContractFunds types.Currency, budget *types.Currency, renterAddress types.Address) (cm api.ContractMetadata, proceed bool, err error) {
	ctx, span := tracing.Tracer.Start(ctx, "formContract")
	defer span.End()
	defer func() {
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "failed to form contract")
		}
	}()
	hk := host.PublicKey
	span.SetAttributes(attribute.Stringer("host", hk))

	// convenience variables
	state := c.ap.state

	// fetch host settings
	scan, err := w.RHPScan(ctx, hk, host.NetAddress, 0)
	if err != nil {
		c.logger.Debugw(err.Error(), "hk", hk)
		return api.ContractMetadata{}, true, err
	}

	// check our budget
	txnFee := state.fee.Mul64(estimatedFileContractTransactionSetSize)
	renterFunds := initialContractFunding(scan.Settings, txnFee, minInitialContractFunds, maxInitialContractFunds)
	if budget.Cmp(renterFunds) < 0 {
		c.logger.Debugw("insufficient budget", "budget", budget, "needed", renterFunds)
		return api.ContractMetadata{}, false, errors.New("insufficient budget")
	}

	// calculate the host collateral
	endHeight := endHeight(state.cfg, c.currentPeriod())
	expectedStorage := renterFundsToExpectedStorage(renterFunds, endHeight-state.cs.BlockHeight, scan.Settings)
	hostCollateral := rhpv2.ContractFormationCollateral(state.cfg.Contracts.Period, expectedStorage, scan.Settings)

	// form contract
	contract, _, err := w.RHPForm(ctx, endHeight, hk, host.NetAddress, renterAddress, renterFunds, hostCollateral)
	if err != nil {
		// TODO: keep track of consecutive failures and break at some point
		c.logger.Errorw(fmt.Sprintf("contract formation failed, err: %v", err), "hk", hk)
		if containsError(err, wallet.ErrInsufficientBalance) {
			return api.ContractMetadata{}, false, err
		}
		return api.ContractMetadata{}, true, err
	}

	// update the budget
	*budget = budget.Sub(renterFunds)

	// persist contract in store
	formedContract, err := c.ap.bus.AddContract(ctx, contract, renterFunds, state.cs.BlockHeight)
	if err != nil {
		c.logger.Errorw(fmt.Sprintf("contract formation failed, err: %v", err), "hk", hk)
		return api.ContractMetadata{}, true, err
	}

	c.logger.Debugw("formation succeeded",
		"hk", hk,
		"fcid", formedContract.ID,
		"renterFunds", renterFunds.String(),
		"collateral", hostCollateral.String(),
	)
	return formedContract, true, nil
}

func (c *contractor) priceTable(ctx context.Context, w Worker, host hostdb.Host) (hostdb.HostPriceTable, error) {
	// scan the host if it hasn't been successfully scanned before, which can
	// occur when contracts are added manually to the bus or database
	if !host.Scanned {
		scan, err := w.RHPScan(ctx, host.PublicKey, host.NetAddress, timeoutHostScan)
		if err != nil {
			return hostdb.HostPriceTable{}, err
		}
		host.Settings = scan.Settings
	}

	ctx, cancel := context.WithTimeout(ctx, timeoutHostPriceTable)
	defer cancel()

	// fetch the price table
	return w.RHPPriceTable(ctx, host.PublicKey, host.Settings.SiamuxAddr())
}

func initialContractFunding(settings rhpv2.HostSettings, txnFee, min, max types.Currency) types.Currency {
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

func initialContractFundingMinMax(cfg api.AutopilotConfig) (min types.Currency, max types.Currency) {
	allowance := cfg.Contracts.Allowance.Div64(cfg.Contracts.Amount)
	min = allowance.Div64(minInitialContractFundingDivisor)
	max = allowance.Div64(maxInitialContractFundingDivisor)
	return
}

func addLeeway(n uint64, pct float64) uint64 {
	if pct < 0 {
		panic("given leeway percent has to be positive")
	}
	return uint64(math.Ceil(float64(n) * pct))
}

func contractMapBool(contracts []types.FileContractID) map[types.FileContractID]bool {
	contractsMap := make(map[types.FileContractID]bool)
	for _, fcid := range contracts {
		contractsMap[fcid] = true
	}
	return contractsMap
}

func endHeight(cfg api.AutopilotConfig, currentPeriod uint64) uint64 {
	return currentPeriod + cfg.Contracts.Period + cfg.Contracts.RenewWindow
}

// renterFundsToExpectedStorage returns how much storage a renter is expected to
// be able to afford given the provided 'renterFunds'.
func renterFundsToExpectedStorage(renterFunds types.Currency, duration uint64, host rhpv2.HostSettings) uint64 {
	costPerByte := host.UploadBandwidthPrice.Add(host.StoragePrice.Mul64(duration)).Add(host.DownloadBandwidthPrice)
	// If storage is free, we can afford 'unlimited' data.
	if costPerByte.IsZero() {
		return math.MaxUint64
	}
	// Catch overflow.
	expectedStorage := renterFunds.Div(costPerByte)
	if expectedStorage.Cmp(types.NewCurrency64(math.MaxUint64)) > 0 {
		return math.MaxUint64
	}
	return expectedStorage.Big().Uint64()
}
