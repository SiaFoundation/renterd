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
	// contracts if the number of contracts dips below 90% of the required
	// contracts
	//
	// NOTE: updating this value indirectly affects 'maxKeepLeeway'
	leewayPctRequiredContracts = 0.9

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
	// revision from the host. This is set to 4 minutes since siad currently
	// blocks for 3 minutes when trying to fetch a revision and not having
	// enough funds in the account used for fetching it. That way we are
	// guaranteed to receive the host's ErrBalanceInsufficient.
	// TODO: This can be lowered once the network uses hostd.
	timeoutHostRevision = 4 * time.Minute

	// timeoutHostScan is the amount of time we wait for a host scan to be
	// completed
	timeoutHostScan = 30 * time.Second
)

type (
	contractor struct {
		ap     *Autopilot
		logger *zap.SugaredLogger

		maintenanceTxnID         types.TransactionID
		revisionSubmissionBuffer uint64

		mu               sync.Mutex
		cachedHostInfo   map[types.PublicKey]hostInfo
		cachedDataStored map[types.PublicKey]uint64
		cachedMinScore   float64
	}

	hostInfo struct {
		Usable         bool
		UnusableResult unusableHostResult
	}

	contractInfo struct {
		contract api.Contract
		settings rhpv2.HostSettings
		usable   bool
	}

	renewal struct {
		from types.FileContractID
		to   types.FileContractID
	}
)

func newContractor(ap *Autopilot, revisionSubmissionBuffer uint64) *contractor {
	return &contractor{
		ap:                       ap,
		logger:                   ap.logger.Named("contractor"),
		revisionSubmissionBuffer: revisionSubmissionBuffer,
	}
}

func (c *contractor) performContractMaintenance(ctx context.Context, w Worker) (bool, error) {
	ctx, span := tracing.Tracer.Start(ctx, "contractor.performContractMaintenance")
	defer span.End()

	if c.ap.isStopped() || !c.ap.isSynced() {
		return false, nil // skip contract maintenance if we're not synced
	}

	c.logger.Info("performing contract maintenance")

	// convenience variables
	state := c.ap.State()

	// no maintenance if no hosts are requested
	//
	// NOTE: this is an important check because we assume Contracts.Amount is
	// not zero in several places
	if state.cfg.Contracts.Amount == 0 {
		c.logger.Warn("contracts is set to zero, skipping contract maintenance")
		return false, nil
	}

	// no maintenance if no allowance was set
	if state.cfg.Contracts.Allowance.IsZero() {
		c.logger.Warn("allowance is set to zero, skipping contract maintenance")
		return false, nil
	}

	// no maintenance if no period was set
	if state.cfg.Contracts.Period == 0 {
		c.logger.Warn("period is set to zero, skipping contract maintenance")
		return false, nil
	}

	// fetch our wallet address
	address, err := c.ap.bus.WalletAddress(ctx)
	if err != nil {
		return false, err
	}

	// fetch current contract set
	currentSet, err := c.ap.bus.ContractSetContracts(ctx, state.cfg.Contracts.Set)
	if err != nil && !strings.Contains(err.Error(), api.ErrContractSetNotFound.Error()) {
		return false, err
	}
	isInCurrentSet := make(map[types.FileContractID]struct{})
	for _, c := range currentSet {
		isInCurrentSet[c.ID] = struct{}{}
	}
	c.logger.Debugf("contract set '%s' holds %d contracts", state.cfg.Contracts.Set, len(currentSet))

	// fetch all contracts from the worker.
	start := time.Now()
	resp, err := w.Contracts(ctx, timeoutHostRevision)
	if err != nil {
		return false, err
	}
	if resp.Error != "" {
		c.logger.Error(resp.Error)
	}
	contracts := resp.Contracts
	c.logger.Debugf("fetched %d contracts from the worker, took %v", len(resp.Contracts), time.Since(start))

	// sort contracts by their size
	sort.Slice(contracts, func(i, j int) bool {
		return contracts[i].FileSize() > contracts[j].FileSize()
	})

	// get used hosts
	usedHosts := make(map[types.PublicKey]struct{})
	for _, contract := range contracts {
		usedHosts[contract.HostKey] = struct{}{}
	}

	// compile map of stored data per host
	contractData := make(map[types.FileContractID]uint64)
	hostData := make(map[types.PublicKey]uint64)
	for _, c := range contracts {
		contractData[c.ID] = c.FileSize()
		hostData[c.HostKey] += c.FileSize()
	}

	// fetch all hosts
	hosts, err := c.ap.bus.Hosts(ctx, 0, -1)
	if err != nil {
		return false, err
	}

	// min score to pass checks.
	var minScore float64
	if len(hosts) > 0 {
		minScore, err = c.managedFindMinAllowedHostScores(ctx, w, hosts, hostData, state.cfg.Contracts.Amount)
		if err != nil {
			return false, fmt.Errorf("failed to determine min score for contract check: %w", err)
		}
	} else {
		c.logger.Warn("could not calculate min score, no hosts found")
	}

	// prepare hosts for cache
	gc := worker.NewGougingChecker(state.gs, state.rs, state.cs, state.fee, state.cfg.Contracts.Period, state.cfg.Contracts.RenewWindow)
	hostInfos := make(map[types.PublicKey]hostInfo)
	for _, h := range hosts {
		// ignore the pricetable's HostBlockHeight by setting it to our own blockheight
		h.PriceTable.HostBlockHeight = state.cs.BlockHeight
		isUsable, unusableResult := isUsableHost(state.cfg, state.rs, gc, h, minScore, hostData[h.PublicKey])
		hostInfos[h.PublicKey] = hostInfo{
			Usable:         isUsable,
			UnusableResult: unusableResult,
		}
	}

	// update cache.
	c.mu.Lock()
	c.cachedHostInfo = hostInfos
	c.cachedDataStored = hostData
	c.cachedMinScore = minScore
	c.mu.Unlock()

	// run checks
	updatedSet, toArchive, toStopUsing, toRefresh, toRenew, err := c.runContractChecks(ctx, w, contracts, isInCurrentSet, minScore)
	if err != nil {
		return false, fmt.Errorf("failed to run contract checks, err: %v", err)
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
		return false, err
	}

	// calculate 'limit' amount of contracts we want to renew
	var limit int
	if len(toRenew) > 0 {
		// when renewing, prioritise contracts that have already been in the set
		// before and out of those prefer the largest ones.
		sort.Slice(toRenew, func(i, j int) bool {
			_, icsI := isInCurrentSet[toRenew[i].contract.ID]
			_, icsJ := isInCurrentSet[toRenew[j].contract.ID]
			if icsI && !icsJ {
				return true
			} else if !icsI && icsJ {
				return false
			}
			return toRenew[i].contract.FileSize() > toRenew[j].contract.FileSize()
		})
		for len(updatedSet)+limit < int(state.cfg.Contracts.Amount) && limit < len(toRenew) {
			// as long as we're missing contracts, increase the renewal limit
			limit++
		}
	}

	// run renewals on contracts that are not in updatedSet yet. We only renew
	// up to 'limit' of those to avoid having too many contracts in the updated
	// set afterwards
	var renewed []renewal
	if limit > 0 {
		var toKeep []contractInfo
		renewed, toKeep = c.runContractRenewals(ctx, w, &remaining, address, toRenew, uint64(limit))
		for _, ri := range renewed {
			updatedSet = append(updatedSet, ri.to)
			contractData[ri.to] = contractData[ri.from]
		}
		for _, ci := range toKeep {
			updatedSet = append(updatedSet, ci.contract.ID)
		}
	}

	// run contract refreshes
	refreshed, err := c.runContractRefreshes(ctx, w, &remaining, address, toRefresh)
	if err != nil {
		c.logger.Errorf("failed to refresh contracts, err: %v", err) // continue
	} else {
		for _, ri := range refreshed {
			updatedSet = append(updatedSet, ri.to)
			contractData[ri.to] = contractData[ri.from]
		}
	}

	// to avoid forming new contracts as soon as we dip below
	// 'Contracts.Amount', we define a threshold but only if we have more
	// contracts than 'Contracts.Amount' already
	threshold := state.cfg.Contracts.Amount
	if uint64(len(contracts)) > state.cfg.Contracts.Amount {
		threshold = addLeeway(threshold, leewayPctRequiredContracts)
	}

	// check if we need to form contracts and add them to the contract set
	var formed []types.FileContractID
	if uint64(len(updatedSet)) < threshold {
		formed, err = c.runContractFormations(ctx, w, hosts, usedHosts, state.cfg.Contracts.Amount-uint64(len(updatedSet)), &remaining, address, minScore)
		if err != nil {
			c.logger.Errorf("failed to form contracts, err: %v", err) // continue
		} else {
			for _, fc := range formed {
				updatedSet = append(updatedSet, fc)
				contractData[fc] = 0
			}
		}
	}

	// cap the amount of contracts we want to keep to the configured amount
	for _, fcid := range updatedSet {
		if _, exists := contractData[fcid]; !exists {
			c.logger.Errorf("contract %v not found in contractData", fcid)
		}
	}
	if len(updatedSet) > int(state.cfg.Contracts.Amount) {
		// sort by contract size
		sort.Slice(updatedSet, func(i, j int) bool {
			return contractData[updatedSet[i]] > contractData[updatedSet[j]]
		})
		for _, c := range updatedSet[state.cfg.Contracts.Amount:] {
			toStopUsing[c] = "truncated"
		}
		updatedSet = updatedSet[:state.cfg.Contracts.Amount]
	}

	// update contract set
	if c.ap.isStopped() {
		return false, errors.New("autopilot stopped before maintenance could be completed")
	}
	err = c.ap.bus.SetContractSet(ctx, state.cfg.Contracts.Set, updatedSet)
	if err != nil {
		return false, err
	}

	// return whether the maintenance changed the contract set
	return c.computeContractSetChanged(currentSet, updatedSet, formed, refreshed, renewed, toStopUsing, contractData), nil
}

func (c *contractor) computeContractSetChanged(oldSet []api.ContractMetadata, newSet, formed []types.FileContractID, refreshed, renewed []renewal, toStopUsing map[types.FileContractID]string, contractData map[types.FileContractID]uint64) bool {
	// build some maps for easier lookups
	previous := make(map[types.FileContractID]struct{})
	for _, c := range oldSet {
		previous[c.ID] = struct{}{}
	}
	updated := make(map[types.FileContractID]struct{})
	for _, c := range newSet {
		updated[c] = struct{}{}
	}
	renewalsFromTo := make(map[types.FileContractID]types.FileContractID)
	renewalsToFrom := make(map[types.FileContractID]types.FileContractID)
	for _, c := range append(refreshed, renewed...) {
		renewalsFromTo[c.from] = c.to
		renewalsToFrom[c.to] = c.from
	}

	// log added and removed contracts
	var added []types.FileContractID
	var removed []types.FileContractID
	for _, contract := range oldSet {
		_, exists := updated[contract.ID]
		_, renewed := updated[renewalsFromTo[contract.ID]]
		if !exists && !renewed {
			removed = append(removed, contract.ID)
			reason, ok := toStopUsing[contract.ID]
			if !ok {
				reason = "unknown"
			}
			c.logger.Debugf("contract %v was removed from the contract set, size: %v, reason: %v", contract.ID, contractData[contract.ID], reason)
		}
	}
	for _, fcid := range newSet {
		_, existed := previous[fcid]
		_, renewed := renewalsToFrom[fcid]
		if !existed && !renewed {
			added = append(added, fcid)
			c.logger.Debugf("contract %v was added to the contract set, size: %v", fcid, contractData[fcid])
		}
	}

	// log renewed contracts that did not make it into the contract set
	for _, fcid := range renewed {
		_, exists := updated[fcid.to]
		if !exists {
			c.logger.Debugf("contract %v was renewed but did not make it into the contract set, size: %v", fcid, contractData[fcid.to])
		}
	}

	// log a warning if the contract set does not contain enough contracts
	logFn := c.logger.Debugw
	if len(newSet) < int(c.ap.State().rs.TotalShards) {
		logFn = c.logger.Warnw
	}

	// log the contract set after maintenance
	logFn(
		"contractset after maintenance",
		"formed", len(formed),
		"renewed", len(renewed),
		"refreshed", len(refreshed),
		"contracts", len(newSet),
		"added", len(added),
		"removed", len(removed),
	)
	return len(added)+len(removed) > 0
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
	cfg := c.ap.State().cfg
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

func (c *contractor) runContractChecks(ctx context.Context, w Worker, contracts []api.Contract, inCurrentSet map[types.FileContractID]struct{}, minScore float64) (toKeep []types.FileContractID, toArchive, toStopUsing map[types.FileContractID]string, toRefresh, toRenew []contractInfo, _ error) {
	if c.ap.isStopped() {
		return
	}
	c.logger.Debug("running contract checks")

	// convenience variables
	state := c.ap.State()

	// calculate 'maxKeepLeeway' which defines the amount of contracts we'll be
	// lenient towards when we fail to either fetch a valid price table or the
	// contract's revision
	maxKeepLeeway := addLeeway(state.cfg.Contracts.Amount, 1-leewayPctRequiredContracts)
	remainingKeepLeeway := maxKeepLeeway

	var notfound int
	defer func() {
		c.logger.Debugw(
			"contracts checks completed",
			"contracts", len(contracts),
			"notfound", notfound,
			"usedKeepLeeway", maxKeepLeeway-remainingKeepLeeway,
			"toKeep", len(toKeep),
			"toArchive", len(toArchive),
			"toRefresh", len(toRefresh),
			"toRenew", len(toRenew),
		)
	}()

	// create a new ip filter
	f := newIPFilter(c.logger)

	// create a gouging checker
	gc := worker.NewGougingChecker(state.gs, state.rs, state.cs, state.fee, state.cfg.Contracts.Period, state.cfg.Contracts.RenewWindow)

	// return variables
	toArchive = make(map[types.FileContractID]string)
	toStopUsing = make(map[types.FileContractID]string)

	// when checking the contracts, do so from largest to smallest. That way, we
	// prefer larger hosts on redundant networks.
	contracts = append([]api.Contract{}, contracts...)
	sort.Slice(contracts, func(i, j int) bool {
		return contracts[i].FileSize() > contracts[j].FileSize()
	})

	// check all contracts
	for _, contract := range contracts {
		// break if autopilot is stopped
		if c.ap.isStopped() {
			break
		}

		// convenience variables
		fcid := contract.ID

		// check if contract is ready to be archived.
		if state.cs.BlockHeight > contract.EndHeight()-c.revisionSubmissionBuffer {
			toArchive[fcid] = errContractExpired.Error()
		} else if contract.Revision != nil && contract.Revision.RevisionNumber == math.MaxUint64 {
			toArchive[fcid] = errContractMaxRevisionNumber.Error()
		} else if contract.RevisionNumber == math.MaxUint64 {
			toArchive[fcid] = errContractMaxRevisionNumber.Error()
		}
		if _, archived := toArchive[fcid]; archived {
			toStopUsing[fcid] = toArchive[fcid]
			continue
		}

		// fetch host from hostdb
		hk := contract.HostKey
		host, err := c.ap.bus.Host(ctx, hk)
		if err != nil {
			c.logger.Errorw(fmt.Sprintf("missing host, err: %v", err), "hk", hk)
			toStopUsing[fcid] = errHostNotFound.Error()
			notfound++
			continue
		}

		// if the host is blocked we ignore it, it might be unblocked later
		if host.Blocked {
			c.logger.Infow("unusable host", "hk", hk, "fcid", fcid, "reasons", errHostBlocked.Error())
			toStopUsing[fcid] = errHostBlocked.Error()
			continue
		}

		// if the host doesn't have a valid pricetable, update it
		var invalidPT bool
		if err := refreshPriceTable(ctx, w, &host.Host); err != nil {
			c.logger.Errorf("could not fetch price table for host %v: %v", host.PublicKey, err)
			invalidPT = true
		}

		// set the host's block height to ours to disable the height check in
		// the gouging checks, in certain edge cases the renter might unsync and
		// would therefor label all hosts as unusable and go on to create a
		// whole new set of contracts with new hosts
		host.PriceTable.HostBlockHeight = state.cs.BlockHeight

		// decide whether the host is still good
		usable, unusableResult := isUsableHost(state.cfg, state.rs, gc, host.Host, minScore, contract.FileSize())
		if !usable {
			reasons := unusableResult.reasons()
			toStopUsing[fcid] = strings.Join(reasons, ",")
			c.logger.Infow("unusable host", "hk", hk, "fcid", fcid, "reasons", reasons)
			continue
		}

		// if we were not able to the contract's revision, we can't properly
		// perform the checks that follow, however we do want to be lenient if
		// this contract is in the current set and we still have leeway left
		if contract.Revision == nil {
			if _, found := inCurrentSet[fcid]; !found || remainingKeepLeeway == 0 {
				toStopUsing[fcid] = errContractNoRevision.Error()
			} else {
				toKeep = append(toKeep, fcid)
				remainingKeepLeeway-- // we let it slide
			}
			continue // can't perform contract checks without revision
		}

		// if we were not able to get a valid price table for the host, but we
		// did pass the host checks, we only want to be lenient if this contract
		// is in the current set and only for a certain number of times,
		// controlled by maxKeepLeeway
		if invalidPT {
			if _, found := inCurrentSet[fcid]; !found || remainingKeepLeeway == 0 {
				toStopUsing[fcid] = "no valid price table"
				continue
			}
			remainingKeepLeeway-- // we let it slide
		}

		// decide whether the contract is still good
		ci := contractInfo{contract: contract, settings: host.Settings}
		renterFunds, err := c.renewFundingEstimate(ctx, ci, state.fee, false)
		if err != nil {
			c.logger.Errorw(fmt.Sprintf("failed to compute renterFunds for contract: %v", err))
		}

		usable, refresh, renew, reasons := isUsableContract(state.cfg, ci, state.cs.BlockHeight, renterFunds, f)
		ci.usable = usable
		if !usable {
			toStopUsing[fcid] = strings.Join(reasons, ",")
			c.logger.Infow(
				"unusable contract",
				"hk", hk,
				"fcid", fcid,
				"reasons", reasons,
				"refresh", refresh,
				"renew", renew,
			)
		}
		if renew {
			toRenew = append(toRenew, ci)
		} else if refresh {
			toRefresh = append(toRefresh, ci)
		} else {
			toKeep = append(toKeep, ci.contract.ID)
		}
	}

	return toKeep, toArchive, toStopUsing, toRefresh, toRenew, nil
}

func (c *contractor) runContractFormations(ctx context.Context, w Worker, hosts []hostdb.Host, usedHosts map[types.PublicKey]struct{}, missing uint64, budget *types.Currency, renterAddress types.Address, minScore float64) ([]types.FileContractID, error) {
	ctx, span := tracing.Tracer.Start(ctx, "runContractFormations")
	defer span.End()

	if c.ap.isStopped() {
		return nil, nil
	}
	var formed []types.FileContractID

	// convenience variables
	state := c.ap.State()

	c.logger.Debugw(
		"run contract formations",
		"usedHosts", len(usedHosts),
		"required", state.cfg.Contracts.Amount,
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

	// fetch candidate hosts
	wanted := int(addLeeway(missing, leewayPctCandidateHosts))
	candidates, _, err := c.candidateHosts(ctx, w, hosts, usedHosts, make(map[types.PublicKey]uint64), wanted, minScore)
	if err != nil {
		return nil, err
	}

	// prepare an IP filter that contains all used hosts
	f := newIPFilter(c.logger)
	for _, h := range hosts {
		if _, used := usedHosts[h.PublicKey]; used {
			_ = f.isRedundantIP(h.NetAddress, h.PublicKey)
		}
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

		// fetch a new price table if necessary
		if err := refreshPriceTable(ctx, w, &host); err != nil {
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

		// check if we already have a contract with a host on that subnet
		if !state.cfg.Hosts.AllowRedundantIPs && f.isRedundantIP(host.NetAddress, host.PublicKey) {
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

func (c *contractor) runContractRenewals(ctx context.Context, w Worker, budget *types.Currency, renterAddress types.Address, toRenew []contractInfo, limit uint64) (renewals []renewal, toKeep []contractInfo) {
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
			"renewals", len(renewals),
			"budget", budget,
		)
	}()

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

		renewed, proceed, err := c.renewContract(ctx, w, ci, budget, renterAddress)
		if err == nil {
			renewals = append(renewals, renewal{from: ci.contract.ID, to: renewed.ID})
			nRenewed++
		} else if ci.usable {
			toKeep = append(toKeep, ci)
			nRenewed++
		}
		if !proceed {
			break
		}
	}
	return renewals, toKeep
}

func (c *contractor) runContractRefreshes(ctx context.Context, w Worker, budget *types.Currency, renterAddress types.Address, toRefresh []contractInfo) (refreshed []renewal, _ error) {
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

		renewed, proceed, err := c.refreshContract(ctx, w, ci, budget, renterAddress)
		if err == nil {
			refreshed = append(refreshed, renewal{from: ci.contract.ID, to: renewed.ID})
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

func (c *contractor) refreshFundingEstimate(ctx context.Context, cfg api.AutopilotConfig, ci contractInfo, fee types.Currency) (types.Currency, error) {
	// refresh with 1.2x the funds
	refreshAmount := ci.contract.TotalCost.Mul64(6).Div64(5)

	// estimate the txn fee
	txnFeeEstimate := fee.Mul64(estimatedFileContractTransactionSetSize)

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

func (c *contractor) renewFundingEstimate(ctx context.Context, ci contractInfo, fee types.Currency, renewing bool) (types.Currency, error) {
	state := c.ap.State()

	// estimate the cost of the current data stored
	dataStored := ci.contract.FileSize()
	storageCost := types.NewCurrency64(dataStored).Mul64(state.cfg.Contracts.Period).Mul(ci.settings.StoragePrice)

	// fetch the spending of the contract we want to renew.
	prevSpending, err := c.contractSpending(ctx, ci.contract, state.period)
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
	newUploadsCost := prevSpending.Uploads.Add(prevUploadDataEstimate.Mul64(state.cfg.Contracts.Period).Mul(ci.settings.StoragePrice))
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
	txnFeeEstimate := fee.Mul64(estimatedFileContractTransactionSetSize)

	// add them all up and then return the estimate plus 33% for error margin
	// and just general volatility of usage pattern.
	estimatedCost := subTotal.Add(siaFundFeeEstimate).Add(txnFeeEstimate)
	estimatedCost = estimatedCost.Add(estimatedCost.Div64(3)) // TODO: arbitrary divisor

	// check for a sane minimum that is equal to the initial contract funding
	// but without an upper cap.
	minInitialContractFunds, _ := initialContractFundingMinMax(state.cfg)
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

func (c *contractor) managedFindMinAllowedHostScores(ctx context.Context, w Worker, hosts []hostdb.Host, storedData map[types.PublicKey]uint64, numContracts uint64) (float64, error) {
	// Pull a new set of hosts from the hostdb that could be used as a new set
	// to match the allowance. The lowest scoring host of these new hosts will
	// be used as a baseline for determining whether our existing contracts are
	// worthwhile.
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
	minScore := lowestScore / minAllowedScoreLeeway
	c.logger.Infow("finished computing minScore",
		"minScore", minScore,
		"lowestScore", lowestScore)
	return minScore, nil
}

func (c *contractor) candidateHosts(ctx context.Context, w Worker, hosts []hostdb.Host, usedHosts map[types.PublicKey]struct{}, storedData map[types.PublicKey]uint64, wanted int, minScore float64) ([]hostdb.Host, []float64, error) {
	c.logger.Debugf("looking for %d candidate hosts", wanted)

	// nothing to do
	if wanted == 0 {
		return nil, nil, nil
	}

	state := c.ap.State()

	// create a gouging checker
	gc := worker.NewGougingChecker(state.gs, state.rs, state.cs, state.fee, state.cfg.Contracts.Period, state.cfg.Contracts.RenewWindow)

	// create list of candidate hosts
	var candidates []hostdb.Host
	var excluded, notcompletedscan int
	for _, h := range hosts {
		// filter out used hosts
		if _, exclude := usedHosts[h.PublicKey]; exclude {
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
		if usable, result := isUsableHost(state.cfg, state.rs, gc, h, minScore, storedData[h.PublicKey]); usable {
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
	if ci.contract.Revision == nil {
		return api.ContractMetadata{}, true, errors.New("can't renew contract without a revision")
	}
	ctx, span := tracing.Tracer.Start(ctx, "renewContract")
	defer span.End()
	defer func() {
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "failed to renew contract")
		}
	}()
	span.SetAttributes(attribute.Stringer("host", ci.contract.HostKey))
	span.SetAttributes(attribute.Stringer("contract", ci.contract.ID))

	// convenience variables
	state := c.ap.State()
	cfg := state.cfg
	cs := state.cs
	contract := ci.contract
	settings := ci.settings
	fcid := contract.ID
	rev := contract.Revision
	hk := contract.HostKey

	// calculate the renter funds
	renterFunds, err := c.renewFundingEstimate(ctx, ci, state.fee, true)
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
	endHeight := endHeight(cfg, state.period)
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
	if ci.contract.Revision == nil {
		return api.ContractMetadata{}, true, errors.New("can't refresh contract without a revision")
	}
	ctx, span := tracing.Tracer.Start(ctx, "refreshContract")
	defer span.End()
	defer func() {
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "failed to refresh contract")
		}
	}()
	span.SetAttributes(attribute.Stringer("host", ci.contract.HostKey))
	span.SetAttributes(attribute.Stringer("contract", ci.contract.ID))

	// convenience variables
	state := c.ap.State()
	contract := ci.contract
	settings := ci.settings
	fcid := contract.ID
	rev := contract.Revision
	hk := contract.HostKey

	// calculate the renter funds
	renterFunds, err := c.refreshFundingEstimate(ctx, state.cfg, ci, state.fee)
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
	expectedStorage := renterFundsToExpectedStorage(renterFunds, contract.EndHeight()-state.cs.BlockHeight, settings)
	newCollateral := rhpv2.ContractRenewalCollateral(rev.FileContract, expectedStorage, settings, state.cs.BlockHeight, contract.EndHeight())

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
	refreshedContract, err := c.ap.bus.AddRenewedContract(ctx, newRevision, renterFunds, state.cs.BlockHeight, contract.ID)
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
	state := c.ap.State()

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
	endHeight := endHeight(state.cfg, state.period)
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

func refreshPriceTable(ctx context.Context, w Worker, host *hostdb.Host) error {
	if !host.Scanned {
		// scan the host if it hasn't been successfully scanned before, which
		// can occur when contracts are added manually to the bus or database
		scan, err := w.RHPScan(ctx, host.PublicKey, host.NetAddress, timeoutHostScan)
		if err != nil {
			return fmt.Errorf("failed to scan host %v: %w", host.PublicKey, err)
		}
		host.Settings = scan.Settings
	} else if !host.PriceTable.Expiry.IsZero() && time.Now().After(host.PriceTable.Expiry) {
		// return the host's pricetable if it's not expired yet
		return nil
	}

	// fetch the price table
	hpt, err := w.RHPPriceTable(ctx, host.PublicKey, host.Settings.SiamuxAddr(), timeoutHostPriceTable)
	if err != nil {
		return fmt.Errorf("failed to fetch price table for host %v: %w", host.PublicKey, err)
	}

	host.PriceTable = hpt
	return nil
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
