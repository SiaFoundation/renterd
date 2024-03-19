package contractor

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/montanaflynn/stats"
	rhpv2 "go.sia.tech/core/rhp/v2"
	rhpv3 "go.sia.tech/core/rhp/v3"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/alerts"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/hostdb"
	"go.sia.tech/renterd/internal/utils"
	"go.sia.tech/renterd/wallet"
	"go.sia.tech/renterd/worker"
	"go.uber.org/zap"
)

const (
	// broadcastRevisionRetriesPerInterval is the number of chances we give a
	// contract that fails to broadcst to be broadcasted again within a single
	// contract broadcast interval.
	broadcastRevisionRetriesPerInterval = 5

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

	// targetBlockTime is the average block time of the Sia network
	targetBlockTime = 10 * time.Minute

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

	// timeoutBroadcastRevision is the amount of time we wait for the broadcast
	// of a revision to succeed.
	timeoutBroadcastRevision = time.Minute
)

type Bus interface {
	AddContract(ctx context.Context, c rhpv2.ContractRevision, contractPrice, totalCost types.Currency, startHeight uint64, state string) (api.ContractMetadata, error)
	AddRenewedContract(ctx context.Context, c rhpv2.ContractRevision, contractPrice, totalCost types.Currency, startHeight uint64, renewedFrom types.FileContractID, state string) (api.ContractMetadata, error)
	AncestorContracts(ctx context.Context, id types.FileContractID, minStartHeight uint64) ([]api.ArchivedContract, error)
	ArchiveContracts(ctx context.Context, toArchive map[types.FileContractID]string) error
	ConsensusState(ctx context.Context) (api.ConsensusState, error)
	Contracts(ctx context.Context, opts api.ContractsOpts) (contracts []api.ContractMetadata, err error)
	FileContractTax(ctx context.Context, payout types.Currency) (types.Currency, error)
	Host(ctx context.Context, hostKey types.PublicKey) (hostdb.HostInfo, error)
	RecordContractSetChurnMetric(ctx context.Context, metrics ...api.ContractSetChurnMetric) error
	SearchHosts(ctx context.Context, opts api.SearchHostOptions) ([]hostdb.HostInfo, error)
	SetContractSet(ctx context.Context, set string, contracts []types.FileContractID) error
}

type Worker interface {
	Contracts(ctx context.Context, hostTimeout time.Duration) (api.ContractsResponse, error)
	RHPBroadcast(ctx context.Context, fcid types.FileContractID) (err error)
	RHPForm(ctx context.Context, endHeight uint64, hk types.PublicKey, hostIP string, renterAddress types.Address, renterFunds types.Currency, hostCollateral types.Currency) (rhpv2.ContractRevision, []types.Transaction, error)
	RHPPriceTable(ctx context.Context, hostKey types.PublicKey, siamuxAddr string, timeout time.Duration) (hostdb.HostPriceTable, error)
	RHPRenew(ctx context.Context, fcid types.FileContractID, endHeight uint64, hk types.PublicKey, hostIP string, hostAddress, renterAddress types.Address, renterFunds, minNewCollateral types.Currency, expectedStorage, windowSize uint64) (api.RHPRenewResponse, error)
	RHPScan(ctx context.Context, hostKey types.PublicKey, hostIP string, timeout time.Duration) (api.RHPScanResponse, error)
}

type (
	Contractor struct {
		alerter  alerts.Alerter
		bus      Bus
		churn    *accumulatedChurn
		resolver *ipResolver
		logger   *zap.SugaredLogger

		revisionBroadcastInterval time.Duration
		revisionLastBroadcast     map[types.FileContractID]time.Time
		revisionSubmissionBuffer  uint64

		mu sync.Mutex

		cachedHostInfo   map[types.PublicKey]hostInfo
		cachedDataStored map[types.PublicKey]uint64
		cachedMinScore   float64

		shutdownCtx       context.Context
		shutdownCtxCancel context.CancelFunc
	}

	hostInfo struct {
		Usable         bool
		UnusableResult unusableHostResult
	}

	scoredHost struct {
		host  hostdb.Host
		score float64
	}

	contractInfo struct {
		contract    api.Contract
		settings    rhpv2.HostSettings
		priceTable  rhpv3.HostPriceTable
		usable      bool
		recoverable bool
		InSet       bool
	}

	contractSetAdditions struct {
		HostKey   types.PublicKey       `json:"hostKey"`
		Additions []contractSetAddition `json:"additions"`
	}

	contractSetAddition struct {
		Size uint64          `json:"size"`
		Time api.TimeRFC3339 `json:"time"`
	}

	contractSetRemovals struct {
		HostKey  types.PublicKey      `json:"hostKey"`
		Removals []contractSetRemoval `json:"removals"`
	}

	contractSetRemoval struct {
		Size   uint64          `json:"size"`
		Reason string          `json:"reasons"`
		Time   api.TimeRFC3339 `json:"time"`
	}

	renewal struct {
		from api.ContractMetadata
		to   api.ContractMetadata
		ci   contractInfo
	}
)

func New(alerter alerts.Alerter, logger *zap.SugaredLogger, revisionSubmissionBuffer uint64, revisionBroadcastInterval time.Duration) *Contractor {
	logger = logger.Named("contractor")
	ctx, cancel := context.WithCancel(context.Background())
	return &Contractor{
		alerter: alerter,
		churn:   newAccumulatedChurn(),
		logger:  logger,

		revisionBroadcastInterval: revisionBroadcastInterval,
		revisionLastBroadcast:     make(map[types.FileContractID]time.Time),
		revisionSubmissionBuffer:  revisionSubmissionBuffer,

		resolver: newIPResolver(ctx, resolverLookupTimeout, logger.Named("resolver")),

		shutdownCtx:       ctx,
		shutdownCtxCancel: cancel,
	}
}

func (c *Contractor) Close() error {
	c.shutdownCtxCancel()
	return nil
}

func canSkipContractMaintenance(ctx context.Context, cfg api.ContractsConfig) (string, bool) {
	select {
	case <-ctx.Done():
		return "", true
	default:
	}

	// no maintenance if no hosts are requested
	//
	// NOTE: this is an important check because we assume Contracts.Amount is
	// not zero in several places
	if cfg.Amount == 0 {
		return "contracts is set to zero, skipping contract maintenance", true
	}

	// no maintenance if no allowance was set
	if cfg.Allowance.IsZero() {
		return "allowance is set to zero, skipping contract maintenance", true
	}

	// no maintenance if no period was set
	if cfg.Period == 0 {
		return "period is set to zero, skipping contract maintenance", true
	}
	return "", false
}

func (c *Contractor) PerformContractMaintenance(ctx context.Context, w Worker, state *MaintenanceState) (bool, error) {
	// check if we can skip maintenance
	if reason, skip := canSkipContractMaintenance(ctx, state.ContractsConfig()); skip {
		if reason != "" {
			c.logger.Warn(reason)
		}
	}
	c.logger.Info("performing contract maintenance")

	// fetch current contract set
	currentSet, err := c.bus.Contracts(ctx, api.ContractsOpts{ContractSet: state.ContractSet()})
	if err != nil && !strings.Contains(err.Error(), api.ErrContractSetNotFound.Error()) {
		return false, err
	}
	isInCurrentSet := make(map[types.FileContractID]struct{})
	for _, c := range currentSet {
		isInCurrentSet[c.ID] = struct{}{}
	}
	c.logger.Debugf("contract set '%s' holds %d contracts", state.ContractSet(), len(currentSet))

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

	// run revision broadcast
	c.runRevisionBroadcast(ctx, w, contracts, isInCurrentSet)

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
	hosts, err := c.bus.SearchHosts(ctx, api.SearchHostOptions{Limit: -1, FilterMode: api.HostFilterModeAllowed})
	if err != nil {
		return false, err
	}

	// check if any used hosts have lost data to warn the user
	var toDismiss []types.Hash256
	for _, h := range hosts {
		if h.Interactions.LostSectors > 0 {
			c.alerter.RegisterAlert(ctx, newLostSectorsAlert(h.PublicKey, h.Interactions.LostSectors))
		} else {
			toDismiss = append(toDismiss, alerts.IDForHost(alertLostSectorsID, h.PublicKey))
		}
	}
	if len(toDismiss) > 0 {
		c.alerter.DismissAlerts(ctx, toDismiss...)
	}

	// fetch candidate hosts
	candidates, unusableHosts, err := c.candidateHosts(ctx, state, hosts, usedHosts, hostData, smallestValidScore) // avoid 0 score hosts
	if err != nil {
		return false, err
	}

	// min score to pass checks
	var minScore float64
	if len(hosts) > 0 {
		minScore = c.calculateMinScore(candidates, state.WantedContracts())
	} else {
		c.logger.Warn("could not calculate min score, no hosts found")
	}

	// fetch consensus state
	cs, err := c.bus.ConsensusState(ctx)
	if err != nil {
		return false, err
	}

	// create gouging checker
	gc := state.GougingChecker(cs)

	// prepare hosts for cache
	hostInfos := make(map[types.PublicKey]hostInfo)
	for _, h := range hosts {
		// ignore the pricetable's HostBlockHeight by setting it to our own blockheight
		h.PriceTable.HostBlockHeight = cs.BlockHeight
		isUsable, unusableResult := isUsableHost(state.ContractsConfig(), state.RS, gc, h, minScore, hostData[h.PublicKey])
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
	updatedSet, toArchive, toStopUsing, toRefresh, toRenew, err := c.runContractChecks(ctx, w, state, contracts, isInCurrentSet, minScore)
	if err != nil {
		return false, fmt.Errorf("failed to run contract checks, err: %v", err)
	}

	// archive contracts
	if len(toArchive) > 0 {
		c.logger.Debugf("archiving %d contracts: %+v", len(toArchive), toArchive)
		if err := c.bus.ArchiveContracts(ctx, toArchive); err != nil {
			c.logger.Errorf("failed to archive contracts, err: %v", err) // continue
		}
	}

	// calculate remaining funds
	remaining := c.remainingFunds(contracts, state)

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
		for len(updatedSet)+limit < int(state.WantedContracts()) && limit < len(toRenew) {
			// as long as we're missing contracts, increase the renewal limit
			limit++
		}
	}

	// run renewals on contracts that are not in updatedSet yet. We only renew
	// up to 'limit' of those to avoid having too many contracts in the updated
	// set afterwards
	var renewed []renewal
	if limit > 0 {
		var toKeep []api.ContractMetadata
		renewed, toKeep = c.runContractRenewals(ctx, w, state, toRenew, &remaining, limit)
		for _, ri := range renewed {
			if ri.ci.usable || ri.ci.recoverable {
				updatedSet = append(updatedSet, ri.to)
			}
			contractData[ri.to.ID] = contractData[ri.from.ID]
		}
		updatedSet = append(updatedSet, toKeep...)
	}

	// run contract refreshes
	refreshed, err := c.runContractRefreshes(ctx, w, state, toRefresh, &remaining)
	if err != nil {
		c.logger.Errorf("failed to refresh contracts, err: %v", err) // continue
	} else {
		for _, ri := range refreshed {
			if ri.ci.usable || ri.ci.recoverable {
				updatedSet = append(updatedSet, ri.to)
			}
			contractData[ri.to.ID] = contractData[ri.from.ID]
		}
	}

	// to avoid forming new contracts as soon as we dip below
	// 'Contracts.Amount', we define a threshold but only if we have more
	// contracts than 'Contracts.Amount' already
	threshold := state.WantedContracts()
	if uint64(len(contracts)) > state.WantedContracts() {
		threshold = addLeeway(threshold, leewayPctRequiredContracts)
	}

	// check if we need to form contracts and add them to the contract set
	var formed []api.ContractMetadata
	if uint64(len(updatedSet)) < threshold && !state.SkipContractFormations {
		formed, err = c.runContractFormations(ctx, w, state, candidates, usedHosts, unusableHosts, state.WantedContracts()-uint64(len(updatedSet)), &remaining)
		if err != nil {
			c.logger.Errorf("failed to form contracts, err: %v", err) // continue
		} else {
			for _, fc := range formed {
				updatedSet = append(updatedSet, fc)
				contractData[fc.ID] = 0
			}
		}
	}

	// cap the amount of contracts we want to keep to the configured amount
	for _, contract := range updatedSet {
		if _, exists := contractData[contract.ID]; !exists {
			c.logger.Errorf("contract %v not found in contractData", contract.ID)
		}
	}
	if len(updatedSet) > int(state.WantedContracts()) {
		// sort by contract size
		sort.Slice(updatedSet, func(i, j int) bool {
			return contractData[updatedSet[i].ID] > contractData[updatedSet[j].ID]
		})
		for _, contract := range updatedSet[state.WantedContracts():] {
			toStopUsing[contract.ID] = "truncated"
		}
		updatedSet = updatedSet[:state.WantedContracts()]
	}

	// convert to set of file contract ids
	var newSet []types.FileContractID
	for _, contract := range updatedSet {
		newSet = append(newSet, contract.ID)
	}

	// update contract set
	err = c.bus.SetContractSet(ctx, state.ContractSet(), newSet)
	if err != nil {
		return false, err
	}

	// return whether the maintenance changed the contract set
	return c.computeContractSetChanged(ctx, state, currentSet, updatedSet, formed, refreshed, renewed, toStopUsing, contractData), nil
}

func (c *Contractor) computeContractSetChanged(ctx context.Context, state *MaintenanceState, oldSet, newSet []api.ContractMetadata, formed []api.ContractMetadata, refreshed, renewed []renewal, toStopUsing map[types.FileContractID]string, contractData map[types.FileContractID]uint64) bool {
	name := state.ContractSet()

	// build set lookups
	inOldSet := make(map[types.FileContractID]struct{})
	for _, c := range oldSet {
		inOldSet[c.ID] = struct{}{}
	}
	inNewSet := make(map[types.FileContractID]struct{})
	for _, c := range newSet {
		inNewSet[c.ID] = struct{}{}
	}

	// build renewal lookups
	renewalsFromTo := make(map[types.FileContractID]types.FileContractID)
	renewalsToFrom := make(map[types.FileContractID]types.FileContractID)
	for _, c := range append(refreshed, renewed...) {
		renewalsFromTo[c.from.ID] = c.to.ID
		renewalsToFrom[c.to.ID] = c.from.ID
	}

	// log added and removed contracts
	setAdditions := make(map[types.FileContractID]contractSetAdditions)
	setRemovals := make(map[types.FileContractID]contractSetRemovals)
	now := api.TimeNow()
	for _, contract := range oldSet {
		_, exists := inNewSet[contract.ID]
		_, renewed := inNewSet[renewalsFromTo[contract.ID]]
		if !exists && !renewed {
			reason, ok := toStopUsing[contract.ID]
			if !ok {
				reason = "unknown"
			}

			if _, exists := setRemovals[contract.ID]; !exists {
				setRemovals[contract.ID] = contractSetRemovals{
					HostKey: contract.HostKey,
				}
			}
			removals := setRemovals[contract.ID]
			removals.Removals = append(removals.Removals, contractSetRemoval{
				Size:   contractData[contract.ID],
				Reason: reason,
				Time:   now,
			})
			setRemovals[contract.ID] = removals
			c.logger.Debugf("contract %v was removed from the contract set, size: %v, reason: %v", contract.ID, contractData[contract.ID], reason)
		}
	}
	for _, contract := range newSet {
		_, existed := inOldSet[contract.ID]
		_, renewed := renewalsToFrom[contract.ID]
		if !existed && !renewed {
			if _, exists := setAdditions[contract.ID]; !exists {
				setAdditions[contract.ID] = contractSetAdditions{
					HostKey: contract.HostKey,
				}
			}
			additions := setAdditions[contract.ID]
			additions.Additions = append(additions.Additions, contractSetAddition{
				Size: contractData[contract.ID],
				Time: now,
			})
			setAdditions[contract.ID] = additions
			c.logger.Debugf("contract %v was added to the contract set, size: %v", contract.ID, contractData[contract.ID])
		}
	}

	// log renewed contracts that did not make it into the contract set
	for _, fcid := range renewed {
		_, exists := inNewSet[fcid.to.ID]
		if !exists {
			c.logger.Debugf("contract %v was renewed but did not make it into the contract set, size: %v", fcid, contractData[fcid.to.ID])
		}
	}

	// log a warning if the contract set does not contain enough contracts
	logFn := c.logger.Debugw
	if len(newSet) < int(state.RS.TotalShards) {
		logFn = c.logger.Warnw
	}

	// record churn metrics
	var metrics []api.ContractSetChurnMetric
	for fcid := range setAdditions {
		metrics = append(metrics, api.ContractSetChurnMetric{
			Name:       state.ContractSet(),
			ContractID: fcid,
			Direction:  api.ChurnDirAdded,
			Timestamp:  now,
		})
	}
	for fcid, removal := range setRemovals {
		metrics = append(metrics, api.ContractSetChurnMetric{
			Name:       state.ContractSet(),
			ContractID: fcid,
			Direction:  api.ChurnDirRemoved,
			Reason:     removal.Removals[0].Reason,
			Timestamp:  now,
		})
	}
	if len(metrics) > 0 {
		if err := c.bus.RecordContractSetChurnMetric(ctx, metrics...); err != nil {
			c.logger.Error("failed to record contract set churn metric:", err)
		}
	}

	// log the contract set after maintenance
	logFn(
		"contractset after maintenance",
		"formed", len(formed),
		"renewed", len(renewed),
		"refreshed", len(refreshed),
		"contracts", len(newSet),
		"added", len(setAdditions),
		"removed", len(setRemovals),
	)
	hasChanged := len(setAdditions)+len(setRemovals) > 0
	if hasChanged {
		if !c.HasAlert(ctx, alertChurnID) {
			c.churn.Reset()
		}
		c.churn.Apply(setAdditions, setRemovals)
		c.alerter.RegisterAlert(ctx, c.churn.Alert(name))
	}
	return hasChanged
}

func (c *Contractor) runContractChecks(ctx context.Context, w Worker, state *MaintenanceState, contracts []api.Contract, inCurrentSet map[types.FileContractID]struct{}, minScore float64) (toKeep []api.ContractMetadata, toArchive, toStopUsing map[types.FileContractID]string, toRefresh, toRenew []contractInfo, _ error) {
	select {
	case <-ctx.Done():
		return
	default:
	}
	c.logger.Debug("running contract checks")

	// fetch consensus state
	cs, err := c.bus.ConsensusState(ctx)
	if err != nil {
		return nil, nil, nil, nil, nil, err
	}

	// create new IP filter
	ipFilter := c.newIPFilter()

	// calculate 'maxKeepLeeway' which defines the amount of contracts we'll be
	// lenient towards when we fail to either fetch a valid price table or the
	// contract's revision
	maxKeepLeeway := addLeeway(state.WantedContracts(), 1-leewayPctRequiredContracts)
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
LOOP:
	for _, contract := range contracts {
		// break if interrupted
		select {
		case <-ctx.Done():
			break LOOP
		default:
		}

		// convenience variables
		fcid := contract.ID

		// check if contract is ready to be archived.
		if cs.BlockHeight > contract.EndHeight()-c.revisionSubmissionBuffer {
			toArchive[fcid] = errContractExpired.Error()
		} else if contract.Revision != nil && contract.Revision.RevisionNumber == math.MaxUint64 {
			toArchive[fcid] = errContractMaxRevisionNumber.Error()
		} else if contract.RevisionNumber == math.MaxUint64 {
			toArchive[fcid] = errContractMaxRevisionNumber.Error()
		} else if contract.State == api.ContractStatePending && cs.BlockHeight-contract.StartHeight > contractConfirmationDeadline {
			toArchive[fcid] = errContractNotConfirmed.Error()
		}
		if _, archived := toArchive[fcid]; archived {
			toStopUsing[fcid] = toArchive[fcid]
			continue
		}

		// fetch host from hostdb
		hk := contract.HostKey
		host, err := c.bus.Host(ctx, hk)
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

		// if the host doesn't have a valid pricetable, update it if we were
		// able to obtain a revision
		invalidPT := contract.Revision == nil
		if contract.Revision != nil {
			if err := refreshPriceTable(ctx, w, &host.Host); err != nil {
				c.logger.Errorf("could not fetch price table for host %v: %v", host.PublicKey, err)
				invalidPT = true
			}
		}

		// refresh the consensus state
		if css, err := c.bus.ConsensusState(ctx); err != nil {
			c.logger.Errorf("could not fetch consensus state, err: %v", err)
		} else {
			cs = css
		}

		// use a new gouging checker for every contract
		gc := state.GougingChecker(cs)

		// set the host's block height to ours to disable the height check in
		// the gouging checks, in certain edge cases the renter might unsync and
		// would therefor label all hosts as unusable and go on to create a
		// whole new set of contracts with new hosts
		host.PriceTable.HostBlockHeight = cs.BlockHeight

		// decide whether the host is still good
		usable, unusableResult := isUsableHost(state.ContractsConfig(), state.RS, gc, host, minScore, contract.FileSize())
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
			} else if !state.AllowRedundantIPs() && ipFilter.IsRedundantIP(contract.HostIP, contract.HostKey) {
				toStopUsing[fcid] = fmt.Sprintf("%v; %v", errHostRedundantIP, errContractNoRevision)
			} else {
				toKeep = append(toKeep, contract.ContractMetadata)
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
		ci := contractInfo{contract: contract, priceTable: host.PriceTable.HostPriceTable, settings: host.Settings}
		usable, recoverable, refresh, renew, reasons := c.isUsableContract(state.AutopilotConfig(), state.RS, ci, cs.BlockHeight, ipFilter)
		ci.usable = usable
		ci.recoverable = recoverable
		if !usable {
			c.logger.Infow(
				"unusable contract",
				"hk", hk,
				"fcid", fcid,
				"reasons", reasons,
				"refresh", refresh,
				"renew", renew,
				"recoverable", recoverable,
			)
		}
		if len(reasons) > 0 {
			toStopUsing[fcid] = strings.Join(reasons, ",")
		}

		if renew {
			toRenew = append(toRenew, ci)
		} else if refresh {
			toRefresh = append(toRefresh, ci)
		} else if usable {
			toKeep = append(toKeep, ci.contract.ContractMetadata)
		}
	}

	return toKeep, toArchive, toStopUsing, toRefresh, toRenew, nil
}

func (c *Contractor) runContractFormations(ctx context.Context, w Worker, state *MaintenanceState, candidates scoredHosts, usedHosts map[types.PublicKey]struct{}, unusableHosts unusableHostResult, missing uint64, budget *types.Currency) (formed []api.ContractMetadata, _ error) {
	select {
	case <-ctx.Done():
		return nil, nil
	default:
	}

	// convenience variables
	shouldFilter := !state.AllowRedundantIPs()

	c.logger.Debugw(
		"run contract formations",
		"usedHosts", len(usedHosts),
		"required", state.WantedContracts(),
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

	// select candidates
	wanted := int(addLeeway(missing, leewayPctCandidateHosts))
	selected := candidates.randSelectByScore(wanted)

	// print warning if we couldn't find enough hosts were found
	c.logger.Debugf("looking for %d candidate hosts", wanted)
	if len(selected) < wanted {
		msg := "no candidate hosts found"
		if len(selected) > 0 {
			msg = fmt.Sprintf("only found %d candidate host(s) out of the %d we wanted", len(selected), wanted)
		}
		if len(candidates) >= wanted {
			c.logger.Warnw(msg, unusableHosts.keysAndValues()...)
		} else {
			c.logger.Debugw(msg, unusableHosts.keysAndValues()...)
		}
	}

	// fetch consensus state
	cs, err := c.bus.ConsensusState(ctx)
	if err != nil {
		return nil, err
	}
	lastStateUpdate := time.Now()

	// prepare a gouging checker
	gc := state.GougingChecker(cs)

	// prepare an IP filter that contains all used hosts
	ipFilter := c.newIPFilter()
	if shouldFilter {
		for _, h := range candidates {
			if _, used := usedHosts[h.host.PublicKey]; used {
				_ = ipFilter.IsRedundantIP(h.host.NetAddress, h.host.PublicKey)
			}
		}
	}

	// calculate min/max contract funds
	minInitialContractFunds, maxInitialContractFunds := initialContractFundingMinMax(state.AutopilotConfig())

LOOP:
	for h := 0; missing > 0 && h < len(selected); h++ {
		host := selected[h].host

		// break if the autopilot is stopped
		select {
		case <-ctx.Done():
			break LOOP
		default:
		}

		// fetch a new price table if necessary
		if err := refreshPriceTable(ctx, w, &host); err != nil {
			c.logger.Errorf("failed to fetch price table for candidate host %v: %v", host.PublicKey, err)
			continue
		}

		// fetch a new consensus state if necessary, we have to do this
		// frequently to ensure we're not performing gouging checks with old
		// consensus state
		if time.Since(lastStateUpdate) > time.Minute {
			if css, err := c.bus.ConsensusState(ctx); err != nil {
				c.logger.Errorf("could not fetch consensus state, err: %v", err)
			} else {
				cs = css
				gc = state.GougingChecker(cs)
			}
		}

		// perform gouging checks on the fly to ensure the host is not gouging its prices
		if breakdown := gc.Check(nil, &host.PriceTable.HostPriceTable); breakdown.Gouging() {
			c.logger.Errorw("candidate host became unusable", "hk", host.PublicKey, "reasons", breakdown.String())
			continue
		}

		// check if we already have a contract with a host on that subnet
		if shouldFilter && ipFilter.IsRedundantIP(host.NetAddress, host.PublicKey) {
			continue
		}

		formedContract, proceed, err := c.formContract(ctx, w, state, host, minInitialContractFunds, maxInitialContractFunds, budget)
		if err == nil {
			// add contract to contract set
			formed = append(formed, formedContract)
			missing--
		}
		if !proceed {
			break
		}
	}

	return formed, nil
}

// runRevisionBroadcast broadcasts contract revisions from the current set of
// contracts. Since we are migrating away from all contracts not in the set and
// are not uploading to those contracts anyway, we only worry about contracts in
// the set.
func (c *Contractor) runRevisionBroadcast(ctx context.Context, w Worker, allContracts []api.Contract, isInSet map[types.FileContractID]struct{}) {
	if c.revisionBroadcastInterval == 0 {
		return // not enabled
	}

	cs, err := c.bus.ConsensusState(ctx)
	if err != nil {
		c.logger.Warnf("revision broadcast failed to fetch blockHeight: %v", err)
		return
	}
	bh := cs.BlockHeight

	successful, failed := 0, 0
	for _, contract := range allContracts {
		// check whether broadcasting is necessary
		timeSinceRevisionHeight := targetBlockTime * time.Duration(bh-contract.RevisionHeight)
		timeSinceLastTry := time.Since(c.revisionLastBroadcast[contract.ID])
		_, inSet := isInSet[contract.ID]
		if !inSet || contract.RevisionHeight == math.MaxUint64 || timeSinceRevisionHeight < c.revisionBroadcastInterval || timeSinceLastTry < c.revisionBroadcastInterval/broadcastRevisionRetriesPerInterval {
			continue // nothing to do
		}

		// remember that we tried to broadcast this contract now
		c.revisionLastBroadcast[contract.ID] = time.Now()

		// ignore contracts for which we weren't able to obtain a revision
		if contract.Revision == nil {
			c.logger.Warnw("failed to broadcast contract revision: failed to fetch revision",
				"hk", contract.HostKey,
				"fcid", contract.ID)
			continue
		}

		// broadcast revision
		ctx, cancel := context.WithTimeout(ctx, timeoutBroadcastRevision)
		err := w.RHPBroadcast(ctx, contract.ID)
		cancel()
		if utils.IsErr(err, errors.New("transaction has a file contract with an outdated revision number")) {
			continue // don't log - revision was already broadcasted
		} else if err != nil {
			c.logger.Warnw(fmt.Sprintf("failed to broadcast contract revision: %v", err),
				"hk", contract.HostKey,
				"fcid", contract.ID)
			failed++
			delete(c.revisionLastBroadcast, contract.ID) // reset to try again
			continue
		}
		successful++
	}
	c.logger.Infow("revision broadcast completed",
		"successful", successful,
		"failed", failed)

	// prune revisionLastBroadcast
	contractMap := make(map[types.FileContractID]struct{})
	for _, contract := range allContracts {
		contractMap[contract.ID] = struct{}{}
	}
	for contractID := range c.revisionLastBroadcast {
		if _, ok := contractMap[contractID]; !ok {
			delete(c.revisionLastBroadcast, contractID)
		}
	}
}

func (c *Contractor) runContractRenewals(ctx context.Context, w Worker, state *MaintenanceState, toRenew []contractInfo, budget *types.Currency, limit int) (renewals []renewal, toKeep []api.ContractMetadata) {
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
			"tokeep", len(toKeep),
			"budget", budget,
		)
	}()

	var i int
	for i = 0; i < len(toRenew); i++ {
		// check if interrupted
		select {
		case <-ctx.Done():
			return
		default:
		}

		// limit the number of contracts to renew
		if len(renewals)+len(toKeep) >= limit {
			break
		}

		// renew and add if it succeeds or if its usable
		contract := toRenew[i].contract.ContractMetadata
		renewed, proceed, err := c.renewContract(ctx, w, state, toRenew[i], budget)
		if err != nil {
			c.alerter.RegisterAlert(ctx, newContractRenewalFailedAlert(contract, !proceed, err))
			if toRenew[i].usable {
				toKeep = append(toKeep, toRenew[i].contract.ContractMetadata)
			}
		} else {
			c.alerter.DismissAlerts(ctx, alerts.IDForContract(alertRenewalFailedID, contract.ID))
			renewals = append(renewals, renewal{from: contract, to: renewed, ci: toRenew[i]})
		}

		// break if we don't want to proceed
		if !proceed {
			break
		}
	}

	// loop through the remaining renewals and add them to the keep list if
	// they're usable and we have 'limit' left
	for j := i; j < len(toRenew); j++ {
		if len(renewals)+len(toKeep) < limit && toRenew[j].usable {
			toKeep = append(toKeep, toRenew[j].contract.ContractMetadata)
		}
	}

	return renewals, toKeep
}

func (c *Contractor) runContractRefreshes(ctx context.Context, w Worker, state *MaintenanceState, toRefresh []contractInfo, budget *types.Currency) (refreshed []renewal, _ error) {
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
		// check if interrupted
		select {
		case <-ctx.Done():
			return
		default:
		}

		// refresh and add if it succeeds
		renewed, proceed, err := c.refreshContract(ctx, w, state, ci, budget)
		if err == nil {
			refreshed = append(refreshed, renewal{from: ci.contract.ContractMetadata, to: renewed, ci: ci})
		}

		// break if we don't want to proceed
		if !proceed {
			break
		}
	}

	return refreshed, nil
}

func (c *Contractor) initialContractFunding(settings rhpv2.HostSettings, txnFee, min, max types.Currency) types.Currency {
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

func (c *Contractor) refreshFundingEstimate(cfg api.AutopilotConfig, ci contractInfo, fee types.Currency) types.Currency {
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
	return refreshAmountCapped
}

func (c *Contractor) renewFundingEstimate(ctx context.Context, state *MaintenanceState, ci contractInfo, fee types.Currency, renewing bool) (types.Currency, error) {
	// estimate the cost of the current data stored
	dataStored := ci.contract.FileSize()
	storageCost := sectorStorageCost(ci.priceTable, state.Period()).Mul64(bytesToSectors(dataStored))

	// fetch the spending of the contract we want to renew.
	prevSpending, err := c.contractSpending(ctx, ci.contract, state.Period())
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
	prevUploadDataEstimate := types.NewCurrency64(dataStored) // default to assuming all data was uploaded
	sectorUploadCost := sectorUploadCost(ci.priceTable, state.Period())
	if !sectorUploadCost.IsZero() {
		prevUploadDataEstimate = prevSpending.Uploads.Div(sectorUploadCost).Mul64(rhpv2.SectorSize)
	}
	if prevUploadDataEstimate.Cmp(types.NewCurrency64(dataStored)) > 0 {
		prevUploadDataEstimate = types.NewCurrency64(dataStored)
	}

	// estimate the
	// - upload cost: previous uploads + prev storage
	// - download cost: assumed to be the same
	// - fund acount cost: assumed to be the same
	newUploadsCost := prevSpending.Uploads.Add(sectorUploadCost.Mul(prevUploadDataEstimate.Div64(rhpv2.SectorSize)))
	newDownloadsCost := prevSpending.Downloads
	newFundAccountCost := prevSpending.FundAccount

	// estimate the siafund fees
	//
	// NOTE: the transaction fees are not included in the siafunds estimate
	// because users are not charged siafund fees on money that doesn't go into
	// the file contract (and the transaction fee goes to the miners, not the
	// file contract).
	subTotal := storageCost.Add(newUploadsCost).Add(newDownloadsCost).Add(newFundAccountCost).Add(ci.settings.ContractPrice)
	siaFundFeeEstimate, err := c.bus.FileContractTax(ctx, subTotal)
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
	minInitialContractFunds, _ := initialContractFundingMinMax(state.AutopilotConfig())
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

func (c *Contractor) calculateMinScore(candidates []scoredHost, numContracts uint64) float64 {
	// return early if there's no hosts
	if len(candidates) == 0 {
		c.logger.Warn("min host score is set to the smallest non-zero float because there are no candidate hosts")
		return smallestValidScore
	}

	// determine the number of random hosts we fetch per iteration when
	// calculating the min score - it contains a constant factor in case the
	// number of contracts is very low and a linear factor to make sure the
	// number is relative to the number of contracts we want to form
	randSetSize := 2*int(numContracts) + 50

	// do multiple rounds to select the lowest score
	var lowestScores []float64
	for r := 0; r < 5; r++ {
		lowestScore := math.MaxFloat64
		for _, host := range scoredHosts(candidates).randSelectByScore(randSetSize) {
			if host.score < lowestScore {
				lowestScore = host.score
			}
		}
		lowestScores = append(lowestScores, lowestScore)
	}

	// compute the min score
	var lowestScore float64
	lowestScore, err := stats.Float64Data(lowestScores).Median()
	if err != nil {
		panic("never fails since len(candidates) > 0 so len(lowestScores) > 0 as well")
	}
	minScore := lowestScore / minAllowedScoreLeeway

	// make sure the min score allows for 'numContracts' contracts to be formed
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].score > candidates[j].score
	})
	if len(candidates) < int(numContracts) {
		return smallestValidScore
	} else if cutoff := candidates[numContracts-1].score; minScore > cutoff {
		minScore = cutoff
	}

	c.logger.Infow("finished computing minScore",
		"candidates", len(candidates),
		"minScore", minScore,
		"numContracts", numContracts,
		"lowestScore", lowestScore)
	return minScore
}

func (c *Contractor) candidateHosts(ctx context.Context, state *MaintenanceState, hosts []hostdb.HostInfo, usedHosts map[types.PublicKey]struct{}, storedData map[types.PublicKey]uint64, minScore float64) ([]scoredHost, unusableHostResult, error) {
	start := time.Now()

	// fetch consensus state
	cs, err := c.bus.ConsensusState(ctx)
	if err != nil {
		return nil, unusableHostResult{}, err
	}

	// create a gouging checker
	gc := state.GougingChecker(cs)

	// select unused hosts that passed a scan
	var unused []hostdb.HostInfo
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
		unused = append(unused, h)
	}

	c.logger.Debugw(fmt.Sprintf("selected %d (potentially) usable hosts for scoring out of %d", len(unused), len(hosts)),
		"excluded", excluded,
		"notcompletedscan", notcompletedscan,
		"used", len(usedHosts))

	// score all unused hosts
	var unusableHostResult unusableHostResult
	var unusable, zeros int
	var candidates []scoredHost
	for _, h := range unused {
		// NOTE: use the price table stored on the host for gouging checks when
		// looking for candidate hosts, fetching the price table on the fly here
		// slows contract maintenance down way too much, we re-evaluate the host
		// right before forming the contract to ensure we do not form a contract
		// with a host that's gouging its prices.
		//
		// NOTE: ignore the pricetable's HostBlockHeight by setting it to our
		// own blockheight
		h.PriceTable.HostBlockHeight = cs.BlockHeight
		usable, result := isUsableHost(state.ContractsConfig(), state.RS, gc, h, minScore, storedData[h.PublicKey])
		if usable {
			candidates = append(candidates, scoredHost{h.Host, result.scoreBreakdown.Score()})
			continue
		}

		// keep track of unusable host results
		unusableHostResult.merge(result)
		if result.scoreBreakdown.Score() == 0 {
			zeros++
		}
		unusable++
	}

	c.logger.Debugw(fmt.Sprintf("scored %d unused hosts out of %v, took %v", len(candidates), len(unused), time.Since(start)),
		"zeroscore", zeros,
		"unusable", unusable,
		"used", len(usedHosts))

	return candidates, unusableHostResult, nil
}

func (c *Contractor) renewContract(ctx context.Context, w Worker, state *MaintenanceState, ci contractInfo, budget *types.Currency) (cm api.ContractMetadata, proceed bool, err error) {
	if ci.contract.Revision == nil {
		return api.ContractMetadata{}, true, errors.New("can't renew contract without a revision")
	}

	// convenience variables
	contract := ci.contract
	settings := ci.settings
	fcid := contract.ID
	rev := contract.Revision
	hk := contract.HostKey

	// fetch consensus state
	cs, err := c.bus.ConsensusState(ctx)
	if err != nil {
		return api.ContractMetadata{}, false, err
	}

	// calculate the renter funds
	renterFunds, err := c.renewFundingEstimate(ctx, state, ci, state.Fee, true)
	if err != nil {
		c.logger.Errorw(fmt.Sprintf("could not get renew funding estimate, err: %v", err), "hk", hk, "fcid", fcid)
		return api.ContractMetadata{}, true, err
	}

	// check our budget
	if budget.Cmp(renterFunds) < 0 {
		c.logger.Debugw("insufficient budget", "budget", budget, "needed", renterFunds)
		return api.ContractMetadata{}, false, errors.New("insufficient budget")
	}

	// sanity check the endheight is not the same on renewals
	endHeight := state.EndHeight()
	if endHeight <= rev.EndHeight() {
		c.logger.Debugw("invalid renewal endheight", "oldEndheight", rev.EndHeight(), "newEndHeight", endHeight, "period", state.Period, "bh", cs.BlockHeight)
		return api.ContractMetadata{}, false, fmt.Errorf("renewal endheight should surpass the current contract endheight, %v <= %v", endHeight, rev.EndHeight())
	}

	// calculate the host collateral
	expectedNewStorage := renterFundsToExpectedStorage(renterFunds, endHeight-cs.BlockHeight, ci.priceTable)

	// renew the contract
	resp, err := w.RHPRenew(ctx, fcid, endHeight, hk, contract.SiamuxAddr, settings.Address, state.Address, renterFunds, types.ZeroCurrency, expectedNewStorage, settings.WindowSize)
	if err != nil {
		c.logger.Errorw(
			"renewal failed",
			zap.Error(err),
			"hk", hk,
			"fcid", fcid,
			"endHeight", endHeight,
			"renterFunds", renterFunds,
			"expectedNewStorage", expectedNewStorage,
		)
		if utils.IsErr(err, wallet.ErrInsufficientBalance) && !worker.IsErrHost(err) {
			return api.ContractMetadata{}, false, err
		}
		return api.ContractMetadata{}, true, err
	}

	// update the budget
	*budget = budget.Sub(renterFunds)

	// persist the contract
	renewedContract, err := c.bus.AddRenewedContract(ctx, resp.Contract, resp.ContractPrice, renterFunds, cs.BlockHeight, fcid, api.ContractStatePending)
	if err != nil {
		c.logger.Errorw(fmt.Sprintf("renewal failed to persist, err: %v", err), "hk", hk, "fcid", fcid)
		return api.ContractMetadata{}, false, err
	}

	newCollateral := resp.Contract.Revision.MissedHostPayout().Sub(resp.ContractPrice)
	c.logger.Debugw(
		"renewal succeeded",
		"fcid", renewedContract.ID,
		"renewedFrom", fcid,
		"renterFunds", renterFunds.String(),
		"newCollateral", newCollateral.String(),
	)
	return renewedContract, true, nil
}

func (c *Contractor) refreshContract(ctx context.Context, w Worker, state *MaintenanceState, ci contractInfo, budget *types.Currency) (cm api.ContractMetadata, proceed bool, err error) {
	if ci.contract.Revision == nil {
		return api.ContractMetadata{}, true, errors.New("can't refresh contract without a revision")
	}

	// convenience variables
	contract := ci.contract
	settings := ci.settings
	fcid := contract.ID
	rev := contract.Revision
	hk := contract.HostKey

	// fetch consensus state
	cs, err := c.bus.ConsensusState(ctx)
	if err != nil {
		return api.ContractMetadata{}, false, err
	}

	// calculate the renter funds
	var renterFunds types.Currency
	if isOutOfFunds(state.AutopilotConfig(), ci.priceTable, ci.contract) {
		renterFunds = c.refreshFundingEstimate(state.AutopilotConfig(), ci, state.Fee)
	} else {
		renterFunds = rev.ValidRenterPayout() // don't increase funds
	}

	// check our budget
	if budget.Cmp(renterFunds) < 0 {
		c.logger.Warnw("insufficient budget for refresh", "hk", hk, "fcid", fcid, "budget", budget, "needed", renterFunds)
		return api.ContractMetadata{}, false, fmt.Errorf("insufficient budget: %s < %s", budget.String(), renterFunds.String())
	}

	expectedStorage := renterFundsToExpectedStorage(renterFunds, contract.EndHeight()-cs.BlockHeight, ci.priceTable)
	unallocatedCollateral := contract.RemainingCollateral()

	// a refresh should always result in a contract that has enough collateral
	minNewCollateral := minRemainingCollateral(state.AutopilotConfig(), state.RS, renterFunds, settings, ci.priceTable).Mul64(2)

	// renew the contract
	resp, err := w.RHPRenew(ctx, contract.ID, contract.EndHeight(), hk, contract.SiamuxAddr, settings.Address, state.Address, renterFunds, minNewCollateral, expectedStorage, settings.WindowSize)
	if err != nil {
		if strings.Contains(err.Error(), "new collateral is too low") {
			c.logger.Debugw("refresh failed: contract wouldn't have enough collateral after refresh",
				"hk", hk,
				"fcid", fcid,
				"unallocatedCollateral", unallocatedCollateral.String(),
				"minNewCollateral", minNewCollateral.String(),
			)
			return api.ContractMetadata{}, true, err
		}
		c.logger.Errorw("refresh failed", zap.Error(err), "hk", hk, "fcid", fcid)
		if utils.IsErr(err, wallet.ErrInsufficientBalance) && !worker.IsErrHost(err) {
			return api.ContractMetadata{}, false, err
		}
		return api.ContractMetadata{}, true, err
	}

	// update the budget
	*budget = budget.Sub(renterFunds)

	// persist the contract
	refreshedContract, err := c.bus.AddRenewedContract(ctx, resp.Contract, resp.ContractPrice, renterFunds, cs.BlockHeight, contract.ID, api.ContractStatePending)
	if err != nil {
		c.logger.Errorw("adding refreshed contract failed", zap.Error(err), "hk", hk, "fcid", fcid)
		return api.ContractMetadata{}, false, err
	}

	// add to renewed set
	newCollateral := resp.Contract.Revision.MissedHostPayout().Sub(resp.ContractPrice)
	c.logger.Debugw("refresh succeeded",
		"fcid", refreshedContract.ID,
		"renewedFrom", contract.ID,
		"renterFunds", renterFunds.String(),
		"minNewCollateral", minNewCollateral.String(),
		"newCollateral", newCollateral.String(),
	)
	return refreshedContract, true, nil
}

func (c *Contractor) formContract(ctx context.Context, w Worker, state *MaintenanceState, host hostdb.Host, minInitialContractFunds, maxInitialContractFunds types.Currency, budget *types.Currency) (cm api.ContractMetadata, proceed bool, err error) {
	// convenience variables
	hk := host.PublicKey

	// fetch host settings
	scan, err := w.RHPScan(ctx, hk, host.NetAddress, 0)
	if err != nil {
		c.logger.Debugw(err.Error(), "hk", hk)
		return api.ContractMetadata{}, true, err
	}

	// fetch consensus state
	cs, err := c.bus.ConsensusState(ctx)
	if err != nil {
		return api.ContractMetadata{}, false, err
	}

	// check our budget
	txnFee := state.Fee.Mul64(estimatedFileContractTransactionSetSize)
	renterFunds := initialContractFunding(scan.Settings, txnFee, minInitialContractFunds, maxInitialContractFunds)
	if budget.Cmp(renterFunds) < 0 {
		c.logger.Debugw("insufficient budget", "budget", budget, "needed", renterFunds)
		return api.ContractMetadata{}, false, errors.New("insufficient budget")
	}

	// calculate the host collateral
	endHeight := state.EndHeight()
	expectedStorage := renterFundsToExpectedStorage(renterFunds, endHeight-cs.BlockHeight, scan.PriceTable)
	hostCollateral := rhpv2.ContractFormationCollateral(state.Period(), expectedStorage, scan.Settings)

	// form contract
	contract, _, err := w.RHPForm(ctx, endHeight, hk, host.NetAddress, state.Address, renterFunds, hostCollateral)
	if err != nil {
		// TODO: keep track of consecutive failures and break at some point
		c.logger.Errorw(fmt.Sprintf("contract formation failed, err: %v", err), "hk", hk)
		if strings.Contains(err.Error(), wallet.ErrInsufficientBalance.Error()) {
			return api.ContractMetadata{}, false, err
		}
		return api.ContractMetadata{}, true, err
	}

	// update the budget
	*budget = budget.Sub(renterFunds)

	// persist contract in store
	contractPrice := contract.Revision.MissedHostPayout().Sub(hostCollateral)
	formedContract, err := c.bus.AddContract(ctx, contract, contractPrice, renterFunds, cs.BlockHeight, api.ContractStatePending)
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

func addLeeway(n uint64, pct float64) uint64 {
	if pct < 0 {
		panic("given leeway percent has to be positive")
	}
	return uint64(math.Ceil(float64(n) * pct))
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

func refreshPriceTable(ctx context.Context, w Worker, host *hostdb.Host) error {
	// return early if the host's pricetable is not expired yet
	if time.Now().Before(host.PriceTable.Expiry) {
		return nil
	}

	// scan the host if it hasn't been successfully scanned before, which
	// can occur when contracts are added manually to the bus or database
	if !host.Scanned {
		scan, err := w.RHPScan(ctx, host.PublicKey, host.NetAddress, timeoutHostScan)
		if err != nil {
			return fmt.Errorf("failed to scan host %v: %w", host.PublicKey, err)
		}
		host.Settings = scan.Settings
	}

	// fetch the price table
	hpt, err := w.RHPPriceTable(ctx, host.PublicKey, host.Settings.SiamuxAddr(), timeoutHostPriceTable)
	if err != nil {
		return fmt.Errorf("failed to fetch price table for host %v: %w", host.PublicKey, err)
	}

	host.PriceTable = hpt
	return nil
}

// renterFundsToExpectedStorage returns how much storage a renter is expected to
// be able to afford given the provided 'renterFunds'.
func renterFundsToExpectedStorage(renterFunds types.Currency, duration uint64, pt rhpv3.HostPriceTable) uint64 {
	costPerSector := sectorUploadCost(pt, duration)
	// Handle free storage.
	if costPerSector.IsZero() {
		costPerSector = types.NewCurrency64(1)
	}
	// Catch overflow.
	expectedStorage := renterFunds.Div(costPerSector).Mul64(rhpv2.SectorSize)
	if expectedStorage.Cmp(types.NewCurrency64(math.MaxUint64)) > 0 {
		expectedStorage = types.NewCurrency64(math.MaxUint64)
	}
	return expectedStorage.Big().Uint64()
}

func (c *Contractor) HasAlert(ctx context.Context, id types.Hash256) bool {
	ar, err := c.alerter.Alerts(ctx, alerts.AlertsOpts{Offset: 0, Limit: -1})
	if err != nil {
		c.logger.Errorf("failed to fetch alerts: %v", err)
		return false
	}
	for _, alert := range ar.Alerts {
		if alert.ID == id {
			return true
		}
	}
	return false
}