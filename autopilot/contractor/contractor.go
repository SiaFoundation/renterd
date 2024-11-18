package contractor

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/montanaflynn/stats"
	"go.sia.tech/core/consensus"
	rhpv2 "go.sia.tech/core/rhp/v2"
	rhpv3 "go.sia.tech/core/rhp/v3"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/wallet"
	"go.sia.tech/renterd/alerts"
	"go.sia.tech/renterd/api"
	rhp3 "go.sia.tech/renterd/internal/rhp/v3"
	"go.sia.tech/renterd/internal/utils"
	"go.uber.org/zap"
	"lukechampine.com/frand"
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

	// failedRenewalForgivenessPeriod is the amount of time we wait before
	// punishing a contract for not being able to refresh
	failedRefreshForgivenessPeriod = 24 * time.Hour

	// leewayPctRequiredContracts is the leeway we apply on the amount of
	// contracts the config dictates we should have, we'll only form new
	// contracts if the number of contracts dips below 90% of the required
	// contracts
	//
	// NOTE: updating this value indirectly affects 'maxKeepLeeway'
	leewayPctRequiredContracts = 0.9

	// minAllowedScoreLeeway is a factor by which a host can be under the lowest
	// score found in a random sample of scores before being considered not
	// usable.
	minAllowedScoreLeeway = 500

	// targetBlockTime is the average block time of the Sia network
	targetBlockTime = 10 * time.Minute

	// timeoutHostRevision is the amount of time we wait to receive the latest
	// revision from the host. This is set to 4 minutes since siad currently
	// blocks for 3 minutes when trying to fetch a revision and not having
	// enough funds in the account used for fetching it. That way we are
	// guaranteed to receive the host's ErrBalanceInsufficient.
	// TODO: This can be lowered once the network uses hostd.
	timeoutHostRevision = 4 * time.Minute

	// timeoutBroadcastRevision is the amount of time we wait for the broadcast
	// of a revision to succeed.
	timeoutBroadcastRevision = time.Minute
)

var (
	InitialContractFunding = types.Siacoins(10)
)

type Bus interface {
	HostScanner

	AncestorContracts(ctx context.Context, id types.FileContractID, minStartHeight uint64) ([]api.ContractMetadata, error)
	ArchiveContracts(ctx context.Context, toArchive map[types.FileContractID]string) error
	BroadcastContract(ctx context.Context, fcid types.FileContractID) (types.TransactionID, error)
	ConsensusState(ctx context.Context) (api.ConsensusState, error)
	ConsensusNetwork(ctx context.Context) (consensus.Network, error)
	Contract(ctx context.Context, id types.FileContractID) (api.ContractMetadata, error)
	Contracts(ctx context.Context, opts api.ContractsOpts) (contracts []api.ContractMetadata, err error)
	FileContractTax(ctx context.Context, payout types.Currency) (types.Currency, error)
	FormContract(ctx context.Context, renterAddress types.Address, renterFunds types.Currency, hostKey types.PublicKey, hostCollateral types.Currency, endHeight uint64) (api.ContractMetadata, error)
	ContractRevision(ctx context.Context, fcid types.FileContractID) (api.Revision, error)
	RenewContract(ctx context.Context, fcid types.FileContractID, endHeight uint64, renterFunds, minNewCollateral types.Currency, expectedNewStorage uint64) (api.ContractMetadata, error)
	Host(ctx context.Context, hostKey types.PublicKey) (api.Host, error)
	Hosts(ctx context.Context, opts api.HostOptions) ([]api.Host, error)
	RecordContractSetChurnMetric(ctx context.Context, metrics ...api.ContractSetChurnMetric) error
	UpdateContractSet(ctx context.Context, set string, toAdd, toRemove []types.FileContractID) error
	UpdateHostCheck(ctx context.Context, autopilotID string, hostKey types.PublicKey, hostCheck api.HostCheck) error
}

type HostScanner interface {
	ScanHost(ctx context.Context, hostKey types.PublicKey, timeout time.Duration) (api.HostScanResponse, error)
}

type contractChecker interface {
	isUsableContract(cfg api.AutopilotConfig, s rhpv2.HostSettings, pt rhpv3.HostPriceTable, rs api.RedundancySettings, contract contract, inSet bool, bh uint64, f *hostSet) (usable, refresh, renew bool, reasons []string)
	pruneContractRefreshFailures(contracts []api.ContractMetadata)
	shouldArchive(c contract, bh uint64, network consensus.Network) error
}

type contractReviser interface {
	formContract(ctx *mCtx, hs HostScanner, host api.Host, minInitialContractFunds types.Currency, logger *zap.SugaredLogger) (cm api.ContractMetadata, ourFault bool, err error)
	renewContract(ctx *mCtx, c contract, h api.Host, logger *zap.SugaredLogger) (cm api.ContractMetadata, ourFault bool, err error)
	refreshContract(ctx *mCtx, c contract, h api.Host, logger *zap.SugaredLogger) (cm api.ContractMetadata, ourFault bool, err error)
}

type revisionBroadcaster interface {
	broadcastRevisions(ctx context.Context, contracts []api.ContractMetadata, logger *zap.SugaredLogger)
}

type (
	Contractor struct {
		alerter alerts.Alerter
		bus     Bus
		churn   *accumulatedChurn
		logger  *zap.SugaredLogger

		revisionBroadcastInterval time.Duration
		revisionLastBroadcast     map[types.FileContractID]time.Time
		revisionSubmissionBuffer  uint64

		firstRefreshFailure map[types.FileContractID]time.Time
	}

	scoredHost struct {
		host  api.Host
		sb    api.HostScoreBreakdown
		score float64
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
)

func New(bus Bus, alerter alerts.Alerter, logger *zap.SugaredLogger, revisionSubmissionBuffer uint64, revisionBroadcastInterval time.Duration) *Contractor {
	logger = logger.Named("contractor")
	return &Contractor{
		bus:     bus,
		alerter: alerter,
		churn:   newAccumulatedChurn(),
		logger:  logger,

		revisionBroadcastInterval: revisionBroadcastInterval,
		revisionLastBroadcast:     make(map[types.FileContractID]time.Time),
		revisionSubmissionBuffer:  revisionSubmissionBuffer,

		firstRefreshFailure: make(map[types.FileContractID]time.Time),
	}
}

func (c *Contractor) PerformContractMaintenance(ctx context.Context, state *MaintenanceState) (bool, error) {
	return performContractMaintenance(newMaintenanceCtx(ctx, state), c.alerter, c.bus, c.churn, c, c, c, c.logger)
}

func (c *Contractor) formContract(ctx *mCtx, hs HostScanner, host api.Host, minInitialContractFunds types.Currency, logger *zap.SugaredLogger) (cm api.ContractMetadata, proceed bool, err error) {
	logger = logger.With("hk", host.PublicKey, "hostVersion", host.Settings.Version, "hostRelease", host.Settings.Release)

	// convenience variables
	hk := host.PublicKey

	// fetch host settings
	scan, err := hs.ScanHost(ctx, hk, 0)
	if err != nil {
		logger.Infow(err.Error(), "hk", hk)
		return api.ContractMetadata{}, true, err
	}

	// fetch consensus state
	cs, err := c.bus.ConsensusState(ctx)
	if err != nil {
		return api.ContractMetadata{}, false, err
	}

	// version specific costs
	var contractPrice, collateral, maxCollateral types.Currency
	if host.IsV2() {
		contractPrice = scan.V2Settings.Prices.ContractPrice
		collateral = scan.V2Settings.Prices.Collateral
		maxCollateral = scan.V2Settings.MaxCollateral
	} else {
		contractPrice = scan.Settings.ContractPrice
		collateral = scan.Settings.Collateral
		maxCollateral = scan.Settings.MaxCollateral
	}

	// check our budget
	txnFee := ctx.state.Fee.Mul64(estimatedFileContractTransactionSetSize)
	renterFunds := initialContractFunding(contractPrice, txnFee, minInitialContractFunds)

	// calculate the host collateral
	endHeight := ctx.EndHeight()
	expectedStorage := renterFundsToExpectedStorage(renterFunds, endHeight-cs.BlockHeight, scan.PriceTable)

	// figure out how much collateral we want the host to pu into the contract
	hostCollateral := collateral.Mul64(expectedStorage).Mul64(ctx.Period())
	if collateral.Cmp(maxCollateral) > 0 {
		collateral = maxCollateral
	}

	// form contract
	contract, err := c.bus.FormContract(ctx, ctx.state.Address, renterFunds, hk, hostCollateral, endHeight)
	if err != nil {
		// TODO: keep track of consecutive failures and break at some point
		logger.Errorw(fmt.Sprintf("contract formation failed, err: %v", err), "hk", hk)
		if utils.IsErr(err, wallet.ErrNotEnoughFunds) {
			return api.ContractMetadata{}, false, err
		}
		return api.ContractMetadata{}, true, err
	}

	logger.Infow("formation succeeded",
		"fcid", contract.ID,
		"renterFunds", renterFunds.String(),
		"collateral", hostCollateral.String(),
	)
	return contract, true, nil
}

func (c *Contractor) pruneContractRefreshFailures(contracts []api.ContractMetadata) {
	contractMap := make(map[types.FileContractID]struct{})
	for _, contract := range contracts {
		contractMap[contract.ID] = struct{}{}
	}
	for fcid := range c.firstRefreshFailure {
		if _, ok := contractMap[fcid]; !ok {
			delete(c.firstRefreshFailure, fcid)
		}
	}
}

func (c *Contractor) refreshContract(ctx *mCtx, contract contract, host api.Host, logger *zap.SugaredLogger) (cm api.ContractMetadata, proceed bool, err error) {
	if contract.Revision == nil {
		return api.ContractMetadata{}, true, errors.New("can't refresh contract without a revision")
	}
	logger = logger.With("to_renew", contract.ID, "hk", contract.HostKey, "hostVersion", host.Settings.Version, "hostRelease", host.Settings.Release)

	// convenience variables
	settings := host.Settings
	pt := host.PriceTable.HostPriceTable
	fcid := contract.ID
	hk := contract.HostKey
	rev := contract.Revision

	// fetch consensus state
	cs, err := c.bus.ConsensusState(ctx)
	if err != nil {
		return api.ContractMetadata{}, false, err
	}

	// calculate the renter funds
	var renterFunds types.Currency
	if isOutOfFunds(ctx.AutopilotConfig(), pt, contract) {
		renterFunds = c.refreshFundingEstimate(contract, logger)
	} else {
		renterFunds = rev.RenterOutput.Value // don't increase funds
	}

	expectedNewStorage := renterFundsToExpectedStorage(renterFunds, contract.EndHeight()-cs.BlockHeight, pt)
	unallocatedCollateral := contract.RemainingCollateral()

	// a refresh should always result in a contract that has enough collateral
	minNewCollateral := minRemainingCollateral(ctx.AutopilotConfig(), ctx.state.RS, renterFunds, settings, pt).Mul64(2)

	// renew the contract
	renewal, err := c.bus.RenewContract(ctx, contract.ID, contract.EndHeight(), renterFunds, minNewCollateral, expectedNewStorage)
	if err != nil {
		if strings.Contains(err.Error(), "new collateral is too low") {
			logger.Infow("refresh failed: contract wouldn't have enough collateral after refresh",
				"hk", hk,
				"fcid", fcid,
				"unallocatedCollateral", unallocatedCollateral.String(),
				"minNewCollateral", minNewCollateral.String(),
			)
			return api.ContractMetadata{}, true, err
		}
		logger.Errorw("refresh failed", zap.Error(err), "hk", hk, "fcid", fcid)
		if utils.IsErr(err, wallet.ErrNotEnoughFunds) && !rhp3.IsErrHost(err) {
			return api.ContractMetadata{}, false, err
		}
		return api.ContractMetadata{}, true, err
	}

	// add to renewed set
	logger.Infow("refresh succeeded",
		"fcid", renewal.ID,
		"renewedFrom", renewal.RenewedFrom,
		"renterFunds", renterFunds.String(),
		"minNewCollateral", minNewCollateral.String(),
	)
	return renewal, true, nil
}

func (c *Contractor) renewContract(ctx *mCtx, contract contract, host api.Host, logger *zap.SugaredLogger) (cm api.ContractMetadata, proceed bool, err error) {
	if contract.Revision == nil {
		return api.ContractMetadata{}, true, errors.New("can't renew contract without a revision")
	}
	logger = logger.With("to_renew", contract.ID, "hk", contract.HostKey, "hostVersion", host.Settings.Version, "hostRelease", host.Settings.Release)

	// convenience variables
	pt := host.PriceTable.HostPriceTable
	fcid := contract.ID
	rev := contract.Revision

	// fetch consensus state
	cs, err := c.bus.ConsensusState(ctx)
	if err != nil {
		return api.ContractMetadata{}, false, err
	}

	// calculate the renter funds for the renewal a.k.a. the funds the renter will
	// be able to spend
	minRenterFunds := InitialContractFunding
	renterFunds := renewFundingEstimate(minRenterFunds, contract.InitialRenterFunds, contract.RenterFunds(), logger)

	// sanity check the endheight is not the same on renewals
	endHeight := ctx.EndHeight()
	if endHeight <= rev.ProofHeight {
		logger.Infow("invalid renewal endheight", "oldEndheight", rev.EndHeight(), "newEndHeight", endHeight, "period", ctx.state.Period, "bh", cs.BlockHeight)
		return api.ContractMetadata{}, false, fmt.Errorf("renewal endheight should surpass the current contract endheight, %v <= %v", endHeight, rev.EndHeight())
	}

	// calculate the expected new storage
	expectedNewStorage := renterFundsToExpectedStorage(renterFunds, endHeight-cs.BlockHeight, pt)

	// renew the contract
	renewal, err := c.bus.RenewContract(ctx, fcid, endHeight, renterFunds, types.ZeroCurrency, expectedNewStorage)
	if err != nil {
		logger.Errorw(
			"renewal failed",
			zap.Error(err),
			"endHeight", endHeight,
			"renterFunds", renterFunds,
			"expectedNewStorage", expectedNewStorage,
		)
		if utils.IsErr(err, wallet.ErrNotEnoughFunds) && !rhp3.IsErrHost(err) {
			return api.ContractMetadata{}, false, err
		}
		return api.ContractMetadata{}, true, err
	}

	logger.Infow(
		"renewal succeeded",
		"fcid", renewal.ID,
		"renewedFrom", renewal.RenewedFrom,
		"renterFunds", renterFunds.String(),
	)
	return renewal, true, nil
}

// broadcastRevisions broadcasts contract revisions from the current set of
// contracts. Since we are migrating away from all contracts not in the set and
// are not uploading to those contracts anyway, we only worry about contracts in
// the set.
func (c *Contractor) broadcastRevisions(ctx context.Context, contracts []api.ContractMetadata, logger *zap.SugaredLogger) {
	if c.revisionBroadcastInterval == 0 {
		return // not enabled
	}

	cs, err := c.bus.ConsensusState(ctx)
	if err != nil {
		logger.Warnf("revision broadcast failed to fetch blockHeight: %v", err)
		return
	}
	bh := cs.BlockHeight

	successful, failed := 0, 0
	for _, contract := range contracts {
		// check whether broadcasting is necessary
		timeSinceRevisionHeight := targetBlockTime * time.Duration(bh-contract.RevisionHeight)
		timeSinceLastTry := time.Since(c.revisionLastBroadcast[contract.ID])
		if contract.RevisionHeight == math.MaxUint64 || timeSinceRevisionHeight < c.revisionBroadcastInterval || timeSinceLastTry < c.revisionBroadcastInterval/broadcastRevisionRetriesPerInterval {
			continue // nothing to do
		}

		// remember that we tried to broadcast this contract now
		c.revisionLastBroadcast[contract.ID] = time.Now()

		// broadcast revision
		ctx, cancel := context.WithTimeout(ctx, timeoutBroadcastRevision)
		_, err := c.bus.BroadcastContract(ctx, contract.ID)
		cancel()
		if utils.IsErr(err, errors.New("transaction has a file contract with an outdated revision number")) {
			continue // don't log - revision was already broadcasted
		} else if err != nil {
			logger.Warnw(fmt.Sprintf("failed to broadcast contract revision: %v", err),
				"hk", contract.HostKey,
				"fcid", contract.ID)
			failed++
			delete(c.revisionLastBroadcast, contract.ID) // reset to try again
			continue
		}
		successful++
	}
	logger.Infow("revision broadcast completed",
		"successful", successful,
		"failed", failed)

	// prune revisionLastBroadcast
	contractMap := make(map[types.FileContractID]struct{})
	for _, contract := range contracts {
		contractMap[contract.ID] = struct{}{}
	}
	for contractID := range c.revisionLastBroadcast {
		if _, ok := contractMap[contractID]; !ok {
			delete(c.revisionLastBroadcast, contractID)
		}
	}
}

func (c *Contractor) refreshFundingEstimate(contract contract, logger *zap.SugaredLogger) types.Currency {
	// refresh with 1.2x the funds
	refreshAmount := contract.InitialRenterFunds.Mul64(6).Div64(5)

	// check for a sane minimum that is equal to the initial contract funding
	// but without an upper cap.
	minimum := InitialContractFunding
	refreshAmountCapped := refreshAmount
	if refreshAmountCapped.Cmp(minimum) < 0 {
		refreshAmountCapped = minimum
	}
	logger.Infow("refresh estimate",
		"fcid", contract.ID,
		"refreshAmount", refreshAmount,
		"refreshAmountCapped", refreshAmountCapped)
	return refreshAmountCapped
}

func (c *Contractor) shouldArchive(contract contract, bh uint64, n consensus.Network) (err error) {
	if bh > contract.EndHeight()-c.revisionSubmissionBuffer {
		return errContractExpired
	} else if contract.Revision != nil && contract.Revision.RevisionNumber == math.MaxUint64 {
		return errContractMaxRevisionNumber
	} else if contract.RevisionNumber == math.MaxUint64 {
		return errContractMaxRevisionNumber
	} else if contract.State == api.ContractStatePending && bh-contract.StartHeight > ContractConfirmationDeadline {
		return errContractNotConfirmed
	} else if !contract.V2 && bh >= n.HardforkV2.RequireHeight {
		return errContractBeyondV2RequireHeight
	}
	return nil
}

func (c *Contractor) shouldForgiveFailedRefresh(fcid types.FileContractID) bool {
	lastFailure, exists := c.firstRefreshFailure[fcid]
	if !exists {
		lastFailure = time.Now()
		c.firstRefreshFailure[fcid] = lastFailure
	}
	return time.Since(lastFailure) < failedRefreshForgivenessPeriod
}

func addLeeway(n uint64, pct float64) uint64 {
	if pct < 0 {
		panic("given leeway percent has to be positive")
	}
	return uint64(math.Ceil(float64(n) * pct))
}

func calculateMinScore(candidates []scoredHost, numContracts uint64, logger *zap.SugaredLogger) float64 {
	logger = logger.Named("calculateMinScore")

	// return early if there's no hosts
	if len(candidates) == 0 {
		logger.Warn("min host score is set to the smallest non-zero float because there are no candidate hosts")
		return minValidScore
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
			if score := host.score; score < lowestScore && score > 0 {
				lowestScore = score
			}
		}
		if lowestScore != math.MaxFloat64 {
			lowestScores = append(lowestScores, lowestScore)
		}
	}
	if len(lowestScores) == 0 {
		logger.Warn("min host score is set to the smallest non-zero float because the lowest score couldn't be determined")
		return minValidScore
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
		return minValidScore
	} else if cutoff := candidates[numContracts-1].score; minScore > cutoff {
		minScore = cutoff
	}

	logger.Infow("finished computing minScore",
		"candidates", len(candidates),
		"minScore", minScore,
		"numContracts", numContracts,
		"lowestScore", lowestScore)
	return minScore
}

func canSkipContractMaintenance(ctx context.Context, cfg api.ContractsConfig) (string, bool) {
	select {
	case <-ctx.Done():
		return "interrupted", true
	default:
	}

	// no maintenance if no hosts are requested
	//
	// NOTE: this is an important check because we assume Contracts.Amount is
	// not zero in several places
	if cfg.Amount == 0 {
		return "contracts is set to zero, skipping contract maintenance", true
	}

	// no maintenance if no period was set
	if cfg.Period == 0 {
		return "period is set to zero, skipping contract maintenance", true
	}
	return "", false
}

func computeContractSetChanged(ctx *mCtx, alerter alerts.Alerter, bus Bus, churn *accumulatedChurn, logger *zap.SugaredLogger, oldSet, newSet []api.ContractMetadata, toStopUsing map[types.FileContractID]string) (bool, error) {
	name := ctx.ContractSet()

	allContracts, err := bus.Contracts(ctx, api.ContractsOpts{})
	if err != nil {
		return false, fmt.Errorf("failed to fetch all contracts: %w", err)
	}
	contractData := make(map[types.FileContractID]uint64)
	for _, c := range allContracts {
		contractData[c.ID] = c.Size
	}

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
	for _, c := range allContracts {
		if c.RenewedFrom != (types.FileContractID{}) {
			renewalsFromTo[c.RenewedFrom] = c.ID
			renewalsToFrom[c.ID] = c.RenewedFrom
		}
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
			logger.Infof("contract %v was removed from the contract set, size: %v, reason: %v", contract.ID, contractData[contract.ID], reason)
		}
	}
	for _, contract := range newSet {
		_, existed := inOldSet[contract.ID]
		_, renewed := inOldSet[renewalsToFrom[contract.ID]]
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
			logger.Infof("contract %v was added to the contract set, size: %v", contract.ID, contractData[contract.ID])
		}
	}

	// log a warning if the contract set does not contain enough contracts
	logFn := logger.Infow
	if len(newSet) < int(ctx.state.RS.TotalShards) {
		logFn = logger.Warnw
	}

	// record churn metrics
	var metrics []api.ContractSetChurnMetric
	for fcid := range setAdditions {
		metrics = append(metrics, api.ContractSetChurnMetric{
			Name:       ctx.ContractSet(),
			ContractID: fcid,
			Direction:  api.ChurnDirAdded,
			Timestamp:  now,
		})
	}
	for fcid, removal := range setRemovals {
		metrics = append(metrics, api.ContractSetChurnMetric{
			Name:       ctx.ContractSet(),
			ContractID: fcid,
			Direction:  api.ChurnDirRemoved,
			Reason:     removal.Removals[0].Reason,
			Timestamp:  now,
		})
	}
	if len(metrics) > 0 {
		if err := bus.RecordContractSetChurnMetric(ctx, metrics...); err != nil {
			logger.Error("failed to record contract set churn metric:", err)
		}
	}

	// log the contract set after maintenance
	logFn(
		"contractset after maintenance",
		"contracts", len(newSet),
		"added", len(setAdditions),
		"removed", len(setRemovals),
	)

	hasAlert := func(id types.Hash256) bool {
		ar, err := alerter.Alerts(ctx, alerts.AlertsOpts{Offset: 0, Limit: -1})
		if err != nil {
			logger.Errorf("failed to fetch alerts: %v", err)
			return false
		}
		for _, alert := range ar.Alerts {
			if alert.ID == id {
				return true
			}
		}
		return false
	}

	hasChanged := len(setAdditions)+len(setRemovals) > 0
	if hasChanged {
		if !hasAlert(alertChurnID) {
			churn.Reset()
		}
		churn.Apply(setAdditions, setRemovals)
		alerter.RegisterAlert(ctx, churn.Alert(name))
	}
	return hasChanged, nil
}

func initialContractFunding(contractPrice, txnFee, minFunding types.Currency) types.Currency {
	funding := contractPrice.Add(txnFee).Mul64(10) // TODO arbitrary multiplier
	if !minFunding.IsZero() && funding.Cmp(minFunding) < 0 {
		return minFunding
	}
	return funding
}

// renewFundingEstimate computes the funds the renter should use to renew a
// contract. 'minRenterFunds' is the minimum amount the renter should use to
// renew a contract, 'initRenterFunds' is the amount the renter used to form the
// contract we are about to renew, and 'remainingRenterFunds' is the amount the
// contract currently has left.
func renewFundingEstimate(minRenterFunds, initRenterFunds, remainingRenterFunds types.Currency, log *zap.SugaredLogger) types.Currency {
	log = log.With("minRenterFunds", minRenterFunds, "initRenterFunds", initRenterFunds, "remainingRenterFunds", remainingRenterFunds)

	// compute the funds used
	usedFunds := types.ZeroCurrency
	if initRenterFunds.Cmp(remainingRenterFunds) >= 0 {
		usedFunds = initRenterFunds.Sub(remainingRenterFunds)
	}
	log = log.With("usedFunds", usedFunds)

	var renterFunds types.Currency
	if usedFunds.IsZero() {
		// if no funds were used, we use a fraction of the previous funding
		log.Info("no funds were used, using half the funding from before")
		renterFunds = initRenterFunds.Div64(2) // half the funds from before
	} else {
		// otherwise we use the remaining funds from before because a renewal
		// shouldn't add more funds, that's what a refresh is for
		renterFunds = remainingRenterFunds
	}

	// but the funds should not drop below the amount we'd fund a new contract with
	if renterFunds.Cmp(minRenterFunds) < 0 {
		log.Info("funds would drop below the minimum, using the minimum")
		renterFunds = minRenterFunds
	}
	return renterFunds
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

// performContractChecks performs maintenance on existing contracts,
// renewing/refreshing any that need it and filtering out contracts that should
// no longer be used. The 'ipFilter' is updated to contain all hosts that we
// keep contracts with and the 'dropOutReasons' map is updated with the reasons
// for dropping out of the set. If a contract is refreshed or renewed, the
// 'remainingFunds' are adjusted.
func performContractChecks(ctx *mCtx, alerter alerts.Alerter, bus Bus, cc contractChecker, cr contractReviser, ipFilter *hostSet, logger *zap.SugaredLogger) ([]api.ContractMetadata, map[types.FileContractID]string, error) {
	var filteredContracts []api.ContractMetadata
	keepContract := func(c api.ContractMetadata, h api.Host) {
		filteredContracts = append(filteredContracts, c)
		ipFilter.Add(ctx, h)
	}
	churnReasons := make(map[types.FileContractID]string)

	// fetch network
	network, err := bus.ConsensusNetwork(ctx)
	if err != nil {
		return nil, nil, err
	}

	// fetch all contracts we already have
	logger.Info("fetching existing contracts")
	start := time.Now()
	contractMetadatas, err := bus.Contracts(ctx, api.ContractsOpts{FilterMode: api.ContractFilterModeAll})
	if err != nil {
		return nil, nil, err
	}
	logger.With("elapsed", time.Since(start)).Info("done fetching existing contracts")

	// fetch the revision for each contract
	var wg sync.WaitGroup
	revisionCtx, cancel := context.WithTimeout(ctx, timeoutHostRevision)
	defer cancel()

	contracts := make([]contract, len(contractMetadatas))
	for i, c := range contractMetadatas {
		contracts[i].ContractMetadata = c

		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			rev, err := bus.ContractRevision(revisionCtx, c.ID)
			if err != nil {
				// print the reason for the missing revisions
				logger.With(zap.Error(err)).
					With("hostKey", c.HostKey).
					With("contractID", c.ID).Debug("failed to fetch contract revision")
			} else {
				contracts[i].Revision = &rev
			}
		}(i)
	}
	wg.Wait()

	// sort them by whether they are in the current set and their size
	ctx.SortContractsForMaintenance(contracts)

	// allow for a leeway of 10% of the required contracts for special cases such as failing to fetch
	remainingLeeway := addLeeway(ctx.WantedContracts(), 1-leewayPctRequiredContracts)

	// perform checks on contracts one-by-one renewing/refreshing
	// contracts as necessary and filtering out contracts that should no
	// longer be used
	logger.With("contracts", len(contracts)).Info("checking existing contracts")
	var renewed, refreshed int
	for _, c := range contracts {
		inSet := c.InSet(ctx.Set())

		logger := logger.With("contractID", c.ID).
			With("inSet", inSet).
			With("hostKey", c.HostKey).
			With("revisionNumber", c.RevisionNumber).
			With("size", c.FileSize()).
			With("state", c.State).
			With("remainingLeeway", remainingLeeway).
			With("revisionAvailable", c.Revision != nil).
			With("filteredContracts", len(filteredContracts)).
			With("wantedContracts", ctx.WantedContracts())

		logger.Debug("checking contract")

		// abort if we have enough contracts
		if uint64(len(filteredContracts)) >= ctx.WantedContracts() {
			churnReasons[c.ID] = "truncated"
			logger.Debug("ignoring contract since we have enough contracts")
			continue
		}

		// fetch recent consensus state
		cs, err := bus.ConsensusState(ctx)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to fetch consensus state: %w", err)
		}
		bh := cs.BlockHeight
		logger = logger.With("blockHeight", bh)

		// check if contract is ready to be archived.
		if reason := cc.shouldArchive(c, bh, network); reason != nil {
			if err := bus.ArchiveContracts(ctx, map[types.FileContractID]string{
				c.ID: reason.Error(),
			}); err != nil {
				logger.With(zap.Error(err)).Error("failed to archive contract")
			} else {
				logger.Debug("successfully archived contract")
			}
			churnReasons[c.ID] = reason.Error()
			continue
		}

		// fetch host
		host, err := bus.Host(ctx, c.HostKey)
		if err != nil {
			logger.With(zap.Error(err)).Warn("missing host")
			churnReasons[c.ID] = api.ErrUsabilityHostNotFound.Error()
			continue
		}

		// extend logger
		logger = logger.With("blocked", host.Blocked)

		// check if host is blocked
		if host.Blocked {
			logger.Info("host is blocked")
			churnReasons[c.ID] = api.ErrUsabilityHostBlocked.Error()
			continue
		}

		// check if host has a redundant ip
		if ctx.ShouldFilterRedundantIPs() && ipFilter.HasRedundantIP(ctx, host) {
			logger.Info("host has redundant IP")
			churnReasons[c.ID] = api.ErrUsabilityHostRedundantIP.Error()
			continue
		}

		// get check
		check, ok := host.Checks[ctx.ApID()]
		if !ok {
			logger.Warn("missing host check")
			churnReasons[c.ID] = api.ErrUsabilityHostNotFound.Error()
			continue
		}

		// NOTE: if we have a contract with a host that is not scanned, we either
		// added the host and contract manually or reset the host scans. In that case,
		// we ignore the fact that the host is not scanned for now to avoid churn.
		if inSet && check.UsabilityBreakdown.NotCompletingScan {
			keepContract(c.ContractMetadata, host)
			logger.Info("ignoring contract with unscanned host")
			continue // no more checks until host is scanned
		}

		// check usability
		if !check.UsabilityBreakdown.IsUsable() {
			reasons := strings.Join(check.UsabilityBreakdown.UnusableReasons(), ",")
			logger.With("reasons", reasons).Info("unusable host")
			churnReasons[c.ID] = reasons
			continue
		}

		// check if revision is available
		if c.Revision == nil {
			if inSet && remainingLeeway > 0 {
				logger.Debug("keeping contract due to leeway")
				keepContract(c.ContractMetadata, host)
				remainingLeeway--
			} else {
				logger.Debug("ignoring contract without revision")
				churnReasons[c.ID] = errContractNoRevision.Error()
			}
			continue // no more checks without revision
		}

		// check if contract is usable
		usable, needsRefresh, needsRenew, reasons := cc.isUsableContract(ctx.AutopilotConfig(), host.Settings, host.PriceTable.HostPriceTable, ctx.state.RS, c, inSet, bh, ipFilter)

		// extend logger
		logger = logger.With("usable", usable).
			With("needsRefresh", needsRefresh).
			With("needsRenew", needsRenew).
			With("reasons", reasons)

			// remember reason for potential drop of contract
		if len(reasons) > 0 {
			churnReasons[c.ID] = strings.Join(reasons, ",")
		}

		contract := c.ContractMetadata

		// renew/refresh as necessary
		var ourFault bool
		if needsRenew {
			var renewedContract api.ContractMetadata
			renewedContract, ourFault, err = cr.renewContract(ctx, c, host, logger)
			if err != nil {
				logger = logger.With(zap.Error(err)).With("ourFault", ourFault)

				// don't register an alert for hosts that are out of funds since the
				// user can't do anything about it
				if !(rhp3.IsErrHost(err) && utils.IsErr(err, wallet.ErrNotEnoughFunds)) {
					alerter.RegisterAlert(ctx, newContractRenewalFailedAlert(contract, !ourFault, err))
				}
				logger.Error("failed to renew contract")
			} else {
				logger.Info("successfully renewed contract")
				alerter.DismissAlerts(ctx, alerts.IDForContract(alertRenewalFailedID, contract.ID))
				contract = renewedContract
				usable = true
				renewed++
			}
		} else if needsRefresh {
			var refreshedContract api.ContractMetadata
			refreshedContract, ourFault, err = cr.refreshContract(ctx, c, host, logger)
			if err != nil {
				logger = logger.With(zap.Error(err)).With("ourFault", ourFault)

				// don't register an alert for hosts that are out of funds since the
				// user can't do anything about it
				if !(rhp3.IsErrHost(err) && utils.IsErr(err, wallet.ErrNotEnoughFunds)) {
					alerter.RegisterAlert(ctx, newContractRenewalFailedAlert(contract, !ourFault, err))
				}
				logger.Error("failed to refresh contract")
			} else {
				logger.Info("successfully refreshed contract")
				alerter.DismissAlerts(ctx, alerts.IDForContract(alertRenewalFailedID, contract.ID))
				contract = refreshedContract
				usable = true
				refreshed++
			}
		}

		// if the renewal/refresh failing was our fault (e.g. we ran out of
		// funds), we should not drop the contract
		if !usable && ourFault {
			logger.Info("keeping contract even though renewal/refresh failed")
			usable = true
		}

		// if the contract is not usable we ignore it
		if !usable {
			if inSet {
				logger.Info("contract is not usable, removing from set")
			} else {
				logger.Debug("contract is not usable, remains out of set")
			}
			continue
		}

		// we keep the contract, add the host to the filter
		logger.Debug("contract is usable and is added / stays in set")
		keepContract(contract, host)
	}
	logger.With("refreshed", refreshed).
		With("renewed", renewed).
		With("filteredContracts", len(filteredContracts)).
		Info("checking existing contracts done")
	return filteredContracts, churnReasons, nil
}

// performContracdtFormations forms up to 'wanted' new contracts with hosts. The
// 'ipFilter' and 'remainingFunds' are updated with every new contract.
func performContractFormations(ctx *mCtx, bus Bus, cr contractReviser, ipFilter *hostSet, logger *zap.SugaredLogger, wanted int) ([]api.ContractMetadata, error) {
	var formedContracts []api.ContractMetadata
	addContract := func(c api.ContractMetadata, h api.Host) {
		formedContracts = append(formedContracts, c)
		wanted--
		ipFilter.Add(ctx, h)
	}

	// early check to avoid fetching all candidates
	if wanted <= 0 {
		logger.Info("already have enough contracts, no need to form new ones")
		return formedContracts, nil // nothing to do
	}
	logger.With("wanted", wanted).Info("trying to form more contracts to fill set")

	// get list of hosts that we already have contracts with
	contracts, err := bus.Contracts(ctx, api.ContractsOpts{})
	if err != nil {
		return nil, fmt.Errorf("failed to fetch contracts: %w", err)
	}
	usedHosts := make(map[types.PublicKey]struct{})
	for _, c := range contracts {
		usedHosts[c.HostKey] = struct{}{}
	}
	allHosts, err := bus.Hosts(ctx, api.HostOptions{
		AutopilotID:   ctx.ApID(),
		FilterMode:    api.HostFilterModeAllowed,
		UsabilityMode: api.UsabilityFilterModeUsable,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to fetch usable hosts: %w", err)
	}

	// filter them
	var candidates scoredHosts
	for _, host := range allHosts {
		logger := logger.With("hostKey", host.PublicKey)
		hc, ok := host.Checks[ctx.ApID()]
		if !ok {
			logger.Warn("missing host check")
			continue
		} else if _, used := usedHosts[host.PublicKey]; used {
			logger.Debug("host already used")
			continue
		} else if score := hc.ScoreBreakdown.Score(); score == 0 {
			logger.Error("host has a score of 0")
			continue
		}
		candidates = append(candidates, newScoredHost(host, hc.ScoreBreakdown))
	}
	logger = logger.With("candidates", len(candidates))

	// select hosts, since we already have all of them in memory we select
	// len(candidates)
	candidates = candidates.randSelectByScore(len(candidates))
	if len(candidates) < wanted {
		logger.Warn("not enough candidates to form new contracts")
	}

	// get the initial contract funds
	minInitialContractFunds := InitialContractFunding

	// form contracts until the new set has the desired size
	for _, candidate := range candidates {
		if wanted == 0 {
			return formedContracts, nil // done
		}

		// break if the autopilot is stopped
		select {
		case <-ctx.Done():
			return nil, context.Cause(ctx)
		default:
		}

		// prepare a logger
		logger := logger.With("hostKey", candidate.host.PublicKey)

		// check if we already have a contract with a host on that address
		if ctx.ShouldFilterRedundantIPs() && ipFilter.HasRedundantIP(ctx, candidate.host) {
			logger.Info("host has redundant IP")
			continue
		}

		formedContract, proceed, err := cr.formContract(ctx, bus, candidate.host, minInitialContractFunds, logger)
		if err != nil {
			logger.With(zap.Error(err)).Error("failed to form contract")
			continue
		}
		if !proceed {
			logger.Error("not proceeding with contract formation")
			break
		}

		// add new contract and host
		addContract(formedContract, candidate.host)
	}
	logger.With("formedContracts", len(formedContracts)).Info("done forming contracts")
	return formedContracts, nil
}

// performHostChecks performs scoring and usability checks on all hosts,
// updating their state in the database.
func performHostChecks(ctx *mCtx, bus Bus, logger *zap.SugaredLogger) error {
	var usabilityBreakdown unusableHostsBreakdown
	// fetch all hosts that are not blocked
	hosts, err := bus.Hosts(ctx, api.HostOptions{})
	if err != nil {
		return fmt.Errorf("failed to fetch all hosts: %w", err)
	}

	var scoredHosts []scoredHost
	for _, host := range hosts {
		// score host
		sb, err := ctx.HostScore(host)
		if err != nil {
			logger.With(zap.Error(err)).Info("failed to score host")
			continue
		}
		scoredHosts = append(scoredHosts, newScoredHost(host, sb))
	}

	// compute minimum score for usable hosts
	minScore := calculateMinScore(scoredHosts, ctx.WantedContracts(), logger)

	// run host checks using the latest consensus state
	cs, err := bus.ConsensusState(ctx)
	if err != nil {
		return fmt.Errorf("failed to fetch consensus state: %w", err)
	}
	for _, h := range scoredHosts {
		h.host.PriceTable.HostBlockHeight = cs.BlockHeight // ignore HostBlockHeight
		hc := checkHost(ctx.GougingChecker(cs), h, minScore, ctx.Period())
		if err := bus.UpdateHostCheck(ctx, ctx.ApID(), h.host.PublicKey, *hc); err != nil {
			return fmt.Errorf("failed to update host check for host %v: %w", h.host.PublicKey, err)
		}
		usabilityBreakdown.track(hc.UsabilityBreakdown)

		if !hc.UsabilityBreakdown.IsUsable() {
			logger.With("hostKey", h.host.PublicKey).
				With("reasons", strings.Join(hc.UsabilityBreakdown.UnusableReasons(), ",")).
				Debug("host is not usable")
		}
	}

	logger.Infow("host checks completed", usabilityBreakdown.keysAndValues()...)
	return nil
}

func performPostMaintenanceTasks(ctx *mCtx, bus Bus, alerter alerts.Alerter, cc contractChecker, rb revisionBroadcaster, logger *zap.SugaredLogger) error {
	// fetch some contract and host info
	allContracts, err := bus.Contracts(ctx, api.ContractsOpts{})
	if err != nil {
		return fmt.Errorf("failed to fetch all contracts: %w", err)
	}
	setContracts, err := bus.Contracts(ctx, api.ContractsOpts{ContractSet: ctx.ContractSet()})
	if err != nil {
		return fmt.Errorf("failed to fetch contracts: %w", err)
	}
	allHosts, err := bus.Hosts(ctx, api.HostOptions{})
	if err != nil {
		return fmt.Errorf("failed to fetch all hosts: %w", err)
	}
	usedHosts := make(map[types.PublicKey]struct{})
	for _, c := range allContracts {
		usedHosts[c.HostKey] = struct{}{}
	}

	// run revision broadcast on contracts in the new set
	rb.broadcastRevisions(ctx, setContracts, logger)

	// register alerts for used hosts with lost sectors
	var toDismiss []types.Hash256
	for _, h := range allHosts {
		if _, used := usedHosts[h.PublicKey]; !used {
			continue
		} else if registerLostSectorsAlert(h.Interactions.LostSectors*rhpv2.SectorSize, h.StoredData) {
			alerter.RegisterAlert(ctx, newLostSectorsAlert(h.PublicKey, h.Settings.Version, h.Settings.Release, h.Interactions.LostSectors))
		} else {
			toDismiss = append(toDismiss, alerts.IDForHost(alertLostSectorsID, h.PublicKey))
		}
	}
	if len(toDismiss) > 0 {
		alerter.DismissAlerts(ctx, toDismiss...)
	}

	// prune refresh failures
	cc.pruneContractRefreshFailures(allContracts)
	return nil
}

func performContractMaintenance(ctx *mCtx, alerter alerts.Alerter, bus Bus, churn *accumulatedChurn, cc contractChecker, cr contractReviser, rb revisionBroadcaster, logger *zap.SugaredLogger) (bool, error) {
	logger = logger.Named("performContractMaintenance").
		Named(hex.EncodeToString(frand.Bytes(16))). // uuid for this iteration
		With("contractSet", ctx.ContractSet())

	// check if we want to run maintenance
	if reason, skip := canSkipContractMaintenance(ctx, ctx.ContractsConfig()); skip {
		logger.With("reason", reason).Info("skipping contract maintenance")
		if err := alerter.RegisterAlert(ctx, newContractMaintenanceSkippedAlert(reason)); err != nil {
			logger.With(zap.Error(err)).Error("failed to register skipped contract maintenance alert")
		}
		return false, nil
	}

	logger.Infow("performing contract maintenance")

	// STEP 1: perform host checks
	if err := performHostChecks(ctx, bus, logger); err != nil {
		return false, err
	}

	// STEP 2: perform contract maintenance
	ipFilter := newHostSet(logger.Named("ipFilter"))
	keptContracts, churnReasons, err := performContractChecks(ctx, alerter, bus, cc, cr, ipFilter, logger)
	if err != nil {
		return false, err
	}

	// STEP 3: perform contract formation
	formedContracts, err := performContractFormations(ctx, bus, cr, ipFilter, logger, int(ctx.WantedContracts())-len(keptContracts))
	if err != nil {
		return false, err
	}

	// fetch old set
	oldSet, err := bus.Contracts(ctx, api.ContractsOpts{ContractSet: ctx.ContractSet()})
	if err != nil && !utils.IsErr(err, api.ErrContractSetNotFound) {
		return false, fmt.Errorf("failed to fetch old contract set: %w", err)
	}

	// merge kept and formed contracts into new set
	newSet := make([]api.ContractMetadata, 0, len(keptContracts)+len(formedContracts))
	newSet = append(newSet, keptContracts...)
	newSet = append(newSet, formedContracts...)

	// STEP 4: update contract set
	if err := updateContractSet(ctx, bus, oldSet, newSet); err != nil {
		return false, err
	}

	// STEP 5: perform minor maintenance such as cleanups and broadcasting
	// revisions
	if err := performPostMaintenanceTasks(ctx, bus, alerter, cc, rb, logger); err != nil {
		return false, err
	}

	// STEP 6: log changes and register alerts
	return computeContractSetChanged(ctx, alerter, bus, churn, logger, oldSet, newSet, churnReasons)
}

func updateContractSet(ctx *mCtx, bus Bus, oldSet, newSet []api.ContractMetadata) error {
	var newSetIDs []types.FileContractID
	for _, contract := range newSet {
		newSetIDs = append(newSetIDs, contract.ID)
	}
	inOldSet := make(map[types.FileContractID]struct{})
	for _, c := range oldSet {
		inOldSet[c.ID] = struct{}{}
	}
	var toAdd []types.FileContractID
	for _, c := range newSet {
		if _, ok := inOldSet[c.ID]; !ok {
			toAdd = append(toAdd, c.ID)
		}
		// only keep contracts that are in the old but not the new set
		delete(inOldSet, c.ID)
	}

	var toRemove []types.FileContractID
	for id := range inOldSet {
		toRemove = append(toRemove, id)
	}
	if err := bus.UpdateContractSet(ctx, ctx.ContractSet(), newSetIDs, toRemove); err != nil {
		return fmt.Errorf("failed to update contract set: %w", err)
	}
	return nil
}
