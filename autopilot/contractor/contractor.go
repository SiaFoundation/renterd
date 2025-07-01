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
	rhpv4 "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/wallet"
	"go.sia.tech/renterd/v2/alerts"
	"go.sia.tech/renterd/v2/api"
	"go.sia.tech/renterd/v2/internal/utils"
	"go.uber.org/zap"
	"lukechampine.com/frand"
)

const (
	// broadcastRevisionRetriesPerInterval is the number of chances we give a
	// contract that fails to broadcst to be broadcasted again within a single
	// contract broadcast interval.
	broadcastRevisionRetriesPerInterval = 5

	// failedRenewalForgivenessPeriod is the amount of time we wait before
	// punishing a contract for not being able to refresh
	failedRefreshForgivenessPeriod = 24 * time.Hour

	// minAllowedScoreLeeway is a factor by which a host can be under the lowest
	// score found in a random sample of scores before being considered not
	// usable.
	minAllowedScoreLeeway = 500

	// targetBlockTime is the average block time of the Sia network
	targetBlockTime = 10 * time.Minute

	// timeoutHostRevision is the amount of time we wait to receive the latest
	// revision from the host
	timeoutHostRevision = time.Minute

	// timeoutBroadcastRevision is the amount of time we wait for the broadcast
	// of a revision to succeed.
	timeoutBroadcastRevision = time.Minute
)

var (
	InitialContractFunding = types.Siacoins(10)
	MinCollateral          = types.Siacoins(1).Div64(10) // 100mS
)

type ConsensusStore interface {
	ConsensusState(ctx context.Context) (api.ConsensusState, error)
	ConsensusNetwork(ctx context.Context) (consensus.Network, error)
}

type ContractManager interface {
	BroadcastContract(ctx context.Context, fcid types.FileContractID) (types.TransactionID, error)
	ContractRevision(ctx context.Context, fcid types.FileContractID) (api.Revision, error)
	FormContract(ctx context.Context, renterAddress types.Address, renterFunds types.Currency, hostKey types.PublicKey, hostCollateral types.Currency, endHeight uint64) (api.ContractMetadata, error)
	RenewContract(ctx context.Context, fcid types.FileContractID, endHeight uint64, renterFunds, minNewCollateral types.Currency) (api.ContractMetadata, error)
}

type Database interface {
	ArchiveContracts(ctx context.Context, toArchive map[types.FileContractID]string) error
	Contracts(ctx context.Context, opts api.ContractsOpts) (contracts []api.ContractMetadata, err error)
	Host(ctx context.Context, hostKey types.PublicKey) (api.Host, error)
	Hosts(ctx context.Context, opts api.HostOptions) ([]api.Host, error)
	UpdateContractUsability(ctx context.Context, contractID types.FileContractID, usability string) (err error)
	UpdateHostCheck(ctx context.Context, hostKey types.PublicKey, hostCheck api.HostChecks) error
}

type HostScanner interface {
	ScanHost(ctx context.Context, hostKey types.PublicKey, timeout time.Duration) (api.HostScanResponse, error)
}

type contractChecker interface {
	isUsableContract(cfg api.AutopilotConfig, contract contract, bh uint64) (usable, refresh, renew bool, reasons []string)
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
		cm      ContractManager
		cs      ConsensusStore
		db      Database
		hs      HostScanner

		churn  accumulatedChurn
		logger *zap.SugaredLogger

		allowRedundantHostIPs bool

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
)

func New(alerter alerts.Alerter, cs ConsensusStore, cm ContractManager, db Database, hs HostScanner, revisionSubmissionBuffer uint64, revisionBroadcastInterval time.Duration, allowRedundantHostIPs bool, logger *zap.Logger) *Contractor {
	logger = logger.Named("contractor")
	return &Contractor{
		cs:      cs,
		cm:      cm,
		db:      db,
		hs:      hs,
		alerter: alerter,
		churn:   make(accumulatedChurn),
		logger:  logger.Sugar(),

		allowRedundantHostIPs: allowRedundantHostIPs,

		revisionBroadcastInterval: revisionBroadcastInterval,
		revisionLastBroadcast:     make(map[types.FileContractID]time.Time),
		revisionSubmissionBuffer:  revisionSubmissionBuffer,

		firstRefreshFailure: make(map[types.FileContractID]time.Time),
	}
}

func (c *Contractor) PerformContractMaintenance(ctx context.Context, state *MaintenanceState) (bool, error) {
	return performContractMaintenance(newMaintenanceCtx(ctx, state), c.alerter, c.db, c.churn, c, c.cm, c, c.cs, c.hs, c, c.allowRedundantHostIPs, c.logger)
}

func (c *Contractor) formContract(ctx *mCtx, hs HostScanner, host api.Host, minInitialContractFunds types.Currency, logger *zap.SugaredLogger) (cm api.ContractMetadata, proceed bool, err error) {
	logger = logger.With("hostKey", host.PublicKey, "hostVersion", host.V2Settings.ProtocolVersion, "hostRelease", host.V2Settings.Release)
	ctx, cancel := ctx.WithTimeout(time.Minute)
	defer cancel()

	// convenience variables
	hk := host.PublicKey

	// fetch host settings
	scan, err := hs.ScanHost(ctx, hk, 30*time.Second)
	if err != nil {
		logger.Infow(err.Error(), "hk", hk)
		return api.ContractMetadata{}, true, err
	}

	// fetch consensus state
	cs, err := c.cs.ConsensusState(ctx)
	if err != nil {
		return api.ContractMetadata{}, false, err
	}
	endHeight := ctx.EndHeight(cs.BlockHeight)

	contractPrice := scan.V2Settings.Prices.ContractPrice
	maxCollateral := scan.V2Settings.MaxCollateral
	renterFunds := InitialContractFunding

	// calculate the host collateral
	hostCollateral := rhpv4.MaxHostCollateral(scan.V2Settings.Prices, renterFunds)
	if hostCollateral.Cmp(maxCollateral) > 0 {
		hostCollateral = maxCollateral
	}

	// shouldn't go below the minimum immediately so we add some buffer
	minCollateral := MinCollateral.Mul64(2).Add(contractPrice)
	if hostCollateral.Cmp(minCollateral) < 0 {
		hostCollateral = minCollateral
	}

	// form contract
	contract, err := c.cm.FormContract(ctx, ctx.state.Address, renterFunds, hk, hostCollateral, endHeight)
	if err != nil {
		logger.Errorw(fmt.Sprintf("contract formation failed, err: %v", err), "hk", hk)
		return api.ContractMetadata{}, !utils.IsErr(err, wallet.ErrNotEnoughFunds), err
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
	logger = logger.With("to_renew", contract.ID, "hk", contract.HostKey, "hostVersion", host.V2Settings.ProtocolVersion, "hostRelease", host.V2Settings.Release)

	// calculate the renter funds
	renterFunds := c.refreshFundingEstimate(contract, logger)

	contractPrice := host.V2Settings.Prices.ContractPrice

	// a refresh should always result in a contract that has enough collateral
	minNewCollateral := MinCollateral.Mul64(2).Add(contractPrice)

	// renew the contract
	renewal, err := c.cm.RenewContract(ctx, contract.ID, contract.EndHeight(), renterFunds, minNewCollateral)
	if err != nil {
		logger.Errorw(
			"refresh failed",
			zap.Error(err),
			"endHeight", contract.EndHeight(),
			"renterFunds", renterFunds,
		)
		if utils.IsErr(err, wallet.ErrNotEnoughFunds) && !utils.IsErrHost(err) {
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
	logger = logger.With("to_renew", contract.ID, "hk", contract.HostKey, "hostVersion", host.V2Settings.ProtocolVersion, "hostRelease", host.V2Settings.Release)

	// convenience variables
	fcid := contract.ID

	// fetch consensus state
	cs, err := c.cs.ConsensusState(ctx)
	if err != nil {
		return api.ContractMetadata{}, false, err
	}

	// calculate the renter funds for the renewal a.k.a. the funds the renter will
	// be able to spend
	minRenterFunds := InitialContractFunding
	renterFunds := renewFundingEstimate(minRenterFunds, contract.InitialRenterFunds, contract.RenterFunds(), logger)

	// sanity check the endheight is not the same on renewals
	endHeight := ctx.EndHeight(cs.BlockHeight)
	if endHeight <= contract.ProofHeight {
		logger.Infow("invalid renewal endheight", "oldEndheight", contract.EndHeight(), "newEndHeight", endHeight, "bh", cs.BlockHeight)
		return api.ContractMetadata{}, false, fmt.Errorf("renewal endheight should surpass the current contract endheight, %v <= %v", endHeight, contract.EndHeight())
	}

	// unlike a refresh, a renewal doesn't require a minimum amount of
	// collateral after the renewal since our primary goal is to extend the
	// lifetime of our data.
	minNewCollateral := types.ZeroCurrency

	// renew the contract
	renewal, err := c.cm.RenewContract(ctx, fcid, endHeight, renterFunds, minNewCollateral)
	if err != nil {
		logger.Errorw(
			"renewal failed",
			zap.Error(err),
			"endHeight", endHeight,
			"renterFunds", renterFunds,
		)
		if utils.IsErr(err, wallet.ErrNotEnoughFunds) && !utils.IsErrHost(err) {
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

// broadcastRevisions broadcasts contract revisions, we only broadcast the
// revision of good contracts since we're migrating away from bad contracts.
func (c *Contractor) broadcastRevisions(ctx context.Context, contracts []api.ContractMetadata, logger *zap.SugaredLogger) {
	if c.revisionBroadcastInterval == 0 {
		return // not enabled
	}

	cs, err := c.cs.ConsensusState(ctx)
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
		if contract.RenewedTo != (types.FileContractID{}) || timeSinceRevisionHeight < c.revisionBroadcastInterval || timeSinceLastTry < c.revisionBroadcastInterval/broadcastRevisionRetriesPerInterval {
			continue // nothing to do
		}

		// remember that we tried to broadcast this contract now
		c.revisionLastBroadcast[contract.ID] = time.Now()

		// broadcast revision
		ctx, cancel := context.WithTimeout(ctx, timeoutBroadcastRevision)
		_, err := c.cm.BroadcastContract(ctx, contract.ID)
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
		return errContractRenewed
	} else if contract.RevisionNumber == math.MaxUint64 {
		return errContractRenewed
	} else if contract.State == api.ContractStatePending && bh-contract.StartHeight > ContractConfirmationDeadline {
		return errContractNotConfirmed
	} else if contract.RenewedTo != (types.FileContractID{}) {
		return errContractRenewed
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

// activeContracts fetches all active contracts as well as their revision.
func activeContracts(ctx context.Context, s Database, cm ContractManager, logger *zap.SugaredLogger) ([]contract, error) {
	// fetch active contracts
	logger.Info("fetching active contracts")
	start := time.Now()
	metadatas, err := s.Contracts(ctx, api.ContractsOpts{
		FilterMode: api.ContractFilterModeActive,
	})
	if err != nil {
		return nil, err
	}
	logger.With("elapsed", time.Since(start)).Info("done fetching active contracts")

	// fetch the revision for each contract
	var wg sync.WaitGroup
	start = time.Now()
	logger.Info("fetching revisions")

	// launch goroutines, apply sane host timeout
	revisionCtx, cancel := context.WithTimeout(ctx, timeoutHostRevision)
	defer cancel()
	contracts := make([]contract, len(metadatas))
	for i, c := range metadatas {
		contracts[i].ContractMetadata = c

		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			rev, err := cm.ContractRevision(revisionCtx, c.ID)
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
	logger.
		With("elapsed", time.Since(start)).
		With("contracts", len(contracts)).
		Info("done fetching all revisions")

	return contracts, nil
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

func hasAlert(ctx context.Context, alerter alerts.Alerter, id types.Hash256, logger *zap.SugaredLogger) bool {
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
		if minFunds := initRenterFunds.Mul64(8).Div64(10); renterFunds.Cmp(minFunds) < 0 {
			renterFunds = minFunds // at least 80% of the initial funds
		}
	}

	// but the funds should not drop below the amount we'd fund a new contract with
	if renterFunds.Cmp(minRenterFunds) < 0 {
		log.Info("funds would drop below the minimum, using the minimum")
		renterFunds = minRenterFunds
	}
	return renterFunds
}

// performContractChecks checks existing contracts, renewing/refreshing any that
// need it and marking contracts that should no longer be used as bad. The
// host filter is updated to contain all hosts that we keep contracts with. If a
// contract is refreshed or renewed, the 'remainingFunds' are adjusted.
func performContractChecks(ctx *mCtx, alerter alerts.Alerter, s Database, churn accumulatedChurn, cc contractChecker, cm ContractManager, cr contractReviser, cs ConsensusStore, hf hostFilter, logger *zap.SugaredLogger) (uint64, error) {
	// fetch network
	network, err := cs.ConsensusNetwork(ctx)
	if err != nil {
		return 0, err
	}

	// fetch active contracts
	contracts, err := activeContracts(ctx, s, cm, logger)
	if err != nil {
		return 0, err
	}

	// keep track of usability updates
	var updates []usabilityUpdate

	// define a helper to a contract's usability
	log := logger.Named("usability")
	updateUsability := func(ctx context.Context, h api.Host, c api.ContractMetadata, usability, context string) {
		if c.Usability != usability {
			log = log.
				With("contractID", c.ID).
				With("usability", c.Usability).
				With("hostKey", c.HostKey).
				With("context", context)
			if err := s.UpdateContractUsability(ctx, c.ID, usability); err != nil {
				log.Errorf("failed to update usability to %s: %v", usability, err)
				return
			}

			log.Infof("successfully updated usability to %s", usability)
			updates = append(updates, usabilityUpdate{c.HostKey, c.ID, c.Size, c.Usability, usability, context})
		}

		if usability == api.ContractUsabilityGood {
			hf.Add(ctx, h)
		}
	}

	// perform checks on contracts one-by-one renewing/refreshing contracts as
	// necessary and filtering out contracts that should no longer be used
	logger.With("contracts", len(contracts)).Info("checking existing contracts")

	var renewed, refreshed, wasGood uint64
	for _, c := range contracts {
		cm := c.ContractMetadata
		if cm.IsGood() {
			wasGood++
		}

		// fetch consensus state
		cs, err := cs.ConsensusState(ctx)
		if err != nil {
			return 0, fmt.Errorf("failed to fetch consensus state: %w", err)
		}

		// create contract logger
		logger := logger.With("contractID", c.ID).
			With("hostKey", c.HostKey).
			With("revisionNumber", c.RevisionNumber).
			With("size", c.FileSize()).
			With("state", c.State).
			With("usability", c.Usability).
			With("revisionAvailable", c.Revision != nil).
			With("wantedContracts", ctx.WantedContracts()).
			With("blockHeight", cs.BlockHeight)
		logger.Debug("checking contract")

		// check if contract is ready to be archived.
		if reason := cc.shouldArchive(c, cs.BlockHeight, network); reason != nil {
			if err := s.ArchiveContracts(ctx, map[types.FileContractID]string{c.ID: reason.Error()}); err != nil {
				logger.With(zap.Error(err)).Error("failed to archive contract")
			} else {
				logger.With("reason", reason).Info("successfully archived contract")
			}
			continue
		}

		// fetch host
		host, err := s.Host(ctx, c.HostKey)
		if err != nil {
			logger.With(zap.Error(err)).Warn("missing host")
			updateUsability(ctx, host, cm, api.ContractUsabilityBad, api.ErrUsabilityHostNotFound.Error())
			continue
		}

		// extend logger
		logger = logger.With("blocked", host.Blocked)

		// check if host is blocked
		if host.Blocked {
			logger.Info("host is blocked")
			updateUsability(ctx, host, cm, api.ContractUsabilityBad, api.ErrUsabilityHostBlocked.Error())
			continue
		}

		// check if host has a redundant ip
		if hf.HasRedundantIP(ctx, host) {
			logger.Info("host has redundant IP")
			updateUsability(ctx, host, cm, api.ContractUsabilityBad, api.ErrUsabilityHostRedundantIP.Error())
			continue
		}

		// get check
		if host.Checks == (api.HostChecks{}) {
			logger.Warn("missing host check")
			updateUsability(ctx, host, cm, api.ContractUsabilityBad, api.ErrUsabilityHostCheckNotFound.Error())
			continue
		}

		// NOTE: if we have a contract with a host that is not scanned, we
		// either added the host and contract manually or reset the host scans.
		// In that case, we ignore the fact that the host is not scanned for now
		// to avoid churn.
		if c.IsGood() && host.Checks.UsabilityBreakdown.NotCompletingScan {
			logger.Info("ignoring contract with unscanned host")
			continue // no more checks until host is scanned
		}

		// check usability
		if !host.Checks.UsabilityBreakdown.IsUsable() {
			logger.Debug("unusable host")
			updateUsability(ctx, host, cm, api.ContractUsabilityBad, host.Checks.UsabilityBreakdown.String())
			continue
		}

		// check if revision is available
		if c.Revision == nil {
			logger.Info("ignoring contract with missing revision")
			updateUsability(ctx, host, cm, c.Usability, "missing revision")
			continue // no more checks without revision
		}

		// check if contract is usable
		usable, needsRefresh, needsRenew, reasons := cc.isUsableContract(ctx.AutopilotConfig(), c, cs.BlockHeight)

		// extend logger
		logger = logger.With("usable", usable).
			With("needsRefresh", needsRefresh).
			With("needsRenew", needsRenew).
			With("reasons", reasons)

		// renew/refresh as necessary
		var ourFault bool
		if needsRenew {
			var renewedContract api.ContractMetadata
			renewedContract, ourFault, err = cr.renewContract(ctx, c, host, logger)
			if err != nil {
				logger = logger.With(zap.Error(err)).With("ourFault", ourFault)
				logger.Error("failed to renew contract")

				// don't register an alert for hosts that are out of funds since the
				// user can't do anything about it
				if !(utils.IsErrHost(err) && utils.IsErr(err, wallet.ErrNotEnoughFunds)) {
					alerter.RegisterAlert(ctx, newContractRenewalFailedAlert(cm, !ourFault, err))
				}
			} else {
				logger.Info("successfully renewed contract")
				alerter.DismissAlerts(ctx, alerts.IDForContract(alertRenewalFailedID, cm.ID))
				cm = renewedContract
				usable = true
				renewed++
			}
		} else if needsRefresh {
			var refreshedContract api.ContractMetadata
			refreshedContract, ourFault, err = cr.refreshContract(ctx, c, host, logger)
			if err != nil {
				logger = logger.With(zap.Error(err)).With("ourFault", ourFault)
				logger.Error("failed to refresh contract")

				// don't register an alert for hosts that are out of funds since the
				// user can't do anything about it
				if !(utils.IsErrHost(err) && utils.IsErr(err, wallet.ErrNotEnoughFunds)) {
					alerter.RegisterAlert(ctx, newContractRenewalFailedAlert(cm, !ourFault, err))
				}
			} else {
				logger.Info("successfully refreshed contract")
				alerter.DismissAlerts(ctx, alerts.IDForContract(alertRenewalFailedID, cm.ID))
				cm = refreshedContract
				usable = true
				refreshed++
			}
		}

		// if the renewal/refresh failing was our fault (e.g. we ran out of
		// funds), we should not drop the contract
		if !usable && ourFault {
			logger.Info("contract is not usable, host is not to blame")
			usable = true
		}

		// if the contract is not usable we ignore it
		if !usable {
			logger.Info("contract is not usable")
			updateUsability(ctx, host, cm, api.ContractUsabilityBad, strings.Join(reasons, ","))
			continue
		}

		// we keep the contract, add the host to the filter
		logger.Debug("contract is usable")
		updateUsability(ctx, host, cm, api.ContractUsabilityGood, "contract is usable")
	}

	// update churn and register alert
	if len(updates) > 0 {
		if !hasAlert(ctx, alerter, alertChurnID, logger) {
			churn.Reset()
		}
		if err := alerter.RegisterAlert(ctx, churn.ApplyUpdates(updates)); err != nil {
			logger.Errorf("failed to register contract usability updated alert: %v", err)
		}
	}

	logger.
		With("refreshed", refreshed).
		With("renewed", renewed).
		With("updated", len(updates)).
		Info("contract checks done")
	return uint64(len(updates)), nil
}

// performContractFormations forms up to 'wanted' new contracts with hosts. The
// 'ipFilter' and 'remainingFunds' are updated with every new contract.
func performContractFormations(ctx *mCtx, bus Database, cr contractReviser, hf hostFilter, hs HostScanner, logger *zap.SugaredLogger) (uint64, error) {
	wanted := int(ctx.WantedContracts())

	// fetch all active contracts
	contracts, err := bus.Contracts(ctx, api.ContractsOpts{
		FilterMode: api.ContractFilterModeActive,
	})
	if err != nil {
		return 0, fmt.Errorf("failed to fetch contracts: %w", err)
	}

	// collect all hosts
	usedHosts := make(map[types.PublicKey]struct{})
	for _, c := range contracts {
		if c.IsGood() {
			wanted--
		}
		usedHosts[c.HostKey] = struct{}{}
	}

	// return early if no more contracts are needed
	if wanted <= 0 {
		logger.Info("already have enough contracts, no need to form new ones")
		return 0, nil
	}

	// fetch all good hosts
	allHosts, err := bus.Hosts(ctx, api.HostOptions{
		FilterMode:    api.HostFilterModeAllowed,
		UsabilityMode: api.UsabilityFilterModeUsable,
	})
	if err != nil {
		return 0, fmt.Errorf("failed to fetch good hosts: %w", err)
	}

	// filter them
	var candidates scoredHosts
	for _, host := range allHosts {
		logger := logger.With("hostKey", host.PublicKey)
		if host.Checks == (api.HostChecks{}) {
			logger.Warnf("missing host check %v", host.PublicKey)
			continue
		}
		if _, used := usedHosts[host.PublicKey]; used {
			logger.Debug("host already used")
			continue
		} else if score := host.Checks.ScoreBreakdown.Score(); score == 0 {
			logger.Error("host has a score of 0")
			continue
		}
		candidates = append(candidates, newScoredHost(host, host.Checks.ScoreBreakdown))
	}
	logger = logger.With("candidates", len(candidates))

	// select hosts, since we already have all of them in memory we select
	// len(candidates)
	candidates = candidates.randSelectByScore(len(candidates))
	if len(candidates) < wanted {
		logger.Warn("insufficient candidate hosts to form the desired amount of new contracts")
	}

	// get the initial contract funds
	minInitialContractFunds := InitialContractFunding

	// form contracts until the new set has the desired size
	var nFormed uint64
	for _, candidate := range candidates {
		if wanted == 0 {
			return nFormed, nil // done
		}

		// break if the autopilot is stopped
		select {
		case <-ctx.Done():
			return 0, context.Cause(ctx)
		default:
		}

		// prepare a logger
		logger := logger.With("hostKey", candidate.host.PublicKey)

		// check if we already have a contract with a host on that address
		if hf.HasRedundantIP(ctx, candidate.host) {
			logger.Info("host has redundant IP")
			continue
		}

		_, proceed, err := cr.formContract(ctx, hs, candidate.host, minInitialContractFunds, logger)
		if err != nil {
			logger.With(zap.Error(err)).Error("failed to form contract")
			continue
		}
		if !proceed {
			logger.Error("not proceeding with contract formation")
			break
		}

		// add new contract and host
		hf.Add(ctx, candidate.host)
		nFormed++
		wanted--
	}
	logger.With("formedContracts", nFormed).Info("done forming contracts")
	return nFormed, nil
}

// performHostChecks performs scoring and usability checks on all hosts,
// updating their state in the database.
func performHostChecks(ctx *mCtx, bus Database, cs ConsensusStore, logger *zap.SugaredLogger) error {
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
	state, err := cs.ConsensusState(ctx)
	if err != nil {
		return fmt.Errorf("failed to fetch consensus state: %w", err)
	}
	for _, h := range scoredHosts {
		// ignore HostBlockHeight
		h.host.V2Settings.Prices.TipHeight = state.BlockHeight
		hc := checkHost(ctx.GougingChecker(state), h, minScore, ctx.Period())
		if err := bus.UpdateHostCheck(ctx, h.host.PublicKey, *hc); err != nil {
			return fmt.Errorf("failed to update host check for host %v: %w", h.host.PublicKey, err)
		}
		usabilityBreakdown.track(hc.UsabilityBreakdown)

		if !hc.UsabilityBreakdown.IsUsable() {
			logger.With("hostKey", h.host.PublicKey).
				With("reasons", hc.UsabilityBreakdown).
				Debug("host is not usable")
		}
	}

	logger.Infow("host checks completed", usabilityBreakdown.keysAndValues()...)
	return nil
}

func performPostMaintenanceTasks(ctx *mCtx, bus Database, alerter alerts.Alerter, cc contractChecker, rb revisionBroadcaster, logger *zap.SugaredLogger) error {
	// fetch some contract and host info
	allContracts, err := bus.Contracts(ctx, api.ContractsOpts{
		FilterMode: api.ContractFilterModeActive,
	})
	if err != nil {
		return fmt.Errorf("failed to fetch all contracts: %w", err)
	}
	var goodContracts []api.ContractMetadata
	for _, c := range allContracts {
		if c.IsGood() {
			goodContracts = append(goodContracts, c)
		}
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
	rb.broadcastRevisions(ctx, goodContracts, logger)

	// register alerts for used hosts with lost sectors
	var toDismiss []types.Hash256
	for _, h := range allHosts {
		if _, used := usedHosts[h.PublicKey]; !used {
			continue
		} else if registerLostSectorsAlert(h.Interactions.LostSectors*rhpv4.LeafSize, h.StoredData) {
			alerter.RegisterAlert(ctx, newLostSectorsAlert(h.PublicKey, h.V2Settings.ProtocolVersion, h.V2Settings.Release, h.Interactions.LostSectors))
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

// performV2ContractMigration migrates v1 contracts to v2 contracts once the v2
// allow height is reached. That means:
// - we form a v2 contract with all hosts that we currently have a v1 contract with
// - we delete the v1 contract afterwards
// - migrations will take over
func performV2ContractMigration(ctx *mCtx, bus Database, cr contractReviser, cs ConsensusStore, hs HostScanner, logger *zap.SugaredLogger) {
	logger = logger.Named("v2ContractMigration")

	network, err := cs.ConsensusNetwork(ctx)
	if err != nil {
		logger.With(zap.Error(err)).Error("failed to fetch consensus network")
		return
	}

	state, err := cs.ConsensusState(ctx)
	if err != nil {
		logger.With(zap.Error(err)).Error("failed to fetch consensus state")
		return
	} else if state.BlockHeight < network.HardforkV2.AllowHeight {
		return // nothing to do yet
	}

	contracts, err := bus.Contracts(ctx, api.ContractsOpts{
		FilterMode: api.ContractFilterModeActive,
	})
	if err != nil {
		logger.With(zap.Error(err)).Error("failed to fetch contracts for migration")
		return
	}

	hostsWithV2Contract := make(map[types.PublicKey]struct{})
	for _, contract := range contracts {
		if contract.V2 {
			hostsWithV2Contract[contract.HostKey] = struct{}{}
		}
	}
	toArchive := make(map[types.FileContractID]struct{})

	for _, contract := range contracts {
		if contract.V2 {
			continue // nothing to do
		} else if _, exists := hostsWithV2Contract[contract.HostKey]; exists {
			toArchive[contract.ID] = struct{}{}
			continue // nothing more to do
		}

		host, err := bus.Host(ctx, contract.HostKey)
		if err != nil {
			logger.Errorw("failed to fetch host for v1 contract", zap.Error(err))
			continue
		}

		// form a new contract with the same host
		_, _, err = cr.formContract(ctx, hs, host, InitialContractFunding, logger)
		if err != nil {
			logger.Errorw("failed to form a v2 contract with the host", zap.Error(err))
			continue
		}

		// remember for archival
		toArchive[contract.ID] = struct{}{}
	}

	// archive contracts
	for id := range toArchive {
		if err := bus.ArchiveContracts(ctx, map[types.FileContractID]string{
			id: "migrated to v2",
		}); err != nil {
			logger.Errorw("failed to archive migrated contract", zap.Error(err))
			continue
		}
	}
}

func performContractMaintenance(ctx *mCtx, alerter alerts.Alerter, s Database, churn accumulatedChurn, cc contractChecker, cm ContractManager, cr contractReviser, cs ConsensusStore, hs HostScanner, rb revisionBroadcaster, allowRedundantHostIPs bool, logger *zap.SugaredLogger) (bool, error) {
	logger = logger.Named("performContractMaintenance").
		Named(hex.EncodeToString(frand.Bytes(16))) // uuid for this iteration

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
	if err := performHostChecks(ctx, s, cs, logger); err != nil {
		return false, err
	}

	// COMPATv2: perform v2 contract migration
	performV2ContractMigration(ctx, s, cr, cs, hs, logger)

	// STEP 2: perform contract maintenance
	hf := newHostFilter(allowRedundantHostIPs, logger)
	nUpdated, err := performContractChecks(ctx, alerter, s, churn, cc, cm, cr, cs, hf, logger)
	if err != nil {
		return false, err
	}

	// STEP 3: perform contract formation
	nFormed, err := performContractFormations(ctx, s, cr, hf, hs, logger)
	if err != nil {
		return false, err
	}

	// STEP 4: perform post maintenance tasks
	return (nUpdated + nFormed) > 0, performPostMaintenanceTasks(ctx, s, alerter, cc, rb, logger)
}
