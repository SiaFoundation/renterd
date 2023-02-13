package autopilot

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.sia.tech/core/consensus"
	rhpv2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/hostdb"
	"go.sia.tech/renterd/internal/tracing"
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

func (c *contractor) isStopped() bool {
	select {
	case <-c.ap.stopChan:
		return true
	default:
		return false
	}
}

func (c *contractor) performContractMaintenance(ctx context.Context, cfg api.AutopilotConfig, cs api.ConsensusState) error {
	ctx, span := tracing.Tracer.Start(ctx, "contractor.performContractMaintenance")
	defer span.End()
	if !cs.Synced {
		return nil // skip contract maintenance if we're not synced
	}

	c.logger.Info("performing contract maintenance")

	// no maintenance if no hosts are requested
	if cfg.Contracts.Amount == 0 {
		c.logger.Debug("no hosts requested, skipping contract maintenance")
		return nil
	}

	// fetch our wallet address
	address, err := c.ap.bus.WalletAddress(ctx)
	if err != nil {
		return err
	}

	// fetch all active contracts from the worker
	start := time.Now()
	resp, err := c.ap.worker.ActiveContracts(ctx, contractHostTimeout)
	if err != nil {
		return err
	}
	if resp.Error != "" {
		c.logger.Error(resp.Error)
	}
	c.logger.Debugf("fetched %d active contracts, took %v", len(resp.Contracts), time.Since(start))
	active := resp.Contracts

	// fetch gouging settings
	gs, err := c.ap.bus.GougingSettings(ctx)
	if err != nil {
		return err
	}

	// fetch redundancy settings
	rs, err := c.ap.bus.RedundancySettings(ctx)
	if err != nil {
		return err
	}

	// run checks
	toDelete, toIgnore, toRefresh, toRenew, err := c.runContractChecks(ctx, cfg, cs.BlockHeight, gs, rs, active)
	if err != nil {
		return fmt.Errorf("failed to run contract checks, err: %v", err)
	}

	// delete contracts
	if len(toDelete) > 0 {
		c.logger.Debugf("deleting %d contracts: %+v", len(toDelete), toDelete)
		if err := c.ap.bus.DeleteContracts(ctx, toDelete); err != nil {
			c.logger.Errorf("failed to delete contracts, err: %v", err) // continue
		}
	}

	// calculate remaining funds
	remaining, err := c.remainingFunds(cfg, active)
	if err != nil {
		return err
	}

	// run renewals
	renewed, err := c.runContractRenewals(ctx, cfg, cs.BlockHeight, &remaining, address, toRenew)
	if err != nil {
		c.logger.Errorf("failed to renew contracts, err: %v", err) // continue
	}

	// run contract refreshes
	refreshed, err := c.runContractRefreshes(ctx, cfg, cs.BlockHeight, &remaining, address, toRefresh)
	if err != nil {
		c.logger.Errorf("failed to refresh contracts, err: %v", err) // continue
	}

	// build the new contract set (excluding formed contracts)
	contractset := buildContractSet(active, toDelete, toIgnore, toRefresh, toRenew, append(renewed, refreshed...))
	numContracts := uint64(len(contractset))

	// check if we need to form contracts and add them to the contract set
	var formed []types.FileContractID
	if numContracts < addLeeway(cfg.Contracts.Amount, leewayPctRequiredContracts) {
		if formed, err = c.runContractFormations(ctx, cfg, active, cfg.Contracts.Amount-numContracts, cs.BlockHeight, &remaining, address); err != nil {
			c.logger.Errorf("failed to form contracts, err: %v", err) // continue
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
	return c.ap.bus.SetContractSet(ctx, cfg.Contracts.Set, contractset)
}

func (c *contractor) performWalletMaintenance(ctx context.Context, cfg api.AutopilotConfig, cs api.ConsensusState) error {
	ctx, span := tracing.Tracer.Start(ctx, "contractor.performWalletMaintenance")
	defer span.End()

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
	outputs, err := b.WalletOutputs(ctx)
	if err != nil {
		return err
	}
	if uint64(len(outputs)) >= cfg.Contracts.Amount {
		l.Debugf("no wallet maintenance needed, plenty of outputs available (%v>=%v)", len(outputs), cfg.Contracts.Amount)
		return nil
	}

	// not enough balance - nothing to do
	amount := cfg.Contracts.Allowance.Div64(cfg.Contracts.Amount)
	balance, err := b.WalletBalance(ctx)
	if err != nil {
		return err
	}
	if balance.Cmp(amount.Mul64(cfg.Contracts.Amount)) < 0 {
		l.Debugf("wallet maintenance skipped, insufficient balance %v < (%v*%v)", balance, cfg.Contracts.Amount, amount)
		return nil
	}

	// redistribute outputs
	id, err := b.WalletRedistribute(ctx, int(cfg.Contracts.Amount), amount)
	if err != nil {
		return fmt.Errorf("failed to redistribute wallet into %d outputs of amount %v, balance %v, err %v", cfg.Contracts.Amount, amount, balance, err)
	}

	l.Debugf("wallet maintenance succeeded, tx %v", id)
	c.maintenanceTxnID = id
	return nil
}

func (c *contractor) runContractChecks(ctx context.Context, cfg api.AutopilotConfig, blockHeight uint64, gs api.GougingSettings, rs api.RedundancySettings, contracts []api.Contract) (toDelete, toIgnore []types.FileContractID, toRefresh, toRenew []contractInfo, _ error) {
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
		// convenience variables
		hk := contract.HostKey()
		fcid := contract.ID

		// fetch host from hostdb
		host, err := c.ap.bus.Host(ctx, hk)
		if err != nil {
			c.logger.Errorw(fmt.Sprintf("missing host, err: %v", err), "hk", hk)
			notfound++
			continue
		}

		// decide whether the host is still good
		usable, reasons := isUsableHost(cfg, gs, rs, f, host)
		if !usable {
			c.logger.Infow("unusable host", "hk", hk, "fcid", fcid, "reasons", errStr(joinErrors(reasons)))
			toIgnore = append(toIgnore, fcid)
			continue
		}

		// grab the settings - this is safe because bad settings make an unusable host
		settings := *host.Settings

		// decide whether the contract is still good
		ci := contractInfo{contract: contract, settings: settings}
		renterFunds, err := c.renewFundingEstimate(ctx, cfg, blockHeight, ci)
		if err != nil {
			c.logger.Errorw(fmt.Sprintf("failed to compute renterFunds for contract: %v", err))
		}

		usable, refresh, renew, reasons := isUsableContract(cfg, ci, blockHeight, renterFunds)
		if !usable {
			c.logger.Infow(
				"unusable contract",
				"hk", hk,
				"fcid", fcid,
				"reasons", errStr(joinErrors(reasons)),
				"refresh", refresh,
				"renew", renew,
			)

			if renew {
				renewIndices[fcid] = len(toRenew)
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
				toDelete = append(toDelete, fcid)
				continue
			}
		}

		// keep track of file size
		contractIds = append(contractIds, fcid)
		contractMap[fcid] = contract.ContractMetadata
		contractSizes[fcid] = contract.FileSize()
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

func (c *contractor) runContractFormations(ctx context.Context, cfg api.AutopilotConfig, active []api.Contract, missing, blockHeight uint64, budget *types.Currency, renterAddress types.Address) ([]types.FileContractID, error) {
	ctx, span := tracing.Tracer.Start(ctx, "runContractFormations")
	defer span.End()

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
	fee, err := c.ap.bus.RecommendedFee(ctx)
	if err != nil {
		return nil, err
	}

	// fetch candidate hosts
	wanted := int(addLeeway(missing, leewayPctCandidateHosts))
	candidates, err := c.candidateHosts(ctx, cfg, used, wanted)
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

		formedContract, proceed, err := c.formContract(ctx, host, fee, minInitialContractFunds, maxInitialContractFunds, blockHeight, budget, renterAddress, cfg)
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

func (c *contractor) runContractRenewals(ctx context.Context, cfg api.AutopilotConfig, blockHeight uint64, budget *types.Currency, renterAddress types.Address, toRenew []contractInfo) ([]api.ContractMetadata, error) {
	ctx, span := tracing.Tracer.Start(ctx, "runContractRenewals")
	defer span.End()

	renewed := make([]api.ContractMetadata, 0, len(toRenew))

	c.logger.Debugw(
		"run contracts renewals",
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

	for _, ci := range toRenew {
		// TODO: keep track of consecutive failures and break at some point

		// break if the contractor was stopped
		if c.isStopped() {
			break
		}

		contract, proceed, err := c.renewContract(ctx, ci, cfg, blockHeight, budget, renterAddress)
		if err == nil {
			renewed = append(renewed, contract)
		}
		if !proceed {
			break
		}
	}

	return renewed, nil
}

func (c *contractor) runContractRefreshes(ctx context.Context, cfg api.AutopilotConfig, blockHeight uint64, budget *types.Currency, renterAddress types.Address, toRefresh []contractInfo) ([]api.ContractMetadata, error) {
	ctx, span := tracing.Tracer.Start(ctx, "runContractRefreshes")
	defer span.End()

	refreshed := make([]api.ContractMetadata, 0, len(toRefresh))

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

		// break if the contractor was stopped
		if c.isStopped() {
			break
		}

		contract, proceed, err := c.refreshContract(ctx, ci, cfg, blockHeight, budget, renterAddress)
		if err == nil {
			refreshed = append(refreshed, contract)
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
	// refresh with double the funds
	refreshAmount := ci.contract.TotalCost.Mul64(2)

	// estimate the txn fee
	txnFee, err := c.ap.bus.RecommendedFee(ctx)
	if err != nil {
		return types.ZeroCurrency, err
	}
	txnFeeEstimate := txnFee.Mul64(estimatedFileContractTransactionSetSize)

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

func (c *contractor) renewFundingEstimate(ctx context.Context, cfg api.AutopilotConfig, blockHeight uint64, ci contractInfo) (types.Currency, error) {
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
	siaFundFeeEstimate := (consensus.State{Index: types.ChainIndex{Height: blockHeight}}).FileContractTax(types.FileContract{Payout: subTotal})

	// estimate the txn fee
	txnFee, err := c.ap.bus.RecommendedFee(ctx)
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
	cappedEstimatedCost := estimatedCost
	if cappedEstimatedCost.Cmp(minimum) < 0 {
		cappedEstimatedCost = minimum
	}
	c.logger.Debugw("renew estimate",
		"fcid", ci.contract.ID,
		"dataStored", dataStored,
		"storageCost", storageCost.String(),
		"prevUploadDataEstimate", prevUploadDataEstimate.String(),
		"estimatedCost", estimatedCost.String(),
		"minInitialContractFunds", minInitialContractFunds.String(),
		"minimum", minimum.String(),
		"cappedEstimatedCost", cappedEstimatedCost.String(),
	)
	return cappedEstimatedCost, nil
}

func (c *contractor) candidateHosts(ctx context.Context, cfg api.AutopilotConfig, used map[types.PublicKey]bool, wanted int) ([]hostdb.Host, error) {
	c.logger.Debugf("looking for %d candidate hosts", wanted)

	// nothing to do
	if wanted == 0 {
		return nil, nil
	}

	// fetch gouging settings
	gs, err := c.ap.bus.GougingSettings(ctx)
	if err != nil {
		return nil, err
	}

	// fetch redundancy settings
	rs, err := c.ap.bus.RedundancySettings(ctx)
	if err != nil {
		return nil, err
	}

	// create IP filter
	ipFilter := newIPFilter(c.logger)

	// fetch all hosts
	hosts, err := c.ap.bus.Hosts(ctx, 0, -1)
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

func (c *contractor) renewContract(ctx context.Context, ci contractInfo, cfg api.AutopilotConfig, blockHeight uint64, budget *types.Currency, renterAddress types.Address) (cm api.ContractMetadata, proceed bool, err error) {
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
	contract := ci.contract
	settings := ci.settings
	fcid := contract.ID
	rev := contract.Revision
	hk := contract.HostKey()

	// calculate the renter funds
	renterFunds, err := c.renewFundingEstimate(ctx, cfg, blockHeight, ci)
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
	newCollateral := ContractRenewalCollateral(rev.FileContract, renterFunds, settings, endHeight)

	// renew the contract
	newRevision, _, err := c.ap.worker.RHPRenew(ctx, fcid, endHeight, hk, contract.HostIP, renterAddress, renterFunds, newCollateral)
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
	renewedContract, err := c.ap.bus.AddRenewedContract(ctx, newRevision, renterFunds, blockHeight, fcid)
	if err != nil {
		c.logger.Errorw(fmt.Sprintf("renewal failed to persist, err: %v", err), "hk", hk, "fcid", fcid)
		return api.ContractMetadata{}, false, err
	}

	c.logger.Debugw(
		"renewal succeeded",
		"fcid", renewedContract.ID,
		"renewedFrom", fcid,
	)
	return renewedContract, true, nil
}

func (c *contractor) refreshContract(ctx context.Context, ci contractInfo, cfg api.AutopilotConfig, blockHeight uint64, budget *types.Currency, renterAddress types.Address) (cm api.ContractMetadata, proceed bool, err error) {
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
		c.logger.Debugw("insufficient budget", "budget", budget, "needed", renterFunds)
		return api.ContractMetadata{}, false, fmt.Errorf("insufficient budget: %s < %s", budget.String(), renterFunds.String())
	}

	// calculate the new collateral
	newCollateral := ContractRenewalCollateral(rev.FileContract, renterFunds, settings, contract.EndHeight())

	// do not refresh if the contract's updated collateral will fall below the threshold anyway
	_, hostMissedPayout, _ := rhpv2.CalculateHostPayouts(rev.FileContract, newCollateral, settings, contract.EndHeight())
	if isBelowCollateralThreshold(newCollateral, hostMissedPayout) {
		err := fmt.Errorf("refresh failed, refreshed contract collateral (%v) is below threshold", hostMissedPayout)
		c.logger.Errorw(err.Error(), "hk", hk, "fcid", fcid, "newCollateral", newCollateral.String(), "hostMissedPayout", hostMissedPayout.String(), "maxCollateral", settings.MaxCollateral)
		return api.ContractMetadata{}, true, err
	}

	// renew the contract
	newRevision, _, err := c.ap.worker.RHPRenew(ctx, contract.ID, contract.EndHeight(), hk, contract.HostIP, renterAddress, renterFunds, newCollateral)
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
	refreshedContract, err := c.ap.bus.AddRenewedContract(ctx, newRevision, renterFunds, blockHeight, contract.ID)
	if err != nil {
		c.logger.Errorw(fmt.Sprintf("refresh failed, err: %v", err), "hk", hk, "fcid", fcid)
		return api.ContractMetadata{}, false, err
	}

	// add to renewed set
	c.logger.Debugw("refresh succeeded",
		"fcid", refreshedContract.ID,
		"renewedFrom", contract.ID)
	return refreshedContract, true, nil
}

func (c *contractor) formContract(ctx context.Context, host hostdb.Host, fee, minInitialContractFunds, maxInitialContractFunds types.Currency, blockHeight uint64, budget *types.Currency, renterAddress types.Address, cfg api.AutopilotConfig) (cm api.ContractMetadata, proceed bool, err error) {
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

	// fetch host settings
	scan, err := c.ap.worker.RHPScan(ctx, hk, host.NetAddress, 0)
	if err != nil {
		c.logger.Debugw(err.Error(), "hk", hk)
		return api.ContractMetadata{}, true, err
	}

	// check our budget
	txnFee := fee.Mul64(estimatedFileContractTransactionSetSize)
	renterFunds := initialContractFunding(scan.Settings, txnFee, minInitialContractFunds, maxInitialContractFunds)
	if budget.Cmp(renterFunds) < 0 {
		c.logger.Debugw("insufficient budget", "budget", budget, "needed", renterFunds)
		return api.ContractMetadata{}, false, errors.New("insufficient budget")
	}

	// calculate the host collateral
	hostCollateral := rhpv2.ContractFormationCollateral(cfg.Contracts.Storage/cfg.Contracts.Amount, cfg.Contracts.Period, scan.Settings)

	// form contract
	contract, _, err := c.ap.worker.RHPForm(ctx, endHeight(cfg, c.currentPeriod()), hk, host.NetAddress, renterAddress, renterFunds, hostCollateral)
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
	formedContract, err := c.ap.bus.AddContract(ctx, contract, renterFunds, blockHeight)
	if err != nil {
		c.logger.Errorw(fmt.Sprintf("contract formation failed, err: %v", err), "hk", hk)
		return api.ContractMetadata{}, true, err
	}

	c.logger.Debugw("formation succeeded",
		"hk", hk,
		"fcid", formedContract.ID)
	return formedContract, true, nil
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

// TODO: remove this after merging the fix in the core package.
func ContractRenewalCollateral(fc types.FileContract, renterFunds types.Currency, host rhpv2.HostSettings, endHeight uint64) types.Currency {
	if endHeight < fc.EndHeight() {
		panic("endHeight should be at least the current end height of the contract")
	}
	extension := endHeight - fc.EndHeight()

	// calculate cost per byte
	costPerByte := host.UploadBandwidthPrice.Add(host.StoragePrice).Add(host.DownloadBandwidthPrice)
	if costPerByte.IsZero() {
		return types.ZeroCurrency
	}

	// calculate the base collateral - if it exceeds MaxCollateral we can't add more collateral
	baseCollateral := host.Collateral.Mul64(fc.Filesize).Mul64(extension)
	if baseCollateral.Cmp(host.MaxCollateral) >= 0 {
		return types.ZeroCurrency
	}

	// calculate the new collateral
	newCollateral := host.Collateral.Mul(renterFunds.Div(costPerByte))

	// if the total collateral is more than the MaxCollateral subtract the delta.
	totalCollateral := baseCollateral.Add(newCollateral)
	if totalCollateral.Cmp(host.MaxCollateral) > 0 {
		delta := totalCollateral.Sub(host.MaxCollateral)
		if delta.Cmp(newCollateral) > 0 {
			newCollateral = types.ZeroCurrency
		} else {
			newCollateral = newCollateral.Sub(delta)
		}
	}

	return newCollateral
}
