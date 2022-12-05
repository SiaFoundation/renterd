package autopilot

import (
	"sort"
	"time"

	"go.sia.tech/renterd/bus"
	"go.sia.tech/renterd/internal/consensus"
	rhpv2 "go.sia.tech/renterd/rhp/v2"
	"go.sia.tech/siad/types"
)

const (
	// estimatedFileContractTransactionSetSize is the estimated blockchain size
	// of a transaction set between a renter and a host that contains a file
	// contract.
	estimatedFileContractTransactionSetSize = 2048

	contractLockingDurationRenew = 30 * time.Second
)

type (
	contractor struct {
		ap *Autopilot

		blockHeight   uint64
		currentPeriod uint64
	}
)

func newContractor(ap *Autopilot) *contractor {
	return &contractor{
		ap: ap,
	}
}

func (c *contractor) applyConsensusState(cfg Config, state bus.ConsensusState) {
	c.blockHeight = state.BlockHeight
	// TODO: update current period
}

func (c *contractor) remainingFunds(cfg Config) (remaining types.Currency) {
	var spent types.Currency // TODO: period spending

	if cfg.Contracts.Allowance.Cmp(spent) > 0 {
		remaining = cfg.Contracts.Allowance.Sub(spent)
	}
	return
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
	active, err := c.ap.bus.ActiveContracts()
	if err != nil {
		return err
	}

	// run checks
	toRenew, toDelete, err := c.runContractChecks(cfg, active)
	if err != nil {
		return err
	}

	// delete contracts
	err = c.ap.bus.DeleteContracts(toDelete)
	if err != nil {
		return err
	}

	// figure out remaining funds
	remaining := c.remainingFunds(cfg)

	// run renewals
	renewed, err := c.runContractRenewals(cfg, &remaining, address, toRenew)
	if err != nil {
		return err
	}

	// run formations
	formed, err := c.runContractFormations(cfg, &remaining, address)
	if err != nil {
		return err
	}

	// update contract set
	err = c.ap.updateDefaultContracts(active, toRenew, renewed, formed, toDelete)
	if err != nil {
		return err
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

	// check every active contract
	for _, contract := range contracts {
		// fetch host from hostdb
		host, err := c.ap.bus.Host(contract.HostKey)
		if err != nil {
			continue // TODO: log err
		}

		// fetch contract from contract store
		contractData, err := c.ap.bus.Contract(contract.ID)
		if err != nil {
			continue // TODO: log err
		}

		// fetch metadata
		metadata, err := c.ap.bus.ContractMetadata(contract.ID)
		if err != nil {
			continue // TODO: log err
		}

		// decide whether the host is still good
		usable, _ := isUsableHost(cfg, f, Host{host})
		if !usable {
			toDelete = append(toDelete, contract.ID) // TODO: log reasons
			continue
		}

		// decide whether the contract is still good
		usable, renewable, _ := isUsableContract(cfg, Host{host}, contractData, metadata, c.blockHeight)
		if !usable && !renewable {
			toDelete = append(toDelete, contract.ID) // TODO: log reasons
			continue
		} else if !usable {
			renewIndices[contract.ID] = len(toRenew)
			toRenew = append(toRenew, contract)
		}

		// keep track of file size
		contractIds = append(contractIds, contract.ID)
		contractSizes[contract.ID] = contractData.Revision.NewFileSize
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

	// perform the renewals
	for _, renew := range toRenew {
		// break if autopilot is stopped
		if c.isStopped() {
			break
		}

		// check our budget
		renterFunds, err := c.renewFundingEstimate(cfg, renew.ID)
		if budget.Cmp(renterFunds) < 0 {
			break
		}

		// derive the renter key
		renterKey := c.ap.deriveRenterKey(renew.HostKey)
		if err != nil {
			return nil, err
		}

		var hostCollateral types.Currency // TODO
		contract, err := c.renewContract(cfg, renew, renterKey, renterAddress, renterFunds, hostCollateral)
		if err != nil {
			// TODO: handle error properly, if the wallet ran out of outputs
			// here there's no point in renewing more contracts until a
			// block is mined, maybe we could/should wait for pending transactions?
			return nil, err
		}

		// update the budget
		*budget = budget.Sub(renterFunds)

		// persist the contract
		err = c.ap.bus.AddRenewedContract(contract, renew.ID)
		if err != nil {
			return nil, err
		}

		// persist the metadata
		// TODO: instead of updating the metadata separately,
		// AddRenewedContract should do that.
		metadata := bus.ContractMetadata{RenewedFrom: renew.ID, TotalCost: renterFunds}
		err = c.ap.bus.UpdateContractMetadata(contract.ID(), metadata)
		if err != nil {
			return nil, err
		}

		// add to renewed set
		renewed = append(renewed, bus.Contract{
			ID:               contract.ID(),
			HostKey:          renew.HostKey,
			HostIP:           renew.HostIP,
			StartHeight:      c.blockHeight,
			EndHeight:        contract.EndHeight(),
			ContractMetadata: metadata,
		})
	}

	return renewed, nil
}

func (c *contractor) runContractFormations(cfg Config, budget *types.Currency, renterAddress types.UnlockHash) ([]bus.Contract, error) {
	// fetch all active contracts
	active, err := c.ap.bus.ActiveContracts()
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
	var formed []bus.Contract
	missing := int(cfg.Contracts.Hosts) - len(active) // TODO: add leeway so we don't form contracts if we dip slightly under `needed` (?)
	canidates, _ := c.candidateHosts(cfg, missing)    // TODO: add leeway so we have more than enough canidates
	for h := 0; missing > 0 && h < len(canidates); h++ {
		// break if autopilot is stopped
		if c.isStopped() {
			break
		}

		// fetch host IP
		candidate := canidates[h]
		host, err := c.ap.bus.Host(candidate)
		if err != nil {
			// TODO: log error
			continue
		}
		hostIP := host.NetAddress()

		// fetch host settings
		scan, err := c.ap.worker.RHPScan(host.PublicKey, hostIP, 0)
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
		contract, err := c.formContract(cfg, candidate, hostIP, hostSettings, renterKey, renterAddress, renterFunds, hostCollateral)
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
		// TODO: Instead of doing a separate metadata update,
		// AddContract should already save the metadata.
		metadata := bus.ContractMetadata{TotalCost: renterFunds}
		err = c.ap.bus.UpdateContractMetadata(contract.ID(), metadata)
		if err != nil {
			return nil, err
		}

		if err != nil {
			return nil, err
		}

		// add contract to contract set
		formed = append(formed, bus.Contract{
			ID:               contract.ID(),
			HostKey:          candidate,
			HostIP:           hostIP,
			StartHeight:      c.blockHeight,
			EndHeight:        contract.EndHeight(),
			ContractMetadata: metadata,
		})

		missing--
	}

	return formed, nil
}

func (c *contractor) renewContract(cfg Config, toRenew bus.Contract, renterKey consensus.PrivateKey, renterAddress types.UnlockHash, renterFunds, hostCollateral types.Currency) (rhpv2.Contract, error) {
	// handle contract locking
	revision, err := c.ap.bus.AcquireContract(toRenew.ID, contractLockingDurationRenew)
	if err != nil {
		return rhpv2.Contract{}, nil
	}
	defer c.ap.bus.ReleaseContract(toRenew.ID)

	// fetch host settings
	scan, err := c.ap.worker.RHPScan(toRenew.HostKey, toRenew.HostIP, 0)
	if err != nil {
		return rhpv2.Contract{}, nil
	}

	// prepare the renewal
	endHeight := c.currentPeriod + cfg.Contracts.Period + cfg.Contracts.RenewWindow
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

func (c *contractor) formContract(cfg Config, hostKey consensus.PublicKey, hostIP string, hostSettings rhpv2.HostSettings, renterKey consensus.PrivateKey, renterAddress types.UnlockHash, renterFunds, hostCollateral types.Currency) (rhpv2.Contract, error) {
	// prepare contract formation
	endHeight := c.currentPeriod + cfg.Contracts.Period + cfg.Contracts.RenewWindow
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
		return types.ZeroCurrency, err
	}

	// fetch host
	host, err := c.ap.bus.Host(contract.HostKey())
	if err != nil {
		return types.ZeroCurrency, err
	}

	// fetch host settings
	scan, err := c.ap.worker.RHPScan(contract.HostKey(), host.NetAddress(), 0)
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
	spendingHistory, err := c.ap.bus.SpendingHistory(id, c.currentPeriod)
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

func (c *contractor) candidateHosts(cfg Config, wanted int) ([]consensus.PublicKey, error) {
	// fetch all contracts
	active, err := c.ap.bus.ActiveContracts()
	if err != nil {
		return nil, err
	}

	// build a map
	used := make(map[string]bool)
	for _, contract := range active {
		used[contract.HostKey.String()] = true
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

		if usable, _ := isUsableHost(cfg, ipFilter, Host{h}); usable {
			filtered = append(filtered, h)
		}
	}

	// update num wanted
	if wanted > len(hosts) {
		wanted = len(hosts)
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

	// select hosts
	var selected []consensus.PublicKey
	for len(selected) < wanted {
		i := randSelectByWeight(scores)
		selected = append(selected, scored[i].PublicKey)

		// remove selected host
		scored[i], scored = scored[len(scored)-1], scored[:len(scored)-1]
		scores[i], scores = scores[len(scores)-1], scores[:len(scores)-1]
	}
	return selected, nil
}

func (c *contractor) isStopped() bool {
	select {
	case <-c.ap.stopChan:
		return true
	default:
	}
	return false
}
