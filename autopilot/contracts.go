package autopilot

import (
	"math"
	"time"

	"go.sia.tech/renterd/bus"
	"go.sia.tech/renterd/internal/consensus"
	rhpv2 "go.sia.tech/renterd/rhp/v2"
	"go.sia.tech/renterd/worker"
	"go.sia.tech/siad/types"
	"golang.org/x/crypto/blake2b"
)

const (
	// contractSetName defines the name of the default contract set
	contractSetName = "autopilot"

	// contractLoopInterval defines the interval with which we run the contract loop
	contractLoopInterval = 15 * time.Minute

	// estimatedFileContractTransactionSetSize is the estimated blockchain size
	// of a transaction set between a renter and a host that contains a file
	// contract.
	estimatedFileContractTransactionSetSize = 2048
)

func logErr(err error) {} // TODO

func (ap *Autopilot) contractLoop() {
	ticker := time.NewTicker(contractLoopInterval)

	for {
		// TODO: add wakeChan
		// TODO: use triggerChan
		select {
		case <-ap.stopChan:
			return
		case <-ticker.C:
		}

		// re-use same state and config in every iteration
		config := ap.store.Config()
		state := ap.store.State()

		// don't run the contract loop if we are not synced
		// TODO: use a channel
		if !state.Synced {
			continue
		}

		// return early if no hosts are requested
		if config.Contracts.Hosts == 0 {
			return
		}

		// fetch our wallet address
		address, err := ap.bus.WalletAddress()
		if err != nil {
			logErr(err)
			continue
		}

		// run checks
		err = ap.runContractChecks()
		if err != nil {
			logErr(err)
			continue
		}

		// figure out remaining funds
		spent, err := ap.periodSpending()
		if err != nil {
			logErr(err)
			continue
		}
		var remaining types.Currency
		if config.Contracts.Allowance.Cmp(spent) > 0 {
			remaining = config.Contracts.Allowance.Sub(spent)
		}

		// run renewals
		renewed, err := ap.runContractRenewals(config, state, &remaining, address)
		if err != nil {
			logErr(err)
			continue
		}

		// run formations
		formed, err := ap.runContractFormations(config, state, &remaining, address)
		if err != nil {
			logErr(err)
			continue
		}

		// update contract set
		err = ap.updateDefaultContractSet(renewed, formed)
		if err != nil {
			logErr(err)
			continue
		}
	}
}

func (ap *Autopilot) updateDefaultContractSet(renewed, formed []worker.Contract) error {
	// fetch current set
	cs, err := ap.defaultContracts()
	if err != nil {
		return err
	}

	// build hostkey -> index map
	csMap := make(map[string]int)
	for i, contract := range cs {
		csMap[contract.HostKey.String()] = i
	}

	// swap renewed contracts
	for _, contract := range renewed {
		index, exists := csMap[contract.HostKey.String()]
		if !exists {
			// TODO: panic/log? shouldn't happen
			csMap[contract.HostKey.String()] = len(cs)
			cs = append(cs, contract)
			continue
		}
		cs[index] = contract
	}

	// append formations
	for _, contract := range formed {
		_, exists := csMap[contract.HostKey.String()]
		if exists {
			// TODO: panic/log? shouldn't happen
			continue
		}
		cs = append(cs, contract)
	}

	// update contract set
	contracts := make([]consensus.PublicKey, len(cs))
	for i, c := range cs {
		contracts[i] = c.HostKey
	}
	err = ap.bus.SetHostSet(contractSetName, contracts)
	if err != nil {
		return err
	}
	return nil
}

func (ap *Autopilot) runContractChecks() error {
	// fetch all active contracts
	active, err := ap.bus.ActiveContracts(math.MaxUint64)
	if err != nil {
		return err
	}

	// TODO: check for IP range violations

	// run checks on the contracts individually
	for _, contract := range active {
		// grab contract metadata
		metadata := contract.Metadata

		// update metadata:
		// TODO: is host in host DB
		// TODO: is max revision
		// TODO: is offline
		// TODO: is gouging
		// TODO: is low score
		// TODO: is out of funds

		// TODO: is up for renewal
		// TODO: is GFU limited (allowance # hsots)

		// update contract metadata
		err = ap.bus.UpdateContractMetadata(contract.ID, metadata)
		if err != nil {
			return err
		}
	}

	return nil
}

func (ap *Autopilot) runContractRenewals(cfg Config, s State, budget *types.Currency, renterAddress types.UnlockHash) ([]worker.Contract, error) {
	// fetch all contracts that are up for renew
	toRenew, err := ap.renewableContracts(s.BlockHeight + cfg.Contracts.RenewWindow)
	if err != nil {
		return nil, err
	}

	// perform the renewals
	var renewed []worker.Contract
	for _, renew := range toRenew {
		// check our budget
		renterFunds, err := ap.renewFundingEstimate(cfg, s, renew.ID)
		if budget.Cmp(renterFunds) < 0 {
			break
		}

		// derive the renter key
		renterKey := ap.deriveRenterKey(renew.HostKey)
		if err != nil {
			return nil, err
		}

		var hostCollateral types.Currency // TODO
		contract, err := ap.renewContract(cfg, s, renew, renterKey, renterAddress, renterFunds, hostCollateral)
		if err != nil {
			// TODO: handle error properly, if the wallet ran out of outputs
			// here there's no point in renewing more contracts until a
			// block is mined, maybe we could/should wait for pending transactions?
			return nil, err
		}

		// update the budget
		*budget = budget.Sub(renterFunds)

		// persist the contract
		err = ap.bus.AddContract(contract)
		if err != nil {
			return nil, err
		}

		// persist the metadata
		err = ap.bus.UpdateContractMetadata(contract.ID(), bus.ContractMetadata{
			RenewedFrom: renew.ID,
		})
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

func (ap *Autopilot) runContractFormations(cfg Config, s State, budget *types.Currency, renterAddress types.UnlockHash) ([]worker.Contract, error) {
	// fetch all active contracts
	active, err := ap.bus.ActiveContracts(math.MaxUint64)
	if err != nil {
		return nil, err
	}

	// fetch recommended txn fee
	fee, err := ap.bus.RecommendedFee()
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
	canidates, _ := ap.hostsForContracts(missing)     // TODO: add leeway so we have more than enough canidates
	for h := 0; missing > 0 && h < len(canidates); h++ {
		// fetch host IP
		candidate := canidates[h]
		host, err := ap.bus.Host(candidate)
		if err != nil {
			logErr(err)
			continue
		}
		hostIP := host.NetAddress()

		// fetch host settings
		scan, err := ap.worker.RHPScan(host.PublicKey, hostIP)
		if err != nil {
			logErr(err)
			continue
		}
		hostSettings := scan.Settings

		// check our budget
		txnFee := fee.Mul64(estimatedFileContractTransactionSetSize)
		renterFunds := ap.initialContractFunding(hostSettings, txnFee, minInitialContractFunds, maxInitialContractFunds)
		if budget.Cmp(renterFunds) < 0 {
			break
		}

		// form contract
		renterKey := ap.deriveRenterKey(candidate)
		var hostCollateral types.Currency // TODO
		contract, err := ap.formContract(cfg, s, candidate, hostIP, hostSettings, renterKey, renterAddress, renterFunds, hostCollateral)
		if err != nil {
			// TODO: handle error properly, if the wallet ran out of outputs
			// here there's no point in forming more contracts until a block
			// is mined, maybe we could/should wait for pending transactions?
			logErr(err)
			continue
		}

		// update the budget
		*budget = budget.Sub(renterFunds)

		// persist contract in store
		err = ap.bus.AddContract(contract)
		if err != nil {
			logErr(err)
			continue
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

func (ap *Autopilot) renewContract(cfg Config, s State, toRenew worker.Contract, renterKey consensus.PrivateKey, renterAddress types.UnlockHash, renterFunds, hostCollateral types.Currency) (rhpv2.Contract, error) {
	// handle contract locking
	revision, err := ap.bus.AcquireContractLock(toRenew.ID)
	if err != nil {
		return rhpv2.Contract{}, nil
	}
	defer ap.bus.ReleaseContractLock(toRenew.ID)

	// fetch host settings
	scan, err := ap.worker.RHPScan(toRenew.HostKey, toRenew.HostIP)
	if err != nil {
		return rhpv2.Contract{}, nil
	}

	// prepare the renewal
	endHeight := s.CurrentPeriod + cfg.Contracts.Period + cfg.Contracts.RenewWindow
	fc, cost, finalPayment, err := ap.worker.RHPPrepareRenew(revision, renterKey, toRenew.HostKey, renterFunds, renterAddress, hostCollateral, endHeight, scan.Settings)
	if err != nil {
		return rhpv2.Contract{}, nil
	}

	// fund the transaction
	txn := types.Transaction{FileContracts: []types.FileContract{fc}}
	toSign, parents, err := ap.bus.WalletFund(&txn, cost)
	if err != nil {
		_ = ap.bus.WalletDiscard(txn) // ignore error
		return rhpv2.Contract{}, err
	}

	// sign the transaction
	err = ap.bus.WalletSign(&txn, toSign, types.FullCoveredFields)
	if err != nil {
		_ = ap.bus.WalletDiscard(txn) // ignore error
		return rhpv2.Contract{}, err
	}

	// renew the contract
	txnSet := append(parents, txn)
	renewed, _, err := ap.worker.RHPRenew(renterKey, toRenew.HostKey, toRenew.HostIP, toRenew.ID, txnSet, finalPayment)
	if err != nil {
		_ = ap.bus.WalletDiscard(txn) // ignore error
		return rhpv2.Contract{}, err
	}
	return renewed, nil
}

func (ap *Autopilot) formContract(cfg Config, s State, hostKey consensus.PublicKey, hostIP string, hostSettings rhpv2.HostSettings, renterKey consensus.PrivateKey, renterAddress types.UnlockHash, renterFunds, hostCollateral types.Currency) (rhpv2.Contract, error) {
	// prepare contract formation
	endHeight := s.CurrentPeriod + cfg.Contracts.Period + cfg.Contracts.RenewWindow
	fc, cost, err := ap.worker.RHPPrepareForm(renterKey, hostKey, renterFunds, renterAddress, hostCollateral, endHeight, hostSettings)
	if err != nil {
		return rhpv2.Contract{}, err
	}

	// fund the transaction
	txn := types.Transaction{FileContracts: []types.FileContract{fc}}
	toSign, parents, err := ap.bus.WalletFund(&txn, cost)
	if err != nil {
		_ = ap.bus.WalletDiscard(txn) // ignore error
		return rhpv2.Contract{}, err
	}

	// sign the transaction
	err = ap.bus.WalletSign(&txn, toSign, types.FullCoveredFields)
	if err != nil {
		_ = ap.bus.WalletDiscard(txn) // ignore error
		return rhpv2.Contract{}, err
	}

	// form the contract
	contract, _, err := ap.worker.RHPForm(renterKey, hostKey, hostIP, append(parents, txn))
	if err != nil {
		_ = ap.bus.WalletDiscard(txn) // ignore error
		return rhpv2.Contract{}, err
	}

	return contract, nil
}

func (ap *Autopilot) defaultContracts() ([]worker.Contract, error) {
	cs, err := ap.bus.HostSetContracts(contractSetName)
	if err != nil {
		return nil, err
	}
	contracts := make([]worker.Contract, 0, len(cs))
	for _, c := range cs {
		if c.ID == (types.FileContractID{}) || c.HostIP == "" {
			continue
		}
		contracts = append(contracts, worker.Contract{
			HostKey:   c.HostKey,
			HostIP:    c.HostIP,
			ID:        c.ID,
			RenterKey: ap.deriveRenterKey(c.HostKey),
		})
	}
	return contracts, nil
}

func (ap *Autopilot) renewableContracts(endHeight uint64) ([]worker.Contract, error) {
	cs, err := ap.bus.ActiveContracts(endHeight)
	if err != nil {
		return nil, err
	}
	contracts := make([]worker.Contract, 0, len(cs))
	for _, c := range cs {
		if c.ID == (types.FileContractID{}) || c.HostIP == "" {
			continue
		}
		contracts = append(contracts, worker.Contract{
			HostKey:   c.HostKey,
			HostIP:    c.HostIP,
			ID:        c.ID,
			RenterKey: ap.deriveRenterKey(c.HostKey),
		})
	}
	return contracts, nil
}

func (ap *Autopilot) initialContractFunding(settings rhpv2.HostSettings, txnFee, min, max types.Currency) types.Currency {
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

func (ap *Autopilot) renewFundingEstimate(cfg Config, s State, cID types.FileContractID) (types.Currency, error) {
	// fetch contract
	c, err := ap.bus.ContractData(cID)
	if err != nil {
		return types.ZeroCurrency, err
	}

	// fetch host
	host, err := ap.bus.Host(c.HostKey())
	if err != nil {
		return types.ZeroCurrency, err
	}

	// fetch host settings
	scan, err := ap.worker.RHPScan(c.HostKey(), host.NetAddress())
	if err != nil {
		return types.ZeroCurrency, err
	}

	// estimate the cost of the current data stored
	dataStored := c.Revision.ToTransaction().FileContractRevisions[0].NewFileSize
	storageCost := types.NewCurrency64(dataStored).Mul64(cfg.Contracts.Period).Mul(scan.Settings.StoragePrice)

	// loop over the contract history to figure out the amount of money spent
	var prevUploadSpending types.Currency
	var prevDownloadSpending types.Currency
	var prevFundAccountSpending types.Currency
	hierarchy, err := ap.bus.ContractHistory(cID, s.CurrentPeriod)
	if err != nil {
		return types.ZeroCurrency, err
	}
	for _, contract := range hierarchy {
		spending := contract.Metadata.Spending
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
	siaFundFeeEstimate := types.Tax(types.BlockHeight(s.BlockHeight), subTtotal)

	// estimate the txn fee
	txnFee, err := ap.bus.RecommendedFee()
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
	minimum := ap.initialContractFunding(scan.Settings, txnFeeEstimate, minInitialContractFunds, types.ZeroCurrency)
	if estimatedCost.Cmp(minimum) < 0 {
		estimatedCost = minimum
	}
	return estimatedCost, nil
}

// TODO: deriving the renter key from the host key using the master key only
// works if we persist a hash of the renter's master key in the database and
// compare it on startup, otherwise there's no way of knowing the derived key is
// usuable
//
// TODO: instead of deriving a renter key use a randomly generated salt so we're
// not limited to one key per host
func (ap *Autopilot) deriveRenterKey(hostKey consensus.PublicKey) consensus.PrivateKey {
	seed := blake2b.Sum256(append(ap.masterKey[:], hostKey[:]...))
	pk := consensus.NewPrivateKeyFromSeed(seed[:])
	for i := range seed {
		seed[i] = 0
	}
	return pk
}

// TODO
func (ap *Autopilot) periodSpending() (types.Currency, error) {
	return types.ZeroCurrency, nil
}
