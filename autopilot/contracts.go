package autopilot

import (
	"time"

	"gitlab.com/NebulousLabs/errors"
	"go.sia.tech/renterd/internal/consensus"
	rhpv2 "go.sia.tech/renterd/rhp/v2"
	"go.sia.tech/renterd/worker"
	"go.sia.tech/siad/types"
	"golang.org/x/crypto/blake2b"
)

const contractSetName = "autopilot"

func logErr(err error) {} // TODO

func (ap *Autopilot) currentHeight() uint64 {
	return ap.store.Tip().Height
}

func (ap *Autopilot) currentPeriod() uint64 {
	c := ap.store.Config()
	h := ap.store.Tip().Height
	return h + c.Contracts.Period
}

// TODO: deriving the renter key from the host key using the master key only
// works if we persist a hash of the renter's master key in the database and
// compare it on startup, otherwise there's no way of knowing the derived key is
// usuable
func (ap *Autopilot) deriveRenterKey(hostKey consensus.PublicKey) consensus.PrivateKey {
	seed := blake2b.Sum256(append(ap.masterKey[:], hostKey[:]...))
	pk := consensus.NewPrivateKeyFromSeed(seed[:])
	for i := range seed {
		seed[i] = 0
	}
	return pk
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

func (ap *Autopilot) renewableContracts(renewWindow uint64) ([]worker.Contract, error) {
	cs, err := ap.bus.RenewableContracts(renewWindow)
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

func (ap *Autopilot) contractLoop() {
	for {
		select {
		// TODO: use build var for loop interval
		// TODO: add wakeChan (to trigger loop)
		// TODO: run loop immediately
		// TODO: logging
		case <-ap.stopChan:
			return
		case <-time.After(time.Hour):
		}

		// don't run the contract loop if we are not synced
		if !ap.store.Synced() {
			continue
		}

		// fetch all contracts
		cs, err := ap.defaultContracts()
		if err != nil {
			logErr(err)
			continue
		}

		// run checks on the contract set
		// TODO: dedupe existing contracts (?)
		// TODO: check for IP range violations (?)

		// run checks on the contracts individually
		// TODO: separate loop (?)
		csMap := make(map[string]int)
		for i, c := range cs {
			csMap[c.ID.String()] = i
			// TODO: is host in host DB
			// TODO: is max revision
			// TODO: is offline
			// TODO: is gouging
			// TODO: is low score
			// TODO: is out of funds
			// TODO: update some contract metadata entity to reflect these checks
		}

		// gather some basic info
		config := ap.store.Config()
		needed := config.Contracts.Hosts
		renewW := config.Contracts.RenewWindow
		period := ap.currentPeriod()
		renterAddress, err := ap.bus.WalletAddress()
		if err != nil {
			logErr(err)
			continue
		}

		// TODO: check remaining funds before renew | form

		// renew all renewable contracts
		toRenew, err := ap.renewableContracts(renewW)
		if err != nil {
			logErr(err)
			continue
		}
		for _, renew := range toRenew {
			// renew contract
			renterKey := ap.deriveRenterKey(renew.HostKey)
			renewed, err := ap.renewContract(renew, renterKey, renterAddress, period)
			if err != nil {
				// TODO: handle error properly, if the wallet ran out of outputs
				// here there's no point in renewing more contracts until a
				// block is mined, maybe we could/should wait for pending transactions?
				logErr(err)
				continue
			}

			// swap contract in contract set
			cs[csMap[renew.ID.String()]] = worker.Contract{
				HostKey:   renew.HostKey,
				HostIP:    renew.HostIP,
				ID:        renewed.ID(),
				RenterKey: renterKey,
			}

			// persist the contract
			err = ap.bus.AddContract(renewed)
			if err != nil {
				logErr(err)
				continue
			}
		}

		// form missing contracts
		missing := int(needed) - len(cs)              // TODO: add leeway so we don't form hosts if we dip slightly under `needed` (?)
		canidates, _ := ap.hostsForContracts(missing) // TODO: add leeway so we have more than enough canidates
		for h := 0; missing > 0 && h < len(canidates); h++ {
			// fetch host IP
			candidate := canidates[h]
			host, err := ap.bus.Host(candidate)
			if err != nil {
				logErr(err)
				continue
			}
			hostIP := host.NetAddress()

			// form contract
			renterKey := ap.deriveRenterKey(candidate)
			formed, err := ap.formContract(candidate, hostIP, renterKey, renterAddress, period)
			if err != nil {
				// TODO: handle error properly, if the wallet ran out of outputs
				// here there's no point in forming more contracts until a block
				// is mined, maybe we could/should wait for pending transactions?
				logErr(err)
				continue
			}

			// add contract to contract set
			cs = append(cs, worker.Contract{
				HostKey:   candidate,
				HostIP:    hostIP,
				ID:        formed.ID(),
				RenterKey: renterKey,
			})

			// persist contract in store
			err = ap.bus.AddContract(formed)
			if err != nil {
				logErr(err)
				continue
			}

			missing--
		}

		// TODO update contract set
		contracts := make([]consensus.PublicKey, len(cs))
		for i, c := range cs {
			contracts[i] = c.HostKey
		}
		err = ap.bus.SetHostSet(contractSetName, contracts)
		if err != nil {
			logErr(err)
		}
	}
}

func (ap *Autopilot) formContract(hostKey consensus.PublicKey, hostIP string, renterKey consensus.PrivateKey, renterAddress types.UnlockHash, period uint64) (rhpv2.Contract, error) {
	// fetch host settings
	scan, err := ap.worker.RHPScan(hostKey, hostIP)
	if err != nil {
		return rhpv2.Contract{}, err
	}

	// prepare contract formation
	endHeight := ap.currentHeight() + period
	renterFunds, hostCollateral := ap.calculateFundsAndCollateral(scan.Settings)
	fc, cost, err := ap.worker.RHPPrepareForm(renterKey, hostKey, renterFunds, renterAddress, hostCollateral, endHeight, scan.Settings)
	if err != nil {
		return rhpv2.Contract{}, err
	}

	// fund the transaction
	txn := types.Transaction{FileContracts: []types.FileContract{fc}}
	toSign, parents, err := ap.bus.WalletFund(&txn, cost)
	if err != nil {
		return rhpv2.Contract{}, errors.Compose(err, ap.bus.WalletDiscard(txn))
	}

	// sign the transaction
	err = ap.bus.WalletSign(&txn, toSign, types.FullCoveredFields)
	if err != nil {
		return rhpv2.Contract{}, errors.Compose(err, ap.bus.WalletDiscard(txn))
	}

	// form the contract
	contract, _, err := ap.worker.RHPForm(renterKey, hostKey, hostIP, append(parents, txn))
	if err != nil {
		return rhpv2.Contract{}, errors.Compose(err, ap.bus.WalletDiscard(txn))
	}

	return contract, nil
}

func (ap *Autopilot) renewContract(c worker.Contract, renterKey consensus.PrivateKey, renterAddress types.UnlockHash, period uint64) (contract rhpv2.Contract, err error) {
	// handle contract locking
	revision, err := ap.bus.AcquireContractLock(c.ID)
	if err != nil {
		return rhpv2.Contract{}, nil
	}
	defer func() {
		err = errors.Compose(err, ap.bus.ReleaseContractLock(c.ID))
	}()

	// fetch host settings
	scan, err := ap.worker.RHPScan(c.HostKey, c.HostIP)
	if err != nil {
		return rhpv2.Contract{}, err
	}

	// prepare the renewal
	endHeight := ap.currentHeight() + period
	renterFunds, hostCollateral := ap.calculateFundsAndCollateral(scan.Settings)
	fc, cost, finalPayment, err := ap.worker.RHPPrepareRenew(revision, renterKey, c.HostKey, renterFunds, renterAddress, hostCollateral, endHeight, scan.Settings)
	if err != nil {
		return rhpv2.Contract{}, err
	}

	// fund the transaction
	txn := types.Transaction{FileContracts: []types.FileContract{fc}}
	toSign, parents, err := ap.bus.WalletFund(&txn, cost)
	if err != nil {
		return rhpv2.Contract{}, errors.Compose(err, ap.bus.WalletDiscard(txn))
	}

	// sign the transaction
	err = ap.bus.WalletSign(&txn, toSign, types.FullCoveredFields)
	if err != nil {
		return rhpv2.Contract{}, errors.Compose(err, ap.bus.WalletDiscard(txn))
	}

	// renew the contract
	txnSet := append(parents, txn)
	renewed, _, err := ap.worker.RHPRenew(renterKey, c.HostKey, c.HostIP, c.ID, txnSet, finalPayment)
	if err != nil {
		return rhpv2.Contract{}, errors.Compose(err, ap.bus.WalletDiscard(txn))
	}
	return renewed, nil
}

func (ap *Autopilot) calculateFundsAndCollateral(hs rhpv2.HostSettings) (types.Currency, types.Currency) {
	c := ap.store.Config()
	download := c.Contracts.Download
	upload := c.Contracts.Upload
	duration := c.Contracts.Period

	uploadCost := hs.UploadBandwidthPrice.Mul64(upload)
	downloadCost := hs.DownloadBandwidthPrice.Mul64(download)
	storageCost := hs.StoragePrice.Mul64(upload).Mul64(duration)

	renterFunds := hs.ContractPrice.Add(uploadCost).Add(downloadCost).Add(storageCost)
	hostCollateral := hs.Collateral.Mul64(upload).Mul64(duration)
	return renterFunds, hostCollateral
}
