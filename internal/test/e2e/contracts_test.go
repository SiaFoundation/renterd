package e2e

import (
	"context"
	"fmt"
	"testing"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/bus/client"
	"go.sia.tech/renterd/internal/test"
)

func TestFormContract(t *testing.T) {
	// configure the autopilot not to form any contracts
	apCfg := test.AutopilotConfig
	apCfg.Contracts.Amount = 0

	// create cluster
	opts := clusterOptsDefault
	opts.autopilotConfig = &apCfg
	cluster := newTestCluster(t, opts)
	defer cluster.Shutdown()

	// convenience variables
	b := cluster.Bus
	tt := cluster.tt

	// add a host
	hosts := cluster.AddHosts(1)
	h, err := b.Host(context.Background(), hosts[0].PublicKey())
	tt.OK(err)

	// fetch consensus state
	cs, err := b.ConsensusState(context.Background())
	tt.OK(err)

	// form a contract using the bus
	wallet, _ := b.Wallet(context.Background())
	ap, err := b.AutopilotConfig(context.Background())
	tt.OK(err)
	endHeight := cs.BlockHeight + ap.Contracts.Period + ap.Contracts.RenewWindow
	contract, err := b.FormContract(context.Background(), wallet.Address, types.Siacoins(1), h.PublicKey, h.NetAddress, types.Siacoins(1), endHeight)
	tt.OK(err)

	// assert the contract was added to the bus
	_, err = b.Contract(context.Background(), contract.ID)
	tt.OK(err)

	// mine to the renew window
	cluster.MineToRenewWindow()

	// update autopilot config to allow for 1 contract, this won't form a
	// contract but will ensure we don't skip contract maintenance, which should
	// renew the contract we formed
	contracts := ap.Contracts
	contracts.Amount = 1
	tt.OK(b.UpdateAutopilotConfig(context.Background(), client.WithContractsConfig(contracts)))

	// assert the contract gets renewed and thus maintained
	var renewalID types.FileContractID
	tt.Retry(300, 100*time.Millisecond, func() error {
		contracts, err := cluster.Bus.Contracts(context.Background(), api.ContractsOpts{})
		if err != nil {
			return err
		}
		if len(contracts) != 1 {
			return fmt.Errorf("unexpected number of contracts %d != 1", len(contracts))
		}
		if contracts[0].RenewedFrom != contract.ID {
			return fmt.Errorf("contract wasn't renewed %v != %v", contracts[0].RenewedFrom, contract.ID)
		}
		renewalID = contracts[0].ID
		return nil
	})

	// assert the contract is good
	tt.Retry(300, 100*time.Millisecond, func() error {
		contracts, err := b.Contracts(context.Background(), api.ContractsOpts{FilterMode: api.ContractFilterModeGood})
		tt.OK(err)
		if len(contracts) != 1 {
			return fmt.Errorf("expected 1 contract, got %v", len(contracts))
		} else if contracts[0].ID != renewalID {
			return fmt.Errorf("expected contract %v, got %v", contract.ID, contracts[0].ID)
		}
		return nil
	})
}
