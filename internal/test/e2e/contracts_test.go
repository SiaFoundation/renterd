package e2e

import (
	"context"
	"fmt"
	"testing"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/internal/test"
	"go.uber.org/zap/zapcore"
)

func TestFormContract(t *testing.T) {
	// configure the autopilot not to form any contracts
	apSettings := test.AutopilotConfig
	apSettings.Contracts.Amount = 0

	// create cluster
	opts := clusterOptsDefault
	opts.autopilotSettings = &apSettings
	opts.logger = newTestLoggerCustom(zapcore.DebugLevel)
	cluster := newTestCluster(t, opts)
	defer cluster.Shutdown()

	// convenience variables
	b := cluster.Bus
	a := cluster.Autopilot
	tt := cluster.tt

	// add a host
	hosts := cluster.AddHosts(1)
	h, err := b.Host(context.Background(), hosts[0].PublicKey())
	tt.OK(err)

	// form a contract using the bus
	wallet, _ := b.Wallet(context.Background())
	ap, err := b.Autopilot(context.Background(), api.DefaultAutopilotID)
	tt.OK(err)
	contract, err := b.FormContract(context.Background(), wallet.Address, types.Siacoins(1), h.PublicKey, h.NetAddress, types.Siacoins(1), ap.EndHeight())
	tt.OK(err)

	// assert the contract was added to the bus
	_, err = b.Contract(context.Background(), contract.ID)
	tt.OK(err)

	// mine to the renew window
	cluster.MineToRenewWindow()

	// update autopilot config to allow for 1 contract, this won't form a
	// contract but will ensure we don't skip contract maintenance, which should
	// renew the contract we formed
	apSettings.Contracts.Amount = 1
	tt.OK(a.UpdateConfig(apSettings))

	// assert the contract gets renewed and thus maintained
	var renewalID types.FileContractID
	tt.Retry(100, 100*time.Millisecond, func() error {
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

	// assert the contract is part of the contract set
	contracts, err := b.Contracts(context.Background(), api.ContractsOpts{ContractSet: test.ContractSet})
	tt.OK(err)
	if len(contracts) != 1 {
		t.Fatalf("expected 1 contract, got %v", len(contracts))
	} else if contracts[0].ID != renewalID {
		t.Fatalf("expected contract %v, got %v", contract.ID, contracts[0].ID)
	}
}
