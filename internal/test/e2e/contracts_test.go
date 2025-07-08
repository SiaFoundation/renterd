package e2e

import (
	"context"
	"fmt"
	"testing"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/renterd/v2/api"
	"go.sia.tech/renterd/v2/bus/client"
	"go.sia.tech/renterd/v2/internal/test"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
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
	contract, err := b.FormContract(context.Background(), wallet.Address, types.Siacoins(1), h.PublicKey, types.Siacoins(1), endHeight)
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

func TestContractUsability(t *testing.T) {
	// prepare network with no maturity delay
	network, genesis := testNetwork()
	network.MaturityDelay = 0
	store, state, err := chain.NewDBStore(chain.NewMemDB(), network, genesis, nil)
	if err != nil {
		t.Fatal(err)
	}

	// prepare custom cluster options
	opts := clusterOptsDefault
	opts.autopilotConfig = &test.AutopilotConfig
	opts.autopilotConfig.Contracts.Amount = 0
	opts.funding = &clusterOptNoFunding
	opts.cm = chain.NewManager(store, state)

	// prepare custom logger
	core, logs := observer.New(zapcore.DebugLevel)
	opts.logger = zap.New(core)

	// create cluster
	cluster := newTestCluster(t, opts)
	defer cluster.Shutdown()

	// convenience variables
	b := cluster.Bus
	tt := cluster.tt

	// assert the wallet is empty and then fund it
	cs := opts.cm.TipState()
	if res, err := b.Wallet(context.Background()); err != nil {
		tt.Fatal(err)
	} else if !res.Spendable.IsZero() {
		tt.Fatalf("expected empty wallet, got %v", res.Spendable)
	} else if err := cluster.mineBlocks(res.Address, 1); err != nil {
		tt.Fatal(err)
	} else {
		time.Sleep(100 * time.Millisecond)
	}

	// assert the wallet is funded
	if res, err := b.Wallet(context.Background()); err != nil {
		tt.Fatal(err)
	} else if res.Confirmed.Cmp(cs.BlockReward()) != 0 {
		tt.Fatal("expected wallet to be funded", res.Confirmed)
	}

	// form a contract and assert it's good
	var contract api.ContractMetadata
	cluster.AddHosts(1)
	opts.autopilotConfig.Contracts.Amount = 1
	if err := b.UpdateAutopilotConfig(context.Background(), client.WithContractsConfig(opts.autopilotConfig.Contracts)); err != nil {
		tt.Fatal(err)
	} else if contracts := cluster.WaitForContracts(); len(contracts) != 1 {
		tt.Fatalf("expected 1 contract, got %d", len(contracts))
	} else if contracts[0].Usability != api.ContractUsabilityGood {
		tt.Fatalf("expected contract to be good, got %s", contracts[0].Usability)
	} else {
		contract = contracts[0]
		time.Sleep(100 * time.Millisecond)
	}

	// spend all the money
	if res, err := b.Wallet(context.Background()); err != nil {
		tt.Fatal(err)
	} else if _, err := b.SendSiacoins(context.Background(), types.StandardAddress(types.GeneratePrivateKey().PublicKey()), res.Unconfirmed, true, true); err != nil {
		tt.Fatal(err)
	} else if err := cluster.mineBlocks(types.VoidAddress, 1); err != nil {
		tt.Fatal(err)
	} else {
		time.Sleep(100 * time.Millisecond)
	}

	// mine to the renew window & trigger autopilot
	renewHeight := contract.EndHeight() - opts.autopilotConfig.Contracts.RenewWindow
	if err := cluster.mineBlocks(types.VoidAddress, renewHeight-opts.cm.Tip().Height); err != nil {
		tt.Fatal(err)
	} else if _, err := cluster.Autopilot.Trigger(context.Background(), false); err != nil {
		tt.Fatal(err)
	} else {
		time.Sleep(2 * testApCfg().Heartbeat)
	}

	// assert the logs contain an entry for the failed contract renewal
	var logFound bool
	for _, entry := range logs.All() {
		fields := entry.ContextMap()
		if val, ok := fields["contractID"]; !ok || val != contract.ID.String() {
			continue
		}
		if entry.Message == "failed to renew contract" && fields["ourFault"] == true {
			logFound = true
			break
		}
	}
	if !logFound {
		t.Fatal("expected log entry for failed contract renewal, but none found")
	}

	// assert the contract is still good
	if updated, err := b.Contract(context.Background(), contract.ID); err != nil {
		tt.Fatal(err)
	} else if updated.Usability != api.ContractUsabilityGood {
		tt.Fatalf("expected contract to be good, got %s", updated.Usability)
	}
}
