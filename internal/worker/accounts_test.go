package worker

import (
	"context"
	"math/big"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/alerts"
	"go.sia.tech/renterd/api"
	"go.uber.org/zap"
)

type mockAccountMgrBackend struct {
	contracts []api.ContractMetadata
}

func (b *mockAccountMgrBackend) Alerts(context.Context, alerts.AlertsOpts) (alerts.AlertsResponse, error) {
	return alerts.AlertsResponse{}, nil
}

func (b *mockAccountMgrBackend) DismissAlerts(context.Context, ...types.Hash256) error {
	return nil
}

func (b *mockAccountMgrBackend) RegisterAlert(context.Context, alerts.Alert) error {
	return nil
}

func (b *mockAccountMgrBackend) FundAccount(ctx context.Context, fcid types.FileContractID, hk types.PublicKey, siamuxAddr string, balance types.Currency) error {
	return nil
}
func (b *mockAccountMgrBackend) SyncAccount(ctx context.Context, fcid types.FileContractID, hk types.PublicKey, siamuxAddr string) error {
	return nil
}
func (b *mockAccountMgrBackend) Accounts(context.Context, string) ([]api.Account, error) {
	return []api.Account{}, nil
}
func (b *mockAccountMgrBackend) UpdateAccounts(context.Context, string, []api.Account, bool) error {
	return nil
}
func (b *mockAccountMgrBackend) ConsensusState(ctx context.Context) (api.ConsensusState, error) {
	return api.ConsensusState{}, nil
}
func (b *mockAccountMgrBackend) DownloadContracts(ctx context.Context) ([]api.ContractMetadata, error) {
	return nil, nil
}

func TestAccounts(t *testing.T) {
	// create a manager with an account for a single host
	hk := types.PublicKey{1}
	b := &mockAccountMgrBackend{
		contracts: []api.ContractMetadata{
			{
				ID:      types.FileContractID{1},
				HostKey: hk,
			},
		},
	}
	mgr, err := NewAccountManager(types.GeneratePrivateKey(), "test", b, b, b, b, b, time.Second, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}

	// create account
	account := mgr.ForHost(hk)

	// assert account exists
	accounts := mgr.Accounts()
	if len(accounts) != 1 {
		t.Fatalf("expected 1 account but got %v", len(accounts))
	}

	comparer := cmp.Comparer(func(i1, i2 *big.Int) bool {
		return i1.Cmp(i2) == 0
	})

	// Newly created accounts are !cleanShutdown and require a sync. Simulate a
	// sync to change that.
	for _, acc := range accounts {
		if expected := (api.Account{
			CleanShutdown: false,
			RequiresSync:  true,
			ID:            account.ID(),
			HostKey:       hk,
			Balance:       types.ZeroCurrency.Big(),
			Drift:         types.ZeroCurrency.Big(),
			Owner:         "test",
		}); !cmp.Equal(acc, expected, comparer) {
			t.Fatal("account doesn't match expectation", cmp.Diff(acc, expected, comparer))
		}
	}

	// set balance to 0SC to simulate a sync
	account.setBalance(types.ZeroCurrency.Big())

	acc := mgr.Account(hk)
	if expected := (api.Account{
		CleanShutdown: true,
		RequiresSync:  false,
		ID:            account.ID(),
		HostKey:       hk,
		Balance:       types.ZeroCurrency.Big(),
		Drift:         types.ZeroCurrency.Big(),
		Owner:         "test",
	}); !cmp.Equal(acc, expected, comparer) {
		t.Fatal("account doesn't match expectation", cmp.Diff(acc, expected, comparer))
	}

	// fund with 1 SC
	account.addAmount(types.Siacoins(1).Big())

	acc = mgr.Account(hk)
	if expected := (api.Account{
		CleanShutdown: true,
		RequiresSync:  false,
		ID:            account.ID(),
		HostKey:       hk,
		Balance:       types.Siacoins(1).Big(),
		Drift:         types.ZeroCurrency.Big(),
		Owner:         "test",
	}); !cmp.Equal(acc, expected, comparer) {
		t.Fatal("account doesn't match expectation", cmp.Diff(acc, expected, comparer))
	}

	// schedule a sync
	account.ScheduleSync()

	acc = mgr.Account(hk)
	if expected := (api.Account{
		CleanShutdown: true,
		RequiresSync:  true,
		ID:            account.ID(),
		HostKey:       hk,
		Balance:       types.Siacoins(1).Big(),
		Drift:         types.ZeroCurrency.Big(),
		Owner:         "test",
	}); !cmp.Equal(acc, expected, comparer) {
		t.Fatal("account doesn't match expectation", cmp.Diff(acc, expected, comparer))
	}

	// update the balance to create some drift, sync should be reset
	newBalance := types.Siacoins(1).Div64(2).Big()
	newDrift := new(big.Int).Neg(newBalance)
	account.setBalance(newBalance)
	acc = mgr.Account(hk)
	if expected := (api.Account{
		CleanShutdown: true,
		RequiresSync:  false,
		ID:            account.ID(),
		HostKey:       hk,
		Balance:       newBalance,
		Drift:         newDrift,
		Owner:         "test",
	}); !cmp.Equal(acc, expected, comparer) {
		t.Fatal("account doesn't match expectation", cmp.Diff(acc, expected, comparer))
	}
}
