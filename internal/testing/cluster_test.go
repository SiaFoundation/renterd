package testing

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"math/big"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"

	rhpv2 "go.sia.tech/core/rhp/v2"
	rhpv3 "go.sia.tech/core/rhp/v3"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/hostdb"
	"go.sia.tech/renterd/object"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"lukechampine.com/frand"
)

// TestNewTestCluster is a test for creating a cluster of Nodes for testing,
// making sure that it forms contracts, renews contracts and shuts down.
func TestNewTestCluster(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	cluster, err := newTestCluster(t.TempDir(), newTestLogger())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := cluster.Shutdown(context.Background()); err != nil {
			t.Fatal(err)
		}
	}()
	b := cluster.Bus
	w := cluster.Worker

	// Try talking to the bus API by adding an object.
	err = b.AddObject(context.Background(), "/foo", object.Object{
		Key: object.GenerateEncryptionKey(),
		Slabs: []object.SlabSlice{
			{
				Slab: object.Slab{
					Key:       object.GenerateEncryptionKey(),
					MinShards: 1,
					Shards:    []object.Sector{}, // slab without sectors
				},
				Offset: 0,
				Length: 0,
			},
		},
	}, map[types.PublicKey]types.FileContractID{})
	if err != nil {
		t.Fatal(err)
	}

	// Try talking to the worker and request the object.
	err = w.DeleteObject(context.Background(), "/foo")
	if err != nil {
		t.Fatal(err)
	}

	// See if autopilot is running by fetching the config.
	_, err = cluster.Autopilot.Config()
	if err != nil {
		t.Fatal(err)
	}

	// Add a host.
	if _, err := cluster.AddHosts(1); err != nil {
		t.Fatal(err)
	}

	// Wait for contracts to form.
	var contract api.Contract
	if contracts, err := cluster.WaitForContracts(); err != nil {
		t.Fatal(err)
	} else {
		contract = contracts[0]
	}

	// Make sure the contract set exists.
	sets, err := cluster.Bus.ContractSets(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(sets) != 1 {
		t.Fatal("invalid number of setse", len(sets))
	}
	if sets[0] != "autopilot" {
		t.Fatal("set name should be 'autopilot' but was", sets[0])
	}

	// Verify startHeight and endHeight of the contract.
	currentPeriod, err := cluster.Autopilot.Status()
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := cluster.Autopilot.Config()
	if err != nil {
		t.Fatal(err)
	}
	if contract.EndHeight() != currentPeriod+cfg.Contracts.Period+cfg.Contracts.RenewWindow {
		t.Fatal("wrong endHeight")
	}

	// Mine blocks until contracts start renewing.
	if err := cluster.MineToRenewWindow(); err != nil {
		t.Fatal(err)
	}

	// Wait for the contract to be renewed.
	err = Retry(100, 100*time.Millisecond, func() error {
		resp, err := w.ActiveContracts(context.Background(), time.Minute)
		if err != nil {
			return err
		}
		contracts := resp.Contracts
		if len(contracts) != 1 {
			return errors.New("no renewed contract")
		}
		if contracts[0].RenewedFrom != contract.ID {
			return fmt.Errorf("contract wasn't renewed %v != %v", contracts[0].RenewedFrom, contract.ID)
		}
		if contracts[0].EndHeight() != contract.EndHeight()+cfg.Contracts.Period {
			t.Fatal("wrong endHeight")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	// Mine until before the window start to give the host time to submit the
	// revision first.
	cs, err := cluster.Bus.ConsensusState(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := cluster.MineBlocks(int(contract.WindowStart - cs.BlockHeight - 4)); err != nil {
		t.Fatal(err)
	}
	if err := cluster.Sync(); err != nil {
		t.Fatal(err)
	}

	// Now wait for the revision and proof to be caught by the hostdb.
	err = Retry(20, time.Second, func() error {
		if err := cluster.MineBlocks(1); err != nil {
			t.Fatal(err)
		}
		// Fetch renewed contract and make sure we caught the proof and revision.
		contracts, err := cluster.Bus.ActiveContracts(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		archivedContracts, err := cluster.Bus.AncestorContracts(context.Background(), contracts[0].ID, 0)
		if err != nil {
			t.Fatal(err)
		}
		if len(archivedContracts) != 1 {
			return fmt.Errorf("should have 1 archived contract but got %v", len(archivedContracts))
		}
		ac := archivedContracts[0]
		if ac.RevisionHeight == 0 || ac.RevisionNumber != math.MaxUint64 {
			return fmt.Errorf("revision information is wrong: %v %v", ac.RevisionHeight, ac.RevisionNumber)
		}
		if ac.ProofHeight != 0 {
			t.Fatal("proof height should be 0 since the contract was renewed and therefore doesn't require a proof")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	// Get host info for every host.
	hosts, err := cluster.Bus.Hosts(context.Background(), 0, math.MaxInt)
	if err != nil {
		t.Fatal(err)
	}
	for _, host := range hosts {
		hi, err := cluster.Autopilot.HostInfo(host.PublicKey)
		if err != nil {
			t.Fatal(err)
		}
		if hi.ScoreBreakdown.Score() == 0 {
			t.Fatal("score shouldn't be 0 because that means one of the fields was 0")
		}
		if hi.Score == 0 {
			t.Fatal("score shouldn't be 0")
		}
		if !hi.Usable {
			t.Fatal("host should be usable")
		}
		if len(hi.UnusableReasons) != 0 {
			t.Fatal("usable hosts don't have any reasons set")
		}
		if reflect.DeepEqual(hi.Host, hostdb.HostInfo{}) {
			t.Fatal("host wasn't set")
		}
	}
	hostInfos, err := cluster.Autopilot.HostInfos(context.Background(), 0, -1, api.HostFilterModeAll, "", nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, hi := range hostInfos {
		if hi.ScoreBreakdown.Score() == 0 {
			t.Fatal("score shouldn't be 0 because that means one of the fields was 0")
		}
		if hi.Score == 0 {
			t.Fatal("score shouldn't be 0")
		}
		if !hi.Usable {
			t.Fatal("host should be usable")
		}
		if len(hi.UnusableReasons) != 0 {
			t.Fatal("usable hosts don't have any reasons set")
		}
		if reflect.DeepEqual(hi.Host, hostdb.HostInfo{}) {
			t.Fatal("host wasn't set")
		}
	}
}

// TestUploadDownload is an integration test that verifies objects can be
// uploaded and download correctly.
func TestUploadDownload(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	// sanity check the default settings
	if defaultAutopilotConfig.Contracts.Amount < uint64(testRedundancySettings.MinShards) {
		t.Fatal("too few hosts to support the redundancy settings")
	}

	// create a test cluster
	cluster, err := newTestCluster(t.TempDir(), newTestLogger())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := cluster.Shutdown(context.Background()); err != nil {
			t.Fatal(err)
		}
	}()

	w := cluster.Worker
	rs := testRedundancySettings

	// add hosts
	if _, err := cluster.AddHostsBlocking(int(rs.TotalShards)); err != nil {
		t.Fatal(err)
	}

	// prepare two files, a small one and a large one
	small := make([]byte, rhpv2.SectorSize/12)
	large := make([]byte, rhpv2.SectorSize*3)

	uploadDownload := func() {
		t.Helper()
		for _, data := range [][]byte{small, large} {
			// prepare some data - make sure it's more than one sector
			if _, err := frand.Read(data); err != nil {
				t.Fatal(err)
			}

			// upload the data
			name := fmt.Sprintf("data_%v", len(data))
			if err := w.UploadObject(context.Background(), bytes.NewReader(data), name); err != nil {
				t.Fatal(err)
			}

			// Should be registered in bus.
			_, entries, err := cluster.Bus.Object(context.Background(), "")
			if err != nil {
				t.Fatal(err)
			}
			var found bool
			for _, entry := range entries {
				if entry == fmt.Sprintf("/%s", name) {
					found = true
					break
				}
			}
			if !found {
				t.Fatal("uploaded object not found in bus")
			}

			// download the data
			var buffer bytes.Buffer
			if err := w.DownloadObject(context.Background(), &buffer, name); err != nil {
				t.Fatal(err)
			}

			// assert it matches
			if !bytes.Equal(data, buffer.Bytes()) {
				t.Fatal("unexpected")
			}
		}
	}

	// Run uploads once.
	uploadDownload()

	// Fuzzy search for uploaded data in various ways.
	objects, err := cluster.Bus.SearchObjects(context.Background(), 0, -1, "")
	if err != nil {
		t.Fatal("should fail")
	}
	if len(objects) != 2 {
		t.Fatalf("should have 2 objects but got %v", len(objects))
	}
	objects, err = cluster.Bus.SearchObjects(context.Background(), 0, -1, "ata")
	if err != nil {
		t.Fatal(err)
	}
	if len(objects) != 2 {
		t.Fatalf("should have 2 objects but got %v", len(objects))
	}
	objects, err = cluster.Bus.SearchObjects(context.Background(), 0, -1, "12288")
	if err != nil {
		t.Fatal(err)
	}
	if len(objects) != 1 {
		t.Fatalf("should have 1 objects but got %v", len(objects))
	}

	// Renew contracts.
	if err := cluster.MineToRenewWindow(); err != nil {
		t.Fatal(err)
	}

	// Wait for the contract to be renewed.
	err = Retry(100, time.Second, func() error {
		cms, err := cluster.Bus.ActiveContracts(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		for _, cm := range cms {
			if cm.RenewedFrom == (types.FileContractID{}) {
				return errors.New("found contract that wasn't renewed")
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	// Run uploads again.
	uploadDownload()

	// Check that the spending was recorded.
	err = Retry(100, testBusFlushInterval, func() error {
		contracts, err := cluster.Bus.ActiveContracts(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		nUploaded := 0
		nDownloaded := 0
		for _, c := range contracts {
			if !c.Spending.Uploads.IsZero() {
				nUploaded++
			}
			if !c.Spending.Downloads.IsZero() {
				nDownloaded++
			}
		}
		if nUploaded < rs.TotalShards {
			return fmt.Errorf("expected at least %v contracts to contain upload spending", rs.TotalShards)
		}
		if nDownloaded < rs.MinShards {
			return fmt.Errorf("expected at least %v contracts to contain download spending", rs.MinShards)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// TestEphemeralAccounts tests the use of ephemeral accounts.
func TestEphemeralAccounts(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	dir := t.TempDir()
	cluster, err := newTestCluster(dir, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	w := cluster.Worker

	// add host
	nodes, err := cluster.AddHosts(1)
	if err != nil {
		t.Fatal(err)
	}
	host := nodes[0]
	hg, err := host.HostGet()
	if err != nil {
		t.Fatal(err)
	}

	// Wait for contracts to form.
	var contract api.Contract
	if contracts, err := cluster.WaitForContracts(); err != nil {
		t.Fatal(err)
	} else {
		contract = contracts[0]
	}

	// Wait for account to appear.
	var accounts []api.Account
	var ctx context.Context
	err = Retry(100, 100*time.Millisecond, func() error {
		accounts, err = w.Accounts(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if len(accounts) != 1 {
			return fmt.Errorf("wrong number of accounts %v", len(accounts))
		}
		if accounts[0].Balance.Cmp(new(big.Int)) == 0 {
			return errors.New("balance is zero")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	acc := accounts[0]
	if acc.Balance.Cmp(types.Siacoins(1).Big()) != 0 {
		t.Fatalf("wrong balance %v", acc.Balance)
	}
	if acc.ID == (rhpv3.Account{}) {
		t.Fatal("account id not set")
	}
	if acc.Host != types.PublicKey(hg.PublicKey.ToPublicKey()) {
		t.Fatal("wrong host")
	}
	if acc.Owner == "" {
		t.Fatal("owner not set")
	}

	// Fetch account from bus directly.
	busAccounts, err := cluster.Bus.Accounts(context.Background(), "worker")
	if err != nil {
		t.Fatal(err)
	}
	if len(busAccounts) != 1 {
		t.Fatal("expected one account but got", len(busAccounts))
	}
	busAcc := busAccounts[0]
	if !reflect.DeepEqual(busAcc, acc) {
		t.Fatal("bus account doesn't match worker account")
	}

	// Check that the spending was recorded for the contract. The recorded
	// spending should be > the fundAmt since it consists of the fundAmt plus
	// fee.
	time.Sleep(2 * testBusFlushInterval)
	cm, err := cluster.Bus.Contract(context.Background(), contract.ID)
	if err != nil {
		t.Fatal(err)
	}
	fundAmt := types.Siacoins(1)
	if cm.Spending.FundAccount.Cmp(fundAmt) <= 0 {
		t.Fatalf("invalid spending reported: %v > %v", fundAmt.String(), cm.Spending.FundAccount.String())
	}

	// Update the balance to create some drift.
	newBalance := fundAmt.Div64(2)
	newDrift := new(big.Int).Sub(newBalance.Big(), fundAmt.Big())
	if err := cluster.Bus.SetBalance(context.Background(), busAcc.ID, "worker", acc.Host, newBalance.Big(), newDrift); err != nil {
		t.Fatal(err)
	}
	busAccounts, err = cluster.Bus.Accounts(context.Background(), "worker")
	if err != nil {
		t.Fatal(err)
	}
	busAcc = busAccounts[0]
	if busAcc.Drift.Cmp(newDrift) != 0 {
		t.Fatalf("drift was %v but should be %v", busAcc.Drift, newDrift)
	}

	// Shut down cluster.
	if err := cluster.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}

	cluster2, err := newTestClusterWithFunding(dir, cluster.dbName, false, zap.New(zapcore.NewNopCore()))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := cluster2.Shutdown(context.Background()); err != nil {
			t.Fatal(err)
		}
	}()

	// Check that accounts were loaded from the bus correctly.
	// NOTE: since we updated the balance directly on the bus, we need to
	// manually fix the balance and drift before comparing.
	accounts[0].Balance = newBalance.Big()
	accounts[0].Drift = newDrift
	accounts2, err := cluster2.Worker.Accounts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(accounts, accounts2) {
		t.Fatal("worker's accounts weren't persisted")
	}

	// Reset drift again.
	if err := cluster2.Worker.ResetDrift(context.Background(), acc.ID); err != nil {
		t.Fatal(err)
	}
	accounts2, err = cluster2.Worker.Accounts(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if accounts2[0].Drift.Cmp(new(big.Int)) != 0 {
		t.Fatal("drift wasn't reset", accounts2[0].Drift.String())
	}
	accounts2, err = cluster2.Bus.Accounts(context.Background(), "worker")
	if err != nil {
		t.Fatal(err)
	}
	if accounts2[0].Drift.Cmp(new(big.Int)) != 0 {
		t.Fatal("drift wasn't reset", accounts2[0].Drift.String())
	}
}

// newTestLogger creates a console logger used for testing.
func newTestLogger() *zap.Logger {
	config := zap.NewProductionEncoderConfig()
	config.EncodeTime = zapcore.RFC3339TimeEncoder
	config.EncodeLevel = zapcore.CapitalColorLevelEncoder
	config.StacktraceKey = ""
	consoleEncoder := zapcore.NewConsoleEncoder(config)

	return zap.New(
		zapcore.NewCore(consoleEncoder, zapcore.AddSync(os.Stdout), zapcore.ErrorLevel),
		zap.AddCaller(),
		zap.AddStacktrace(zapcore.ErrorLevel),
	)
}

func TestParallelUpload(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	// create a test cluster
	cluster, err := newTestCluster(t.TempDir(), newTestLogger())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := cluster.Shutdown(context.Background()); err != nil {
			t.Fatal(err)
		}
	}()

	w := cluster.Worker
	rs := testRedundancySettings

	// add hosts
	if _, err := cluster.AddHostsBlocking(int(rs.TotalShards)); err != nil {
		t.Fatal(err)
	}

	// prepare two files, a small one and a large one

	upload := func() error {
		t.Helper()
		// prepare some data - make sure it's more than one sector
		data := make([]byte, rhpv2.SectorSize)
		if _, err := frand.Read(data); err != nil {
			return err
		}

		// upload the data
		name := fmt.Sprintf("data_%v", hex.EncodeToString(data[:16]))
		if err := w.UploadObject(context.Background(), bytes.NewReader(data), name); err != nil {
			return err
		}
		return nil
	}

	// Upload in parallel
	var wg sync.WaitGroup
	for i := 0; i < 1; i++ {
		wg.Add(1)
		go func() {
			if err := upload(); err != nil {
				t.Error(err)
				return
			}
			wg.Done()
		}()
	}
	wg.Wait()
}
