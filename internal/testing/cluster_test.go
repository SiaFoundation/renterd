package testing

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
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

	cluster, err := newTestCluster(t.TempDir(), newTestLoggerCustom(zapcore.DebugLevel))
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
	err = b.AddObject(context.Background(), "foo", object.Object{
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
	expectedEndHeight := currentPeriod + cfg.Contracts.Period + cfg.Contracts.RenewWindow
	if contract.EndHeight() != expectedEndHeight || contract.Revision.EndHeight() != expectedEndHeight {
		t.Fatal("wrong endHeight", contract.EndHeight(), contract.Revision.EndHeight())
	}

	// Mine blocks until contracts start renewing.
	if err := cluster.MineToRenewWindow(); err != nil {
		t.Fatal(err)
	}

	// Wait for the contract to be renewed.
	err = Retry(100, 100*time.Millisecond, func() error {
		contracts, err := cluster.Bus.Contracts(context.Background())
		if err != nil {
			return err
		}
		if len(contracts) != 1 {
			return errors.New("no renewed contract")
		}
		if contracts[0].RenewedFrom != contract.ID {
			return fmt.Errorf("contract wasn't renewed %v != %v", contracts[0].RenewedFrom, contract.ID)
		}
		if contracts[0].ProofHeight != 0 {
			return errors.New("proof height should be 0 since the contract was renewed and therefore doesn't require a proof")
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
	if cs.LastBlockTime.IsZero() {
		t.Fatal("last block time not set")
	}

	// Now wait for the revision and proof to be caught by the hostdb.
	err = Retry(20, time.Second, func() error {
		if err := cluster.MineBlocks(1); err != nil {
			t.Fatal(err)
		}
		// Fetch renewed contract and make sure we caught the proof and revision.
		contracts, err := cluster.Bus.Contracts(context.Background())
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
			js, _ := json.MarshalIndent(hi.ScoreBreakdown, "", "  ")
			t.Fatalf("score shouldn't be 0 because that means one of the fields was 0: %s", string(js))
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
	hostInfos, err := cluster.Autopilot.HostInfos(context.Background(), api.HostFilterModeAll, api.UsabilityFilterModeAll, "", nil, 0, -1)
	if err != nil {
		t.Fatal(err)
	}
	for _, hi := range hostInfos {
		if hi.ScoreBreakdown.Score() == 0 {
			js, _ := json.MarshalIndent(hi.ScoreBreakdown, "", "  ")
			t.Fatalf("score shouldn't be 0 because that means one of the fields was 0: %s", string(js))
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
	hostInfosUsable, err := cluster.Autopilot.HostInfos(context.Background(), api.HostFilterModeAll, api.UsabilityFilterModeUsable, "", nil, 0, -1)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(hostInfos, hostInfosUsable) {
		t.Fatal("result for 'usable' should match the result for ''")
	}
	hostInfosUnusable, err := cluster.Autopilot.HostInfos(context.Background(), api.HostFilterModeAll, api.UsabilityFilterModeUnusable, "", nil, 0, -1)
	if err != nil {
		t.Fatal(err)
	}
	if len(hostInfosUnusable) != 0 {
		t.Fatal("there should be no unusable hosts", len(hostInfosUnusable))
	}
}

// TestUploadDownloadEmpty is an integration test that verifies empty objects
// can be uploaded and download correctly.
func TestUploadDownloadEmpty(t *testing.T) {
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
	if _, err := cluster.AddHostsBlocking(rs.TotalShards); err != nil {
		t.Fatal(err)
	}

	// wait for accounts to be funded
	if _, err := cluster.WaitForAccounts(); err != nil {
		t.Fatal(err)
	}

	// upload an empty file
	if err := w.UploadObject(context.Background(), bytes.NewReader(nil), "empty"); err != nil {
		t.Fatal(err)
	}

	// download the empty file
	var buffer bytes.Buffer
	if err := w.DownloadObject(context.Background(), &buffer, "empty"); err != nil {
		t.Fatal(err)
	}

	// assert it's empty
	if len(buffer.Bytes()) != 0 {
		t.Fatal("unexpected")
	}
}

// TestUploadDownloadBasic is an integration test that verifies objects can be
// uploaded and download correctly.
func TestUploadDownloadBasic(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	// sanity check the default settings
	if testAutopilotConfig.Contracts.Amount < uint64(testRedundancySettings.MinShards) {
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

	b := cluster.Bus
	w := cluster.Worker
	rs := testRedundancySettings

	// add hosts
	if _, err := cluster.AddHostsBlocking(rs.TotalShards); err != nil {
		t.Fatal(err)
	}

	// wait for accounts to be funded
	if _, err := cluster.WaitForAccounts(); err != nil {
		t.Fatal(err)
	}

	// upload two files under /foo
	file1 := make([]byte, rhpv2.SectorSize/12)
	file2 := make([]byte, rhpv2.SectorSize/12)
	frand.Read(file1)
	frand.Read(file2)
	if err := w.UploadObject(context.Background(), bytes.NewReader(file1), "fileś/file1"); err != nil {
		t.Fatal(err)
	}
	if err := w.UploadObject(context.Background(), bytes.NewReader(file2), "fileś/file2"); err != nil {
		t.Fatal(err)
	}

	// fetch all entries from the worker
	entries, err := cluster.Worker.ObjectEntries(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatal("expected one entry to be returned", len(entries))
	}

	// fetch entries with "file" prefix
	_, entries, err = cluster.Bus.Object(context.Background(), "fileś/", "file", 0, -1)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 {
		t.Fatal("expected two entry to be returned", len(entries))
	}

	// fetch entries with "fileś" prefix
	_, entries, err = cluster.Bus.Object(context.Background(), "fileś/", "foo", 0, -1)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatal("expected no entries to be returned", len(entries))
	}

	// fetch entries from the worker for unexisting path
	entries, err = cluster.Worker.ObjectEntries(context.Background(), "bar/")
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatal("expected no entries to be returned", len(entries))
	}

	// prepare two files, a small one and a large one
	small := make([]byte, rhpv2.SectorSize/12)
	large := make([]byte, rhpv2.SectorSize*3)

	// upload the data
	for _, data := range [][]byte{small, large} {
		if _, err := frand.Read(data); err != nil {
			t.Fatal(err)
		}

		name := fmt.Sprintf("data_%v", len(data))
		if err := w.UploadObject(context.Background(), bytes.NewReader(data), name); err != nil {
			t.Fatal(err)
		}
	}

	// check objects stats.
	info, err := cluster.Bus.ObjectsStats()
	if err != nil {
		t.Fatal(err)
	}
	objectsSize := uint64(len(file1) + len(file2) + len(small) + len(large))
	if info.TotalObjectsSize != objectsSize {
		t.Error("wrong size", info.TotalObjectsSize, len(small)+len(large))
	}
	sectorsSize := 15 * rhpv2.SectorSize
	if info.TotalSectorsSize != uint64(sectorsSize) {
		t.Error("wrong size", info.TotalSectorsSize, sectorsSize)
	}
	if info.TotalUploadedSize != uint64(sectorsSize) {
		t.Error("wrong size", info.TotalUploadedSize, sectorsSize)
	}
	if info.NumObjects != 4 {
		t.Error("wrong number of objects", info.NumObjects, 4)
	}

	// download the data
	for _, data := range [][]byte{small, large} {
		name := fmt.Sprintf("data_%v", len(data))

		var buffer bytes.Buffer
		if err := w.DownloadObject(context.Background(), &buffer, name); err != nil {
			t.Fatal(err)
		}

		// assert it matches
		if !bytes.Equal(data, buffer.Bytes()) {
			t.Fatal("unexpected")
		}
	}

	// update the bus setting and specify a non-existing contract set
	err = b.UpdateSetting(context.Background(), api.SettingContractSet, api.ContractSetSettings{Set: t.Name()})
	if err != nil {
		t.Fatal(err)
	}
	err = b.SetContractSet(context.Background(), t.Name(), nil)
	if err != nil {
		t.Fatal(err)
	}

	// assert there are no contracts in the set
	csc, err := b.ContractSetContracts(context.Background(), t.Name())
	if err != nil {
		t.Fatal(err)
	}
	if len(csc) != 0 {
		t.Fatalf("expected no contracts, got %v", len(csc))
	}

	// download the data again
	for _, data := range [][]byte{small, large} {
		name := fmt.Sprintf("data_%v", len(data))

		var buffer bytes.Buffer
		if err := w.DownloadObject(context.Background(), &buffer, name); err != nil {
			t.Fatal(err)
		}

		// assert it matches
		if !bytes.Equal(data, buffer.Bytes()) {
			t.Fatal("unexpected")
		}

		// delete the object
		if err := w.DeleteObject(context.Background(), name); err != nil {
			t.Fatal(err)
		}
	}
}

// TestUploadDownloadSpending is an integration test that verifies the upload
// and download spending metrics are tracked properly.
func TestUploadDownloadSpending(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	// sanity check the default settings
	if testAutopilotConfig.Contracts.Amount < uint64(testRedundancySettings.MinShards) {
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
	if _, err := cluster.AddHostsBlocking(rs.TotalShards); err != nil {
		t.Fatal(err)
	}

	// wait for accounts to be funded
	if _, err := cluster.WaitForAccounts(); err != nil {
		t.Fatal(err)
	}

	// check that the funding was recorded
	err = Retry(100, testBusFlushInterval, func() error {
		cms, err := cluster.Bus.Contracts(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if len(cms) == 0 {
			t.Fatal("no contracts found")
		}

		nFunded := 0
		for _, c := range cms {
			if !c.Spending.Uploads.IsZero() {
				t.Fatal("upload spending should be zero")
			}
			if !c.Spending.Downloads.IsZero() {
				t.Fatal("download spending should be zero")
			}
			if !c.Spending.FundAccount.IsZero() {
				nFunded++
				if c.RevisionNumber == 0 {
					t.Fatal("contract was used for funding but revision wasn't incremented")
				}
			}
		}
		if nFunded < rs.TotalShards {
			return fmt.Errorf("not enough contracts have fund account spending, %v<%v", nFunded, rs.TotalShards)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	// prepare two files, a small one and a large one
	small := make([]byte, rhpv2.SectorSize/12)
	large := make([]byte, rhpv2.SectorSize*3)
	files := [][]byte{small, large}

	uploadDownload := func() {
		t.Helper()
		for _, data := range files {
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
			_, entries, err := cluster.Bus.Object(context.Background(), "", "", 0, -1)
			if err != nil {
				t.Fatal(err)
			}
			var found bool
			for _, entry := range entries {
				if entry.Name == fmt.Sprintf("/%s", name) {
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

	// run uploads once
	uploadDownload()

	// Fuzzy search for uploaded data in various ways.
	objects, err := cluster.Bus.SearchObjects(context.Background(), "", 0, -1)
	if err != nil {
		t.Fatal(err)
	}
	if len(objects) != 2 {
		t.Fatalf("should have 2 objects but got %v", len(objects))
	}
	objects, err = cluster.Bus.SearchObjects(context.Background(), "ata", 0, -1)
	if err != nil {
		t.Fatal(err)
	}
	if len(objects) != 2 {
		t.Fatalf("should have 2 objects but got %v", len(objects))
	}
	objects, err = cluster.Bus.SearchObjects(context.Background(), "1258", 0, -1)
	if err != nil {
		t.Fatal(err)
	}
	if len(objects) != 1 {
		t.Fatalf("should have 1 objects but got %v", len(objects))
	}

	// renew contracts.
	if err := cluster.MineToRenewWindow(); err != nil {
		t.Fatal(err)
	}

	// wait for the contract to be renewed
	err = Retry(100, 100*time.Millisecond, func() error {
		cms, err := cluster.Bus.Contracts(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if len(cms) == 0 {
			t.Fatal("no contracts found")
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

	// run uploads again
	uploadDownload()

	// check that the spending was recorded
	err = Retry(100, testBusFlushInterval, func() error {
		cms, err := cluster.Bus.Contracts(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if len(cms) == 0 {
			t.Fatal("no contracts found")
		}

		for _, c := range cms {
			if c.Spending.Uploads.IsZero() {
				t.Fatal("upload spending shouldn't be zero")
			}
			if !c.Spending.Downloads.IsZero() {
				t.Fatal("download spending should be zero")
			}
			if c.RevisionNumber == 0 {
				t.Fatalf("revision number for contract wasn't recorded: %v", c.RevisionNumber)
			}
			if c.Size == 0 {
				t.Fatalf("size for contract wasn't recorded: %v", c.Size)
			}
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

	// add host
	nodes, err := cluster.AddHosts(1)
	if err != nil {
		t.Fatal(err)
	}
	host := nodes[0]

	// make the cost of fetching a revision 0. That allows us to check for exact
	// balances when funding the account and avoid NDFs.
	settings := host.settings.Settings()
	settings.BaseRPCPrice = types.ZeroCurrency
	settings.EgressPrice = types.ZeroCurrency
	if err := host.settings.UpdateSettings(settings); err != nil {
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
	accounts, err := cluster.WaitForAccounts()
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
	if acc.HostKey != types.PublicKey(host.PublicKey()) {
		t.Fatal("wrong host")
	}

	// Fetch account from bus directly.
	busAccounts, err := cluster.Bus.Accounts(context.Background())
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
	if err := cluster.Bus.SetBalance(context.Background(), busAcc.ID, acc.HostKey, newBalance.Big()); err != nil {
		t.Fatal(err)
	}
	busAccounts, err = cluster.Bus.Accounts(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	busAcc = busAccounts[0]
	if busAcc.Drift.Cmp(newDrift) != 0 {
		t.Fatalf("drift was %v but should be %v", busAcc.Drift, newDrift)
	}

	// Reboot cluster.
	cluster2, err := cluster.Reboot(context.Background())
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
	accounts2, err := cluster2.Bus.Accounts(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(accounts, accounts2) {
		t.Fatal("worker's accounts weren't persisted")
	}

	// Reset drift again.
	if err := cluster2.Bus.ResetDrift(context.Background(), acc.ID); err != nil {
		t.Fatal(err)
	}
	accounts2, err = cluster2.Bus.Accounts(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if accounts2[0].Drift.Cmp(new(big.Int)) != 0 {
		t.Fatal("drift wasn't reset", accounts2[0].Drift.String())
	}
	accounts2, err = cluster2.Bus.Accounts(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if accounts2[0].Drift.Cmp(new(big.Int)) != 0 {
		t.Fatal("drift wasn't reset", accounts2[0].Drift.String())
	}
}

// newTestLogger creates a console logger used for testing.
func newTestLogger() *zap.Logger {
	return newTestLoggerCustom(zapcore.ErrorLevel)
}

// newTestLoggerCustom creates a console logger used for testing and allows
// passing in the desired log level.
func newTestLoggerCustom(level zapcore.Level) *zap.Logger {
	config := zap.NewProductionEncoderConfig()
	config.EncodeTime = zapcore.RFC3339TimeEncoder
	config.EncodeLevel = zapcore.CapitalColorLevelEncoder
	config.StacktraceKey = ""
	consoleEncoder := zapcore.NewConsoleEncoder(config)

	return zap.New(
		zapcore.NewCore(consoleEncoder, zapcore.AddSync(os.Stdout), level),
		zap.AddCaller(),
		zap.AddStacktrace(level),
	)
}

// TestParallelUpload tests uploading multiple files in parallel.
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

	// wait for accounts to be funded
	_, err = cluster.WaitForAccounts()
	if err != nil {
		t.Fatal(err)
	}

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
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := upload(); err != nil {
				t.Error(err)
				return
			}
		}()
	}
	wg.Wait()
}

// TestParallelDownload tests downloading a file in parallel.
func TestParallelDownload(t *testing.T) {
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

	// Wait for accounts to be funded.
	_, err = cluster.WaitForAccounts()
	if err != nil {
		t.Fatal(err)
	}

	// upload the data
	data := frand.Bytes(rhpv2.SectorSize)
	if err := w.UploadObject(context.Background(), bytes.NewReader(data), "foo"); err != nil {
		t.Fatal(err)
	}

	download := func() error {
		t.Helper()
		buf := bytes.NewBuffer(nil)
		err := w.DownloadObject(context.Background(), buf, "foo")
		if err != nil {
			return err
		}
		if !bytes.Equal(buf.Bytes(), data) {
			return errors.New("data mismatch")
		}
		return nil
	}

	// Upload in parallel
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := download(); err != nil {
				t.Error(err)
				return
			}
		}()
	}
	wg.Wait()
}

// TestEphemeralAccountSync verifies that setting the requiresSync flag makes
// the autopilot resync the balance between renter and host.
func TestEphemeralAccountSync(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	dir := t.TempDir()
	cluster, err := newTestCluster(dir, newTestLogger())
	if err != nil {
		t.Fatal(err)
	}

	// add host
	_, err = cluster.AddHosts(1)
	if err != nil {
		t.Fatal(err)
	}

	// Wait for account to appear.
	accounts, err := cluster.WaitForAccounts()
	if err != nil {
		t.Fatal(err)
	}
	if len(accounts) != 1 || accounts[0].RequiresSync {
		t.Fatal("account shouldn't require a sync")
	}

	// Shut down the autopilot to prevent it from manipulating the account.
	if err := cluster.ShutdownAutopilot(context.Background()); err != nil {
		t.Fatal(err)
	}

	// Fetch the account balance before setting the balance
	accounts, err = cluster.Bus.Accounts(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(accounts) != 1 {
		t.Fatal("unexpected number of accounts")
	}
	acc := accounts[0]

	// Set requiresSync flag on bus and balance to 0.
	if err := cluster.Bus.SetBalance(context.Background(), acc.ID, acc.HostKey, new(big.Int)); err != nil {
		t.Fatal(err)
	}
	if err := cluster.Bus.ScheduleSync(context.Background(), acc.ID, acc.HostKey); err != nil {
		t.Fatal(err)
	}
	accounts, err = cluster.Bus.Accounts(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(accounts) != 1 || !accounts[0].RequiresSync {
		t.Fatal("account wasn't updated")
	}

	// Restart cluster to have worker fetch the account from the bus again.
	cluster2, err := cluster.Reboot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := cluster2.Shutdown(context.Background()); err != nil {
			t.Fatal(err)
		}
	}()

	// Account should need a sync.
	account, err := cluster2.Bus.Account(context.Background(), acc.ID, acc.HostKey)
	if err != nil {
		t.Fatal(err)
	}
	if !account.RequiresSync {
		t.Fatal("flag wasn't persisted")
	}

	// Wait for autopilot to sync and reset flag.
	err = Retry(100, 100*time.Millisecond, func() error {
		account, err := cluster2.Bus.Account(context.Background(), acc.ID, acc.HostKey)
		if err != nil {
			t.Fatal(err)
		}
		if account.RequiresSync {
			return errors.New("account wasn't synced")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	// Flag should also be reset on bus now.
	accounts, err = cluster2.Bus.Accounts(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(accounts) != 1 || accounts[0].RequiresSync {
		t.Fatal("account wasn't updated")
	}
}

// TestUploadDownloadSameHost uploads a file to the same host through different
// contracts and tries downloading the file again.
func TestUploadDownloadSameHost(t *testing.T) {
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

	// add host.
	if _, err := cluster.AddHostsBlocking(1); err != nil {
		t.Fatal(err)
	}

	// wait for accounts to be funded
	if _, err := cluster.WaitForAccounts(); err != nil {
		t.Fatal(err)
	}

	// shut down the autopilot to prevent it from doing contract maintenance if any kind
	if err := cluster.ShutdownAutopilot(context.Background()); err != nil {
		t.Fatal(err)
	}

	// get wallet address.
	renterAddress, err := cluster.Bus.WalletAddress(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	ac, err := cluster.Worker.Contracts(context.Background(), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	contracts := ac.Contracts
	if len(contracts) != 1 {
		t.Fatal("expected 1 contract", len(contracts))
	}
	c := contracts[0]

	// Form 2 more contracts with the same host.
	rev2, _, err := cluster.Worker.RHPForm(context.Background(), c.WindowStart, c.HostKey, c.HostIP, renterAddress, c.RenterFunds(), c.Revision.ValidHostPayout())
	if err != nil {
		t.Fatal(err)
	}
	c2, err := cluster.Bus.AddContract(context.Background(), rev2, c.TotalCost, c.StartHeight)
	if err != nil {
		t.Fatal(err)
	}
	rev3, _, err := cluster.Worker.RHPForm(context.Background(), c.WindowStart, c.HostKey, c.HostIP, renterAddress, c.RenterFunds(), c.Revision.ValidHostPayout())
	if err != nil {
		t.Fatal(err)
	}
	c3, err := cluster.Bus.AddContract(context.Background(), rev3, c.TotalCost, c.StartHeight)
	if err != nil {
		t.Fatal(err)
	}

	// Create a contract set with all 3 contracts.
	err = cluster.Bus.SetContractSet(context.Background(), "test", []types.FileContractID{c.ID, c2.ID, c3.ID})
	if err != nil {
		t.Fatal(err)
	}
	err = cluster.Bus.UpdateSetting(context.Background(), api.SettingContractSet, api.ContractSetSettings{
		Set: "test",
	})
	if err != nil {
		t.Fatal(err)
	}

	// Check the bus returns the desired upload params and contract set contracts.
	up, err := cluster.Bus.UploadParams(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if up.ContractSet != "test" {
		t.Fatal("unexpected contractset", up.ContractSet)
	}
	csc, err := cluster.Bus.ContractSetContracts(context.Background(), up.ContractSet)
	if err != nil {
		t.Fatal(err)
	}
	if len(csc) != 3 {
		t.Fatal("expected 3 contracts", len(csc))
	}

	// Upload a file.
	data := frand.Bytes(5*rhpv2.SectorSize + 1)
	err = cluster.Worker.UploadObject(context.Background(), bytes.NewReader(data), "foo")
	if err != nil {
		t.Fatal(err)
	}

	// Download the file multiple times.
	for i := 0; i < 5; i++ {
		buf := &bytes.Buffer{}
		err = cluster.Worker.DownloadObject(context.Background(), buf, "foo")
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(buf.Bytes(), data) {
			t.Fatal("data mismatch")
		}
	}
}

func TestContractArchival(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	// create a test cluster
	cluster, err := newTestCluster(t.TempDir(), zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := cluster.Shutdown(context.Background()); err != nil {
			t.Fatal(err)
		}
	}()

	// add host.
	if _, err := cluster.AddHostsBlocking(1); err != nil {
		t.Fatal(err)
	}

	// check that we have 1 contract
	contracts, err := cluster.Bus.Contracts(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(contracts) != 1 {
		t.Fatal("expected 1 contract", len(contracts))
	}

	// remove the host
	if err := cluster.RemoveHost(cluster.hosts[0]); err != nil {
		t.Fatal(err)
	}

	// mine until the contract is archived
	endHeight := contracts[0].WindowEnd
	cs, err := cluster.Bus.ConsensusState(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := cluster.MineBlocks(int(endHeight - cs.BlockHeight + 1)); err != nil {
		t.Fatal(err)
	}

	// check that we have 0 contracts
	err = Retry(100, 100*time.Millisecond, func() error {
		contracts, err := cluster.Bus.Contracts(context.Background())
		if err != nil {
			return err
		}
		if len(contracts) != 0 {
			return fmt.Errorf("expected 0 contracts, got %v", len(contracts))
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
