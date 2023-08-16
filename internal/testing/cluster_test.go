package testing

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/big"
	"os"
	"reflect"
	"sort"
	"strings"
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

	// Upload packing should be disabled by default.
	ups, err := b.UploadPackingSettings(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if ups.Enabled {
		t.Fatalf("expected upload packing to be disabled by default, got %v", ups.Enabled)
	}

	// Try talking to the bus API by adding an object.
	err = b.AddObject(context.Background(), "foo", testAutopilotConfig.Contracts.Set, object.Object{
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
	err = w.DeleteObject(context.Background(), "foo", false)
	if err != nil {
		t.Fatal(err)
	}

	// See if autopilot is running by triggering the loop.
	_, err = cluster.Autopilot.Trigger(false)
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
	if sets[0] != testAutopilotConfig.Contracts.Set {
		t.Fatal("set name should be 'autopilot' but was", sets[0])
	}

	// Verify startHeight and endHeight of the contract.
	cfg, currentPeriod, err := cluster.AutopilotConfig(context.Background())
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
		if hi.Checks.ScoreBreakdown.Score() == 0 {
			js, _ := json.MarshalIndent(hi.Checks.ScoreBreakdown, "", "  ")
			t.Fatalf("score shouldn't be 0 because that means one of the fields was 0: %s", string(js))
		}
		if hi.Checks.Score == 0 {
			t.Fatal("score shouldn't be 0")
		}
		if !hi.Checks.Usable {
			t.Fatal("host should be usable")
		}
		if len(hi.Checks.UnusableReasons) != 0 {
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
		if hi.Checks.ScoreBreakdown.Score() == 0 {
			js, _ := json.MarshalIndent(hi.Checks.ScoreBreakdown, "", "  ")
			t.Fatalf("score shouldn't be 0 because that means one of the fields was 0: %s", string(js))
		}
		if hi.Checks.Score == 0 {
			t.Fatal("score shouldn't be 0")
		}
		if !hi.Checks.Usable {
			t.Fatal("host should be usable")
		}
		if len(hi.Checks.UnusableReasons) != 0 {
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

	// Fetch the autopilot status
	status, err := cluster.Autopilot.Status()
	if err != nil {
		t.Fatal(err)
	}
	if time.Time(status.MigratingLastStart).IsZero() {
		t.Fatal("autopilot should have completed a migration")
	}
	if time.Time(status.ScanningLastStart).IsZero() {
		t.Fatal("autopilot should have completed a scan")
	}
	if status.UptimeMS == 0 {
		t.Fatal("uptime should be set")
	}
	if !status.Configured {
		t.Fatal("autopilot should be configured")
	}
}

// TestObjectEntries is an integration test that verifies objects are uploaded,
// download and deleted from and to the paths we would expect. It is similar to
// the TestObjectEntries unit test, but uses the worker and bus client to verify
// paths are passed correctly.
func TestObjectEntries(t *testing.T) {
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

	// upload the following paths
	uploads := []struct {
		path string
		size int
	}{
		{"/foo/bar", 1},
		{"/foo/bat", 2},
		{"/foo/baz/quux", 3},
		{"/foo/baz/quuz", 4},
		{"/gab/guub", 5},
		{"/fileś/śpecial", 6}, // utf8
		{"//double/", 7},
		{"///triple", 8},
		{"/FOO/bar", 9}, // test case sensitivity
	}

	for _, upload := range uploads {
		if upload.size == 0 {
			if err := w.UploadObject(context.Background(), bytes.NewReader(nil), upload.path); err != nil {
				t.Fatal(err)
			}
		} else {
			data := make([]byte, upload.size)
			frand.Read(data)
			if err := w.UploadObject(context.Background(), bytes.NewReader(data), upload.path); err != nil {
				t.Fatal(err)
			}
		}
	}

	tests := []struct {
		path   string
		prefix string
		want   []api.ObjectMetadata
	}{
		{"/", "", []api.ObjectMetadata{{Name: "//", Size: 15, Health: 1}, {Name: "/FOO/", Size: 9, Health: 1}, {Name: "/fileś/", Size: 6, Health: 1}, {Name: "/foo/", Size: 10, Health: 1}, {Name: "/gab/", Size: 5, Health: 1}}},
		{"//", "", []api.ObjectMetadata{{Name: "///", Size: 8, Health: 1}, {Name: "//double/", Size: 7, Health: 1}}},
		{"///", "", []api.ObjectMetadata{{Name: "///triple", Size: 8, Health: 1}}},
		{"/foo/", "", []api.ObjectMetadata{{Name: "/foo/bar", Size: 1, Health: 1}, {Name: "/foo/bat", Size: 2, Health: 1}, {Name: "/foo/baz/", Size: 7, Health: 1}}},
		{"/FOO/", "", []api.ObjectMetadata{{Name: "/FOO/bar", Size: 9, Health: 1}}},
		{"/foo/baz/", "", []api.ObjectMetadata{{Name: "/foo/baz/quux", Size: 3, Health: 1}, {Name: "/foo/baz/quuz", Size: 4, Health: 1}}},
		{"/gab/", "", []api.ObjectMetadata{{Name: "/gab/guub", Size: 5, Health: 1}}},
		{"/fileś/", "", []api.ObjectMetadata{{Name: "/fileś/śpecial", Size: 6, Health: 1}}},

		{"/", "f", []api.ObjectMetadata{{Name: "/fileś/", Size: 6, Health: 1}, {Name: "/foo/", Size: 10, Health: 1}}},
		{"/foo/", "fo", []api.ObjectMetadata{}},
		{"/foo/baz/", "quux", []api.ObjectMetadata{{Name: "/foo/baz/quux", Size: 3, Health: 1}}},
		{"/gab/", "/guub", []api.ObjectMetadata{}},
	}
	for _, test := range tests {
		// use the bus client
		_, got, err := b.Object(context.Background(), test.path, api.ObjectsWithPrefix(test.prefix))
		if err != nil {
			t.Fatal(err, test.path)
		}
		if !(len(got) == 0 && len(test.want) == 0) && !reflect.DeepEqual(got, test.want) {
			t.Errorf("\nlist: %v\nprefix: %v\ngot: %v\nwant: %v", test.path, test.prefix, got, test.want)
		}
		for offset := 0; offset < len(test.want); offset++ {
			_, got, err := b.Object(context.Background(), test.path, api.ObjectsWithPrefix(test.prefix), api.ObjectsWithOffset(offset), api.ObjectsWithLimit(1))
			if err != nil {
				t.Fatal(err)
			}
			if len(got) != 1 || got[0] != test.want[offset] {
				t.Errorf("\nlist: %v\nprefix: %v\ngot: %v\nwant: %v", test.path, test.prefix, got, test.want[offset])
			}
		}

		// use the worker client
		got, err = w.ObjectEntries(context.Background(), test.path, test.prefix, 0, -1)
		if err != nil {
			t.Fatal(err)
		}
		if !(len(got) == 0 && len(test.want) == 0) && !reflect.DeepEqual(got, test.want) {
			t.Errorf("\nlist: %v\nprefix: %v\ngot: %v\nwant: %v", test.path, test.prefix, got, test.want)
		}
		for _, entry := range got {
			if !strings.HasSuffix(entry.Name, "/") {
				if err := w.DownloadObject(context.Background(), io.Discard, entry.Name); err != nil {
					t.Fatal(err)
				}
			}
		}
	}

	// delete all uploads
	for _, upload := range uploads {
		err = w.DeleteObject(context.Background(), upload.path, false)
		if err != nil {
			t.Fatal(err)
		}
	}

	// assert root dir is empty
	if entries, err := w.ObjectEntries(context.Background(), "/", "", 0, -1); err != nil {
		t.Fatal(err)
	} else if len(entries) != 0 {
		t.Fatal("there should be no entries left", entries)
	}
}

// TestObjectsRename tests renaming objects and downloading them afterwards.
func TestObjectsRename(t *testing.T) {
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

	// upload the following paths
	uploads := []string{
		"/foo/bar",
		"/foo/bat",
		"/foo/baz",
		"/foo/baz/quuz",
	}
	for _, path := range uploads {
		if err := w.UploadObject(context.Background(), bytes.NewReader(nil), path); err != nil {
			t.Fatal(err)
		}
	}

	// rename
	if err := b.RenameObjects(context.Background(), "/foo/", "/"); err != nil {
		t.Fatal(err)
	}
	if err := b.RenameObject(context.Background(), "/baz/quuz", "/quuz"); err != nil {
		t.Fatal(err)
	}

	// try to download the files
	for _, path := range []string{
		"/bar",
		"/bat",
		"/baz",
		"/quuz",
	} {
		buf := bytes.NewBuffer(nil)
		if err := w.DownloadObject(context.Background(), buf, path); err != nil {
			t.Fatal(err)
		}
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

	// prepare a file
	data := make([]byte, 128)
	if _, err := frand.Read(data); err != nil {
		t.Fatal(err)
	}

	// upload the data
	name := fmt.Sprintf("data_%v", len(data))
	if err := w.UploadObject(context.Background(), bytes.NewReader(data), name); err != nil {
		t.Fatal(err)
	}

	// download data
	var buffer bytes.Buffer
	if err := w.DownloadObject(context.Background(), &buffer, name); err != nil {
		t.Fatal(err)
	}

	// assert it matches
	if !bytes.Equal(data, buffer.Bytes()) {
		t.Fatal("unexpected", len(data), buffer.Len())
	}

	// download again, 32 bytes at a time.
	for i := uint64(0); i < 4; i++ {
		offset := i * 32
		var buffer bytes.Buffer
		if err := w.DownloadObject(context.Background(), &buffer, name, api.DownloadWithRange(offset, 32)); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(data[offset:offset+32], buffer.Bytes()) {
			fmt.Println(data[offset : offset+32])
			fmt.Println(buffer.Bytes())
			t.Fatalf("mismatch for offset %v", offset)
		}
	}

	// fetch the contracts.
	contracts, err := cluster.Bus.Contracts(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	// broadcast the revision for each contract and assert the revision height
	// is 0.
	for _, c := range contracts {
		if c.RevisionHeight != 0 {
			t.Fatal("revision height should be 0")
		}
		if err := w.RHPBroadcast(context.Background(), c.ID); err != nil {
			t.Fatal(err)
		}
	}

	// mine a block to get the revisions mined.
	if err := cluster.MineBlocks(1); err != nil {
		t.Fatal(err)
	}

	// check the revision height was updated.
	err = Retry(100, 100*time.Millisecond, func() error {
		// fetch the contracts.
		contracts, err := cluster.Bus.Contracts(context.Background())
		if err != nil {
			return err
		}
		// assert the revision height was updated.
		for _, c := range contracts {
			if c.RevisionHeight == 0 {
				return errors.New("revision height should be > 0")
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// TestUploadDownloadBasic is an integration test that verifies objects can be
// uploaded and download correctly.
func TestUploadDownloadExtended(t *testing.T) {
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
	entries, err := cluster.Worker.ObjectEntries(context.Background(), "", "", 0, -1)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatal("expected one entry to be returned", len(entries))
	}

	// fetch entries with "file" prefix
	_, entries, err = cluster.Bus.Object(context.Background(), "fileś/", api.ObjectsWithPrefix("file"))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 {
		t.Fatal("expected two entry to be returned", len(entries))
	}

	// fetch entries with "fileś" prefix
	_, entries, err = cluster.Bus.Object(context.Background(), "fileś/", api.ObjectsWithPrefix("foo"))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatal("expected no entries to be returned", len(entries))
	}

	// fetch entries from the worker for unexisting path
	entries, err = cluster.Worker.ObjectEntries(context.Background(), "bar/", "", 0, -1)
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
	cfg, _, err := cluster.AutopilotConfig(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	cfg.Contracts.Set = t.Name()
	err = cluster.UpdateAutopilotConfig(context.Background(), cfg)
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
		if err := w.DeleteObject(context.Background(), name, false); err != nil {
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
			_, entries, err := cluster.Bus.Object(context.Background(), "")
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
		// fetch contracts
		cms, err := cluster.Bus.Contracts(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if len(cms) == 0 {
			t.Fatal("no contracts found")
		}

		// fetch contract set contracts
		contracts, err := cluster.Bus.ContractSetContracts(context.Background(), testAutopilotConfig.Contracts.Set)
		if err != nil {
			t.Fatal(err)
		}
		currentSet := make(map[types.FileContractID]struct{})
		for _, c := range contracts {
			currentSet[c.ID] = struct{}{}
		}

		// assert all contracts are renewed and in the set
		for _, cm := range cms {
			if cm.RenewedFrom == (types.FileContractID{}) {
				return errors.New("found contract that wasn't renewed")
			}
			if _, inset := currentSet[cm.ID]; !inset {
				return errors.New("found renewed contract that wasn't part of the set")
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
	minExpectedBalance := types.Siacoins(1).Sub(types.NewCurrency64(1))
	if acc.Balance.Cmp(minExpectedBalance.Big()) < 0 {
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
	maxNewDrift := newDrift.Add(newDrift, types.NewCurrency64(2).Big()) // forgive 2H
	if busAcc.Drift.Cmp(maxNewDrift) > 0 {
		t.Fatalf("drift was %v but should be %v", busAcc.Drift, maxNewDrift)
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

	// Check that accounts were loaded from the bus.
	accounts2, err := cluster2.Bus.Accounts(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, acc := range accounts2 {
		if acc.Balance.Cmp(big.NewInt(0)) == 0 {
			t.Fatal("account balance wasn't loaded")
		} else if acc.Drift.Cmp(big.NewInt(0)) == 0 {
			t.Fatal("account drift wasn't loaded")
		}
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
		name := fmt.Sprintf("/dir/data_%v", hex.EncodeToString(data[:16]))
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

	// Check if objects exist.
	objects, err := cluster.Bus.SearchObjects(context.Background(), "/dir/", 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(objects) != 3 {
		t.Fatal("wrong number of objects", len(objects))
	}

	// Upload one more object.
	if err := w.UploadObject(context.Background(), bytes.NewReader([]byte("data")), "/foo"); err != nil {
		t.Fatal(err)
	}

	objects, err = cluster.Bus.SearchObjects(context.Background(), "/", 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(objects) != 4 {
		t.Fatal("wrong number of objects", len(objects))
	}

	// Delete all objects under /dir/.
	if err := cluster.Bus.DeleteObject(context.Background(), "/dir/", true); err != nil {
		t.Fatal(err)
	}
	objects, err = cluster.Bus.SearchObjects(context.Background(), "/", 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(objects) != 1 {
		t.Fatal("objects weren't deleted")
	}

	// Delete all objects under /.
	if err := cluster.Bus.DeleteObject(context.Background(), "/", true); err != nil {
		t.Fatal(err)
	}
	objects, err = cluster.Bus.SearchObjects(context.Background(), "/", 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(objects) != 0 {
		t.Fatal("objects weren't deleted")
	}
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

	// Download in parallel
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

	// get wallet address
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

	// form 2 more contracts with the same host
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

	// create a contract set with all 3 contracts
	err = cluster.Bus.SetContractSet(context.Background(), testAutopilotConfig.Contracts.Set, []types.FileContractID{c.ID, c2.ID, c3.ID})
	if err != nil {
		t.Fatal(err)
	}

	// check the bus returns the desired contracts
	up, err := cluster.Bus.UploadParams(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	csc, err := cluster.Bus.ContractSetContracts(context.Background(), up.ContractSet)
	if err != nil {
		t.Fatal(err)
	}
	if len(csc) != 3 {
		t.Fatal("expected 3 contracts", len(csc))
	}

	// upload a file
	data := frand.Bytes(5*rhpv2.SectorSize + 1)
	err = cluster.Worker.UploadObject(context.Background(), bytes.NewReader(data), "foo")
	if err != nil {
		t.Fatal(err)
	}

	// Download the file multiple times.
	var wg sync.WaitGroup
	for tt := 0; tt < 3; tt++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 5; i++ {
				buf := &bytes.Buffer{}
				if err := cluster.Worker.DownloadObject(context.Background(), buf, "foo"); err != nil {
					t.Error(err)
					break
				}
				if !bytes.Equal(buf.Bytes(), data) {
					t.Error("data mismatch")
					break
				}
			}
		}()
	}
	wg.Wait()
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

func TestWalletTransactions(t *testing.T) {
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

	// Make sure we get transactions that are spread out over multiple seconds.
	time.Sleep(time.Second)
	if err := cluster.MineBlocks(1); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second)
	if err := cluster.MineBlocks(1); err != nil {
		t.Fatal(err)
	}

	// Get all transactions of the wallet.
	allTxns, err := b.WalletTransactions(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(allTxns) < 5 {
		t.Fatalf("expected at least 5 transactions, got %v", len(allTxns))
	}
	if !sort.SliceIsSorted(allTxns, func(i, j int) bool {
		return allTxns[i].Timestamp.Unix() > allTxns[j].Timestamp.Unix()
	}) {
		t.Fatal("transactions are not sorted by timestamp")
	}

	// Get the transactions at an offset and compare.
	txns, err := b.WalletTransactions(context.Background(), api.WalletTransactionsWithOffset(2))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(txns, allTxns[2:]) {
		t.Fatal("transactions don't match")
	}

	// Find the first index that has a different timestamp than the first.
	var txnIdx int
	for i := 1; i < len(allTxns); i++ {
		if allTxns[i].Timestamp.Unix() != allTxns[0].Timestamp.Unix() {
			txnIdx = i
			break
		}
	}
	medianTxnTimestamp := allTxns[txnIdx].Timestamp

	// Limit the number of transactions to 5.
	txns, err = b.WalletTransactions(context.Background(), api.WalletTransactionsWithLimit(5))
	if err != nil {
		t.Fatal(err)
	}
	if len(txns) != 5 {
		t.Fatalf("expected exactly 5 transactions, got %v", len(txns))
	}

	// Fetch txns before and since median.
	txns, err = b.WalletTransactions(context.Background(), api.WalletTransactionsWithBefore(medianTxnTimestamp))
	if err != nil {
		t.Fatal(err)
	}
	if len(txns) == 0 {
		for _, txn := range allTxns {
			fmt.Println(txn.Timestamp.Unix())
		}
		t.Fatal("expected at least 1 transaction before median timestamp", medianTxnTimestamp.Unix())
	}
	for _, txn := range txns {
		if txn.Timestamp.Unix() >= medianTxnTimestamp.Unix() {
			t.Fatal("expected only transactions before median timestamp")
		}
	}
	txns, err = b.WalletTransactions(context.Background(), api.WalletTransactionsWithSince(medianTxnTimestamp))
	if err != nil {
		t.Fatal(err)
	}
	if len(txns) == 0 {
		for _, txn := range allTxns {
			fmt.Println(txn.Timestamp.Unix())
		}
		t.Fatal("expected at least 1 transaction after median timestamp")
	}
	for _, txn := range txns {
		if txn.Timestamp.Unix() < medianTxnTimestamp.Unix() {
			t.Fatal("expected only transactions after median timestamp", medianTxnTimestamp.Unix())
		}
	}
}

func TestUploadPacking(t *testing.T) {
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

	// Enable upload packing.
	err = b.UpdateSetting(context.Background(), api.SettingUploadPacking, api.UploadPackingSettings{
		Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}

	// add hosts
	if _, err := cluster.AddHostsBlocking(rs.TotalShards); err != nil {
		t.Fatal(err)
	}

	// wait for accounts to be funded
	if _, err := cluster.WaitForAccounts(); err != nil {
		t.Fatal(err)
	}

	// prepare 3 files which are all smaller than a slab but together make up
	// for 2 full slabs.
	slabSize := rhpv2.SectorSize * rs.MinShards
	totalDataSize := 2 * slabSize
	data1 := make([]byte, (totalDataSize-(slabSize-256))/2)
	data2 := make([]byte, slabSize-256) // large partial slab
	data3 := make([]byte, (totalDataSize-(slabSize-256))/2)
	frand.Read(data1)
	frand.Read(data2)
	frand.Read(data3)

	// declare helpers
	download := func(name string, data []byte, offset, length uint64) {
		t.Helper()
		var buffer bytes.Buffer
		if err := w.DownloadObject(context.Background(), &buffer, name,
			api.DownloadWithRange(offset, length)); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(data[offset:offset+length], buffer.Bytes()) {
			t.Fatal("unexpected", len(data), buffer.Len())
		}
	}
	uploadDownload := func(name string, data []byte) {
		t.Helper()
		if err := w.UploadObject(context.Background(), bytes.NewReader(data), name); err != nil {
			t.Fatal(err)
		}
		download(name, data, 0, uint64(len(data)))
		obj, _, err := b.Object(context.Background(), name)
		if err != nil {
			t.Fatal(err)
		}
		if obj.Size != int64(len(data)) {
			t.Fatal("unexpected size after upload", obj.Size, len(data))
		}
		entries, err := w.ObjectEntries(context.Background(), "/", "", 0, -1)
		if err != nil {
			t.Fatal(err)
		}
		var found bool
		for _, entry := range entries {
			if entry.Name == "/"+name {
				if entry.Size != int64(len(data)) {
					t.Fatal("unexpected size after upload", entry.Size, len(data))
				}
				found = true
				break
			}
		}
		if !found {
			t.Fatal("object not found in list", name, entries)
		}
	}

	// upload file 1 and download it.
	uploadDownload("file1", data1)

	// download it 32 bytes at a time.
	for i := uint64(0); i < 4; i++ {
		download("file1", data1, 32*i, 32)
	}

	// upload file 2 and download it.
	uploadDownload("file2", data2)

	// file 1 should still be available.
	for i := uint64(0); i < 4; i++ {
		download("file1", data1, 32*i, 32)
	}

	// upload file 3 and download it.
	uploadDownload("file3", data3)

	// file 1 should still be available.
	for i := uint64(0); i < 4; i++ {
		download("file1", data1, 32*i, 32)
	}

	// file 2 should still be available. Download each half separately.
	download("file2", data2, 0, uint64(len(data2)))
	download("file2", data2, 0, uint64(len(data2))/2)
	download("file2", data2, uint64(len(data2))/2, uint64(len(data2))/2)

	// download file 3 32 bytes at a time as well.
	for i := uint64(0); i < 4; i++ {
		download("file3", data3, 32*i, 32)
	}

	// upload 2 more files which are half a slab each to test filling up a slab
	// exactly.
	data4 := make([]byte, slabSize/2)
	data5 := make([]byte, slabSize/2)
	uploadDownload("file4", data4)
	uploadDownload("file5", data5)
	download("file4", data4, 0, uint64(len(data4)))

	// check the object stats
	os, err := b.ObjectsStats()
	if err != nil {
		t.Fatal(err)
	}
	if os.NumObjects != 5 {
		t.Fatal("expected 5 objects, got", os.NumObjects)
	}
	totalObjectSize := uint64(3 * slabSize)
	totalRedundantSize := totalObjectSize * uint64(rs.TotalShards) / uint64(rs.MinShards)
	if os.TotalObjectsSize != totalObjectSize {
		t.Fatalf("expected totalObjectSize of %v, got %v", totalObjectSize, os.TotalObjectsSize)
	}
	if os.TotalSectorsSize != uint64(totalRedundantSize) {
		t.Errorf("expected totalSectorSize of %v, got %v", totalRedundantSize, os.TotalSectorsSize)
	}
	if os.TotalUploadedSize != uint64(totalRedundantSize) {
		t.Errorf("expected totalUploadedSize of %v, got %v", totalRedundantSize, os.TotalUploadedSize)
	}
}

func TestWallet(t *testing.T) {
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

	// Check wallet info is sane after startup.
	initialInfo, err := b.Wallet(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if initialInfo.ScanHeight == 0 {
		t.Fatal("wallet scan height should not be 0")
	}
	if initialInfo.Confirmed.IsZero() {
		t.Fatal("wallet confirmed balance should not be zero")
	}
	if !initialInfo.Spendable.Equals(initialInfo.Confirmed) {
		t.Fatal("wallet spendable balance should match confirmed")
	}
	if !initialInfo.Unconfirmed.IsZero() {
		t.Fatal("wallet unconfirmed balance should be zero")
	}
	if initialInfo.Address == (types.Address{}) {
		t.Fatal("wallet address should be set")
	}

	// Send 1 SC to an address outside our wallet. We manually do this to be in
	// control of the miner fees.
	sendAmt := types.HastingsPerSiacoin
	minerFee := types.NewCurrency64(1)
	txn := types.Transaction{
		SiacoinOutputs: []types.SiacoinOutput{
			{Value: sendAmt, Address: types.VoidAddress},
		},
		MinerFees: []types.Currency{minerFee},
	}
	toSign, parents, err := b.WalletFund(context.Background(), &txn, txn.SiacoinOutputs[0].Value)
	if err != nil {
		t.Fatal(err)
	}
	err = b.WalletSign(context.Background(), &txn, toSign, types.CoveredFields{WholeTransaction: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := b.BroadcastTransaction(context.Background(), append(parents, txn)); err != nil {
		t.Fatal(err)
	}

	// The wallet should still have the same confirmed balance, a lower
	// spendable balance and a greater unconfirmed balance.
	var info api.WalletResponse
	err = Retry(600, 100*time.Millisecond, func() error {
		info, err = b.Wallet(context.Background())
		if err != nil {
			return err
		}
		if !info.Confirmed.Equals(initialInfo.Confirmed) {
			return fmt.Errorf("wallet confirmed balance should not have changed: %v %v", info.Confirmed, initialInfo.Confirmed)
		}
		// The diffs of the spendable balance and unconfirmed balance should add up
		// to the amount of money sent as well as the miner fees used.
		spendableDiff := initialInfo.Spendable.Sub(info.Spendable)
		unconfirmedDiff := info.Unconfirmed
		withdrawnAmt := spendableDiff.Sub(unconfirmedDiff)
		expectedWithdrawnAmt := sendAmt.Add(minerFee)
		if !withdrawnAmt.Equals(expectedWithdrawnAmt) {
			return fmt.Errorf("withdrawn amt doesn't match expectation: %v %v", withdrawnAmt.ExactString(), expectedWithdrawnAmt.ExactString())
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestSlabBufferStats(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	// sanity check the default settings
	if testAutopilotConfig.Contracts.Amount < uint64(testRedundancySettings.MinShards) {
		t.Fatal("too few hosts to support the redundancy settings")
	}

	// create a test cluster
	busCfg := testBusCfg()
	threshold := 1 << 12 // 4 KiB
	busCfg.SlabBufferCompletionThreshold = int64(threshold)
	cluster, err := newTestClusterCustom(t.TempDir(), "", true, types.GeneratePrivateKey(), busCfg, testWorkerCfg(), testApCfg(), newTestLogger())
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

	// Enable upload packing.
	err = b.UpdateSetting(context.Background(), api.SettingUploadPacking, api.UploadPackingSettings{
		Enabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}

	// add hosts
	if _, err := cluster.AddHostsBlocking(rs.TotalShards); err != nil {
		t.Fatal(err)
	}

	// wait for accounts to be funded
	if _, err := cluster.WaitForAccounts(); err != nil {
		t.Fatal(err)
	}

	// prepare 3 files which are all smaller than a slab but together make up
	// for 2 full slabs.
	slabSize := rhpv2.SectorSize * rs.MinShards
	data1 := make([]byte, (slabSize - threshold - 1))
	data2 := make([]byte, 1)
	frand.Read(data1)
	frand.Read(data2)

	// upload the first file - buffer should still be incomplete after this
	if err := w.UploadObject(context.Background(), bytes.NewReader(data1), "1"); err != nil {
		t.Fatal(err)
	}

	// check the object stats
	os, err := b.ObjectsStats()
	if err != nil {
		t.Fatal(err)
	}
	if os.NumObjects != 1 {
		t.Fatal("expected 1 object, got", os.NumObjects)
	}
	if os.TotalObjectsSize != uint64(len(data1)) {
		t.Fatalf("expected totalObjectSize of %v, got %v", len(data1), os.TotalObjectsSize)
	}
	if os.TotalSectorsSize != 0 {
		t.Fatal("expected totalSectorSize of 0, got", os.TotalSectorsSize)
	}
	if os.TotalUploadedSize != 0 {
		t.Fatal("expected totalUploadedSize of 0, got", os.TotalUploadedSize)
	}
	buffers, err := b.SlabBuffers()
	if err != nil {
		t.Fatal(err)
	}
	if len(buffers) != 1 {
		t.Fatal("expected 1 slab buffer, got", len(buffers))
	}
	if buffers[0].ContractSet != testContractSet {
		t.Fatalf("expected slab buffer contract set of %v, got %v", testContractSet, buffers[0].ContractSet)
	}
	if buffers[0].Size != int64(len(data1)) {
		t.Fatalf("expected slab buffer size of %v, got %v", len(data1), buffers[0].Size)
	}
	if buffers[0].MaxSize != int64(slabSize) {
		t.Fatalf("expected slab buffer max size of %v, got %v", slabSize, buffers[0].MaxSize)
	}
	if buffers[0].Complete {
		t.Fatal("expected slab buffer to be incomplete")
	}
	if buffers[0].Filename == "" {
		t.Fatal("expected slab buffer to have a filename")
	}
	if buffers[0].Locked {
		t.Fatal("expected slab buffer to be unlocked")
	}

	// upload the second file - this should fill the buffer
	if err := w.UploadObject(context.Background(), bytes.NewReader(data2), "2"); err != nil {
		t.Fatal(err)
	}

	os, err = b.ObjectsStats()
	if err != nil {
		t.Fatal(err)
	}
	if os.NumObjects != 2 {
		t.Fatal("expected 1 object, got", os.NumObjects)
	}
	if os.TotalObjectsSize != uint64(len(data1)+len(data2)) {
		t.Fatalf("expected totalObjectSize of %v, got %v", len(data1)+len(data2), os.TotalObjectsSize)
	}
	if os.TotalSectorsSize != 3*rhpv2.SectorSize {
		t.Fatalf("expected totalSectorSize of %v, got %v", 3*rhpv2.SectorSize, os.TotalSectorsSize)
	}
	if os.TotalUploadedSize != 3*rhpv2.SectorSize {
		t.Fatalf("expected totalUploadedSize of %v, got %v", 3*rhpv2.SectorSize, os.TotalUploadedSize)
	}
	buffers, err = b.SlabBuffers()
	if err != nil {
		t.Fatal(err)
	}
	if len(buffers) != 0 {
		t.Fatal("expected 0 slab buffers, got", len(buffers))
	}
}
