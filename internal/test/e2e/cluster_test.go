package e2e

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
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	rhpv2 "go.sia.tech/core/rhp/v2"
	rhpv3 "go.sia.tech/core/rhp/v3"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/alerts"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/hostdb"
	"go.sia.tech/renterd/internal/test"
	"go.sia.tech/renterd/object"
	"go.sia.tech/renterd/wallet"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"lukechampine.com/frand"
)

// TestNewTestCluster is a test for creating a cluster of Nodes for testing,
// making sure that it forms contracts, renews contracts and shuts down.
func TestNewTestCluster(t *testing.T) {
	cluster := newTestCluster(t, clusterOptsDefault)
	defer cluster.Shutdown()
	b := cluster.Bus
	tt := cluster.tt

	// Upload packing should be disabled by default.
	ups, err := b.UploadPackingSettings(context.Background())
	tt.OK(err)
	if ups.Enabled {
		t.Fatalf("expected upload packing to be disabled by default, got %v", ups.Enabled)
	}

	// See if autopilot is running by triggering the loop.
	_, err = cluster.Autopilot.Trigger(false)
	tt.OK(err)

	// Add a host.
	cluster.AddHosts(1)

	// Wait for contracts to form.
	var contract api.Contract
	contracts := cluster.WaitForContracts()
	contract = contracts[0]

	// Verify startHeight and endHeight of the contract.
	cfg, currentPeriod := cluster.AutopilotConfig(context.Background())
	expectedEndHeight := currentPeriod + cfg.Contracts.Period + cfg.Contracts.RenewWindow
	if contract.EndHeight() != expectedEndHeight || contract.Revision.EndHeight() != expectedEndHeight {
		t.Fatal("wrong endHeight", contract.EndHeight(), contract.Revision.EndHeight())
	} else if contract.TotalCost.IsZero() || contract.ContractPrice.IsZero() {
		t.Fatal("TotalCost and ContractPrice shouldn't be zero")
	}

	// Wait for contract set to form
	cluster.WaitForContractSetContracts(cfg.Contracts.Set, int(cfg.Contracts.Amount))

	// Mine blocks until contracts start renewing.
	cluster.MineToRenewWindow()

	// Wait for the contract to be renewed.
	tt.Retry(100, 100*time.Millisecond, func() error {
		contracts, err := cluster.Bus.Contracts(context.Background(), api.ContractsOpts{})
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
		if contracts[0].ContractPrice.IsZero() {
			return errors.New("contract price shouldn't be zero")
		}
		if contracts[0].State != api.ContractStatePending {
			return fmt.Errorf("contract should be pending but was %v", contracts[0].State)
		}
		return nil
	})

	// Mine until before the window start to give the host time to submit the
	// revision first.
	cs, err := cluster.Bus.ConsensusState(context.Background())
	tt.OK(err)
	cluster.MineBlocks(int(contract.WindowStart - cs.BlockHeight - 4))
	cluster.Sync()
	if cs.LastBlockTime.IsZero() {
		t.Fatal("last block time not set")
	}

	// Now wait for the revision and proof to be caught by the hostdb.
	var ac api.ArchivedContract
	tt.Retry(20, time.Second, func() error {
		cluster.MineBlocks(1)

		// Fetch renewed contract and make sure we caught the proof and revision.
		contracts, err := cluster.Bus.Contracts(context.Background(), api.ContractsOpts{})
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
		ac = archivedContracts[0]
		if ac.RevisionHeight == 0 || ac.RevisionNumber != math.MaxUint64 {
			return fmt.Errorf("revision information is wrong: %v %v", ac.RevisionHeight, ac.RevisionNumber)
		}
		if ac.ProofHeight != 0 {
			t.Fatal("proof height should be 0 since the contract was renewed and therefore doesn't require a proof")
		}
		if ac.State != api.ContractStateComplete {
			return fmt.Errorf("contract should be complete but was %v", ac.State)
		}
		archivedContracts, err = cluster.Bus.AncestorContracts(context.Background(), contracts[0].ID, math.MaxUint32)
		if err != nil {
			t.Fatal(err)
		}
		if len(archivedContracts) != 0 {
			return fmt.Errorf("should have 0 archived contracts but got %v", len(archivedContracts))
		}
		return nil
	})

	// Get host info for every host.
	hosts, err := cluster.Bus.Hosts(context.Background(), api.GetHostsOptions{})
	tt.OK(err)
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
		if reflect.DeepEqual(hi.Host, hostdb.Host{}) {
			t.Fatal("host wasn't set")
		}
	}
	hostInfos, err := cluster.Autopilot.HostInfos(context.Background(), api.HostFilterModeAll, api.UsabilityFilterModeAll, "", nil, 0, -1)
	tt.OK(err)

	allHosts := make(map[types.PublicKey]struct{})
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
		if reflect.DeepEqual(hi.Host, hostdb.Host{}) {
			t.Fatal("host wasn't set")
		}
		allHosts[hi.Host.PublicKey] = struct{}{}
	}

	hostInfosUnusable, err := cluster.Autopilot.HostInfos(context.Background(), api.HostFilterModeAll, api.UsabilityFilterModeUnusable, "", nil, 0, -1)
	tt.OK(err)
	if len(hostInfosUnusable) != 0 {
		t.Fatal("there should be no unusable hosts", len(hostInfosUnusable))
	}

	hostInfosUsable, err := cluster.Autopilot.HostInfos(context.Background(), api.HostFilterModeAll, api.UsabilityFilterModeUsable, "", nil, 0, -1)
	tt.OK(err)
	for _, hI := range hostInfosUsable {
		delete(allHosts, hI.Host.PublicKey)
	}
	if len(hostInfosUsable) != len(hostInfos) || len(allHosts) != 0 {
		t.Fatalf("result for 'usable' should match the result for 'all', \n\nall: %+v \n\nusable: %+v", hostInfos, hostInfosUsable)
	}

	// Fetch the autopilot state
	state, err := cluster.Autopilot.State()
	tt.OK(err)
	if time.Time(state.StartTime).IsZero() {
		t.Fatal("autopilot should have start time")
	}
	if time.Time(state.MigratingLastStart).IsZero() {
		t.Fatal("autopilot should have completed a migration")
	}
	if time.Time(state.ScanningLastStart).IsZero() {
		t.Fatal("autopilot should have completed a scan")
	}
	if state.UptimeMS == 0 {
		t.Fatal("uptime should be set")
	}
	if !state.Configured {
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

	// assertMetadata asserts ModTime, ETag and MimeType are set and then clears
	// them afterwards so we can compare without having to specify the metadata
	start := time.Now()
	assertMetadata := func(entries []api.ObjectMetadata) {
		for i := range entries {
			// assert mod time
			if !strings.HasSuffix(entries[i].Name, "/") && !entries[i].ModTime.Std().After(start.UTC()) {
				t.Fatal("mod time should be set")
			}
			entries[i].ModTime = api.TimeRFC3339{}

			// assert mime type
			if entries[i].MimeType == "" {
				t.Fatal("mime type should be set", entries[i].MimeType, entries[i].Name)
			}
			entries[i].MimeType = ""

			// assert etag
			if entries[i].ETag == "" {
				t.Fatal("ETag should be set")
			}
			entries[i].ETag = ""
		}
	}

	// create a test cluster
	cluster := newTestCluster(t, testClusterOptions{
		hosts: test.RedundancySettings.TotalShards,
	})
	defer cluster.Shutdown()

	b := cluster.Bus
	w := cluster.Worker
	tt := cluster.tt

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
			tt.OKAll(w.UploadObject(context.Background(), bytes.NewReader(nil), api.DefaultBucketName, upload.path, api.UploadObjectOptions{}))
		} else {
			data := make([]byte, upload.size)
			frand.Read(data)
			tt.OKAll(w.UploadObject(context.Background(), bytes.NewReader(data), api.DefaultBucketName, upload.path, api.UploadObjectOptions{}))
		}
	}

	tests := []struct {
		path    string
		prefix  string
		sortBy  string
		sortDir string
		want    []api.ObjectMetadata
	}{
		{"/", "", "", "", []api.ObjectMetadata{{Name: "//", Size: 15, Health: 1}, {Name: "/FOO/", Size: 9, Health: 1}, {Name: "/fileś/", Size: 6, Health: 1}, {Name: "/foo/", Size: 10, Health: 1}, {Name: "/gab/", Size: 5, Health: 1}}},
		{"//", "", "", "", []api.ObjectMetadata{{Name: "///", Size: 8, Health: 1}, {Name: "//double/", Size: 7, Health: 1}}},
		{"///", "", "", "", []api.ObjectMetadata{{Name: "///triple", Size: 8, Health: 1}}},
		{"/foo/", "", "", "", []api.ObjectMetadata{{Name: "/foo/bar", Size: 1, Health: 1}, {Name: "/foo/bat", Size: 2, Health: 1}, {Name: "/foo/baz/", Size: 7, Health: 1}}},
		{"/FOO/", "", "", "", []api.ObjectMetadata{{Name: "/FOO/bar", Size: 9, Health: 1}}},
		{"/foo/baz/", "", "", "", []api.ObjectMetadata{{Name: "/foo/baz/quux", Size: 3, Health: 1}, {Name: "/foo/baz/quuz", Size: 4, Health: 1}}},
		{"/gab/", "", "", "", []api.ObjectMetadata{{Name: "/gab/guub", Size: 5, Health: 1}}},
		{"/fileś/", "", "", "", []api.ObjectMetadata{{Name: "/fileś/śpecial", Size: 6, Health: 1}}},

		{"/", "f", "", "", []api.ObjectMetadata{{Name: "/fileś/", Size: 6, Health: 1}, {Name: "/foo/", Size: 10, Health: 1}}},
		{"/foo/", "fo", "", "", []api.ObjectMetadata{}},
		{"/foo/baz/", "quux", "", "", []api.ObjectMetadata{{Name: "/foo/baz/quux", Size: 3, Health: 1}}},
		{"/gab/", "/guub", "", "", []api.ObjectMetadata{}},

		{"/", "", "name", "ASC", []api.ObjectMetadata{{Name: "//", Size: 15, Health: 1}, {Name: "/FOO/", Size: 9, Health: 1}, {Name: "/fileś/", Size: 6, Health: 1}, {Name: "/foo/", Size: 10, Health: 1}, {Name: "/gab/", Size: 5, Health: 1}}},
		{"/", "", "name", "DESC", []api.ObjectMetadata{{Name: "/gab/", Size: 5, Health: 1}, {Name: "/foo/", Size: 10, Health: 1}, {Name: "/fileś/", Size: 6, Health: 1}, {Name: "/FOO/", Size: 9, Health: 1}, {Name: "//", Size: 15, Health: 1}}},

		{"/", "", "health", "ASC", []api.ObjectMetadata{{Name: "//", Size: 15, Health: 1}, {Name: "/FOO/", Size: 9, Health: 1}, {Name: "/fileś/", Size: 6, Health: 1}, {Name: "/foo/", Size: 10, Health: 1}, {Name: "/gab/", Size: 5, Health: 1}}},
		{"/", "", "health", "DESC", []api.ObjectMetadata{{Name: "//", Size: 15, Health: 1}, {Name: "/FOO/", Size: 9, Health: 1}, {Name: "/fileś/", Size: 6, Health: 1}, {Name: "/foo/", Size: 10, Health: 1}, {Name: "/gab/", Size: 5, Health: 1}}},

		{"/", "", "size", "ASC", []api.ObjectMetadata{{Name: "/gab/", Size: 5, Health: 1}, {Name: "/fileś/", Size: 6, Health: 1}, {Name: "/FOO/", Size: 9, Health: 1}, {Name: "/foo/", Size: 10, Health: 1}, {Name: "//", Size: 15, Health: 1}}},
		{"/", "", "size", "DESC", []api.ObjectMetadata{{Name: "//", Size: 15, Health: 1}, {Name: "/foo/", Size: 10, Health: 1}, {Name: "/FOO/", Size: 9, Health: 1}, {Name: "/fileś/", Size: 6, Health: 1}, {Name: "/gab/", Size: 5, Health: 1}}},
	}
	for _, test := range tests {
		// use the bus client
		res, err := b.Object(context.Background(), api.DefaultBucketName, test.path, api.GetObjectOptions{
			Prefix:  test.prefix,
			SortBy:  test.sortBy,
			SortDir: test.sortDir,
		})
		if err != nil {
			t.Fatal(err, test.path)
		}
		assertMetadata(res.Entries)

		if !(len(res.Entries) == 0 && len(test.want) == 0) && !reflect.DeepEqual(res.Entries, test.want) {
			t.Fatalf("\nlist: %v\nprefix: %v\nsortBy: %v\nsortDir: %v\ngot: %v\nwant: %v", test.path, test.prefix, test.sortBy, test.sortDir, res.Entries, test.want)
		}
		for offset := 0; offset < len(test.want); offset++ {
			res, err := b.Object(context.Background(), api.DefaultBucketName, test.path, api.GetObjectOptions{
				Prefix:  test.prefix,
				SortBy:  test.sortBy,
				SortDir: test.sortDir,
				Offset:  offset,
				Limit:   1,
			})
			if err != nil {
				t.Fatal(err)
			}
			assertMetadata(res.Entries)

			if len(res.Entries) != 1 || res.Entries[0] != test.want[offset] {
				t.Fatalf("\nlist: %v\nprefix: %v\nsortBy: %v\nsortDir: %v\ngot: %v\nwant: %v", test.path, test.prefix, test.sortBy, test.sortDir, res.Entries, test.want[offset])
			}
			moreRemaining := len(test.want)-offset-1 > 0
			if res.HasMore != moreRemaining {
				t.Fatalf("invalid value for hasMore (%t) at offset (%d) test (%+v)", res.HasMore, offset, test)
			}

			// make sure we stay within bounds
			if offset+1 >= len(test.want) {
				continue
			}

			res, err = b.Object(context.Background(), api.DefaultBucketName, test.path, api.GetObjectOptions{
				Prefix:  test.prefix,
				SortBy:  test.sortBy,
				SortDir: test.sortDir,
				Marker:  test.want[offset].Name,
				Limit:   1,
			})
			if err != nil {
				t.Fatalf("\nlist: %v\nprefix: %v\nsortBy: %v\nsortDir: %vmarker: %v\n\nerr: %v", test.path, test.prefix, test.sortBy, test.sortDir, test.want[offset].Name, err)
			}
			assertMetadata(res.Entries)

			if len(res.Entries) != 1 || res.Entries[0] != test.want[offset+1] {
				t.Errorf("\nlist: %v\nprefix: %v\nmarker: %v\ngot: %v\nwant: %v", test.path, test.prefix, test.want[offset].Name, res.Entries, test.want[offset+1])
			}

			moreRemaining = len(test.want)-offset-2 > 0
			if res.HasMore != moreRemaining {
				t.Errorf("invalid value for hasMore (%t) at marker (%s) test (%+v)", res.HasMore, test.want[offset].Name, test)
			}
		}

		// use the worker client
		got, err := w.ObjectEntries(context.Background(), api.DefaultBucketName, test.path, api.GetObjectOptions{
			Prefix:  test.prefix,
			SortBy:  test.sortBy,
			SortDir: test.sortDir,
		})
		if err != nil {
			t.Fatal(err)
		}
		assertMetadata(got)

		if !(len(got) == 0 && len(test.want) == 0) && !reflect.DeepEqual(got, test.want) {
			t.Errorf("\nlist: %v\nprefix: %v\ngot: %v\nwant: %v", test.path, test.prefix, got, test.want)
		}
		for _, entry := range got {
			if !strings.HasSuffix(entry.Name, "/") {
				if err := w.DownloadObject(context.Background(), io.Discard, api.DefaultBucketName, entry.Name, api.DownloadObjectOptions{}); err != nil {
					t.Fatal(err)
				}
			}
		}
	}

	// delete all uploads
	for _, upload := range uploads {
		tt.OK(w.DeleteObject(context.Background(), api.DefaultBucketName, upload.path, api.DeleteObjectOptions{}))
	}

	// assert root dir is empty
	if entries, err := w.ObjectEntries(context.Background(), api.DefaultBucketName, "/", api.GetObjectOptions{}); err != nil {
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
	cluster := newTestCluster(t, testClusterOptions{
		hosts: test.RedundancySettings.TotalShards,
	})
	defer cluster.Shutdown()

	b := cluster.Bus
	w := cluster.Worker
	tt := cluster.tt

	// upload the following paths
	uploads := []string{
		"/foo/bar",
		"/foo/bat",
		"/foo/baz",
		"/foo/baz/quuz",
		"/bar2",
	}
	for _, path := range uploads {
		tt.OKAll(w.UploadObject(context.Background(), bytes.NewReader(nil), api.DefaultBucketName, path, api.UploadObjectOptions{}))
	}

	// rename
	if err := b.RenameObjects(context.Background(), api.DefaultBucketName, "/foo/", "/", false); err != nil {
		t.Fatal(err)
	}
	if err := b.RenameObject(context.Background(), api.DefaultBucketName, "/baz/quuz", "/quuz", false); err != nil {
		t.Fatal(err)
	}
	if err := b.RenameObject(context.Background(), api.DefaultBucketName, "/bar2", "/bar", false); err == nil || !strings.Contains(err.Error(), api.ErrObjectExists.Error()) {
		t.Fatal(err)
	} else if err := b.RenameObject(context.Background(), api.DefaultBucketName, "/bar2", "/bar", true); err != nil {
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
		if err := w.DownloadObject(context.Background(), buf, api.DefaultBucketName, path, api.DownloadObjectOptions{}); err != nil {
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
	cluster := newTestCluster(t, testClusterOptions{
		hosts: test.RedundancySettings.TotalShards,
	})
	defer cluster.Shutdown()

	w := cluster.Worker
	tt := cluster.tt

	// upload an empty file
	tt.OKAll(w.UploadObject(context.Background(), bytes.NewReader(nil), api.DefaultBucketName, "empty", api.UploadObjectOptions{}))

	// download the empty file
	var buffer bytes.Buffer
	tt.OK(w.DownloadObject(context.Background(), &buffer, api.DefaultBucketName, "empty", api.DownloadObjectOptions{}))

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
	if test.AutopilotConfig.Contracts.Amount < uint64(test.RedundancySettings.MinShards) {
		t.Fatal("too few hosts to support the redundancy settings")
	}

	// create a test cluster
	cluster := newTestCluster(t, testClusterOptions{
		hosts: test.RedundancySettings.TotalShards,
	})
	defer cluster.Shutdown()

	w := cluster.Worker
	tt := cluster.tt

	// prepare a file
	data := make([]byte, 128)
	tt.OKAll(frand.Read(data))

	// upload the data
	path := fmt.Sprintf("data_%v", len(data))
	tt.OKAll(w.UploadObject(context.Background(), bytes.NewReader(data), api.DefaultBucketName, path, api.UploadObjectOptions{}))

	// fetch object and check its slabs
	resp, err := cluster.Bus.Object(context.Background(), api.DefaultBucketName, path, api.GetObjectOptions{})
	tt.OK(err)
	for _, slab := range resp.Object.Slabs {
		hosts := make(map[types.PublicKey]struct{})
		roots := make(map[types.Hash256]struct{})
		if len(slab.Shards) != test.RedundancySettings.TotalShards {
			t.Fatal("wrong amount of shards", len(slab.Shards), test.RedundancySettings.TotalShards)
		}
		for _, shard := range slab.Shards {
			if shard.LatestHost == (types.PublicKey{}) {
				t.Fatal("latest host should be set")
			} else if len(shard.Contracts) != 1 {
				t.Fatal("each shard should have a host")
			} else if _, found := roots[shard.Root]; found {
				t.Fatal("each root should only exist once per slab")
			}
			for hpk, contracts := range shard.Contracts {
				if len(contracts) != 1 {
					t.Fatal("each host should have one contract")
				} else if _, found := hosts[hpk]; found {
					t.Fatal("each host should only be used once per slab")
				}
				hosts[hpk] = struct{}{}
			}
			roots[shard.Root] = struct{}{}
		}
	}

	// download data
	var buffer bytes.Buffer
	tt.OK(w.DownloadObject(context.Background(), &buffer, api.DefaultBucketName, path, api.DownloadObjectOptions{}))

	// assert it matches
	if !bytes.Equal(data, buffer.Bytes()) {
		t.Fatal("unexpected", len(data), buffer.Len())
	}

	// download again, 32 bytes at a time.
	for i := int64(0); i < 4; i++ {
		offset := i * 32
		var buffer bytes.Buffer
		tt.OK(w.DownloadObject(context.Background(), &buffer, api.DefaultBucketName, path, api.DownloadObjectOptions{Range: api.DownloadRange{Offset: offset, Length: 32}}))
		if !bytes.Equal(data[offset:offset+32], buffer.Bytes()) {
			fmt.Println(data[offset : offset+32])
			fmt.Println(buffer.Bytes())
			t.Fatalf("mismatch for offset %v", offset)
		}
	}

	// fetch the contracts.
	contracts, err := cluster.Bus.Contracts(context.Background(), api.ContractsOpts{})
	tt.OK(err)

	// broadcast the revision for each contract and assert the revision height
	// is 0.
	for _, c := range contracts {
		if c.RevisionHeight != 0 {
			t.Fatal("revision height should be 0")
		}
		tt.OK(w.RHPBroadcast(context.Background(), c.ID))
	}

	// mine a block to get the revisions mined.
	cluster.MineBlocks(1)

	// check the revision height was updated.
	tt.Retry(100, 100*time.Millisecond, func() error {
		// fetch the contracts.
		contracts, err := cluster.Bus.Contracts(context.Background(), api.ContractsOpts{})
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
}

// TestUploadDownloadExtended is an integration test that verifies objects can
// be uploaded and download correctly.
func TestUploadDownloadExtended(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	// sanity check the default settings
	if test.AutopilotConfig.Contracts.Amount < uint64(test.RedundancySettings.MinShards) {
		t.Fatal("too few hosts to support the redundancy settings")
	}

	// create a test cluster
	cluster := newTestCluster(t, testClusterOptions{
		hosts: test.RedundancySettings.TotalShards,
	})
	defer cluster.Shutdown()

	b := cluster.Bus
	w := cluster.Worker
	tt := cluster.tt

	// upload two files under /foo
	file1 := make([]byte, rhpv2.SectorSize/12)
	file2 := make([]byte, rhpv2.SectorSize/12)
	frand.Read(file1)
	frand.Read(file2)
	tt.OKAll(w.UploadObject(context.Background(), bytes.NewReader(file1), api.DefaultBucketName, "fileś/file1", api.UploadObjectOptions{}))
	tt.OKAll(w.UploadObject(context.Background(), bytes.NewReader(file2), api.DefaultBucketName, "fileś/file2", api.UploadObjectOptions{}))

	// fetch all entries from the worker
	entries, err := cluster.Worker.ObjectEntries(context.Background(), api.DefaultBucketName, "", api.GetObjectOptions{})
	tt.OK(err)

	if len(entries) != 1 {
		t.Fatal("expected one entry to be returned", len(entries))
	}
	if entries[0].MimeType != "application/octet-stream" {
		t.Fatal("wrong mime type", entries[0].MimeType)
	}

	// fetch entries with "file" prefix
	res, err := cluster.Bus.Object(context.Background(), api.DefaultBucketName, "fileś/", api.GetObjectOptions{Prefix: "file"})
	tt.OK(err)
	if len(res.Entries) != 2 {
		t.Fatal("expected two entry to be returned", len(entries))
	}

	// fetch entries with "fileś" prefix
	res, err = cluster.Bus.Object(context.Background(), api.DefaultBucketName, "fileś/", api.GetObjectOptions{Prefix: "foo"})
	tt.OK(err)
	if len(res.Entries) != 0 {
		t.Fatal("expected no entries to be returned", len(entries))
	}

	// fetch entries from the worker for unexisting path
	entries, err = cluster.Worker.ObjectEntries(context.Background(), api.DefaultBucketName, "bar/", api.GetObjectOptions{})
	tt.OK(err)
	if len(entries) != 0 {
		t.Fatal("expected no entries to be returned", len(entries))
	}

	// prepare two files, a small one and a large one
	small := make([]byte, rhpv2.SectorSize/12)
	large := make([]byte, rhpv2.SectorSize*3)

	// upload the data
	for _, data := range [][]byte{small, large} {
		tt.OKAll(frand.Read(data))
		path := fmt.Sprintf("data_%v", len(data))
		tt.OKAll(w.UploadObject(context.Background(), bytes.NewReader(data), api.DefaultBucketName, path, api.UploadObjectOptions{}))
	}

	// check objects stats.
	tt.Retry(100, 100*time.Millisecond, func() error {
		for _, opts := range []api.ObjectsStatsOpts{
			{},                              // any bucket
			{Bucket: api.DefaultBucketName}, // specific bucket
		} {
			info, err := cluster.Bus.ObjectsStats(context.Background(), opts)
			tt.OK(err)
			objectsSize := uint64(len(file1) + len(file2) + len(small) + len(large))
			if info.TotalObjectsSize != objectsSize {
				return fmt.Errorf("wrong size %v %v", info.TotalObjectsSize, objectsSize)
			}
			sectorsSize := 15 * rhpv2.SectorSize
			if info.TotalSectorsSize != uint64(sectorsSize) {
				return fmt.Errorf("wrong size %v %v", info.TotalSectorsSize, sectorsSize)
			}
			if info.TotalUploadedSize != uint64(sectorsSize) {
				return fmt.Errorf("wrong size %v %v", info.TotalUploadedSize, sectorsSize)
			}
			if info.NumObjects != 4 {
				return fmt.Errorf("wrong number of objects %v %v", info.NumObjects, 4)
			}
			if info.MinHealth != 1 {
				return fmt.Errorf("expected minHealth of 1, got %v", info.MinHealth)
			}
		}
		return nil
	})

	// download the data
	for _, data := range [][]byte{small, large} {
		name := fmt.Sprintf("data_%v", len(data))
		var buffer bytes.Buffer
		tt.OK(w.DownloadObject(context.Background(), &buffer, api.DefaultBucketName, name, api.DownloadObjectOptions{}))

		// assert it matches
		if !bytes.Equal(data, buffer.Bytes()) {
			t.Fatal("unexpected")
		}
	}

	// update the bus setting and specify a non-existing contract set
	cfg, _ := cluster.AutopilotConfig(context.Background())
	cfg.Contracts.Set = t.Name()
	cluster.UpdateAutopilotConfig(context.Background(), cfg)
	tt.OK(b.SetContractSet(context.Background(), t.Name(), nil))

	// assert there are no contracts in the set
	csc, err := b.Contracts(context.Background(), api.ContractsOpts{ContractSet: t.Name()})
	tt.OK(err)
	if len(csc) != 0 {
		t.Fatalf("expected no contracts, got %v", len(csc))
	}

	// download the data again
	for _, data := range [][]byte{small, large} {
		path := fmt.Sprintf("data_%v", len(data))

		var buffer bytes.Buffer
		tt.OK(w.DownloadObject(context.Background(), &buffer, api.DefaultBucketName, path, api.DownloadObjectOptions{}))

		// assert it matches
		if !bytes.Equal(data, buffer.Bytes()) {
			t.Fatal("unexpected")
		}

		// delete the object
		tt.OK(w.DeleteObject(context.Background(), api.DefaultBucketName, path, api.DeleteObjectOptions{}))
	}
}

// TestUploadDownloadSpending is an integration test that verifies the upload
// and download spending metrics are tracked properly.
func TestUploadDownloadSpending(t *testing.T) {
	// sanity check the default settings
	if test.AutopilotConfig.Contracts.Amount < uint64(test.RedundancySettings.MinShards) {
		t.Fatal("too few hosts to support the redundancy settings")
	}

	// create a test cluster
	cluster := newTestCluster(t, testClusterOptions{
		hosts: test.RedundancySettings.TotalShards,
	})
	defer cluster.Shutdown()

	w := cluster.Worker
	rs := test.RedundancySettings
	tt := cluster.tt

	// check that the funding was recorded
	tt.Retry(100, testBusFlushInterval, func() error {
		cms, err := cluster.Bus.Contracts(context.Background(), api.ContractsOpts{})
		tt.OK(err)
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

	// prepare two files, a small one and a large one
	small := make([]byte, rhpv2.SectorSize/12)
	large := make([]byte, rhpv2.SectorSize*3)
	files := [][]byte{small, large}

	uploadDownload := func() {
		t.Helper()
		for _, data := range files {
			// prepare some data - make sure it's more than one sector
			tt.OKAll(frand.Read(data))

			// upload the data
			path := fmt.Sprintf("data_%v", len(data))
			tt.OKAll(w.UploadObject(context.Background(), bytes.NewReader(data), api.DefaultBucketName, path, api.UploadObjectOptions{}))

			// Should be registered in bus.
			res, err := cluster.Bus.Object(context.Background(), api.DefaultBucketName, "", api.GetObjectOptions{})
			tt.OK(err)

			var found bool
			for _, entry := range res.Entries {
				if entry.Name == fmt.Sprintf("/%s", path) {
					found = true
					break
				}
			}
			if !found {
				t.Fatal("uploaded object not found in bus")
			}

			// download the data
			var buffer bytes.Buffer
			tt.OK(w.DownloadObject(context.Background(), &buffer, api.DefaultBucketName, path, api.DownloadObjectOptions{}))

			// assert it matches
			if !bytes.Equal(data, buffer.Bytes()) {
				t.Fatal("unexpected")
			}
		}
	}

	// run uploads once
	uploadDownload()

	// Fuzzy search for uploaded data in various ways.
	objects, err := cluster.Bus.SearchObjects(context.Background(), api.DefaultBucketName, api.SearchObjectOptions{})
	tt.OK(err)
	if len(objects) != 2 {
		t.Fatalf("should have 2 objects but got %v", len(objects))
	}
	objects, err = cluster.Bus.SearchObjects(context.Background(), api.DefaultBucketName, api.SearchObjectOptions{Key: "ata"})
	tt.OK(err)
	if len(objects) != 2 {
		t.Fatalf("should have 2 objects but got %v", len(objects))
	}
	objects, err = cluster.Bus.SearchObjects(context.Background(), api.DefaultBucketName, api.SearchObjectOptions{Key: "1258"})
	tt.OK(err)
	if len(objects) != 1 {
		t.Fatalf("should have 1 objects but got %v", len(objects))
	}

	// renew contracts.
	cluster.MineToRenewWindow()

	// wait for the contract to be renewed
	tt.Retry(10, time.Second, func() error {
		// mine a block
		cluster.MineBlocks(1)

		// fetch contracts
		cms, err := cluster.Bus.Contracts(context.Background(), api.ContractsOpts{})
		tt.OK(err)
		if len(cms) == 0 {
			t.Fatal("no contracts found")
		}

		// fetch contract set contracts
		contracts, err := cluster.Bus.Contracts(context.Background(), api.ContractsOpts{ContractSet: test.AutopilotConfig.Contracts.Set})
		tt.OK(err)
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

	// run uploads again
	uploadDownload()

	// check that the spending was recorded
	tt.Retry(100, testBusFlushInterval, func() error {
		cms, err := cluster.Bus.Contracts(context.Background(), api.ContractsOpts{})
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
	tt.OK(err)
}

// TestEphemeralAccounts tests the use of ephemeral accounts.
func TestEphemeralAccounts(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	dir := t.TempDir()
	cluster := newTestCluster(t, testClusterOptions{
		dir:    dir,
		logger: zap.NewNop(),
	})
	defer cluster.Shutdown()
	tt := cluster.tt

	// add host
	nodes := cluster.AddHosts(1)
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
	contracts := cluster.WaitForContracts()
	contract = contracts[0]

	// Wait for account to appear.
	accounts := cluster.WaitForAccounts()

	// Shut down the autopilot to prevent it from interfering with the test.
	cluster.ShutdownAutopilot(context.Background())

	// Newly created accounts are !cleanShutdown. Simulate a sync to change
	// that.
	for _, acc := range accounts {
		if acc.CleanShutdown {
			t.Fatal("new account should indicate an unclean shutdown")
		} else if acc.RequiresSync {
			t.Fatal("new account should not require a sync")
		}
		if err := cluster.Bus.SetBalance(context.Background(), acc.ID, acc.HostKey, acc.Balance); err != nil {
			t.Fatal(err)
		}
	}

	// Fetch accounts again.
	accounts, err := cluster.Bus.Accounts(context.Background())
	tt.OK(err)

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
	if !acc.CleanShutdown {
		t.Fatal("account should indicate a clean shutdown")
	}

	// Fetch account from bus directly.
	busAccounts, err := cluster.Bus.Accounts(context.Background())
	tt.OK(err)
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
	fundAmt := types.Siacoins(1)
	tt.Retry(10, testBusFlushInterval, func() error {
		cm, err := cluster.Bus.Contract(context.Background(), contract.ID)
		tt.OK(err)

		if cm.Spending.FundAccount.Cmp(fundAmt) <= 0 {
			return fmt.Errorf("invalid spending reported: %v > %v", fundAmt.String(), cm.Spending.FundAccount.String())
		}
		return nil
	})

	// Update the balance to create some drift.
	newBalance := fundAmt.Div64(2)
	newDrift := new(big.Int).Sub(newBalance.Big(), fundAmt.Big())
	if err := cluster.Bus.SetBalance(context.Background(), busAcc.ID, acc.HostKey, newBalance.Big()); err != nil {
		t.Fatal(err)
	}
	busAccounts, err = cluster.Bus.Accounts(context.Background())
	tt.OK(err)
	busAcc = busAccounts[0]
	maxNewDrift := newDrift.Add(newDrift, types.NewCurrency64(2).Big()) // forgive 2H
	if busAcc.Drift.Cmp(maxNewDrift) > 0 {
		t.Fatalf("drift was %v but should be %v", busAcc.Drift, maxNewDrift)
	}

	// Reboot cluster.
	cluster2 := cluster.Reboot(t)
	defer cluster2.Shutdown()

	// Check that accounts were loaded from the bus.
	accounts2, err := cluster2.Bus.Accounts(context.Background())
	tt.OK(err)
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
	tt.OK(err)
	if accounts2[0].Drift.Cmp(new(big.Int)) != 0 {
		t.Fatal("drift wasn't reset", accounts2[0].Drift.String())
	}
	accounts2, err = cluster2.Bus.Accounts(context.Background())
	tt.OK(err)
	if accounts2[0].Drift.Cmp(new(big.Int)) != 0 {
		t.Fatal("drift wasn't reset", accounts2[0].Drift.String())
	}
}

// TestParallelUpload tests uploading multiple files in parallel.
func TestParallelUpload(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	// create a test cluster
	cluster := newTestCluster(t, testClusterOptions{
		hosts: test.RedundancySettings.TotalShards,
	})
	defer cluster.Shutdown()

	w := cluster.Worker
	tt := cluster.tt

	upload := func() {
		t.Helper()
		// prepare some data - make sure it's more than one sector
		data := make([]byte, rhpv2.SectorSize)
		tt.OKAll(frand.Read(data))

		// upload the data
		path := fmt.Sprintf("/dir/data_%v", hex.EncodeToString(data[:16]))
		tt.OKAll(w.UploadObject(context.Background(), bytes.NewReader(data), api.DefaultBucketName, path, api.UploadObjectOptions{}))
	}

	// Upload in parallel
	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			upload()
		}()
	}
	wg.Wait()

	// Check if objects exist.
	objects, err := cluster.Bus.SearchObjects(context.Background(), api.DefaultBucketName, api.SearchObjectOptions{Key: "/dir/", Limit: 100})
	tt.OK(err)
	if len(objects) != 3 {
		t.Fatal("wrong number of objects", len(objects))
	}

	// Upload one more object.
	tt.OKAll(w.UploadObject(context.Background(), bytes.NewReader([]byte("data")), api.DefaultBucketName, "/foo", api.UploadObjectOptions{}))

	objects, err = cluster.Bus.SearchObjects(context.Background(), api.DefaultBucketName, api.SearchObjectOptions{Key: "/", Limit: 100})
	tt.OK(err)
	if len(objects) != 4 {
		t.Fatal("wrong number of objects", len(objects))
	}

	// Delete all objects under /dir/.
	if err := cluster.Bus.DeleteObject(context.Background(), api.DefaultBucketName, "/dir/", api.DeleteObjectOptions{Batch: true}); err != nil {
		t.Fatal(err)
	}
	objects, err = cluster.Bus.SearchObjects(context.Background(), api.DefaultBucketName, api.SearchObjectOptions{Key: "/", Limit: 100})
	tt.OK(err)
	if len(objects) != 1 {
		t.Fatal("objects weren't deleted")
	}

	// Delete all objects under /.
	if err := cluster.Bus.DeleteObject(context.Background(), api.DefaultBucketName, "/", api.DeleteObjectOptions{Batch: true}); err != nil {
		t.Fatal(err)
	}
	objects, err = cluster.Bus.SearchObjects(context.Background(), api.DefaultBucketName, api.SearchObjectOptions{Key: "/", Limit: 100})
	tt.OK(err)
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
	cluster := newTestCluster(t, testClusterOptions{
		hosts: test.RedundancySettings.TotalShards,
	})
	defer cluster.Shutdown()

	w := cluster.Worker
	tt := cluster.tt

	// upload the data
	data := frand.Bytes(rhpv2.SectorSize)
	tt.OKAll(w.UploadObject(context.Background(), bytes.NewReader(data), api.DefaultBucketName, "foo", api.UploadObjectOptions{}))

	download := func() error {
		t.Helper()
		buf := bytes.NewBuffer(nil)
		err := w.DownloadObject(context.Background(), buf, api.DefaultBucketName, "foo", api.DownloadObjectOptions{})
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
	cluster := newTestCluster(t, testClusterOptions{
		dir:   dir,
		hosts: 1,
	})
	tt := cluster.tt

	// Shut down the autopilot to prevent it from manipulating the account.
	cluster.ShutdownAutopilot(context.Background())

	// Fetch the account balance before setting the balance
	accounts, err := cluster.Bus.Accounts(context.Background())
	tt.OK(err)
	if len(accounts) != 1 || accounts[0].RequiresSync {
		t.Fatal("account shouldn't require a sync")
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
	tt.OK(err)
	if len(accounts) != 1 || !accounts[0].RequiresSync {
		t.Fatal("account wasn't updated")
	}

	// Restart cluster to have worker fetch the account from the bus again.
	cluster2 := cluster.Reboot(t)
	defer cluster2.Shutdown()

	// Account should need a sync.
	account, err := cluster2.Bus.Account(context.Background(), acc.ID, acc.HostKey)
	tt.OK(err)
	if !account.RequiresSync {
		t.Fatal("flag wasn't persisted")
	}

	// Wait for autopilot to sync and reset flag.
	tt.Retry(100, 100*time.Millisecond, func() error {
		account, err := cluster2.Bus.Account(context.Background(), acc.ID, acc.HostKey)
		if err != nil {
			t.Fatal(err)
		}
		if account.RequiresSync {
			return errors.New("account wasn't synced")
		}
		return nil
	})

	// Flag should also be reset on bus now.
	accounts, err = cluster2.Bus.Accounts(context.Background())
	tt.OK(err)
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
	cluster := newTestCluster(t, testClusterOptions{
		hosts: test.RedundancySettings.TotalShards,
	})
	defer cluster.Shutdown()
	tt := cluster.tt
	b := cluster.Bus
	w := cluster.Worker

	// shut down the autopilot to prevent it from doing contract maintenance if any kind
	cluster.ShutdownAutopilot(context.Background())

	// upload 3 objects so every host has 3 sectors
	var err error
	var res api.ObjectsResponse
	shards := make(map[types.PublicKey][]object.Sector)
	for i := 0; i < 3; i++ {
		// upload object
		tt.OKAll(w.UploadObject(context.Background(), bytes.NewReader(frand.Bytes(rhpv2.SectorSize)), api.DefaultBucketName, fmt.Sprintf("foo_%d", i), api.UploadObjectOptions{}))

		// download object from bus and keep track of its shards
		res, err = b.Object(context.Background(), api.DefaultBucketName, fmt.Sprintf("foo_%d", i), api.GetObjectOptions{})
		tt.OK(err)
		for _, shard := range res.Object.Slabs[0].Shards {
			shards[shard.LatestHost] = append(shards[shard.LatestHost], shard)
		}

		// delete the object
		tt.OK(b.DeleteObject(context.Background(), api.DefaultBucketName, fmt.Sprintf("foo_%d", i), api.DeleteObjectOptions{}))
	}

	// build a frankenstein object constructed with all sectors on the same host
	res.Object.Slabs[0].Shards = shards[res.Object.Slabs[0].Shards[0].LatestHost]
	tt.OK(b.AddObject(context.Background(), api.DefaultBucketName, "frankenstein", test.ContractSet, *res.Object.Object, api.AddObjectOptions{}))

	// assert we can download this object
	tt.OK(w.DownloadObject(context.Background(), io.Discard, api.DefaultBucketName, "frankenstein", api.DownloadObjectOptions{}))
}

func TestContractArchival(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	// create a test cluster
	cluster := newTestCluster(t, testClusterOptions{
		hosts:  1,
		logger: zap.NewNop(),
	})
	defer cluster.Shutdown()
	tt := cluster.tt

	// check that we have 1 contract
	contracts, err := cluster.Bus.Contracts(context.Background(), api.ContractsOpts{})
	tt.OK(err)
	if len(contracts) != 1 {
		t.Fatal("expected 1 contract", len(contracts))
	}

	// remove the host
	cluster.RemoveHost(cluster.hosts[0])

	// mine until the contract is archived
	endHeight := contracts[0].WindowEnd
	cs, err := cluster.Bus.ConsensusState(context.Background())
	tt.OK(err)
	cluster.MineBlocks(int(endHeight - cs.BlockHeight + 1))

	// check that we have 0 contracts
	tt.Retry(100, 100*time.Millisecond, func() error {
		contracts, err := cluster.Bus.Contracts(context.Background(), api.ContractsOpts{})
		if err != nil {
			return err
		}
		if len(contracts) != 0 {
			return fmt.Errorf("expected 0 contracts, got %v", len(contracts))
		}
		return nil
	})
}

func TestUnconfirmedContractArchival(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	// create a test cluster
	cluster := newTestCluster(t, testClusterOptions{
		logger: zap.NewNop(),
		hosts:  1,
	})
	defer cluster.Shutdown()
	tt := cluster.tt

	cs, err := cluster.Bus.ConsensusState(context.Background())
	tt.OK(err)

	// we should have a contract with the host
	contracts, err := cluster.Bus.Contracts(context.Background(), api.ContractsOpts{})
	tt.OK(err)
	if len(contracts) != 1 {
		t.Fatalf("expected 1 contract, got %v", len(contracts))
	}
	c := contracts[0]

	// add a contract to the bus
	_, err = cluster.Bus.AddContract(context.Background(), rhpv2.ContractRevision{
		Revision: types.FileContractRevision{
			ParentID: types.FileContractID{1},
			UnlockConditions: types.UnlockConditions{
				PublicKeys: []types.UnlockKey{
					c.HostKey.UnlockKey(),
					c.HostKey.UnlockKey(),
				},
			},
			FileContract: types.FileContract{
				Filesize:       0,
				FileMerkleRoot: types.Hash256{},
				WindowStart:    math.MaxUint32,
				WindowEnd:      math.MaxUint32 + 10,
				Payout:         types.ZeroCurrency,
				UnlockHash:     types.Hash256{},
				RevisionNumber: 0,
			},
		},
	}, types.NewCurrency64(1), types.Siacoins(1), cs.BlockHeight, api.ContractStatePending)
	tt.OK(err)

	// should have 2 contracts now
	contracts, err = cluster.Bus.Contracts(context.Background(), api.ContractsOpts{})
	tt.OK(err)
	if len(contracts) != 2 {
		t.Fatalf("expected 2 contracts, got %v", len(contracts))
	}

	// mine for 20 blocks to make sure we are beyond the 18 block deadline for
	// contract confirmation
	cluster.MineBlocks(20)

	tt.Retry(100, 100*time.Millisecond, func() error {
		contracts, err := cluster.Bus.Contracts(context.Background(), api.ContractsOpts{})
		tt.OK(err)
		if len(contracts) != 1 {
			return fmt.Errorf("expected 1 contract, got %v", len(contracts))
		}
		if contracts[0].ID != c.ID {
			t.Fatalf("expected contract %v, got %v", c.ID, contracts[0].ID)
		}
		return nil
	})
}

func TestWalletTransactions(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	cluster := newTestCluster(t, testClusterOptions{
		logger: newTestLoggerCustom(zapcore.DebugLevel),
	})
	defer cluster.Shutdown()
	b := cluster.Bus
	tt := cluster.tt

	// Make sure we get transactions that are spread out over multiple seconds.
	time.Sleep(time.Second)
	cluster.MineBlocks(1)
	time.Sleep(time.Second)
	cluster.MineBlocks(1)

	// Get all transactions of the wallet.
	allTxns, err := b.WalletTransactions(context.Background())
	tt.OK(err)
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
	tt.OK(err)
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
	tt.OK(err)
	if len(txns) != 5 {
		t.Fatalf("expected exactly 5 transactions, got %v", len(txns))
	}

	// Fetch txns before and since median.
	txns, err = b.WalletTransactions(context.Background(), api.WalletTransactionsWithBefore(medianTxnTimestamp))
	tt.OK(err)
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
	tt.OK(err)
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
	if test.AutopilotConfig.Contracts.Amount < uint64(test.RedundancySettings.MinShards) {
		t.Fatal("too few hosts to support the redundancy settings")
	}

	// create a test cluster
	cluster := newTestCluster(t, testClusterOptions{
		hosts:         test.RedundancySettings.TotalShards,
		uploadPacking: true,
	})
	defer cluster.Shutdown()

	b := cluster.Bus
	w := cluster.Worker
	rs := test.RedundancySettings
	tt := cluster.tt

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
	download := func(path string, data []byte, offset, length int64) {
		t.Helper()
		var buffer bytes.Buffer
		if err := w.DownloadObject(
			context.Background(),
			&buffer,
			api.DefaultBucketName,
			path,
			api.DownloadObjectOptions{Range: api.DownloadRange{Offset: offset, Length: length}},
		); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(data[offset:offset+length], buffer.Bytes()) {
			t.Fatal("unexpected", len(data), buffer.Len())
		}
	}
	uploadDownload := func(name string, data []byte) {
		t.Helper()
		tt.OKAll(w.UploadObject(context.Background(), bytes.NewReader(data), api.DefaultBucketName, name, api.UploadObjectOptions{}))
		download(name, data, 0, int64(len(data)))
		res, err := b.Object(context.Background(), api.DefaultBucketName, name, api.GetObjectOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if res.Object.Size != int64(len(data)) {
			t.Fatal("unexpected size after upload", res.Object.Size, len(data))
		}
		entries, err := w.ObjectEntries(context.Background(), api.DefaultBucketName, "/", api.GetObjectOptions{})
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
	for i := int64(0); i < 4; i++ {
		download("file1", data1, 32*i, 32)
	}

	// upload file 2 and download it.
	uploadDownload("file2", data2)

	// file 1 should still be available.
	for i := int64(0); i < 4; i++ {
		download("file1", data1, 32*i, 32)
	}

	// upload file 3 and download it.
	uploadDownload("file3", data3)

	// file 1 should still be available.
	for i := int64(0); i < 4; i++ {
		download("file1", data1, 32*i, 32)
	}

	// file 2 should still be available. Download each half separately.
	download("file2", data2, 0, int64(len(data2)))
	download("file2", data2, 0, int64(len(data2))/2)
	download("file2", data2, int64(len(data2))/2, int64(len(data2))/2)

	// download file 3 32 bytes at a time as well.
	for i := int64(0); i < 4; i++ {
		download("file3", data3, 32*i, 32)
	}

	// upload 2 more files which are half a slab each to test filling up a slab
	// exactly.
	data4 := make([]byte, slabSize/2)
	data5 := make([]byte, slabSize/2)
	uploadDownload("file4", data4)
	uploadDownload("file5", data5)
	download("file4", data4, 0, int64(len(data4)))

	// assert number of objects
	os, err := b.ObjectsStats(context.Background(), api.ObjectsStatsOpts{})
	tt.OK(err)
	if os.NumObjects != 5 {
		t.Fatalf("expected 5 objects, got %v", os.NumObjects)
	}

	// check the object size stats, we use a retry loop since packed slabs are
	// uploaded in a separate goroutine, so the object stats might lag a bit
	tt.Retry(60, time.Second, func() error {
		os, err := b.ObjectsStats(context.Background(), api.ObjectsStatsOpts{})
		if err != nil {
			t.Fatal(err)
		}

		totalObjectSize := uint64(3 * slabSize)
		totalRedundantSize := totalObjectSize * uint64(rs.TotalShards) / uint64(rs.MinShards)
		if os.TotalObjectsSize != totalObjectSize {
			return fmt.Errorf("expected totalObjectSize of %v, got %v", totalObjectSize, os.TotalObjectsSize)
		}
		if os.TotalSectorsSize != uint64(totalRedundantSize) {
			return fmt.Errorf("expected totalSectorSize of %v, got %v", totalRedundantSize, os.TotalSectorsSize)
		}
		if os.TotalUploadedSize != uint64(totalRedundantSize) {
			return fmt.Errorf("expected totalUploadedSize of %v, got %v", totalRedundantSize, os.TotalUploadedSize)
		}
		if os.MinHealth != 1 {
			return fmt.Errorf("expected minHealth of 1, got %v", os.MinHealth)
		}
		return nil
	})

	// ObjectsBySlabKey should return 2 objects for the slab of file1 since file1
	// and file2 share the same slab.
	res, err := b.Object(context.Background(), api.DefaultBucketName, "file1", api.GetObjectOptions{})
	tt.OK(err)
	objs, err := b.ObjectsBySlabKey(context.Background(), api.DefaultBucketName, res.Object.Slabs[0].Key)
	tt.OK(err)
	if len(objs) != 2 {
		t.Fatal("expected 2 objects", len(objs))
	}
	sort.Slice(objs, func(i, j int) bool {
		return objs[i].Name < objs[j].Name // make result deterministic
	})
	if objs[0].Name != "/file1" {
		t.Fatal("expected file1", objs[0].Name)
	} else if objs[1].Name != "/file2" {
		t.Fatal("expected file2", objs[1].Name)
	}
}

func TestWallet(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	cluster := newTestCluster(t, testClusterOptions{
		logger: newTestLoggerCustom(zapcore.DebugLevel),
	})
	defer cluster.Shutdown()
	b := cluster.Bus
	tt := cluster.tt

	// Check wallet info is sane after startup.
	wallet, err := b.Wallet(context.Background())
	tt.OK(err)
	if wallet.ScanHeight == 0 {
		t.Fatal("wallet scan height should not be 0")
	}
	if wallet.Confirmed.IsZero() {
		t.Fatal("wallet confirmed balance should not be zero")
	}
	if !wallet.Spendable.Equals(wallet.Confirmed) {
		t.Fatal("wallet spendable balance should match confirmed")
	}
	if !wallet.Unconfirmed.IsZero() {
		t.Fatal("wallet unconfirmed balance should be zero")
	}
	if wallet.Address == (types.Address{}) {
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
	toSign, parents, err := b.WalletFund(context.Background(), &txn, txn.SiacoinOutputs[0].Value, false)
	tt.OK(err)
	err = b.WalletSign(context.Background(), &txn, toSign, types.CoveredFields{WholeTransaction: true})
	tt.OK(err)
	tt.OK(b.BroadcastTransaction(context.Background(), append(parents, txn)))

	// The wallet should still have the same confirmed balance, a lower
	// spendable balance and a greater unconfirmed balance.
	tt.Retry(600, 100*time.Millisecond, func() error {
		updated, err := b.Wallet(context.Background())
		tt.OK(err)
		if !updated.Confirmed.Equals(wallet.Confirmed) {
			return fmt.Errorf("wallet confirmed balance should not have changed: %v %v", updated.Confirmed, wallet.Confirmed)
		}

		// The diffs of the spendable balance and unconfirmed balance should add up
		// to the amount of money sent as well as the miner fees used.
		spendableDiff := wallet.Spendable.Sub(updated.Spendable)
		if updated.Unconfirmed.Cmp(spendableDiff) > 0 {
			t.Fatalf("unconfirmed balance can't be greater than the difference in spendable balance here: \nconfirmed %v (%v) - >%v (%v) \nunconfirmed %v (%v) -> %v (%v) \nspendable %v (%v) -> %v (%v) \nfee %v (%v)",
				wallet.Confirmed, wallet.Confirmed.ExactString(), updated.Confirmed, updated.Confirmed.ExactString(),
				wallet.Unconfirmed, wallet.Unconfirmed.ExactString(), updated.Unconfirmed, updated.Unconfirmed.ExactString(),
				wallet.Spendable, wallet.Spendable.ExactString(), updated.Spendable, updated.Spendable.ExactString(),
				minerFee, minerFee.ExactString())
		}
		withdrawnAmt := spendableDiff.Sub(updated.Unconfirmed)
		expectedWithdrawnAmt := sendAmt.Add(minerFee)
		if !withdrawnAmt.Equals(expectedWithdrawnAmt) {
			return fmt.Errorf("withdrawn amount doesn't match expectation: %v!=%v", withdrawnAmt.ExactString(), expectedWithdrawnAmt.ExactString())
		}
		return nil
	})
}

func TestSlabBufferStats(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	// sanity check the default settings
	if test.AutopilotConfig.Contracts.Amount < uint64(test.RedundancySettings.MinShards) {
		t.Fatal("too few hosts to support the redundancy settings")
	}

	// create a test cluster
	busCfg := testBusCfg()
	threshold := 1 << 12 // 4 KiB
	busCfg.SlabBufferCompletionThreshold = int64(threshold)
	cluster := newTestCluster(t, testClusterOptions{
		busCfg:        &busCfg,
		hosts:         test.RedundancySettings.TotalShards,
		uploadPacking: true,
	})
	defer cluster.Shutdown()

	b := cluster.Bus
	w := cluster.Worker
	rs := test.RedundancySettings
	tt := cluster.tt

	// prepare 3 files which are all smaller than a slab but together make up
	// for 2 full slabs.
	slabSize := rhpv2.SectorSize * rs.MinShards
	data1 := make([]byte, (slabSize - threshold - 1))
	data2 := make([]byte, 1)
	frand.Read(data1)
	frand.Read(data2)

	// upload the first file - buffer should still be incomplete after this
	tt.OKAll(w.UploadObject(context.Background(), bytes.NewReader(data1), api.DefaultBucketName, "1", api.UploadObjectOptions{}))

	// assert number of objects
	os, err := b.ObjectsStats(context.Background(), api.ObjectsStatsOpts{})
	tt.OK(err)
	if os.NumObjects != 1 {
		t.Fatalf("expected 1 object, got %d", os.NumObjects)
	}

	// check the object size stats, we use a retry loop since packed slabs are
	// uploaded in a separate goroutine, so the object stats might lag a bit
	tt.Retry(60, time.Second, func() error {
		os, err := b.ObjectsStats(context.Background(), api.ObjectsStatsOpts{})
		if err != nil {
			t.Fatal(err)
		}
		if os.TotalObjectsSize != uint64(len(data1)) {
			return fmt.Errorf("expected totalObjectSize of %d, got %d", len(data1), os.TotalObjectsSize)
		}
		if os.TotalSectorsSize != 0 {
			return fmt.Errorf("expected totalSectorSize of 0, got %d", os.TotalSectorsSize)
		}
		if os.TotalUploadedSize != 0 {
			return fmt.Errorf("expected totalUploadedSize of 0, got %d", os.TotalUploadedSize)
		}
		if os.MinHealth != 1 {
			t.Errorf("expected minHealth of 1, got %v", os.MinHealth)
		}
		return nil
	})

	// check the slab buffers
	buffers, err := b.SlabBuffers()
	tt.OK(err)
	if len(buffers) != 1 {
		t.Fatal("expected 1 slab buffer, got", len(buffers))
	}
	if buffers[0].ContractSet != test.ContractSet {
		t.Fatalf("expected slab buffer contract set of %v, got %v", test.ContractSet, buffers[0].ContractSet)
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
	tt.OKAll(w.UploadObject(context.Background(), bytes.NewReader(data2), api.DefaultBucketName, "2", api.UploadObjectOptions{}))

	// assert number of objects
	os, err = b.ObjectsStats(context.Background(), api.ObjectsStatsOpts{})
	tt.OK(err)
	if os.NumObjects != 2 {
		t.Fatalf("expected 1 object, got %d", os.NumObjects)
	}

	// check the object size stats, we use a retry loop since packed slabs are
	// uploaded in a separate goroutine, so the object stats might lag a bit
	tt.Retry(60, time.Second, func() error {
		os, err := b.ObjectsStats(context.Background(), api.ObjectsStatsOpts{})
		tt.OK(err)
		if os.TotalObjectsSize != uint64(len(data1)+len(data2)) {
			return fmt.Errorf("expected totalObjectSize of %d, got %d", len(data1)+len(data2), os.TotalObjectsSize)
		}
		if os.TotalSectorsSize != 3*rhpv2.SectorSize {
			return fmt.Errorf("expected totalSectorSize of %d, got %d", 3*rhpv2.SectorSize, os.TotalSectorsSize)
		}
		if os.TotalUploadedSize != 3*rhpv2.SectorSize {
			return fmt.Errorf("expected totalUploadedSize of %d, got %d", 3*rhpv2.SectorSize, os.TotalUploadedSize)
		}
		if os.MinHealth != 1 {
			t.Errorf("expected minHealth of 1, got %v", os.MinHealth)
		}
		return nil
	})

	// check the slab buffers, again a retry loop to avoid NDFs
	tt.Retry(100, 100*time.Millisecond, func() error {
		buffers, err = b.SlabBuffers()
		tt.OK(err)
		if len(buffers) != 0 {
			return fmt.Errorf("expected 0 slab buffers, got %d", len(buffers))
		}
		return nil
	})
}

func TestAlerts(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	cluster := newTestCluster(t, testClusterOptions{
		logger: newTestLoggerCustom(zapcore.DebugLevel),
	})
	defer cluster.Shutdown()
	b := cluster.Bus
	tt := cluster.tt

	// register alert
	alert := alerts.Alert{
		ID:       frand.Entropy256(),
		Severity: alerts.SeverityCritical,
		Message:  "test",
		Data: map[string]interface{}{
			"origin": "test",
		},
		Timestamp: time.Now(),
	}
	tt.OK(b.RegisterAlert(context.Background(), alert))
	findAlert := func(id types.Hash256) *alerts.Alert {
		t.Helper()
		ar, err := b.Alerts(context.Background(), alerts.AlertsOpts{})
		tt.OK(err)
		for _, alert := range ar.Alerts {
			if alert.ID == id {
				return &alert
			}
		}
		return nil
	}
	foundAlert := findAlert(alert.ID)
	if foundAlert == nil {
		t.Fatal("alert not found")
	}
	if !cmp.Equal(*foundAlert, alert) {
		t.Fatal("alert mismatch", cmp.Diff(*foundAlert, alert))
	}

	// dismiss alert
	tt.OK(b.DismissAlerts(context.Background(), alert.ID))
	foundAlert = findAlert(alert.ID)
	if foundAlert != nil {
		t.Fatal("alert found")
	}

	// register 2 alerts
	alert2 := alert
	alert2.ID = frand.Entropy256()
	alert2.Timestamp = time.Now().Add(time.Second)
	tt.OK(b.RegisterAlert(context.Background(), alert))
	tt.OK(b.RegisterAlert(context.Background(), alert2))
	if foundAlert := findAlert(alert.ID); foundAlert == nil {
		t.Fatal("alert not found")
	} else if foundAlert := findAlert(alert2.ID); foundAlert == nil {
		t.Fatal("alert not found")
	}

	// try to find with offset = 1
	ar, err := b.Alerts(context.Background(), alerts.AlertsOpts{Offset: 1})
	foundAlerts := ar.Alerts
	tt.OK(err)
	if len(foundAlerts) != 1 || foundAlerts[0].ID != alert.ID {
		t.Fatal("wrong alert")
	}

	// try to find with limit = 1
	ar, err = b.Alerts(context.Background(), alerts.AlertsOpts{Limit: 1})
	foundAlerts = ar.Alerts
	tt.OK(err)
	if len(foundAlerts) != 1 || foundAlerts[0].ID != alert2.ID {
		t.Fatal("wrong alert")
	}

	// register more alerts
	for severity := alerts.SeverityInfo; severity <= alerts.SeverityCritical; severity++ {
		for j := 0; j < 3*int(severity); j++ {
			tt.OK(b.RegisterAlert(context.Background(), alerts.Alert{
				ID:       frand.Entropy256(),
				Severity: severity,
				Message:  "test",
				Data: map[string]interface{}{
					"origin": "test",
				},
				Timestamp: time.Now(),
			}))
		}
	}
	for severity := alerts.SeverityInfo; severity <= alerts.SeverityCritical; severity++ {
		ar, err = b.Alerts(context.Background(), alerts.AlertsOpts{Severity: severity})
		tt.OK(err)
		if ar.Total() != 32 {
			t.Fatal("expected 32 alerts", ar.Total())
		} else if ar.Totals.Info != 3 {
			t.Fatal("expected 3 info alerts", ar.Totals.Info)
		} else if ar.Totals.Warning != 6 {
			t.Fatal("expected 6 warning alerts", ar.Totals.Warning)
		} else if ar.Totals.Error != 9 {
			t.Fatal("expected 9 error alerts", ar.Totals.Error)
		} else if ar.Totals.Critical != 14 {
			t.Fatal("expected 14 critical alerts", ar.Totals.Critical)
		} else if severity == alerts.SeverityInfo && len(ar.Alerts) != ar.Totals.Info {
			t.Fatalf("expected %v info alerts, got %v", ar.Totals.Info, len(ar.Alerts))
		} else if severity == alerts.SeverityWarning && len(ar.Alerts) != ar.Totals.Warning {
			t.Fatalf("expected %v warning alerts, got %v", ar.Totals.Warning, len(ar.Alerts))
		} else if severity == alerts.SeverityError && len(ar.Alerts) != ar.Totals.Error {
			t.Fatalf("expected %v error alerts, got %v", ar.Totals.Error, len(ar.Alerts))
		} else if severity == alerts.SeverityCritical && len(ar.Alerts) != ar.Totals.Critical {
			t.Fatalf("expected %v critical alerts, got %v", ar.Totals.Critical, len(ar.Alerts))
		}
	}
}

func TestMultipartUploads(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	cluster := newTestCluster(t, testClusterOptions{
		hosts:         test.RedundancySettings.TotalShards,
		uploadPacking: true,
	})
	defer cluster.Shutdown()
	defer cluster.Shutdown()
	b := cluster.Bus
	w := cluster.Worker
	tt := cluster.tt

	// Start a new multipart upload.
	objPath := "/foo"
	mpr, err := b.CreateMultipartUpload(context.Background(), api.DefaultBucketName, objPath, api.CreateMultipartOptions{GenerateKey: true})
	tt.OK(err)
	if mpr.UploadID == "" {
		t.Fatal("expected non-empty upload ID")
	}

	// List uploads
	lmu, err := b.MultipartUploads(context.Background(), api.DefaultBucketName, "/f", "", "", 0)
	tt.OK(err)
	if len(lmu.Uploads) != 1 {
		t.Fatal("expected 1 upload got", len(lmu.Uploads))
	} else if upload := lmu.Uploads[0]; upload.UploadID != mpr.UploadID || upload.Path != objPath {
		t.Fatal("unexpected upload:", upload)
	}

	// Add 3 parts out of order to make sure the object is reconstructed
	// correctly.
	putPart := func(partNum int, offset int, data []byte) string {
		t.Helper()
		res, err := w.UploadMultipartUploadPart(context.Background(), bytes.NewReader(data), api.DefaultBucketName, objPath, mpr.UploadID, partNum, api.UploadMultipartUploadPartOptions{EncryptionOffset: &offset})
		tt.OK(err)
		if res.ETag == "" {
			t.Fatal("expected non-empty ETag")
		}
		return res.ETag
	}

	data1 := frand.Bytes(64)
	data2 := frand.Bytes(128)
	data3 := frand.Bytes(64)
	etag2 := putPart(2, len(data1), data2)
	etag1 := putPart(1, 0, data1)
	etag3 := putPart(3, len(data1)+len(data2), data3)
	size := int64(len(data1) + len(data2) + len(data3))
	expectedData := data1
	expectedData = append(expectedData, data2...)
	expectedData = append(expectedData, data3...)

	// List parts
	mup, err := b.MultipartUploadParts(context.Background(), api.DefaultBucketName, objPath, mpr.UploadID, 0, 0)
	tt.OK(err)
	if len(mup.Parts) != 3 {
		t.Fatal("expected 3 parts got", len(mup.Parts))
	} else if part1 := mup.Parts[0]; part1.PartNumber != 1 || part1.Size != int64(len(data1)) || part1.ETag == "" {
		t.Fatal("unexpected part:", part1)
	} else if part2 := mup.Parts[1]; part2.PartNumber != 2 || part2.Size != int64(len(data2)) || part2.ETag == "" {
		t.Fatal("unexpected part:", part2)
	} else if part3 := mup.Parts[2]; part3.PartNumber != 3 || part3.Size != int64(len(data3)) || part3.ETag == "" {
		t.Fatal("unexpected part:", part3)
	}

	// Check objects stats.
	os, err := b.ObjectsStats(context.Background(), api.ObjectsStatsOpts{})
	tt.OK(err)
	if os.NumObjects != 0 {
		t.Fatalf("expected 0 object, got %v", os.NumObjects)
	} else if os.TotalObjectsSize != 0 {
		t.Fatalf("expected object size of 0, got %v", os.TotalObjectsSize)
	} else if os.NumUnfinishedObjects != 1 {
		t.Fatalf("expected 1 unfinished object, got %v", os.NumUnfinishedObjects)
	} else if os.TotalUnfinishedObjectsSize != uint64(size) {
		t.Fatalf("expected unfinished object size of %v, got %v", size, os.TotalUnfinishedObjectsSize)
	}

	// Complete upload
	ui, err := b.CompleteMultipartUpload(context.Background(), api.DefaultBucketName, objPath, mpr.UploadID, []api.MultipartCompletedPart{
		{
			PartNumber: 1,
			ETag:       etag1,
		},
		{
			PartNumber: 2,
			ETag:       etag2,
		},
		{
			PartNumber: 3,
			ETag:       etag3,
		},
	}, api.CompleteMultipartOptions{})
	tt.OK(err)
	if ui.ETag == "" {
		t.Fatal("unexpected response:", ui)
	}

	// Download object
	gor, err := w.GetObject(context.Background(), api.DefaultBucketName, objPath, api.DownloadObjectOptions{})
	tt.OK(err)
	if gor.Range != nil {
		t.Fatal("unexpected range:", gor.Range)
	} else if gor.Size != size {
		t.Fatal("unexpected size:", gor.Size)
	} else if data, err := io.ReadAll(gor.Content); err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(data, expectedData) {
		t.Fatal("unexpected data:", cmp.Diff(data, expectedData))
	}

	// Download a range of the object
	gor, err = w.GetObject(context.Background(), api.DefaultBucketName, objPath, api.DownloadObjectOptions{Range: api.DownloadRange{Offset: 0, Length: 1}})
	tt.OK(err)
	if gor.Range == nil || gor.Range.Offset != 0 || gor.Range.Length != 1 {
		t.Fatal("unexpected range:", gor.Range)
	} else if gor.Size != size {
		t.Fatal("unexpected size:", gor.Size)
	} else if data, err := io.ReadAll(gor.Content); err != nil {
		t.Fatal(err)
	} else if expectedData := data1[:1]; !bytes.Equal(data, expectedData) {
		t.Fatal("unexpected data:", cmp.Diff(data, expectedData))
	}

	// Check objects stats.
	os, err = b.ObjectsStats(context.Background(), api.ObjectsStatsOpts{})
	tt.OK(err)
	if os.NumObjects != 1 {
		t.Fatalf("expected 1 object, got %v", os.NumObjects)
	} else if os.TotalObjectsSize != uint64(size) {
		t.Fatalf("expected object size of %v, got %v", size, os.TotalObjectsSize)
	} else if os.NumUnfinishedObjects != 0 {
		t.Fatalf("expected 0 unfinished object, got %v", os.NumUnfinishedObjects)
	} else if os.TotalUnfinishedObjectsSize != 0 {
		t.Fatalf("expected unfinished object size of 0, got %v", os.TotalUnfinishedObjectsSize)
	}
}

func TestWalletSendUnconfirmed(t *testing.T) {
	cluster := newTestCluster(t, clusterOptsDefault)
	defer cluster.Shutdown()
	b := cluster.Bus
	tt := cluster.tt

	wr, err := b.Wallet(context.Background())
	tt.OK(err)

	// check balance
	if !wr.Unconfirmed.IsZero() {
		t.Fatal("wallet should not have unconfirmed balance")
	} else if wr.Confirmed.IsZero() {
		t.Fatal("wallet should have confirmed balance")
	}

	// send the full balance back to the weallet
	toSend := wr.Confirmed.Sub(types.Siacoins(1).Div64(100)) // leave some for the fee
	tt.OK(b.SendSiacoins(context.Background(), []types.SiacoinOutput{
		{
			Address: wr.Address,
			Value:   toSend,
		},
	}, false))

	// the unconfirmed balance should have changed to slightly more than toSend
	// since we paid a fee
	wr, err = b.Wallet(context.Background())
	tt.OK(err)

	if wr.Unconfirmed.Cmp(toSend) < 0 || wr.Unconfirmed.Add(types.Siacoins(1)).Cmp(toSend) < 0 {
		t.Fatal("wallet should have unconfirmed balance")
	}
	fmt.Println(wr.Confirmed, wr.Unconfirmed)

	// try again - this should fail
	err = b.SendSiacoins(context.Background(), []types.SiacoinOutput{
		{
			Address: wr.Address,
			Value:   toSend,
		},
	}, false)
	tt.AssertIs(err, wallet.ErrInsufficientBalance)

	// try again - this time using unconfirmed transactions
	tt.OK(b.SendSiacoins(context.Background(), []types.SiacoinOutput{
		{
			Address: wr.Address,
			Value:   toSend,
		},
	}, true))

	// the unconfirmed balance should be almost the same
	wr, err = b.Wallet(context.Background())
	tt.OK(err)

	if wr.Unconfirmed.Cmp(toSend) < 0 || wr.Unconfirmed.Add(types.Siacoins(1)).Cmp(toSend) < 0 {
		t.Fatal("wallet should have unconfirmed balance")
	}
	fmt.Println(wr.Confirmed, wr.Unconfirmed)

	// mine a block, this should confirm the transactions
	cluster.MineBlocks(1)
	tt.Retry(100, time.Millisecond, func() error {
		wr, err = b.Wallet(context.Background())
		tt.OK(err)

		if !wr.Unconfirmed.IsZero() {
			return fmt.Errorf("wallet should not have unconfirmed balance")
		} else if wr.Confirmed.Cmp(toSend) < 0 || wr.Confirmed.Add(types.Siacoins(1)).Cmp(toSend) < 0 {
			return fmt.Errorf("wallet should have almost the same confirmed balance as in the beginning")
		}
		return nil
	})
}

func TestWalletFormUnconfirmed(t *testing.T) {
	// New cluster with autopilot disabled
	cfg := clusterOptsDefault
	cfg.skipSettingAutopilot = true
	cluster := newTestCluster(t, cfg)
	defer cluster.Shutdown()
	b := cluster.Bus
	tt := cluster.tt

	// Add a host.
	cluster.AddHosts(1)

	// Send the full balance back to the wallet to make sure it's all
	// unconfirmed.
	wr, err := b.Wallet(context.Background())
	tt.OK(err)
	tt.OK(b.SendSiacoins(context.Background(), []types.SiacoinOutput{
		{
			Address: wr.Address,
			Value:   wr.Confirmed.Sub(types.Siacoins(1).Div64(100)), // leave some for the fee
		},
	}, false))

	// There should be hardly any money in the wallet.
	wr, err = b.Wallet(context.Background())
	tt.OK(err)
	if wr.Confirmed.Sub(wr.Unconfirmed).Cmp(types.Siacoins(1).Div64(100)) > 0 {
		t.Fatal("wallet should have hardly any confirmed balance")
	}

	// There shouldn't be any contracts at this point.
	contracts, err := b.Contracts(context.Background(), api.ContractsOpts{})
	tt.OK(err)
	if len(contracts) != 0 {
		t.Fatal("expected 0 contracts", len(contracts))
	}

	// Enable autopilot by setting it.
	cluster.UpdateAutopilotConfig(context.Background(), test.AutopilotConfig)

	// Wait for a contract to form.
	contractsFormed := cluster.WaitForContracts()
	if len(contractsFormed) != 1 {
		t.Fatal("expected 1 contract", len(contracts))
	}
}

func TestBusRecordedMetrics(t *testing.T) {
	startTime := time.Now().UTC().Round(time.Second)

	cluster := newTestCluster(t, testClusterOptions{
		hosts: 1,
	})
	defer cluster.Shutdown()

	// Get contract set metrics.
	csMetrics, err := cluster.Bus.ContractSetMetrics(context.Background(), startTime, api.MetricMaxIntervals, time.Second, api.ContractSetMetricsQueryOpts{})
	cluster.tt.OK(err)

	for i := 0; i < len(csMetrics); i++ {
		// Remove metrics from before contract was formed.
		if csMetrics[i].Contracts > 0 {
			csMetrics = csMetrics[i:]
			break
		}
	}
	if len(csMetrics) == 0 {
		t.Fatal("expected at least 1 metric with contracts")
	}
	for _, m := range csMetrics {
		if m.Contracts != 1 {
			t.Fatalf("expected 1 contract, got %v", m.Contracts)
		} else if m.Name != test.ContractSet {
			t.Fatalf("expected contract set %v, got %v", test.ContractSet, m.Name)
		} else if m.Timestamp.Std().Before(startTime) {
			t.Fatalf("expected time to be after start time %v, got %v", startTime, m.Timestamp.Std())
		}
	}

	// Get churn metrics. Should have 1 for the new contract.
	cscMetrics, err := cluster.Bus.ContractSetChurnMetrics(context.Background(), startTime, api.MetricMaxIntervals, time.Second, api.ContractSetChurnMetricsQueryOpts{})
	cluster.tt.OK(err)

	if len(cscMetrics) != 1 {
		t.Fatalf("expected 1 metric, got %v", len(cscMetrics))
	} else if m := cscMetrics[0]; m.Direction != api.ChurnDirAdded {
		t.Fatalf("expected added churn, got %v", m.Direction)
	} else if m.ContractID == (types.FileContractID{}) {
		t.Fatal("expected non-zero FCID")
	} else if m.Name != test.ContractSet {
		t.Fatalf("expected contract set %v, got %v", test.ContractSet, m.Name)
	} else if m.Timestamp.Std().Before(startTime) {
		t.Fatalf("expected time to be after start time %v, got %v", startTime, m.Timestamp.Std())
	}

	// Get contract metrics.
	var cMetrics []api.ContractMetric
	cluster.tt.Retry(100, 100*time.Millisecond, func() error {
		// Retry fetching metrics since they are buffered.
		cMetrics, err = cluster.Bus.ContractMetrics(context.Background(), startTime, api.MetricMaxIntervals, time.Second, api.ContractMetricsQueryOpts{})
		cluster.tt.OK(err)
		if len(cMetrics) != 1 {
			return fmt.Errorf("expected 1 metric, got %v", len(cMetrics))
		}
		return nil
	})

	if len(cMetrics) != 1 {
		t.Fatalf("expected 1 metric, got %v", len(cMetrics))
	} else if m := cMetrics[0]; m.Timestamp.Std().Before(startTime) {
		t.Fatalf("expected time to be after start time %v, got %v", startTime, m.Timestamp.Std())
	} else if m.ContractID != (types.FileContractID{}) {
		t.Fatal("expected zero FCID")
	} else if m.HostKey != (types.PublicKey{}) {
		t.Fatal("expected zero Host")
	} else if m.RemainingCollateral == (types.Currency{}) {
		t.Fatal("expected non-zero RemainingCollateral")
	} else if m.RemainingFunds == (types.Currency{}) {
		t.Fatal("expected non-zero RemainingFunds")
	} else if m.RevisionNumber != 0 {
		t.Fatal("expected zero RevisionNumber")
	} else if !m.UploadSpending.IsZero() {
		t.Fatal("expected zero UploadSpending")
	} else if !m.DownloadSpending.IsZero() {
		t.Fatal("expected zero DownloadSpending")
	} else if m.FundAccountSpending == (types.Currency{}) {
		t.Fatal("expected non-zero FundAccountSpending")
	} else if !m.DeleteSpending.IsZero() {
		t.Fatal("expected zero DeleteSpending")
	} else if !m.ListSpending.IsZero() {
		t.Fatal("expected zero ListSpending")
	}

	// Prune one of the metrics
	if err := cluster.Bus.PruneMetrics(context.Background(), api.MetricContract, time.Now()); err != nil {
		t.Fatal(err)
	} else if cMetrics, err = cluster.Bus.ContractMetrics(context.Background(), startTime, api.MetricMaxIntervals, time.Second, api.ContractMetricsQueryOpts{}); err != nil {
		t.Fatal(err)
	} else if len(cMetrics) > 0 {
		t.Fatalf("expected 0 metrics, got %v", len(cscMetrics))
	}
}

func TestMultipartUploadWrappedByPartialSlabs(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	cluster := newTestCluster(t, testClusterOptions{
		hosts:         test.RedundancySettings.TotalShards,
		uploadPacking: true,
	})
	defer cluster.Shutdown()

	b := cluster.Bus
	w := cluster.Worker
	slabSize := test.RedundancySettings.SlabSizeNoRedundancy()
	tt := cluster.tt

	// start a new multipart upload. We upload the parts in reverse order
	objPath := "/foo"
	key := object.GenerateEncryptionKey()
	mpr, err := b.CreateMultipartUpload(context.Background(), api.DefaultBucketName, objPath, api.CreateMultipartOptions{Key: &key})
	tt.OK(err)
	if mpr.UploadID == "" {
		t.Fatal("expected non-empty upload ID")
	}

	// upload a part that is a partial slab
	part3Data := bytes.Repeat([]byte{3}, int(slabSize)/4)
	offset := int(slabSize + slabSize/4)
	resp3, err := w.UploadMultipartUploadPart(context.Background(), bytes.NewReader(part3Data), api.DefaultBucketName, objPath, mpr.UploadID, 3, api.UploadMultipartUploadPartOptions{
		EncryptionOffset: &offset,
	})
	tt.OK(err)

	// upload a part that is exactly a full slab
	part2Data := bytes.Repeat([]byte{2}, int(slabSize))
	offset = int(slabSize / 4)
	resp2, err := w.UploadMultipartUploadPart(context.Background(), bytes.NewReader(part2Data), api.DefaultBucketName, objPath, mpr.UploadID, 2, api.UploadMultipartUploadPartOptions{
		EncryptionOffset: &offset,
	})
	tt.OK(err)

	// upload another part the same size as the first one
	part1Data := bytes.Repeat([]byte{1}, int(slabSize)/4)
	offset = 0
	resp1, err := w.UploadMultipartUploadPart(context.Background(), bytes.NewReader(part1Data), api.DefaultBucketName, objPath, mpr.UploadID, 1, api.UploadMultipartUploadPartOptions{
		EncryptionOffset: &offset,
	})
	tt.OK(err)

	// combine all parts data
	expectedData := part1Data
	expectedData = append(expectedData, part2Data...)
	expectedData = append(expectedData, part3Data...)

	// finish the upload
	tt.OKAll(b.CompleteMultipartUpload(context.Background(), api.DefaultBucketName, objPath, mpr.UploadID, []api.MultipartCompletedPart{
		{
			PartNumber: 1,
			ETag:       resp1.ETag,
		},
		{
			PartNumber: 2,
			ETag:       resp2.ETag,
		},
		{
			PartNumber: 3,
			ETag:       resp3.ETag,
		},
	}, api.CompleteMultipartOptions{}))

	// download the object and verify its integrity
	dst := new(bytes.Buffer)
	tt.OK(w.DownloadObject(context.Background(), dst, api.DefaultBucketName, objPath, api.DownloadObjectOptions{}))
	receivedData := dst.Bytes()
	if len(receivedData) != len(expectedData) {
		t.Fatalf("expected %v bytes, got %v", len(expectedData), len(receivedData))
	} else if !bytes.Equal(receivedData, expectedData) {
		t.Fatal("unexpected data")
	}
}

func TestWalletRedistribute(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	cluster := newTestCluster(t, testClusterOptions{
		hosts:         test.RedundancySettings.TotalShards,
		uploadPacking: true,
	})
	defer cluster.Shutdown()

	// redistribute into 5 outputs
	_, err := cluster.Bus.WalletRedistribute(context.Background(), 5, types.Siacoins(10))
	if err != nil {
		t.Fatal(err)
	}
	cluster.MineBlocks(1)

	// assert we have 5 outputs with 10 SC
	outputs, err := cluster.Bus.WalletOutputs(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	var cnt int
	for _, output := range outputs {
		if output.Value.Cmp(types.Siacoins(10)) == 0 {
			cnt++
		}
	}
	if cnt != 5 {
		t.Fatalf("expected 5 outputs with 10 SC, got %v", cnt)
	}

	// assert redistributing into 3 outputs succeeds, used to fail because we
	// were broadcasting an empty transaction set
	_, err = cluster.Bus.WalletRedistribute(context.Background(), 3, types.Siacoins(10))
	if err != nil {
		t.Fatal(err)
	}
}

func TestHostScan(t *testing.T) {
	// New cluster with autopilot disabled
	cfg := clusterOptsDefault
	cfg.skipRunningAutopilot = true
	cluster := newTestCluster(t, cfg)
	defer cluster.Shutdown()

	b := cluster.Bus
	w := cluster.Worker
	tt := cluster.tt

	// add 2 hosts to the cluster, 1 to scan and 1 to make sure we always have 1
	// peer and consider ourselves connected to the internet
	hosts := cluster.AddHosts(2)
	host := hosts[0]

	settings, err := host.RHPv2Settings()
	tt.OK(err)

	hk := host.PublicKey()
	hostIP := settings.NetAddress

	assertHost := func(ls time.Time, lss, slss bool, ts uint64) {
		t.Helper()

		hi, err := b.Host(context.Background(), host.PublicKey())
		tt.OK(err)

		if ls.IsZero() && !hi.Interactions.LastScan.IsZero() {
			t.Fatal("expected last scan to be zero")
		} else if !ls.IsZero() && !hi.Interactions.LastScan.After(ls) {
			t.Fatal("expected last scan to be after", ls)
		} else if hi.Interactions.LastScanSuccess != lss {
			t.Fatalf("expected last scan success to be %v, got %v", lss, hi.Interactions.LastScanSuccess)
		} else if hi.Interactions.SecondToLastScanSuccess != slss {
			t.Fatalf("expected second to last scan success to be %v, got %v", slss, hi.Interactions.SecondToLastScanSuccess)
		} else if hi.Interactions.TotalScans != ts {
			t.Fatalf("expected total scans to be %v, got %v", ts, hi.Interactions.TotalScans)
		}
	}

	scanHost := func() error {
		// timing on the CI can be weird, wait a bit to make sure time passes
		// between scans
		time.Sleep(time.Millisecond)

		resp, err := w.RHPScan(context.Background(), hk, hostIP, 10*time.Second)
		tt.OK(err)
		if resp.ScanError != "" {
			return errors.New(resp.ScanError)
		}
		return nil
	}

	assertHost(time.Time{}, false, false, 0)

	// scan the host the first time
	ls := time.Now()
	if err := scanHost(); err != nil {
		t.Fatal(err)
	}
	assertHost(ls, true, false, 1)

	// scan the host the second time
	ls = time.Now()
	if err := scanHost(); err != nil {
		t.Fatal(err)
	}
	assertHost(ls, true, true, 2)

	// close the host to make scans fail
	tt.OK(host.Close())

	// scan the host a third time
	ls = time.Now()
	if err := scanHost(); err == nil {
		t.Fatal("expected scan error")
	}
	assertHost(ls, false, true, 3)

	// fetch hosts for scanning with maxLastScan set to now which should return
	// all hosts
	tt.Retry(100, 100*time.Millisecond, func() error {
		toScan, err := b.HostsForScanning(context.Background(), api.HostsForScanningOptions{
			MaxLastScan: api.TimeRFC3339(time.Now()),
		})
		tt.OK(err)
		if len(toScan) != 2 {
			return fmt.Errorf("expected 2 hosts, got %v", len(toScan))
		}
		return nil
	})

	// fetch hosts again with the unix epoch timestamp which should only return
	// 1 host since that one hasn't been scanned yet
	toScan, err := b.HostsForScanning(context.Background(), api.HostsForScanningOptions{
		MaxLastScan: api.TimeRFC3339(time.Unix(0, 1)),
	})
	tt.OK(err)
	if len(toScan) != 1 {
		t.Fatalf("expected 1 hosts, got %v", len(toScan))
	}
}
