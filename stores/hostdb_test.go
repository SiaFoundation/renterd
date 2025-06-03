package stores

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/coreutils/rhp/v4/siamux"
	"go.sia.tech/renterd/v2/api"
	"go.sia.tech/renterd/v2/internal/gouging"
	rhp4 "go.sia.tech/renterd/v2/internal/rhp/v4"
	"go.sia.tech/renterd/v2/internal/test"
	sql "go.sia.tech/renterd/v2/stores/sql"
)

// TestSQLHostDB tests the basic functionality of SQLHostDB using an in-memory
// SQLite DB.
func TestSQLHostDB(t *testing.T) {
	ss := newTestSQLStore(t, defaultTestSQLStoreConfig)
	defer ss.Close()

	// Try to fetch a random host. Should fail.
	ctx := context.Background()
	hk := types.GeneratePrivateKey().PublicKey()
	_, err := ss.Host(ctx, hk)
	if !errors.Is(err, api.ErrHostNotFound) {
		t.Fatal(err)
	}

	// Add the host
	err = ss.addTestHost(hk)
	if err != nil {
		t.Fatal(err)
	}

	// Assert it's returned
	allHosts, err := ss.Hosts(ctx, api.HostOptions{
		FilterMode:      api.HostFilterModeAll,
		UsabilityMode:   api.UsabilityFilterModeAll,
		AddressContains: "",
		KeyIn:           nil,
		Offset:          0,
		Limit:           -1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(allHosts) != 1 || allHosts[0].PublicKey != hk {
		t.Fatal("unexpected result", len(allHosts))
	}

	// Insert an announcement for the host and another one for an unknown
	// host.
	if err := ss.announceHost(hk, "address"); err != nil {
		t.Fatal(err)
	}

	// Fetch the host
	h, err := ss.Host(ctx, hk)
	if err != nil {
		t.Fatal(err)
	} else if h.V2SiamuxAddr() != "address" {
		t.Fatalf("unexpected address: %v", h.V2SiamuxAddr())
	} else if h.LastAnnouncement.IsZero() {
		t.Fatal("last announcement not set")
	}

	// Same thing again but with hosts.
	hosts, err := ss.Hosts(ctx, api.HostOptions{
		FilterMode:      api.HostFilterModeAll,
		UsabilityMode:   api.UsabilityFilterModeAll,
		AddressContains: "",
		KeyIn:           nil,
		Offset:          0,
		Limit:           -1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(hosts) != 1 {
		t.Fatal("wrong number of hosts", len(hosts))
	}
	h1 := hosts[0]
	if !reflect.DeepEqual(h1, h) {
		fmt.Println(h1)
		fmt.Println(h)
		t.Fatal("mismatch")
	}

	// Same thing again but with Host.
	h2, err := ss.Host(ctx, hk)
	if err != nil {
		t.Fatal(err)
	}
	if h2.V2SiamuxAddr() != h.V2SiamuxAddr() {
		t.Fatal("wrong net address")
	}
	if h2.KnownSince.IsZero() {
		t.Fatal("known since not set")
	}

	// Insert another announcement for an unknown host.
	randomHK := types.PublicKey{1, 4, 7}
	if err := ss.announceHost(types.PublicKey{1, 4, 7}, "na"); err != nil {
		t.Fatal(err)
	}
	h3, err := ss.Host(ctx, randomHK)
	if err != nil {
		t.Fatal(err)
	}
	if h3.V2SiamuxAddr() != "na" {
		t.Fatal("wrong net address")
	}
	if h3.KnownSince.IsZero() {
		t.Fatal("known since not set")
	}
}

// TestHosts is a unit test for the Hosts method of the SQLHostDB type.
func TestHosts(t *testing.T) {
	ss := newTestSQLStore(t, defaultTestSQLStoreConfig)
	defer ss.Close()
	ctx := context.Background()

	// add 3 hosts
	var hks []types.PublicKey
	for i := 1; i <= 3; i++ {
		hk := types.PublicKey{byte(i)}
		na := fmt.Sprintf("foo.com:100%d", i)
		switch i {
		case 1, 2:
			if err := ss.announceHost(hk, na); err != nil {
				t.Fatal(err)
			}
		case 3:
			if err := ss.announceV2Host(hk, na); err != nil {
				t.Fatal(err)
			}
		default:
			t.Fatal("unexpected")
		}
		hks = append(hks, hk)
	}
	hk1, hk2, hk3 := hks[0], hks[1], hks[2]

	// search all hosts
	his, err := ss.Hosts(context.Background(), api.HostOptions{
		FilterMode:      api.HostFilterModeAll,
		UsabilityMode:   api.UsabilityFilterModeAll,
		AddressContains: "",
		KeyIn:           nil,
		Offset:          0,
		Limit:           -1,
	})
	if err != nil {
		t.Fatal(err)
	} else if len(his) != 3 {
		t.Fatal("unexpected")
	}

	// assert offset & limit are taken into account
	his, err = ss.Hosts(context.Background(), api.HostOptions{
		FilterMode:      api.HostFilterModeAll,
		UsabilityMode:   api.UsabilityFilterModeAll,
		AddressContains: "",
		KeyIn:           nil,
		Offset:          0,
		Limit:           1,
	})
	if err != nil {
		t.Fatal(err)
	} else if len(his) != 1 {
		t.Fatal("unexpected")
	}
	his, err = ss.Hosts(context.Background(), api.HostOptions{
		FilterMode:      api.HostFilterModeAll,
		UsabilityMode:   api.UsabilityFilterModeAll,
		AddressContains: "",
		KeyIn:           nil,
		Offset:          1,
		Limit:           2,
	})
	if err != nil {
		t.Fatal(err)
	} else if len(his) != 2 {
		t.Fatal("unexpected")
	}
	his, err = ss.Hosts(context.Background(), api.HostOptions{
		FilterMode:      api.HostFilterModeAll,
		UsabilityMode:   api.UsabilityFilterModeAll,
		AddressContains: "",
		KeyIn:           nil,
		Offset:          3,
		Limit:           1,
	})
	if err != nil {
		t.Fatal(err)
	} else if len(his) != 0 {
		t.Fatal("unexpected")
	}

	// assert address and key filters are taken into account
	for i := 1; i <= 3; i++ {
		if hosts, err := ss.Hosts(ctx, api.HostOptions{
			FilterMode:      api.HostFilterModeAll,
			UsabilityMode:   api.UsabilityFilterModeAll,
			AddressContains: fmt.Sprintf("com:100%d", i),
			KeyIn:           nil,
			Offset:          0,
			Limit:           -1,
		}); err != nil || len(hosts) != 1 {
			t.Fatal("unexpected", len(hosts), err)
		}
	}
	if hosts, err := ss.Hosts(ctx, api.HostOptions{
		FilterMode:      api.HostFilterModeAll,
		UsabilityMode:   api.UsabilityFilterModeAll,
		AddressContains: "",
		KeyIn:           []types.PublicKey{hk2, hk3},
		Offset:          0,
		Limit:           -1,
	}); err != nil || len(hosts) != 2 {
		t.Fatal("unexpected", len(hosts), err)
	}
	if hosts, err := ss.Hosts(ctx, api.HostOptions{
		FilterMode:      api.HostFilterModeAll,
		UsabilityMode:   api.UsabilityFilterModeAll,
		AddressContains: "com:1002",
		KeyIn:           []types.PublicKey{hk2, hk3},
		Offset:          0,
		Limit:           -1,
	}); err != nil || len(hosts) != 1 {
		t.Fatal("unexpected", len(hosts), err)
	}
	if hosts, err := ss.Hosts(ctx, api.HostOptions{
		FilterMode:      api.HostFilterModeAll,
		UsabilityMode:   api.UsabilityFilterModeAll,
		AddressContains: "com:1002",
		KeyIn:           []types.PublicKey{hk1},
		Offset:          0,
		Limit:           -1,
	}); err != nil || len(hosts) != 0 {
		t.Fatal("unexpected", len(hosts), err)
	}

	// assert host filter mode is taken into account
	err = ss.UpdateHostBlocklistEntries(context.Background(), []string{"foo.com:1001"}, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	his, err = ss.Hosts(context.Background(), api.HostOptions{
		FilterMode:      api.HostFilterModeAllowed,
		UsabilityMode:   api.UsabilityFilterModeAll,
		AddressContains: "",
		KeyIn:           nil,
		Offset:          0,
		Limit:           -1,
	})
	if err != nil {
		t.Fatal(err)
	} else if len(his) != 2 {
		t.Fatal("unexpected")
	} else if his[0].PublicKey != (types.PublicKey{2}) || his[1].PublicKey != (types.PublicKey{3}) {
		t.Fatal("unexpected", his[0].PublicKey, his[1].PublicKey)
	}
	his, err = ss.Hosts(context.Background(), api.HostOptions{
		FilterMode:      api.HostFilterModeBlocked,
		UsabilityMode:   api.UsabilityFilterModeAll,
		AddressContains: "",
		KeyIn:           nil,
		Offset:          0,
		Limit:           -1,
	})
	if err != nil {
		t.Fatal(err)
	} else if len(his) != 1 {
		t.Fatal("unexpected")
	} else if his[0].PublicKey != (types.PublicKey{1}) {
		t.Fatal("unexpected", his)
	}
	err = ss.UpdateHostBlocklistEntries(context.Background(), nil, nil, true)
	if err != nil {
		t.Fatal(err)
	}

	// add host checks
	h1c := newTestHostCheck()
	h1c.ScoreBreakdown.Age = .1
	err = ss.UpdateHostCheck(context.Background(), hk1, h1c)
	if err != nil {
		t.Fatal(err)
	}
	h2c := newTestHostCheck()
	h2c.ScoreBreakdown.Age = .21
	err = ss.UpdateHostCheck(context.Background(), hk2, h2c)
	if err != nil {
		t.Fatal(err)
	}

	// assert number of host checks
	checkCount := func() int64 {
		t.Helper()
		return ss.Count("host_checks")
	}
	if cnt := checkCount(); cnt != 2 {
		t.Fatal("unexpected", cnt)
	}

	// fetch all hosts
	his, err = ss.Hosts(context.Background(), api.HostOptions{
		FilterMode:      api.HostFilterModeAll,
		UsabilityMode:   api.UsabilityFilterModeAll,
		AddressContains: "",
		KeyIn:           nil,
		Offset:          0,
		Limit:           -1,
	})
	if err != nil {
		t.Fatal(err)
	} else if len(his) != 3 {
		t.Fatal("unexpected", len(his))
	}

	// assert h1 and h2 have the expected checks
	if his[0].Checks != h1c {
		t.Fatal("unexpected", his[0].Checks)
	} else if his[1].Checks != h2c {
		t.Fatal("unexpected", his[1].Checks)
	}

	// use reflection to check whether usability is correctly taken into account
	// for every field of the usability breakdown
	opts := api.HostOptions{
		FilterMode:      api.HostFilterModeAll,
		AddressContains: "",
		KeyIn:           nil,
		Offset:          0,
		Limit:           -1,
	}

	assertHostUsability := func() error {
		t.Helper()

		opts.UsabilityMode = api.UsabilityFilterModeUsable
		if his, err := ss.Hosts(ctx, opts); err != nil {
			return err
		} else if len(his) != 1 {
			return fmt.Errorf("expected one usable host, but got %d", len(his))
		} else if his[0].PublicKey != hk1 {
			return fmt.Errorf("unexpected host is usable, hk %v", his[0].PublicKey)
		}

		opts.UsabilityMode = api.UsabilityFilterModeUnusable
		if his, err := ss.Hosts(context.Background(), opts); err != nil {
			return err
		} else if len(his) != 1 {
			return fmt.Errorf("expected one unusable host, but got %d", len(his))
		} else if his[0].PublicKey != hk2 {
			return fmt.Errorf("unexpected host is unusable, hk %v", his[0].PublicKey)
		}

		return nil
	}

	v := reflect.ValueOf(&h2c.UsabilityBreakdown).Elem()
	for i := 0; i < v.NumField(); i++ {
		field := v.Field(i)
		if !field.CanSet() || field.Kind() != reflect.Bool {
			continue
		}

		field.SetBool(true)
		if err := ss.UpdateHostCheck(ctx, hk2, h2c); err != nil {
			t.Fatalf("failed to update host check after setting %s: %v", v.Type().Field(i).Name, err)
		} else if err := assertHostUsability(); err != nil {
			t.Fatalf("usability filter is not taken into account after setting %s: %v", v.Type().Field(i).Name, err)
		}
		field.SetBool(false)
	}

	// assert cascade delete on host
	_, err = ss.DB().Exec(context.Background(), "DELETE FROM hosts WHERE public_key = ?", sql.PublicKey(types.PublicKey{1}))
	if err != nil {
		t.Fatal(err)
	}
	if cnt := checkCount(); cnt != 1 {
		t.Fatal("unexpected", cnt)
	}
}

func TestUsableHosts(t *testing.T) {
	ss := newTestSQLStore(t, defaultTestSQLStoreConfig)
	defer ss.Close()
	ctx := context.Background()

	// prepare hosts & contracts
	//
	// h1: usable
	// h2: not usable - blocked
	// h3: not usable - no host check
	// h4: not usable - no contract
	var hks []types.PublicKey
	for i := 1; i <= 4; i++ {
		// add host
		hk := types.PublicKey{byte(i)}
		addr := fmt.Sprintf("foo.com:100%d", i)
		err := ss.addCustomTestHost(hk, addr)
		if err != nil {
			t.Fatal(err)
		}
		hks = append(hks, hk)

		// add host scan
		hs := test.NewV2HostSettings()
		s1 := newTestScan(hk, time.Now(), hs, true)
		if err := ss.RecordHostScans(context.Background(), []api.HostScan{s1}); err != nil {
			t.Fatal(err)
		}

		// add host check
		if i != 3 {
			hc := newTestHostCheck()
			err = ss.UpdateHostCheck(context.Background(), hk, hc)
			if err != nil {
				t.Fatal(err)
			}
		}

		// add contract
		if i != 4 {
			_, err = ss.addTestContract(types.FileContractID{byte(i)}, hk)
			if err != nil {
				t.Fatal(err)
			}
		}

		// block host
		if i == 2 {
			err = ss.UpdateHostBlocklistEntries(context.Background(), []string{addr}, nil, false)
			if err != nil {
				t.Fatal(err)
			}
		}
	}

	// assert h1 is usable
	hosts, err := ss.UsableHosts(ctx)
	if err != nil {
		t.Fatal(err)
	} else if len(hosts) != 1 {
		t.Fatal("unexpected", len(hosts))
	} else if hosts[0].PublicKey != hks[0] {
		t.Fatal("unexpected", hosts)
	} else if hosts[0].V2SiamuxAddr() != "foo.com:9983" {
		t.Fatal("unexpected", hosts)
	}

	// create gouging checker
	gs := test.GougingSettings
	cs := api.ConsensusState{Synced: true}
	gc := gouging.NewChecker(gs, cs)

	// assert h1 is not gouging
	h1 := hosts[0]
	if gc.CheckV2(h1.V2HS).Gouging() {
		t.Fatal("unexpected")
	}

	// record a scan for h1 to make it gouging
	hs := test.NewV2HostSettings()
	hs.Prices.IngressPrice = gs.MaxUploadPrice
	s1 := newTestScan(h1.PublicKey, time.Now(), hs, true)
	if err := ss.RecordHostScans(context.Background(), []api.HostScan{s1}); err != nil {
		t.Fatal(err)
	}

	// fetch it again
	hosts, err = ss.UsableHosts(ctx)
	if err != nil {
		t.Fatal(err)
	} else if len(hosts) != 1 {
		t.Fatal("unexpected", len(hosts))
	}

	// assert h1 is now gouging
	h1 = hosts[0]
	if !gc.CheckV2(h1.V2HS).Gouging() {
		t.Fatal("unexpected", h1.V2HS.Prices.IngressPrice, gs.MaxUploadPrice)
	}

	// create helper to assert number of usable hosts
	assertNumUsableHosts := func(n int) {
		t.Helper()
		hosts, err = ss.UsableHosts(ctx)
		if err != nil {
			t.Fatal(err)
		} else if len(hosts) != n {
			t.Fatal("unexpected", len(hosts))
		}
	}

	// unblock h2
	if err := ss.UpdateHostBlocklistEntries(context.Background(), nil, nil, true); err != nil {
		t.Fatal(err)
	}

	assertNumUsableHosts(2)

	// add host check for h3
	hc := newTestHostCheck()
	err = ss.UpdateHostCheck(context.Background(), types.PublicKey{3}, hc)
	if err != nil {
		t.Fatal(err)
	}

	assertNumUsableHosts(3)

	// add contract for h4
	_, err = ss.addTestContract(types.FileContractID{byte(4)}, types.PublicKey{4})
	if err != nil {
		t.Fatal(err)
	}

	assertNumUsableHosts(4)

	// add an allowlist
	ss.UpdateHostAllowlistEntries(context.Background(), []types.PublicKey{{9}}, nil, false)

	assertNumUsableHosts(0)
}

// TestRecordScan is a test for recording scans.
func TestRecordScan(t *testing.T) {
	ss := newTestSQLStore(t, defaultTestSQLStoreConfig)
	defer ss.Close()

	// Add a host.
	hk := types.GeneratePrivateKey().PublicKey()
	err := ss.addCustomTestHost(hk, "host.com")
	if err != nil {
		t.Fatal(err)
	}

	// The host shouldn't have any interaction related fields set.
	ctx := context.Background()
	host, err := ss.Host(ctx, hk)
	if err != nil {
		t.Fatal(err)
	}
	if host.Interactions != (api.HostInteractions{}) {
		t.Fatal("mismatch", cmp.Diff(host.Interactions, api.HostInteractions{}))
	}
	if host.V2Settings != (rhp4.HostSettings{}) {
		t.Fatal("mismatch")
	}

	// Fetch the host directly to get the creation time.
	h, err := ss.Host(ctx, hk)
	if err != nil {
		t.Fatal(err)
	} else if h.KnownSince.IsZero() {
		t.Fatal("known since not set")
	}

	// Record a scan.
	firstScanTime := time.Now().UTC()
	settings := test.NewV2HostSettings()
	settings.Prices.TipHeight = 123
	if err := ss.RecordHostScans(ctx, []api.HostScan{newTestScan(hk, firstScanTime, settings, true)}); err != nil {
		t.Fatal(err)
	}
	host, err = ss.Host(ctx, hk)
	if err != nil {
		t.Fatal(err)
	} else if time.Now().Before(host.V2Settings.Prices.ValidUntil) {
		t.Fatal("invalid expiry")
	} else if host.V2Settings.Prices.TipHeight != settings.Prices.TipHeight {
		t.Fatalf("mismatch %v %v", host.V2Settings.Prices.TipHeight, settings.Prices.TipHeight)
	}

	// We expect no uptime or downtime from only a single scan.
	uptime := time.Duration(0)
	downtime := time.Duration(0)
	if host.Interactions.LastScan.UnixMilli() != firstScanTime.UnixMilli() {
		t.Fatal("wrong time")
	}
	host.Interactions.LastScan = time.Time{}
	if expected := (api.HostInteractions{
		TotalScans:              1,
		LastScan:                time.Time{},
		LastScanSuccess:         true,
		SecondToLastScanSuccess: false,
		Uptime:                  uptime,
		Downtime:                downtime,
		SuccessfulInteractions:  1,
		FailedInteractions:      0,
	}); host.Interactions != expected {
		t.Fatal("mismatch", cmp.Diff(host.Interactions, expected))
	}
	if !reflect.DeepEqual(host.V2Settings, settings) {
		t.Fatal("mismatch")
	}

	// Record another scan 1 hour after the previous one. We don't pass any
	// subnets this time.
	secondScanTime := firstScanTime.Add(time.Hour)
	settings.Prices.TipHeight = 456
	if err := ss.RecordHostScans(ctx, []api.HostScan{newTestScan(hk, secondScanTime, settings, true)}); err != nil {
		t.Fatal(err)
	}
	host, err = ss.Host(ctx, hk)
	if err != nil {
		t.Fatal(err)
	} else if host.Interactions.LastScan.UnixMilli() != secondScanTime.UnixMilli() {
		t.Fatal("wrong time")
	}
	host.Interactions.LastScan = time.Time{}
	uptime += secondScanTime.Sub(firstScanTime)
	if host.Interactions != (api.HostInteractions{
		TotalScans:              2,
		LastScan:                time.Time{},
		LastScanSuccess:         true,
		SecondToLastScanSuccess: true,
		Uptime:                  uptime,
		Downtime:                downtime,
		SuccessfulInteractions:  2,
		FailedInteractions:      0,
	}) {
		t.Fatal("mismatch")
	}

	// Record another scan 2 hours after the second one. This time it fails.
	thirdScanTime := secondScanTime.Add(2 * time.Hour)
	if err := ss.RecordHostScans(ctx, []api.HostScan{newTestScan(hk, thirdScanTime, settings, false)}); err != nil {
		t.Fatal(err)
	}
	host, err = ss.Host(ctx, hk)
	if err != nil {
		t.Fatal(err)
	}
	if host.Interactions.LastScan.UnixMilli() != thirdScanTime.UnixMilli() {
		t.Fatal("wrong time")
	}
	host.Interactions.LastScan = time.Time{}
	downtime += thirdScanTime.Sub(secondScanTime)
	if host.Interactions != (api.HostInteractions{
		TotalScans:              3,
		LastScan:                time.Time{},
		LastScanSuccess:         false,
		SecondToLastScanSuccess: true,
		Uptime:                  uptime,
		Downtime:                downtime,
		SuccessfulInteractions:  2,
		FailedInteractions:      1,
	}) {
		t.Fatal("mismatch")
	}
}

func TestRemoveHosts(t *testing.T) {
	ss := newTestSQLStore(t, defaultTestSQLStoreConfig)
	defer ss.Close()

	// add a host
	hk := types.GeneratePrivateKey().PublicKey()
	err := ss.addTestHost(hk)
	if err != nil {
		t.Fatal(err)
	}

	// fetch the host and assert the downtime is zero
	h, err := ss.Host(context.Background(), hk)
	if err != nil {
		t.Fatal(err)
	} else if h.Interactions.Downtime != 0 {
		t.Fatal("downtime is not zero")
	}

	// assert no hosts are removed
	removed, err := ss.RemoveOfflineHosts(context.Background(), 0, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if removed != 0 {
		t.Fatal("expected no hosts to be removed")
	}

	now := time.Now().UTC()
	t1 := now.Add(-time.Minute * 120) // 2 hours ago
	t2 := now.Add(-time.Minute * 90)  // 1.5 hours ago (30min downtime)
	hi1 := newTestScan(hk, t1, rhp4.HostSettings{}, false)
	hi2 := newTestScan(hk, t2, rhp4.HostSettings{}, false)

	// record interactions
	if err := ss.RecordHostScans(context.Background(), []api.HostScan{hi1, hi2}); err != nil {
		t.Fatal(err)
	}

	// fetch the host and assert the downtime is 30 minutes and he has 2 recent scan failures
	h, err = ss.Host(context.Background(), hk)
	if err != nil {
		t.Fatal(err)
	}
	if h.Interactions.Downtime.Minutes() != 30 {
		t.Fatal("downtime is not 30 minutes", h.Interactions.Downtime.Minutes())
	}
	if h.Interactions.FailedInteractions != 2 {
		t.Fatal("recent scan failures is not 2", h.Interactions.FailedInteractions)
	}

	// assert no hosts are removed
	removed, err = ss.RemoveOfflineHosts(context.Background(), 0, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if removed != 0 {
		t.Fatal("expected no hosts to be removed")
	}

	// record interactions
	t3 := now.Add(-time.Minute * 60) // 1 hour ago (60min downtime)
	hi3 := newTestScan(hk, t3, rhp4.HostSettings{}, false)
	if err := ss.RecordHostScans(context.Background(), []api.HostScan{hi3}); err != nil {
		t.Fatal(err)
	}

	// assert no hosts are removed at 61 minutes
	removed, err = ss.RemoveOfflineHosts(context.Background(), 0, time.Minute*61)
	if err != nil {
		t.Fatal(err)
	}
	if removed != 0 {
		t.Fatal("expected no hosts to be removed")
	}

	// assert no hosts are removed at 60 minutes if we require at least 4 failed scans
	removed, err = ss.RemoveOfflineHosts(context.Background(), 4, time.Minute*60)
	if err != nil {
		t.Fatal(err)
	}
	if removed != 0 {
		t.Fatal("expected no hosts to be removed")
	}

	// assert hosts gets removed at 60 minutes if we require at least 3 failed scans
	removed, err = ss.RemoveOfflineHosts(context.Background(), 3, time.Minute*60)
	if err != nil {
		t.Fatal(err)
	}
	if removed != 1 {
		t.Fatal("expected 1 host to be removed")
	}

	// assert host is removed from the database
	if _, err = ss.Host(context.Background(), hk); !errors.Is(err, api.ErrHostNotFound) {
		t.Fatal("expected record not found error")
	}
}

func TestSQLHostAllowlist(t *testing.T) {
	ss := newTestSQLStore(t, defaultTestSQLStoreConfig)
	defer ss.Close()

	ctx := context.Background()

	numEntries := func() int {
		t.Helper()
		bl, err := ss.HostAllowlist(ctx)
		if err != nil {
			t.Fatal(err)
		}
		return len(bl)
	}

	numHosts := func() int {
		t.Helper()
		hosts, err := ss.Hosts(ctx, api.HostOptions{
			FilterMode:      api.HostFilterModeAllowed,
			UsabilityMode:   api.UsabilityFilterModeAll,
			AddressContains: "",
			KeyIn:           nil,
			Offset:          0,
			Limit:           -1,
		})
		if err != nil {
			t.Fatal(err)
		}
		return len(hosts)
	}

	numRelations := func() (cnt int64) {
		t.Helper()
		return ss.Count("host_allowlist_entry_hosts")
	}

	isAllowed := func(hk types.PublicKey) bool {
		t.Helper()
		host, err := ss.Host(ctx, hk)
		if err != nil {
			t.Fatal(err)
		}
		return !host.Blocked
	}

	// add three hosts
	hks, err := ss.addTestHosts(3)
	if err != nil {
		t.Fatal(err)
	}
	hk1, hk2, hk3 := hks[0], hks[1], hks[2]

	// assert we can find them
	if numHosts() != 3 {
		t.Fatalf("unexpected number of hosts, %v != 3", numHosts())
	}

	// assert allowlist is empty
	if numEntries() != 0 {
		t.Fatalf("unexpected number of entries in allowlist, %v != 0", numEntries())
	}

	// assert we can add entries to the allowlist
	err = ss.UpdateHostAllowlistEntries(ctx, []types.PublicKey{hk1, hk2}, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	if numEntries() != 2 {
		t.Fatalf("unexpected number of entries in allowlist, %v != 2", numEntries())
	}
	if numRelations() != 2 {
		t.Fatalf("unexpected number of entries in join table, %v != 2", numRelations())
	}

	// assert both h1 and h2 are allowed now
	if !isAllowed(hk1) || !isAllowed(hk2) || isAllowed(hk3) {
		t.Fatal("unexpected hosts are allowed", isAllowed(hk1), isAllowed(hk2), isAllowed(hk3))
	}

	// assert adding the same entry is a no-op and we can remove an entry at the same time
	err = ss.UpdateHostAllowlistEntries(ctx, []types.PublicKey{hk1}, []types.PublicKey{hk2}, false)
	if err != nil {
		t.Fatal(err)
	}
	if numEntries() != 1 {
		t.Fatalf("unexpected number of entries in allowlist, %v != 1", numEntries())
	}
	if numRelations() != 1 {
		t.Fatalf("unexpected number of entries in join table, %v != 1", numRelations())
	}

	// assert only h1 is allowed now
	if !isAllowed(hk1) || isAllowed(hk2) || isAllowed(hk3) {
		t.Fatal("unexpected hosts are allowed", isAllowed(hk1), isAllowed(hk2), isAllowed(hk3))
	}

	// assert removing a non-existing entry is a no-op
	err = ss.UpdateHostAllowlistEntries(ctx, nil, []types.PublicKey{hk2}, false)
	if err != nil {
		t.Fatal(err)
	}

	assertHosts := func(total, allowed, blocked int) error {
		t.Helper()
		hosts, err := ss.Hosts(context.Background(), api.HostOptions{
			FilterMode:      api.HostFilterModeAll,
			UsabilityMode:   api.UsabilityFilterModeAll,
			AddressContains: "",
			KeyIn:           nil,
			Offset:          0,
			Limit:           -1,
		})
		if err != nil {
			return err
		}
		if len(hosts) != total {
			return fmt.Errorf("invalid number of hosts: %v", len(hosts))
		}
		hosts, err = ss.Hosts(context.Background(), api.HostOptions{
			FilterMode:      api.HostFilterModeAllowed,
			UsabilityMode:   api.UsabilityFilterModeAll,
			AddressContains: "",
			KeyIn:           nil,
			Offset:          0,
			Limit:           -1,
		})
		if err != nil {
			return err
		}
		if len(hosts) != allowed {
			return fmt.Errorf("invalid number of hosts: %v", len(hosts))
		}
		hosts, err = ss.Hosts(context.Background(), api.HostOptions{
			FilterMode:      api.HostFilterModeBlocked,
			UsabilityMode:   api.UsabilityFilterModeAll,
			AddressContains: "",
			KeyIn:           nil,
			Offset:          0,
			Limit:           -1,
		})
		if err != nil {
			return err
		}
		if len(hosts) != blocked {
			return fmt.Errorf("invalid number of hosts: %v", len(hosts))
		}
		return nil
	}

	// Search for hosts using different modes. Should have 3 hosts in total, 2
	// allowed ones and 2 blocked ones.
	if err := assertHosts(3, 1, 2); err != nil {
		t.Fatal(err)
	}

	// remove host 1
	if err := ss.DeleteHost(hk1); err != nil {
		t.Fatal(err)
	}
	if numHosts() != 0 {
		t.Fatalf("unexpected number of hosts, %v != 0", numHosts())
	}
	if numRelations() != 0 {
		t.Fatalf("unexpected number of entries in join table, %v != 0", numRelations())
	}
	if numEntries() != 1 {
		t.Fatalf("unexpected number of entries in blocklist, %v != 1", numEntries())
	}

	// Search for hosts using different modes. Should have 2 hosts in total, 0
	// allowed ones and 2 blocked ones.
	if err := assertHosts(2, 0, 2); err != nil {
		t.Fatal(err)
	}

	// remove the allowlist entry for h1
	err = ss.UpdateHostAllowlistEntries(ctx, nil, []types.PublicKey{hk1}, false)
	if err != nil {
		t.Fatal(err)
	}

	// Search for hosts using different modes. Should have 2 hosts in total, 2
	// allowed ones and 0 blocked ones.
	if err := assertHosts(2, 2, 0); err != nil {
		t.Fatal(err)
	}

	// assert hosts reappear
	if numHosts() != 2 {
		t.Fatalf("unexpected number of hosts, %v != 2", numHosts())
	}
	if numEntries() != 0 {
		t.Fatalf("unexpected number of entries in blocklist, %v != 0", numEntries())
	}
}

func TestSQLHostBlocklist(t *testing.T) {
	ss := newTestSQLStore(t, defaultTestSQLStoreConfig)
	defer ss.Close()

	ctx := context.Background()

	numEntries := func() int {
		t.Helper()
		bl, err := ss.HostBlocklist(ctx)
		if err != nil {
			t.Fatal(err)
		}
		return len(bl)
	}

	numHosts := func() int {
		t.Helper()
		hosts, err := ss.Hosts(ctx, api.HostOptions{
			FilterMode:      api.HostFilterModeAllowed,
			UsabilityMode:   api.UsabilityFilterModeAll,
			AddressContains: "",
			KeyIn:           nil,
			Offset:          0,
			Limit:           -1,
		})
		if err != nil {
			t.Fatal(err)
		}
		return len(hosts)
	}

	numAllowlistRelations := func() (cnt int64) {
		t.Helper()
		return ss.Count("host_allowlist_entry_hosts")
	}

	numBlocklistRelations := func() (cnt int64) {
		t.Helper()
		return ss.Count("host_blocklist_entry_hosts")
	}

	isBlocked := func(hk types.PublicKey) bool {
		t.Helper()
		host, _ := ss.Host(ctx, hk)
		return host.Blocked
	}

	// add three hosts
	hk1 := types.GeneratePrivateKey().PublicKey()
	if err := ss.addCustomTestHost(hk1, "foo.bar.com:1000"); err != nil {
		t.Fatal(err)
	}
	hk2 := types.GeneratePrivateKey().PublicKey()
	if err := ss.addCustomTestHost(hk2, "bar.baz.com:2000"); err != nil {
		t.Fatal(err)
	}
	hk3 := types.GeneratePrivateKey().PublicKey()
	if err := ss.addCustomTestHost(hk3, "foobar.com:3000"); err != nil {
		t.Fatal(err)
	}

	// assert we can find them
	if numHosts() != 3 {
		t.Fatalf("unexpected number of hosts, %v != 3", numHosts())
	}

	// assert blocklist is empty
	if numEntries() != 0 {
		t.Fatalf("unexpected number of entries in blocklist, %v != 0", numEntries())
	}

	// assert we can add entries to the blocklist
	err := ss.UpdateHostBlocklistEntries(ctx, []string{"foo.bar.com", "bar.com"}, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	if numEntries() != 2 {
		t.Fatalf("unexpected number of entries in blocklist, %v != 2", numEntries())
	}
	if numBlocklistRelations() != 2 {
		t.Fatalf("unexpected number of entries in join table, %v != 2", numBlocklistRelations())
	}

	// assert only host 1 is blocked now
	if !isBlocked(hk1) || isBlocked(hk2) || isBlocked(hk3) {
		t.Fatal("unexpected host is blocked", isBlocked(hk1), isBlocked(hk2), isBlocked(hk3))
	}
	if host, err := ss.Host(ctx, hk1); err != nil {
		t.Fatal("unexpected err", err)
	} else if !host.Blocked {
		t.Fatal("expected host to be blocked")
	}

	// assert adding the same entry is a no-op, and we can remove entries at the same time
	err = ss.UpdateHostBlocklistEntries(ctx, []string{"foo.bar.com", "bar.com"}, []string{"foo.bar.com"}, false)
	if err != nil {
		t.Fatal(err)
	}
	if numEntries() != 1 {
		t.Fatalf("unexpected number of entries in blocklist, %v != 1", numEntries())
	}
	if numBlocklistRelations() != 1 {
		t.Fatalf("unexpected number of entries in join table, %v != 1", numBlocklistRelations())
	}

	// assert the host is still blocked
	if !isBlocked(hk1) || isBlocked(hk2) {
		t.Fatal("unexpected host is blocked", isBlocked(hk1), isBlocked(hk2))
	}

	// assert removing a non-existing entry is a no-op
	err = ss.UpdateHostBlocklistEntries(ctx, nil, []string{"foo.bar.com"}, false)
	if err != nil {
		t.Fatal(err)
	}
	// remove the other entry and assert the delete cascaded properly
	err = ss.UpdateHostBlocklistEntries(ctx, nil, []string{"bar.com"}, false)
	if err != nil {
		t.Fatal(err)
	}
	if numEntries() != 0 {
		t.Fatalf("unexpected number of entries in blocklist, %v != 0", numEntries())
	}
	if isBlocked(hk1) || isBlocked(hk2) {
		t.Fatal("unexpected host is blocked", isBlocked(hk1), isBlocked(hk2))
	}
	if numBlocklistRelations() != 0 {
		t.Fatalf("unexpected number of entries in join table, %v != 0", numBlocklistRelations())
	}

	// block the second host
	err = ss.UpdateHostBlocklistEntries(ctx, []string{"baz.com"}, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	if isBlocked(hk1) || !isBlocked(hk2) {
		t.Fatal("unexpected host is blocked", isBlocked(hk1), isBlocked(hk2))
	}
	if numBlocklistRelations() != 1 {
		t.Fatalf("unexpected number of entries in join table, %v != 1", numBlocklistRelations())
	}

	// delete host 2 and assert the delete cascaded properly
	if err = ss.DeleteHost(hk2); err != nil {
		t.Fatal(err)
	}
	if numHosts() != 2 {
		t.Fatalf("unexpected number of hosts, %v != 2", numHosts())
	}
	if numBlocklistRelations() != 0 {
		t.Fatalf("unexpected number of entries in join table, %v != 0", numBlocklistRelations())
	}
	if numEntries() != 1 {
		t.Fatalf("unexpected number of entries in blocklist, %v != 1", numEntries())
	}

	// add two hosts, one that should be blocked by 'baz.com' and one that should not
	hk4 := types.GeneratePrivateKey().PublicKey()
	if err := ss.addCustomTestHost(hk4, "foo.baz.com:3000"); err != nil {
		t.Fatal(err)
	}
	hk5 := types.GeneratePrivateKey().PublicKey()
	if err := ss.addCustomTestHost(hk5, "foo.baz.commmmm:3000"); err != nil {
		t.Fatal(err)
	}

	if host, err := ss.Host(ctx, hk4); err != nil {
		t.Fatal(err)
	} else if !host.Blocked {
		t.Fatal("expected host to be blocked")
	}
	if _, err = ss.Host(ctx, hk5); err != nil {
		t.Fatal("expected host to be found")
	}

	// now update host 4's address so it's no longer blocked
	if err := ss.addCustomTestHost(hk4, "foo.baz.commmmm:3000"); err != nil {
		t.Fatal(err)
	}
	if _, err = ss.Host(ctx, hk4); err != nil {
		t.Fatal("expected host to be found")
	}
	if numBlocklistRelations() != 0 {
		t.Fatalf("unexpected number of entries in join table, %v != 0", numBlocklistRelations())
	}

	// add another entry that blocks multiple hosts
	err = ss.UpdateHostBlocklistEntries(ctx, []string{"com"}, nil, false)
	if err != nil {
		t.Fatal(err)
	}

	// assert 2 out of 5 hosts are blocked
	if !isBlocked(hk1) || !isBlocked(hk3) || isBlocked(hk4) || isBlocked(hk5) {
		t.Fatal("unexpected host is blocked", isBlocked(hk1), isBlocked(hk3), isBlocked(hk4), isBlocked(hk5))
	}

	// add host 5 to the allowlist
	err = ss.UpdateHostAllowlistEntries(ctx, []types.PublicKey{hk5}, nil, false)
	if err != nil {
		t.Fatal(err)
	}

	// assert all hosts except host 5 are blocked
	if !isBlocked(hk1) || !isBlocked(hk3) || !isBlocked(hk4) || isBlocked(hk5) {
		t.Fatal("unexpected host is blocked", isBlocked(hk1), isBlocked(hk3), isBlocked(hk4), isBlocked(hk5))
	}

	// add a rule to block host 5
	err = ss.UpdateHostBlocklistEntries(ctx, []string{"foo.baz.commmmm"}, nil, false)
	if err != nil {
		t.Fatal(err)
	}

	// assert all hosts are blocked
	if !isBlocked(hk1) || !isBlocked(hk3) || !isBlocked(hk4) || !isBlocked(hk5) {
		t.Fatal("unexpected host is blocked", isBlocked(hk1), isBlocked(hk3), isBlocked(hk4), isBlocked(hk5))
	}

	// clear the blocklist
	if numBlocklistRelations() == 0 {
		t.Fatalf("expected more than zero entries in join table, instead there were %v", numBlocklistRelations())
	}
	err = ss.UpdateHostBlocklistEntries(ctx, nil, nil, true)
	if err != nil {
		t.Fatal(err)
	}
	if numBlocklistRelations() != 0 {
		t.Fatalf("expected zero entries in join table, instead there were %v", numBlocklistRelations())
	}

	// clear the allowlist
	if numAllowlistRelations() == 0 {
		t.Fatalf("expected more than zero entries in join table, instead there were %v", numBlocklistRelations())
	}
	err = ss.UpdateHostAllowlistEntries(ctx, nil, nil, true)
	if err != nil {
		t.Fatal(err)
	}
	if numAllowlistRelations() != 0 {
		t.Fatalf("expected zero entries in join table, instead there were %v", numBlocklistRelations())
	}
}

func TestSQLHostBlocklistBasic(t *testing.T) {
	ss := newTestSQLStore(t, defaultTestSQLStoreConfig)
	defer ss.Close()

	ctx := context.Background()

	// add a host
	hk1 := types.GeneratePrivateKey().PublicKey()
	if err := ss.addCustomTestHost(hk1, "foo.bar.com:1000"); err != nil {
		t.Fatal(err)
	}

	// block that host
	err := ss.UpdateHostBlocklistEntries(ctx, []string{"foo.bar.com"}, nil, false)
	if err != nil {
		t.Fatal(err)
	}

	// assert it's blocked
	host, _ := ss.Host(ctx, hk1)
	if !host.Blocked {
		t.Fatal("unexpected")
	}

	// reannounce to ensure it's no longer blocked
	if err := ss.addCustomTestHost(hk1, "bar.baz.com:1000"); err != nil {
		t.Fatal(err)
	}

	// assert it's no longer blocked
	host, _ = ss.Host(ctx, hk1)
	if host.Blocked {
		t.Fatal("unexpected")
	}
}

// newTestScan returns a host interaction with given parameters.
func newTestScan(hk types.PublicKey, scanTime time.Time, settings rhp4.HostSettings, success bool) api.HostScan {
	return api.HostScan{
		HostKey:   hk,
		Success:   success,
		Timestamp: scanTime,
	}
}

func newTestHostCheck() api.HostChecks {
	return api.HostChecks{

		GougingBreakdown: api.HostGougingBreakdown{
			DownloadErr: "bar",
			GougingErr:  "baz",
			PruneErr:    "qux",
			UploadErr:   "quuz",
		},
		ScoreBreakdown: api.HostScoreBreakdown{
			Age:              .1,
			Collateral:       .2,
			Interactions:     .3,
			StorageRemaining: .4,
			Uptime:           .5,
			Version:          .6,
			Prices:           .7,
		},
		UsabilityBreakdown: api.HostUsabilityBreakdown{
			Blocked:               false,
			Offline:               false,
			LowMaxDuration:        false,
			LowScore:              false,
			RedundantIP:           false,
			Gouging:               false,
			NotAcceptingContracts: false,
			NotAnnounced:          false,
			NotCompletingScan:     false,
		},
	}
}

// addCustomTestHost ensures a host with given hostkey and net address exists.
func (s *testSQLStore) addCustomTestHost(hk types.PublicKey, na string) error {
	if err := s.announceHost(hk, na); err != nil {
		return err
	}
	return nil
}

// addTestHost ensures a host with given hostkey exists.
func (s *testSQLStore) addTestHost(hk types.PublicKey) error {
	return s.addCustomTestHost(hk, "")
}

// addTestHosts adds 'n' hosts to the db and returns their keys.
func (s *testSQLStore) addTestHosts(n int) (keys []types.PublicKey, err error) {
	cnt := s.Count("contracts")

	for i := 0; i < n; i++ {
		keys = append(keys, types.PublicKey{byte(int(cnt) + i + 1)})
		if err := s.addTestHost(keys[len(keys)-1]); err != nil {
			return nil, err
		}
	}
	return
}

// announceHost adds a host announcement to the database.
func (s *testSQLStore) announceHost(hk types.PublicKey, na string) error {
	return s.db.Transaction(context.Background(), func(tx sql.DatabaseTx) error {
		return tx.ProcessChainUpdate(context.Background(), func(tx sql.ChainUpdateTx) error {
			return tx.UpdateHost(hk, na, nil, 42, types.BlockID{1, 2, 3}, time.Now().UTC().Round(time.Second))
		})
	})
}

// announceHost adds a v2 host announcement to the database.
func (s *testSQLStore) announceV2Host(hk types.PublicKey, na string) error {
	return s.db.Transaction(context.Background(), func(tx sql.DatabaseTx) error {
		return tx.ProcessChainUpdate(context.Background(), func(tx sql.ChainUpdateTx) error {
			return tx.UpdateHost(hk, "", chain.V2HostAnnouncement{
				{
					Address:  na,
					Protocol: siamux.Protocol,
				},
			}, 42, types.BlockID{1, 2, 3}, time.Now().UTC().Round(time.Second))
		})
	})
}

func (s *testSQLStore) DeleteHost(hk types.PublicKey) error {
	_, err := s.DB().Exec(context.Background(), "DELETE FROM hosts WHERE public_key = ?", sql.PublicKey(hk))
	return err
}
