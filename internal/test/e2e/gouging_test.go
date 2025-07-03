package e2e

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	rhpv4 "go.sia.tech/core/rhp/v4"
	"go.sia.tech/renterd/v2/api"
	"go.sia.tech/renterd/v2/bus/client"
	"go.sia.tech/renterd/v2/internal/test"
	"lukechampine.com/frand"
)

func TestGouging(t *testing.T) {
	// create a new test cluster
	cluster := newTestCluster(t, clusterOptsDefault)
	defer cluster.Shutdown()

	b := cluster.Bus
	w := cluster.Worker
	tt := cluster.tt

	// add hosts with short price table validity to speed up test
	gs, err := b.GougingSettings(context.Background())
	tt.OK(err)
	gs.MinPriceTableValidity = api.DurationMS(10 * time.Second)
	tt.OK(cluster.bs.UpdateGougingSettings(context.Background(), gs))

	n := int(test.AutopilotConfig.Contracts.Amount)
	for i := 0; i < n; i++ {
		h := cluster.NewHost()
		settings := h.settings.RHP4Settings()
		settings.Prices.ValidUntil = time.Now().Add(time.Second).Add(time.Duration(gs.MinPriceTableValidity))
		h.UpdateSettings(settings)
		cluster.AddHost(h)
	}
	cluster.WaitForAccounts()

	// assert all hosts are usable
	h, err := b.UsableHosts(context.Background())
	tt.OK(err)
	if len(h) != n {
		t.Fatal("unexpected number of hosts")
	}

	// build a hosts map
	hostsMap := make(map[string]*Host)
	for _, h := range cluster.hosts {
		hostsMap[h.PublicKey().String()] = h
	}

	// generate random data
	data := make([]byte, rhpv4.SectorSize/12)
	frand.Read(data)

	// upload the data
	path := fmt.Sprintf("data_%v", len(data))
	tt.OKAll(w.UploadObject(context.Background(), bytes.NewReader(data), testBucket, path, api.UploadObjectOptions{}))

	// download the data
	var buffer bytes.Buffer
	tt.OK(w.DownloadObject(context.Background(), &buffer, testBucket, path, api.DownloadObjectOptions{}))
	if !bytes.Equal(data, buffer.Bytes()) {
		t.Fatal("unexpected data")
	}

	// fetch current host settings
	settings := cluster.hosts[0].settings.RHP4Settings()

	// update host settings
	updated := settings
	updated.Prices.StoragePrice = updated.Prices.StoragePrice.Mul64(2)
	cluster.hosts[0].UpdateSettings(updated)

	// update gouging settings
	gs = test.GougingSettings
	gs.MaxStoragePrice = settings.Prices.StoragePrice
	if err := b.UpdateGougingSettings(context.Background(), gs); err != nil {
		t.Fatal(err)
	}

	// make sure the price table expires so the worker is forced to fetch it
	// again, this is necessary for the host to be considered price gouging
	time.Sleep(time.Duration(gs.MinPriceTableValidity))

	// assert all but one host are usable
	h, err = b.UsableHosts(context.Background())
	tt.OK(err)
	if len(h) != n-1 {
		t.Fatal("unexpected number of hosts", len(h), n-1)
	}

	// upload some data - should fail
	tt.FailAll(w.UploadObject(context.Background(), bytes.NewReader(data), testBucket, path, api.UploadObjectOptions{}))

	// update all host settings so they're gouging
	for _, h := range cluster.hosts {
		settings := h.settings.RHP4Settings()
		settings.Prices.StoragePrice = settings.Prices.StoragePrice.Mul64(2)
		h.UpdateSettings(settings)

		// scan the host
		tt.OKAll(cluster.Bus.ScanHost(context.Background(), h.PublicKey(), time.Second))
	}
	time.Sleep(testWorkerCfg().CacheExpiry) // wait for cache to refresh

	// download the data - won't work since the hosts are not usable anymore
	tt.FailAll(w.DownloadObject(context.Background(), io.Discard, testBucket, path, api.DownloadObjectOptions{}))

	// try optimising gouging settings
	resp, err := cluster.Autopilot.EvaluateConfig(context.Background(), test.AutopilotConfig, gs, test.RedundancySettings)
	tt.OK(err)
	if resp.Recommendation == nil {
		t.Fatal("expected recommendation")
	} else if resp.Unusable.Gouging.Upload != 3 {
		t.Fatalf("expected 3 gouging errors, got %+v", resp.Unusable.Gouging.Upload)
	}

	// set optimised settings
	tt.OK(b.UpdateGougingSettings(context.Background(), resp.Recommendation.GougingSettings))

	// evaluate optimised settings
	resp, err = cluster.Autopilot.EvaluateConfig(context.Background(), test.AutopilotConfig, resp.Recommendation.GougingSettings, test.RedundancySettings)
	tt.OK(err)
	if resp.Recommendation != nil {
		t.Fatal("expected no recommendation")
	} else if resp.Usable != 3 {
		t.Fatalf("expected 3 usable hosts, got %v", resp.Usable)
	}

	// upload some data - should work now once contract maintenance is done
	tt.Retry(30, time.Second, func() error {
		_, err := w.UploadObject(context.Background(), bytes.NewReader(data), testBucket, path, api.UploadObjectOptions{})
		return err
	})
}

func TestHostMinVersion(t *testing.T) {
	// create a new test cluster
	n := int(test.AutopilotConfig.Contracts.Amount)
	cluster := newTestCluster(t, testClusterOptions{hosts: n})
	defer cluster.Shutdown()
	tt := cluster.tt

	if cluster.cm.Tip().Height >= cluster.network.HardforkV2.AllowHeight {
		t.Skip("only runs against v1 network")
	}

	// set min version to a high value
	hosts := test.AutopilotConfig.Hosts
	hosts.MinProtocolVersion = "99.99.99"
	tt.OK(cluster.Bus.UpdateAutopilotConfig(context.Background(), client.WithHostsConfig(hosts)))

	// contracts in set should drop to 0
	tt.Retry(100, 100*time.Millisecond, func() error {
		contracts, err := cluster.Bus.Contracts(context.Background(), api.ContractsOpts{FilterMode: api.ContractFilterModeGood})
		tt.OK(err)
		if len(contracts) != 0 {
			return fmt.Errorf("expected 0 contracts, got %v", len(contracts))
		}
		return nil
	})
}
