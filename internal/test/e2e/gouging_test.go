package e2e

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	rhpv2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/internal/test"
	"go.uber.org/zap/zapcore"
	"lukechampine.com/frand"
)

func TestGouging(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	// create a new test cluster
	cluster := newTestCluster(t, testClusterOptions{
		logger: newTestLoggerCustom(zapcore.ErrorLevel),
	})
	defer cluster.Shutdown()

	cfg := test.AutopilotConfig.Contracts
	b := cluster.Bus
	w := cluster.Worker
	tt := cluster.tt

	// mine enough blocks for the current period to become > period
	cluster.MineBlocks(int(cfg.Period) * 2)

	// add hosts
	tt.OKAll(cluster.AddHostsBlocking(int(test.AutopilotConfig.Contracts.Amount)))
	cluster.WaitForAccounts()

	// build a hosts map
	hostsMap := make(map[string]*Host)
	for _, h := range cluster.hosts {
		hostsMap[h.PublicKey().String()] = h
	}

	// upload and download some data, asserting we have a working contract set
	data := make([]byte, rhpv2.SectorSize/12)
	tt.OKAll(frand.Read(data))

	// upload the data
	path := fmt.Sprintf("data_%v", len(data))
	tt.OKAll(w.UploadObject(context.Background(), bytes.NewReader(data), api.DefaultBucketName, path, api.UploadObjectOptions{}))

	// download the data
	var buffer bytes.Buffer
	tt.OK(w.DownloadObject(context.Background(), &buffer, api.DefaultBucketName, path, api.DownloadObjectOptions{}))
	if !bytes.Equal(data, buffer.Bytes()) {
		t.Fatal("unexpected data")
	}

	// update the gouging settings to limit the max storage price to 100H
	gs := test.GougingSettings
	gs.MaxStoragePrice = types.NewCurrency64(100)
	if err := b.UpdateSetting(context.Background(), api.SettingGouging, gs); err != nil {
		t.Fatal(err)
	}
	// fetch current contract set
	contracts, err := b.Contracts(context.Background(), api.ContractsOpts{ContractSet: cfg.Set})
	tt.OK(err)

	// update one host's settings so it's gouging
	hk := contracts[0].HostKey
	host := hostsMap[hk.String()]
	settings := host.settings.Settings()
	settings.StoragePrice = types.NewCurrency64(101) // gouging
	tt.OK(host.UpdateSettings(settings))

	// make sure the price table expires so the worker is forced to fetch it
	// again, this is necessary for the host to be considered price gouging
	time.Sleep(defaultHostSettings.PriceTableValidity)

	// upload some data - should fail
	tt.FailAll(w.UploadObject(context.Background(), bytes.NewReader(data), api.DefaultBucketName, path, api.UploadObjectOptions{}))

	// update all host settings so they're gouging
	for _, h := range cluster.hosts {
		settings := h.settings.Settings()
		settings.StoragePrice = types.NewCurrency64(101)
		if err := h.UpdateSettings(settings); err != nil {
			t.Fatal(err)
		}
	}

	// make sure the price table expires so the worker is forced to fetch it
	// again, this is necessary for the host to be considered price gouging
	time.Sleep(defaultHostSettings.PriceTableValidity)

	// download the data - should still work
	tt.OKAll(w.DownloadObject(context.Background(), io.Discard, api.DefaultBucketName, path, api.DownloadObjectOptions{}))

	// try optimising gouging settings
	resp, err := cluster.Autopilot.EvaluateConfig(context.Background(), test.AutopilotConfig, gs, test.RedundancySettings)
	tt.OK(err)
	if resp.Recommendation == nil {
		t.Fatal("expected recommendation")
	} else if resp.Unusable.Gouging.Gouging != 3 {
		t.Fatalf("expected 3 gouging errors, got %v", resp.Unusable.Gouging)
	}

	// set optimised settings
	tt.OK(b.UpdateSetting(context.Background(), api.SettingGouging, resp.Recommendation.GougingSettings))

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
		_, err := w.UploadObject(context.Background(), bytes.NewReader(data), api.DefaultBucketName, path, api.UploadObjectOptions{})
		return err
	})
}

func TestHostMinVersion(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	// create a new test cluster
	cluster := newTestCluster(t, testClusterOptions{
		hosts: int(test.AutopilotConfig.Contracts.Amount),
	})
	defer cluster.Shutdown()
	tt := cluster.tt

	// set min version to a high value
	cfg := test.AutopilotConfig
	cfg.Hosts.MinProtocolVersion = "99.99.99"
	cluster.UpdateAutopilotConfig(context.Background(), cfg)

	// contracts in set should drop to 0
	tt.Retry(100, 100*time.Millisecond, func() error {
		contracts, err := cluster.Bus.Contracts(context.Background(), api.ContractsOpts{
			ContractSet: test.AutopilotConfig.Contracts.Set,
		})
		tt.OK(err)
		if len(contracts) != 0 {
			return fmt.Errorf("expected 0 contracts, got %v", len(contracts))
		}
		return nil
	})
}
