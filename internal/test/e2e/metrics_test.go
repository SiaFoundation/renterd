package e2e

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
	"time"

	rhpv2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/internal/test"
	"lukechampine.com/frand"
)

func TestMetrics(t *testing.T) {
	// register start time
	start := time.Now()

	// enable pruning
	apCfg := test.AutopilotConfig
	apCfg.Contracts.Prune = true

	// create a test cluster
	cluster := newTestCluster(t, testClusterOptions{
		hosts:           test.RedundancySettings.TotalShards,
		autopilotConfig: &apCfg,
	})
	defer cluster.Shutdown()

	// convenience variables
	b := cluster.Bus
	w := cluster.Worker
	tt := cluster.tt

	// upload, download, delete
	data := frand.Bytes(rhpv2.SectorSize)
	tt.OKAll(w.UploadObject(context.Background(), bytes.NewReader(data), testBucket, "foo", api.UploadObjectOptions{}))
	tt.OK(w.DownloadObject(context.Background(), io.Discard, testBucket, "foo", api.DownloadObjectOptions{}))
	tt.OK(w.DeleteObject(context.Background(), testBucket, "foo"))

	tt.Retry(30, time.Second, func() (err error) {
		defer func() {
			if err != nil {
				cluster.MineBlocks(1)
			}
		}()

		// check contract metrics
		cm, err := b.ContractMetrics(context.Background(), start, 10, time.Minute, api.ContractMetricsQueryOpts{})
		tt.OK(err)
		if len(cm) == 0 {
			return errors.New("no contract metrics")
		}

		// check contract prune metrics
		cpm, err := b.ContractPruneMetrics(context.Background(), start, 10, time.Minute, api.ContractPruneMetricsQueryOpts{})
		tt.OK(err)
		if len(cpm) == 0 {
			return errors.New("no contract prune metrics")
		}

		// check wallet metrics
		wm, err := b.WalletMetrics(context.Background(), start, 10, time.Minute, api.WalletMetricsQueryOpts{})
		tt.OK(err)
		if len(wm) == 0 {
			return errors.New("no wallet metrics")
		}
		return nil
	})
}
