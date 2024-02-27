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
	if testing.Short() {
		t.SkipNow()
	}

	// register start time
	start := time.Now()

	// enable pruning
	apCfg := test.AutopilotConfig
	apCfg.Contracts.Prune = true

	// create a test cluster
	cluster := newTestCluster(t, testClusterOptions{
		hosts:             test.RedundancySettings.TotalShards,
		autopilotSettings: &apCfg,
	})
	defer cluster.Shutdown()

	// convenience variables
	b := cluster.Bus
	w := cluster.Worker
	tt := cluster.tt

	// upload, download, delete
	data := frand.Bytes(rhpv2.SectorSize)
	tt.OKAll(w.UploadObject(context.Background(), bytes.NewReader(data), api.DefaultBucketName, "foo", api.UploadObjectOptions{}))
	tt.OK(w.DownloadObject(context.Background(), io.Discard, api.DefaultBucketName, "foo", api.DownloadObjectOptions{}))
	tt.OK(w.DeleteObject(context.Background(), api.DefaultBucketName, "foo", api.DeleteObjectOptions{}))

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

		// check contract set metrics
		csm, err := b.ContractSetMetrics(context.Background(), start, 10, time.Minute, api.ContractSetMetricsQueryOpts{})
		tt.OK(err)
		if len(csm) == 0 {
			return errors.New("no contract set metrics")
		}

		// check contract set metrics
		cscm, err := b.ContractSetChurnMetrics(context.Background(), start, 10, time.Minute, api.ContractSetChurnMetricsQueryOpts{})
		tt.OK(err)
		if len(cscm) == 0 {
			return errors.New("no contract set churn metrics")
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
