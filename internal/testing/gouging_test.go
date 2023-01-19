package testing

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"go.sia.tech/renterd/internal/consensus"
	rhpv2 "go.sia.tech/renterd/rhp/v2"
	"go.sia.tech/siad/node/api/client"
	"go.sia.tech/siad/types"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"lukechampine.com/frand"
)

func TestGouging(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	// create a new test cluster
	cluster, err := newTestCluster(t.TempDir(), zap.New(zapcore.NewNopCore()))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := cluster.Shutdown(context.Background()); err != nil {
			t.Fatal(err)
		}
	}()

	cfg := defaultAutopilotConfig.Contracts
	b := cluster.Bus
	w := cluster.Worker

	// add hosts
	hosts, err := cluster.AddHosts(int(cfg.Hosts))
	if err != nil {
		t.Fatal(err)
	}

	// build a hosts map
	hostsmap := make(map[string]*TestNode)
	for _, h := range hosts {
		hostsmap[h.HostKey().String()] = h
	}

	// helper that waits until the contract set is ready
	t.Helper()
	waitForContractSet := func() error {
		return Retry(30, time.Second, func() error {
			if contracts, err := b.Contracts("autopilot"); err != nil {
				t.Fatal(err)
			} else if len(contracts) != int(cfg.Hosts) {
				return fmt.Errorf("contract set not ready yet, %v!=%v", len(contracts), int(cfg.Hosts))
			}
			return nil
		})
	}

	// helper that waits untail a certain host is removed from the contract set
	t.Helper()
	waitForHostRemoval := func(hk consensus.PublicKey) error {
		return Retry(30, time.Second, func() error {
			if contracts, err := b.Contracts("autopilot"); err != nil {
				t.Fatal(err)
			} else {
				for _, c := range contracts {
					if c.HostKey == hk {
						return errors.New("host still in contract set")
					}
				}
			}
			return nil
		})
	}

	// wait until we have a full contract set
	if err := waitForContractSet(); err != nil {
		t.Fatal(err)
	}

	// upload and download some data, asserting we have a working contract set
	data := make([]byte, rhpv2.SectorSize/12)
	if _, err := frand.Read(data); err != nil {
		t.Fatal(err)
	}

	// upload the data
	name := fmt.Sprintf("data_%v", len(data))
	if err := w.UploadObject(bytes.NewReader(data), name); err != nil {
		t.Fatal(err)
	}

	// download the data
	var buffer bytes.Buffer
	if err := w.DownloadObject(&buffer, name); err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		param client.HostParam
		value types.Currency
	}{
		{"mindownloadbandwidthprice", types.SiacoinPrecision},
		{"minuploadbandwidthprice", types.SiacoinPrecision},
		{"mincontractprice", types.SiacoinPrecision.Mul64(10)},
	}

	for _, c := range cases {
		// fetch current contract set
		contracts, err := b.Contracts("autopilot")
		if err != nil {
			t.Fatal(err)
		}

		// update the host settings so it's gouging
		hk := contracts[0].HostKey
		if err := hostsmap[hk.String()].HostModifySettingPost(c.param, c.value); err != nil {
			t.Fatal(err)
		}

		// assert it was removed from the contract set
		if err := waitForHostRemoval(hk); err != nil {
			t.Fatal(err)
		}
	}

	// upload some data - should fail
	if err := w.UploadObject(bytes.NewReader(data), name); err == nil {
		t.Fatal("expected upload to fail")
	}

	// update all host settings so they're gouging
	for _, h := range hosts {
		if err := h.HostModifySettingPost("mindownloadbandwidthprice", types.SiacoinPrecision); err != nil {
			t.Fatal(err)
		}
	}

	// download the data - should fail
	if err := w.DownloadObject(&buffer, name); err == nil {
		t.Fatal("expected download to fail")
	}
}
