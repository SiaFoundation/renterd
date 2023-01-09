package testing

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"go.sia.tech/renterd/autopilot"
	"go.sia.tech/renterd/bus"
	"go.sia.tech/renterd/internal/consensus"
	rhpv2 "go.sia.tech/renterd/rhp/v2"
	"go.sia.tech/siad/node/api/client"
	"go.sia.tech/siad/siatest"
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
	w := cluster.Worker
	b := cluster.Bus

	// add hosts
	hosts, err := cluster.AddHosts(int(defaultRedundancy.TotalShards))
	if err != nil {
		t.Fatal(err)
	}

	// create helper to update host settings
	updateHostSetting := func(param client.HostParam, value interface{}) {
		for _, h := range hosts {
			if err := h.HostModifySettingPost(param, value); err != nil {
				t.Fatal(err)
			}
			if err := h.HostAnnouncePost(); err != nil {
				t.Fatal(err)
			}
			time.Sleep(time.Second)
		}
	}

	// wait for contracts to form
	if _, err := cluster.WaitForContracts(); err != nil {
		t.Fatal(err)
	}

	// do small upload and download asserting a working contract set
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

	// fetch the host settings of the first host so we can reset
	settings, err := hostSettings(b, hosts[0])
	if err != nil {
		t.Fatal(err)
	}

	// force download gouging error - assert it fails, reset and assert it succeeds again
	updateHostSetting("mindownloadbandwidthprice", types.SiacoinPrecision)
	if err := w.DownloadObject(&buffer, name); err == nil {
		t.Fatal("expected download to fail")
	}
	updateHostSetting("mindownloadbandwidthprice", settings.DownloadBandwidthPrice)
	if err := w.DownloadObject(&buffer, name); err != nil {
		t.Fatal(err)
	}

	// force upload gouging error - assert it fails, reset and assert it succeeds again
	updateHostSetting("minuploadbandwidthprice", types.SiacoinPrecision)
	if err := w.UploadObject(bytes.NewReader(data), name+"2"); err == nil {
		t.Fatal("expected download to fail")
	}
	updateHostSetting("minuploadbandwidthprice", settings.UploadBandwidthPrice)

	if err := w.UploadObject(bytes.NewReader(data), name+"2"); err != nil {
		t.Fatal(err)
	}

	// force renew gouging error
	updateHostSetting("mincontractprice", types.SiacoinPrecision)

	// mine until we're at the renew window
	if err = cluster.MineToRenewWindow(); err == nil {
		t.Fatal(err)
	}

	// mine blocks until we've past it
	cfg, err := cluster.Autopilot.Config()
	if err != nil {
		t.Fatal(err)
	}
	if err := cluster.MineBlocks(int(cfg.Contracts.RenewWindow)); err == nil {
		t.Fatal(err)
	}

	// assert there's no active contracts
	contracts, err := b.ActiveContracts()
	if err != nil {
		t.Fatal(err)
	}
	if len(contracts) != 0 {
		t.Fatal("expected no active contracts")
	}
}

func hostSettings(b *bus.Client, h *siatest.TestNode) (rhpv2.HostSettings, error) {
	hpk, err := h.HostPublicKey()
	if err != nil {
		return rhpv2.HostSettings{}, err
	}
	host, err := b.Host(consensus.PublicKey(hpk.ToPublicKey()))
	if err != nil {
		return rhpv2.HostSettings{}, err
	}
	settings, _, found := (&autopilot.Host{Host: host}).LastKnownSettings()
	if !found {
		return rhpv2.HostSettings{}, errors.New("settings not found")
	}
	return settings, nil
}
