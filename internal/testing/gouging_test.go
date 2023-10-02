package testing

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	rhpv2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
	"go.uber.org/zap/zapcore"
	"lukechampine.com/frand"
)

func TestGouging(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	// create a new test cluster
	cluster, err := newTestCluster(t.TempDir(), newTestLoggerCustom(zapcore.DebugLevel))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := cluster.Shutdown(context.Background()); err != nil {
			t.Fatal(err)
		}
	}()

	cfg := testAutopilotConfig.Contracts
	b := cluster.Bus
	w := cluster.Worker

	// add hosts
	hosts, err := cluster.AddHostsBlocking(int(cfg.Amount))
	if err != nil {
		t.Fatal(err)
	}

	// build a hosts map
	hostsMap := make(map[string]*Host)
	for _, h := range hosts {
		hostsMap[h.PublicKey().String()] = h
	}

	// wait until we have a full contract set
	if err := cluster.WaitForContractSet(cfg.Set, int(cfg.Amount)); err != nil {
		t.Fatal(err)
	}

	// wait until accounts are ready and funded
	if _, err := cluster.WaitForAccounts(); err != nil {
		t.Fatal(err)
	}

	// upload and download some data, asserting we have a working contract set
	data := make([]byte, rhpv2.SectorSize/12)
	if _, err := frand.Read(data); err != nil {
		t.Fatal(err)
	}

	// upload the data
	name := fmt.Sprintf("data_%v", len(data))
	if _, err := w.UploadObject(context.Background(), bytes.NewReader(data), name); err != nil {
		t.Fatal(err)
	}

	// download the data
	var buffer bytes.Buffer
	if err := w.DownloadObject(context.Background(), &buffer, name); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(data, buffer.Bytes()) {
		t.Fatal("unexpected data")
	}

	// fetch current contract set
	contracts, err := b.ContractSetContracts(context.Background(), cfg.Set)
	if err != nil {
		t.Fatal(err)
	}

	// update the host settings so it's gouging
	hk := contracts[0].HostKey
	host := hostsMap[hk.String()]
	settings := host.settings.Settings()
	settings.IngressPrice = types.Siacoins(1)
	settings.EgressPrice = types.Siacoins(1)
	settings.ContractPrice = types.Siacoins(11)
	if err := host.UpdateSettings(settings); err != nil {
		t.Fatal(err)
	}

	// make sure the price table expires so the worker is forced to fetch it
	// again, this is necessary for the host to be considered price gouging
	time.Sleep(defaultHostSettings.PriceTableValidity)

	// upload some data - should fail
	if _, err := w.UploadObject(context.Background(), bytes.NewReader(data), name); err == nil {
		t.Fatal("expected upload to fail")
	}

	// update all host settings so they're gouging
	for _, h := range hosts {
		settings := h.settings.Settings()
		settings.EgressPrice = types.Siacoins(1)
		if err := h.UpdateSettings(settings); err != nil {
			t.Fatal(err)
		}
	}

	// make sure the price table expires so the worker is forced to fetch it
	// again, this is necessary for the host to be considered price gouging
	time.Sleep(defaultHostSettings.PriceTableValidity)

	// download the data - should fail
	buffer.Reset()
	if err := w.DownloadObject(context.Background(), &buffer, name); err == nil {
		t.Fatal("expected download to fail")
	}
}
