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
	cluster := newTestCluster(t, testClusterOptions{
		logger: newTestLoggerCustom(zapcore.DebugLevel),
	})
	defer cluster.Shutdown()

	cfg := testAutopilotConfig.Contracts
	b := cluster.Bus
	w := cluster.Worker
	tt := cluster.tt

	// add hosts
	hosts := cluster.AddHostsBlocking(int(cfg.Amount))

	// build a hosts map
	hostsMap := make(map[string]*Host)
	for _, h := range hosts {
		hostsMap[h.PublicKey().String()] = h
	}

	// wait until we have a full contract set
	cluster.WaitForContractSet(cfg.Set, int(cfg.Amount))

	// wait until accounts are ready and funded
	cluster.WaitForAccounts()

	// upload and download some data, asserting we have a working contract set
	data := make([]byte, rhpv2.SectorSize/12)
	tt.OKAll(frand.Read(data))

	// upload the data
	name := fmt.Sprintf("data_%v", len(data))
	tt.OKAll(w.UploadObject(context.Background(), bytes.NewReader(data), name))

	// download the data
	var buffer bytes.Buffer
	tt.OK(w.DownloadObject(context.Background(), &buffer, name))
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
	tt.OK(host.UpdateSettings(settings))

	// make sure the price table expires so the worker is forced to fetch it
	// again, this is necessary for the host to be considered price gouging
	time.Sleep(defaultHostSettings.PriceTableValidity)

	// upload some data - should fail
	tt.OKAll(w.UploadObject(context.Background(), bytes.NewReader(data), name))

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
