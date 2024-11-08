package e2e

import (
	"context"
	"strings"
	"testing"

	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/internal/test"
	"go.sia.tech/renterd/internal/utils"
)

func TestAutopilot(t *testing.T) {
	// create test cluster
	cluster := newTestCluster(t, clusterOptsDefault)
	defer cluster.Shutdown()
	tt := cluster.tt
	b := cluster.Bus

	// assert autopilot is enabled by default
	ap, err := b.Autopilot(context.Background())
	tt.OK(err)
	if !ap.Enabled {
		t.Fatal("autopilot should be enabled by default")
	}

	// assert hosts and contracts config are defaulted
	if ap.Contracts != test.AutopilotConfig.Contracts {
		t.Fatalf("contracts config should be defaulted, got %v", ap.Contracts)
	} else if ap.Hosts != test.AutopilotConfig.Hosts {
		t.Fatalf("hosts config should be defaulted, got %v", ap.Hosts)
	}

	// assert hosts config is validated
	hcfg := ap.Hosts
	hcfg.MaxDowntimeHours = 99*365*24 + 1 // exceed by one
	if err := b.UpdateHostsConfig(context.Background(), hcfg); !utils.IsErr(err, api.ErrMaxDowntimeHoursTooHigh) {
		t.Fatal("unexpected", err)
	}
	hcfg.MaxDowntimeHours = 99 * 365 * 24 // allowed max
	tt.OK(b.UpdateHostsConfig(context.Background(), hcfg))

	hcfg.MinProtocolVersion = "not a version"
	if err := b.UpdateHostsConfig(context.Background(), hcfg); !utils.IsErr(err, api.ErrInvalidReleaseVersion) {
		t.Fatal("unexpected")
	}

	// assert contracts config is validated
	ccfg := ap.Contracts
	ccfg.Period = 0 // invalid period
	if err := b.UpdateContractsConfig(context.Background(), ccfg); err == nil || !strings.Contains(err.Error(), "period must be greater than 0") {
		t.Fatal("unexpected", err)
	}
	ccfg.Period = 1      // valid period
	ccfg.RenewWindow = 0 // invalid renew window
	if err := b.UpdateContractsConfig(context.Background(), ccfg); err == nil || !strings.Contains(err.Error(), "renewWindow must be greater than 0") {
		t.Fatal("unexpected", err)
	}
	ccfg.RenewWindow = 1 // valid renew window
	if err := b.UpdateContractsConfig(context.Background(), ccfg); err != nil {
		t.Fatal(err)
	}

	// assert we can disable the autopilot
	tt.OK(b.EnableAutopilot(context.Background(), false))
	ap, err = b.Autopilot(context.Background())
	tt.OK(err)
	if ap.Enabled {
		t.Fatal("autopilot should be disabled")
	}
}
