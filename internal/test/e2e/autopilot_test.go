package e2e

import (
	"context"
	"strings"
	"testing"

	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/bus/client"
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
	ap, err := b.AutopilotConfig(context.Background())
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

	// assert h config is validated
	h := ap.Hosts
	h.MaxDowntimeHours = 99*365*24 + 1 // exceed by one
	if err := b.UpdateAutopilotConfig(context.Background(), client.WithHostsConfig(h)); !utils.IsErr(err, api.ErrMaxDowntimeHoursTooHigh) {
		t.Fatal("unexpected", err)
	}
	h.MaxDowntimeHours = 99 * 365 * 24 // allowed max
	tt.OK(b.UpdateAutopilotConfig(context.Background(), client.WithHostsConfig(h)))

	h.MinProtocolVersion = "not a version"
	if err := b.UpdateAutopilotConfig(context.Background(), client.WithHostsConfig(h)); !utils.IsErr(err, api.ErrInvalidReleaseVersion) {
		t.Fatal("unexpected")
	}

	// assert c config is validated
	c := ap.Contracts
	c.Period = 0 // invalid period
	if err := b.UpdateAutopilotConfig(context.Background(), client.WithContractsConfig(c)); err == nil || !strings.Contains(err.Error(), "period must be greater than 0") {
		t.Fatal("unexpected", err)
	}
	c.Period = 1      // valid period
	c.RenewWindow = 0 // invalid renew window
	if err := b.UpdateAutopilotConfig(context.Background(), client.WithContractsConfig(c)); err == nil || !strings.Contains(err.Error(), "renewWindow must be greater than 0") {
		t.Fatal("unexpected", err)
	}
	c.RenewWindow = 1 // valid renew window
	if err := b.UpdateAutopilotConfig(context.Background(), client.WithContractsConfig(c)); err != nil {
		t.Fatal(err)
	}

	// assert we can disable the autopilot
	tt.OK(b.UpdateAutopilotConfig(context.Background(), client.WithAutopilotEnabled(false)))
	ap, err = b.AutopilotConfig(context.Background())
	tt.OK(err)
	if ap.Enabled {
		t.Fatal("autopilot should be disabled")
	}
}
