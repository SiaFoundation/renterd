package testing

import (
	"context"
	"fmt"
	"testing"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
)

func TestBlocklist(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	ctx := context.Background()

	// create a new test cluster
	cluster, err := newTestCluster(t.TempDir(), newTestLogger())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := cluster.Shutdown(ctx); err != nil {
			t.Fatal(err)
		}
	}()
	b := cluster.Bus

	// add hosts
	_, err = cluster.AddHostsBlocking(3)
	if err != nil {
		t.Fatal(err)
	}

	// wait until we have 3 contracts in the set
	var contracts []api.ContractMetadata
	if err := Retry(5, time.Second, func() (err error) {
		contracts, err = b.ContractSetContracts(ctx, testAutopilotConfig.Contracts.Set)
		if err != nil {
			t.Fatal(err)
		} else if len(contracts) != 3 {
			err = fmt.Errorf("unexpected number of contracts, %v != 3", len(contracts))
			return
		}
		return
	}); err != nil {
		t.Fatal(err)
	}

	// add h1 and h2 to the allowlist
	hk1 := contracts[0].HostKey
	hk2 := contracts[1].HostKey
	hk3 := contracts[2].HostKey
	err = b.UpdateHostAllowlist(ctx, []types.PublicKey{hk1, hk2}, nil, false)
	if err != nil {
		t.Fatal(err)
	}

	// assert h3 is no longer in the contract set
	if err := Retry(5, time.Second, func() error {
		contracts, err := b.ContractSetContracts(ctx, testAutopilotConfig.Contracts.Set)
		if err != nil {
			t.Fatal(err)
		} else if len(contracts) != 2 {
			return fmt.Errorf("unexpected number of contracts, %v != 2", len(contracts))
		}
		for _, c := range contracts {
			if c.HostKey == hk3 {
				return fmt.Errorf("unexpected contract for host %v", hk3)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	// add h1 to the blocklist
	h1, err := b.Host(context.Background(), hk1)
	if err != nil {
		t.Fatal(err)
	}
	err = b.UpdateHostBlocklist(ctx, []string{h1.NetAddress}, nil, false)
	if err != nil {
		t.Fatal(err)
	}

	// assert h1 is no longer in the contract set
	if err := Retry(5, time.Second, func() error {
		contracts, err := b.ContractSetContracts(ctx, testAutopilotConfig.Contracts.Set)
		if err != nil {
			t.Fatal(err)
		} else if len(contracts) != 1 {
			return fmt.Errorf("unexpected number of contracts, %v != 1", len(contracts))
		}
		for _, c := range contracts {
			if c.HostKey == hk1 {
				return fmt.Errorf("unexpected contract for host %v", hk1)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	// clear the allowlist and blocklist and assert we have 3 contracts again
	err = b.UpdateHostAllowlist(ctx, nil, []types.PublicKey{hk1, hk2}, false)
	if err != nil {
		t.Fatal(err)
	}
	err = b.UpdateHostBlocklist(ctx, nil, []string{h1.NetAddress}, false)
	if err != nil {
		t.Fatal(err)
	}
	if err := Retry(5, time.Second, func() error {
		contracts, err := b.ContractSetContracts(ctx, testAutopilotConfig.Contracts.Set)
		if err != nil {
			t.Fatal(err)
		} else if len(contracts) != 3 {
			return fmt.Errorf("unexpected number of contracts, %v != 3", len(contracts))
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	// create a new host
	h, err := cluster.NewHost()
	if err != nil {
		t.Fatal(err)
	}

	// update blocklist to block just that host
	err = b.UpdateHostBlocklist(context.Background(), []string{h.RHPv2Addr()}, nil, false)
	if err != nil {
		t.Fatal(err)
	}

	// add the host
	err = cluster.AddHost(h)
	if err != nil {
		t.Fatal(err)
	}

	// try and fetch the host
	host, err := b.Host(context.Background(), h.PublicKey())
	if err != nil {
		t.Fatal(err)
	}

	// assert it's blocked
	if !host.Blocked {
		t.Fatal("expected host to be blocked")
	}

	// clear blocklist
	err = b.UpdateHostBlocklist(context.Background(), nil, nil, true)
	if err != nil {
		t.Fatal(err)
	}

	// try and fetch the host again
	host, err = b.Host(context.Background(), h.PublicKey())
	if err != nil {
		t.Fatal(err)
	}

	// assert it's no longer blocked
	if host.Blocked {
		t.Fatal("expected host not to be blocked")
	}

	// assert we have 4 hosts
	hosts, err := b.Hosts(context.Background(), 0, -1)
	if err != nil {
		t.Fatal(err)
	}
	if len(hosts) != 4 {
		t.Fatal("unexpected number of hosts", len(hosts))
	}

	// create a new host
	h, err = cluster.NewHost()
	if err != nil {
		t.Fatal(err)
	}

	// update allowlist to allow just that host
	err = b.UpdateHostAllowlist(context.Background(), []types.PublicKey{h.PublicKey()}, nil, false)
	if err != nil {
		t.Fatal(err)
	}

	// add the host
	err = cluster.AddHost(h)
	if err != nil {
		t.Fatal(err)
	}

	// try and fetch the host
	host, err = b.Host(context.Background(), h.PublicKey())
	if err != nil {
		t.Fatal(err)
	}

	// assert it's not blocked
	if host.Blocked {
		t.Fatal("expected host to not be blocked")
	}

	// assert all others are blocked
	hosts, err = b.Hosts(context.Background(), 0, -1)
	if err != nil {
		t.Fatal(err)
	}
	if len(hosts) != 1 {
		t.Fatal("unexpected number of hosts", len(hosts))
	}

	// clear allowlist
	err = b.UpdateHostAllowlist(context.Background(), nil, nil, true)
	if err != nil {
		t.Fatal(err)
	}

	// assert no hosts are blocked
	hosts, err = b.Hosts(context.Background(), 0, -1)
	if err != nil {
		t.Fatal(err)
	}
	if len(hosts) != 5 {
		t.Fatal("unexpected number of hosts", len(hosts))
	}
}
