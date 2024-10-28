package e2e

import (
	"context"
	"fmt"
	"testing"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/internal/test"
)

func TestBlocklist(t *testing.T) {
	ctx := context.Background()

	// create a new test cluster
	cluster := newTestCluster(t, testClusterOptions{
		hosts: 3,
	})
	defer cluster.Shutdown()
	b := cluster.Bus
	tt := cluster.tt

	// fetch contracts
	opts := api.ContractsOpts{ContractSet: test.AutopilotConfig.Contracts.Set}
	contracts, err := b.Contracts(ctx, opts)
	tt.OK(err)
	if len(contracts) != 3 {
		t.Fatalf("unexpected number of contracts, %v != 3", len(contracts))
	}

	// add h1 and h2 to the allowlist
	hk1 := contracts[0].HostKey
	hk2 := contracts[1].HostKey
	hk3 := contracts[2].HostKey
	err = b.UpdateHostAllowlist(ctx, []types.PublicKey{hk1, hk2}, nil, false)
	tt.OK(err)

	// assert h3 is no longer in the contract set
	tt.Retry(100, 100*time.Millisecond, func() error {
		contracts, err := b.Contracts(ctx, opts)
		tt.OK(err)
		if len(contracts) != 2 {
			return fmt.Errorf("unexpected number of contracts in set '%v', %v != 2", opts.ContractSet, len(contracts))
		}
		for _, c := range contracts {
			if c.HostKey == hk3 {
				return fmt.Errorf("unexpected contract for host %v", hk3)
			}
		}
		return nil
	})

	// add h1 to the blocklist
	h1, err := b.Host(context.Background(), hk1)
	tt.OK(err)
	tt.OK(b.UpdateHostBlocklist(ctx, []string{h1.NetAddress}, nil, false))

	// assert h1 is no longer in the contract set
	tt.Retry(100, 100*time.Millisecond, func() error {
		contracts, err := b.Contracts(ctx, api.ContractsOpts{ContractSet: test.AutopilotConfig.Contracts.Set})
		tt.OK(err)
		if len(contracts) != 1 {
			return fmt.Errorf("unexpected number of contracts in set '%v', %v != 1", opts.ContractSet, len(contracts))
		}
		for _, c := range contracts {
			if c.HostKey == hk1 {
				return fmt.Errorf("unexpected contract for host %v", hk1)
			}
		}
		return nil
	})

	// clear the allowlist and blocklist and assert we have 3 contracts again
	tt.OK(b.UpdateHostAllowlist(ctx, nil, []types.PublicKey{hk1, hk2}, false))
	tt.OK(b.UpdateHostBlocklist(ctx, nil, []string{h1.NetAddress}, false))
	tt.Retry(100, 100*time.Millisecond, func() error {
		contracts, err := b.Contracts(ctx, opts)
		tt.OK(err)
		if len(contracts) != 3 {
			return fmt.Errorf("unexpected number of contracts in set '%v', %v != 3", opts.ContractSet, len(contracts))
		}
		return nil
	})

	// create a new host
	h := cluster.NewHost()

	// update blocklist to block just that host
	tt.OK(b.UpdateHostBlocklist(context.Background(), []string{h.RHPv2Addr()}, nil, false))

	// add the host
	cluster.AddHost(h)

	// try and fetch the host
	host, err := b.Host(context.Background(), h.PublicKey())
	tt.OK(err)

	// assert it's blocked
	if !host.Blocked {
		t.Fatal("expected host to be blocked")
	}

	// clear blocklist
	tt.OK(b.UpdateHostBlocklist(context.Background(), nil, nil, true))

	// try and fetch the host again
	host, err = b.Host(context.Background(), h.PublicKey())
	tt.OK(err)

	// assert it's no longer blocked
	if host.Blocked {
		t.Fatal("expected host not to be blocked")
	}

	// assert we have 4 hosts
	hosts, err := b.Hosts(context.Background(), api.HostOptions{})
	tt.OK(err)
	if len(hosts) != 4 {
		t.Fatal("unexpected number of hosts", len(hosts))
	}

	// create a new host
	h = cluster.NewHost()

	// update allowlist to allow just that host
	tt.OK(b.UpdateHostAllowlist(context.Background(), []types.PublicKey{h.PublicKey()}, nil, false))

	// add the host
	cluster.AddHost(h)

	// try and fetch the host
	host, err = b.Host(context.Background(), h.PublicKey())
	tt.OK(err)

	// assert it's not blocked
	if host.Blocked {
		t.Fatal("expected host to not be blocked")
	}

	// assert all others are blocked
	hosts, err = b.Hosts(context.Background(), api.HostOptions{})
	tt.OK(err)
	if len(hosts) != 1 {
		t.Fatal("unexpected number of hosts", len(hosts))
	}

	// clear allowlist
	tt.OK(b.UpdateHostAllowlist(context.Background(), nil, nil, true))

	// assert no hosts are blocked
	hosts, err = b.Hosts(context.Background(), api.HostOptions{})
	tt.OK(err)
	if len(hosts) != 5 {
		t.Fatal("unexpected number of hosts", len(hosts))
	}
}
