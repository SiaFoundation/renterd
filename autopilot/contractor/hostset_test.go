package contractor

import (
	"testing"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"go.uber.org/zap"
)

func TestHostSet(t *testing.T) {
	hs := &hostSet{
		subnetToHostKey: make(map[string]string),
		logger:          zap.NewNop().Sugar(),
	}

	// Host with no subnets
	host1 := api.Host{
		PublicKey:         types.GeneratePrivateKey().PublicKey(),
		ResolvedAddresses: []string{},
	}
	if !hs.HasRedundantIP(host1) {
		t.Fatalf("Expected host with no subnets to be considered redundant")
	}

	// Host with more than 2 subnets
	host2 := api.Host{
		PublicKey:         types.GeneratePrivateKey().PublicKey(),
		ResolvedAddresses: []string{"192.168.1.1", "10.0.0.1", "172.16.0.1"},
	}
	if !hs.HasRedundantIP(host2) {
		t.Fatalf("Expected host with more than 2 subnets to be considered redundant")
	}

	// New host with unique subnet
	host3 := api.Host{
		PublicKey:         types.GeneratePrivateKey().PublicKey(),
		ResolvedAddresses: []string{"192.168.2.1"},
	}
	if hs.HasRedundantIP(host3) {
		t.Fatal("Expected new host with unique subnet to not be considered redundant")
	}
	hs.Add(host3)

	// New host with same subnet but different public key
	host4 := api.Host{
		PublicKey:         types.GeneratePrivateKey().PublicKey(),
		ResolvedAddresses: []string{"192.168.2.1"},
	}
	if !hs.HasRedundantIP(host4) {
		t.Fatal("Expected host with same subnet but different public key to be considered redundant")
	}

	// Same host from before
	if hs.HasRedundantIP(host3) {
		t.Fatal("Expected same host to not be considered redundant")
	}

	// Host with two valid subnets
	host5 := api.Host{
		PublicKey:         types.GeneratePrivateKey().PublicKey(),
		ResolvedAddresses: []string{"192.168.3.1", "10.0.0.1"},
	}
	if hs.HasRedundantIP(host5) {
		t.Fatal("Expected host with two valid subnets to not be considered redundant")
	}
	hs.Add(host5)

	// New host with one overlapping subnet
	host6 := api.Host{
		PublicKey:         types.GeneratePrivateKey().PublicKey(),
		ResolvedAddresses: []string{"10.0.0.1", "172.16.0.1"},
	}
	if !hs.HasRedundantIP(host6) {
		t.Fatal("Expected host with one overlapping subnet to be considered redundant")
	}
}
