package contractor

import (
	"testing"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"go.uber.org/zap"
)

func TestHostSet(t *testing.T) {
	hs := newHostSet(zap.NewNop().Sugar())

	// Host with no subnets
	host1 := api.Host{
		PublicKey:         types.GeneratePrivateKey().PublicKey(),
		NetAddress:        "",
		V2SiamuxAddresses: []string{},
	}
	if hs.HasRedundantIP(host1) {
		t.Fatalf("Expected host with no subnets to not be redundant")
	}

	// Host with more than 2 subnets
	host2 := api.Host{
		PublicKey:         types.GeneratePrivateKey().PublicKey(),
		NetAddress:        "1.1.1.1:1111",
		V2SiamuxAddresses: []string{"2.2.2.2:2222", "3.3.3.3:3333"},
	}
	if !hs.HasRedundantIP(host2) {
		t.Fatalf("Expected host with more than 2 subnets to be considered redundant")
	}

	// New host with unique subnet
	host3 := api.Host{
		PublicKey:         types.GeneratePrivateKey().PublicKey(),
		NetAddress:        "",
		V2SiamuxAddresses: []string{"4.4.4.4:4444"},
	}
	if hs.HasRedundantIP(host3) {
		t.Fatal("Expected new host with unique subnet to not be considered redundant")
	}
	hs.Add(host3)

	// New host with same subnet but different public key
	host4 := api.Host{
		PublicKey:         types.GeneratePrivateKey().PublicKey(),
		NetAddress:        "",
		V2SiamuxAddresses: []string{"4.4.4.4:4444"},
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
		NetAddress:        "",
		V2SiamuxAddresses: []string{"5.5.5.5:5555", "[2001:0db8:85a3:0000:0000:8a2e:0370:7334]:5555"},
	}
	if hs.HasRedundantIP(host5) {
		t.Fatal("Expected host with two valid subnets to not be considered redundant")
	}
	hs.Add(host5)

	// New host with one overlapping subnet
	host6 := api.Host{
		PublicKey:         types.GeneratePrivateKey().PublicKey(),
		NetAddress:        "",
		V2SiamuxAddresses: []string{"6.6.6.6:6666", "7.7.7.7:7777"},
	}
	if !hs.HasRedundantIP(host6) {
		t.Fatal("Expected host with one overlapping subnet to be considered redundant")
	}
	host7 := api.Host{
		PublicKey:         types.GeneratePrivateKey().PublicKey(),
		NetAddress:        "6.6.6.6:6666",
		V2SiamuxAddresses: []string{"8.8.8.8:8888"},
	}
	if !hs.HasRedundantIP(host7) {
		t.Fatal("Expected host with one overlapping subnet to be considered redundant")
	}
}
