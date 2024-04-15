package contractor

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/internal/utils"
	"go.uber.org/zap"
)

var (
	ipv4Localhost = net.IP{127, 0, 0, 1}
	ipv6Localhost = net.IP{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1}
)

type testResolver struct {
	addr map[string][]net.IPAddr
	err  error
}

func (r *testResolver) LookupIPAddr(ctx context.Context, host string) ([]net.IPAddr, error) {
	// return error if set
	if err := r.err; err != nil {
		r.err = nil
		return nil, err
	}
	// return IP addr if set
	if addrs, ok := r.addr[host]; ok {
		return addrs, nil
	}
	return nil, nil
}

func (r *testResolver) setNextErr(err error)                    { r.err = err }
func (r *testResolver) setAddr(host string, addrs []net.IPAddr) { r.addr[host] = addrs }

func newTestResolver() *testResolver {
	return &testResolver{addr: make(map[string][]net.IPAddr)}
}

func newTestIPResolver(r resolver) *ipResolver {
	ipr := newIPResolver(context.Background(), time.Minute, zap.NewNop().Sugar())
	ipr.resolver = r
	return ipr
}

func newTestIPFilter(r resolver) *ipFilter {
	return &ipFilter{
		subnetToHostKey: make(map[string]string),
		resolver:        newTestIPResolver(r),
		logger:          zap.NewNop().Sugar(),
	}
}

func TestIPResolver(t *testing.T) {
	r := newTestResolver()
	ipr := newTestIPResolver(r)

	// test lookup error
	r.setNextErr(errors.New("unknown error"))
	if _, err := ipr.lookup("example.com:1234"); !utils.IsErr(err, errors.New("unknown error")) {
		t.Fatal("unexpected error", err)
	}

	// test IO timeout - no cache entry
	r.setNextErr(ErrIOTimeout)
	if _, err := ipr.lookup("example.com:1234"); !utils.IsErr(err, ErrIOTimeout) {
		t.Fatal("unexpected error", err)
	}

	// test IO timeout - expired cache entry
	ipr.cache["example.com:1234"] = ipCacheEntry{subnets: []string{"a"}}
	r.setNextErr(ErrIOTimeout)
	if _, err := ipr.lookup("example.com:1234"); !utils.IsErr(err, ErrIOTimeout) {
		t.Fatal("unexpected error", err)
	}

	// test IO timeout - live cache entry
	ipr.cache["example.com:1234"] = ipCacheEntry{created: time.Now(), subnets: []string{"a"}}
	r.setNextErr(ErrIOTimeout)
	if subnets, err := ipr.lookup("example.com:1234"); err != nil {
		t.Fatal("unexpected error", err)
	} else if len(subnets) != 1 || subnets[0] != "a" {
		t.Fatal("unexpected subnets", subnets)
	}

	// test too many addresses - more than two
	r.setAddr("example.com", []net.IPAddr{{}, {}, {}})
	if _, err := ipr.lookup("example.com:1234"); !utils.IsErr(err, errTooManyAddresses) {
		t.Fatal("unexpected error", err)
	}

	// test too many addresses - two of the same type
	r.setAddr("example.com", []net.IPAddr{{IP: net.IPv4(1, 2, 3, 4)}, {IP: net.IPv4(1, 2, 3, 4)}})
	if _, err := ipr.lookup("example.com:1234"); !utils.IsErr(err, errTooManyAddresses) {
		t.Fatal("unexpected error", err)
	}

	// test invalid addresses
	r.setAddr("example.com", []net.IPAddr{{IP: ipv4Localhost}, {IP: net.IP{127, 0, 0, 2}}})
	if _, err := ipr.lookup("example.com:1234"); !utils.IsErr(err, errTooManyAddresses) {
		t.Fatal("unexpected error", err)
	}

	// test valid addresses
	r.setAddr("example.com", []net.IPAddr{{IP: ipv4Localhost}, {IP: ipv6Localhost}})
	if subnets, err := ipr.lookup("example.com:1234"); err != nil {
		t.Fatal("unexpected error", err)
	} else if len(subnets) != 2 || subnets[0] != "127.0.0.0/24" || subnets[1] != "::/32" {
		t.Fatal("unexpected subnets", subnets)
	}
}

func TestIPFilter(t *testing.T) {
	r := newTestResolver()
	r.setAddr("host1.com", []net.IPAddr{{IP: net.IP{192, 168, 0, 1}}})
	r.setAddr("host2.com", []net.IPAddr{{IP: net.IP{192, 168, 1, 1}}})
	r.setAddr("host3.com", []net.IPAddr{{IP: net.IP{192, 168, 2, 1}}})
	ipf := newTestIPFilter(r)

	// add 3 hosts - unique IPs
	r1 := ipf.IsRedundantIP("host1.com:1234", types.PublicKey{1})
	r2 := ipf.IsRedundantIP("host2.com:1234", types.PublicKey{2})
	r3 := ipf.IsRedundantIP("host3.com:1234", types.PublicKey{3})
	if r1 || r2 || r3 {
		t.Fatal("unexpected result", r1, r2, r3)
	}

	// try add 4th host - redundant IP
	r.setAddr("host4.com", []net.IPAddr{{IP: net.IP{192, 168, 0, 12}}})
	if redundant := ipf.IsRedundantIP("host4.com:1234", types.PublicKey{4}); !redundant {
		t.Fatal("unexpected result", redundant)
	}

	// add 4th host - unique IP - 2 subnets
	r.setAddr("host4.com", []net.IPAddr{{IP: net.IP{192, 168, 3, 1}}, {IP: net.ParseIP("2001:0db8:85a3::8a2e:0370:7334")}})
	if redundant := ipf.IsRedundantIP("host4.com:1234", types.PublicKey{4}); redundant {
		t.Fatal("unexpected result", redundant)
	}

	// try add 5th host - redundant IP based on the IPv6 subnet from host4
	r.setAddr("host5.com", []net.IPAddr{{IP: net.ParseIP("2001:0db8:85b3::8a2e:0370:7335")}})
	if redundant := ipf.IsRedundantIP("host5.com:1234", types.PublicKey{5}); !redundant {
		t.Fatal("unexpected result", redundant)
	}

	// add 5th host - unique IP
	r.setAddr("host5.com", []net.IPAddr{{IP: net.ParseIP("2001:0db9:85b3::8a2e:0370:7335")}})
	if redundant := ipf.IsRedundantIP("host5.com:1234", types.PublicKey{5}); redundant {
		t.Fatal("unexpected result", redundant)
	}
}
