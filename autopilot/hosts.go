package autopilot

import (
	"encoding/json"
	"net"
	"time"

	"go.sia.tech/renterd/hostdb"
	"go.sia.tech/renterd/internal/consensus"
	rhpv2 "go.sia.tech/renterd/rhp/v2"
	"golang.org/x/crypto/blake2b"
)

type Host struct {
	hostdb.Host
}

func (h *Host) IsHost(host string) bool {
	if host == "" {
		return false
	}
	if h.PublicKey.String() == host {
		return true
	}
	if h.NetAddress() == host {
		return true
	}
	_, ipNet, err := net.ParseCIDR(host)
	if err != nil {
		return false
	}
	ip, err := net.ResolveIPAddr("ip", h.NetAddress())
	if err != nil {
		return false
	}
	return ipNet.Contains(ip.IP)
}

func (h *Host) IsOnline() bool {
	switch scans := h.LatestHostScans(2); len(scans) {
	case 0:
		return false
	case 1:
		return scans[0].Success
	default:
		return scans[0].Success || scans[1].Success
	}
}

// LastKnownSettings returns the host's last settings
func (h *Host) LastKnownSettings() (rhpv2.HostSettings, time.Time, bool) {
	for i := len(h.Interactions) - 1; i >= 0; i-- {
		if !h.Interactions[i].Success {
			continue
		}
		if h.Interactions[i].Type != "scan" {
			continue
		}
		var settings rhpv2.HostSettings
		if err := json.Unmarshal(h.Interactions[i].Result, &settings); err != nil {
			continue
		}
		return settings, h.Interactions[i].Timestamp, true
	}
	return rhpv2.HostSettings{}, time.Time{}, false
}

func (h *Host) LatestHostScans(limit int) (scans []hostdb.Interaction) {
	for i := len(h.Interactions) - 1; i >= 0; i-- {
		if h.Interactions[i].Type == "scan" {
			if scans = append(scans, h.Interactions[i]); len(scans) == limit {
				break
			}
		}
	}
	return
}

// TODO: deriving the renter key from the host key using the master key only
// works if we persist a hash of the renter's master key in the database and
// compare it on startup, otherwise there's no way of knowing the derived key is
// usuable
//
// TODO: instead of deriving a renter key use a randomly generated salt so we're
// not limited to one key per host
func (ap *Autopilot) deriveRenterKey(hostKey consensus.PublicKey) consensus.PrivateKey {
	seed := blake2b.Sum256(append(ap.masterKey[:], hostKey[:]...))
	pk := consensus.NewPrivateKeyFromSeed(seed[:])
	for i := range seed {
		seed[i] = 0
	}
	return pk
}
