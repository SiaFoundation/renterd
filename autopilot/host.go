package autopilot

import (
	"encoding/json"
	"net"
	"time"

	"go.sia.tech/renterd/hostdb"
	rhpv2 "go.sia.tech/renterd/rhp/v2"
	"go.sia.tech/renterd/worker"
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
	scans := h.LatestHostScans(2)
	if len(scans) == 0 {
		return false
	}
	for _, s := range scans {
		if worker.IsSuccessfulInteraction(s) {
			return true
		}
	}
	return false
}

// LastKnownSettings returns the host's last settings
func (h *Host) LastKnownSettings() (rhpv2.HostSettings, time.Time, bool) {
	for i := len(h.Interactions) - 1; i >= 0; i-- {
		if h.Interactions[i].Type != "scan" {
			continue
		}
		var sr worker.ScanResult
		if err := json.Unmarshal(h.Interactions[i].Result, &sr); err != nil {
			continue
		}
		if sr.Error != "" {
			continue
		}
		return sr.Settings, h.Interactions[i].Timestamp, true
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
