package autopilot

import (
	"net"

	"go.sia.tech/renterd/hostdb"
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
	if h.NetAddress == host {
		return true
	}
	_, ipNet, err := net.ParseCIDR(host)
	if err != nil {
		return false
	}
	ip, err := net.ResolveIPAddr("ip", h.NetAddress)
	if err != nil {
		return false
	}
	return ipNet.Contains(ip.IP)
}
