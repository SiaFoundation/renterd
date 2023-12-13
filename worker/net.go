package worker

import (
	"context"
	"fmt"
	"net"
)

var privateSubnets []*net.IPNet

func init() {
	for _, subnet := range []string{
		"10.0.0.0/8",
		"172.16.0.0/12",
		"192.168.0.0/16",
		"100.64.0.0/10",
	} {
		_, subnet, err := net.ParseCIDR(subnet)
		if err != nil {
			panic(fmt.Sprintf("failed to parse subnet: %v", err))
		}
		privateSubnets = append(privateSubnets, subnet)
	}
}

func isPrivateIP(addr net.IP) bool {
	if addr.IsLoopback() || addr.IsLinkLocalUnicast() || addr.IsLinkLocalMulticast() {
		return true
	}

	for _, block := range privateSubnets {
		if block.Contains(addr) {
			return true
		}
	}
	return false
}

func dial(ctx context.Context, hostIP string) (net.Conn, error) {
	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", hostIP)
	return conn, err
}
