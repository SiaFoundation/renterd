package stores

import (
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"go.sia.tech/coreutils/syncer"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type (
	dbSyncerPeer struct {
		Model

		Address      string `gorm:"unique;index:idx_syncer_peers_address;NOT NULL"`
		FirstSeen    unixTimeMS
		LastConnect  unixTimeMS
		SyncedBlocks unsigned64
		SyncDuration unsigned64
	}

	dbSyncerBan struct {
		Model

		NetCidr    string     `gorm:"unique;index:idx_syncer_bans_net_cidr;NOT NULL"`
		Expiration unixTimeMS `gorm:"index:idx_syncer_bans_expiration;NOT NULL"`
		Reason     string
	}
)

var (
	// TODO: use syncer.ErrPeerNotFound when added
	ErrPeerNotFound = errors.New("peer not found")
)

var (
	_ syncer.PeerStore = (*SQLStore)(nil)
)

func (dbSyncerPeer) TableName() string {
	return "syncer_peers"
}

func (dbSyncerBan) TableName() string {
	return "syncer_bans"
}

func (p dbSyncerPeer) info() syncer.PeerInfo {
	return syncer.PeerInfo{
		Address:      p.Address,
		FirstSeen:    time.Time(p.FirstSeen),
		LastConnect:  time.Time(p.LastConnect),
		SyncedBlocks: uint64(p.SyncedBlocks),
		SyncDuration: time.Duration(p.SyncDuration),
	}
}

// AddPeer adds a peer to the store. If the peer already exists, nil should
// be returned.
func (s *SQLStore) AddPeer(addr string) error {
	return s.retryTransaction(func(tx *gorm.DB) error {
		res := tx.
			Clauses(clause.OnConflict{
				Columns:   []clause.Column{{Name: "address"}},
				DoNothing: true,
			}).
			Create(&dbSyncerPeer{Address: addr, FirstSeen: unixTimeMS(time.Now())})
		return res.Error
	})
}

// Peers returns the set of known peers.
func (s *SQLStore) Peers() ([]syncer.PeerInfo, error) {
	var peers []dbSyncerPeer
	if err := s.db.Model(&dbSyncerPeer{}).Find(&peers).Error; err != nil {
		return nil, err
	}

	infos := make([]syncer.PeerInfo, len(peers))
	for i, peer := range peers {
		infos[i] = peer.info()
	}
	return infos, nil
}

// UpdatePeerInfo updates the metadata for the specified peer. If the peer
// is not found, the error should be ErrPeerNotFound.
func (s *SQLStore) UpdatePeerInfo(addr string, fn func(*syncer.PeerInfo)) error {
	return s.retryTransaction(func(tx *gorm.DB) error {
		var peer dbSyncerPeer
		err := tx.
			Where("address = ?", addr).
			Take(&peer).
			Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return ErrPeerNotFound
		} else if err != nil {
			return err
		}

		update := peer.info()
		fn(&update)

		return tx.Model(&peer).Updates(map[string]interface{}{
			"last_connect":  unixTimeMS(update.LastConnect),
			"synced_blocks": unsigned64(update.SyncedBlocks),
			"sync_duration": unsigned64(update.SyncDuration),
		}).Error
	})
}

// Ban temporarily bans one or more IPs. The addr should either be a single
// IP with port (e.g. 1.2.3.4:5678) or a CIDR subnet (e.g. 1.2.3.4/16).
func (s *SQLStore) Ban(addr string, duration time.Duration, reason string) error {
	cidr, err := normalizePeer(addr)
	if err != nil {
		return err
	}

	return s.retryTransaction(func(tx *gorm.DB) error {
		res := tx.
			Clauses(clause.OnConflict{
				Columns: []clause.Column{{Name: "net_cidr"}},
				DoUpdates: clause.Assignments(map[string]interface{}{
					"expiration": unixTimeMS(time.Now().Add(duration)),
					"reason":     reason,
				}),
			}).
			Create(&dbSyncerBan{
				NetCidr:    cidr,
				Expiration: unixTimeMS(time.Now().Add(duration)),
				Reason:     reason,
			})
		return res.Error
	})

}

// Banned returns true, nil if the peer is banned.
func (s *SQLStore) Banned(addr string) (bool, error) {
	// normalize the address to a CIDR
	netCIDR, err := normalizePeer(addr)
	if err != nil {
		return false, err
	}

	// parse the subnet
	_, subnet, err := net.ParseCIDR(netCIDR)
	if err != nil {
		return false, err
	}

	// check all subnets from the given subnet to the max subnet length
	var maxMaskLen int
	if subnet.IP.To4() != nil {
		maxMaskLen = 32
	} else {
		maxMaskLen = 128
	}

	checkSubnets := make([]string, 0, maxMaskLen)
	for i := maxMaskLen; i > 0; i-- {
		_, subnet, err := net.ParseCIDR(subnet.IP.String() + "/" + strconv.Itoa(i))
		if err != nil {
			return false, err
		}
		checkSubnets = append(checkSubnets, subnet.String())
	}

	var ban dbSyncerBan
	if err := s.retryTransaction(func(tx *gorm.DB) error {
		return tx.
			Model(&dbSyncerBan{}).
			Where("net_cidr IN ?", checkSubnets).
			Order("expiration DESC").
			First(&ban).
			Error
	}); err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		return false, err
	}

	return time.Now().Before(time.Time(ban.Expiration)), nil
}

// normalizePeer normalizes a peer address to a CIDR subnet.
func normalizePeer(peer string) (string, error) {
	host, _, err := net.SplitHostPort(peer)
	if err != nil {
		host = peer
	}
	if strings.IndexByte(host, '/') != -1 {
		_, subnet, err := net.ParseCIDR(host)
		if err != nil {
			return "", fmt.Errorf("failed to parse CIDR: %w", err)
		}
		return subnet.String(), nil
	}

	ip := net.ParseIP(host)
	if ip == nil {
		return "", errors.New("invalid IP address")
	}

	var maskLen int
	if ip.To4() != nil {
		maskLen = 32
	} else {
		maskLen = 128
	}

	_, normalized, err := net.ParseCIDR(fmt.Sprintf("%s/%d", ip.String(), maskLen))
	if err != nil {
		panic("failed to parse CIDR")
	}
	return normalized.String(), nil
}
