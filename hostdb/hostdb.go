package hostdb

import (
	"encoding/json"
	"time"

	"gitlab.com/NebulousLabs/encoding"
	"go.sia.tech/renterd/internal/consensus"
	"go.sia.tech/siad/crypto"
	"go.sia.tech/siad/modules"
	"go.sia.tech/siad/types"
)

// Announcement represents a host announcement in a given block.
type Announcement struct {
	Index      consensus.ChainIndex
	Timestamp  time.Time
	NetAddress string
}

type hostAnnouncement struct {
	modules.HostAnnouncement
	Signature consensus.Signature
}

// ForEachAnnouncement calls fn on each host announcement in a block.
func ForEachAnnouncement(b types.Block, height types.BlockHeight, fn func(consensus.PublicKey, Announcement)) {
	for _, txn := range b.Transactions {
		for _, arb := range txn.ArbitraryData {
			// decode announcement
			var ha hostAnnouncement
			if err := encoding.Unmarshal(arb, &ha); err != nil {
				continue
			} else if ha.Specifier != modules.PrefixHostAnnouncement {
				continue
			}
			// verify signature
			var hostKey consensus.PublicKey
			copy(hostKey[:], ha.PublicKey.Key)
			annHash := consensus.Hash256(crypto.HashObject(ha.HostAnnouncement))
			if !hostKey.VerifyHash(annHash, ha.Signature) {
				continue
			}

			fn(hostKey, Announcement{
				Index: consensus.ChainIndex{
					Height: uint64(height),
					ID:     consensus.BlockID(b.ID()),
				},
				Timestamp:  time.Unix(int64(b.Timestamp), 0),
				NetAddress: string(ha.NetAddress),
			})
		}
	}
}

// Interaction represents an interaction with a host at a given time.
type Interaction struct {
	Timestamp time.Time
	Type      string
	Result    json.RawMessage
}

// A Host pairs a host's public key with a set of interactions.
type Host struct {
	PublicKey     consensus.PublicKey
	Announcements []Announcement
	Interactions  []Interaction
}

// NetAddress returns the host's last announced NetAddress, if available.
func (h *Host) NetAddress() string {
	if len(h.Announcements) == 0 {
		return ""
	}
	return h.Announcements[len(h.Announcements)-1].NetAddress
}
