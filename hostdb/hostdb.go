package hostdb

import (
	"time"

	"gitlab.com/NebulousLabs/encoding"
	"go.sia.tech/core/types"
	"go.sia.tech/siad/crypto"
	"go.sia.tech/siad/modules"
)

// Announcement represents a host announcement in a given block.
type Announcement struct {
	Index      types.ChainIndex
	Timestamp  time.Time
	NetAddress string
}

type hostAnnouncement struct {
	modules.HostAnnouncement
	Signature types.Signature
}

// ForEachAnnouncement calls fn on each host announcement in a block.
func ForEachAnnouncement(b types.Block, height uint64, fn func(types.PublicKey, Announcement)) {
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
			var hostKey types.PublicKey
			copy(hostKey[:], ha.PublicKey.Key)
			annHash := types.Hash256(crypto.HashObject(ha.HostAnnouncement))
			if !hostKey.VerifyHash(annHash, ha.Signature) {
				continue
			}

			// verify net address
			if ha.NetAddress == "" {
				continue
			}

			fn(hostKey, Announcement{
				Index: types.ChainIndex{
					Height: height,
					ID:     b.ID(),
				},
				Timestamp:  b.Timestamp,
				NetAddress: string(ha.NetAddress),
			})
		}
	}
}
