package hostdb

import (
	"encoding/json"
	"time"

	"gitlab.com/NebulousLabs/encoding"
	"go.sia.tech/renterd/internal/consensus"
	"go.sia.tech/renterd/rhp/v2"
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

type ScanResult struct {
	Error    string
	Settings rhp.HostSettings `json:"settings,omitempty"`
}

const InteractionTypeScan = "scan"

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

// Interactions contains metadata about a host's interactions.
type Interactions struct {
	TotalScans              uint64
	LastScan                time.Time
	LastScanSuccess         bool
	SecondToLastScanSuccess bool
	Uptime                  time.Duration
	Downtime                time.Duration

	SuccessfulInteractions float64
	FailedInteractions     float64
}

type Interaction struct {
	Result    json.RawMessage
	Success   bool
	Timestamp time.Time
	Type      string
}

// A Host pairs a host's public key with a set of interactions.
type Host struct {
	KnownSince   time.Time
	PublicKey    consensus.PublicKey
	NetAddress   string
	Settings     *rhp.HostSettings
	Interactions Interactions
}

// IsOnline returns whether a host is considered online.
func (h Host) IsOnline() bool {
	if h.Interactions.TotalScans == 0 {
		return false
	} else if h.Interactions.TotalScans == 1 {
		return h.Interactions.LastScanSuccess
	}
	return h.Interactions.LastScanSuccess || h.Interactions.SecondToLastScanSuccess
}
