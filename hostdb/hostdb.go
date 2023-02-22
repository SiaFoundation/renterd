package hostdb

import (
	"encoding/json"
	"time"

	"gitlab.com/NebulousLabs/encoding"
	rhpv2 "go.sia.tech/core/rhp/v2"
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

type ScanResult struct {
	Error    string
	Settings rhpv2.HostSettings `json:"settings,omitempty"`
}

const InteractionTypeScan = "scan"

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
			annHash := types.Hash256(crypto.HashObject(ha.HostAnnouncement)) // TODO
			if !hostKey.VerifyHash(annHash, ha.Signature) {
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
	Host      types.PublicKey
	Result    json.RawMessage
	Success   bool
	Timestamp time.Time
	Type      string
}

// HostAddress contains the address of a specific host identified by a public
// key.
type HostAddress struct {
	PublicKey  types.PublicKey `json:"public_key"`
	NetAddress string          `json:"net_address"`
}

// A Host pairs a host's public key with a set of interactions.
type Host struct {
	KnownSince   time.Time           `json:"knownSince"`
	PublicKey    types.PublicKey     `json:"public_key"`
	NetAddress   string              `json:"netAddress"`
	Settings     *rhpv2.HostSettings `json:"settings"`
	Interactions Interactions        `json:"interactions"`
}

// HostInfo extends the host type with a field indicating whether it is blocked or not.
type HostInfo struct {
	Host
	Blocked bool `json:"blocked"`
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
