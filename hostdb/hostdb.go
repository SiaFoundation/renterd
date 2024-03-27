package hostdb

import (
	"time"

	"gitlab.com/NebulousLabs/encoding"
	rhpv2 "go.sia.tech/core/rhp/v2"
	rhpv3 "go.sia.tech/core/rhp/v3"
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

// Interactions contains metadata about a host's interactions.
type Interactions struct {
	TotalScans              uint64        `json:"totalScans"`
	LastScan                time.Time     `json:"lastScan"`
	LastScanSuccess         bool          `json:"lastScanSuccess"`
	LostSectors             uint64        `json:"lostSectors"`
	SecondToLastScanSuccess bool          `json:"secondToLastScanSuccess"`
	Uptime                  time.Duration `json:"uptime"`
	Downtime                time.Duration `json:"downtime"`

	SuccessfulInteractions float64 `json:"successfulInteractions"`
	FailedInteractions     float64 `json:"failedInteractions"`
}

type HostScan struct {
	HostKey    types.PublicKey `json:"hostKey"`
	Success    bool
	Timestamp  time.Time
	Settings   rhpv2.HostSettings
	PriceTable rhpv3.HostPriceTable
}

type PriceTableUpdate struct {
	HostKey    types.PublicKey `json:"hostKey"`
	Success    bool
	Timestamp  time.Time
	PriceTable HostPriceTable
}

// HostAddress contains the address of a specific host identified by a public
// key.
type HostAddress struct {
	PublicKey  types.PublicKey `json:"publicKey"`
	NetAddress string          `json:"netAddress"`
}

// A Host pairs a host's public key with a set of interactions.
type Host struct {
	KnownSince       time.Time          `json:"knownSince"`
	LastAnnouncement time.Time          `json:"lastAnnouncement"`
	PublicKey        types.PublicKey    `json:"publicKey"`
	NetAddress       string             `json:"netAddress"`
	PriceTable       HostPriceTable     `json:"priceTable"`
	Settings         rhpv2.HostSettings `json:"settings"`
	Interactions     Interactions       `json:"interactions"`
	Scanned          bool               `json:"scanned"`
}

// A HostPriceTable extends the host price table with its expiry.
type HostPriceTable struct {
	rhpv3.HostPriceTable
	Expiry time.Time `json:"expiry"`
}

// IsAnnounced returns whether the host has been announced.
func (h Host) IsAnnounced() bool {
	return !h.LastAnnouncement.IsZero()
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
