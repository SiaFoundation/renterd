package hostdb

import (
	"bytes"
	"time"

	rhpv2 "go.sia.tech/core/rhp/v2"
	rhpv3 "go.sia.tech/core/rhp/v3"
	"go.sia.tech/core/types"
)

const (
	hostAnnouncementSpecifier = "HostAnnouncement"
)

// Announcement represents a host announcement in a given block.
type Announcement struct {
	Index      types.ChainIndex
	Timestamp  time.Time
	NetAddress string
}

type hostAnnouncement struct {
	Specifier  types.Specifier
	NetAddress string
	PublicKey  types.UnlockKey
	Signature  types.Signature
}

func (ha *hostAnnouncement) DecodeFrom(d *types.Decoder) {
	ha.Specifier.DecodeFrom(d)
	ha.NetAddress = d.ReadString()
	ha.PublicKey.DecodeFrom(d)
	ha.Signature.DecodeFrom(d)
}

func (ha hostAnnouncement) EncodeTo(e *types.Encoder) {
	ha.Specifier.EncodeTo(e)
	e.WriteString(ha.NetAddress)
	ha.PublicKey.EncodeTo(e)
	ha.Signature.EncodeTo(e)
}

func (ha hostAnnouncement) HostKey() types.PublicKey {
	var hk types.PublicKey
	copy(hk[:], ha.PublicKey.Key)
	return hk
}

func (ha hostAnnouncement) VerifySignature() bool {
	buf := new(bytes.Buffer)
	e := types.NewEncoder(buf)
	ha.Specifier.EncodeTo(e)
	e.WriteString(ha.NetAddress)
	ha.PublicKey.EncodeTo(e)
	e.Flush()
	annHash := types.HashBytes(buf.Bytes())
	return ha.HostKey().VerifyHash(annHash, ha.Signature)
}

// ForEachAnnouncement calls fn on each host announcement in a block.
func ForEachAnnouncement(b types.Block, ci types.ChainIndex, fn func(types.PublicKey, Announcement)) {
	for _, txn := range b.Transactions {
		for _, arb := range txn.ArbitraryData {
			// decode announcement
			var ha hostAnnouncement
			dec := types.NewBufDecoder(arb)
			ha.DecodeFrom(dec)
			if err := dec.Err(); err != nil {
				continue
			} else if ha.Specifier != types.NewSpecifier(hostAnnouncementSpecifier) {
				continue
			}
			// verify signature
			if !ha.VerifySignature() {
				continue
			}
			fn(ha.HostKey(), Announcement{
				Index:      ci,
				Timestamp:  b.Timestamp,
				NetAddress: string(ha.NetAddress),
			})
		}
	}
	for _, txn := range b.V2Transactions() {
		for _, att := range txn.Attestations {
			if att.Key != hostAnnouncementSpecifier {
				continue
			}
			fn(att.PublicKey, Announcement{
				Index:      ci,
				Timestamp:  b.Timestamp,
				NetAddress: string(att.Value),
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

// HostInfo extends the host type with a field indicating whether it is blocked or not.
type HostInfo struct {
	Host
	Blocked bool `json:"blocked"`
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
