package hostdb

import (
	"bytes"
	"time"

	rhpv2 "go.sia.tech/core/rhp/v2"
	rhpv3 "go.sia.tech/core/rhp/v3"
	"go.sia.tech/core/types"
)

const AnnouncementSpecifier = "HostAnnouncement"

// Announcement represents a host announcement in a given block.
type Announcement struct {
	Specifier  types.Specifier
	NetAddress string
	PublicKey  types.UnlockKey
	Signature  types.Signature
}

func (a *Announcement) DecodeFrom(d *types.Decoder) {
	a.Specifier.DecodeFrom(d)
	a.NetAddress = d.ReadString()
	a.PublicKey.DecodeFrom(d)
	a.Signature.DecodeFrom(d)
}

func (a Announcement) EncodeTo(e *types.Encoder) {
	a.Specifier.EncodeTo(e)
	e.WriteString(a.NetAddress)
	a.PublicKey.EncodeTo(e)
	a.Signature.EncodeTo(e)
}

func (a Announcement) HostKey() types.PublicKey {
	var hk types.PublicKey
	copy(hk[:], a.PublicKey.Key)
	return hk
}

func (a Announcement) VerifySignature() bool {
	buf := new(bytes.Buffer)
	e := types.NewEncoder(buf)
	a.Specifier.EncodeTo(e)
	e.WriteString(a.NetAddress)
	a.PublicKey.EncodeTo(e)
	e.Flush()
	annHash := types.HashBytes(buf.Bytes())
	return a.HostKey().VerifyHash(annHash, a.Signature)
}

// ForEachAnnouncement calls fn on each host announcement in a block.
func ForEachAnnouncement(b types.Block, fn func(Announcement)) {
	for _, txn := range b.Transactions {
		for _, arb := range txn.ArbitraryData {
			// decode announcement
			var ha Announcement
			dec := types.NewBufDecoder(arb)
			ha.DecodeFrom(dec)
			if err := dec.Err(); err != nil {
				continue
			} else if ha.Specifier != types.NewSpecifier(AnnouncementSpecifier) {
				continue
			}
			// verify signature
			if !ha.VerifySignature() {
				continue
			}
			fn(ha)
		}
	}
	for _, txn := range b.V2Transactions() {
		for _, att := range txn.Attestations {
			if att.Key != AnnouncementSpecifier {
				continue
			}
			fn(Announcement{
				Specifier:  types.NewSpecifier(AnnouncementSpecifier),
				NetAddress: string(att.Value),
				PublicKey:  att.PublicKey.UnlockKey(),
				Signature:  att.Signature,
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
