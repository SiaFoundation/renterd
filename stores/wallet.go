package stores

import (
	"bytes"
	"math"
	"time"

	"gitlab.com/NebulousLabs/encoding"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/wallet"
	"gorm.io/gorm"
)

type (
	dbWalletEvent struct {
		Model

		// event
		EventID        hash256 `gorm:"unique;index;NOT NULL;size:32"`
		Inflow         currency
		Outflow        currency
		Transaction    types.Transaction `gorm:"serializer:json"`
		MaturityHeight uint64            `gorm:"index"`
		Source         string            `gorm:"index:idx_events_source"`
		Timestamp      int64             `gorm:"index:idx_events_timestamp"`

		// chain index
		Height  uint64  `gorm:"index"`
		BlockID hash256 `gorm:"size:32"`
	}

	dbWalletOutput struct {
		Model

		// siacoin element
		OutputID       hash256 `gorm:"unique;index;NOT NULL;size:32"`
		LeafIndex      uint64
		MerkleProof    merkleProof
		Value          currency
		Address        hash256 `gorm:"size:32"`
		MaturityHeight uint64  `gorm:"index"`

		// chain index
		Height  uint64  `gorm:"index"`
		BlockID hash256 `gorm:"size:32"`
	}

	outputChange struct {
		addition bool
		se       dbWalletOutput
	}

	eventChange struct {
		addition bool
		event    dbWalletEvent
	}
)

// TableName implements the gorm.Tabler interface.
func (dbWalletEvent) TableName() string { return "wallet_events" }

// TableName implements the gorm.Tabler interface.
func (dbWalletOutput) TableName() string { return "wallet_outputs" }

func (e dbWalletEvent) Index() types.ChainIndex {
	return types.ChainIndex{
		Height: e.Height,
		ID:     types.BlockID(e.BlockID),
	}
}

func (se dbWalletOutput) Index() types.ChainIndex {
	return types.ChainIndex{
		Height: se.Height,
		ID:     types.BlockID(se.BlockID),
	}
}

// Tip returns the consensus change ID and block height of the last wallet
// change.
func (s *SQLStore) Tip() (types.ChainIndex, error) {
	return s.cs.Tip(), nil
}

// UnspentSiacoinElements returns a list of all unspent siacoin outputs
func (s *SQLStore) UnspentSiacoinElements() ([]wallet.SiacoinElement, error) {
	var dbElems []dbWalletOutput
	if err := s.db.Find(&dbElems).Error; err != nil {
		return nil, err
	}

	elements := make([]wallet.SiacoinElement, len(dbElems))
	for i, el := range dbElems {
		elements[i] = wallet.SiacoinElement{
			SiacoinElement: types.SiacoinElement{
				StateElement: types.StateElement{
					ID:          types.Hash256(el.OutputID),
					LeafIndex:   el.LeafIndex,
					MerkleProof: el.MerkleProof,
				},
				MaturityHeight: el.MaturityHeight,
				SiacoinOutput: types.SiacoinOutput{
					Address: types.Address(el.Address),
					Value:   types.Currency(el.Value),
				},
			},
			Index: types.ChainIndex{
				Height: el.Height,
				ID:     types.BlockID(el.BlockID),
			},
			// TODO: Index missing
		}
	}
	return elements, nil
}

// WalletEvents returns a paginated list of events, ordered by maturity height,
// descending. If no more events are available, (nil, nil) is returned.
func (s *SQLStore) WalletEvents(offset, limit int) ([]wallet.Event, error) {
	if limit == 0 || limit == -1 {
		limit = math.MaxInt64
	}

	var dbEvents []dbWalletEvent
	err := s.db.Raw("SELECT * FROM events ORDER BY timestamp DESC LIMIT ? OFFSET ?",
		limit, offset).Scan(&dbEvents).
		Error
	if err != nil {
		return nil, err
	}

	events := make([]wallet.Event, len(dbEvents))
	for i, e := range dbEvents {
		events[i] = wallet.Event{
			ID: types.Hash256(e.EventID),
			Index: types.ChainIndex{
				Height: e.Height,
				ID:     types.BlockID(e.BlockID),
			},
			Inflow:         types.Currency(e.Inflow),
			Outflow:        types.Currency(e.Outflow),
			Transaction:    e.Transaction,
			Source:         wallet.EventSource(e.Source),
			MaturityHeight: e.MaturityHeight,
			Timestamp:      time.Unix(e.Timestamp, 0),
		}
	}
	return events, nil
}

// WalletEventCount returns the number of events relevant to the wallet.
func (s *SQLStore) WalletEventCount() (uint64, error) {
	var count int64
	if err := s.db.Model(&dbWalletEvent{}).Count(&count).Error; err != nil {
		return 0, err
	}
	return uint64(count), nil
}

func convertToCore(siad encoding.SiaMarshaler, core types.DecoderFrom) {
	var buf bytes.Buffer
	siad.MarshalSia(&buf)
	d := types.NewBufDecoder(buf.Bytes())
	core.DecodeFrom(d)
	if d.Err() != nil {
		panic(d.Err())
	}
}

func applyUnappliedOutputAdditions(tx *gorm.DB, sco dbWalletOutput) error {
	return tx.Create(&sco).Error
}

func applyUnappliedOutputRemovals(tx *gorm.DB, oid hash256) error {
	return tx.Where("output_id", oid).
		Delete(&dbWalletOutput{}).
		Error
}

func applyUnappliedEventAdditions(tx *gorm.DB, event dbWalletEvent) error {
	return tx.Create(&event).Error
}

func applyUnappliedEventRemovals(tx *gorm.DB, eventID hash256) error {
	return tx.Where("event_id", eventID).
		Delete(&dbWalletEvent{}).
		Error
}
