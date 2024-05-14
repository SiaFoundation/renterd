package stores

import (
	"errors"
	"math"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/wallet"
	"gorm.io/gorm"
)

var (
	_ wallet.SingleAddressStore = (*SQLStore)(nil)
)

type (
	dbWalletEvent struct {
		Model

		// event
		EventID        hash256 `gorm:"unique;index:idx_events_event_id;NOT NULL;size:32"`
		Inflow         currency
		Outflow        currency
		Transaction    types.Transaction `gorm:"serializer:json"`
		MaturityHeight uint64            `gorm:"index:idx_wallet_events_maturity_height"`
		Source         string            `gorm:"index:idx_wallet_events_source"`
		Timestamp      int64             `gorm:"index:idx_wallet_events_timestamp"`

		// chain index
		Height  uint64  `gorm:"index:idx_wallet_events_height"`
		BlockID hash256 `gorm:"size:32"`
	}

	dbWalletOutput struct {
		Model

		// siacoin element
		OutputID       hash256 `gorm:"unique;index:idx_wallet_outputs_output_id;NOT NULL;size:32"`
		LeafIndex      uint64
		MerkleProof    merkleProof
		Value          currency
		Address        hash256 `gorm:"size:32"`
		MaturityHeight uint64  `gorm:"index:idx_wallet_outputs_maturity_height"`
	}
)

// TableName implements the gorm.Tabler interface.
func (dbWalletEvent) TableName() string {
	return "wallet_events"
}

// TableName implements the gorm.Tabler interface.
func (dbWalletOutput) TableName() string {
	return "wallet_outputs"
}

func (e dbWalletEvent) Index() types.ChainIndex {
	return types.ChainIndex{
		Height: e.Height,
		ID:     types.BlockID(e.BlockID),
	}
}

// Tip returns the consensus change ID and block height of the last wallet
// change.
func (s *SQLStore) Tip() (types.ChainIndex, error) {
	var cs dbConsensusInfo
	if err := s.db.
		Model(&dbConsensusInfo{}).
		First(&cs).Error; errors.Is(err, gorm.ErrRecordNotFound) {
		return types.ChainIndex{}, nil
	} else if err != nil {
		return types.ChainIndex{}, err
	}
	return types.ChainIndex{
		Height: cs.Height,
		ID:     types.BlockID(cs.BlockID),
	}, nil
}

// UnspentSiacoinElements returns a list of all unspent siacoin outputs
func (s *SQLStore) UnspentSiacoinElements() ([]types.SiacoinElement, error) {
	var dbElems []dbWalletOutput
	if err := s.db.Find(&dbElems).Error; err != nil {
		return nil, err
	}

	elements := make([]types.SiacoinElement, len(dbElems))
	for i, el := range dbElems {
		elements[i] = types.SiacoinElement{
			StateElement: types.StateElement{
				ID:          types.Hash256(el.OutputID),
				LeafIndex:   el.LeafIndex,
				MerkleProof: el.MerkleProof.proof,
			},
			MaturityHeight: el.MaturityHeight,
			SiacoinOutput: types.SiacoinOutput{
				Address: types.Address(el.Address),
				Value:   types.Currency(el.Value),
			},
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
	err := s.db.Raw("SELECT * FROM wallet_events ORDER BY timestamp DESC LIMIT ? OFFSET ?",
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
