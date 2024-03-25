package chain

import (
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/coreutils/wallet"
	"go.sia.tech/renterd/api"
)

type (
	Update struct {
		Index types.ChainIndex

		ContractUpdates     map[types.FileContractID]*ContractUpdate
		HostAnnouncements   map[types.PublicKey]HostAnnouncement
		WalletOutputUpdates map[types.Hash256]WalletOutputUpdate
		WalletEventUpdates  []WalletEventUpdate
	}

	ContractUpdate struct {
		ProofHeight *uint64
		Size        *uint64
		State       api.ContractState

		RevisionHeight *uint64
		RevisionNumber *uint64
	}

	HostAnnouncement struct {
		Announcement chain.HostAnnouncement
		BlockHeight  uint64
		BlockID      types.BlockID
		Timestamp    time.Time
	}

	WalletEventUpdate struct {
		Addition bool
		Event    wallet.Event
	}

	WalletOutputUpdate struct {
		Addition bool
		Element  wallet.SiacoinElement
		ID       types.Hash256
	}
)

// NewChainUpdate returns a new ChainUpdate.
func NewChainUpdate(index types.ChainIndex) *Update {
	return &Update{
		Index:               index,
		ContractUpdates:     make(map[types.FileContractID]*ContractUpdate),
		HostAnnouncements:   make(map[types.PublicKey]HostAnnouncement),
		WalletOutputUpdates: make(map[types.Hash256]WalletOutputUpdate),
	}
}

// ContractUpdate returns the ContractUpdate for the given file contract ID. If
// it doesn't exist, it is created.
func (cu *Update) ContractUpdate(fcid types.FileContractID, state api.ContractState) *ContractUpdate {
	_, ok := cu.ContractUpdates[fcid]
	if !ok {
		cu.ContractUpdates[fcid] = &ContractUpdate{State: state}
	}
	return cu.ContractUpdates[fcid]
}

// HasUpdates returns true if the ChainUpdate contains any updates.
func (cu *Update) HasUpdates() bool {
	return len(cu.ContractUpdates) > 0 ||
		len(cu.HostAnnouncements) > 0 ||
		len(cu.WalletOutputUpdates) > 0 ||
		len(cu.WalletEventUpdates) > 0
}

// AddEvents is called with all relevant events added in the update.
func (cu *Update) AddEvents(events []wallet.Event) error {
	for _, event := range events {
		cu.WalletEventUpdates = append(cu.WalletEventUpdates, WalletEventUpdate{
			Addition: true,
			Event:    event,
		})
	}
	return nil
}

// AddSiacoinElements is called with all new siacoin elements in the
// update. Ephemeral siacoin elements are not included.
func (cu *Update) AddSiacoinElements(ses []wallet.SiacoinElement) error {
	for _, se := range ses {
		cu.WalletOutputUpdates[se.ID] = WalletOutputUpdate{
			Addition: true,
			ID:       se.ID,
			Element:  se,
		}
	}
	return nil
}

// RemoveSiacoinElements is called with all siacoin elements that were
// spent in the update.
func (cu *Update) RemoveSiacoinElements(ids []types.SiacoinOutputID) error {
	for _, id := range ids {
		cu.WalletOutputUpdates[types.Hash256(id)] = WalletOutputUpdate{
			Addition: false,
			ID:       types.Hash256(id),
		}
	}
	return nil
}

// WalletStateElements returns all state elements in the database. It is used
// to update the proofs of all state elements affected by the update.
func (cu *Update) WalletStateElements() (elements []types.StateElement, _ error) {
	for id, el := range cu.WalletOutputUpdates {
		elements = append(elements, types.StateElement{
			ID:          id,
			LeafIndex:   el.Element.LeafIndex,
			MerkleProof: el.Element.MerkleProof,
		})
	}
	return
}

// UpdateStateElements updates the proofs of all state elements affected by the
// update.
func (cu *Update) UpdateStateElements(elements []types.StateElement) error {
	for _, se := range elements {
		curr := cu.WalletOutputUpdates[se.ID]
		curr.Element.MerkleProof = se.MerkleProof
		curr.Element.LeafIndex = se.LeafIndex
		cu.WalletOutputUpdates[se.ID] = curr
	}
	return nil
}

// RevertIndex is called with the chain index that is being reverted. Any events
// and siacoin elements that were created by the index should be removed.
func (cu *Update) RevertIndex(index types.ChainIndex) error {
	// remove any events that were added in the reverted block
	filtered := cu.WalletEventUpdates[:0]
	for i := range cu.WalletEventUpdates {
		if cu.WalletEventUpdates[i].Event.Index != index {
			filtered = append(filtered, cu.WalletEventUpdates[i])
		}
	}
	cu.WalletEventUpdates = filtered

	// remove any siacoin elements that were added in the reverted block
	for id, el := range cu.WalletOutputUpdates {
		if el.Element.Index == index {
			delete(cu.WalletOutputUpdates, id)
		}
	}

	return nil
}
