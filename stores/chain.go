package stores

import (
	"fmt"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/wallet"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/chain"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var (
	_ chain.ChainStore    = (*SQLStore)(nil)
	_ chain.ChainUpdateTx = (*chainUpdateTx)(nil)
)

// chainUpdateTx implements the ChainUpdateTx interface.
type chainUpdateTx struct {
	tx *gorm.DB
}

// ProcessChainUpdate returns a callback function that process a chain update
// inside a transaction.
func (s *SQLStore) ProcessChainUpdate(fn func(tx chain.ChainUpdateTx) error) (err error) {
	// begin a transaction
	tx := s.db.Begin()
	defer func() {
		if r := recover(); r != nil {
			tx.Rollback()
		}
	}()
	if err := tx.Error; err != nil {
		return err
	}

	// call the update function with the wrapped tx
	if err := fn(&chainUpdateTx{tx: tx}); err != nil {
		tx.Rollback()
		return err
	}

	// commit the changes
	return tx.Commit().Error
}

func (s *SQLStore) UpdateChainState(reverted []chain.RevertUpdate, applied []chain.ApplyUpdate) error {
	return s.ProcessChainUpdate(func(tx chain.ChainUpdateTx) error {
		return wallet.UpdateChainState(tx, s.walletAddress, applied, reverted)
	})
}

// ApplyIndex is called with the chain index that is being applied. Any
// transactions and siacoin elements that were created by the index should be
// added and any siacoin elements that were spent should be removed.
func (u *chainUpdateTx) ApplyIndex(index types.ChainIndex, created, spent []types.SiacoinElement, events []wallet.Event) error {
	// remove spent outputs
	for _, e := range spent {
		if res := u.tx.
			Where("output_id", hash256(e.ID)).
			Delete(&dbWalletOutput{}); res.Error != nil {
			return res.Error
		} else if res.RowsAffected != 1 {
			return fmt.Errorf("spent output with id %v not found ", e.ID)
		}
	}

	// create outputs
	for _, e := range created {
		if err := u.tx.
			Clauses(clause.OnConflict{
				DoNothing: true,
				Columns:   []clause.Column{{Name: "output_id"}},
			}).
			Create(&dbWalletOutput{
				OutputID:       hash256(e.ID),
				LeafIndex:      e.StateElement.LeafIndex,
				MerkleProof:    merkleProof{proof: e.StateElement.MerkleProof},
				Value:          currency(e.SiacoinOutput.Value),
				Address:        hash256(e.SiacoinOutput.Address),
				MaturityHeight: e.MaturityHeight,
			}).Error; err != nil {
			return nil
		}
	}

	// create events
	for _, e := range events {
		if err := u.tx.
			Clauses(clause.OnConflict{
				DoNothing: true,
				Columns:   []clause.Column{{Name: "event_id"}},
			}).
			Create(&dbWalletEvent{
				EventID:        hash256(e.ID),
				Inflow:         currency(e.Inflow),
				Outflow:        currency(e.Outflow),
				Transaction:    e.Transaction,
				MaturityHeight: e.MaturityHeight,
				Source:         string(e.Source),
				Timestamp:      e.Timestamp.Unix(),
				Height:         e.Index.Height,
				BlockID:        hash256(e.Index.ID),
			}).Error; err != nil {
			return err
		}
	}
	return nil
}

// Commit commits the updates to the database.
func (u *chainUpdateTx) Commit() error {
	return u.tx.Commit().Error
}

// Rollback rolls back the transaction
func (u *chainUpdateTx) Rollback() error {
	return u.tx.Rollback().Error
}

// ContractState returns the state of a file contract.
func (u *chainUpdateTx) ContractState(fcid types.FileContractID) (api.ContractState, error) {
	var state contractState
	err := u.tx.
		Select("state").
		Model(&dbContract{}).
		Where("fcid", fileContractID(fcid)).
		Scan(&state).
		Error

	if err == gorm.ErrRecordNotFound {
		err = u.tx.
			Select("state").
			Model(&dbArchivedContract{}).
			Where("fcid", fileContractID(fcid)).
			Scan(&state).
			Error
	}

	if err != nil {
		return "", err
	}
	return api.ContractState(state.String()), nil
}

// RemoveSiacoinElements is called with all siacoin elements that were spent in
// the update.
func (u *chainUpdateTx) RemoveSiacoinElements(ids []types.SiacoinOutputID) error {
	for _, id := range ids {
		if err := u.tx.
			Where("output_id", hash256(id)).
			Delete(&dbWalletOutput{}).
			Error; err != nil {
			return err
		}
	}
	return nil
}

// RevertIndex is called with the chain index that is being reverted. Any
// transactions and siacoin elements that were created by the index should be
// removed.
func (u *chainUpdateTx) RevertIndex(index types.ChainIndex, removed, unspent []types.SiacoinElement) error {
	// recreate unspent outputs
	for _, e := range unspent {
		if err := u.tx.
			Clauses(clause.OnConflict{
				DoNothing: true,
				Columns:   []clause.Column{{Name: "output_id"}},
			}).
			Create(&dbWalletOutput{
				OutputID:       hash256(e.ID),
				LeafIndex:      e.StateElement.LeafIndex,
				MerkleProof:    merkleProof{proof: e.StateElement.MerkleProof},
				Value:          currency(e.SiacoinOutput.Value),
				Address:        hash256(e.SiacoinOutput.Address),
				MaturityHeight: e.MaturityHeight,
			}).Error; err != nil {
			return nil
		}
	}

	// remove outputs created at the reverted index
	for _, e := range removed {
		if err := u.tx.
			Where("output_id", hash256(e.ID)).
			Delete(&dbWalletOutput{}).
			Error; err != nil {
			return err
		}
	}

	// remove events created at the reverted index
	return u.tx.
		Model(&dbWalletEvent{}).
		Where("height = ? AND block_id = ?", index.Height, hash256(index.ID)).
		Delete(&dbWalletEvent{}).
		Error
}

// UpdateChainIndex updates the chain index in the database.
func (u *chainUpdateTx) UpdateChainIndex(index types.ChainIndex) error {
	return u.tx.
		Model(&dbConsensusInfo{}).
		Where(&dbConsensusInfo{Model: Model{ID: consensusInfoID}}).
		Updates(map[string]interface{}{
			"height":   index.Height,
			"block_id": hash256(index.ID),
		}).
		Error
}

// UpdateContract updates the revision height, revision number, and size the
// contract with given fcid.
func (u *chainUpdateTx) UpdateContract(fcid types.FileContractID, revisionHeight, revisionNumber, size uint64) error {
	// isUpdatedRevision indicates whether the given revision number is greater
	// than the one currently set on the contract
	isUpdatedRevision := func(currRevStr string) bool {
		var currRev uint64
		_, _ = fmt.Sscan(currRevStr, &currRev)
		return revisionNumber > currRev
	}

	// update either active or archived contract
	var update interface{}
	var c dbContract
	if err := u.tx.
		Model(&dbContract{}).
		Where("fcid", fileContractID(fcid)).
		Take(&c).Error; err == nil {
		c.RevisionHeight = revisionHeight
		if isUpdatedRevision(c.RevisionNumber) {
			c.RevisionNumber = fmt.Sprint(revisionNumber)
			c.Size = size
		}
		update = c
	} else if err == gorm.ErrRecordNotFound {
		// try archived contracts
		var ac dbArchivedContract
		if err := u.tx.
			Model(&dbArchivedContract{}).
			Where("fcid", fileContractID(fcid)).
			Take(&ac).Error; err == nil {
			ac.RevisionHeight = revisionHeight
			if isUpdatedRevision(ac.RevisionNumber) {
				ac.RevisionNumber = fmt.Sprint(revisionNumber)
				ac.Size = size
			}
			update = ac
		}
	}
	if update == nil {
		return nil
	}

	return u.tx.Save(update).Error
}

// UpdateContractState updates the state of the contract with given fcid.
func (u *chainUpdateTx) UpdateContractState(fcid types.FileContractID, state api.ContractState) error {
	var cs contractState
	if err := cs.LoadString(string(state)); err != nil {
		return err
	}

	if err := u.tx.
		Model(&dbContract{}).
		Where("fcid", fileContractID(fcid)).
		Update("state", cs).
		Error; err != nil {
		return err
	}
	return u.tx.
		Model(&dbArchivedContract{}).
		Where("fcid", fileContractID(fcid)).
		Update("state", cs).
		Error
}

// UpdateContractProofHeight updates the proof height of the contract with given
// fcid.
func (u *chainUpdateTx) UpdateContractProofHeight(fcid types.FileContractID, proofHeight uint64) error {
	if err := u.tx.
		Model(&dbContract{}).
		Where("fcid", fileContractID(fcid)).
		Update("proof_height", proofHeight).
		Error; err != nil {
		return err
	}
	return u.tx.
		Model(&dbArchivedContract{}).
		Where("fcid", fileContractID(fcid)).
		Update("proof_height", proofHeight).
		Error
}

// UpdateFailedContracts marks active contract as failed if the current
// blockheight surposses their window_end.
func (u *chainUpdateTx) UpdateFailedContracts(blockHeight uint64) error {
	return u.tx.
		Model(&dbContract{}).
		Where("window_end <= ?", blockHeight).
		Where("state", contractStateActive).
		Update("state", contractStateFailed).
		Error
}

// UpdateHost creates the announcement and upserts the host in the database.
func (u *chainUpdateTx) UpdateHost(hk types.PublicKey, ha chain.HostAnnouncement, bh uint64, blockID types.BlockID, ts time.Time) error {
	// create the announcement
	if err := u.tx.Create(&dbAnnouncement{
		HostKey:     publicKey(hk),
		BlockHeight: bh,
		BlockID:     blockID.String(),
		NetAddress:  ha.NetAddress,
	}).Error; err != nil {
		return err
	}

	// create the host
	if err := u.tx.Create(&dbHost{
		PublicKey:        publicKey(hk),
		LastAnnouncement: ts.UTC(),
		NetAddress:       ha.NetAddress,
	}).Error; err != nil {
		return err
	}

	// fetch blocklists
	allowlist, blocklist, err := getBlocklists(u.tx)
	if err != nil {
		return fmt.Errorf("%w; failed to fetch blocklists", err)
	}

	// return early if there are no allowlist or blocklist entries
	if len(allowlist)+len(blocklist) == 0 {
		return nil
	}

	// update blocklist
	if err := updateBlocklist(u.tx, hk, allowlist, blocklist); err != nil {
		return fmt.Errorf("%w; failed to update blocklist for host %v", err, hk)
	}

	return nil
}

// UpdateStateElements updates the proofs of all state elements affected by the
// update.
func (u *chainUpdateTx) UpdateStateElements(elements []types.StateElement) error {
	if len(elements) == 0 {
		return nil
	}

	utxos, err := u.outputs()
	if err != nil {
		return err
	}

	lookup := make(map[types.Hash256]int)
	for i, utxo := range utxos {
		lookup[types.Hash256(utxo.OutputID)] = i
	}

	for _, el := range elements {
		if index, ok := lookup[el.ID]; ok {
			update := utxos[index]
			update.LeafIndex = el.LeafIndex
			update.MerkleProof = merkleProof{proof: el.MerkleProof}
			utxos[index] = update
		}
	}

	return u.tx.Save(utxos).Error
}

// WalletStateElements implements the ChainStore interface and returns all state
// elements in the database.
func (u *chainUpdateTx) WalletStateElements() ([]types.StateElement, error) {
	type row struct {
		ID          hash256
		LeafIndex   uint64
		MerkleProof merkleProof
	}
	var rows []row
	if err := u.tx.
		Model(&dbWalletOutput{}).
		Select("output_id AS id", "leaf_index", "merkle_proof").
		Find(&rows).
		Error; err != nil {
		return nil, err
	}
	elements := make([]types.StateElement, 0, len(rows))
	for _, r := range rows {
		elements = append(elements, types.StateElement{
			ID:          types.Hash256(r.ID),
			LeafIndex:   r.LeafIndex,
			MerkleProof: r.MerkleProof.proof,
		})
	}
	return elements, nil
}

func (u *chainUpdateTx) outputs() (outputs []dbWalletOutput, err error) {
	err = u.tx.
		Model(&dbWalletOutput{}).
		Find(&outputs).
		Error
	return
}
