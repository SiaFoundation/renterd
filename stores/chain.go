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

	logs []logEntry // only logged if tx was successfully committed
}

type logEntry struct {
	msg           string
	keysAndValues []interface{}
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
	u := &chainUpdateTx{tx: tx}
	if err := fn(u); err != nil {
		tx.Rollback()
		return err
	}

	// commit the changes
	err = tx.Commit().Error
	if err != nil {
		return err
	}

	// debug log
	l := s.logger.Named("chainupdate")
	for _, log := range u.logs {
		l.Debugw(log.msg, log.keysAndValues...)
	}

	return nil
}

// UpdateChainState process the given revert and apply updates.
func (s *SQLStore) UpdateChainState(reverted []chain.RevertUpdate, applied []chain.ApplyUpdate) error {
	return s.ProcessChainUpdate(func(tx chain.ChainUpdateTx) error {
		return wallet.UpdateChainState(tx, s.walletAddress, applied, reverted)
	})
}

// ApplyIndex is called with the chain index that is being applied. Any
// transactions and siacoin elements that were created by the index should be
// added and any siacoin elements that were spent should be removed.
func (u *chainUpdateTx) ApplyIndex(index types.ChainIndex, created, spent []types.SiacoinElement, events []wallet.Event) error {
	u.debug("applying index", "height", index.Height, "block_id", index.ID)

	// remove spent outputs
	for _, e := range spent {
		if res := u.tx.
			Where("output_id", hash256(e.ID)).
			Delete(&dbWalletOutput{}); res.Error != nil {
			return res.Error
		} else if res.RowsAffected != 1 {
			return fmt.Errorf("spent output with id %v not found ", e.ID)
		}
		u.debug(fmt.Sprintf("remove output %v", e.ID), "height", index.Height, "block_id", index.ID)
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
		u.debug(fmt.Sprintf("create output %v", e.ID), "height", index.Height, "block_id", index.ID)
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
		u.debug(fmt.Sprintf("create event %v", e.ID), "height", index.Height, "block_id", index.ID)
	}
	return nil
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

// RevertIndex is called with the chain index that is being reverted. Any
// transactions and siacoin elements that were created by the index should be
// removed.
func (u *chainUpdateTx) RevertIndex(index types.ChainIndex, removed, unspent []types.SiacoinElement) error {
	u.debug("reverting index", "height", index.Height, "block_id", index.ID)

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
		u.debug(fmt.Sprintf("recreate unspent output %v", e.ID), "height", index.Height, "block_id", index.ID)
	}

	// remove outputs created at the reverted index
	for _, e := range removed {
		if err := u.tx.
			Where("output_id", hash256(e.ID)).
			Delete(&dbWalletOutput{}).
			Error; err != nil {
			return err
		}
		u.debug(fmt.Sprintf("remove output %v", e.ID), "height", index.Height, "block_id", index.ID)
	}

	// remove events created at the reverted index
	res := u.tx.
		Model(&dbWalletEvent{}).
		Where("height = ? AND block_id = ?", index.Height, hash256(index.ID)).
		Delete(&dbWalletEvent{})
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected > 0 {
		u.debug(fmt.Sprintf("removed %d events", res.RowsAffected), "height", index.Height, "block_id", index.ID)
	}
	return nil
}

// UpdateChainIndex updates the chain index in the database.
func (u *chainUpdateTx) UpdateChainIndex(index types.ChainIndex) error {
	if err := u.tx.
		Model(&dbConsensusInfo{}).
		Where(&dbConsensusInfo{Model: Model{ID: consensusInfoID}}).
		Updates(map[string]interface{}{
			"height":   index.Height,
			"block_id": hash256(index.ID),
		}).
		Error; err != nil {
		return err
	}
	u.debug("updating index", "height", index.Height, "block_id", index.ID)
	return nil
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
	var msg string
	var update interface{}
	var c dbContract
	if err := u.tx.
		Model(&dbContract{}).
		Where("fcid", fileContractID(fcid)).
		Take(&c).Error; err == nil {
		bkp := c
		c.RevisionHeight = revisionHeight
		if isUpdatedRevision(c.RevisionNumber) {
			c.RevisionNumber = fmt.Sprint(revisionNumber)
			c.Size = size
		}
		msg = fmt.Sprintf("update contract, revision number %s -> %s, revision height %d -> %d, size %d -> %d", bkp.RevisionNumber, c.RevisionNumber, bkp.RevisionHeight, c.RevisionHeight, bkp.Size, c.Size)
		update = c
	} else if err == gorm.ErrRecordNotFound {
		// try archived contracts
		var ac dbArchivedContract
		if err := u.tx.
			Model(&dbArchivedContract{}).
			Where("fcid", fileContractID(fcid)).
			Take(&ac).Error; err == nil {
			bkp := ac
			ac.RevisionHeight = revisionHeight
			if isUpdatedRevision(ac.RevisionNumber) {
				ac.RevisionNumber = fmt.Sprint(revisionNumber)
				ac.Size = size
			}
			msg = fmt.Sprintf("update archived contract, revision number %s -> %s, revision height %d -> %d, size %d -> %d", bkp.RevisionNumber, ac.RevisionNumber, bkp.RevisionHeight, ac.RevisionHeight, bkp.Size, ac.Size)
			update = ac
		}
	}
	if update == nil {
		return nil
	}

	if err := u.tx.Save(update).Error; err != nil {
		return err
	}
	u.debug(msg, "fcid", fcid)
	return nil
}

// UpdateContractState updates the state of the contract with given fcid.
func (u *chainUpdateTx) UpdateContractState(fcid types.FileContractID, state api.ContractState) error {
	var cs contractState
	if err := cs.LoadString(string(state)); err != nil {
		return err
	}

	// try update contract
	res := u.tx.
		Model(&dbContract{}).
		Where("fcid", fileContractID(fcid)).
		Update("state", cs)
	if res.Error != nil {
		return res.Error
	} else if res.RowsAffected == 1 {
		u.debug(fmt.Sprintf("updated contract state to '%s'", state), "fcid", fcid)
		return nil
	}

	// try update archived contract
	res = u.tx.
		Model(&dbArchivedContract{}).
		Where("fcid", fileContractID(fcid)).
		Update("state", cs)
	if res.Error != nil {
		return res.Error
	} else if res.RowsAffected == 1 {
		u.debug(fmt.Sprintf("updated archived contract state to '%s'", state), "fcid", fcid)
		return nil
	}

	return nil
}

// UpdateContractProofHeight updates the proof height of the contract with given
// fcid.
func (u *chainUpdateTx) UpdateContractProofHeight(fcid types.FileContractID, proofHeight uint64) error {
	// try update contract
	res := u.tx.
		Model(&dbContract{}).
		Where("fcid", fileContractID(fcid)).
		Update("proof_height", proofHeight)
	if res.Error != nil {
		return res.Error
	} else if res.RowsAffected == 1 {
		u.debug(fmt.Sprintf("updated contract proof height to '%d'", proofHeight), "fcid", fcid)
		return nil
	}

	// try update archived contract
	res = u.tx.
		Model(&dbArchivedContract{}).
		Where("fcid", fileContractID(fcid)).
		Update("proof_height", proofHeight)
	if res.Error != nil {
		return res.Error
	} else if res.RowsAffected == 1 {
		u.debug(fmt.Sprintf("updated archived contract proof height to '%d'", proofHeight), "fcid", fcid)
		return nil
	}
	return nil
}

// UpdateFailedContracts marks active contract as failed if the current
// blockheight surposses their window_end.
func (u *chainUpdateTx) UpdateFailedContracts(blockHeight uint64) error {
	if res := u.tx.
		Model(&dbContract{}).
		Where("window_end <= ?", blockHeight).
		Where("state", contractStateActive).
		Update("state", contractStateFailed); res.Error != nil {
		return res.Error
	} else if res.RowsAffected > 0 {
		u.debug(fmt.Sprintf("marked %d active contracts as failed", res.RowsAffected), "window_end", blockHeight)
	}
	return nil
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

func (u *chainUpdateTx) debug(msg string, keysAndValues ...interface{}) {
	u.logs = append(u.logs, logEntry{msg: msg, keysAndValues: keysAndValues})
}
