package sql

import (
	"context"
	dsql "database/sql"
	"errors"
	"fmt"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/wallet"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/internal/sql"
	"go.uber.org/zap"
)

var (
	ErrIndexMissmatch = errors.New("index missmatch")
	ErrOutputNotFound = errors.New("output not found")
)

func GetContractState(ctx context.Context, tx sql.Tx, fcid types.FileContractID) (api.ContractState, error) {
	var cse ContractState
	err := tx.
		QueryRow(ctx, `SELECT state FROM contracts WHERE fcid = ?`, FileContractID(fcid)).
		Scan(&cse)
	if errors.Is(err, dsql.ErrNoRows) {
		return "", contractNotFoundErr(fcid)
	} else if err != nil {
		return "", fmt.Errorf("failed to fetch contract state: %w", err)
	}

	return api.ContractState(cse.String()), nil
}

func UpdateChainIndex(ctx context.Context, tx sql.Tx, index types.ChainIndex, l *zap.SugaredLogger) error {
	l.Debugw("update chain index", "height", index.Height, "block_id", index.ID)

	if res, err := tx.Exec(ctx,
		fmt.Sprintf("UPDATE consensus_infos SET height = ?, block_id = ? WHERE id = %d", sql.ConsensusInfoID),
		index.Height,
		Hash256(index.ID),
	); err != nil {
		return fmt.Errorf("failed to update chain index: %w", err)
	} else if n, err := res.RowsAffected(); err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	} else if n != 1 {
		return fmt.Errorf("failed to update chain index: no rows affected")
	}

	return nil
}

func UpdateContractRevision(ctx context.Context, tx sql.Tx, fcid types.FileContractID, revisionHeight, revisionNumber, size uint64, l *zap.SugaredLogger) error {
	// fetch current contract, in SQLite we could use a single query to
	// perform the conditional update, however we have to compare the
	// revision number which are stored as strings so we need to fetch the
	// current contract info separately
	var currRevisionHeight, currSize uint64
	var currRevisionNumber Uint64Str
	err := tx.
		QueryRow(ctx, `SELECT revision_height, revision_number, COALESCE(size, 0) FROM contracts WHERE fcid = ?`, FileContractID(fcid)).
		Scan(&currRevisionHeight, &currRevisionNumber, &currSize)
	if errors.Is(err, dsql.ErrNoRows) {
		return contractNotFoundErr(fcid)
	} else if err != nil {
		return fmt.Errorf("failed to fetch contract %v: %w", fcid, err)
	}

	// update contract
	err = updateContract(ctx, tx, fcid, currRevisionHeight, uint64(currRevisionNumber), revisionHeight, revisionNumber, size)
	if err != nil {
		return fmt.Errorf("failed to update contract %v: %w", fcid, err)
	}

	l.Debugw(fmt.Sprintf("updated contract %v: revision number %d -> %d, revision height %d -> %d, size %d -> %d", fcid, currRevisionNumber, revisionNumber, currRevisionHeight, revisionHeight, currSize, size))
	return nil
}

func UpdateContractProofHeight(ctx context.Context, tx sql.Tx, fcid types.FileContractID, proofHeight uint64, l *zap.SugaredLogger) error {
	l.Debugw("update contract proof height", "fcid", fcid, "proof_height", proofHeight)

	res, err := tx.Exec(ctx, `UPDATE contracts SET proof_height = ? WHERE fcid = ?`, proofHeight, FileContractID(fcid))
	if err != nil {
		return err
	} else if n, err := res.RowsAffected(); err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	} else if n == 0 {
		return fmt.Errorf("expected to update 1 row, but updated %d", n)
	}

	return nil
}

func UpdateContractState(ctx context.Context, tx sql.Tx, fcid types.FileContractID, state api.ContractState, l *zap.SugaredLogger) error {
	l.Debugw("update contract state", "fcid", fcid, "state", state)

	var cs ContractState
	if err := cs.LoadString(string(state)); err != nil {
		return err
	}

	res, err := tx.Exec(ctx, `UPDATE contracts SET state = ? WHERE fcid = ?`, cs, FileContractID(fcid))
	if err != nil {
		return err
	} else if n, err := res.RowsAffected(); err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	} else if n == 0 {
		return fmt.Errorf("expected to update 1 row, but updated %d", n)
	}

	return nil
}

func UpdateFailedContracts(ctx context.Context, tx sql.Tx, blockHeight uint64, l *zap.SugaredLogger) error {
	l.Debugw("update failed contracts", "block_height", blockHeight)

	if res, err := tx.Exec(ctx,
		"UPDATE contracts SET state = ? WHERE window_end <= ? AND state = ?",
		ContractStateFromString(api.ContractStateFailed),
		blockHeight,
		ContractStateFromString(api.ContractStateActive),
	); err != nil {
		return fmt.Errorf("failed to update failed contracts: %w", err)
	} else if n, err := res.RowsAffected(); err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	} else if n > 0 {
		l.Debugw(fmt.Sprintf("marked %d active contracts as failed", n), "window_end", blockHeight)
	}

	return nil
}

// UpdateWalletSiacoinElementProofs updates the proofs of all state elements
// affected by the update. ProofUpdater.UpdateElementProof must be called
// for each state element in the database.
func UpdateWalletSiacoinElementProofs(ctx context.Context, tx sql.Tx, updater wallet.ProofUpdater) error {
	se, err := getSiacoinStateElements(ctx, tx)
	if err != nil {
		return fmt.Errorf("failed to get siacoin state elements: %w", err)
	} else if len(se) == 0 {
		return nil
	}
	for i := range se {
		ele := types.StateElement{
			LeafIndex:   se[i].LeafIndex,
			MerkleProof: se[i].MerkleProof.Hashes,
		}
		updater.UpdateElementProof(&ele)
		se[i].LeafIndex = ele.LeafIndex
		se[i].MerkleProof.Hashes = ele.MerkleProof
	}
	return updateSiacoinStateElements(ctx, tx, se)
}

func getSiacoinStateElements(ctx context.Context, tx sql.Tx) (elements []StateElement, err error) {
	rows, err := tx.Query(ctx, "SELECT output_id, leaf_index, merkle_proof FROM wallet_outputs")
	if err != nil {
		return nil, fmt.Errorf("failed to fetch state elements: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		el, err := scanStateElement(rows)
		if err != nil {
			return nil, fmt.Errorf("failed to scan state element: %w", err)
		}
		elements = append(elements, el)
	}
	return elements, rows.Err()
}

func updateSiacoinStateElements(ctx context.Context, tx sql.Tx, elements []StateElement) error {
	updateStmt, err := tx.Prepare(ctx, "UPDATE wallet_outputs SET leaf_index = ?, merkle_proof= ?  WHERE output_id = ?")
	if err != nil {
		return fmt.Errorf("failed to prepare statement to update state elements: %w", err)
	}
	defer updateStmt.Close()

	for _, el := range elements {
		if _, err := updateStmt.Exec(ctx, el.LeafIndex, el.MerkleProof, el.ID); err != nil {
			return fmt.Errorf("failed to update state element '%v': %w", el.ID, err)
		}
	}
	return nil
}

func contractNotFoundErr(fcid types.FileContractID) error {
	return fmt.Errorf("%w: %v", api.ErrContractNotFound, fcid)
}

func updateContract(ctx context.Context, tx sql.Tx, fcid types.FileContractID, currRevisionHeight, currRevisionNumber, revisionHeight, revisionNumber, size uint64) (err error) {
	if revisionNumber > currRevisionNumber {
		_, err = tx.Exec(
			ctx,
			"UPDATE contracts SET revision_height = ?, revision_number = ?, size = ? WHERE fcid = ?",
			revisionHeight,
			fmt.Sprint(revisionNumber),
			size,
			FileContractID(fcid),
		)
	} else if revisionHeight > currRevisionHeight {
		_, err = tx.Exec(
			ctx,
			"UPDATE contracts SET revision_height = ? WHERE fcid = ?",
			revisionHeight,
			FileContractID(fcid),
		)
	}
	return
}
