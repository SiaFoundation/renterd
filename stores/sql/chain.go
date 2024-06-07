package sql

import (
	"context"
	dsql "database/sql"
	"errors"
	"fmt"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/internal/sql"
	"go.uber.org/zap"
)

var contractTables = []string{
	"contracts",
	"archived_contracts",
}

func ContractState(ctx context.Context, tx sql.Tx, fcid types.FileContractID) (api.ContractState, error) {
	var cse ContractStateEnum
	err := tx.
		QueryRow(ctx,
			fmt.Sprintf("SELECT state FROM (SELECT state, fcid FROM %s UNION SELECT state, fcid FROM %s) as combined WHERE fcid = ?",
				contractTables[0],
				contractTables[1]),
			FileContractID(fcid),
		).
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

func UpdateContract(ctx context.Context, tx sql.Tx, fcid types.FileContractID, revisionHeight, revisionNumber, size uint64, l *zap.SugaredLogger) error {
	for _, table := range contractTables {
		// fetch current contract, in SQLite we could use a single query to
		// perform the conditional update, however we have to compare the
		// revision number which are stored as strings so we need to fetch the
		// current contract info separately
		curr, err := contractInfo(ctx, tx, table, fcid)
		if err != nil {
			if errors.Is(err, dsql.ErrNoRows) {
				continue
			}
			return fmt.Errorf("failed to fetch '%s' info for %v: %w", table[:len(table)-1], fcid, err)
		}

		// update contract
		err = updateContract(ctx, tx, table, fcid, revisionHeight, revisionNumber, curr.RevNumber(), curr.RevisionHeight, size)
		if err != nil {
			return fmt.Errorf("failed to update '%s' %v: %w", table[:len(table)-1], fcid, err)
		}

		l.Debugw(fmt.Sprintf("update %s, revision number %s -> %s, revision height %d -> %d, size %d -> %d", table[:len(table)-1], curr.RevisionNumber, fmt.Sprint(revisionNumber), curr.RevisionHeight, revisionHeight, curr.Size, size), "fcid", fcid)
		return nil
	}

	return contractNotFoundErr(fcid)
}

func UpdateContractProofHeight(ctx context.Context, tx sql.Tx, fcid types.FileContractID, proofHeight uint64, l *zap.SugaredLogger) error {
	l.Debugw("update contract proof height", "fcid", fcid, "proof_height", proofHeight)

	for _, table := range contractTables {
		ok, err := updateContractProofHeight(ctx, tx, table, fcid, proofHeight)
		if err != nil {
			return fmt.Errorf("failed to update '%s' %v proof height: %w", table[:len(table)-1], fcid, err)
		} else if ok {
			break
		}
	}

	return nil
}

func UpdateContractState(ctx context.Context, tx sql.Tx, fcid types.FileContractID, state api.ContractState, l *zap.SugaredLogger) error {
	l.Debugw("update contract state", "fcid", fcid, "state", state)

	var cs ContractStateEnum
	if err := cs.LoadString(string(state)); err != nil {
		return err
	}

	for _, table := range contractTables {
		ok, err := updateContractState(ctx, tx, table, fcid, cs)
		if err != nil {
			return fmt.Errorf("failed to update %s state: %w", table[:len(table)-1], err)
		} else if ok {
			break
		}
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

func UpdateStateElements(ctx context.Context, tx sql.Tx, elements []types.StateElement) error {
	if len(elements) == 0 {
		return nil
	}

	updateStmt, err := tx.Prepare(ctx, "UPDATE wallet_outputs SET leaf_index = ?, merkle_proof= ?  WHERE output_id = ?")
	if err != nil {
		return fmt.Errorf("failed to prepare statement to update state elements: %w", err)
	}
	defer updateStmt.Close()

	for _, el := range elements {
		if _, err := updateStmt.Exec(ctx, el.LeafIndex, MerkleProof{Hashes: el.MerkleProof}, Hash256(el.ID)); err != nil {
			return fmt.Errorf("failed to update state element '%v': %w", el.ID, err)
		}
	}

	return nil
}

func WalletStateElements(ctx context.Context, tx sql.Tx) ([]types.StateElement, error) {
	rows, err := tx.Query(ctx, "SELECT output_id, leaf_index, merkle_proof FROM wallet_outputs")
	if err != nil {
		return nil, fmt.Errorf("failed to fetch state elements: %w", err)
	}
	defer rows.Close()

	var elements []types.StateElement
	for rows.Next() {
		if el, err := scanStateElement(rows); err != nil {
			return nil, fmt.Errorf("failed to scan state element: %w", err)
		} else {
			elements = append(elements, el)
		}
	}
	return elements, nil
}

func contractInfo(ctx context.Context, tx sql.Tx, table string, fcid types.FileContractID) (info ContractInfo, err error) {
	err = tx.
		QueryRow(ctx, fmt.Sprintf("SELECT revision_height, revision_number, size FROM %s WHERE fcid = ?", table), FileContractID(fcid)).
		Scan(&info.RevisionHeight, &info.RevisionNumber, &info.Size)
	return
}

func contractNotFoundErr(fcid types.FileContractID) error {
	return fmt.Errorf("%w: %v", api.ErrContractNotFound, fcid)
}

func updateContract(ctx context.Context, tx sql.Tx, table string, fcid types.FileContractID, revisionHeight, revisionNumber, currRevisionNumber, currRevisionHeight, size uint64) (err error) {
	var res dsql.Result
	if revisionNumber > currRevisionNumber {
		res, err = tx.Exec(
			ctx,
			fmt.Sprintf("UPDATE %s SET revision_height = ?, revision_number = ?, size = ? WHERE fcid = ?", table),
			revisionHeight,
			fmt.Sprint(revisionNumber),
			size,
			FileContractID(fcid),
		)
	} else if revisionHeight > currRevisionHeight {
		res, err = tx.Exec(
			ctx,
			fmt.Sprintf("UPDATE %s SET revision_height = ? WHERE fcid = ?", table),
			revisionHeight,
			FileContractID(fcid),
		)
	} else {
		return nil
	}

	if err == nil {
		if n, err := res.RowsAffected(); err != nil {
			return fmt.Errorf("failed to get rows affected: %w", err)
		} else if n != 1 {
			return fmt.Errorf("failed to update %s: no rows affected", table[:len(table)-1])
		}
	}
	return
}

func updateContractProofHeight(ctx context.Context, tx sql.Tx, table string, fcid types.FileContractID, proofHeight uint64) (bool, error) {
	res, err := tx.Exec(ctx, fmt.Sprintf("UPDATE %s SET proof_height = ? WHERE fcid = ?", table), proofHeight, FileContractID(fcid))
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("failed to get rows affected: %w", err)
	}
	return n == 1, nil
}

func updateContractState(ctx context.Context, tx sql.Tx, table string, fcid types.FileContractID, cs ContractStateEnum) (bool, error) {
	res, err := tx.Exec(ctx, fmt.Sprintf("UPDATE %s SET state = ? WHERE fcid = ?", table), cs, FileContractID(fcid))
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("failed to get rows affected: %w", err)
	}
	return n == 1, nil
}
