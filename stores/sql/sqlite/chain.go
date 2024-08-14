package sqlite

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	dsql "database/sql"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/coreutils/wallet"
	"go.sia.tech/renterd/api"
	isql "go.sia.tech/renterd/internal/sql"
	ssql "go.sia.tech/renterd/stores/sql"
	"go.uber.org/zap"
)

var (
	_ ssql.ChainUpdateTx = (*chainUpdateTx)(nil)
)

type chainUpdateTx struct {
	ctx context.Context
	tx  isql.Tx
	l   *zap.SugaredLogger
}

func (c chainUpdateTx) WalletApplyIndex(index types.ChainIndex, created, spent []types.SiacoinElement, events []wallet.Event, timestamp time.Time) error {
	c.l.Debugw("applying index", "height", index.Height, "block_id", index.ID)

	if len(spent) > 0 {
		// prepare statement to delete spent outputs
		deleteSpentStmt, err := c.tx.Prepare(c.ctx, "DELETE FROM wallet_outputs WHERE output_id = ?")
		if err != nil {
			return fmt.Errorf("failed to prepare statement to delete spent outputs: %w", err)
		}
		defer deleteSpentStmt.Close()

		// delete spent outputs
		for _, e := range spent {
			c.l.Debugw(fmt.Sprintf("remove output %v", e.ID), "height", index.Height, "block_id", index.ID)
			if res, err := deleteSpentStmt.Exec(c.ctx, ssql.Hash256(e.ID)); err != nil {
				return fmt.Errorf("failed to delete spent output: %w", err)
			} else if n, err := res.RowsAffected(); err != nil {
				return fmt.Errorf("failed to get rows affected: %w", err)
			} else if n != 1 {
				return fmt.Errorf("failed to delete spent output: no rows affected")
			}
		}
	}

	if len(created) > 0 {
		// prepare statement to insert new outputs
		insertOutputStmt, err := c.tx.Prepare(c.ctx, "INSERT OR IGNORE INTO wallet_outputs (created_at, output_id, leaf_index, merkle_proof, value, address, maturity_height) VALUES (?, ?, ?, ?, ?, ?, ?)")
		if err != nil {
			return fmt.Errorf("failed to prepare statement to insert new outputs: %w", err)
		}
		defer insertOutputStmt.Close()

		// insert new outputs
		for _, e := range created {
			c.l.Debugw(fmt.Sprintf("create output %v", e.ID), "height", index.Height, "block_id", index.ID)
			if _, err := insertOutputStmt.Exec(c.ctx,
				time.Now().UTC(),
				ssql.Hash256(e.ID),
				e.StateElement.LeafIndex,
				ssql.MerkleProof{Hashes: e.StateElement.MerkleProof},
				ssql.Currency(e.SiacoinOutput.Value),
				ssql.Hash256(e.SiacoinOutput.Address),
				e.MaturityHeight,
			); err != nil {
				return fmt.Errorf("failed to insert new output: %w", err)
			}
		}
	}

	if len(events) > 0 {
		// prepare statement to insert new events
		insertEventStmt, err := c.tx.Prepare(c.ctx, `INSERT OR IGNORE INTO wallet_events (created_at, height, block_id, event_id, inflow, outflow, type, data, maturity_height, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
		if err != nil {
			return fmt.Errorf("failed to prepare statement to insert new events: %w", err)
		}
		defer insertEventStmt.Close()

		// insert new events
		for _, e := range events {
			c.l.Debugw(fmt.Sprintf("create event %v", e.ID), "height", index.Height, "block_id", index.ID)
			data, err := json.Marshal(e.Data)
			if err != nil {
				c.l.Error(err)
				return err
			}
			if _, err := insertEventStmt.Exec(c.ctx,
				time.Now().UTC(),
				e.Index.Height,
				ssql.Hash256(e.Index.ID),
				ssql.Hash256(e.ID),
				ssql.Currency(e.SiacoinInflow()),
				ssql.Currency(e.SiacoinOutflow()),
				e.Type,
				data,
				e.MaturityHeight,
				ssql.UnixTimeNS(e.Timestamp),
			); err != nil {
				return fmt.Errorf("failed to insert new event: %w", err)
			}
		}
	}
	return nil
}

func (c chainUpdateTx) ContractState(fcid types.FileContractID) (api.ContractState, error) {
	return ssql.GetContractState(c.ctx, c.tx, fcid)
}

func (c chainUpdateTx) WalletRevertIndex(index types.ChainIndex, removed, unspent []types.SiacoinElement, timestamp time.Time) error {
	c.l.Debugw("reverting index", "height", index.Height, "block_id", index.ID)

	if len(removed) > 0 {
		// prepare statement to delete removed outputs
		deleteRemovedStmt, err := c.tx.Prepare(c.ctx, "DELETE FROM wallet_outputs WHERE output_id = ?")
		if err != nil {
			return fmt.Errorf("failed to prepare statement to delete removed outputs: %w", err)
		}
		defer deleteRemovedStmt.Close()

		// delete removed outputs
		for _, e := range removed {
			c.l.Debugw(fmt.Sprintf("remove output %v", e.ID), "height", index.Height, "block_id", index.ID)
			if res, err := deleteRemovedStmt.Exec(c.ctx, e.ID); err != nil {
				return fmt.Errorf("failed to delete removed output: %w", err)
			} else if n, err := res.RowsAffected(); err != nil {
				return fmt.Errorf("failed to get rows affected: %w", err)
			} else if n != 1 {
				return fmt.Errorf("failed to delete removed output: no rows affected")
			}
		}
	}

	if len(unspent) > 0 {
		// prepare statement to insert unspent outputs
		insertOutputStmt, err := c.tx.Prepare(c.ctx, "INSERT OR IGNORE INTO wallet_outputs (created_at, output_id, leaf_index, merkle_proof, value, address, maturity_height) VALUES (?, ?, ?, ?, ?, ?, ?)")
		if err != nil {
			return fmt.Errorf("failed to prepare statement to insert unspent outputs: %w", err)
		}
		defer insertOutputStmt.Close()

		// insert unspent outputs
		for _, e := range unspent {
			c.l.Debugw(fmt.Sprintf("recreate unspent output %v", e.ID), "height", index.Height, "block_id", index.ID)
			if _, err := insertOutputStmt.Exec(c.ctx,
				time.Now().UTC(),
				e.ID,
				e.StateElement.LeafIndex,
				ssql.MerkleProof{Hashes: e.StateElement.MerkleProof},
				ssql.Currency(e.SiacoinOutput.Value),
				ssql.Hash256(e.SiacoinOutput.Address),
				e.MaturityHeight,
			); err != nil {
				return fmt.Errorf("failed to insert unspent output: %w", err)
			}
		}
	}

	// remove events created at the reverted index
	res, err := c.tx.Exec(c.ctx, "DELETE FROM wallet_events WHERE height = ? AND block_id = ?", index.Height, ssql.Hash256(index.ID))
	if err != nil {
		return fmt.Errorf("failed to delete events: %w", err)
	} else if n, err := res.RowsAffected(); err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	} else if n > 0 {
		c.l.Debugw(fmt.Sprintf("removed %d events", n), "height", index.Height, "block_id", index.ID)
	}
	return nil
}

func (c chainUpdateTx) UpdateChainIndex(index types.ChainIndex) error {
	return ssql.UpdateChainIndex(c.ctx, c.tx, index, c.l)
}

func (c chainUpdateTx) UpdateContract(fcid types.FileContractID, revisionHeight, revisionNumber, size uint64) error {
	return ssql.UpdateContract(c.ctx, c.tx, fcid, revisionHeight, revisionNumber, size, c.l)
}

func (c chainUpdateTx) UpdateContractProofHeight(fcid types.FileContractID, proofHeight uint64) error {
	return ssql.UpdateContractProofHeight(c.ctx, c.tx, fcid, proofHeight, c.l)
}

func (c chainUpdateTx) UpdateContractState(fcid types.FileContractID, state api.ContractState) error {
	return ssql.UpdateContractState(c.ctx, c.tx, fcid, state, c.l)
}

func (c chainUpdateTx) UpdateFailedContracts(blockHeight uint64) error {
	return ssql.UpdateFailedContracts(c.ctx, c.tx, blockHeight, c.l)
}

func (c chainUpdateTx) UpdateHost(hk types.PublicKey, ha chain.HostAnnouncement, bh uint64, blockID types.BlockID, ts time.Time) error { //
	c.l.Debugw("update host", "hk", hk, "netaddress", ha.NetAddress)

	// create the announcement
	if _, err := c.tx.Exec(c.ctx,
		"INSERT OR IGNORE INTO host_announcements (created_at,host_key, block_height, block_id, net_address) VALUES (?, ?, ?, ?, ?)",
		time.Now().UTC(),
		ssql.PublicKey(hk),
		bh,
		blockID.String(),
		ha.NetAddress,
	); err != nil {
		return fmt.Errorf("failed to insert host announcement: %w", err)
	}

	// create the host
	var hostID int64
	if err := c.tx.QueryRow(c.ctx, `
	INSERT INTO hosts (created_at, public_key, settings, price_table, total_scans, last_scan, last_scan_success, second_to_last_scan_success, scanned, uptime, downtime, recent_downtime, recent_scan_failures, successful_interactions, failed_interactions, lost_sectors, last_announcement, net_address)
	VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	ON CONFLICT(public_key) DO UPDATE SET
		last_announcement = EXCLUDED.last_announcement,
		net_address = EXCLUDED.net_address
	RETURNING id`,
		time.Now().UTC(),
		ssql.PublicKey(hk),
		ssql.HostSettings{},
		ssql.PriceTable{},
		0,
		0,
		false,
		false,
		0,
		0,
		0,
		0,
		0,
		0,
		0,
		0,
		ts.UTC(),
		ha.NetAddress,
	).Scan(&hostID); err != nil {
		if errors.Is(err, dsql.ErrNoRows) {
			err = c.tx.QueryRow(c.ctx,
				"UPDATE hosts SET last_announcement = ?, net_address = ? WHERE public_key = ? RETURNING id",
				ts.UTC(),
				ha.NetAddress,
				ssql.PublicKey(hk),
			).Scan(&hostID)
			if err != nil {
				return fmt.Errorf("failed to fetch host id after conflict: %w", err)
			}
		} else {
			return fmt.Errorf("failed to insert host: %w", err)
		}
	}

	// update allow list
	rows, err := c.tx.Query(c.ctx, "SELECT id, entry FROM host_allowlist_entries")
	if err != nil {
		return fmt.Errorf("failed to fetch allow list: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var id int64
		var pk ssql.PublicKey
		if err := rows.Scan(&id, &pk); err != nil {
			return fmt.Errorf("failed to scan row: %w", err)
		}
		if hk == types.PublicKey(pk) {
			if _, err := c.tx.Exec(c.ctx,
				"INSERT OR IGNORE INTO host_allowlist_entry_hosts (db_allowlist_entry_id, db_host_id) VALUES (?,?)",
				id,
				hostID,
			); err != nil {
				return fmt.Errorf("failed to insert host into allowlist: %w", err)
			}
		}
	}

	// update blocklist
	values := []string{ha.NetAddress}
	host, _, err := net.SplitHostPort(ha.NetAddress)
	if err == nil {
		values = append(values, host)
	}

	rows, err = c.tx.Query(c.ctx, "SELECT id, entry FROM host_blocklist_entries")
	if err != nil {
		return fmt.Errorf("failed to fetch block list: %w", err)
	}
	defer rows.Close()

	type row struct {
		id    int64
		entry string
	}
	var entries []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.id, &r.entry); err != nil {
			return fmt.Errorf("failed to scan row: %w", err)
		}
		entries = append(entries, r)
	}

	for _, row := range entries {
		var blocked bool
		for _, value := range values {
			if value == row.entry || strings.HasSuffix(value, "."+row.entry) {
				blocked = true
				break
			}
		}
		if blocked {
			if _, err := c.tx.Exec(c.ctx,
				"INSERT OR IGNORE INTO host_blocklist_entry_hosts (db_blocklist_entry_id, db_host_id) VALUES (?,?)",
				row.id,
				hostID,
			); err != nil {
				return fmt.Errorf("failed to insert host into blocklist: %w", err)
			}
		} else {
			if _, err := c.tx.Exec(c.ctx,
				"DELETE FROM host_blocklist_entry_hosts WHERE db_blocklist_entry_id = ? AND db_host_id = ?",
				row.id,
				hostID,
			); err != nil {
				return fmt.Errorf("failed to remove host from blocklist: %w", err)
			}
		}
	}

	return nil
}

func (c chainUpdateTx) UpdateWalletStateElements(elements []types.StateElement) error {
	return ssql.UpdateWalletStateElements(c.ctx, c.tx, elements)
}

func (c chainUpdateTx) WalletStateElements() ([]types.StateElement, error) {
	return ssql.WalletStateElements(c.ctx, c.tx)
}
