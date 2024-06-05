package mysql

import (
	"context"
	"fmt"
	"net"
	"strings"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/wallet"
	"go.sia.tech/renterd/api"
	"go.sia.tech/renterd/internal/chain"
	isql "go.sia.tech/renterd/internal/sql"
	ssql "go.sia.tech/renterd/stores/sql"
	"go.uber.org/zap"
)

var _ chain.ChainUpdateTx = (*ChainUpdateTx)(nil)

type ChainUpdateTx struct {
	ctx context.Context
	tx  isql.Tx
	l   *zap.SugaredLogger
}

func (c ChainUpdateTx) ApplyIndex(index types.ChainIndex, created, spent []types.SiacoinElement, events []wallet.Event) error {
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
		insertOutputStmt, err := c.tx.Prepare(c.ctx, "INSERT IGNORE INTO wallet_outputs (created_at, output_id, leaf_index, merkle_proof, value, address, maturity_height) VALUES (?, ?, ?, ?, ?, ?, ?)")
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
		insertEventStmt, err := c.tx.Prepare(c.ctx, "INSERT IGNORE INTO wallet_events (created_at, event_id, height, block_id, inflow, outflow, type, data, maturity_height, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
		if err != nil {
			return fmt.Errorf("failed to prepare statement to insert new events: %w", err)
		}
		defer insertEventStmt.Close()

		// insert new events
		for _, e := range events {
			c.l.Debugw(fmt.Sprintf("create event %v", e.ID), "height", index.Height, "block_id", index.ID)
			data, err := ssql.ToEventData(e.Data)
			if err != nil {
				c.l.Error(err)
				return err
			}
			if _, err := insertEventStmt.Exec(c.ctx,
				time.Now().UTC(),
				ssql.Hash256(e.ID),
				e.Index.Height,
				ssql.Hash256(e.Index.ID),
				ssql.Currency(e.Inflow),
				ssql.Currency(e.Outflow),
				e.Type,
				data,
				e.MaturityHeight,
				e.Timestamp.Unix(),
			); err != nil {
				return fmt.Errorf("failed to insert new event: %w", err)
			}
		}
	}
	return nil
}

func (c ChainUpdateTx) ContractState(fcid types.FileContractID) (api.ContractState, error) {
	return ssql.ContractState(c.ctx, c.tx, fcid)
}

func (c ChainUpdateTx) RevertIndex(index types.ChainIndex, removed, unspent []types.SiacoinElement) error {
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
			if res, err := deleteRemovedStmt.Exec(c.ctx, ssql.Hash256(e.ID)); err != nil {
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
		insertOutputStmt, err := c.tx.Prepare(c.ctx, "INSERT IGNORE INTO wallet_outputs (created_at, output_id, leaf_index, merkle_proof, value, address, maturity_height) VALUES (?, ?, ?, ?, ?, ?, ?)")
		if err != nil {
			return fmt.Errorf("failed to prepare statement to insert unspent outputs: %w", err)
		}
		defer insertOutputStmt.Close()

		// insert unspent outputs
		for _, e := range unspent {
			c.l.Debugw(fmt.Sprintf("recreate unspent output %v", e.ID), "height", index.Height, "block_id", index.ID)
			if _, err := insertOutputStmt.Exec(c.ctx,
				time.Now().UTC(),
				ssql.Hash256(e.ID),
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

func (c ChainUpdateTx) UpdateChainIndex(index types.ChainIndex) error {
	return ssql.UpdateChainIndex(c.ctx, c.tx, index, c.l)
}

func (c ChainUpdateTx) UpdateContract(fcid types.FileContractID, revisionHeight, revisionNumber, size uint64) error {
	return ssql.UpdateContract(c.ctx, c.tx, fcid, revisionHeight, revisionNumber, size, c.l)
}

func (c ChainUpdateTx) UpdateContractProofHeight(fcid types.FileContractID, proofHeight uint64) error {
	return ssql.UpdateContractProofHeight(c.ctx, c.tx, fcid, proofHeight, c.l)
}

func (c ChainUpdateTx) UpdateContractState(fcid types.FileContractID, state api.ContractState) error {
	return ssql.UpdateContractState(c.ctx, c.tx, fcid, state, c.l)
}

func (c ChainUpdateTx) UpdateFailedContracts(blockHeight uint64) error {
	return ssql.UpdateFailedContracts(c.ctx, c.tx, blockHeight, c.l)
}

func (c ChainUpdateTx) UpdateHost(hk types.PublicKey, ha chain.HostAnnouncement, bh uint64, blockID types.BlockID, ts time.Time) error { //
	c.l.Debugw("update host", "hk", hk, "netaddress", ha.NetAddress)

	// create the announcement
	if _, err := c.tx.Exec(c.ctx,
		"INSERT IGNORE INTO host_announcements (created_at, host_key, block_height, block_id, net_address) VALUES (?, ?, ?, ?, ?)",
		time.Now().UTC(),
		ssql.PublicKey(hk),
		bh,
		blockID.String(),
		ha.NetAddress,
	); err != nil {
		return fmt.Errorf("failed to insert host announcement: %w", err)
	}

	// create the host
	if res, err := c.tx.Exec(c.ctx, `
	INSERT INTO hosts (created_at, public_key, settings, price_table, total_scans, last_scan, last_scan_success, second_to_last_scan_success, scanned, uptime, downtime, recent_downtime, recent_scan_failures, successful_interactions, failed_interactions, lost_sectors, last_announcement, net_address)
	VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	ON DUPLICATE KEY UPDATE
		last_announcement = VALUES(last_announcement),
		net_address = VALUES(net_address)
	`,
		time.Now().UTC(),
		ssql.PublicKey(hk),
		ssql.Settings{},
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
	); err != nil {
		return fmt.Errorf("failed to insert host: %w", err)
	} else if n, err := res.RowsAffected(); err != nil {
		return fmt.Errorf("failed to check rows affected: %w", err)
	} else if n == 0 {
		return fmt.Errorf("failed to insert host: no rows affected")
	}

	// fetch host id
	var hostID int64
	if err := c.tx.QueryRow(c.ctx,
		"SELECT id FROM hosts WHERE public_key = ?",
		ssql.PublicKey(hk),
	).Scan(&hostID); err != nil {
		return fmt.Errorf("failed to fetch host id: %w", err)
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
				"INSERT IGNORE INTO host_allowlist_entry_hosts (db_allowlist_entry_id, db_host_id) VALUES (?,?)",
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
	for rows.Next() {
		var id int64
		var entry string
		if err := rows.Scan(&id, &entry); err != nil {
			return fmt.Errorf("failed to scan row: %w", err)
		}

		var blocked bool
		for _, value := range values {
			if value == entry || strings.HasSuffix(value, "."+entry) {
				blocked = true
				break
			}
		}
		if blocked {
			if _, err := c.tx.Exec(c.ctx,
				"INSERT IGNORE INTO host_blocklist_entry_hosts (db_blocklist_entry_id, db_host_id) VALUES (?,?)",
				id,
				hostID,
			); err != nil {
				return fmt.Errorf("failed to insert host into blocklist: %w", err)
			}
		}
	}

	return nil
}

func (c ChainUpdateTx) UpdateStateElements(elements []types.StateElement) error {
	return ssql.UpdateStateElements(c.ctx, c.tx, elements)
}

func (c ChainUpdateTx) WalletStateElements() ([]types.StateElement, error) {
	return ssql.WalletStateElements(c.ctx, c.tx)
}
