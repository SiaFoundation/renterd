package stores

import (
	"context"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/wallet"
	"go.sia.tech/renterd/v2/stores/sql"
)

var (
	_ wallet.SingleAddressStore = (*SQLStore)(nil)
)

// Tip returns the consensus change ID and block height of the last wallet
// change.
func (s *SQLStore) Tip() (ci types.ChainIndex, err error) {
	err = s.db.Transaction(s.shutdownCtx, func(tx sql.DatabaseTx) error {
		ci, err = tx.Tip(s.shutdownCtx)
		return err
	})
	return
}

// UnspentSiacoinElements returns a list of all unspent siacoin outputs
func (s *SQLStore) UnspentSiacoinElements() (elements []types.SiacoinElement, err error) {
	err = s.db.Transaction(context.Background(), func(tx sql.DatabaseTx) (err error) {
		elements, err = tx.UnspentSiacoinElements(context.Background())
		return
	})
	return
}

// WalletEvents returns a paginated list of events, ordered by maturity height,
// descending. If no more events are available, (nil, nil) is returned.
func (s *SQLStore) WalletEvents(offset, limit int) (events []wallet.Event, err error) {
	err = s.db.Transaction(context.Background(), func(tx sql.DatabaseTx) (err error) {
		events, err = tx.WalletEvents(context.Background(), offset, limit)
		return
	})
	return
}

// WalletEventCount returns the number of events relevant to the wallet.
func (s *SQLStore) WalletEventCount() (count uint64, err error) {
	err = s.db.Transaction(context.Background(), func(tx sql.DatabaseTx) (err error) {
		count, err = tx.WalletEventCount(context.Background())
		return
	})
	return
}

// LockUTXOs locks the specified UTXOs until the specified time.
func (s *SQLStore) LockUTXOs(scois []types.SiacoinOutputID, until time.Time) error {
	return s.db.Transaction(s.shutdownCtx, func(tx sql.DatabaseTx) error {
		return tx.WalletLockOutputs(s.shutdownCtx, scois, until)
	})
}

// LockedUTXOs returns a list of UTXOs that are currently locked until the
// specified time.
func (s *SQLStore) LockedUTXOs(t time.Time) (utxos []types.SiacoinOutputID, err error) {
	err = s.db.Transaction(s.shutdownCtx, func(tx sql.DatabaseTx) (err error) {
		utxos, err = tx.WalletLockedOutputs(s.shutdownCtx, t)
		return
	})
	return
}

// ReleaseUTXOs releases the specified UTXOs, making them available for use
// again. If the UTXOs are not locked, this is a no-op.
func (s *SQLStore) ReleaseUTXOs(scois []types.SiacoinOutputID) error {
	return s.db.Transaction(s.shutdownCtx, func(tx sql.DatabaseTx) error {
		return tx.WalletReleaseOutputs(s.shutdownCtx, scois)
	})
}
