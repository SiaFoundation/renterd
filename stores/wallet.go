package stores

import (
	"context"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/wallet"
	"go.sia.tech/renterd/v2/stores/sql"
)

var (
	_ wallet.SingleAddressStore = (*SQLStore)(nil)
)

// AddBroadcastedSet adds a set of broadcasted transactions. The wallet
// will periodically rebroadcast the transactions in this set until all
// transactions are gone from the transaction pool or one week has
// passed.
func (s *SQLStore) AddBroadcastedSet(txnSet wallet.BroadcastedSet) error {
	return s.db.Transaction(s.shutdownCtx, func(tx sql.DatabaseTx) error {
		return tx.WalletAddBroadcastedSet(s.shutdownCtx, txnSet)
	})
}

// BroadcastedSets returns recently broadcasted sets.
func (s *SQLStore) BroadcastedSets() (sets []wallet.BroadcastedSet, err error) {
	err = s.db.Transaction(s.shutdownCtx, func(tx sql.DatabaseTx) error {
		sets, err = tx.WalletBroadcastedSets(s.shutdownCtx)
		return err
	})
	return
}

func (s *SQLStore) RemoveBroadcastedSet(txnSet wallet.BroadcastedSet) error {
	return s.db.Transaction(s.shutdownCtx, func(tx sql.DatabaseTx) error {
		return tx.WalletRemoveBroadcastedSet(s.shutdownCtx, txnSet)
	})
}

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
func (s *SQLStore) UnspentSiacoinElements() (ci types.ChainIndex, elements []types.SiacoinElement, err error) {
	err = s.db.Transaction(context.Background(), func(tx sql.DatabaseTx) (err error) {
		ci, elements, err = tx.UnspentSiacoinElements(context.Background())
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

// WalletEvent returns a wallet event by its ID. If the event does not exist,
// ErrorNotFound is returned.
func (s *SQLStore) WalletEvent(id types.Hash256) (event wallet.Event, err error) {
	err = s.db.Transaction(context.Background(), func(tx sql.DatabaseTx) (err error) {
		event, err = tx.WalletEvent(context.Background(), id)
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
