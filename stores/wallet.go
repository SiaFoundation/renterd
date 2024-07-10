package stores

import (
	"context"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/wallet"
	"go.sia.tech/renterd/stores/sql"
)

var (
	_ wallet.SingleAddressStore = (*SQLStore)(nil)
)

// Tip returns the consensus change ID and block height of the last wallet
// change.
func (s *SQLStore) Tip() (ci types.ChainIndex, err error) {
	err = s.bMain.Transaction(s.shutdownCtx, func(tx sql.DatabaseTx) error {
		ci, err = tx.Tip(s.shutdownCtx)
		return err
	})
	return
}

// UnspentSiacoinElements returns a list of all unspent siacoin outputs
func (s *SQLStore) UnspentSiacoinElements() (elements []types.SiacoinElement, err error) {
	err = s.bMain.Transaction(context.Background(), func(tx sql.DatabaseTx) (err error) {
		elements, err = tx.UnspentSiacoinElements(context.Background())
		return
	})
	return
}

// WalletEvents returns a paginated list of events, ordered by maturity height,
// descending. If no more events are available, (nil, nil) is returned.
func (s *SQLStore) WalletEvents(offset, limit int) (events []wallet.Event, err error) {
	err = s.bMain.Transaction(context.Background(), func(tx sql.DatabaseTx) (err error) {
		events, err = tx.WalletEvents(context.Background(), offset, limit)
		return
	})
	return
}

// WalletEventCount returns the number of events relevant to the wallet.
func (s *SQLStore) WalletEventCount() (count uint64, err error) {
	err = s.bMain.Transaction(context.Background(), func(tx sql.DatabaseTx) (err error) {
		count, err = tx.WalletEventCount(context.Background())
		return
	})
	return
}
