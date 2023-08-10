package wallet_test

import (
	"strings"
	"testing"
	"time"

	"go.sia.tech/core/consensus"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/wallet"
	"go.uber.org/zap"
	"lukechampine.com/frand"
)

// mockStore implements wallet.SingleAddressStore and allows to manipulate the
// wallet's utxos
type mockStore struct {
	utxos []wallet.SiacoinElement
}

func (s *mockStore) Balance() (types.Currency, error) { return types.ZeroCurrency, nil }
func (s *mockStore) Height() uint64                   { return 0 }
func (s *mockStore) UnspentSiacoinElements(bool) ([]wallet.SiacoinElement, error) {
	return s.utxos, nil
}
func (s *mockStore) Transactions(before, since time.Time, offset, limit int) ([]wallet.Transaction, error) {
	return nil, nil
}

var cs = consensus.State{
	Index: types.ChainIndex{
		Height: 1,
		ID:     types.BlockID{},
	},
}

// TestWalletRedistribute is a small unit test that covers the functionality of
// the 'Redistribute' method on the wallet.
func TestWalletRedistribute(t *testing.T) {
	oneSC := types.Siacoins(1)

	// create a wallet with one output
	priv := types.GeneratePrivateKey()
	pub := priv.PublicKey()
	utxo := wallet.SiacoinElement{
		types.SiacoinOutput{
			Value:   oneSC.Mul64(20),
			Address: wallet.StandardAddress(pub),
		},
		randomOutputID(),
		0,
	}
	s := &mockStore{utxos: []wallet.SiacoinElement{utxo}}
	w := wallet.NewSingleAddressWallet(priv, s, 0, zap.NewNop().Sugar())

	numOutputsWithValue := func(v types.Currency) (c uint64) {
		utxos, _ := w.UnspentOutputs()
		for _, utxo := range utxos {
			if utxo.Value.Equals(v) {
				c++
			}
		}
		return
	}

	applyTxn := func(txn types.Transaction) {
		for _, input := range txn.SiacoinInputs {
			for i, utxo := range s.utxos {
				if input.ParentID == types.SiacoinOutputID(utxo.ID) {
					s.utxos[i] = s.utxos[len(s.utxos)-1]
					s.utxos = s.utxos[:len(s.utxos)-1]
				}
			}
		}
		for _, output := range txn.SiacoinOutputs {
			s.utxos = append(s.utxos, wallet.SiacoinElement{output, randomOutputID(), 0})
		}
	}

	// assert number of outputs
	if utxos, err := w.UnspentOutputs(); err != nil {
		t.Fatal(err)
	} else if len(utxos) != 1 {
		t.Fatalf("unexpected number of outputs, %v != 1", len(utxos))
	}

	// split into 3 outputs of 6SC each
	amount := oneSC.Mul64(6)
	if txn, _, err := w.Redistribute(cs, 3, amount, types.NewCurrency64(1), nil); err != nil {
		t.Fatal(err)
	} else {
		applyTxn(txn)
	}

	// assert number of outputs
	if utxos, err := w.UnspentOutputs(); err != nil {
		t.Fatal(err)
	} else if len(s.utxos) != 4 {
		t.Fatalf("unexpected number of outputs, %v != 4", len(utxos))
	}

	// assert number of outputs that hold 6SC
	if cnt := numOutputsWithValue(amount); cnt != 3 {
		t.Fatalf("unexpected number of 6SC outputs, %v != 3", cnt)
	}

	// split into 3 outputs of 7SC each, expect this to fail
	_, _, err := w.Redistribute(cs, 3, oneSC.Mul64(7), types.NewCurrency64(1), nil)
	if err == nil || !strings.Contains(err.Error(), "insufficient balance") {
		t.Fatalf("unexpected err: '%v'", err)
	}

	// split into 2 outputs of 9SC
	amount = oneSC.Mul64(9)
	if txn, _, err := w.Redistribute(cs, 2, oneSC.Mul64(9), types.NewCurrency64(1), nil); err != nil {
		t.Fatal(err)
	} else {
		applyTxn(txn)
	}

	// assert number of outputs
	if utxos, err := w.UnspentOutputs(); err != nil {
		t.Fatal(err)
	} else if len(s.utxos) != 3 {
		t.Fatalf("unexpected number of outputs, %v != 3", len(utxos))
	}

	// assert number of outputs that hold 9SC
	if cnt := numOutputsWithValue(amount); cnt != 2 {
		t.Fatalf("unexpected number of 9SC outputs, %v != 2", cnt)
	}
}

func randomOutputID() (t types.Hash256) {
	frand.Read(t[:])
	return
}
