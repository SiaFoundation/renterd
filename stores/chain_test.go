package stores

import (
	"testing"
)

// TestApplyChainUpdate verifies the functionality of ApplyChainUpdate.
func TestApplyChainUpdate(t *testing.T) {
	t.Skip("TODO: rewrite")
	// db := newTestSQLStore(t, defaultTestSQLStoreConfig)
	// defer db.Close()

	// // prepare chain update
	// hk := types.PublicKey{1}
	// want := types.ChainIndex{Height: 1, ID: types.BlockID{1}}
	// cu := chain.NewChainUpdate(nil)
	// cu.HostUpdates[hk] = chain.HostUpdate{
	// 	Announcement: chain.HostAnnouncement{NetAddress: "foo.com:1000"},
	// 	BlockHeight:  want.Height,
	// 	BlockID:      want.ID,
	// 	Timestamp:    time.Now().UTC().Round(time.Second),
	// }
	// cu.Index = want

	// // assert announcements are processed
	// err := db.ApplyChainUpdate(context.Background(), cu)
	// if err != nil {
	// 	t.Fatal(err)
	// }
	// _, err = db.Host(context.Background(), hk)
	// if err != nil {
	// 	t.Fatal(err)
	// }

	// // assert chain index is updated
	// got, err := db.ChainIndex()
	// if err != nil {
	// 	t.Fatal(err)
	// } else if reflect.DeepEqual(got, want) {
	// 	t.Fatal("unexpected")
	// }

	// // add a contract
	// fcid := types.FileContractID{1}
	// _, err = db.addTestContract(fcid, hk)
	// if err != nil {
	// 	t.Fatal(err)
	// }

	// // update the contract
	// cu = chain.NewChainUpdate(nil)
	// cu.Index = types.ChainIndex{Height: 2, ID: types.BlockID{2}}
	// cu.ContractUpdates[types.FileContractID{1}] = &chain.ContractUpdate{State: api.ContractStateActive}
	// err = db.ApplyChainUpdate(context.Background(), cu)
	// if err != nil {
	// 	t.Fatal(err)
	// }

	// // assert contract state
	// c, err := db.Contract(context.Background(), fcid)
	// if err != nil {
	// 	t.Fatal(err)
	// } else if c.State != api.ContractStateActive {
	// 	t.Fatal("unexpected")
	// }

	// // add wallet output & event
	// cu = chain.NewChainUpdate(nil)
	// cu.Index = types.ChainIndex{Height: 3, ID: types.BlockID{3}}
	// cu.WalletOutputUpdates[types.Hash256{1}] = chain.WalletOutputUpdate{Addition: true, Element: wallet.SiacoinElement{
	// 	SiacoinElement: types.SiacoinElement{
	// 		StateElement: types.StateElement{
	// 			ID:          types.Hash256{1},
	// 			LeafIndex:   1,
	// 			MerkleProof: []types.Hash256{{1}, {2}, {3}},
	// 		},
	// 		SiacoinOutput:  types.SiacoinOutput{Value: types.NewCurrency64(1)},
	// 		MaturityHeight: 5,
	// 	},
	// 	Index: types.ChainIndex{Height: 3, ID: types.BlockID{3}},
	// }}
	// cu.WalletEventUpdates = []chain.WalletEventUpdate{{Addition: true, Event: wallet.Event{
	// 	ID:             types.Hash256{1},
	// 	Index:          types.ChainIndex{Height: 3, ID: types.BlockID{3}},
	// 	Inflow:         types.Siacoins(1),
	// 	Outflow:        types.Siacoins(2),
	// 	Transaction:    types.Transaction{},
	// 	Source:         wallet.EventSourceTransaction,
	// 	MaturityHeight: 5,
	// 	Timestamp:      time.Now().UTC(),
	// }}}
	// err = db.ApplyChainUpdate(context.Background(), cu)
	// if err != nil {
	// 	t.Fatal(err)
	// }

	// // assert event was added
	// events, err := db.WalletEvents(0, -1)
	// if err != nil {
	// 	t.Fatal(err)
	// } else if len(events) != 1 {
	// 	t.Fatal("unexpected")
	// } else if events[0].ID != (types.Hash256{1}) {
	// 	t.Fatal("unexpected")
	// }

	// // assert output was added
	// var output dbWalletOutput
	// err = db.db.Model(&dbWalletOutput{}).Where("output_id = ?", hash256(types.Hash256{1})).First(&output).Error
	// if err != nil {
	// 	t.Fatal(err)
	// }
	// if output.Index().Height != 3 {
	// 	t.Fatal("unexpected")
	// }
}
