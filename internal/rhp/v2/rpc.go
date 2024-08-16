package rhp

import (
	"context"
	"encoding/json"
	"fmt"

	rhpv2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
	"go.sia.tech/renterd/internal/utils"
)

// rpcFormContract forms a contract with a host.
func rpcFormContract(ctx context.Context, t *rhpv2.Transport, renterKey types.PrivateKey, txnSet []types.Transaction) (_ rhpv2.ContractRevision, _ []types.Transaction, err error) {
	defer utils.WrapErr(ctx, "FormContract", &err)

	// strip our signatures before sending
	parents, txn := txnSet[:len(txnSet)-1], txnSet[len(txnSet)-1]
	renterContractSignatures := txn.Signatures
	txnSet[len(txnSet)-1].Signatures = nil

	// create request
	renterPubkey := renterKey.PublicKey()
	req := &rhpv2.RPCFormContractRequest{
		Transactions: txnSet,
		RenterKey:    renterPubkey.UnlockKey(),
	}
	if err := t.WriteRequest(rhpv2.RPCFormContractID, req); err != nil {
		return rhpv2.ContractRevision{}, nil, err
	}

	// execute form contract RPC
	var resp rhpv2.RPCFormContractAdditions
	if err := t.ReadResponse(&resp, 65536); err != nil {
		return rhpv2.ContractRevision{}, nil, err
	}

	// merge host additions with txn
	txn.SiacoinInputs = append(txn.SiacoinInputs, resp.Inputs...)
	txn.SiacoinOutputs = append(txn.SiacoinOutputs, resp.Outputs...)

	// create initial (no-op) revision, transaction, and signature
	fc := txn.FileContracts[0]
	initRevision := types.FileContractRevision{
		ParentID: txn.FileContractID(0),
		UnlockConditions: types.UnlockConditions{
			PublicKeys: []types.UnlockKey{
				renterPubkey.UnlockKey(),
				t.HostKey().UnlockKey(),
			},
			SignaturesRequired: 2,
		},
		FileContract: types.FileContract{
			RevisionNumber:     1,
			Filesize:           fc.Filesize,
			FileMerkleRoot:     fc.FileMerkleRoot,
			WindowStart:        fc.WindowStart,
			WindowEnd:          fc.WindowEnd,
			ValidProofOutputs:  fc.ValidProofOutputs,
			MissedProofOutputs: fc.MissedProofOutputs,
			UnlockHash:         fc.UnlockHash,
		},
	}
	revSig := renterKey.SignHash(hashRevision(initRevision))
	renterRevisionSig := types.TransactionSignature{
		ParentID:       types.Hash256(initRevision.ParentID),
		CoveredFields:  types.CoveredFields{FileContractRevisions: []uint64{0}},
		PublicKeyIndex: 0,
		Signature:      revSig[:],
	}

	// write our signatures
	renterSigs := &rhpv2.RPCFormContractSignatures{
		ContractSignatures: renterContractSignatures,
		RevisionSignature:  renterRevisionSig,
	}
	if err := t.WriteResponse(renterSigs); err != nil {
		return rhpv2.ContractRevision{}, nil, err
	}

	// read the host's signatures and merge them with our own
	var hostSigs rhpv2.RPCFormContractSignatures
	if err := t.ReadResponse(&hostSigs, minMessageSize); err != nil {
		return rhpv2.ContractRevision{}, nil, err
	}

	txn.Signatures = make([]types.TransactionSignature, 0, len(renterContractSignatures)+len(hostSigs.ContractSignatures))
	txn.Signatures = append(txn.Signatures, renterContractSignatures...)
	txn.Signatures = append(txn.Signatures, hostSigs.ContractSignatures...)

	signedTxnSet := make([]types.Transaction, 0, len(resp.Parents)+len(parents)+1)
	signedTxnSet = append(signedTxnSet, resp.Parents...)
	signedTxnSet = append(signedTxnSet, parents...)
	signedTxnSet = append(signedTxnSet, txn)
	return rhpv2.ContractRevision{
		Revision: initRevision,
		Signatures: [2]types.TransactionSignature{
			renterRevisionSig,
			hostSigs.RevisionSignature,
		},
	}, signedTxnSet, nil
}

// rpcSettings calls the Settings RPC, returning the host's reported settings.
func rpcSettings(ctx context.Context, t *rhpv2.Transport) (settings rhpv2.HostSettings, err error) {
	defer utils.WrapErr(ctx, "Settings", &err)

	var resp rhpv2.RPCSettingsResponse
	if err := t.Call(rhpv2.RPCSettingsID, nil, &resp); err != nil {
		return rhpv2.HostSettings{}, err
	} else if err := json.Unmarshal(resp.Settings, &settings); err != nil {
		return rhpv2.HostSettings{}, fmt.Errorf("couldn't unmarshal json: %w", err)
	}
	return settings, nil
}
