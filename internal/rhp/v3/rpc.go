package rhp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"time"

	rhpv2 "go.sia.tech/core/rhp/v2"
	rhpv3 "go.sia.tech/core/rhp/v3"
	"go.sia.tech/core/types"
	"go.sia.tech/mux/v1"
	"go.sia.tech/renterd/v2/api"
	"go.sia.tech/renterd/v2/internal/gouging"
	"go.sia.tech/renterd/v2/internal/utils"
)

type (
	// PriceTablePaymentFunc is a function that can be passed in to RPCPriceTable.
	// It is called after the price table is received from the host and supposed to
	// create a payment for that table and return it. It can also be used to perform
	// gouging checks before paying for the table.
	PriceTablePaymentFunc func(pt rhpv3.HostPriceTable) (rhpv3.PaymentMethod, error)

	DiscardTxnFn   func(err *error)
	PrepareRenewFn func(pt rhpv3.HostPriceTable) (toSign []types.Hash256, txnSet []types.Transaction, fundAmount types.Currency, discard DiscardTxnFn, err error)
	SignTxnFn      func(txn *types.Transaction, toSign []types.Hash256, cf types.CoveredFields)
)

// rpcPriceTable calls the UpdatePriceTable RPC.
func rpcPriceTable(ctx context.Context, t *transportV3, paymentFunc PriceTablePaymentFunc) (_ api.HostPriceTable, err error) {
	defer utils.WrapErr(ctx, "PriceTable", &err)

	s, err := t.DialStream(ctx)
	if err != nil {
		return api.HostPriceTable{}, err
	}
	defer s.Close()

	var pt rhpv3.HostPriceTable
	var ptr rhpv3.RPCUpdatePriceTableResponse
	if err := s.WriteRequest(rhpv3.RPCUpdatePriceTableID, nil); err != nil {
		return api.HostPriceTable{}, fmt.Errorf("couldn't send RPCUpdatePriceTableID: %w", err)
	} else if err := s.ReadResponse(&ptr, maxPriceTableSize); err != nil {
		return api.HostPriceTable{}, fmt.Errorf("couldn't read RPCUpdatePriceTableResponse: %w", err)
	} else if err := json.Unmarshal(ptr.PriceTableJSON, &pt); err != nil {
		return api.HostPriceTable{}, fmt.Errorf("couldn't unmarshal price table: %w", err)
	} else if payment, err := paymentFunc(pt); err != nil {
		return api.HostPriceTable{}, fmt.Errorf("couldn't create payment: %w", err)
	} else if payment == nil {
		return api.HostPriceTable{
			HostPriceTable: pt,
			Expiry:         time.Now(),
		}, nil // intended not to pay
	} else if err := processPayment(s, payment); err != nil {
		return api.HostPriceTable{}, fmt.Errorf("couldn't process payment: %w", err)
	} else if err := s.ReadResponse(&rhpv3.RPCPriceTableResponse{}, 0); err != nil {
		return api.HostPriceTable{}, fmt.Errorf("couldn't read RPCPriceTableResponse: %w", err)
	} else {
		return api.HostPriceTable{
			HostPriceTable: pt,
			Expiry:         time.Now().Add(pt.Validity),
		}, nil
	}
}

// rpcAccountBalance calls the AccountBalance RPC.
func rpcAccountBalance(ctx context.Context, t *transportV3, payment rhpv3.PaymentMethod, account rhpv3.Account, settingsID rhpv3.SettingsID) (bal types.Currency, err error) {
	defer utils.WrapErr(ctx, "AccountBalance", &err)
	s, err := t.DialStream(ctx)
	if err != nil {
		return types.ZeroCurrency, err
	}
	defer s.Close()

	req := rhpv3.RPCAccountBalanceRequest{
		Account: account,
	}
	var resp rhpv3.RPCAccountBalanceResponse
	if err := s.WriteRequest(rhpv3.RPCAccountBalanceID, &settingsID); err != nil {
		return types.ZeroCurrency, err
	} else if err := processPayment(s, payment); err != nil {
		return types.ZeroCurrency, err
	} else if err := s.WriteResponse(&req); err != nil {
		return types.ZeroCurrency, err
	} else if err := s.ReadResponse(&resp, 128); err != nil {
		return types.ZeroCurrency, err
	}
	return resp.Balance, nil
}

// rpcFundAccount calls the FundAccount RPC.
func rpcFundAccount(ctx context.Context, t *transportV3, payment rhpv3.PaymentMethod, account rhpv3.Account, settingsID rhpv3.SettingsID) (err error) {
	defer utils.WrapErr(ctx, "FundAccount", &err)
	s, err := t.DialStream(ctx)
	if err != nil {
		return err
	}
	defer s.Close()

	req := rhpv3.RPCFundAccountRequest{
		Account: account,
	}
	var resp rhpv3.RPCFundAccountResponse
	if err := s.WriteRequest(rhpv3.RPCFundAccountID, &settingsID); err != nil {
		return err
	} else if err := s.WriteResponse(&req); err != nil {
		return err
	} else if err := processPayment(s, payment); err != nil {
		return err
	} else if err := s.ReadResponse(&resp, defaultRPCResponseMaxSize); err != nil {
		return err
	}
	return nil
}

// rpcLatestRevision calls the LatestRevision RPC. The paymentFunc allows for
// fetching a pricetable using the fetched revision to pay for it. If
// paymentFunc returns 'nil' as payment, the host is not paid.
func rpcLatestRevision(ctx context.Context, t *transportV3, contractID types.FileContractID) (_ types.FileContractRevision, err error) {
	defer utils.WrapErr(ctx, "LatestRevision", &err)
	s, err := t.DialStream(ctx)
	if err != nil {
		return types.FileContractRevision{}, err
	}
	defer s.Close()
	req := rhpv3.RPCLatestRevisionRequest{
		ContractID: contractID,
	}
	var resp rhpv3.RPCLatestRevisionResponse
	if err := s.WriteRequest(rhpv3.RPCLatestRevisionID, &req); err != nil {
		return types.FileContractRevision{}, err
	} else if err := s.ReadResponse(&resp, defaultRPCResponseMaxSize); err != nil {
		return types.FileContractRevision{}, err
	}
	return resp.Revision, nil
}

// rpcReadSector calls the ExecuteProgram RPC with a ReadSector instruction.
func rpcReadSector(ctx context.Context, t *transportV3, w io.Writer, pt rhpv3.HostPriceTable, payment rhpv3.PaymentMethod, offset, length uint64, merkleRoot types.Hash256) (cost, refund types.Currency, err error) {
	defer utils.WrapErr(ctx, "ReadSector", &err)
	s, err := t.DialStream(ctx)
	if err != nil {
		return types.ZeroCurrency, types.ZeroCurrency, err
	}
	defer s.Close()

	var buf bytes.Buffer
	e := types.NewEncoder(&buf)
	e.WriteUint64(uint64(length))
	e.WriteUint64(uint64(offset))
	merkleRoot.EncodeTo(e)
	e.Flush()

	req := rhpv3.RPCExecuteProgramRequest{
		FileContractID: types.FileContractID{},
		Program: []rhpv3.Instruction{&rhpv3.InstrReadSector{
			LengthOffset:     0,
			OffsetOffset:     8,
			MerkleRootOffset: 16,
			ProofRequired:    true,
		}},
		ProgramData: buf.Bytes(),
	}

	var cancellationToken types.Specifier
	var resp rhpv3.RPCExecuteProgramResponse
	if err = s.WriteRequest(rhpv3.RPCExecuteProgramID, &pt.UID); err != nil {
		return
	} else if err = processPayment(s, payment); err != nil {
		return
	} else if err = s.WriteResponse(&req); err != nil {
		return
	} else if err = s.ReadResponse(&cancellationToken, 16); err != nil {
		return
	} else if err = s.ReadResponse(&resp, rhpv2.SectorSize+responseLeeway); err != nil {
		return
	}

	// check response error
	if err = resp.Error; err != nil {
		refund = resp.FailureRefund
		return
	}
	cost = resp.TotalCost

	// verify proof
	proofStart := uint64(offset) / rhpv2.LeafSize
	proofEnd := uint64(offset+length) / rhpv2.LeafSize
	verifier := rhpv2.NewRangeProofVerifier(proofStart, proofEnd)
	_, err = verifier.ReadFrom(bytes.NewReader(resp.Output))
	if err != nil {
		err = fmt.Errorf("failed to read proof: %w", err)
		return
	} else if !verifier.Verify(resp.Proof, merkleRoot) {
		err = errors.New("proof verification failed")
		return
	}

	_, err = w.Write(resp.Output)
	return
}

func rpcAppendSector(ctx context.Context, t *transportV3, renterKey types.PrivateKey, pt rhpv3.HostPriceTable, rev *types.FileContractRevision, payment rhpv3.PaymentMethod, sectorRoot types.Hash256, sector *[rhpv2.SectorSize]byte) (cost types.Currency, err error) {
	defer utils.WrapErr(ctx, "AppendSector", &err)

	// sanity check revision first
	if rev.RevisionNumber == math.MaxUint64 {
		return types.ZeroCurrency, ErrMaxRevisionReached
	}

	s, err := t.DialStream(ctx)
	if err != nil {
		return types.ZeroCurrency, err
	}
	defer s.Close()

	req := rhpv3.RPCExecuteProgramRequest{
		FileContractID: rev.ParentID,
		Program: []rhpv3.Instruction{&rhpv3.InstrAppendSector{
			SectorDataOffset: 0,
			ProofRequired:    true,
		}},
		ProgramData: (*sector)[:],
	}

	var cancellationToken types.Specifier
	var executeResp rhpv3.RPCExecuteProgramResponse
	if err = s.WriteRequest(rhpv3.RPCExecuteProgramID, &pt.UID); err != nil {
		return
	} else if err = processPayment(s, payment); err != nil {
		return
	} else if err = s.WriteResponse(&req); err != nil {
		return
	} else if err = s.ReadResponse(&cancellationToken, 16); err != nil {
		return
	} else if err = s.ReadResponse(&executeResp, defaultRPCResponseMaxSize); err != nil {
		return
	}

	// compute expected collateral and refund
	expectedCost, expectedCollateral, expectedRefund, err := uploadSectorCost(pt, rev.WindowEnd)
	if err != nil {
		return types.ZeroCurrency, err
	}

	// apply leeways.
	// TODO: remove once most hosts use hostd. Then we can check for exact values.
	expectedCollateral = expectedCollateral.Mul64(9).Div64(10)
	expectedCost = expectedCost.Mul64(11).Div64(10)
	expectedRefund = expectedRefund.Mul64(9).Div64(10)

	// check if the cost, collateral and refund match our expectation.
	if executeResp.TotalCost.Cmp(expectedCost) > 0 {
		return types.ZeroCurrency, fmt.Errorf("cost exceeds expectation: %v > %v", executeResp.TotalCost.String(), expectedCost.String())
	}
	if executeResp.FailureRefund.Cmp(expectedRefund) < 0 {
		return types.ZeroCurrency, fmt.Errorf("insufficient refund: %v < %v", executeResp.FailureRefund.String(), expectedRefund.String())
	}
	if executeResp.AdditionalCollateral.Cmp(expectedCollateral) < 0 {
		return types.ZeroCurrency, fmt.Errorf("insufficient collateral: %v < %v", executeResp.AdditionalCollateral.String(), expectedCollateral.String())
	}

	// set the cost and refund
	cost = executeResp.TotalCost
	defer func() {
		if err != nil {
			cost = types.ZeroCurrency
			if executeResp.FailureRefund.Cmp(cost) < 0 {
				cost = cost.Sub(executeResp.FailureRefund)
			}
		}
	}()

	// check response error
	if err = executeResp.Error; err != nil {
		return
	}
	cost = executeResp.TotalCost

	// include the refund in the collateral
	collateral := executeResp.AdditionalCollateral.Add(executeResp.FailureRefund)

	// check proof
	if rev.Filesize == 0 {
		// For the first upload to a contract we don't get a proof. So we just
		// assert that the new contract root matches the root of the sector.
		if rev.Filesize == 0 && executeResp.NewMerkleRoot != sectorRoot {
			return types.ZeroCurrency, fmt.Errorf("merkle root doesn't match the sector root upon first upload to contract: %v != %v", executeResp.NewMerkleRoot, sectorRoot)
		}
	} else {
		// Otherwise we make sure the proof was transmitted and verify it.
		actions := []rhpv2.RPCWriteAction{{Type: rhpv2.RPCWriteActionAppend}} // TODO: change once rhpv3 support is available
		if !rhpv2.VerifyDiffProof(actions, rev.Filesize/rhpv2.SectorSize, executeResp.Proof, []types.Hash256{}, rev.FileMerkleRoot, executeResp.NewMerkleRoot, []types.Hash256{sectorRoot}) {
			return types.ZeroCurrency, errors.New("proof verification failed")
		}
	}

	// finalize the program with a new revision.
	newRevision := *rev
	newValid, newMissed, err := updateRevisionOutputs(&newRevision, types.ZeroCurrency, collateral)
	if err != nil {
		return types.ZeroCurrency, err
	}
	newRevision.Filesize += rhpv2.SectorSize
	newRevision.RevisionNumber++
	newRevision.FileMerkleRoot = executeResp.NewMerkleRoot

	finalizeReq := rhpv3.RPCFinalizeProgramRequest{
		Signature:         renterKey.SignHash(hashRevision(newRevision)),
		ValidProofValues:  newValid,
		MissedProofValues: newMissed,
		RevisionNumber:    newRevision.RevisionNumber,
	}

	var finalizeResp rhpv3.RPCFinalizeProgramResponse
	if err = s.WriteResponse(&finalizeReq); err != nil {
		return
	} else if err = s.ReadResponse(&finalizeResp, 64); err != nil {
		return
	}

	// read one more time to receive a potential error in case finalising the
	// contract fails after receiving the RPCFinalizeProgramResponse. This also
	// guarantees that the program is finalised before we return.
	// TODO: remove once most hosts use hostd.
	errFinalise := s.ReadResponse(&finalizeResp, 64)
	if errFinalise != nil &&
		!errors.Is(errFinalise, io.EOF) &&
		!errors.Is(errFinalise, mux.ErrClosedConn) &&
		!errors.Is(errFinalise, mux.ErrClosedStream) &&
		!errors.Is(errFinalise, mux.ErrPeerClosedStream) &&
		!errors.Is(errFinalise, mux.ErrPeerClosedConn) {
		err = errFinalise
		return
	}

	*rev = newRevision
	return
}

func rpcRenew(ctx context.Context, t *transportV3, gc gouging.Checker, rev types.FileContractRevision, renterKey types.PrivateKey, prepareTxnFn PrepareRenewFn, signTxnFn SignTxnFn) (_ rhpv2.ContractRevision, _ []types.Transaction, _, _ types.Currency, err error) {
	defer utils.WrapErr(ctx, "RPCRenew", &err)

	s, err := t.DialStream(ctx)
	if err != nil {
		return rhpv2.ContractRevision{}, nil, types.Currency{}, types.Currency{}, fmt.Errorf("failed to dial stream: %w", err)
	}
	defer s.Close()

	// Send the ptUID.
	if err = s.WriteRequest(rhpv3.RPCRenewContractID, &rhpv3.SettingsID{}); err != nil {
		return rhpv2.ContractRevision{}, nil, types.Currency{}, types.Currency{}, fmt.Errorf("failed to send ptUID: %w", err)
	}

	// Read the temporary one from the host.
	var ptResp rhpv3.RPCUpdatePriceTableResponse
	if err = s.ReadResponse(&ptResp, defaultRPCResponseMaxSize); err != nil {
		return rhpv2.ContractRevision{}, nil, types.Currency{}, types.Currency{}, fmt.Errorf("failed to read RPCUpdatePriceTableResponse: %w", err)
	}
	var pt rhpv3.HostPriceTable
	if err = json.Unmarshal(ptResp.PriceTableJSON, &pt); err != nil {
		return rhpv2.ContractRevision{}, nil, types.Currency{}, types.Currency{}, fmt.Errorf("failed to unmarshal price table: %w", err)
	}

	// Perform gouging checks.
	if breakdown := gc.CheckV1(nil, &pt); breakdown.Gouging() {
		return rhpv2.ContractRevision{}, nil, types.Currency{}, types.Currency{}, fmt.Errorf("host gouging during renew: %v", breakdown)
	}

	// Prepare the signed transaction that contains the final revision as well
	// as the new contract
	toSign, txnSet, fundAmount, discard, err := prepareTxnFn(pt)
	if err != nil {
		return rhpv2.ContractRevision{}, nil, types.Currency{}, types.Currency{}, fmt.Errorf("failed to prepare renew: %w", err)
	}

	// Starting from here, we need to make sure to release the txn on error.
	defer discard(&err)

	parents, txn := txnSet[:len(txnSet)-1], txnSet[len(txnSet)-1]

	// Sign only the revision and contract. We can't sign everything because
	// then the host can't add its own outputs.
	h := types.NewHasher()
	txn.FileContracts[0].EncodeTo(h.E)
	txn.FileContractRevisions[0].EncodeTo(h.E)
	finalRevisionSignature := renterKey.SignHash(h.Sum())

	// Send the request.
	req := rhpv3.RPCRenewContractRequest{
		TransactionSet:         txnSet,
		RenterKey:              rev.UnlockConditions.PublicKeys[0],
		FinalRevisionSignature: finalRevisionSignature,
	}
	if err = s.WriteResponse(&req); err != nil {
		return rhpv2.ContractRevision{}, nil, types.Currency{}, types.Currency{}, fmt.Errorf("failed to send RPCRenewContractRequest: %w", err)
	}

	// Incorporate the host's additions.
	var hostAdditions rhpv3.RPCRenewContractHostAdditions
	if err = s.ReadResponse(&hostAdditions, defaultRPCResponseMaxSize); err != nil {
		return rhpv2.ContractRevision{}, nil, types.Currency{}, types.Currency{}, fmt.Errorf("failed to read RPCRenewContractHostAdditions: %w", err)
	}
	parents = append(parents, hostAdditions.Parents...)
	txn.SiacoinInputs = append(txn.SiacoinInputs, hostAdditions.SiacoinInputs...)
	txn.SiacoinOutputs = append(txn.SiacoinOutputs, hostAdditions.SiacoinOutputs...)
	finalRevRenterSig := types.TransactionSignature{
		ParentID:       types.Hash256(rev.ParentID),
		PublicKeyIndex: 0, // renter key is first
		CoveredFields: types.CoveredFields{
			FileContracts:         []uint64{0},
			FileContractRevisions: []uint64{0},
		},
		Signature: finalRevisionSignature[:],
	}
	finalRevHostSig := types.TransactionSignature{
		ParentID:       types.Hash256(rev.ParentID),
		PublicKeyIndex: 1,
		CoveredFields: types.CoveredFields{
			FileContracts:         []uint64{0},
			FileContractRevisions: []uint64{0},
		},
		Signature: hostAdditions.FinalRevisionSignature[:],
	}
	txn.Signatures = []types.TransactionSignature{finalRevRenterSig, finalRevHostSig}

	// Sign the inputs we funded the txn with and cover the whole txn including
	// the existing signatures.
	cf := types.CoveredFields{
		WholeTransaction: true,
		Signatures:       []uint64{0, 1},
	}
	signTxnFn(&txn, toSign, cf)

	// Create a new no-op revision and sign it.
	noOpRevision := initialRevision(txn, rev.UnlockConditions.PublicKeys[1], renterKey.PublicKey().UnlockKey())
	h = types.NewHasher()
	noOpRevision.EncodeTo(h.E)
	renterNoOpSig := renterKey.SignHash(h.Sum())
	renterNoOpRevisionSignature := types.TransactionSignature{
		ParentID:       types.Hash256(noOpRevision.ParentID),
		PublicKeyIndex: 0, // renter key is first
		CoveredFields: types.CoveredFields{
			FileContractRevisions: []uint64{0},
		},
		Signature: renterNoOpSig[:],
	}

	// Send the newly added signatures to the host and the signature for the
	// initial no-op revision.
	rs := rhpv3.RPCRenewSignatures{
		TransactionSignatures: txn.Signatures[2:],
		RevisionSignature:     renterNoOpRevisionSignature,
	}
	if err = s.WriteResponse(&rs); err != nil {
		return rhpv2.ContractRevision{}, nil, types.Currency{}, types.Currency{}, fmt.Errorf("failed to send RPCRenewSignatures: %w", err)
	}

	// Receive the host's signatures.
	var hostSigs rhpv3.RPCRenewSignatures
	if err = s.ReadResponse(&hostSigs, defaultRPCResponseMaxSize); err != nil {
		return rhpv2.ContractRevision{}, nil, types.Currency{}, types.Currency{}, fmt.Errorf("failed to read RPCRenewSignatures: %w", err)
	}
	txn.Signatures = append(txn.Signatures, hostSigs.TransactionSignatures...)

	// Add the parents to get the full txnSet.
	txnSet = parents
	txnSet = append(txnSet, txn)

	return rhpv2.ContractRevision{
		Revision:   noOpRevision,
		Signatures: [2]types.TransactionSignature{renterNoOpRevisionSignature, hostSigs.RevisionSignature},
	}, txnSet, pt.ContractPrice, fundAmount, nil
}

// wrapRPCErr extracts the innermost error, wraps it in either a errHost or
// errTransport and finally wraps it using the provided fnName.
func wrapRPCErr(err *error, fnName string) {
	if *err == nil {
		return
	}
	innerErr := *err
	for errors.Unwrap(innerErr) != nil {
		innerErr = errors.Unwrap(innerErr)
	}
	if errors.As(*err, new(*rhpv3.RPCError)) {
		*err = fmt.Errorf("%w: '%w'", utils.ErrHost, innerErr)
	} else {
		*err = fmt.Errorf("%w: '%w'", errTransport, innerErr)
	}
	*err = fmt.Errorf("%s: %w", fnName, *err)
}
