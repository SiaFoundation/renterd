package rhp

import (
	"go.sia.tech/core/types"
)

// A ProtocolObject is an object that can be serialized for transport in the
// renter-host protocol.
type ProtocolObject interface {
	types.EncoderTo
	types.DecoderFrom
}

// EncodeTo implements types.EncoderTo.
func (r *RPCError) EncodeTo(e *types.Encoder) {
	r.Type.EncodeTo(e)
	e.WriteBytes(r.Data)
	e.WriteString(r.Description)
}

// DecodeFrom implements types.DecoderFrom.
func (r *RPCError) DecodeFrom(d *types.Decoder) {
	r.Type.DecodeFrom(d)
	r.Data = d.ReadBytes()
	r.Description = d.ReadString()
}

func (resp *rpcResponse) EncodeTo(e *types.Encoder) {
	e.WriteBool(resp.err != nil)
	if resp.err != nil {
		resp.err.EncodeTo(e)
		return
	}
	resp.data.EncodeTo(e)
}

func (resp *rpcResponse) DecodeFrom(d *types.Decoder) {
	if d.ReadBool() {
		resp.err = new(RPCError)
		resp.err.DecodeFrom(d)
		return
	}
	resp.data.DecodeFrom(d)
}

func (r *loopKeyExchangeRequest) EncodeTo(e *types.Encoder) {
	loopEnter.EncodeTo(e)
	e.Write(r.PublicKey[:])
	e.WritePrefix(len(r.Ciphers))
	for i := range r.Ciphers {
		r.Ciphers[i].EncodeTo(e)
	}
}

func (r *loopKeyExchangeRequest) DecodeFrom(d *types.Decoder) {
	new(types.Specifier).DecodeFrom(d) // loopEnter
	d.Read(r.PublicKey[:])
	r.Ciphers = make([]types.Specifier, d.ReadPrefix())
	for i := range r.Ciphers {
		r.Ciphers[i].DecodeFrom(d)
	}
}

func (r *loopKeyExchangeResponse) EncodeTo(e *types.Encoder) {
	e.Write(r.PublicKey[:])
	e.WriteBytes(r.Signature[:])
	r.Cipher.EncodeTo(e)
}

func (r *loopKeyExchangeResponse) DecodeFrom(d *types.Decoder) {
	d.Read(r.PublicKey[:])
	copy(r.Signature[:], d.ReadBytes())
	r.Cipher.DecodeFrom(d)
}

// RPCFormContract

func (r *RPCFormContractRequest) EncodeTo(e *types.Encoder) {
	e.WritePrefix(len(r.Transactions))
	for i := range r.Transactions {
		r.Transactions[i].EncodeTo(e)
	}
	r.RenterKey.EncodeTo(e)
}

func (r *RPCFormContractRequest) DecodeFrom(d *types.Decoder) {
	r.Transactions = make([]types.Transaction, d.ReadPrefix())
	for i := range r.Transactions {
		r.Transactions[i].DecodeFrom(d)
	}
	r.RenterKey.DecodeFrom(d)
}

func (r *RPCFormContractAdditions) EncodeTo(e *types.Encoder) {
	e.WritePrefix(len(r.Parents))
	for i := range r.Parents {
		r.Parents[i].EncodeTo(e)
	}
	e.WritePrefix(len(r.Inputs))
	for i := range r.Inputs {
		r.Inputs[i].EncodeTo(e)
	}
	e.WritePrefix(len(r.Outputs))
	for i := range r.Outputs {
		r.Outputs[i].EncodeTo(e)
	}
}

func (r *RPCFormContractAdditions) DecodeFrom(d *types.Decoder) {
	r.Parents = make([]types.Transaction, d.ReadPrefix())
	for i := range r.Parents {
		r.Parents[i].DecodeFrom(d)
	}
	r.Inputs = make([]types.SiacoinInput, d.ReadPrefix())
	for i := range r.Inputs {
		r.Inputs[i].DecodeFrom(d)
	}
	r.Outputs = make([]types.SiacoinOutput, d.ReadPrefix())
	for i := range r.Outputs {
		r.Outputs[i].DecodeFrom(d)
	}
}

func (r *RPCFormContractSignatures) EncodeTo(e *types.Encoder) {
	e.WritePrefix(len(r.ContractSignatures))
	for i := range r.ContractSignatures {
		r.ContractSignatures[i].EncodeTo(e)
	}
	r.RevisionSignature.EncodeTo(e)
}

func (r *RPCFormContractSignatures) DecodeFrom(d *types.Decoder) {
	r.ContractSignatures = make([]types.TransactionSignature, d.ReadPrefix())
	for i := range r.ContractSignatures {
		r.ContractSignatures[i].DecodeFrom(d)
	}
	r.RevisionSignature.DecodeFrom(d)
}

// RPCRenewAndClear

func (r *RPCRenewAndClearContractRequest) EncodeTo(e *types.Encoder) {
	e.WritePrefix(len(r.Transactions))
	for i := range r.Transactions {
		r.Transactions[i].EncodeTo(e)
	}
	r.RenterKey.EncodeTo(e)
	e.WritePrefix(len(r.FinalValidProofValues))
	for i := range r.FinalValidProofValues {
		r.FinalValidProofValues[i].EncodeTo(e)
	}
	e.WritePrefix(len(r.FinalMissedProofValues))
	for i := range r.FinalMissedProofValues {
		r.FinalMissedProofValues[i].EncodeTo(e)
	}
}

func (r *RPCRenewAndClearContractRequest) DecodeFrom(d *types.Decoder) {
	r.Transactions = make([]types.Transaction, d.ReadPrefix())
	for i := range r.Transactions {
		r.Transactions[i].DecodeFrom(d)
	}
	r.RenterKey.DecodeFrom(d)
	r.FinalValidProofValues = make([]types.Currency, d.ReadPrefix())
	for i := range r.FinalValidProofValues {
		r.FinalValidProofValues[i].DecodeFrom(d)
	}
	r.FinalMissedProofValues = make([]types.Currency, d.ReadPrefix())
	for i := range r.FinalMissedProofValues {
		r.FinalMissedProofValues[i].DecodeFrom(d)
	}
}

func (r *RPCRenewAndClearContractSignatures) EncodeTo(e *types.Encoder) {
	e.WritePrefix(len(r.ContractSignatures))
	for i := range r.ContractSignatures {
		r.ContractSignatures[i].EncodeTo(e)
	}
	r.RevisionSignature.EncodeTo(e)
	e.WriteBytes(r.FinalRevisionSignature[:])
}

func (r *RPCRenewAndClearContractSignatures) DecodeFrom(d *types.Decoder) {
	r.ContractSignatures = make([]types.TransactionSignature, d.ReadPrefix())
	for i := range r.ContractSignatures {
		r.ContractSignatures[i].DecodeFrom(d)
	}
	r.RevisionSignature.DecodeFrom(d)
	copy(r.FinalRevisionSignature[:], d.ReadBytes())
}

// RPCLock

func (r *RPCLockRequest) EncodeTo(e *types.Encoder) {
	e.Write(r.ContractID[:])
	e.WriteBytes(r.Signature[:])
	e.WriteUint64(r.Timeout)
}

func (r *RPCLockRequest) DecodeFrom(d *types.Decoder) {
	d.Read(r.ContractID[:])
	copy(r.Signature[:], d.ReadBytes())
	r.Timeout = d.ReadUint64()
}

func (r *RPCLockResponse) EncodeTo(e *types.Encoder) {
	e.WriteBool(r.Acquired)
	e.Write(r.NewChallenge[:])
	r.Revision.EncodeTo(e)
	e.WritePrefix(len(r.Signatures))
	for i := range r.Signatures {
		r.Signatures[i].EncodeTo(e)
	}
}

func (r *RPCLockResponse) DecodeFrom(d *types.Decoder) {
	r.Acquired = d.ReadBool()
	d.Read(r.NewChallenge[:])
	r.Revision.DecodeFrom(d)
	r.Signatures = make([]types.TransactionSignature, d.ReadPrefix())
	for i := range r.Signatures {
		r.Signatures[i].DecodeFrom(d)
	}
}

// RPCRead

func (r *RPCReadRequest) EncodeTo(e *types.Encoder) {
	e.WritePrefix(len(r.Sections))
	for i := range r.Sections {
		e.Write(r.Sections[i].MerkleRoot[:])
		e.WriteUint64(uint64(r.Sections[i].Offset))
		e.WriteUint64(uint64(r.Sections[i].Length))
	}
	e.WriteBool(r.MerkleProof)
	e.WriteUint64(r.RevisionNumber)
	e.WritePrefix(len(r.ValidProofValues))
	for i := range r.ValidProofValues {
		r.ValidProofValues[i].EncodeTo(e)
	}
	e.WritePrefix(len(r.MissedProofValues))
	for i := range r.MissedProofValues {
		r.MissedProofValues[i].EncodeTo(e)
	}
	e.WriteBytes(r.Signature[:])
}

func (r *RPCReadRequest) DecodeFrom(d *types.Decoder) {
	r.Sections = make([]RPCReadRequestSection, d.ReadPrefix())
	for i := range r.Sections {
		d.Read(r.Sections[i].MerkleRoot[:])
		r.Sections[i].Offset = d.ReadUint64()
		r.Sections[i].Length = d.ReadUint64()
	}
	r.MerkleProof = d.ReadBool()
	r.RevisionNumber = d.ReadUint64()
	r.ValidProofValues = make([]types.Currency, d.ReadPrefix())
	for i := range r.ValidProofValues {
		r.ValidProofValues[i].DecodeFrom(d)
	}
	r.MissedProofValues = make([]types.Currency, d.ReadPrefix())
	for i := range r.MissedProofValues {
		r.MissedProofValues[i].DecodeFrom(d)
	}
	copy(r.Signature[:], d.ReadBytes())
}

func (r *RPCReadResponse) EncodeTo(e *types.Encoder) {
	e.WriteBytes(r.Signature[:])
	e.WriteBytes(r.Data)
	e.WritePrefix(len(r.MerkleProof))
	for i := range r.MerkleProof {
		e.Write(r.MerkleProof[i][:])
	}
}

func (r *RPCReadResponse) DecodeFrom(d *types.Decoder) {
	copy(r.Signature[:], d.ReadBytes())

	// r.Data will typically be large (4 MiB), so reuse the existing capacity if
	// possible.
	//
	// NOTE: for maximum efficiency, we should be doing this for every slice,
	// but in most cases the extra performance isn't worth the aliasing issues.
	dataLen := d.ReadPrefix()
	if cap(r.Data) < dataLen {
		r.Data = make([]byte, dataLen)
	}
	r.Data = r.Data[:dataLen]
	d.Read(r.Data)

	r.MerkleProof = make([]types.Hash256, d.ReadPrefix())
	for i := range r.MerkleProof {
		d.Read(r.MerkleProof[i][:])
	}
}

// RPCSectorRoots

func (r *RPCSectorRootsRequest) EncodeTo(e *types.Encoder) {
	e.WriteUint64(r.RootOffset)
	e.WriteUint64(r.NumRoots)
	e.WriteUint64(r.RevisionNumber)
	e.WritePrefix(len(r.ValidProofValues))
	for i := range r.ValidProofValues {
		r.ValidProofValues[i].EncodeTo(e)
	}
	e.WritePrefix(len(r.MissedProofValues))
	for i := range r.MissedProofValues {
		r.MissedProofValues[i].EncodeTo(e)
	}
	e.WriteBytes(r.Signature[:])
}

func (r *RPCSectorRootsRequest) DecodeFrom(d *types.Decoder) {
	r.RootOffset = d.ReadUint64()
	r.NumRoots = d.ReadUint64()
	r.RevisionNumber = d.ReadUint64()
	r.ValidProofValues = make([]types.Currency, d.ReadPrefix())
	for i := range r.ValidProofValues {
		r.ValidProofValues[i].DecodeFrom(d)
	}
	r.MissedProofValues = make([]types.Currency, d.ReadPrefix())
	for i := range r.MissedProofValues {
		r.MissedProofValues[i].DecodeFrom(d)
	}
	copy(r.Signature[:], d.ReadBytes())
}

func (r *RPCSectorRootsResponse) EncodeTo(e *types.Encoder) {
	e.WriteBytes(r.Signature[:])
	e.WritePrefix(len(r.SectorRoots))
	for i := range r.SectorRoots {
		e.Write(r.SectorRoots[i][:])
	}
	e.WritePrefix(len(r.MerkleProof))
	for i := range r.MerkleProof {
		e.Write(r.MerkleProof[i][:])
	}
}

func (r *RPCSectorRootsResponse) DecodeFrom(d *types.Decoder) {
	copy(r.Signature[:], d.ReadBytes())
	r.SectorRoots = make([]types.Hash256, d.ReadPrefix())
	for i := range r.SectorRoots {
		d.Read(r.SectorRoots[i][:])
	}
	r.MerkleProof = make([]types.Hash256, d.ReadPrefix())
	for i := range r.MerkleProof {
		d.Read(r.MerkleProof[i][:])
	}
}

// RPCSettings

func (r *RPCSettingsResponse) EncodeTo(e *types.Encoder) {
	e.WriteBytes(r.Settings)
}

func (r *RPCSettingsResponse) DecodeFrom(d *types.Decoder) {
	r.Settings = d.ReadBytes()
}

// RPCWrite

func (r *RPCWriteRequest) EncodeTo(e *types.Encoder) {
	e.WritePrefix(len(r.Actions))
	for i := range r.Actions {
		e.Write(r.Actions[i].Type[:])
		e.WriteUint64(r.Actions[i].A)
		e.WriteUint64(r.Actions[i].B)
		e.WriteBytes(r.Actions[i].Data)
	}
	e.WriteBool(r.MerkleProof)
	e.WriteUint64(r.RevisionNumber)
	e.WritePrefix(len(r.ValidProofValues))
	for i := range r.ValidProofValues {
		r.ValidProofValues[i].EncodeTo(e)
	}
	e.WritePrefix(len(r.MissedProofValues))
	for i := range r.MissedProofValues {
		r.MissedProofValues[i].EncodeTo(e)
	}
}

func (r *RPCWriteRequest) DecodeFrom(d *types.Decoder) {
	r.Actions = make([]RPCWriteAction, d.ReadPrefix())
	for i := range r.Actions {
		d.Read(r.Actions[i].Type[:])
		r.Actions[i].A = d.ReadUint64()
		r.Actions[i].B = d.ReadUint64()
		r.Actions[i].Data = d.ReadBytes()
	}
	r.MerkleProof = d.ReadBool()
	r.RevisionNumber = d.ReadUint64()
	r.ValidProofValues = make([]types.Currency, d.ReadPrefix())
	for i := range r.ValidProofValues {
		r.ValidProofValues[i].DecodeFrom(d)
	}
	r.MissedProofValues = make([]types.Currency, d.ReadPrefix())
	for i := range r.MissedProofValues {
		r.MissedProofValues[i].DecodeFrom(d)
	}
}

func (r *RPCWriteMerkleProof) EncodeTo(e *types.Encoder) {
	e.WritePrefix(len(r.OldSubtreeHashes))
	for i := range r.OldSubtreeHashes {
		e.Write(r.OldSubtreeHashes[i][:])
	}
	e.WritePrefix(len(r.OldLeafHashes))
	for i := range r.OldLeafHashes {
		e.Write(r.OldLeafHashes[i][:])
	}
	e.Write(r.NewMerkleRoot[:])
}

func (r *RPCWriteMerkleProof) DecodeFrom(d *types.Decoder) {
	r.OldSubtreeHashes = make([]types.Hash256, d.ReadPrefix())
	for i := range r.OldSubtreeHashes {
		d.Read(r.OldSubtreeHashes[i][:])
	}
	r.OldLeafHashes = make([]types.Hash256, d.ReadPrefix())
	for i := range r.OldLeafHashes {
		d.Read(r.OldLeafHashes[i][:])
	}
	d.Read(r.NewMerkleRoot[:])
}

func (r *RPCWriteResponse) EncodeTo(e *types.Encoder) {
	e.WriteBytes(r.Signature[:])
}

func (r *RPCWriteResponse) DecodeFrom(d *types.Decoder) {
	copy(r.Signature[:], d.ReadBytes())
}
