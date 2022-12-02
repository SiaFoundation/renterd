package rhp

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/big"
	"reflect"
	"unsafe"

	"gitlab.com/NebulousLabs/encoding"
	"go.sia.tech/siad/crypto"
	"go.sia.tech/siad/types"
)

var (
	sizeofCurrency             = (&objCurrency{}).marshalledSize()
	sizeofFileContract         = (&objFileContract{}).marshalledSize()
	sizeofFileContractRevision = (&objFileContractRevision{}).marshalledSize()
	sizeofSiacoinInput         = (&objSiacoinInput{}).marshalledSize()
	sizeofSiacoinOutput        = (&objSiacoinOutput{}).marshalledSize()
	sizeofSiafundInput         = (&objSiafundInput{}).marshalledSize()
	sizeofSiafundOutput        = (&objSiafundOutput{}).marshalledSize()
	sizeofSiaPublicKey         = (&objSiaPublicKey{}).marshalledSize()
	sizeofSpecifier            = (&Specifier{}).marshalledSize()
	sizeofStorageProof         = (&objStorageProof{}).marshalledSize()
	sizeofTransaction          = (&objTransaction{}).marshalledSize()
	sizeofTransactionSignature = (&objTransactionSignature{}).marshalledSize()
)

// A ProtocolObject is an object that can be serialized for transport in the
// renter-host protocol.
type ProtocolObject interface {
	marshalledSize() int
	marshalBuffer(b *objBuffer)
	unmarshalBuffer(b *objBuffer) error
}

type objBuffer struct {
	buf bytes.Buffer
	lr  io.LimitedReader
	err error // sticky
}

func (b *objBuffer) grow(n int)        { b.buf.Grow(n) }
func (b *objBuffer) bytes() []byte     { return b.buf.Bytes() }
func (b *objBuffer) next(n int) []byte { return b.buf.Next(n) }

func (b *objBuffer) write(p []byte) {
	b.buf.Write(p)
}

func (b *objBuffer) read(p []byte) {
	if b.err != nil {
		return
	}
	_, b.err = io.ReadFull(&b.buf, p)
}

func (b *objBuffer) writeString(s string) {
	b.writePrefix(len(s))
	b.buf.WriteString(s)
}

func (b *objBuffer) writeBool(p bool) {
	if p {
		b.buf.WriteByte(1)
	} else {
		b.buf.WriteByte(0)
	}
}

func (b *objBuffer) readBool() bool {
	if b.err != nil {
		return false
	}
	c, err := b.buf.ReadByte()
	if err != nil {
		b.err = err
		return false
	}
	if c != 0 && c != 1 {
		b.err = errors.New("invalid boolean")
		return false
	}
	return c == 1
}

func (b *objBuffer) writeUint64(u uint64) {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, u)
	b.buf.Write(buf)
}

func (b *objBuffer) readUint64() uint64 {
	if b.err != nil {
		return 0
	}
	buf := b.buf.Next(8)
	if len(buf) < 8 {
		b.err = io.EOF
		return 0
	}
	return binary.LittleEndian.Uint64(buf)
}

func (b *objBuffer) writePrefix(i int) {
	b.writeUint64(uint64(i))
}

func (b *objBuffer) readPrefix(elemSize int) int {
	n := b.readUint64()
	if n > uint64(b.buf.Len()/elemSize) {
		b.err = fmt.Errorf("marshalled object contains invalid length prefix (%v elems x %v bytes/elem > %v bytes left in message)", n, elemSize, b.buf.Len())
		return 0
	}
	return int(n)
}

func (b *objBuffer) writePrefixedBytes(p []byte) {
	b.writePrefix(len(p))
	b.write(p)
}

func (b *objBuffer) readPrefixedBytes() []byte {
	p := make([]byte, b.readPrefix(1))
	b.read(p)
	return p
}

func (b *objBuffer) copyN(r io.Reader, n uint64) error {
	if b.err != nil {
		return b.err
	}
	b.lr = io.LimitedReader{R: r, N: int64(n)}
	read, err := b.buf.ReadFrom(&b.lr)
	if err != nil {
		b.err = err
	} else if read != int64(n) {
		b.err = io.ErrUnexpectedEOF
	}
	return b.err
}

func (b *objBuffer) Err() error {
	return b.err
}

func (b *objBuffer) reset() {
	b.buf.Reset()
	b.err = nil
}

// Specifier

func (s *Specifier) marshalledSize() int {
	return 16
}

func (s *Specifier) marshalBuffer(b *objBuffer) {
	b.write(s[:])
}

func (s *Specifier) unmarshalBuffer(b *objBuffer) error {
	b.read(s[:])
	return b.Err()
}

// RPCError

func (e *RPCError) marshalledSize() int {
	return len(e.Type) + 8 + len(e.Data) + 8 + len(e.Description)
}

func (e *RPCError) marshalBuffer(b *objBuffer) {
	e.Type.marshalBuffer(b)
	b.writePrefixedBytes(e.Data)
	b.writeString(e.Description)
}

func (e *RPCError) unmarshalBuffer(b *objBuffer) error {
	e.Type.unmarshalBuffer(b)
	e.Data = b.readPrefixedBytes()
	desc := b.readPrefixedBytes()
	e.Description = string(desc)
	return b.Err()
}

// rpcResponse

func (resp *rpcResponse) marshalledSize() int {
	if resp.err != nil {
		return 1 + resp.err.marshalledSize()
	}
	return 1 + resp.data.marshalledSize()
}

func (resp *rpcResponse) marshalBuffer(b *objBuffer) {
	b.writeBool(resp.err != nil)
	if resp.err != nil {
		resp.err.marshalBuffer(b)
	} else {
		resp.data.marshalBuffer(b)
	}
}

func (resp *rpcResponse) unmarshalBuffer(b *objBuffer) error {
	if isErr := b.readBool(); isErr {
		resp.err = new(RPCError)
		resp.err.unmarshalBuffer(b)
	} else {
		resp.data.unmarshalBuffer(b)
	}
	return b.Err()
}

// RPCFormContract

func (r *RPCFormContractRequest) marshalledSize() int {
	txnsSize := 8
	for i := range r.Transactions {
		txnsSize += (*objTransaction)(&r.Transactions[i]).marshalledSize()
	}
	return txnsSize + (*objSiaPublicKey)(&r.RenterKey).marshalledSize()
}

func (r *RPCFormContractRequest) marshalBuffer(b *objBuffer) {
	b.writePrefix(len(r.Transactions))
	for i := range r.Transactions {
		(*objTransaction)(&r.Transactions[i]).marshalBuffer(b)
	}
	(*objSiaPublicKey)(&r.RenterKey).marshalBuffer(b)
}

func (r *RPCFormContractRequest) unmarshalBuffer(b *objBuffer) error {
	r.Transactions = make([]types.Transaction, b.readPrefix(sizeofTransaction))
	for i := range r.Transactions {
		(*objTransaction)(&r.Transactions[i]).unmarshalBuffer(b)
	}
	(*objSiaPublicKey)(&r.RenterKey).unmarshalBuffer(b)
	return b.Err()
}

func (r *RPCFormContractAdditions) marshalledSize() int {
	parentsSize := 8
	for i := range r.Parents {
		parentsSize += (*objTransaction)(&r.Parents[i]).marshalledSize()
	}
	inputsSize := 8
	for i := range r.Inputs {
		inputsSize += (*objSiacoinInput)(&r.Inputs[i]).marshalledSize()
	}
	outputsSize := 8
	for i := range r.Outputs {
		outputsSize += (*objSiacoinOutput)(&r.Outputs[i]).marshalledSize()
	}
	return parentsSize + inputsSize + outputsSize
}

func (r *RPCFormContractAdditions) marshalBuffer(b *objBuffer) {
	b.writePrefix(len(r.Parents))
	for i := range r.Parents {
		(*objTransaction)(&r.Parents[i]).marshalBuffer(b)
	}
	b.writePrefix(len(r.Inputs))
	for i := range r.Inputs {
		(*objSiacoinInput)(&r.Inputs[i]).marshalBuffer(b)
	}
	b.writePrefix(len(r.Outputs))
	for i := range r.Outputs {
		(*objSiacoinOutput)(&r.Outputs[i]).marshalBuffer(b)
	}
}

func (r *RPCFormContractAdditions) unmarshalBuffer(b *objBuffer) error {
	r.Parents = make([]types.Transaction, b.readPrefix(sizeofTransaction))
	for i := range r.Parents {
		(*objTransaction)(&r.Parents[i]).unmarshalBuffer(b)
	}
	r.Inputs = make([]types.SiacoinInput, b.readPrefix(sizeofSiacoinInput))
	for i := range r.Inputs {
		(*objSiacoinInput)(&r.Inputs[i]).unmarshalBuffer(b)
	}
	r.Outputs = make([]types.SiacoinOutput, b.readPrefix(sizeofSiacoinOutput))
	for i := range r.Outputs {
		(*objSiacoinOutput)(&r.Outputs[i]).unmarshalBuffer(b)
	}
	return b.Err()
}

func (r *RPCFormContractSignatures) marshalledSize() int {
	sigsSize := 8
	for i := range r.ContractSignatures {
		sigsSize += (*objTransactionSignature)(&r.ContractSignatures[i]).marshalledSize()
	}
	return sigsSize + (*objTransactionSignature)(&r.RevisionSignature).marshalledSize()
}

func (r *RPCFormContractSignatures) marshalBuffer(b *objBuffer) {
	b.writePrefix(len(r.ContractSignatures))
	for i := range r.ContractSignatures {
		(*objTransactionSignature)(&r.ContractSignatures[i]).marshalBuffer(b)
	}
	(*objTransactionSignature)(&r.RevisionSignature).marshalBuffer(b)
}

func (r *RPCFormContractSignatures) unmarshalBuffer(b *objBuffer) error {
	r.ContractSignatures = make([]types.TransactionSignature, b.readPrefix(sizeofTransactionSignature))
	for i := range r.ContractSignatures {
		(*objTransactionSignature)(&r.ContractSignatures[i]).unmarshalBuffer(b)
	}
	(*objTransactionSignature)(&r.RevisionSignature).unmarshalBuffer(b)
	return b.Err()
}

// RPCRenewAndClear

func (r *RPCRenewAndClearContractRequest) marshalledSize() int {
	txnsSize := 24
	for i := range r.Transactions {
		txnsSize += (*objTransaction)(&r.Transactions[i]).marshalledSize()
	}
	txnsSize += (*objSiaPublicKey)(&r.RenterKey).marshalledSize()
	for i := range r.FinalValidProofValues {
		txnsSize += (*objCurrency)(&r.FinalValidProofValues[i]).marshalledSize()
	}
	for i := range r.FinalMissedProofValues {
		txnsSize += (*objCurrency)(&r.FinalMissedProofValues[i]).marshalledSize()
	}
	return txnsSize
}

func (r *RPCRenewAndClearContractRequest) marshalBuffer(b *objBuffer) {
	b.writePrefix(len(r.Transactions))
	for i := range r.Transactions {
		(*objTransaction)(&r.Transactions[i]).marshalBuffer(b)
	}
	(*objSiaPublicKey)(&r.RenterKey).marshalBuffer(b)
	b.writePrefix(len(r.FinalValidProofValues))
	for i := range r.FinalValidProofValues {
		(*objCurrency)(&r.FinalValidProofValues[i]).marshalBuffer(b)
	}
	b.writePrefix(len(r.FinalMissedProofValues))
	for i := range r.FinalMissedProofValues {
		(*objCurrency)(&r.FinalMissedProofValues[i]).marshalBuffer(b)
	}
}

func (r *RPCRenewAndClearContractRequest) unmarshalBuffer(b *objBuffer) error {
	r.Transactions = make([]types.Transaction, b.readPrefix(sizeofTransaction))
	for i := range r.Transactions {
		(*objTransaction)(&r.Transactions[i]).unmarshalBuffer(b)
	}
	(*objSiaPublicKey)(&r.RenterKey).unmarshalBuffer(b)
	r.FinalValidProofValues = make([]types.Currency, b.readPrefix(sizeofCurrency))
	for i := range r.FinalValidProofValues {
		(*objCurrency)(&r.FinalValidProofValues[i]).unmarshalBuffer(b)
	}
	r.FinalMissedProofValues = make([]types.Currency, b.readPrefix(sizeofCurrency))
	for i := range r.FinalMissedProofValues {
		(*objCurrency)(&r.FinalMissedProofValues[i]).unmarshalBuffer(b)
	}
	return b.Err()
}

func (r *RPCRenewAndClearContractSignatures) marshalledSize() int {
	sigsSize := 16
	for i := range r.ContractSignatures {
		sigsSize += (*objTransactionSignature)(&r.ContractSignatures[i]).marshalledSize()
	}
	return sigsSize + (*objTransactionSignature)(&r.RevisionSignature).marshalledSize() + len(r.FinalRevisionSignature)
}

func (r *RPCRenewAndClearContractSignatures) marshalBuffer(b *objBuffer) {
	b.writePrefix(len(r.ContractSignatures))
	for i := range r.ContractSignatures {
		(*objTransactionSignature)(&r.ContractSignatures[i]).marshalBuffer(b)
	}
	(*objTransactionSignature)(&r.RevisionSignature).marshalBuffer(b)
	b.writePrefixedBytes(r.FinalRevisionSignature[:])
}

func (r *RPCRenewAndClearContractSignatures) unmarshalBuffer(b *objBuffer) error {
	r.ContractSignatures = make([]types.TransactionSignature, b.readPrefix(sizeofTransactionSignature))
	for i := range r.ContractSignatures {
		(*objTransactionSignature)(&r.ContractSignatures[i]).unmarshalBuffer(b)
	}
	(*objTransactionSignature)(&r.RevisionSignature).unmarshalBuffer(b)
	copy(r.FinalRevisionSignature[:], b.readPrefixedBytes())
	return b.Err()
}

// RPCLock

func (r *RPCLockRequest) marshalledSize() int {
	return len(r.ContractID) + 8 + len(r.Signature) + 8
}

func (r *RPCLockRequest) marshalBuffer(b *objBuffer) {
	b.write(r.ContractID[:])
	b.writePrefixedBytes(r.Signature[:])
	b.writeUint64(r.Timeout)
}

func (r *RPCLockRequest) unmarshalBuffer(b *objBuffer) error {
	b.read(r.ContractID[:])
	copy(r.Signature[:], b.readPrefixedBytes())
	r.Timeout = b.readUint64()
	return b.Err()
}

func (r *RPCLockResponse) marshalledSize() int {
	sigsSize := 8
	for i := range r.Signatures {
		sigsSize += (*objTransactionSignature)(&r.Signatures[i]).marshalledSize()
	}
	return 1 + len(r.NewChallenge) + (*objFileContractRevision)(&r.Revision).marshalledSize() + sigsSize
}

func (r *RPCLockResponse) marshalBuffer(b *objBuffer) {
	b.writeBool(r.Acquired)
	b.write(r.NewChallenge[:])
	(*objFileContractRevision)(&r.Revision).marshalBuffer(b)
	b.writePrefix(len(r.Signatures))
	for i := range r.Signatures {
		(*objTransactionSignature)(&r.Signatures[i]).marshalBuffer(b)
	}
}

func (r *RPCLockResponse) unmarshalBuffer(b *objBuffer) error {
	r.Acquired = b.readBool()
	b.read(r.NewChallenge[:])
	(*objFileContractRevision)(&r.Revision).unmarshalBuffer(b)
	r.Signatures = make([]types.TransactionSignature, b.readPrefix(sizeofTransactionSignature))
	for i := range r.Signatures {
		(*objTransactionSignature)(&r.Signatures[i]).unmarshalBuffer(b)
	}
	return b.Err()
}

// RPCRead

func (r *RPCReadRequest) marshalledSize() int {
	sectionsSize := 8 + len(r.Sections)*(32+8+8)
	validSize := 8
	for i := range r.NewValidProofValues {
		validSize += (*objCurrency)(&r.NewValidProofValues[i]).marshalledSize()
	}
	missedSize := 8
	for i := range r.NewMissedProofValues {
		missedSize += (*objCurrency)(&r.NewMissedProofValues[i]).marshalledSize()
	}
	return sectionsSize + 1 + 8 + validSize + missedSize + 8 + len(r.Signature)
}

func (r *RPCReadRequest) marshalBuffer(b *objBuffer) {
	b.writePrefix(len(r.Sections))
	for i := range r.Sections {
		b.write(r.Sections[i].MerkleRoot[:])
		b.writeUint64(uint64(r.Sections[i].Offset))
		b.writeUint64(uint64(r.Sections[i].Length))
	}
	b.writeBool(r.MerkleProof)
	b.writeUint64(r.NewRevisionNumber)
	b.writePrefix(len(r.NewValidProofValues))
	for i := range r.NewValidProofValues {
		(*objCurrency)(&r.NewValidProofValues[i]).marshalBuffer(b)
	}
	b.writePrefix(len(r.NewMissedProofValues))
	for i := range r.NewMissedProofValues {
		(*objCurrency)(&r.NewMissedProofValues[i]).marshalBuffer(b)
	}
	b.writePrefixedBytes(r.Signature[:])
}

func (r *RPCReadRequest) unmarshalBuffer(b *objBuffer) error {
	r.Sections = make([]RPCReadRequestSection, b.readPrefix(32+8+8))
	for i := range r.Sections {
		b.read(r.Sections[i].MerkleRoot[:])
		r.Sections[i].Offset = b.readUint64()
		r.Sections[i].Length = b.readUint64()
	}
	r.MerkleProof = b.readBool()
	r.NewRevisionNumber = b.readUint64()
	r.NewValidProofValues = make([]types.Currency, b.readPrefix(sizeofCurrency))
	for i := range r.NewValidProofValues {
		(*objCurrency)(&r.NewValidProofValues[i]).unmarshalBuffer(b)
	}
	r.NewMissedProofValues = make([]types.Currency, b.readPrefix(sizeofCurrency))
	for i := range r.NewMissedProofValues {
		(*objCurrency)(&r.NewMissedProofValues[i]).unmarshalBuffer(b)
	}
	copy(r.Signature[:], b.readPrefixedBytes())
	return b.Err()
}

func (r *RPCReadResponse) marshalledSize() int {
	return 8 + len(r.Signature) + 8 + len(r.Data) + 8 + len(r.MerkleProof)*32
}

func (r *RPCReadResponse) marshalBuffer(b *objBuffer) {
	b.writePrefixedBytes(r.Signature[:])
	b.writePrefixedBytes(r.Data)
	b.writePrefix(len(r.MerkleProof))
	for i := range r.MerkleProof {
		b.write(r.MerkleProof[i][:])
	}
}

func (r *RPCReadResponse) unmarshalBuffer(b *objBuffer) error {
	copy(r.Signature[:], b.readPrefixedBytes())

	// r.Data will typically be large (4 MiB), so reuse the existing capacity if
	// possible.
	//
	// NOTE: for maximum efficiency, we should be doing this for every slice,
	// but in most cases the extra performance isn't worth the aliasing issues.
	dataLen := b.readPrefix(1)
	if cap(r.Data) < dataLen {
		r.Data = make([]byte, dataLen)
	}
	r.Data = r.Data[:dataLen]
	b.read(r.Data)

	r.MerkleProof = make([]Hash256, b.readPrefix(32))
	for i := range r.MerkleProof {
		b.read(r.MerkleProof[i][:])
	}
	return b.Err()
}

// RPCSectorRoots

func (r *RPCSectorRootsRequest) marshalledSize() int {
	validSize := 8
	for i := range r.NewValidProofValues {
		validSize += (*objCurrency)(&r.NewValidProofValues[i]).marshalledSize()
	}
	missedSize := 8
	for i := range r.NewMissedProofValues {
		missedSize += (*objCurrency)(&r.NewMissedProofValues[i]).marshalledSize()
	}
	return 8 + 8 + 8 + validSize + missedSize + 8 + len(r.Signature)
}

func (r *RPCSectorRootsRequest) marshalBuffer(b *objBuffer) {
	b.writeUint64(r.RootOffset)
	b.writeUint64(r.NumRoots)
	b.writeUint64(r.NewRevisionNumber)
	b.writePrefix(len(r.NewValidProofValues))
	for i := range r.NewValidProofValues {
		(*objCurrency)(&r.NewValidProofValues[i]).marshalBuffer(b)
	}
	b.writePrefix(len(r.NewMissedProofValues))
	for i := range r.NewMissedProofValues {
		(*objCurrency)(&r.NewMissedProofValues[i]).marshalBuffer(b)
	}
	b.writePrefixedBytes(r.Signature[:])
}

func (r *RPCSectorRootsRequest) unmarshalBuffer(b *objBuffer) error {
	r.RootOffset = b.readUint64()
	r.NumRoots = b.readUint64()
	r.NewRevisionNumber = b.readUint64()
	r.NewValidProofValues = make([]types.Currency, b.readPrefix(sizeofCurrency))
	for i := range r.NewValidProofValues {
		(*objCurrency)(&r.NewValidProofValues[i]).unmarshalBuffer(b)
	}
	r.NewMissedProofValues = make([]types.Currency, b.readPrefix(sizeofCurrency))
	for i := range r.NewMissedProofValues {
		(*objCurrency)(&r.NewMissedProofValues[i]).unmarshalBuffer(b)
	}
	copy(r.Signature[:], b.readPrefixedBytes())
	return b.Err()
}

func (r *RPCSectorRootsResponse) marshalledSize() int {
	return 8 + len(r.Signature) + 8 + len(r.SectorRoots)*32 + 8 + len(r.MerkleProof)*32
}

func (r *RPCSectorRootsResponse) marshalBuffer(b *objBuffer) {
	b.writePrefixedBytes(r.Signature[:])
	b.writePrefix(len(r.SectorRoots))
	for i := range r.SectorRoots {
		b.write(r.SectorRoots[i][:])
	}
	b.writePrefix(len(r.MerkleProof))
	for i := range r.MerkleProof {
		b.write(r.MerkleProof[i][:])
	}
}

func (r *RPCSectorRootsResponse) unmarshalBuffer(b *objBuffer) error {
	copy(r.Signature[:], b.readPrefixedBytes())
	r.SectorRoots = make([]Hash256, b.readPrefix(32))
	for i := range r.SectorRoots {
		b.read(r.SectorRoots[i][:])
	}
	r.MerkleProof = make([]Hash256, b.readPrefix(32))
	for i := range r.MerkleProof {
		b.read(r.MerkleProof[i][:])
	}
	return b.Err()
}

// RPCSettings

func (r *RPCSettingsResponse) marshalledSize() int {
	return 8 + len(r.Settings)
}

func (r *RPCSettingsResponse) marshalBuffer(b *objBuffer) {
	b.writePrefixedBytes(r.Settings)
}

func (r *RPCSettingsResponse) unmarshalBuffer(b *objBuffer) error {
	r.Settings = b.readPrefixedBytes()
	return b.Err()
}

// RPCWrite

func (r *RPCWriteRequest) marshalledSize() int {
	actionsSize := 8
	for i := range r.Actions {
		actionsSize += 8 + 8 + 8 + len(r.Actions[i].Data) + len(r.Actions[i].Type)
	}
	validSize := 8
	for i := range r.NewValidProofValues {
		validSize += (*objCurrency)(&r.NewValidProofValues[i]).marshalledSize()
	}
	missedSize := 8
	for i := range r.NewMissedProofValues {
		missedSize += (*objCurrency)(&r.NewMissedProofValues[i]).marshalledSize()
	}
	return actionsSize + 1 + 8 + validSize + missedSize
}

func (r *RPCWriteRequest) marshalBuffer(b *objBuffer) {
	b.writePrefix(len(r.Actions))
	for i := range r.Actions {
		b.write(r.Actions[i].Type[:])
		b.writeUint64(r.Actions[i].A)
		b.writeUint64(r.Actions[i].B)
		b.writePrefixedBytes(r.Actions[i].Data)
	}
	b.writeBool(r.MerkleProof)
	b.writeUint64(r.NewRevisionNumber)
	b.writePrefix(len(r.NewValidProofValues))
	for i := range r.NewValidProofValues {
		(*objCurrency)(&r.NewValidProofValues[i]).marshalBuffer(b)
	}
	b.writePrefix(len(r.NewMissedProofValues))
	for i := range r.NewMissedProofValues {
		(*objCurrency)(&r.NewMissedProofValues[i]).marshalBuffer(b)
	}
}

func (r *RPCWriteRequest) unmarshalBuffer(b *objBuffer) error {
	r.Actions = make([]RPCWriteAction, b.readPrefix(16+8+8+8))
	for i := range r.Actions {
		b.read(r.Actions[i].Type[:])
		r.Actions[i].A = b.readUint64()
		r.Actions[i].B = b.readUint64()
		r.Actions[i].Data = b.readPrefixedBytes()
	}
	r.MerkleProof = b.readBool()
	r.NewRevisionNumber = b.readUint64()
	r.NewValidProofValues = make([]types.Currency, b.readPrefix(sizeofCurrency))
	for i := range r.NewValidProofValues {
		(*objCurrency)(&r.NewValidProofValues[i]).unmarshalBuffer(b)
	}
	r.NewMissedProofValues = make([]types.Currency, b.readPrefix(sizeofCurrency))
	for i := range r.NewMissedProofValues {
		(*objCurrency)(&r.NewMissedProofValues[i]).unmarshalBuffer(b)
	}
	return b.Err()
}

func (r *RPCWriteMerkleProof) marshalledSize() int {
	return 8 + len(r.OldSubtreeHashes)*32 + 8 + len(r.OldLeafHashes)*32 + len(r.NewMerkleRoot)
}

func (r *RPCWriteMerkleProof) marshalBuffer(b *objBuffer) {
	b.writePrefix(len(r.OldSubtreeHashes))
	for i := range r.OldSubtreeHashes {
		b.write(r.OldSubtreeHashes[i][:])
	}
	b.writePrefix(len(r.OldLeafHashes))
	for i := range r.OldLeafHashes {
		b.write(r.OldLeafHashes[i][:])
	}
	b.write(r.NewMerkleRoot[:])
}

func (r *RPCWriteMerkleProof) unmarshalBuffer(b *objBuffer) error {
	r.OldSubtreeHashes = make([]Hash256, b.readPrefix(32))
	for i := range r.OldSubtreeHashes {
		b.read(r.OldSubtreeHashes[i][:])
	}
	r.OldLeafHashes = make([]Hash256, b.readPrefix(32))
	for i := range r.OldLeafHashes {
		b.read(r.OldLeafHashes[i][:])
	}
	b.read(r.NewMerkleRoot[:])
	return b.Err()
}

func (r *RPCWriteResponse) marshalledSize() int {
	return 8 + len(r.Signature)
}

func (r *RPCWriteResponse) marshalBuffer(b *objBuffer) {
	b.writePrefixedBytes(r.Signature[:])
}

func (r *RPCWriteResponse) unmarshalBuffer(b *objBuffer) error {
	copy(r.Signature[:], b.readPrefixedBytes())
	return b.Err()
}

// generic objects

type objSiaPublicKey types.SiaPublicKey

func (spk *objSiaPublicKey) marshalledSize() int {
	return len(spk.Algorithm) + 8 + len(spk.Key)
}

func (spk *objSiaPublicKey) marshalBuffer(b *objBuffer) {
	b.write(spk.Algorithm[:])
	b.writePrefixedBytes(spk.Key)
}

func (spk *objSiaPublicKey) unmarshalBuffer(b *objBuffer) error {
	b.read(spk.Algorithm[:])
	spk.Key = b.readPrefixedBytes()
	return b.Err()
}

type objCoveredFields types.CoveredFields

func (cf *objCoveredFields) foreach(fn func(*[]uint64)) {
	fn(&cf.SiacoinInputs)
	fn(&cf.SiacoinOutputs)
	fn(&cf.FileContracts)
	fn(&cf.FileContractRevisions)
	fn(&cf.StorageProofs)
	fn(&cf.SiafundInputs)
	fn(&cf.SiafundOutputs)
	fn(&cf.MinerFees)
	fn(&cf.ArbitraryData)
	fn(&cf.TransactionSignatures)
}

func (cf *objCoveredFields) marshalledSize() int {
	fieldsSize := 0
	cf.foreach(func(f *[]uint64) {
		fieldsSize += 8 + len(*f)*8
	})
	return 1 + fieldsSize
}

func (cf *objCoveredFields) marshalBuffer(b *objBuffer) {
	b.writeBool(cf.WholeTransaction)
	cf.foreach(func(f *[]uint64) {
		b.writePrefix(len(*f))
		for _, u := range *f {
			b.writeUint64(u)
		}
	})
}

func (cf *objCoveredFields) unmarshalBuffer(b *objBuffer) error {
	cf.WholeTransaction = b.readBool()
	cf.foreach(func(f *[]uint64) {
		*f = make([]uint64, b.readPrefix(8))
		for i := range *f {
			(*f)[i] = b.readUint64()
		}
	})
	return b.Err()
}

type objTransactionSignature types.TransactionSignature

func (sig *objTransactionSignature) marshalledSize() int {
	return len(sig.ParentID) + 8 + 8 + (*objCoveredFields)(&sig.CoveredFields).marshalledSize() + 8 + len(sig.Signature)
}

func (sig *objTransactionSignature) marshalBuffer(b *objBuffer) {
	b.write(sig.ParentID[:])
	b.writeUint64(sig.PublicKeyIndex)
	b.writeUint64(uint64(sig.Timelock))
	(*objCoveredFields)(&sig.CoveredFields).marshalBuffer(b)
	b.writePrefixedBytes(sig.Signature)
}

func (sig *objTransactionSignature) unmarshalBuffer(b *objBuffer) error {
	b.read(sig.ParentID[:])
	sig.PublicKeyIndex = b.readUint64()
	sig.Timelock = types.BlockHeight(b.readUint64())
	(*objCoveredFields)(&sig.CoveredFields).unmarshalBuffer(b)
	sig.Signature = b.readPrefixedBytes()
	return b.Err()
}

type objCurrency types.Currency

func (c *objCurrency) big() *big.Int {
	return &(*struct {
		i big.Int
	})(unsafe.Pointer(c)).i
}

func (c *objCurrency) marshalledSize() int {
	const (
		_m    = ^big.Word(0)
		_logS = _m>>8&1 + _m>>16&1 + _m>>32&1
		_S    = 1 << _logS
	)
	bits := c.big().Bits()
	size := len(bits) * _S
zeros:
	for i := len(bits) - 1; i >= 0; i-- {
		for j := _S - 1; j >= 0; j-- {
			if (bits[i] >> uintptr(j*8)) != 0 {
				break zeros
			}
			size--
		}
	}
	return 8 + size
}

func (c *objCurrency) marshalBuffer(b *objBuffer) {
	const (
		_m    = ^big.Word(0)
		_logS = _m>>8&1 + _m>>16&1 + _m>>32&1
		_S    = 1 << _logS
	)
	bits := c.big().Bits()
	var i int
	for i = len(bits)*_S - 1; i >= 0; i-- {
		if bits[i/_S]>>(uint(i%_S)*8) != 0 {
			break
		}
	}
	b.writePrefix(i + 1)
	for ; i >= 0; i-- {
		b.buf.WriteByte(byte(bits[i/_S] >> (uint(i%_S) * 8)))
	}
}

func (c *objCurrency) unmarshalBuffer(b *objBuffer) error {
	c.big().SetBytes(b.readPrefixedBytes())
	return b.Err()
}

type objUnlockConditions types.UnlockConditions

func (uc *objUnlockConditions) marshalledSize() int {
	keysSize := 8
	for i := range uc.PublicKeys {
		keysSize += (*objSiaPublicKey)(&uc.PublicKeys[i]).marshalledSize()
	}
	return 8 + 8 + keysSize
}

func (uc *objUnlockConditions) marshalBuffer(b *objBuffer) {
	b.writeUint64(uint64(uc.Timelock))
	b.writePrefix(len(uc.PublicKeys))
	for i := range uc.PublicKeys {
		(*objSiaPublicKey)(&uc.PublicKeys[i]).marshalBuffer(b)
	}
	b.writeUint64(uc.SignaturesRequired)
}

func (uc *objUnlockConditions) unmarshalBuffer(b *objBuffer) error {
	uc.Timelock = types.BlockHeight(b.readUint64())
	uc.PublicKeys = make([]types.SiaPublicKey, b.readPrefix(sizeofSiaPublicKey))
	for i := range uc.PublicKeys {
		(*objSiaPublicKey)(&uc.PublicKeys[i]).unmarshalBuffer(b)
	}
	uc.SignaturesRequired = b.readUint64()
	return b.Err()
}

type objSiacoinInput types.SiacoinInput

func (sci *objSiacoinInput) marshalledSize() int {
	return len(sci.ParentID) + (*objUnlockConditions)(&sci.UnlockConditions).marshalledSize()
}

func (sci *objSiacoinInput) marshalBuffer(b *objBuffer) {
	b.write(sci.ParentID[:])
	(*objUnlockConditions)(&sci.UnlockConditions).marshalBuffer(b)
}

func (sci *objSiacoinInput) unmarshalBuffer(b *objBuffer) error {
	b.read(sci.ParentID[:])
	(*objUnlockConditions)(&sci.UnlockConditions).unmarshalBuffer(b)
	return b.Err()
}

type objSiacoinOutput types.SiacoinOutput

func (sco *objSiacoinOutput) marshalledSize() int {
	return len(sco.UnlockHash) + (*objCurrency)(&sco.Value).marshalledSize()
}

func (sco *objSiacoinOutput) marshalBuffer(b *objBuffer) {
	(*objCurrency)(&sco.Value).marshalBuffer(b)
	b.write(sco.UnlockHash[:])
}

func (sco *objSiacoinOutput) unmarshalBuffer(b *objBuffer) error {
	(*objCurrency)(&sco.Value).unmarshalBuffer(b)
	b.read(sco.UnlockHash[:])
	return b.Err()
}

type objSiafundInput types.SiafundInput

func (sfi *objSiafundInput) marshalledSize() int {
	return len(sfi.ParentID) + (*objUnlockConditions)(&sfi.UnlockConditions).marshalledSize() + len(sfi.ClaimUnlockHash)
}

func (sfi *objSiafundInput) marshalBuffer(b *objBuffer) {
	b.write(sfi.ParentID[:])
	(*objUnlockConditions)(&sfi.UnlockConditions).marshalBuffer(b)
	b.write(sfi.ClaimUnlockHash[:])
}

func (sfi *objSiafundInput) unmarshalBuffer(b *objBuffer) error {
	b.read(sfi.ParentID[:])
	(*objUnlockConditions)(&sfi.UnlockConditions).unmarshalBuffer(b)
	b.read(sfi.ClaimUnlockHash[:])
	return b.Err()
}

type objSiafundOutput types.SiafundOutput

func (sfo *objSiafundOutput) marshalledSize() int {
	return (*objCurrency)(&sfo.Value).marshalledSize() + len(sfo.UnlockHash) + (*objCurrency)(&sfo.ClaimStart).marshalledSize()
}

func (sfo *objSiafundOutput) marshalBuffer(b *objBuffer) {
	(*objCurrency)(&sfo.Value).marshalBuffer(b)
	b.write(sfo.UnlockHash[:])
	(*objCurrency)(&sfo.ClaimStart).marshalBuffer(b)
}

func (sfo *objSiafundOutput) unmarshalBuffer(b *objBuffer) error {
	(*objCurrency)(&sfo.Value).unmarshalBuffer(b)
	b.read(sfo.UnlockHash[:])
	(*objCurrency)(&sfo.ClaimStart).unmarshalBuffer(b)
	return b.Err()
}

type objFileContract types.FileContract

func (fc *objFileContract) marshalledSize() int {
	validSize := 8
	for i := range fc.ValidProofOutputs {
		validSize += (*objSiacoinOutput)(&fc.ValidProofOutputs[i]).marshalledSize()
	}
	missedSize := 8
	for i := range fc.MissedProofOutputs {
		missedSize += (*objSiacoinOutput)(&fc.MissedProofOutputs[i]).marshalledSize()
	}
	return 8 + len(fc.FileMerkleRoot) + 8 + 8 + (*objCurrency)(&fc.Payout).marshalledSize() + validSize + missedSize + len(fc.UnlockHash) + 8
}

func (fc *objFileContract) marshalBuffer(b *objBuffer) {
	b.writeUint64(fc.FileSize)
	b.write(fc.FileMerkleRoot[:])
	b.writeUint64(uint64(fc.WindowStart))
	b.writeUint64(uint64(fc.WindowEnd))
	(*objCurrency)(&fc.Payout).marshalBuffer(b)
	b.writePrefix(len(fc.ValidProofOutputs))
	for i := range fc.ValidProofOutputs {
		(*objSiacoinOutput)(&fc.ValidProofOutputs[i]).marshalBuffer(b)
	}
	b.writePrefix(len(fc.MissedProofOutputs))
	for i := range fc.MissedProofOutputs {
		(*objSiacoinOutput)(&fc.MissedProofOutputs[i]).marshalBuffer(b)
	}
	b.write(fc.UnlockHash[:])
	b.writeUint64(fc.RevisionNumber)
}

func (fc *objFileContract) unmarshalBuffer(b *objBuffer) error {
	fc.FileSize = b.readUint64()
	b.read(fc.FileMerkleRoot[:])
	fc.WindowStart = types.BlockHeight(b.readUint64())
	fc.WindowEnd = types.BlockHeight(b.readUint64())
	(*objCurrency)(&fc.Payout).unmarshalBuffer(b)
	fc.ValidProofOutputs = make([]types.SiacoinOutput, b.readPrefix(sizeofSiacoinOutput))
	for i := range fc.ValidProofOutputs {
		(*objSiacoinOutput)(&fc.ValidProofOutputs[i]).unmarshalBuffer(b)
	}
	fc.MissedProofOutputs = make([]types.SiacoinOutput, b.readPrefix(sizeofSiacoinOutput))
	for i := range fc.MissedProofOutputs {
		(*objSiacoinOutput)(&fc.MissedProofOutputs[i]).unmarshalBuffer(b)
	}
	b.read(fc.UnlockHash[:])
	fc.RevisionNumber = b.readUint64()
	return b.Err()
}

type objFileContractRevision types.FileContractRevision

func (fcr *objFileContractRevision) marshalledSize() int {
	validSize := 8
	for i := range fcr.NewValidProofOutputs {
		validSize += (*objSiacoinOutput)(&fcr.NewValidProofOutputs[i]).marshalledSize()
	}
	missedSize := 8
	for i := range fcr.NewMissedProofOutputs {
		missedSize += (*objSiacoinOutput)(&fcr.NewMissedProofOutputs[i]).marshalledSize()
	}
	return len(fcr.ParentID) + (*objUnlockConditions)(&fcr.UnlockConditions).marshalledSize() + 8 + 8 + len(fcr.NewFileMerkleRoot) + 8 + 8 + validSize + missedSize + len(fcr.NewUnlockHash)
}

func (fcr *objFileContractRevision) marshalBuffer(b *objBuffer) {
	b.write(fcr.ParentID[:])
	(*objUnlockConditions)(&fcr.UnlockConditions).marshalBuffer(b)
	b.writeUint64(fcr.NewRevisionNumber)
	b.writeUint64(fcr.NewFileSize)
	b.write(fcr.NewFileMerkleRoot[:])
	b.writeUint64(uint64(fcr.NewWindowStart))
	b.writeUint64(uint64(fcr.NewWindowEnd))
	b.writePrefix(len(fcr.NewValidProofOutputs))
	for i := range fcr.NewValidProofOutputs {
		(*objSiacoinOutput)(&fcr.NewValidProofOutputs[i]).marshalBuffer(b)
	}
	b.writePrefix(len(fcr.NewMissedProofOutputs))
	for i := range fcr.NewMissedProofOutputs {
		(*objSiacoinOutput)(&fcr.NewMissedProofOutputs[i]).marshalBuffer(b)
	}
	b.write(fcr.NewUnlockHash[:])
}

func (fcr *objFileContractRevision) unmarshalBuffer(b *objBuffer) error {
	b.read(fcr.ParentID[:])
	(*objUnlockConditions)(&fcr.UnlockConditions).unmarshalBuffer(b)
	fcr.NewRevisionNumber = b.readUint64()
	fcr.NewFileSize = b.readUint64()
	b.read(fcr.NewFileMerkleRoot[:])
	fcr.NewWindowStart = types.BlockHeight(b.readUint64())
	fcr.NewWindowEnd = types.BlockHeight(b.readUint64())
	fcr.NewValidProofOutputs = make([]types.SiacoinOutput, b.readPrefix(sizeofSiacoinOutput))
	for i := range fcr.NewValidProofOutputs {
		(*objSiacoinOutput)(&fcr.NewValidProofOutputs[i]).unmarshalBuffer(b)
	}
	fcr.NewMissedProofOutputs = make([]types.SiacoinOutput, b.readPrefix(sizeofSiacoinOutput))
	for i := range fcr.NewMissedProofOutputs {
		(*objSiacoinOutput)(&fcr.NewMissedProofOutputs[i]).unmarshalBuffer(b)
	}
	b.read(fcr.NewUnlockHash[:])
	return b.Err()
}

type objStorageProof types.StorageProof

func (sp *objStorageProof) marshalledSize() int {
	return len(sp.ParentID) + len(sp.Segment) + 8 + 32*len(sp.HashSet)
}

func (sp *objStorageProof) marshalBuffer(b *objBuffer) {
	b.write(sp.ParentID[:])
	b.write(sp.Segment[:])
	b.writePrefix(len(sp.HashSet))
	for i := range sp.HashSet {
		b.write(sp.HashSet[i][:])
	}
}

func (sp *objStorageProof) unmarshalBuffer(b *objBuffer) error {
	b.read(sp.ParentID[:])
	b.read(sp.Segment[:])
	sp.HashSet = make([]crypto.Hash, b.readPrefix(32))
	for i := range sp.HashSet {
		b.read(sp.HashSet[i][:])
	}
	return b.Err()
}

type objTransaction types.Transaction

func (t *objTransaction) marshalledSize() int {
	size := 8
	for i := range t.SiacoinInputs {
		size += (*objSiacoinInput)(&t.SiacoinInputs[i]).marshalledSize()
	}
	size += 8
	for i := range t.SiacoinOutputs {
		size += (*objSiacoinOutput)(&t.SiacoinOutputs[i]).marshalledSize()
	}
	size += 8
	for i := range t.FileContracts {
		size += (*objFileContract)(&t.FileContracts[i]).marshalledSize()
	}
	size += 8
	for i := range t.FileContractRevisions {
		size += (*objFileContractRevision)(&t.FileContractRevisions[i]).marshalledSize()
	}
	size += 8
	for i := range t.StorageProofs {
		size += (*objStorageProof)(&t.StorageProofs[i]).marshalledSize()
	}
	size += 8
	for i := range t.SiafundInputs {
		size += (*objSiafundInput)(&t.SiafundInputs[i]).marshalledSize()
	}
	size += 8
	for i := range t.SiafundOutputs {
		size += (*objSiafundOutput)(&t.SiafundOutputs[i]).marshalledSize()
	}
	size += 8
	for i := range t.MinerFees {
		size += (*objCurrency)(&t.MinerFees[i]).marshalledSize()
	}
	size += 8
	for i := range t.ArbitraryData {
		size += 8 + len(t.ArbitraryData[i])
	}
	size += 8
	for i := range t.TransactionSignatures {
		size += (*objTransactionSignature)(&t.TransactionSignatures[i]).marshalledSize()
	}
	return size
}

func (t *objTransaction) marshalBuffer(b *objBuffer) {
	b.writePrefix(len(t.SiacoinInputs))
	for i := range t.SiacoinInputs {
		(*objSiacoinInput)(&t.SiacoinInputs[i]).marshalBuffer(b)
	}
	b.writePrefix(len(t.SiacoinOutputs))
	for i := range t.SiacoinOutputs {
		(*objSiacoinOutput)(&t.SiacoinOutputs[i]).marshalBuffer(b)
	}
	b.writePrefix(len(t.FileContracts))
	for i := range t.FileContracts {
		(*objFileContract)(&t.FileContracts[i]).marshalBuffer(b)
	}
	b.writePrefix(len(t.FileContractRevisions))
	for i := range t.FileContractRevisions {
		(*objFileContractRevision)(&t.FileContractRevisions[i]).marshalBuffer(b)
	}
	b.writePrefix(len(t.StorageProofs))
	for i := range t.StorageProofs {
		(*objStorageProof)(&t.StorageProofs[i]).marshalBuffer(b)
	}
	b.writePrefix(len(t.SiafundInputs))
	for i := range t.SiafundInputs {
		(*objSiafundInput)(&t.SiafundInputs[i]).marshalBuffer(b)
	}
	b.writePrefix(len(t.SiafundOutputs))
	for i := range t.SiafundOutputs {
		(*objSiafundOutput)(&t.SiafundOutputs[i]).marshalBuffer(b)
	}
	b.writePrefix(len(t.MinerFees))
	for i := range t.MinerFees {
		(*objCurrency)(&t.MinerFees[i]).marshalBuffer(b)
	}
	b.writePrefix(len(t.ArbitraryData))
	for i := range t.ArbitraryData {
		b.writePrefixedBytes(t.ArbitraryData[i])
	}
	b.writePrefix(len(t.TransactionSignatures))
	for i := range t.TransactionSignatures {
		(*objTransactionSignature)(&t.TransactionSignatures[i]).marshalBuffer(b)
	}
}

func (t *objTransaction) unmarshalBuffer(b *objBuffer) error {
	t.SiacoinInputs = make([]types.SiacoinInput, b.readPrefix(sizeofSiacoinInput))
	for i := range t.SiacoinInputs {
		(*objSiacoinInput)(&t.SiacoinInputs[i]).unmarshalBuffer(b)
	}
	t.SiacoinOutputs = make([]types.SiacoinOutput, b.readPrefix(sizeofSiacoinOutput))
	for i := range t.SiacoinOutputs {
		(*objSiacoinOutput)(&t.SiacoinOutputs[i]).unmarshalBuffer(b)
	}
	t.FileContracts = make([]types.FileContract, b.readPrefix(sizeofFileContract))
	for i := range t.FileContracts {
		(*objFileContract)(&t.FileContracts[i]).unmarshalBuffer(b)
	}
	t.FileContractRevisions = make([]types.FileContractRevision, b.readPrefix(sizeofFileContractRevision))
	for i := range t.FileContractRevisions {
		(*objFileContractRevision)(&t.FileContractRevisions[i]).unmarshalBuffer(b)
	}
	t.StorageProofs = make([]types.StorageProof, b.readPrefix(sizeofStorageProof))
	for i := range t.StorageProofs {
		(*objStorageProof)(&t.StorageProofs[i]).unmarshalBuffer(b)
	}
	t.SiafundInputs = make([]types.SiafundInput, b.readPrefix(sizeofSiafundInput))
	for i := range t.SiafundInputs {
		(*objSiafundInput)(&t.SiafundInputs[i]).unmarshalBuffer(b)
	}
	t.SiafundOutputs = make([]types.SiafundOutput, b.readPrefix(sizeofSiafundOutput))
	for i := range t.SiafundOutputs {
		(*objSiafundOutput)(&t.SiafundOutputs[i]).unmarshalBuffer(b)
	}
	t.MinerFees = make([]types.Currency, b.readPrefix(sizeofCurrency))
	for i := range t.MinerFees {
		(*objCurrency)(&t.MinerFees[i]).unmarshalBuffer(b)
	}
	t.ArbitraryData = make([][]byte, b.readPrefix(1))
	for i := range t.ArbitraryData {
		t.ArbitraryData[i] = b.readPrefixedBytes()
	}
	t.TransactionSignatures = make([]types.TransactionSignature, b.readPrefix(sizeofTransactionSignature))
	for i := range t.TransactionSignatures {
		(*objTransactionSignature)(&t.TransactionSignatures[i]).unmarshalBuffer(b)
	}
	return b.Err()
}

// Handshake objects (these are sent unencrypted; they are not ProtocolObjects)

func (req *loopKeyExchangeRequest) writeTo(w io.Writer) error {
	buf := make([]byte, 16+32+8+len(req.Ciphers)*sizeofSpecifier)
	copy(buf[:16], loopEnter[:])
	copy(buf[16:48], req.PublicKey[:])
	binary.LittleEndian.PutUint64(buf[48:56], uint64(len(req.Ciphers)))
	for i := range req.Ciphers {
		copy(buf[56+(i*16):], req.Ciphers[i][:])
	}
	_, err := w.Write(buf)
	return err
}

func (req *loopKeyExchangeRequest) readFrom(r io.Reader) error {
	// first read, to get r.PublicKey (and hopefully ciphers as well)
	buf := make([]byte, 1024)
	n, err := io.ReadAtLeast(r, buf, 56)
	if err != nil {
		return err
	}
	var id Specifier
	copy(id[:], buf[:16])
	if id != loopEnter {
		return fmt.Errorf("renter sent wrong specifier %q", id.String())
	}
	copy(req.PublicKey[:], buf[16:48])
	numCiphers := binary.LittleEndian.Uint64(buf[48:])
	if numCiphers > 16 {
		return fmt.Errorf("renter sent too many ciphers (%v)", numCiphers)
	}
	// second read, if necessary, to get ciphers
	ciphersSize := int(numCiphers) * sizeofSpecifier
	if rem := (56 + ciphersSize) - n; rem > 0 {
		if _, err := io.ReadFull(r, buf[n:][:rem]); err != nil {
			return err
		}
	}
	buf = buf[56:]
	req.Ciphers = make([]Specifier, numCiphers)
	for i := range req.Ciphers {
		copy(req.Ciphers[i][:], buf[i*sizeofSpecifier:])
	}
	return nil
}

func (resp *loopKeyExchangeResponse) writeTo(w io.Writer) error {
	buf := make([]byte, 56+len(resp.Signature))
	copy(buf[:32], resp.PublicKey[:])
	binary.LittleEndian.PutUint64(buf[32:], uint64(len(resp.Signature)))
	n := copy(buf[40:], resp.Signature[:])
	copy(buf[40+n:], resp.Cipher[:])
	_, err := w.Write(buf)
	return err
}

func (resp *loopKeyExchangeResponse) readFrom(r io.Reader) error {
	// only handle 64-byte signatures for now
	buf := make([]byte, 32+8+64+16)
	if _, err := io.ReadFull(r, buf); err != nil {
		return err
	}
	copy(resp.PublicKey[:], buf[:32])
	sigLen := binary.LittleEndian.Uint64(buf[32:])
	if sigLen != 64 {
		return fmt.Errorf("host sent non-ed25519 signature (%v bytes)", sigLen)
	}
	copy(resp.Signature[:], buf[8+32:][:64])
	copy(resp.Cipher[:], buf[8+32+64:])
	return nil
}

// alternative encoding; currently unused

type compactSettings HostSettings

func (settings *compactSettings) writeTo(w io.Writer) error {
	enc := encoding.NewEncoder(w)
	v := reflect.ValueOf(settings).Elem()
	t := v.Type()
	if err := enc.Encode(t.NumField()); err != nil {
		return err
	}
	for i := 0; i < t.NumField(); i++ {
		if err := enc.EncodeAll(t.Field(i).Name, v.Field(i).Interface()); err != nil {
			return err
		}
	}
	return nil
}

func (settings *compactSettings) readFrom(r io.Reader) error {
	dec := encoding.NewDecoder(r, 1<<20)
	v := reflect.ValueOf(settings).Elem()
	numFields := dec.NextUint64()
	for i := uint64(0); i < numFields; i++ {
		var field string
		if err := dec.Decode(&field); err != nil {
			return err
		}
		f := v.FieldByName(field)
		if err := dec.Decode(f.Addr().Interface()); err != nil {
			return err
		}
	}
	return dec.Err()
}
