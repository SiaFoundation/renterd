package rhp

import (
	"crypto/cipher"
	"crypto/subtle"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"strings"
	"sync"
	"sync/atomic"

	"go.sia.tech/siad/crypto"
	"golang.org/x/crypto/blake2b"
	"golang.org/x/crypto/chacha20"
	"golang.org/x/crypto/chacha20poly1305"
	"golang.org/x/crypto/poly1305"
	"lukechampine.com/frand"
)

// minMessageSize is the minimum size of an RPC message. If an encoded message
// would be smaller than minMessageSize, the sender MAY pad it with random data.
// This hinders traffic analysis by obscuring the true sizes of messages.
const minMessageSize = 4096

var (
	// Handshake specifiers
	loopEnter = newSpecifier("LoopEnter")
	loopExit  = newSpecifier("LoopExit")

	// RPC ciphers
	cipherChaCha20Poly1305 = newSpecifier("ChaCha20Poly1305")
	cipherNoOverlap        = newSpecifier("NoOverlap")

	// ErrRenterClosed is returned by (*Transport).ReadID when the renter sends the
	// Transport termination signal.
	ErrRenterClosed = errors.New("renter has terminated Transport")
)

// An RPCError may be sent instead of a response object to any RPC.
type RPCError struct {
	Type        Specifier
	Data        []byte // structure depends on Type
	Description string // human-readable error string
}

// Error implements the error interface.
func (e *RPCError) Error() string {
	return e.Description
}

// Is reports whether this error matches target.
func (e *RPCError) Is(target error) bool {
	return strings.Contains(e.Description, target.Error())
}

// helper type for encoding and decoding RPC response messages, which can
// represent either valid data or an error.
type rpcResponse struct {
	err  *RPCError
	data ProtocolObject
}

// A Transport facilitates the exchange of RPCs via the renter-host protocol,
// version 2.
type Transport struct {
	conn      net.Conn
	aead      cipher.AEAD
	key       []byte // for RawResponse
	inbuf     objBuffer
	outbuf    objBuffer
	challenge [16]byte
	isRenter  bool
	hostKey   PublicKey

	mu     sync.Mutex
	r, w   uint64
	err    error // set when Transport is prematurely closed
	closed bool
}

func (t *Transport) setErr(err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err != nil && t.err == nil {
		if ne, ok := err.(net.Error); !ok || !ne.Temporary() {
			t.conn.Close()
			t.err = err
		}
	}
}

// HostKey returns the host's public key.
func (t *Transport) HostKey() PublicKey { return t.hostKey }

// BytesRead returns the number of bytes read from the underlying connection.
func (t *Transport) BytesRead() uint64 { return atomic.LoadUint64(&t.r) }

// BytesWritten returns the number of bytes written to the underlying connection.
func (t *Transport) BytesWritten() uint64 { return atomic.LoadUint64(&t.w) }

// PrematureCloseErr returns the error that resulted in the Transport being closed
// prematurely.
func (t *Transport) PrematureCloseErr() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.err
}

// IsClosed returns whether the Transport is closed. Check PrematureCloseErr to
// determine whether the Transport was closed gracefully.
func (t *Transport) IsClosed() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.closed || t.err != nil
}

// SetChallenge sets the current Transport challenge.
func (t *Transport) SetChallenge(challenge [16]byte) {
	t.challenge = challenge
}

func hashChallenge(challenge [16]byte) [32]byte {
	c := make([]byte, 32)
	copy(c[:16], "challenge")
	copy(c[16:], challenge[:])
	return blake2b.Sum256(c)
}

// SignChallenge signs the current Transport challenge.
func (t *Transport) SignChallenge(priv PrivateKey) Signature {
	return priv.SignHash(hashChallenge(t.challenge))
}

func (t *Transport) writeMessage(obj ProtocolObject) error {
	if err := t.PrematureCloseErr(); err != nil {
		return err
	}
	// generate random nonce
	nonce := make([]byte, 256)[:t.aead.NonceSize()] // avoid heap alloc
	frand.Read(nonce)

	// pad short messages to minMessageSize
	msgSize := 8 + t.aead.NonceSize() + obj.marshalledSize() + t.aead.Overhead()
	if msgSize < minMessageSize {
		msgSize = minMessageSize
	}

	// write length prefix, nonce, and object directly into buffer
	t.outbuf.reset()
	t.outbuf.grow(msgSize)
	t.outbuf.writePrefix(msgSize - 8)
	t.outbuf.write(nonce)
	obj.marshalBuffer(&t.outbuf)

	// encrypt the object in-place
	msg := t.outbuf.bytes()[:msgSize]
	msgNonce := msg[8:][:len(nonce)]
	payload := msg[8+len(nonce) : msgSize-t.aead.Overhead()]
	t.aead.Seal(payload[:0], msgNonce, payload, nil)

	n, err := t.conn.Write(msg)
	atomic.AddUint64(&t.w, uint64(n))
	t.setErr(err)
	return err
}

func (t *Transport) readMessage(obj ProtocolObject, maxLen uint64) error {
	if err := t.PrematureCloseErr(); err != nil {
		return err
	}
	if maxLen < minMessageSize {
		maxLen = minMessageSize
	}
	t.inbuf.reset()
	if err := t.inbuf.copyN(t.conn, 8); err != nil {
		t.setErr(err)
		return err
	}
	msgSize := t.inbuf.readUint64()
	if msgSize > maxLen {
		return fmt.Errorf("message size (%v bytes) exceeds maxLen of %v bytes", msgSize, maxLen)
	} else if msgSize < uint64(t.aead.NonceSize()+t.aead.Overhead()) {
		return fmt.Errorf("message size (%v bytes) is too small (nonce + MAC is %v bytes)", msgSize, t.aead.NonceSize()+t.aead.Overhead())
	}

	t.inbuf.reset()
	t.inbuf.grow(int(msgSize))
	if err := t.inbuf.copyN(t.conn, msgSize); err != nil {
		t.setErr(err)
		return err
	}
	atomic.AddUint64(&t.r, uint64(8+msgSize))

	nonce := t.inbuf.next(t.aead.NonceSize())
	paddedPayload := t.inbuf.bytes()
	_, err := t.aead.Open(paddedPayload[:0], nonce, paddedPayload, nil)
	if err != nil {
		t.setErr(err) // not an I/O error, but still fatal
		return err
	}
	return obj.unmarshalBuffer(&t.inbuf)
}

// WriteRequest sends an encrypted RPC request, comprising an RPC ID and a
// request object.
func (t *Transport) WriteRequest(rpcID Specifier, req ProtocolObject) error {
	if err := t.writeMessage(&rpcID); err != nil {
		return fmt.Errorf("WriteRequestID: %w", err)
	}
	if req != nil {
		if err := t.writeMessage(req); err != nil {
			return fmt.Errorf("WriteRequest: %w", err)
		}
	}
	return nil
}

// ReadID reads an RPC request ID. If the renter sends the Transport termination
// signal, ReadID returns ErrRenterClosed.
func (t *Transport) ReadID() (rpcID Specifier, err error) {
	defer wrapErr(&err, "ReadID")
	err = t.readMessage(&rpcID, minMessageSize)
	if rpcID == loopExit {
		err = ErrRenterClosed
	}
	return
}

// ReadRequest reads an RPC request using the new loop protocol.
func (t *Transport) ReadRequest(req ProtocolObject, maxLen uint64) (err error) {
	defer wrapErr(&err, "ReadRequest")
	return t.readMessage(req, maxLen)
}

// WriteResponse writes an RPC response object.
func (t *Transport) WriteResponse(resp ProtocolObject) (e error) {
	defer wrapErr(&e, "WriteResponse")
	return t.writeMessage(&rpcResponse{nil, resp})
}

// WriteResponseErr writes an error. If err is an *RPCError, it is sent
// directly; otherwise, a generic RPCError is created from err's Error string.
func (t *Transport) WriteResponseErr(err error) (e error) {
	defer wrapErr(&e, "WriteResponseErr")
	re, ok := err.(*RPCError)
	if err != nil && !ok {
		re = &RPCError{Description: err.Error()}
	}
	return t.writeMessage(&rpcResponse{re, nil})
}

// ReadResponse reads an RPC response. If the response is an error, it is
// returned directly.
func (t *Transport) ReadResponse(resp ProtocolObject, maxLen uint64) (err error) {
	defer wrapErr(&err, "ReadResponse")
	rr := rpcResponse{nil, resp}
	if err := t.readMessage(&rr, maxLen); err != nil {
		return err
	} else if rr.err != nil {
		return rr.err
	}
	return nil
}

// Call is a helper method that writes a request and then reads a response.
func (t *Transport) Call(rpcID Specifier, req, resp ProtocolObject) error {
	if err := t.WriteRequest(rpcID, req); err != nil {
		return err
	}
	// use a maxlen large enough for all RPCs except Read, Write, and
	// SectorRoots (which don't use Call anyway)
	err := t.ReadResponse(resp, 4096)
	return wrapResponseErr(err, fmt.Sprintf("couldn't read %v response", rpcID), fmt.Sprintf("host rejected %v request", rpcID))
}

// A ResponseReader contains an unencrypted, unauthenticated RPC response
// message.
type ResponseReader struct {
	msgR   io.Reader
	tagR   io.Reader
	mac    *poly1305.MAC
	clen   uint64
	setErr func(error)
}

// Read implements io.Reader.
func (rr *ResponseReader) Read(p []byte) (int, error) {
	n, err := rr.msgR.Read(p)
	if err != io.EOF {
		// EOF is expected, since this is a limited reader
		rr.setErr(err)
	}
	return n, err
}

// VerifyTag verifies the authentication tag appended to the message. VerifyTag
// must be called after Read returns io.EOF, and the message must be discarded
// if VerifyTag returns a non-nil error.
func (rr *ResponseReader) VerifyTag() error {
	// the caller may not have consumed the full message (e.g. if it was padded
	// to minMessageSize), so make sure the whole thing is written to the MAC
	if _, err := io.Copy(ioutil.Discard, rr); err != nil {
		return err
	}

	var tag [poly1305.TagSize]byte
	if _, err := io.ReadFull(rr.tagR, tag[:]); err != nil {
		rr.setErr(err)
		return err
	}
	// MAC is padded to 16 bytes, and covers the length of AD (0 in this case)
	// and ciphertext
	tail := make([]byte, 0, 32)[:32-(rr.clen%16)]
	binary.LittleEndian.PutUint64(tail[len(tail)-8:], rr.clen)
	rr.mac.Write(tail)
	var ourTag [poly1305.TagSize]byte
	rr.mac.Sum(ourTag[:0])
	if subtle.ConstantTimeCompare(tag[:], ourTag[:]) != 1 {
		err := errors.New("chacha20poly1305: message authentication failed")
		rr.setErr(err) // not an I/O error, but still fatal
		return err
	}
	return nil
}

// RawResponse returns a stream containing the (unencrypted, unauthenticated)
// content of the next message. The Reader must be fully consumed by the caller,
// after which the caller should call VerifyTag to authenticate the message. If
// the response was an RPCError, it is authenticated and returned immediately.
func (t *Transport) RawResponse(maxLen uint64) (*ResponseReader, error) {
	if maxLen < minMessageSize {
		maxLen = minMessageSize
	}
	t.inbuf.reset()
	if err := t.inbuf.copyN(t.conn, 8); err != nil {
		t.setErr(err)
		return nil, err
	}
	msgSize := t.inbuf.readUint64()
	if msgSize > maxLen {
		return nil, fmt.Errorf("message size (%v bytes) exceeds maxLen of %v bytes", msgSize, maxLen)
	} else if msgSize < uint64(t.aead.NonceSize()+t.aead.Overhead()) {
		return nil, fmt.Errorf("message size (%v bytes) is too small (nonce + MAC is %v bytes)", msgSize, t.aead.NonceSize()+t.aead.Overhead())
	}
	msgSize -= uint64(t.aead.NonceSize() + t.aead.Overhead())

	t.inbuf.reset()
	t.inbuf.grow(t.aead.NonceSize())
	if err := t.inbuf.copyN(t.conn, uint64(t.aead.NonceSize())); err != nil {
		t.setErr(err)
		return nil, err
	}
	nonce := t.inbuf.next(t.aead.NonceSize())

	// construct reader
	c, _ := chacha20.NewUnauthenticatedCipher(t.key, nonce)
	var polyKey [32]byte
	c.XORKeyStream(polyKey[:], polyKey[:])
	mac := poly1305.New(&polyKey)
	c.SetCounter(1)
	rr := &ResponseReader{
		msgR: cipher.StreamReader{
			R: io.TeeReader(io.LimitReader(t.conn, int64(msgSize)), mac),
			S: c,
		},
		tagR:   io.LimitReader(t.conn, poly1305.TagSize),
		mac:    mac,
		clen:   msgSize,
		setErr: t.setErr,
	}

	// check if response is an RPCError
	if err := t.inbuf.copyN(rr, 1); err != nil {
		return nil, err
	}
	if isErr := t.inbuf.readBool(); isErr {
		if err := t.inbuf.copyN(rr, msgSize-1); err != nil {
			return nil, err
		}
		err := new(RPCError)
		err.unmarshalBuffer(&t.inbuf)
		if err := rr.VerifyTag(); err != nil {
			return nil, err
		}
		return nil, err
	}
	// not an error; pass rest of stream to caller
	return rr, nil
}

// Close gracefully terminates the RPC loop and closes the connection.
func (t *Transport) Close() (err error) {
	defer wrapErr(&err, "Close")
	if t.IsClosed() {
		return nil
	}
	t.mu.Lock()
	t.closed = true
	t.mu.Unlock()
	if t.isRenter {
		t.writeMessage(&loopExit)
	}
	return t.conn.Close()
}

func hashKeys(k1, k2 [32]byte) Hash256 {
	return blake2b.Sum256(append(append(make([]byte, 0, len(k1)+len(k2)), k1[:]...), k2[:]...))
}

// NewHostTransport conducts the hosts's half of the renter-host protocol
// handshake, returning a Transport that can be used to handle RPC requests.
func NewHostTransport(conn net.Conn, priv PrivateKey) (_ *Transport, err error) {
	defer wrapErr(&err, "NewHostTransport")
	var req loopKeyExchangeRequest
	if err := req.readFrom(conn); err != nil {
		return nil, err
	}

	var supportsChaCha bool
	for _, c := range req.Ciphers {
		if c == cipherChaCha20Poly1305 {
			supportsChaCha = true
		}
	}
	if !supportsChaCha {
		(&loopKeyExchangeResponse{Cipher: cipherNoOverlap}).writeTo(conn)
		return nil, errors.New("no supported ciphers")
	}

	xsk, xpk := crypto.GenerateX25519KeyPair()
	h := hashKeys(req.PublicKey, xpk)
	resp := loopKeyExchangeResponse{
		Cipher:    cipherChaCha20Poly1305,
		PublicKey: xpk,
		Signature: priv.SignHash(h),
	}
	if err := resp.writeTo(conn); err != nil {
		return nil, err
	}

	cipherKey := crypto.DeriveSharedSecret(xsk, req.PublicKey)
	aead, _ := chacha20poly1305.New(cipherKey[:]) // no error possible
	t := &Transport{
		conn:      conn,
		aead:      aead,
		key:       cipherKey[:],
		challenge: frand.Entropy128(),
		isRenter:  false,
		hostKey:   priv.PublicKey(),
	}
	// hack: cast challenge to Specifier to make it a ProtocolObject
	if err := t.writeMessage((*Specifier)(&t.challenge)); err != nil {
		return nil, err
	}
	return t, nil
}

// NewRenterTransport conducts the renter's half of the renter-host protocol
// handshake, returning a Transport that can be used to make RPC requests.
func NewRenterTransport(conn net.Conn, pub PublicKey) (_ *Transport, err error) {
	defer wrapErr(&err, "NewRenterTransport")

	xsk, xpk := crypto.GenerateX25519KeyPair()
	req := &loopKeyExchangeRequest{
		PublicKey: xpk,
		Ciphers:   []Specifier{cipherChaCha20Poly1305},
	}
	if err := req.writeTo(conn); err != nil {
		return nil, fmt.Errorf("couldn't write handshake: %w", err)
	}
	var resp loopKeyExchangeResponse
	if err := resp.readFrom(conn); err != nil {
		return nil, fmt.Errorf("couldn't read host's handshake: %w", err)
	}
	// validate the signature before doing anything else
	h := hashKeys(req.PublicKey, resp.PublicKey)
	if !pub.VerifyHash(h, resp.Signature) {
		return nil, errors.New("host's handshake signature was invalid")
	}
	if resp.Cipher == cipherNoOverlap {
		return nil, errors.New("host does not support any of our proposed ciphers")
	} else if resp.Cipher != cipherChaCha20Poly1305 {
		return nil, errors.New("host selected unsupported cipher")
	}

	cipherKey := crypto.DeriveSharedSecret(xsk, resp.PublicKey)
	aead, _ := chacha20poly1305.New(cipherKey[:]) // no error possible
	t := &Transport{
		conn:     conn,
		aead:     aead,
		key:      cipherKey[:],
		isRenter: true,
		hostKey:  pub,
	}
	// hack: cast challenge to Specifier to make it a ProtocolObject
	if err := t.readMessage((*Specifier)(&t.challenge), minMessageSize); err != nil {
		return nil, err
	}
	return t, nil
}

// Handshake objects
type (
	loopKeyExchangeRequest struct {
		PublicKey crypto.X25519PublicKey
		Ciphers   []Specifier
	}

	loopKeyExchangeResponse struct {
		PublicKey crypto.X25519PublicKey
		Signature Signature
		Cipher    Specifier
	}
)
