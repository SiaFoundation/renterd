package slab

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"time"

	"go.sia.tech/renterd/internal/consensus"
	rhpv2 "go.sia.tech/renterd/rhp/v2"
	"go.sia.tech/siad/types"
	"golang.org/x/crypto/chacha20"
	"lukechampine.com/frand"
)

// A HostError associates an error with a given host.
type HostError struct {
	HostKey consensus.PublicKey
	Err     error
}

// Error implements error.
func (he HostError) Error() string {
	return fmt.Sprintf("%x: %v", he.HostKey[:4], he.Err.Error())
}

// Unwrap returns the underlying error.
func (he HostError) Unwrap() error {
	return he.Err
}

// A HostErrorSet is a collection of errors from various hosts.
type HostErrorSet []*HostError

// Error implements error.
func (hes HostErrorSet) Error() string {
	strs := make([]string, len(hes))
	for i := range strs {
		strs[i] = hes[i].Error()
	}
	// include a leading newline so that the first error isn't printed on the
	// same line as the error context
	return "\n" + strings.Join(strs, "\n")
}

type Session struct {
	*rhpv2.Session
	hostKey       consensus.PublicKey
	hostIP        string
	contractID    types.FileContractID
	renterKey     consensus.PrivateKey
	settings      rhpv2.HostSettings
	currentHeight uint64
	lastSeen      time.Time
}

func (s *Session) reconnect(ctx context.Context) error {
	if s.Session != nil {
		// if it hasn't been long since the last reconnect, assume the
		// connection is still open
		if time.Since(s.lastSeen) < 2*time.Minute {
			s.lastSeen = time.Now()
			return nil
		}
		// otherwise, the connection *might* still be open; test by sending
		// a "ping" RPC
		if _, err := rhpv2.RPCSettings(s.Transport()); err == nil {
			s.lastSeen = time.Now()
			return nil
		}
		// connection timed out, or some other error occurred; close our
		// end (just in case) and fallthrough to the reconnection logic
		s.Close()
	}
	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", s.hostIP)
	if err != nil {
		return err
	}
	t, err := rhpv2.NewRenterTransport(conn, s.hostKey)
	if err != nil {
		return err
	}
	s.settings, err = rhpv2.RPCSettings(t)
	if err != nil {
		t.Close()
		return err
	}
	s.Session, err = rhpv2.RPCLock(t, s.contractID, s.renterKey, 10*time.Second)
	if err != nil {
		t.Close()
		return err
	}
	s.lastSeen = time.Now()
	return nil
}

func (s *Session) UploadSector(ctx context.Context, sector *[rhpv2.SectorSize]byte) (consensus.Hash256, error) {
	storageDuration := s.currentHeight - uint64(s.Contract().Revision.NewWindowStart)
	price, collateral := rhpv2.RPCAppendCost(s.settings, storageDuration)
	return s.Append(sector, price, collateral)
}

func (s *Session) DownloadSector(ctx context.Context, w io.Writer, root consensus.Hash256, offset, length uint32) error {
	sections := []rhpv2.RPCReadRequestSection{{
		MerkleRoot: root,
		Offset:     uint64(offset),
		Length:     uint64(length),
	}}
	price := rhpv2.RPCReadCost(s.settings, sections)
	return s.Read(w, sections, price)
}

type HostSet struct {
	hosts         map[consensus.PublicKey]*Session
	currentHeight uint64
}

// Close closes all of the sessions in the set.
func (hs *HostSet) Close() error {
	for hostKey, sess := range hs.hosts {
		sess.Close()
		delete(hs.hosts, hostKey)
	}
	return nil
}

func (hs *HostSet) Host(host consensus.PublicKey) (*Session, error) {
	sess, ok := hs.hosts[host]
	if !ok {
		return nil, errors.New("unknown host")
	}
	if err := sess.reconnect(context.TODO()); err != nil {
		return nil, err
	}
	return sess, nil
}

// AddHost adds a host to the set.
func (hs *HostSet) AddHost(hostKey consensus.PublicKey, hostIP string, contractID types.FileContractID, renterKey consensus.PrivateKey) {
	hs.hosts[hostKey] = &Session{
		hostKey:       hostKey,
		hostIP:        hostIP,
		contractID:    contractID,
		renterKey:     renterKey,
		currentHeight: hs.currentHeight,
	}
}

func NewHostSet(currentHeight uint64) *HostSet {
	return &HostSet{
		hosts:         make(map[consensus.PublicKey]*Session),
		currentHeight: currentHeight,
	}
}

type (
	SectorUploader interface {
		UploadSector(ctx context.Context, sector *[rhpv2.SectorSize]byte) (consensus.Hash256, error)
	}

	SectorDownloader interface {
		DownloadSector(ctx context.Context, w io.Writer, root consensus.Hash256, offset, length uint32) error
	}

	SectorDeleter interface {
		DeleteSector(ctx context.Context, s Sector) error
	}
)

type SerialSlabUploader struct {
	Hosts map[consensus.PublicKey]SectorUploader
}

func (ssu SerialSlabUploader) UploadSlab(ctx context.Context, shards [][]byte) ([]Sector, error) {
	var sectors []Sector
	var errs HostErrorSet
	for hostKey, su := range ssu.Hosts {
		root, err := su.UploadSector(ctx, (*[rhpv2.SectorSize]byte)(shards[len(sectors)]))
		if err != nil {
			errs = append(errs, &HostError{hostKey, err})
			continue
		}
		sectors = append(sectors, Sector{
			Host: hostKey,
			Root: root,
		})
		if len(sectors) == len(shards) {
			break
		}
	}
	if len(sectors) < len(shards) {
		return nil, errs
	}
	return sectors, nil
}

func NewSerialSlabUploader(set *HostSet) *SerialSlabUploader {
	hosts := make(map[consensus.PublicKey]SectorUploader)
	for hostKey, sess := range set.hosts {
		hosts[hostKey] = sess
	}
	return &SerialSlabUploader{
		Hosts: hosts,
	}
}

type SerialSlabDownloader struct {
	Hosts map[consensus.PublicKey]SectorDownloader
}

func (ssd SerialSlabDownloader) DownloadSlab(ctx context.Context, ss SlabSlice) ([][]byte, error) {
	offset, length := ss.SectorRegion()
	shards := make([][]byte, len(ss.Shards))
	rem := ss.MinShards
	var errs HostErrorSet
	for i, sector := range ss.Shards {
		sd, ok := ssd.Hosts[sector.Host]
		if !ok {
			errs = append(errs, &HostError{sector.Host, errors.New("unknown host")})
			continue
		}
		var buf bytes.Buffer
		if err := sd.DownloadSector(ctx, &buf, sector.Root, offset, length); err != nil {
			errs = append(errs, &HostError{sector.Host, err})
			continue
		}
		shards[i] = buf.Bytes()
		if rem--; rem == 0 {
			break
		}
	}
	if rem > 0 {
		return nil, errs
	}
	return shards, nil
}

func NewSerialSlabDownloader(set *HostSet) *SerialSlabDownloader {
	hosts := make(map[consensus.PublicKey]SectorDownloader)
	for hostKey, sess := range set.hosts {
		hosts[hostKey] = sess
	}
	return &SerialSlabDownloader{
		Hosts: hosts,
	}
}

type (
	SlabReader interface {
		ReadSlab() (Slab, [][]byte, error)
	}

	SlabUploader interface {
		UploadSlab(ctx context.Context, shards [][]byte) ([]Sector, error)
	}

	// A SlabDownloader downloads a set of shards that, when recovered, contain
	// the specified SlabSlice. Shard data can only be downloaded in multiplies
	// of rhpv2.LeafSize, so if the offset and length of ss are not
	// leaf-aligned, the recovered data will contain unwanted bytes at the
	// beginning and/or end.
	SlabDownloader interface {
		DownloadSlab(ctx context.Context, ss SlabSlice) ([][]byte, error)
	}

	SlabDeleter interface {
		DeleteSlab(ctx context.Context, s Slab) error
	}
)

type UniformSlabReader struct {
	r         io.Reader
	rsc       RSCode
	buf       []byte
	shards    [][]byte
	minShards uint8
}

func (usr *UniformSlabReader) ReadSlab() (Slab, [][]byte, error) {
	_, err := io.ReadFull(usr.r, usr.buf)
	if err != nil && err != io.ErrUnexpectedEOF {
		return Slab{}, nil, err
	}
	usr.rsc.Encode(usr.buf, usr.shards)
	var key EncryptionKey
	frand.Read(key[:])
	for i := range usr.shards {
		key.XORKeyStream(usr.shards[i], uint8(i), 0)
	}
	return Slab{
		Key:       key,
		MinShards: usr.minShards,
	}, usr.shards, nil
}

func NewUniformSlabReader(r io.Reader, m, n uint8) *UniformSlabReader {
	shards := make([][]byte, n)
	for i := range shards {
		shards[i] = make([]byte, 0, rhpv2.SectorSize)
	}
	return &UniformSlabReader{
		r:         r,
		rsc:       NewRSCode(m, n),
		buf:       make([]byte, int(m)*rhpv2.SectorSize),
		shards:    shards,
		minShards: m,
	}
}

func WriteSlab(w io.Writer, ss SlabSlice, shards [][]byte) error {
	rsc := NewRSCode(ss.MinShards, uint8(len(shards)))
	minChunkSize := rhpv2.LeafSize * uint32(ss.MinShards)
	for i := range shards {
		ss.Key.XORKeyStream(shards[i], uint8(i), ss.Offset/minChunkSize)
	}
	skip := ss.Offset % minChunkSize
	return rsc.Recover(w, shards, int(skip), int(ss.Length))
}

// NewCipher returns a ChaCha cipher with the specified seek offset.
func NewCipher(key EncryptionKey, offset int64) *chacha20.Cipher {
	c, _ := chacha20.NewUnauthenticatedCipher(key[:], make([]byte, 24))
	c.SetCounter(uint32(offset / 64))
	var buf [64]byte
	c.XORKeyStream(buf[:offset%64], buf[:offset%64])
	return c
}

type SerialSlabsUploader struct {
	SlabUploader SlabUploader
}

func (ssu SerialSlabsUploader) UploadSlabs(ctx context.Context, sr SlabReader) ([]Slab, error) {
	var slabs []Slab
	for {
		s, shards, err := sr.ReadSlab()
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}
		s.Shards, err = ssu.SlabUploader.UploadSlab(ctx, shards)
		if err != nil {
			return nil, err
		}
		slabs = append(slabs, s)
	}
	return slabs, nil
}

func slabsSize(slabs []SlabSlice) (n int64) {
	for _, ss := range slabs {
		n += int64(ss.Length)
	}
	return
}

func slabsForDownload(slabs []SlabSlice, offset, length int64) []SlabSlice {
	firstOffset := offset
	for i, ss := range slabs {
		if firstOffset <= int64(ss.Length) {
			slabs = slabs[i:]
			break
		}
		firstOffset -= int64(ss.Length)
	}
	lastLength := length
	for i, ss := range slabs {
		if lastLength <= int64(ss.Length) {
			slabs = slabs[:i+1]
			break
		}
		lastLength -= int64(ss.Length)
	}
	// mutate a copy
	slabs = append([]SlabSlice(nil), slabs...)
	slabs[0].Offset += uint32(firstOffset)
	slabs[0].Length -= uint32(firstOffset)
	slabs[len(slabs)-1].Length = uint32(lastLength)
	return slabs
}

type SerialSlabsDownloader struct {
	SlabDownloader SlabDownloader
}

func (ssd SerialSlabsDownloader) DownloadSlabs(ctx context.Context, w io.Writer, slabs []SlabSlice, offset, length int64) error {
	if offset < 0 || length < 0 || offset+length > slabsSize(slabs) {
		return errors.New("requested range is out of bounds")
	} else if length == 0 {
		return nil
	}

	slabs = slabsForDownload(slabs, offset, length)
	for _, ss := range slabs {
		shards, err := ssd.SlabDownloader.DownloadSlab(ctx, ss)
		if err != nil {
			return err
		}
		if err := WriteSlab(w, ss, shards); err != nil {
			return err
		}
	}
	return nil
}

type SerialSlabsDeleter struct {
	SlabDeleter SlabDeleter
}

func (ssd SerialSlabsDeleter) DeleteSlabs(ctx context.Context, slabs []Slab) error {
	for _, s := range slabs {
		if err := ssd.SlabDeleter.DeleteSlab(ctx, s); err != nil {
			return err
		}
	}
	return nil
}
