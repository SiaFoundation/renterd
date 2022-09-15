package slab

import (
	"bytes"
	"errors"
	"io"

	"go.sia.tech/renterd/internal/consensus"
	rhpv2 "go.sia.tech/renterd/rhp/v2"
)

// A Host stores contract data.
type Host interface {
	PublicKey() consensus.PublicKey
	UploadSector(sector *[rhpv2.SectorSize]byte) (consensus.Hash256, error)
	DownloadSector(w io.Writer, root consensus.Hash256, offset, length uint32) error
	DeleteSectors(roots []consensus.Hash256) error
}

// serialUploadSlab uploads the provided shards one at a time.
func serialUploadSlab(shards [][]byte, hosts []Host) ([]Sector, error) {
	var sectors []Sector
	var errs HostErrorSet
	for _, h := range hosts {
		root, err := h.UploadSector((*[rhpv2.SectorSize]byte)(shards[len(sectors)]))
		if err != nil {
			errs = append(errs, &HostError{h.PublicKey(), err})
			continue
		}
		sectors = append(sectors, Sector{
			Host: h.PublicKey(),
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

// UploadSlabs uploads slabs read from the provided Reader.
func UploadSlabs(r io.Reader, m, n uint8, hosts []Host) ([]Slab, error) {
	buf := make([]byte, int(m)*rhpv2.SectorSize)
	shards := make([][]byte, n)
	var slabs []Slab
	for {
		// read slab data, encode, and encrypt
		_, err := io.ReadFull(r, buf)
		if err == io.EOF {
			break
		} else if err != nil && err != io.ErrUnexpectedEOF {
			return nil, err
		}
		s := Slab{
			Key:       GenerateEncryptionKey(),
			MinShards: m,
		}
		s.Encode(buf, shards)
		s.Encrypt(shards)
		s.Shards, err = serialUploadSlab(shards, hosts)
		if err != nil {
			return nil, err
		}
		slabs = append(slabs, s)
	}
	return slabs, nil
}

func slabsForDownload(slabs []Slice, offset, length int64) []Slice {
	// mutate a copy
	slabs = append([]Slice(nil), slabs...)

	firstOffset := offset
	for i, ss := range slabs {
		if firstOffset <= int64(ss.Length) {
			slabs = slabs[i:]
			break
		}
		firstOffset -= int64(ss.Length)
	}
	slabs[0].Offset += uint32(firstOffset)
	slabs[0].Length -= uint32(firstOffset)

	lastLength := length
	for i, ss := range slabs {
		if lastLength <= int64(ss.Length) {
			slabs = slabs[:i+1]
			break
		}
		lastLength -= int64(ss.Length)
	}
	slabs[len(slabs)-1].Length = uint32(lastLength)
	return slabs
}

// serialDownloadSlab downloads the shards comprising a slab one at a time.
func serialDownloadSlab(s Slice, hosts []Host) ([][]byte, error) {
	offset, length := s.SectorRegion()
	shards := make([][]byte, len(s.Shards))
	rem := s.MinShards
	var errs HostErrorSet
	for _, h := range hosts {
		var i int
		for i = range s.Shards {
			if s.Shards[i].Host == h.PublicKey() {
				break
			}
		}
		if i == len(s.Shards) {
			errs = append(errs, &HostError{h.PublicKey(), errors.New("slab is not stored on this host")})
			continue
		}
		var buf bytes.Buffer
		if err := h.DownloadSector(&buf, s.Shards[i].Root, offset, length); err != nil {
			errs = append(errs, &HostError{h.PublicKey(), err})
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

// DownloadSlabs downloads data from the supplied slabs.
func DownloadSlabs(w io.Writer, slabs []Slice, offset, length int64, hosts []Host) error {
	var slabsSize int64
	for _, ss := range slabs {
		slabsSize += int64(ss.Length)
	}
	if offset < 0 || length < 0 || offset+length > slabsSize {
		return errors.New("requested range is out of bounds")
	} else if length == 0 {
		return nil
	}

	slabs = slabsForDownload(slabs, offset, length)
	for _, ss := range slabs {
		shards, err := serialDownloadSlab(ss, hosts)
		if err != nil {
			return err
		}
		ss.Decrypt(shards)
		if err := ss.Recover(w, shards); err != nil {
			return err
		}
	}
	return nil
}

// DeleteSlabs deletes a set of slabs from the provided hosts.
func DeleteSlabs(slabs []Slab, hosts []Host) error {
	rootsByHost := make(map[consensus.PublicKey][]consensus.Hash256)
	for _, s := range slabs {
		for _, sector := range s.Shards {
			rootsByHost[sector.Host] = append(rootsByHost[sector.Host], sector.Root)
		}
	}
	var errs HostErrorSet
	for _, h := range hosts {
		// NOTE: if host is not storing any sectors, the map lookup will return
		// nil, making this a no-op
		if err := h.DeleteSectors(rootsByHost[h.PublicKey()]); err != nil {
			errs = append(errs, &HostError{h.PublicKey(), err})
			continue
		}
	}
	if len(errs) > 0 {
		return errs
	}
	return nil
}

// serialMigrateSlab migrates a slab one shard at a time.
func serialMigrateSlab(s *Slab, from, to []Host) error {
	ss := Slice{*s, 0, uint32(s.MinShards) * rhpv2.SectorSize}
	shards, err := serialDownloadSlab(ss, from)
	if err != nil {
		return err
	}
	ss.Decrypt(shards)
	if err := s.Reconstruct(shards); err != nil {
		return err
	}
	s.Encrypt(shards)

	queue := to
	migrate := func(shard []byte) (Sector, error) {
		var errs HostErrorSet
		for len(queue) > 0 {
			h := queue[0]
			queue = queue[1:]
			root, err := h.UploadSector((*[rhpv2.SectorSize]byte)(shard))
			if err != nil {
				errs = append(errs, &HostError{h.PublicKey(), err})
				continue
			}
			return Sector{
				Host: h.PublicKey(),
				Root: root,
			}, nil
		}
		if len(errs) > 0 {
			return Sector{}, errs
		}
		return Sector{}, errors.New("no hosts available")
	}
	alreadyMigrated := func(shard Sector) bool {
		for _, h := range to {
			if shard.Host == h.PublicKey() {
				return true
			}
		}
		return false
	}

	for i, shard := range shards {
		if alreadyMigrated(s.Shards[i]) {
			continue
		}
		s.Shards[i], err = migrate(shard)
		if err != nil {
			return err
		}
	}
	return nil
}

// MigrateSlabs migrates the provided slabs.
func MigrateSlabs(slabs []Slab, from, to []Host) error {
	for i := range slabs {
		if err := serialMigrateSlab(&slabs[i], from, to); err != nil {
			return err
		}
	}
	return nil
}
