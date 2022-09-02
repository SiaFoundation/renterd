package slab

import (
	"bytes"
	"errors"
	"io"

	"go.sia.tech/renterd/internal/consensus"
	rhpv2 "go.sia.tech/renterd/rhp/v2"
)

type (
	// A SectorUploader uploads a sector, returning its Merkle root.
	SectorUploader interface {
		UploadSector(sector *[rhpv2.SectorSize]byte) (consensus.Hash256, error)
	}

	// A SectorDownloader downloads a slice of a sector.
	SectorDownloader interface {
		DownloadSector(w io.Writer, root consensus.Hash256, offset, length uint32) error
	}

	// A SectorDeleter deletes sectors.
	SectorDeleter interface {
		DeleteSectors(roots []consensus.Hash256) error
	}
)

// serialUploadSlab uploads the provided shards one at a time.
func serialUploadSlab(shards [][]byte, hosts map[consensus.PublicKey]SectorUploader) ([]Sector, error) {
	var sectors []Sector
	var errs HostErrorSet
	for hostKey, su := range hosts {
		root, err := su.UploadSector((*[rhpv2.SectorSize]byte)(shards[len(sectors)]))
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

// UploadSlabs uploads slabs read from the provided Reader.
func UploadSlabs(r io.Reader, m, n uint8, hosts map[consensus.PublicKey]SectorUploader) ([]Slab, error) {
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
		EncodeSlab(s, buf, shards)
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
func serialDownloadSlab(s Slice, hosts map[consensus.PublicKey]SectorDownloader) ([][]byte, error) {
	offset, length := s.SectorRegion()
	shards := make([][]byte, len(s.Shards))
	rem := s.MinShards
	var errs HostErrorSet
	for i, sector := range s.Shards {
		sd, ok := hosts[sector.Host]
		if !ok {
			errs = append(errs, &HostError{sector.Host, errors.New("unknown host")})
			continue
		}
		var buf bytes.Buffer
		if err := sd.DownloadSector(&buf, sector.Root, offset, length); err != nil {
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

// DownloadSlabs downloads data from the supplied slabs.
func DownloadSlabs(w io.Writer, slabs []Slice, offset, length int64, hosts map[consensus.PublicKey]SectorDownloader) error {
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
		if err := RecoverSlab(w, ss, shards); err != nil {
			return err
		}
	}
	return nil
}

// DeleteSlab deletes a set of slabs from the provided hosts.
func DeleteSlabs(slabs []Slab, hosts map[consensus.PublicKey]SectorDeleter) error {
	rootsByHost := make(map[consensus.PublicKey][]consensus.Hash256)
	for _, s := range slabs {
		for _, sector := range s.Shards {
			rootsByHost[sector.Host] = append(rootsByHost[sector.Host], sector.Root)
		}
	}
	var errs HostErrorSet
	for hostKey, roots := range rootsByHost {
		sd, ok := hosts[hostKey]
		if !ok {
			errs = append(errs, &HostError{hostKey, errors.New("unknown host")})
			continue
		}
		if err := sd.DeleteSectors(roots); err != nil {
			errs = append(errs, &HostError{hostKey, err})
			continue
		}
	}
	if len(errs) > 0 {
		return errs
	}
	return nil
}

// serialMigrateSlab migrates a slab one shard at a time.
func serialMigrateSlab(s *Slab, from map[consensus.PublicKey]SectorDownloader, to map[consensus.PublicKey]SectorUploader) error {
	ss := Slice{*s, 0, uint32(s.MinShards) * rhpv2.SectorSize}
	shards, err := serialDownloadSlab(ss, from)
	if err != nil {
		return err
	}
	ss.Decrypt(shards)
	if err := ReconstructSlab(*s, shards); err != nil {
		return err
	}
	s.Encrypt(shards)

	hosts := make([]consensus.PublicKey, 0, len(to))
	for hostKey := range to {
		hosts = append(hosts, hostKey)
	}
	migrate := func(shard []byte) (Sector, error) {
		var errs HostErrorSet
		for len(hosts) > 0 {
			hostKey := hosts[0]
			hosts = hosts[1:]
			root, err := to[hostKey].UploadSector((*[rhpv2.SectorSize]byte)(shard))
			if err != nil {
				errs = append(errs, &HostError{hostKey, err})
				continue
			}
			return Sector{
				Host: hostKey,
				Root: root,
			}, nil
		}
		if len(errs) > 0 {
			return Sector{}, errs
		}
		return Sector{}, errors.New("no hosts available")
	}

	for i, shard := range shards {
		if to[s.Shards[i].Host] != nil {
			continue // already on a good host
		}
		s.Shards[i], err = migrate(shard)
		if err != nil {
			return err
		}
	}
	return nil
}

// MigrateSlabs migrates the provided slabs.
func MigrateSlabs(slabs []Slab, from map[consensus.PublicKey]SectorDownloader, to map[consensus.PublicKey]SectorUploader) error {
	for i := range slabs {
		if err := serialMigrateSlab(&slabs[i], from, to); err != nil {
			return err
		}
	}
	return nil
}
