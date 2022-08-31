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

	// An Uploader uploads slab data.
	Uploader interface {
		UploadSlab(shards [][]byte) ([]Sector, error)
	}

	// A Downloader downloads slab data. Sector data can only be downloaded in
	// multiplies of rhpv2.LeafSize, so if the offset and length of s are not
	// leaf-aligned, the returned shards will contain unwanted bytes at the
	// beginning and/or end.
	Downloader interface {
		DownloadSlab(s Slice) ([][]byte, error)
	}

	// A Migrator migrates slab data.
	Migrator interface {
		MigrateSlab(s *Slab) error
	}
)

// A SerialSlabUploader uploads the shards comprising a slab one at a time.
type SerialSlabUploader struct {
	Hosts map[consensus.PublicKey]SectorUploader
}

// UploadSlab implements Uploader.
func (ssu SerialSlabUploader) UploadSlab(shards [][]byte) ([]Sector, error) {
	var sectors []Sector
	var errs HostErrorSet
	for hostKey, su := range ssu.Hosts {
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

// A SerialSlabDownloader downloads the shards comprising a slab one at a time.
type SerialSlabDownloader struct {
	Hosts map[consensus.PublicKey]SectorDownloader
}

// DownloadSlab implements Downloader.
func (ssd SerialSlabDownloader) DownloadSlab(s Slice) ([][]byte, error) {
	offset, length := s.SectorRegion()
	shards := make([][]byte, len(s.Shards))
	rem := s.MinShards
	var errs HostErrorSet
	for i, sector := range s.Shards {
		sd, ok := ssd.Hosts[sector.Host]
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

// A SerialSlabsUploader uploads slabs one at a time.
type SerialSlabsUploader struct {
	Uploader Uploader
}

// UploadSlabs uploads slabs read from the provided Reader.
func (ssu SerialSlabsUploader) UploadSlabs(r io.Reader, m, n uint8) ([]Slab, error) {
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
		s.Shards, err = ssu.Uploader.UploadSlab(shards)
		if err != nil {
			return nil, err
		}
		slabs = append(slabs, s)
	}
	return slabs, nil
}

func slabsSize(slabs []Slice) (n int64) {
	for _, ss := range slabs {
		n += int64(ss.Length)
	}
	return
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

// A SerialSlabsDownloader downloads slabs one at a time.
type SerialSlabsDownloader struct {
	Downloader Downloader
}

// DownloadSlabs downloads data from the supplied slabs.
func (ssd SerialSlabsDownloader) DownloadSlabs(w io.Writer, slabs []Slice, offset, length int64) error {
	if offset < 0 || length < 0 || offset+length > slabsSize(slabs) {
		return errors.New("requested range is out of bounds")
	} else if length == 0 {
		return nil
	}

	slabs = slabsForDownload(slabs, offset, length)
	for _, ss := range slabs {
		shards, err := ssd.Downloader.DownloadSlab(ss)
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

// A SerialSlabDeleter deletes slabs one host at a time.
type SerialSlabsDeleter struct {
	Hosts map[consensus.PublicKey]SectorDeleter
}

// DeleteSlab implements Deleter.
func (ssd SerialSlabsDeleter) DeleteSlabs(slabs []Slab) error {
	rootsByHost := make(map[consensus.PublicKey][]consensus.Hash256)
	for _, s := range slabs {
		for _, sector := range s.Shards {
			rootsByHost[sector.Host] = append(rootsByHost[sector.Host], sector.Root)
		}
	}
	var errs HostErrorSet
	for hostKey, roots := range rootsByHost {
		sd, ok := ssd.Hosts[hostKey]
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

// A SerialSlabMigrator migrates the shards comprising a slab one at a time.
type SerialSlabMigrator struct {
	From map[consensus.PublicKey]SectorDownloader
	To   map[consensus.PublicKey]SectorUploader
}

// MigrateSlab implements Migrator.
func (ssd SerialSlabMigrator) MigrateSlab(s *Slab) error {
	ss := Slice{*s, 0, uint32(s.MinShards) * rhpv2.SectorSize}
	shards, err := SerialSlabDownloader{ssd.From}.DownloadSlab(ss)
	if err != nil {
		return err
	}
	ss.Decrypt(shards)
	if err := ReconstructSlab(*s, shards); err != nil {
		return err
	}
	s.Encrypt(shards)

	hosts := make([]consensus.PublicKey, 0, len(ssd.To))
	for hostKey := range ssd.To {
		hosts = append(hosts, hostKey)
	}
	migrate := func(shard []byte) (Sector, error) {
		var errs HostErrorSet
		for len(hosts) > 0 {
			hostKey := hosts[0]
			hosts = hosts[1:]
			root, err := ssd.To[hostKey].UploadSector((*[rhpv2.SectorSize]byte)(shard))
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
		if ssd.To[s.Shards[i].Host] != nil {
			continue // already on a good host
		}
		s.Shards[i], err = migrate(shard)
		if err != nil {
			return err
		}
	}
	return nil
}

// A SerialSlabsMigrator migrates slabs one at a time.
type SerialSlabsMigrator struct {
	Migrator Migrator
}

// MigrateSlabs migrates the provided slabs.
func (ssd SerialSlabsMigrator) MigrateSlabs(slabs []Slab) error {
	for i := range slabs {
		if err := ssd.Migrator.MigrateSlab(&slabs[i]); err != nil {
			return err
		}
	}
	return nil
}
