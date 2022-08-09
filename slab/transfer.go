package slab

import (
	"bytes"
	"context"
	"errors"
	"io"

	"go.sia.tech/renterd/internal/consensus"
	rhpv2 "go.sia.tech/renterd/rhp/v2"
)

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

	SlabReader interface {
		ReadSlab() (Slab, [][]byte, error)
	}

	SlabUploader interface {
		UploadSlab(ctx context.Context, shards [][]byte) ([]Sector, error)
	}

	// A SlabDownloader downloads a set of shards that, when recovered, contain
	// the specified Slice. Shard data can only be downloaded in multiplies
	// of rhpv2.LeafSize, so if the offset and length of ss are not
	// leaf-aligned, the recovered data will contain unwanted bytes at the
	// beginning and/or end.
	SlabDownloader interface {
		DownloadSlab(ctx context.Context, ss Slice) ([][]byte, error)
	}

	SlabDeleter interface {
		DeleteSlab(ctx context.Context, s Slab) error
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

func (ssd SerialSlabDownloader) DownloadSlab(ctx context.Context, ss Slice) ([][]byte, error) {
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

func slabsSize(slabs []Slice) (n int64) {
	for _, ss := range slabs {
		n += int64(ss.Length)
	}
	return
}

func slabsForDownload(slabs []Slice, offset, length int64) []Slice {
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
	slabs = append([]Slice(nil), slabs...)
	slabs[0].Offset += uint32(firstOffset)
	slabs[0].Length -= uint32(firstOffset)
	slabs[len(slabs)-1].Length = uint32(lastLength)
	return slabs
}

type SerialSlabsDownloader struct {
	SlabDownloader SlabDownloader
}

func (ssd SerialSlabsDownloader) DownloadSlabs(ctx context.Context, w io.Writer, slabs []Slice, offset, length int64) error {
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
		if err := RecoverSlab(w, ss, shards); err != nil {
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
