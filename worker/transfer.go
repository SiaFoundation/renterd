package worker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	"go.sia.tech/renterd/internal/consensus"
	"go.sia.tech/renterd/object"
	rhpv2 "go.sia.tech/renterd/rhp/v2"
	"go.sia.tech/siad/types"
)

// A sectorStore stores contract data.
type sectorStore interface {
	Contract() types.FileContractID
	PublicKey() consensus.PublicKey
	UploadSector(ctx context.Context, sector *[rhpv2.SectorSize]byte) (consensus.Hash256, error)
	DownloadSector(ctx context.Context, w io.Writer, root consensus.Hash256, offset, length uint32) error
	DeleteSectors(roots []consensus.Hash256) error
}

func parallelUploadSlab(ctx context.Context, shards [][]byte, hosts []sectorStore) ([]object.Sector, error) {
	if len(hosts) < len(shards) {
		return nil, fmt.Errorf("fewer hosts than shards, %v<%v", len(hosts), len(shards))
	}

	type req struct {
		host       sectorStore
		shardIndex int
	}
	type resp struct {
		req  req
		root consensus.Hash256
		err  error
	}
	reqChan := make(chan req, len(shards))
	defer close(reqChan)
	respChan := make(chan resp, len(shards))
	worker := func() {
		for req := range reqChan {
			root, err := req.host.UploadSector(ctx, (*[rhpv2.SectorSize]byte)(shards[req.shardIndex]))
			respChan <- resp{req, root, err}
		}
	}

	// spawn workers and send initial requests
	hostIndex := 0
	inflight := 0
	for i := range shards {
		go worker()
		reqChan <- req{hosts[hostIndex], i}
		hostIndex++
		inflight++
	}
	// collect responses
	var errs HostErrorSet
	sectors := make([]object.Sector, len(shards))
	rem := len(shards)
	for rem > 0 && inflight > 0 {
		resp := <-respChan
		inflight--

		if resp.err != nil {
			errs = append(errs, &HostError{resp.req.host.PublicKey(), resp.err})
			// try next host
			if hostIndex < len(hosts) {
				reqChan <- req{hosts[hostIndex], resp.req.shardIndex}
				hostIndex++
				inflight++
			}
		} else {
			sectors[resp.req.shardIndex] = object.Sector{
				Host: resp.req.host.PublicKey(),
				Root: resp.root,
			}
			rem--
		}
	}
	if rem > 0 {
		return nil, errs
	}
	return sectors, nil
}

func uploadSlab(ctx context.Context, r io.Reader, m, n uint8, hosts []sectorStore) (object.Slab, int, error) {
	buf := make([]byte, int(m)*rhpv2.SectorSize)
	shards := make([][]byte, n)
	length, err := io.ReadFull(r, buf)
	if err != nil && err != io.ErrUnexpectedEOF {
		return object.Slab{}, 0, err
	}
	s := object.Slab{
		Key:       object.GenerateEncryptionKey(),
		MinShards: m,
	}
	s.Encode(buf, shards)
	s.Encrypt(shards)

	s.Shards, err = parallelUploadSlab(ctx, shards, hosts)
	if err != nil {
		return object.Slab{}, 0, err
	}
	return s, length, nil
}

func parallelDownloadSlab(ctx context.Context, ss object.SlabSlice, hosts []sectorStore) ([][]byte, error) {
	if len(hosts) < int(ss.MinShards) {
		return nil, errors.New("not enough hosts to recover shard")
	}

	type req struct {
		hostIndex int
	}
	type resp struct {
		req   req
		shard []byte
		err   error
	}
	reqChan := make(chan req, ss.MinShards)
	defer close(reqChan)
	respChan := make(chan resp, ss.MinShards)
	worker := func() {
		for req := range reqChan {
			h := hosts[req.hostIndex]
			var shard *object.Sector
			for i := range ss.Shards {
				if ss.Shards[i].Host == h.PublicKey() {
					shard = &ss.Shards[i]
					break
				}
			}
			if shard == nil {
				respChan <- resp{req, nil, errors.New("slab is not stored on this host")}
				continue
			}
			offset, length := ss.SectorRegion()
			var buf bytes.Buffer
			err := h.DownloadSector(ctx, &buf, shard.Root, offset, length)
			respChan <- resp{req, buf.Bytes(), err}
		}
	}

	// spawn workers and send initial requests
	hostIndex := 0
	inflight := 0
	for i := uint8(0); i < ss.MinShards; i++ {
		go worker()
		reqChan <- req{hostIndex}
		hostIndex++
		inflight++
	}
	// collect responses
	var errs HostErrorSet
	shards := make([][]byte, len(ss.Shards))
	rem := ss.MinShards
	for rem > 0 && inflight > 0 {
		resp := <-respChan
		inflight--

		if resp.err != nil {
			errs = append(errs, &HostError{hosts[resp.req.hostIndex].PublicKey(), resp.err})
			// try next host
			if hostIndex < len(hosts) {
				reqChan <- req{hostIndex}
				hostIndex++
				inflight++
			}
		} else {
			for i := range ss.Shards {
				if ss.Shards[i].Host == hosts[resp.req.hostIndex].PublicKey() {
					shards[i] = resp.shard
					rem--
					break
				}
			}
		}
	}
	if rem > 0 {
		return nil, errs
	}
	return shards, nil
}

func downloadSlab(ctx context.Context, w io.Writer, ss object.SlabSlice, hosts []sectorStore) error {
	shards, err := parallelDownloadSlab(ctx, ss, hosts)
	if err != nil {
		return err
	}
	ss.Decrypt(shards)
	err = ss.Recover(w, shards)
	if err != nil {
		return err
	}
	return nil
}

// slabsForDownload returns the slices that comprise the specified offset-length
// span within slabs.
func slabsForDownload(slabs []object.SlabSlice, offset, length int64) []object.SlabSlice {
	// mutate a copy
	slabs = append([]object.SlabSlice(nil), slabs...)

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

func deleteSlabs(ctx context.Context, slabs []object.Slab, hosts []sectorStore) error {
	rootsBysectorStore := make(map[consensus.PublicKey][]consensus.Hash256)
	for _, s := range slabs {
		for _, sector := range s.Shards {
			rootsBysectorStore[sector.Host] = append(rootsBysectorStore[sector.Host], sector.Root)
		}
	}

	errChan := make(chan *HostError)
	for _, h := range hosts {
		go func(h sectorStore) {
			// NOTE: if host is not storing any sectors, the map lookup will return
			// nil, making this a no-op
			err := h.DeleteSectors(rootsBysectorStore[h.PublicKey()])
			if err != nil {
				errChan <- &HostError{h.PublicKey(), err}
			} else {
				errChan <- nil
			}
		}(h)
	}

	var errs HostErrorSet
	for range hosts {
		if err := <-errChan; err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return errs
	}
	return nil
}

func migrateSlab(ctx context.Context, s *object.Slab, from, to []sectorStore) error {
	// determine which shards need migration
	var shardIndices []int
outer:
	for i, shard := range s.Shards {
		for _, h := range to {
			if h.PublicKey() == shard.Host {
				continue outer
			}
		}
		shardIndices = append(shardIndices, i)
	}
	if len(shardIndices) == 0 {
		return nil
	} else if len(shardIndices) > len(to) {
		return errors.New("not enough hosts to migrate shard")
	}

	// download + reconstruct slab
	ss := object.SlabSlice{
		Slab:   *s,
		Offset: 0,
		Length: uint32(s.MinShards) * rhpv2.SectorSize,
	}
	shards, err := parallelDownloadSlab(ctx, ss, from)
	if err != nil {
		return err
	}
	ss.Decrypt(shards)
	if err := s.Reconstruct(shards); err != nil {
		return err
	}
	s.Encrypt(shards)

	// spawn workers and send initial requests
	type req struct {
		host       sectorStore
		shardIndex int
	}
	type resp struct {
		req  req
		root consensus.Hash256
		err  error
	}
	reqChan := make(chan req, len(shardIndices))
	defer close(reqChan)
	respChan := make(chan resp, len(shardIndices))
	worker := func() {
		for req := range reqChan {
			root, err := req.host.UploadSector(ctx, (*[rhpv2.SectorSize]byte)(shards[req.shardIndex]))
			respChan <- resp{req, root, err}
		}
	}
	hostIndex := 0
	inflight := 0
	for _, i := range shardIndices {
		go worker()
		reqChan <- req{to[hostIndex], i}
		hostIndex++
		inflight++
	}
	// collect responses
	rem := len(shardIndices)
	var errs HostErrorSet
	for rem > 0 && inflight > 0 {
		resp := <-respChan
		inflight--
		if resp.err != nil {
			errs = append(errs, &HostError{resp.req.host.PublicKey(), resp.err})
			// try next host
			if hostIndex < len(to) {
				reqChan <- req{to[hostIndex], resp.req.shardIndex}
				hostIndex++
				inflight++
			}
		} else {
			s.Shards[resp.req.shardIndex] = object.Sector{
				Host: resp.req.host.PublicKey(),
				Root: resp.root,
			}
			rem--
		}
	}
	if rem > 0 {
		return errs
	}
	return nil
}
