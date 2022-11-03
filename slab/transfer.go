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

// parallelUploadSlab uploads the provided shards in parallel.
func parallelUploadSlab(shards [][]byte, hosts []Host) ([]Sector, error) {
	if len(hosts) < len(shards) {
		return nil, errors.New("fewer hosts than shards")
	}

	type req struct {
		host       Host
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
			root, err := req.host.UploadSector((*[rhpv2.SectorSize]byte)(shards[req.shardIndex]))
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
	sectors := make([]Sector, len(shards))
	rem := len(shards)
	var errs HostErrorSet
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
			sectors[resp.req.shardIndex] = Sector{
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

// UploadSlab uploads a slab.
func UploadSlab(r io.Reader, m, n uint8, hosts []Host) (Slab, error) {
	buf := make([]byte, int(m)*rhpv2.SectorSize)
	shards := make([][]byte, n)
	_, err := io.ReadFull(r, buf)
	if err != nil && err != io.ErrUnexpectedEOF {
		return Slab{}, err
	}
	s := Slab{
		Key:       GenerateEncryptionKey(),
		MinShards: m,
	}
	s.Encode(buf, shards)
	s.Encrypt(shards)
	s.Shards, err = parallelUploadSlab(shards, hosts)
	if err != nil {
		return Slab{}, err
	}
	return s, nil
}

// parallelDownloadSlab downloads the shards comprising a slab in parallel.
func parallelDownloadSlab(s Slice, hosts []Host) ([][]byte, error) {
	if len(hosts) < int(s.MinShards) {
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
	reqChan := make(chan req, s.MinShards)
	defer close(reqChan)
	respChan := make(chan resp, s.MinShards)
	worker := func() {
		for req := range reqChan {
			h := hosts[req.hostIndex]
			var shard *Sector
			for i := range s.Shards {
				if s.Shards[i].Host == h.PublicKey() {
					shard = &s.Shards[i]
					break
				}
			}
			if shard == nil {
				respChan <- resp{req, nil, errors.New("slab is not stored on this host")}
				continue
			}
			offset, length := s.SectorRegion()
			var buf bytes.Buffer
			err := h.DownloadSector(&buf, shard.Root, offset, length)
			respChan <- resp{req, buf.Bytes(), err}
		}
	}

	// spawn workers and send initial requests
	hostIndex := 0
	inflight := 0
	for i := uint8(0); i < s.MinShards; i++ {
		go worker()
		reqChan <- req{hostIndex}
		hostIndex++
		inflight++
	}
	// collect responses
	shards := make([][]byte, len(s.Shards))
	rem := s.MinShards
	var errs HostErrorSet
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
			for i := range s.Shards {
				if s.Shards[i].Host == hosts[resp.req.hostIndex].PublicKey() {
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

// DownloadSlab downloads slab data.
func DownloadSlab(w io.Writer, s Slice, hosts []Host) error {
	shards, err := parallelDownloadSlab(s, hosts)
	if err != nil {
		return err
	}
	s.Decrypt(shards)
	if err := s.Recover(w, shards); err != nil {
		return err
	}
	return nil
}

// SlabsForDownload returns the slices that comprise the specified offset-length
// span within slabs.
func SlabsForDownload(slabs []Slice, offset, length int64) []Slice {
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

// DeleteSlabs deletes a set of slabs from the provided hosts.
func DeleteSlabs(slabs []Slab, hosts []Host) error {
	rootsByHost := make(map[consensus.PublicKey][]consensus.Hash256)
	for _, s := range slabs {
		for _, sector := range s.Shards {
			rootsByHost[sector.Host] = append(rootsByHost[sector.Host], sector.Root)
		}
	}
	errChan := make(chan *HostError)
	for _, h := range hosts {
		go func(h Host) {
			// NOTE: if host is not storing any sectors, the map lookup will return
			// nil, making this a no-op
			if err := h.DeleteSectors(rootsByHost[h.PublicKey()]); err != nil {
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

// MigrateSlab migrates a slab.
func MigrateSlab(s *Slab, from, to []Host) error {
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
	ss := Slice{*s, 0, uint32(s.MinShards) * rhpv2.SectorSize}
	shards, err := parallelDownloadSlab(ss, from)
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
		host       Host
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
			root, err := req.host.UploadSector((*[rhpv2.SectorSize]byte)(shards[req.shardIndex]))
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
			s.Shards[resp.req.shardIndex] = Sector{
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
