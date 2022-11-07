package slab

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

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
type (
	// A HostInteractionSet groups host interactions in the order they occurred.
	HostInteractionSet []*HostInteraction

	// A HostInteraction contains relevant information about an interaction with
	// a host involving slabs.
	HostInteraction struct {
		Timestamp time.Time
		Type      string
		HostKey   consensus.PublicKey
		Duration  time.Duration
		Err       error

		NumRoots uint64 // only for DeleteSectors
	}
)

// Error implements error.
func (hi HostInteraction) Error() string {
	if hi.Err == nil {
		return ""
	}
	return fmt.Sprintf("%x: %v", hi.HostKey[:4], hi.Err.Error())
}

// Error implements error.
func (his HostInteractionSet) Error() string {
	var sb strings.Builder
	for _, hi := range his {
		if hi.Err != nil {
			sb.WriteString(hi.Error())
		}
	}
	// include a leading newline so that the first error isn't printed on the
	// same line as the error context
	return "\n" + sb.String()
}

// HasError returns true if the interaction set contains at least one
// interaction that resulted in an error.
func (his HostInteractionSet) HasError() bool {
	for _, hi := range his {
		if hi.Err != nil {
			return true
		}
	}
	return false
}

// parallelUploadSlab uploads the provided shards in parallel.
func parallelUploadSlab(shards [][]byte, hosts []Host) ([]Sector, []*HostInteraction, error) {
	if len(hosts) < len(shards) {
		return nil, nil, errors.New("fewer hosts than shards")
	}

	type req struct {
		host       Host
		shardIndex int
	}
	type resp struct {
		req      req
		root     consensus.Hash256
		duration time.Duration
		err      error
	}
	reqChan := make(chan req, len(shards))
	defer close(reqChan)
	respChan := make(chan resp, len(shards))
	worker := func() {
		for req := range reqChan {
			start := time.Now()
			root, err := req.host.UploadSector((*[rhpv2.SectorSize]byte)(shards[req.shardIndex]))
			respChan <- resp{req, root, time.Since(start), err}
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
	var interactions HostInteractionSet
	sectors := make([]Sector, len(shards))
	rem := len(shards)
	for rem > 0 && inflight > 0 {
		resp := <-respChan
		inflight--

		interactions = append(interactions, &HostInteraction{
			Timestamp: time.Now(),
			Duration:  resp.duration,
			Err:       resp.err,
			HostKey:   resp.req.host.PublicKey(),
			Type:      "UploadSector",
		})

		if resp.err != nil {
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
		return nil, interactions, fmt.Errorf("slab upload failed, %v shards remaining, host errors: %v", rem, interactions.Error())
	}
	return sectors, interactions, nil
}

// UploadSlab uploads a slab.
func UploadSlab(r io.Reader, m, n uint8, hosts []Host) (Slab, []*HostInteraction, error) {
	buf := make([]byte, int(m)*rhpv2.SectorSize)
	shards := make([][]byte, n)
	_, err := io.ReadFull(r, buf)
	if err != nil && err != io.ErrUnexpectedEOF {
		return Slab{}, nil, err
	}
	s := Slab{
		Key:       GenerateEncryptionKey(),
		MinShards: m,
	}
	s.Encode(buf, shards)
	s.Encrypt(shards)

	var interactions []*HostInteraction
	s.Shards, interactions, err = parallelUploadSlab(shards, hosts)
	if err != nil {
		return Slab{}, interactions, err
	}
	return s, interactions, nil
}

// parallelDownloadSlab downloads the shards comprising a slab in parallel.
func parallelDownloadSlab(s Slice, hosts []Host) ([][]byte, []*HostInteraction, error) {
	if len(hosts) < int(s.MinShards) {
		return nil, nil, errors.New("not enough hosts to recover shard")
	}

	type req struct {
		hostIndex int
	}
	type resp struct {
		req      req
		shard    []byte
		duration time.Duration
		err      error
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
				respChan <- resp{req, nil, 0, errors.New("slab is not stored on this host")}
				continue
			}
			offset, length := s.SectorRegion()
			var buf bytes.Buffer
			start := time.Now()
			err := h.DownloadSector(&buf, shard.Root, offset, length)
			respChan <- resp{req, buf.Bytes(), time.Since(start), err}
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
	var interactions HostInteractionSet
	shards := make([][]byte, len(s.Shards))
	rem := s.MinShards
	for rem > 0 && inflight > 0 {
		resp := <-respChan
		inflight--

		interactions = append(interactions, &HostInteraction{
			Timestamp: time.Now(),
			Duration:  resp.duration,
			Err:       resp.err,
			HostKey:   hosts[resp.req.hostIndex].PublicKey(),
			Type:      "DownloadSector",
		})

		if resp.err != nil {
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
		return nil, interactions, fmt.Errorf("slab download failed, %v shards remaining, host errors: %v", rem, interactions.Error())
	}
	return shards, interactions, nil
}

// DownloadSlab downloads slab data.
func DownloadSlab(w io.Writer, s Slice, hosts []Host) ([]*HostInteraction, error) {
	shards, interactions, err := parallelDownloadSlab(s, hosts)
	if err != nil {
		return interactions, err
	}
	s.Decrypt(shards)
	if err := s.Recover(w, shards); err != nil {
		return interactions, err
	}
	return interactions, nil
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
func DeleteSlabs(slabs []Slab, hosts []Host) ([]*HostInteraction, error) {
	rootsByHost := make(map[consensus.PublicKey][]consensus.Hash256)
	for _, s := range slabs {
		for _, sector := range s.Shards {
			rootsByHost[sector.Host] = append(rootsByHost[sector.Host], sector.Root)
		}
	}

	interactionChan := make(chan *HostInteraction)
	for _, h := range hosts {
		go func(h Host) {
			// NOTE: if host is not storing any sectors, the map lookup will return
			// nil, making this a no-op
			start := time.Now()
			err := h.DeleteSectors(rootsByHost[h.PublicKey()])
			interactionChan <- &HostInteraction{
				Timestamp: time.Now(),
				HostKey:   h.PublicKey(),
				Err:       err,
				Duration:  time.Since(start),
				Type:      "DeleteSectors",
				NumRoots:  uint64(len(rootsByHost[h.PublicKey()])),
			}
		}(h)
	}

	interactions := make([]*HostInteraction, len(hosts))
	for i := range hosts {
		interactions[i] = <-interactionChan
	}

	if HostInteractionSet(interactions).HasError() {
		return interactions, HostInteractionSet(interactions)
	}
	return interactions, nil
}

// MigrateSlab migrates a slab.
func MigrateSlab(s *Slab, from, to []Host) ([]*HostInteraction, error) {
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
		return nil, nil
	} else if len(shardIndices) > len(to) {
		return nil, errors.New("not enough hosts to migrate shard")
	}

	// download + reconstruct slab
	ss := Slice{*s, 0, uint32(s.MinShards) * rhpv2.SectorSize}
	shards, dlInteractions, err := parallelDownloadSlab(ss, from)
	if err != nil {
		return dlInteractions, err
	}
	ss.Decrypt(shards)
	if err := s.Reconstruct(shards); err != nil {
		return dlInteractions, err
	}
	s.Encrypt(shards)

	// reupload + overwrite migrated shards
	var m int
	migrations := make([][]byte, len(shardIndices))
	for _, i := range shardIndices {
		migrations[m] = shards[i]
		m++
	}

	migrated, ulInteractions, err := parallelUploadSlab(migrations, to)
	if err != nil {
		return append(dlInteractions, ulInteractions...), err
	}
	m = 0
	for _, i := range shardIndices {
		s.Shards[i] = migrated[m]
		m++
	}
	return append(dlInteractions, ulInteractions...), nil
}
