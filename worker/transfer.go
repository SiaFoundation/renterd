package worker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/renterd/object"
	rhpv2 "go.sia.tech/renterd/rhp/v2"
	"lukechampine.com/frand"
)

const (
	contractLockingUploadPriority   = 1
	contractLockingDownloadPriority = 2
)

var errUploadSectorTimeout = errors.New("upload sector timed out")

// A sectorStore stores contract data.
type sectorStore interface {
	Contract() types.FileContractID
	PublicKey() types.PublicKey
	UploadSector(ctx context.Context, sector *[rhpv2.SectorSize]byte) (types.Hash256, error)
	DownloadSector(ctx context.Context, w io.Writer, root types.Hash256, offset, length uint32) error
	DeleteSectors(ctx context.Context, roots []types.Hash256) error
}

func parallelUploadSlab(ctx context.Context, shards [][]byte, hosts []sectorStore, locker contractLocker, uploadSectorTimeout time.Duration) ([]object.Sector, []int, error) {
	if len(hosts) < len(shards) {
		return nil, nil, fmt.Errorf("fewer hosts than shards, %v<%v", len(hosts), len(shards))
	}

	type req struct {
		host       sectorStore
		shardIndex int
	}
	type resp struct {
		req  req
		root types.Hash256
		err  error
	}
	reqChan := make(chan req, len(shards))
	defer close(reqChan)
	respChan := make(chan resp, len(shards))
	worker := func() {
		for r := range reqChan {
			doneChan := make(chan struct{})
			go func(req req) {
				defer close(doneChan)

				lockID, err := locker.AcquireContract(ctx, req.host.Contract(), contractLockingUploadPriority, 30*time.Second)
				if err != nil {
					respChan <- resp{req, types.Hash256{}, err}
					return
				}
				defer locker.ReleaseContract(req.host.Contract(), lockID)

				root, err := req.host.UploadSector(ctx, (*[rhpv2.SectorSize]byte)(shards[req.shardIndex]))
				respChan <- resp{req, root, err}
			}(r)

			if uploadSectorTimeout > 0 {
				timer := time.NewTimer(uploadSectorTimeout)
				select {
				case <-timer.C:
					respChan <- resp{
						req: r,
						err: errUploadSectorTimeout}
				case <-doneChan:
					if !timer.Stop() {
						<-timer.C
					}
				}
			}

			<-doneChan
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
		if errors.Is(resp.err, errUploadSectorTimeout) {
			inflight--
		}

		if resp.err != nil {
			errs = append(errs, &HostError{resp.req.host.PublicKey(), resp.err})
			// try next host
			if hostIndex < len(hosts) {
				reqChan <- req{hosts[hostIndex], resp.req.shardIndex}
				hostIndex++
				inflight++
			}
		} else if sectors[resp.req.shardIndex].Root == (types.Hash256{}) {
			sectors[resp.req.shardIndex] = object.Sector{
				Host: resp.req.host.PublicKey(),
				Root: resp.root,
			}
			rem--
		}
	}
	if rem > 0 {
		return nil, nil, errs
	}

	// make hosts map
	hostsMap := make(map[types.PublicKey]int)
	for i, h := range hosts {
		hostsMap[h.PublicKey()] = i
	}

	// collect slow host indices
	var slowHosts []int
	for _, he := range errs {
		if errors.Is(he, errUploadSectorTimeout) {
			if _, exists := hostsMap[he.HostKey]; !exists {
				panic("host not found in hostsmap")
			}
			slowHosts = append(slowHosts, hostsMap[he.HostKey])
		}
	}
	return sectors, slowHosts, nil
}

func uploadSlab(ctx context.Context, r io.Reader, m, n uint8, hosts []sectorStore, locker contractLocker, uploadSectorTimeout time.Duration) (object.Slab, int, []int, error) {
	buf := make([]byte, int(m)*rhpv2.SectorSize)
	shards := make([][]byte, n)
	length, err := io.ReadFull(r, buf)
	if err != nil && err != io.ErrUnexpectedEOF {
		return object.Slab{}, 0, nil, err
	}
	s := object.Slab{
		Key:       object.GenerateEncryptionKey(),
		MinShards: m,
	}
	s.Encode(buf, shards)
	s.Encrypt(shards)

	sectors, slowHosts, err := parallelUploadSlab(ctx, shards, hosts, locker, uploadSectorTimeout)
	if err != nil {
		return object.Slab{}, 0, nil, err
	}

	s.Shards = sectors
	return s, length, slowHosts, nil
}

func parallelDownloadSlab(ctx context.Context, ss object.SlabSlice, hosts []sectorStore, locker contractLocker) ([][]byte, error) {
	if len(hosts) < int(ss.MinShards) {
		return nil, errors.New("not enough hosts to recover shard")
	}

	// Randomize order of hosts to make sure we don't always start with the
	// same one.
	frand.Shuffle(len(hosts), func(i, j int) { hosts[i], hosts[j] = hosts[j], hosts[i] })

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
			lockID, err := locker.AcquireContract(ctx, h.Contract(), contractLockingDownloadPriority, 30*time.Second)
			if err != nil {
				respChan <- resp{req, nil, err}
				return
			}
			err = h.DownloadSector(ctx, &buf, shard.Root, offset, length)
			locker.ReleaseContract(h.Contract(), lockID)
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

func downloadSlab(ctx context.Context, w io.Writer, ss object.SlabSlice, hosts []sectorStore, locker contractLocker) error {
	shards, err := parallelDownloadSlab(ctx, ss, hosts, locker)
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
	rootsBysectorStore := make(map[types.PublicKey][]types.Hash256)
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
			err := h.DeleteSectors(ctx, rootsBysectorStore[h.PublicKey()])
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

func migrateSlab(ctx context.Context, s *object.Slab, hosts []sectorStore, locker contractLocker, uploadSectorTimeout time.Duration) error {
	hostsMap := make(map[string]struct{})
	usedMap := make(map[string]struct{})

	// make a map of good hosts
	for _, h := range hosts {
		hostsMap[h.PublicKey().String()] = struct{}{}
	}

	// collect indices of shards that need to be migrated
	var shardIndices []int
	for i, shard := range s.Shards {
		// bad host
		if _, exists := hostsMap[shard.Host.String()]; !exists {
			shardIndices = append(shardIndices, i)
			continue
		}

		// reused host
		_, exists := usedMap[shard.Host.String()]
		if exists {
			shardIndices = append(shardIndices, i)
			continue
		}

		usedMap[shard.Host.String()] = struct{}{}
	}

	// if all shards are on good hosts, we're done
	if len(shardIndices) == 0 {
		return nil
	}

	// perform some sanity checks
	if len(s.Shards)-len(shardIndices) > int(s.MinShards) {
		return errors.New("not enough hosts to download unhealthy shard")
	} else if len(shardIndices) > len(hosts) {
		return errors.New("not enough hosts to migrate shard")
	}

	// download + reconstruct slab
	ss := object.SlabSlice{
		Slab:   *s,
		Offset: 0,
		Length: uint32(s.MinShards) * rhpv2.SectorSize,
	}
	shards, err := parallelDownloadSlab(ctx, ss, hosts, locker)
	if err != nil {
		return err
	}
	ss.Decrypt(shards)
	if err := s.Reconstruct(shards); err != nil {
		return err
	}
	s.Encrypt(shards)

	// filter it down to the shards we need to migrate
	for i, si := range shardIndices {
		shards[i] = shards[si]
	}
	shards = shards[:len(shardIndices)]

	// filter out the hosts we used already
	filtered := hosts[:0]
	for _, h := range hosts {
		if _, used := usedMap[h.PublicKey().String()]; !used {
			filtered = append(filtered, h)
		}
	}

	// randomize order of hosts to make sure we don't migrate to the same hosts all the time
	frand.Shuffle(len(filtered), func(i, j int) { filtered[i], filtered[j] = filtered[j], filtered[i] })

	// reupload those shards
	uploaded, _, err := parallelUploadSlab(ctx, shards, filtered, locker, uploadSectorTimeout)
	if err != nil {
		return err
	}

	// overwrite the unhealthy shards with the newly migrated ones
	for i, si := range shardIndices {
		s.Shards[si] = uploaded[i]
	}
	return nil
}
